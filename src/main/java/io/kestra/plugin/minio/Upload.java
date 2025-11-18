package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.minio.model.ObjectOutput;
import io.minio.MinioAsyncClient;
import io.minio.ObjectWriteResponse;
import io.minio.UploadObjectArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: minio_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: upload_to_storage
                    type: io.kestra.plugin.minio.Upload
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    from: "{{ inputs.file }}"
                    bucket: "my-bucket"
                    key: "path/to/file"
                """
        ),
        @Example(
            title = "Upload file to S3-compatible storage (e.g., DigitalOcean Spaces).",
            full = true,
            code = """
                id: s3_compatible_upload
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv

                  - id: upload_to_storage
                    type: io.kestra.plugin.minio.Upload
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    endpoint: https://<region>.digitaloceanspaces.com
                    bucket: "kestra-test-bucket"
                    from: "{{ outputs.http_download.uri }}"
                    key: "data/orders.csv"
                """
        )
    },
    metrics = {
        @Metric(
            name = "file.count",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of files successfully uploaded to the MinIO bucket."
        ),
        @Metric(
            name = "file.size",
            type = Counter.TYPE,
            unit = "bytes",
            description = "Size of the uploaded files in bytes."
        )
    }
)
@Schema(
    title = "Upload a file to a MinIO bucket."
)
public class Upload extends AbstractMinioObject implements RunnableTask<Upload.Output>, Data.From {

    @Schema(
        title = "The key where to upload the file.",
        description = "a full key (with filename) or the directory path if from is multiple files."
    )
    private Property<String> key;

    @Schema(
        title = "The file(s) to upload.",
        description = "Can be a single file, a list of files or json array.",
        anyOf = {List.class, String.class, Map.class}
    )
    @NotNull
    @PluginProperty(dynamic = true, internalStorageURI = true)
    private Object from;

    @Schema(
        title = "A standard MIME type describing the format of the contents."
    )
    private Property<String> contentType;

    @Schema(
        title = "A map of metadata to store with the object."
    )
    private Property<Map<String, String>> metadata;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElseThrow();
        String baseKey = runContext.render(this.key).as(String.class).orElseThrow();

        Map<String, String> filesToUpload = parseFromProperty(runContext);
        if (filesToUpload.isEmpty()) {
            throw new IllegalArgumentException("No files to upload: the 'from' property contains an empty collection or array");
        }

        try (MinioAsyncClient client = this.asyncClient(runContext)) {

            if (filesToUpload.size() == 1) {
                return uploadSingle(runContext, client, bucket, baseKey, filesToUpload.values().iterator().next());
            } else {
                return uploadMultiple(runContext, client, bucket, baseKey, filesToUpload);
            }
        }
    }

    private Map<String, String> parseFromProperty(RunContext runContext) throws Exception {
        Data data = Data.from(this.from);

        try {
            Function<Map<String, Object>, Map<String, String>> mapper = map -> {
                Map<String, String> result = new HashMap<>();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    result.put(entry.getKey(), entry.getValue().toString());
                }
                return result;
            };

            @SuppressWarnings("unchecked")
            Flux<Map<String, String>> rFromMap = data.readAs(runContext, (Class<Map<String, String>>) Map.of().getClass(), mapper);
            return Objects.requireNonNull(rFromMap.blockFirst());
        } catch (Exception e) {
            runContext.logger().debug("'from' property is not a Map, trying List...", e);
        }

        try {
            @SuppressWarnings("unchecked")
            Flux<java.util.List<String>> asList = data.readAs(
                runContext, (Class<java.util.List<String>>) java.util.List.of().getClass(),
                raw -> raw.keySet().stream().toList()
            );
            java.util.List<String> uris = asList.flatMapIterable(i -> i).collectList().block();
            Map<String, String> r = new HashMap<>();
            for (String uri : Objects.requireNonNull(uris)) {
                r.put(FilenameUtils.getName(uri), uri);
            }
            return r;
        } catch (Exception e) {
            runContext.logger().debug("'from' property is not a List, trying String...", e);
        }

        Flux<String> rFromString = data.readAs(runContext, String.class, Object::toString);
        return uriListToMap(Objects.requireNonNull(rFromString.collectList().block()));
    }

    private Map<String, String> uriListToMap(java.util.List<String> rUriList) {
        Map<String, String> rUriMap = new HashMap<>();
        for (String rUri : rUriList) {
            rUriMap.put(FilenameUtils.getName(rUri), rUri);
        }
        return rUriMap;
    }

    private Output uploadSingle(RunContext runContext, MinioAsyncClient client,
                                String bucket, String key, String uri) throws Exception {
        File tmp = copyTemp(runContext, uri);

        UploadObjectArgs.Builder builder = UploadObjectArgs.builder()
            .bucket(bucket)
            .object(key)
            .filename(tmp.getAbsolutePath());

        applyOptions(runContext, builder);

        ObjectWriteResponse res = client.uploadObject(builder.build()).get();

        runContext.metric(Counter.of("file.count", 1));
        runContext.metric(Counter.of("file.size", tmp.length()));

        return Output.builder()
            .bucket(bucket)
            .key(key)
            .eTag(res.etag())
            .versionId(res.versionId())
            .build();
    }

    private Output uploadMultiple(RunContext runContext, MinioAsyncClient client,
                                  String bucket, String baseKey, Map<String, String> files) throws Exception {

        for (Map.Entry<String, String> entry : files.entrySet()) {
            String relativeName = entry.getKey();
            String uri = entry.getValue();
            String finalKey = Path.of(baseKey, relativeName).toString();

            File tmp = copyTemp(runContext, uri);

            UploadObjectArgs.Builder builder = UploadObjectArgs.builder()
                .bucket(bucket)
                .object(finalKey)
                .filename(tmp.getAbsolutePath());

            applyOptions(runContext, builder);

            ObjectWriteResponse res = client.uploadObject(builder.build()).get();

            runContext.metric(Counter.of("file.count", 1));
            runContext.metric(Counter.of("file.size", tmp.length()));
        }

        return Output.builder()
            .bucket(bucket)
            .key(baseKey)
            .build();
    }

    private File copyTemp(RunContext runContext, String uri) throws Exception {
        File temp = runContext.workingDir().createTempFile(FilenameUtils.getExtension(uri)).toFile();
        URI from = new URI(runContext.render(uri));
        Files.copy(runContext.storage().getFile(from), temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return temp;
    }

    private void applyOptions(RunContext runContext, UploadObjectArgs.Builder builder) throws Exception {
        var metadataValue = runContext.render(this.metadata).asMap(String.class, String.class);
        if (!metadataValue.isEmpty()) {
            builder.userMetadata(metadataValue);
        }

        if (this.contentType != null) {
            builder.contentType(runContext.render(this.contentType).as(String.class).orElseThrow());
        }
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String key;
    }
}
