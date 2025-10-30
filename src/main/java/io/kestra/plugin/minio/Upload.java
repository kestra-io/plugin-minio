package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.property.Data;
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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


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
        String bucket = runContext.render(this.bucket).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Bucket name is required"));
        String key = runContext.render(this.key).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Object key is required"));

        List<String> renderedFroms = Data.from(this.from)
            .read(runContext)
            .map(Object::toString)
            .collectList()
            .block();

        try (MinioAsyncClient client = this.asyncClient(runContext)) {
            var metadataValue = runContext.render(this.metadata).asMap(String.class, String.class);

            for (String renderedFrom : renderedFroms) {
                URI fromUri = new URI(renderedFrom);
                Path tempFile = runContext.workingDir()
                    .createTempFile(FilenameUtils.getExtension(renderedFrom));

                Files.copy(runContext.storage().getFile(fromUri), tempFile, StandardCopyOption.REPLACE_EXISTING);

                String objectKey = (renderedFroms.size() > 1)
                    ? Path.of(key, FilenameUtils.getName(renderedFrom)).toString()
                    : key;

                UploadObjectArgs.Builder builder = UploadObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectKey)
                    .filename(tempFile.toString());

                if (!metadataValue.isEmpty()) {
                    builder.userMetadata(metadataValue);
                }

                if (this.contentType != null) {
                    builder.contentType(runContext.render(this.contentType).as(String.class).orElse(null));
                }

                runContext.logger().info("Uploading file '{}' to bucket '{}' as '{}'", renderedFrom, bucket, objectKey);

                CompletableFuture<ObjectWriteResponse> upload = client.uploadObject(builder.build());
                ObjectWriteResponse response = upload.get();

                runContext.metric(Counter.of("file.count", 1));
                runContext.metric(Counter.of("file.size", Files.size(tempFile)));

                if (renderedFroms.size() == 1) {
                    return Output.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .eTag(response.etag())
                        .versionId(response.versionId())
                        .build();
                }
            }

            return Output.builder()
                .bucket(bucket)
                .key(key)
                .build();
        }
    }

    @SuperBuilder
    @Getter
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private final String bucket;
        private final String key;
    }
}
