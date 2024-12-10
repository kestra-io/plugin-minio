package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
                  id: file
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
            title = "Upload file to an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
              id: s3_compatible_upload
              namespace: company.team

              tasks:
                - id: http_download
                  type: io.kestra.plugin.core.http.Download
                  uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv\

                - id: upload_to_storage
                  type: io.kestra.plugin.minio.Upload
                  accessKeyId: "<access-key>"
                  secretKeyId: "<secret-key>"
                  endpoint: https://<region>.digitaloceanspaces.com  #example regions: nyc3, tor1
                  bucket: "kestra-test-bucket"
                  from: "{{ outputs.http_download.uri }}"
                  key: "data/orders.csv"
              """
        )
    }
)
@Schema(
    title = "Upload a file to a bucket."
)
public class Upload extends AbstractMinioObject implements RunnableTask<Upload.Output> {

    @Schema(
        title = "The key where to upload the file.",
        description = "a full key (with filename) or the directory path if from is multiple files."
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "The file(s) to upload.",
        description = "Can be a single file, a list of files or json array.",
        anyOf = {List.class, String.class}
    )
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "A standard MIME type describing the format of the contents."
    )
    @PluginProperty(dynamic = true)
    private String contentType;

    @Schema(
        title = "A map of metadata to store with the object."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> metadata;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElse(null);
        String key = runContext.render(this.key);

        String[] renderedFroms;
        if (this.from instanceof Collection<?> fromURIs) {
            renderedFroms = fromURIs.stream().map(throwFunction(from -> runContext.render((String) from))).toArray(String[]::new);
        } else if (this.from instanceof String) {
            renderedFroms = new String[]{runContext.render((String) this.from)};
        } else {
            renderedFroms = JacksonMapper.ofJson().readValue(runContext.render((String) this.from), String[].class);
        }

        try (MinioAsyncClient client = this.asyncClient(runContext)) {
            UploadObjectArgs.Builder builder = UploadObjectArgs.builder()
                .bucket(bucket)
                .object(key);

            if (this.metadata != null) {
                builder.userMetadata(runContext.renderMap(this.metadata));
            }

            if (this.contentType != null) {
                builder.contentType(runContext.render(this.contentType));
            }

            for (String renderedFrom : renderedFroms) {
                File tempFile = runContext.workingDir().createTempFile(FilenameUtils.getExtension(renderedFrom)).toFile();
                URI from = new URI(runContext.render(renderedFrom));
                Files.copy(runContext.storage().getFile(from), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                // if multiple files, it's a dir
                if (renderedFroms.length > 1) {
                    builder.object(Path.of(key, FilenameUtils.getName(renderedFrom)).toString());
                }

                builder.filename(tempFile.getPath());

                UploadObjectArgs objectToUpload = builder.build();

                runContext.logger().debug("Uploading to '{}'", objectToUpload.object());

                CompletableFuture<ObjectWriteResponse> upload = client.uploadObject(objectToUpload);

                ObjectWriteResponse response = upload.get();

                runContext.metric(Counter.of("file.count", 1));
                runContext.metric(Counter.of("file.size", tempFile.length()));

                if (renderedFroms.length == 1) {
                    return Output
                        .builder()
                        .bucket(bucket)
                        .key(key)
                        .eTag(response.etag())
                        .versionId(response.versionId())
                        .build();
                }
            }

            return Output
                .builder()
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
