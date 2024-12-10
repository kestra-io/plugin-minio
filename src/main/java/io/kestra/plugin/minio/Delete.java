package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
                id: minio_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.minio.Delete
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    key: "path/to/file"
                """
        ),
        @Example(
            title = "Delete file from an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
              id: s3_compatible_delete
              namespace: company.team

              tasks:
                - id: delete
                  type: io.kestra.plugin.minio.Delete
                  accessKeyId: "<access-key>"
                  secretKeyId: "<secret-key>"
                  endpoint: https://<region>.digitaloceanspaces.com
                  bucket: "kestra-test-bucket"
                  key: "path/to/file"
              """
        )
    }
)
@Schema(
    title = "Delete a file from a bucket."
)
public class Delete extends AbstractMinioObject implements RunnableTask<Delete.Output> {

    @Schema(
        title = "The key to delete."
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "Indicates whether Object Lock should bypass Governance-mode restrictions to process this operation."
    )
    @PluginProperty
    private Boolean bypassGovernanceRetention;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElse(null);
        String key = runContext.render(this.key);

        try (MinioClient minioClient = this.client(runContext)) {
            RemoveObjectArgs.Builder builder = RemoveObjectArgs.builder()
                .bucket(bucket)
                .object(key);

            if (this.bypassGovernanceRetention != null) {
                builder.bypassGovernanceMode(this.bypassGovernanceRetention);
            }

            RemoveObjectArgs request = builder.build();

            minioClient.removeObject(request);

            return Output
                .builder()
                .bucket(bucket)
                .key(key)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private String bucket;
        private String key;
    }

}
