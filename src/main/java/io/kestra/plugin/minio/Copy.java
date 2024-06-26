package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.minio.model.ObjectOutput;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
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
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "from:",
                "  bucket: \"my-bucket\"",
                "  key: \"path/to/file\"",
                "to:",
                "  bucket: \"my-bucket2\"",
                "  key: \"path/to/file2\"",
            }
        )
    }
)
@Schema(
    title = "Copy a file between buckets."
)
public class Copy extends AbstractMinioObject implements RunnableTask<Copy.Output> {

    @Schema(
        title = "The source bucket and key."
    )
    @PluginProperty
    private CopyObjectFrom from;

    @Schema(
        title = "The destination bucket and key."
    )
    @PluginProperty
    private CopyObject to;

    @Schema(
        title = "Whether to delete the source file after download."
    )
    @PluginProperty
    @Builder.Default
    private Boolean delete = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (MinioClient minioClient = this.client(runContext)) {
            CopySource.Builder sourceBuilder = CopySource.builder()
                .bucket(runContext.render(this.from.bucket))
                .object(runContext.render(this.from.key));

            if (this.from.versionId != null) {
                sourceBuilder.versionId(runContext.render(this.from.versionId));
            }

            CopyObjectArgs.Builder builder = CopyObjectArgs.builder()
                .bucket(runContext.render(this.to.bucket != null ? this.to.bucket : this.from.bucket))
                .object(runContext.render(this.to.key))
                .source(sourceBuilder.build());

            CopyObjectArgs request = builder.build();

            ObjectWriteResponse response = minioClient.copyObject(request);

            if (this.delete) {
                Delete.builder()
                    .id(this.id)
                    .type(Delete.class.getName())
                    .region(this.region)
                    .endpoint(this.endpoint)
                    .accessKeyId(this.accessKeyId)
                    .secretKeyId(this.secretKeyId)
                    .bucket(this.from.bucket)
                    .key(this.from.key)
                    .build()
                    .run(runContext);
            }

            return Output
                .builder()
                .bucket(response.bucket())
                .key(response.object())
                .eTag(response.etag())
                .build();
        }
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class CopyObject {
        @Schema(
            title = "The bucket name"
        )
        @PluginProperty(dynamic = true)
        String bucket;

        @Schema(
            title = "The bucket key"
        )
        @PluginProperty(dynamic = true)
        String key;
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class CopyObjectFrom extends CopyObject {
        @Schema(
            title = "The specific version of the object."
        )
        @PluginProperty(dynamic = true)
        private String versionId;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        private String bucket;
        private String key;
    }

}
