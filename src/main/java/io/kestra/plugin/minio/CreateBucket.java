package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
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
            title = "Create a new bucket with some options",
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "bucket: \"my-bucket\""
            }
        )
    }
)
@Schema(
    title = "Create a bucket"
)
public class CreateBucket extends AbstractMinioObject implements RunnableTask<CreateBucket.Output> {

    @Schema(
        title = "Specifies whether you want Object Lock to be enabled for the new bucket."
    )
    @PluginProperty
    private Boolean objectLockEnabledForBucket;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);

        try (MinioClient client = this.client(runContext)) {

            BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder().bucket(bucket).build();

            if (client.bucketExists(bucketExistsArgs)) {
                return Output
                    .builder()
                    .bucket(bucket)
                    .build();
            }
            MakeBucketArgs.Builder requestBuilder = MakeBucketArgs.builder().bucket(bucket);

            if (this.objectLockEnabledForBucket != null) {
                requestBuilder.objectLock(objectLockEnabledForBucket);
            }

            client.makeBucket(requestBuilder.build());

            return Output
                .builder()
                .bucket(bucket)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final String bucket;
    }

}
