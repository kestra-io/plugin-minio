package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
            full = true,
            code = """
                id: minio_create_bucket
                namespace: company.team

                tasks:
                  - id: create_bucket
                    type: io.kestra.plugin.minio.CreateBucket
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                """
        ),
        @Example(
            title = "Create a new bucket on an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
              id: s3_compatible_bucket
              namespace: company.team

              tasks:
                - id: create_bucket
                  type: io.kestra.plugin.minio.CreateBucket
                  accessKeyId: "<access_key>"
                  secretKeyId: "<secret_key>"
                  endpoint: https://<region>.digitaloceanspaces.com  #example region: nyc3, tor1
                  bucket: "kestra-test-bucket"
              """
        )
    }
)
@Schema(
    title = "Create a MinIO bucket."
)
public class CreateBucket extends AbstractMinioObject implements RunnableTask<CreateBucket.Output> {

    @Schema(
        title = "Specifies whether you want Object Lock to be enabled for the new bucket."
    )
    private Property<Boolean> objectLockEnabledForBucket;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElse(null);

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
                requestBuilder.objectLock(runContext.render(objectLockEnabledForBucket).as(Boolean.class).orElseThrow());
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
