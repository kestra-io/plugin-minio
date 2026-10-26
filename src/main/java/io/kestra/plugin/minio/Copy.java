package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.minio.model.ObjectOutput;

import io.minio.CopyObjectArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.SourceObject;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

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
                id: minio_copy
                namespace: company.team

                tasks:
                  - id: copy
                    type: io.kestra.plugin.minio.Copy
                    accessKeyId: "<access-key>"
                    secretKeyId: "{{ secret('MINIO_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    from:
                      bucket: "my-bucket"
                      key: "path/to/file"
                    to:
                      bucket: "my-bucket2"
                      key: "path/to/file2"
                """
        ),
        @Example(
            title = "Copy file in an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
                id: s3_compatible_copy
                namespace: company.team

                tasks:
                  - id: copy_file
                    type: io.kestra.plugin.minio.Copy
                    accessKeyId: "<access-key>"
                    secretKeyId: "{{ secret('MINIO_SECRET_KEY_ID') }}"
                    endpoint: https://<region>.digitaloceanspaces.com
                    from:
                      bucket: "my-bucket"
                      key: "path/to/file"
                    to:
                      bucket: "my-bucket2"
                      key: "path/to/file2"
                """
        ),
        @Example(
            title = "Copy only when the source ETag matches a listed object",
            full = true,
            code = """
                id: minio_copy_if_match
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.minio.List
                    accessKeyId: "<access-key>"
                    secretKeyId: "{{ secret('MINIO_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "path/to/"

                  - id: copy
                    type: io.kestra.plugin.minio.Copy
                    accessKeyId: "<access-key>"
                    secretKeyId: "{{ secret('MINIO_SECRET_KEY_ID') }}"
                    region: "eu-central-1"
                    from:
                      bucket: "my-bucket"
                      key: "path/to/file"
                      matchETag: "{{ outputs.list.objects[0].etag }}"
                    to:
                      bucket: "my-bucket"
                      key: "path/to/file-copy"
                """
        )
    }
)
@Schema(
    title = "Copy a file between MinIO buckets",
    description = "Copies an object from one MinIO bucket and key to another, optionally deleting the source afterwards."
)
public class Copy extends AbstractMinioObject implements RunnableTask<Copy.Output> {

    @Schema(
        title = "The source bucket and key"
    )
    @PluginProperty(group = "source")
    private CopyObjectFrom from;

    @Schema(
        title = "The destination bucket and key"
    )
    @PluginProperty(group = "destination")
    private CopyObject to;

    @Schema(
        title = "Whether to delete the source file after download"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> delete = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (MinioClient minioClient = this.client(runContext)) {
            SourceObject.Builder sourceBuilder = SourceObject.builder()
                .bucket(runContext.render(this.from.bucket).as(String.class).orElse(null))
                .object(runContext.render(this.from.key).as(String.class).orElse(null));

            if (this.from.versionId != null) {
                sourceBuilder.versionId(runContext.render(this.from.versionId).as(String.class).orElseThrow());
            }

            if (this.from.matchETag != null) {
                sourceBuilder.matchETag(runContext.render(this.from.matchETag).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("from.matchETag rendered to an empty value; check the expression")));
            }

            if (this.from.notMatchETag != null) {
                sourceBuilder.notMatchETag(runContext.render(this.from.notMatchETag).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("from.notMatchETag rendered to an empty value; check the expression")));
            }

            if (this.from.modifiedSince != null) {
                var rModifiedSince = runContext.render(this.from.modifiedSince).as(Instant.class)
                    .orElseThrow(() -> new IllegalArgumentException("from.modifiedSince rendered to an empty value; check the expression"));
                sourceBuilder.modifiedSince(ZonedDateTime.ofInstant(rModifiedSince, ZoneOffset.UTC));
            }

            if (this.from.unmodifiedSince != null) {
                var rUnmodifiedSince = runContext.render(this.from.unmodifiedSince).as(Instant.class)
                    .orElseThrow(() -> new IllegalArgumentException("from.unmodifiedSince rendered to an empty value; check the expression"));
                sourceBuilder.unmodifiedSince(ZonedDateTime.ofInstant(rUnmodifiedSince, ZoneOffset.UTC));
            }

            CopyObjectArgs.Builder builder = CopyObjectArgs.builder()
                .bucket(runContext.render(this.to.bucket != null ? this.to.bucket : this.from.bucket).as(String.class).orElseThrow())
                .object(runContext.render(this.to.key).as(String.class).orElse(null))
                .source(sourceBuilder.build());

            CopyObjectArgs request = builder.build();

            ObjectWriteResponse response = minioClient.copyObject(request);

            if (runContext.render(this.delete).as(Boolean.class).orElseThrow()) {
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
        Property<String> bucket;

        @Schema(
            title = "The bucket key"
        )
        Property<String> key;
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class CopyObjectFrom extends CopyObject {
        @Schema(
            title = "The specific version of the object"
        )
        private Property<String> versionId;

        @Schema(
            title = "Copy only if the source ETag matches",
            description = """
                Maps to the S3 `x-amz-copy-source-if-match` header.
                The copy fails with a precondition error when the source ETag differs.
                """
        )
        @PluginProperty(group = "reliability")
        private Property<String> matchETag;

        @Schema(
            title = "Copy only if the source ETag does not match",
            description = """
                Maps to the S3 `x-amz-copy-source-if-none-match` header.
                The copy fails with a precondition error when the source ETag matches the given value.
                """
        )
        @PluginProperty(group = "reliability")
        private Property<String> notMatchETag;

        @Schema(
            title = "Copy only if the source was modified after this time",
            description = """
                Maps to the S3 `x-amz-copy-source-if-modified-since` header.
                Use an ISO-8601 instant in UTC, for example `2024-06-13T12:00:00Z`.
                The copy fails with a precondition error when the source was not modified after this time.
                """
        )
        @PluginProperty(group = "reliability")
        private Property<Instant> modifiedSince;

        @Schema(
            title = "Copy only if the source was not modified after this time",
            description = """
                Maps to the S3 `x-amz-copy-source-if-unmodified-since` header.
                Use an ISO-8601 instant in UTC, for example `2024-06-13T12:00:00Z`.
                The copy fails with a precondition error when the source was modified after this time.
                """
        )
        @PluginProperty(group = "reliability")
        private Property<Instant> unmodifiedSince;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output extends ObjectOutput implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The bucket name")
        private String bucket;
        @Schema(title = "The object key")
        private String key;
    }

}
