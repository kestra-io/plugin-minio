package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.minio.DownloadObjectArgs;
import io.minio.MinioAsyncClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;

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
                "bucket: \"my-bucket\"",
                "key: \"path/to/file\""
            }
        ),
        @Example(
            title = "Download file from an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
              id: s3_compatible_download
              namespace: company.team
              tasks:
                - id: "download_from_storage"
                  type: "io.kestra.plugin.minio.Download"
                  accessKeyId: "<access-key>"
                  secretKeyId: "<secret-key>"
                  endpoint: https://<region>.digitaloceanspaces.com
                  bucket: "kestra-test-bucket"
                  key: "data/orders.csv"
              """
        )
    }
)
@Schema(
    title = "Download a file from a bucket."
)
public class Download extends AbstractMinioObject implements RunnableTask<Download.Output> {

    @Schema(
        title = "The key of a file to download."
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "The specific version of the object."
    )
    @PluginProperty(dynamic = true)
    protected String versionId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);
        String key = runContext.render(this.key);

        try (MinioAsyncClient client = this.asyncClient(runContext)) {
            Pair<URI, Long> output = MinioService.download(runContext, client, bucket, key, this.versionId);

            long length = output.getRight();
            URI uri = output.getLeft();

            runContext.metric(Counter.of("file.size", length));

            return Output
                .builder()
                .uri(uri)
                .contentLength(length)
                .build();
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final URI uri;

        @Schema(
            title = "The size of the body in bytes."
        )
        private final Long contentLength;
    }

}
