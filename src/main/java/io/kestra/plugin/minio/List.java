package io.kestra.plugin.minio;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.minio.model.MinioObject;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Spliterator;
import java.util.stream.StreamSupport;

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
                id: minio_list
                namespace: company.team
                
                tasks:
                  - id: list_objects
                    type: io.kestra.plugin.minio.List
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                """
        ),
        @Example(
            title = "List files from an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
              id: s3_compatible_list
              namespace: company.team

              tasks:
                - id: list_objects
                  type: io.kestra.plugin.minio.List
                  accessKeyId: "<access-key>"
                  secretKeyId: "<secret-key>"
                  endpoint: https://<region>.digitaloceanspaces.com
                  bucket: "kestra-test-bucket"
              """
        )
    }
)
@Schema(
    title = "List keys on a bucket."
)
public class List extends AbstractMinioObject implements RunnableTask<List.Output> {

    public enum Filter {
        FILES,
        DIRECTORY,
        BOTH
    }

    @Schema(
        title = "Limits the response to keys that begin with the specified prefix."
    )
    @PluginProperty(dynamic = true)
    private String prefix;

    @Schema(
        title = "Limits the response to keys that ends with the specified string."
    )
    @PluginProperty(dynamic = true)
    private String startAfter;

    @Schema(
        title = "A delimiter is a character you use to group keys."
    )
    @PluginProperty(dynamic = true)
    private String delimiter;

    @Schema(
        title = "Marker is where you want to start listing from.",
        description = "Start listing after this specified key. Marker can be any key in the bucket."
    )
    @PluginProperty(dynamic = true)
    private String marker;

    @Schema(
        title = "Sets the maximum number of keys returned in the response.",
        description = "By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Integer maxKeys = 1000;

    @Schema(
        title = "A regexp to filter on full key.",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
    )
    @PluginProperty(dynamic = true)
    protected String regexp;

    @Schema(
        title = "The type of objects to filter: files, directory, or both."
    )
    @PluginProperty
    @Builder.Default
    protected final Filter filter = Filter.BOTH;

    @Schema(
        title = "Indicates whether it should look into subfolders."
    )
    @PluginProperty
    @Builder.Default
    public Boolean recursive = true;

    @Schema(
        title = "Indicates whether task should include versions in output."
    )
    @PluginProperty
    @Builder.Default
    public Boolean includeVersions = true;

    /* Uncomment for List Objects (client.listObjects) version 2 only
    @Schema(
        title = "Indicates whether task should include user metadata in output."
    )
    @PluginProperty
    @Builder.Default
    public Boolean includeUserMetadata = true;

    @Schema(
        title = "Indicates whether task should include owner data in output."
    )
    @PluginProperty
    @Builder.Default
    public Boolean fetchOwner = true;
    */

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket);

        try (MinioClient client = this.client(runContext)) {
            ListObjectsArgs.Builder requestBuilder = ListObjectsArgs
                .builder()
                .bucket(bucket)
                .recursive(recursive)
                .maxKeys(this.maxKeys);

            if (this.prefix != null) {
                requestBuilder.prefix(runContext.render(this.prefix));
            }

            if (this.startAfter != null) {
                requestBuilder.startAfter(runContext.render(this.startAfter));
            }

            if (this.delimiter != null) {
                requestBuilder.delimiter(runContext.render(this.delimiter));
            }

            if (this.marker != null) {
                requestBuilder.marker(runContext.render(this.marker));
            }

            if (this.includeVersions != null) {
                requestBuilder.includeVersions(this.includeVersions);
            }

            String regExp = runContext.render(this.regexp);

            Iterable<Result<Item>> response = client.listObjects(requestBuilder.build());

            Spliterator<Result<Item>> spliterator = response.spliterator();

            runContext.metric(Counter.of("size", spliterator.getExactSizeIfKnown()));

            runContext.logger().debug(
                "Found '{}' keys on {} with regexp='{}', prefix={}",
                spliterator.getExactSizeIfKnown(),
                bucket,
                regExp,
                runContext.render(this.prefix)
            );

            java.util.List<MinioObject> minioObjects = StreamSupport.stream(spliterator, false)
                .map(throwFunction(Result::get))
                .filter(item -> filter(item, regExp))
                .map(MinioObject::of)
                .toList();

            return Output
                .builder()
                .objects(minioObjects)
                .build();
        }
    }

    private boolean filter(Item object, String regExp) {
        return
            (regExp == null || object.objectName().matches(regExp)) &&
                (filter.equals(Filter.BOTH) ||
                    (filter.equals(Filter.DIRECTORY) && object.objectName().endsWith("/")) ||
                    (filter.equals(Filter.FILES) && !object.objectName().endsWith("/"))
                );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @JsonInclude
        @Schema(
            title = "The list of objects."
        )
        private final java.util.List<MinioObject> objects;
    }

}
