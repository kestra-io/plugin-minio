package io.kestra.plugin.minio;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
    title = "List objects on a MinIO bucket."
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
    private Property<String> prefix;

    @Schema(
        title = "Limits the response to keys that ends with the specified string."
    )
    private Property<String> startAfter;

    @Schema(
        title = "A delimiter is a character you use to group keys."
    )
    private Property<String> delimiter;

    @Schema(
        title = "Marker is where you want to start listing from.",
        description = "Start listing after this specified key. Marker can be any key in the bucket."
    )
    private Property<String> marker;

    @Schema(
        title = "Sets the maximum number of keys returned in the response.",
        description = "By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more."
    )
    @Builder.Default
    private Property<Integer> maxKeys = Property.of(1000);

    @Schema(
        title = "A regexp to filter on full key.",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
    )
    protected Property<String> regexp;

    @Schema(
        title = "The type of objects to filter: files, directory, or both."
    )
    @Builder.Default
    protected final Property<Filter> filter = Property.of(Filter.BOTH);

    @Schema(
        title = "Indicates whether it should look into subfolders."
    )
    @Builder.Default
    public Property<Boolean> recursive = Property.of(true);

    @Schema(
        title = "Indicates whether task should include versions in output."
    )
    @Builder.Default
    public Property<Boolean> includeVersions = Property.of(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String bucket = runContext.render(this.bucket).as(String.class).orElse(null);

        try (MinioClient client = this.client(runContext)) {
            ListObjectsArgs.Builder requestBuilder = ListObjectsArgs
                .builder()
                .bucket(bucket)
                .recursive(runContext.render(recursive).as(Boolean.class).orElseThrow())
                .maxKeys(runContext.render(this.maxKeys).as(Integer.class).orElseThrow());


            runContext.render(this.prefix).as(String.class).ifPresent(requestBuilder::prefix);
            runContext.render(this.startAfter).as(String.class).ifPresent(requestBuilder::startAfter);
            runContext.render(this.delimiter).as(String.class).ifPresent(requestBuilder::delimiter);
            runContext.render(this.marker).as(String.class).ifPresent(requestBuilder::marker);
            runContext.render(this.includeVersions).as(Boolean.class).ifPresent(requestBuilder::includeVersions);

            String regExp = runContext.render(this.regexp).as(String.class).orElse(null);

            Iterable<Result<Item>> response = client.listObjects(requestBuilder.build());

            Spliterator<Result<Item>> spliterator = response.spliterator();

            runContext.metric(Counter.of("size", spliterator.getExactSizeIfKnown()));

            runContext.logger().debug(
                "Found '{}' keys on {} with regexp='{}', prefix={}",
                spliterator.getExactSizeIfKnown(),
                bucket,
                regExp,
                runContext.render(this.prefix).as(String.class).orElse(null)
            );

            var filterValue = runContext.render(this.filter).as(Filter.class).orElseThrow();
            java.util.List<MinioObject> minioObjects = StreamSupport.stream(spliterator, false)
                .map(throwFunction(Result::get))
                .filter(item -> filter(item, regExp, filterValue))
                .map(MinioObject::of)
                .toList();

            return Output
                .builder()
                .objects(minioObjects)
                .build();
        }
    }

    private boolean filter(Item object, String regExp, Filter filter) {
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
