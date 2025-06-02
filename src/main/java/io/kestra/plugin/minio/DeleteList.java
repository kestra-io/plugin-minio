package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.minio.model.MinioObject;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.util.NoSuchElementException;
import java.util.function.Function;

import static io.kestra.core.utils.Rethrow.throwConsumer;
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
                id: minio_delete_objects
                namespace: company.team

                tasks:
                  - id: delete_objects
                    type: io.kestra.plugin.minio.DeleteList
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                """
        ),
        @Example(
            title = "Delete files from an S3-compatible storage — here, Spaces Object Storage from Digital Ocean.",
            full = true,
            code = """
              id: s3_compatible_delete_objects
              namespace: company.team

              tasks:
                - id: delete_objects
                  type: io.kestra.plugin.minio.DeleteList
                  accessKeyId: "<access-key>"
                  secretKeyId: "<secret-key>"
                  endpoint: https://<region>.digitaloceanspaces.com
                  bucket: "kestra-test-bucket"
                  prefix: "sub-dir"
              """
        )
    }
)
@Schema(
    title = "Delete a list of keys on a MinIO bucket."
)
public class DeleteList extends AbstractMinioObject implements RunnableTask<DeleteList.Output> {

    @Schema(
        title = "Limits the response to keys that begin with the specified prefix."
    )
    private Property<String> prefix;

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
    private Property<Integer> maxKeys = Property.ofValue(1000);

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
    protected final Property<List.Filter> filter = Property.ofValue(List.Filter.BOTH);

    @Min(2)
    @Schema(
        title = "Number of concurrent parallels deletion"
    )
    @PluginProperty
    private Integer concurrent;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @Builder.Default
    private final Property<Boolean> errorOnEmpty = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String bucket = runContext.render(this.bucket).as(String.class).orElse(null);

        try (MinioClient client = this.client(runContext)) {

            Flux<MinioObject> flowable = Flux.create(
                throwConsumer(emitter -> {
                        list(runContext).forEach(emitter::next);

                        emitter.complete();
                    }
                ), FluxSink.OverflowStrategy.BUFFER);

            Flux<Long> result;

            if (this.concurrent != null) {
                result = flowable
                    .parallel(this.concurrent)
                    .runOn(Schedulers.boundedElastic())
                    .map(throwFunction(o -> delete(logger, client, bucket).apply(o)))
                    .sequential();
            } else {
                result = flowable.map(throwFunction(o -> delete(logger, client, bucket).apply(o)));
            }

            org.apache.commons.lang3.tuple.Pair<Long, Long> finalResult = result
                .reduce(
                    org.apache.commons.lang3.tuple.Pair.of(0L, 0L),
                    (pair, size) -> org.apache.commons.lang3.tuple.Pair.of(pair.getLeft() + 1, pair.getRight() + size)
                )
                .block();

            runContext.metric(Counter.of("count", finalResult.getLeft()));
            runContext.metric(Counter.of("size", finalResult.getRight()));

            if (runContext.render(errorOnEmpty).as(Boolean.class).orElseThrow() && finalResult.getLeft() == 0) {
                throw new NoSuchElementException(
                    "Unable to find any files to delete on " +
                        runContext.render(this.bucket).as(String.class).orElse(null) + " " +
                        "with regexp='" + runContext.render(this.regexp) + "', " +
                        "prefix='" + runContext.render(this.prefix) + "'"
                );
            }

            logger.info("Deleted {} keys for {} bytes", finalResult.getLeft(), finalResult.getValue());

            return Output
                .builder()
                .count(finalResult.getLeft())
                .size(finalResult.getRight())
                .build();
        }
    }

    private java.util.List<MinioObject> list(RunContext runContext) throws Exception {
        io.kestra.plugin.minio.List task = io.kestra.plugin.minio.List
            .builder()
            .id(this.id)
            .type(io.kestra.plugin.minio.List.class.getName())
            .region(this.region)
            .endpoint(this.endpoint)
            .accessKeyId(this.accessKeyId)
            .secretKeyId(this.secretKeyId)
            .bucket(this.bucket)
            .prefix(this.prefix)
            .delimiter(this.delimiter)
            .marker(this.marker)
            .maxKeys(this.maxKeys)
            .regexp(this.regexp)
            .filter(this.filter)
            .build();

        return task.run(runContext).getObjects();
    }

    public static Function<MinioObject, Long> delete(Logger logger, MinioClient client, String bucket) throws Exception {
        return throwFunction(object -> {
            logger.debug("Deleting '{}'", object.getKey());

            RemoveObjectArgs.Builder request = RemoveObjectArgs
                .builder()
                .bucket(bucket)
                .object(object.getKey());

            client.removeObject(request.build());

            return object.getSize();
        });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Builder.Default
        @Schema(
            title = "The count of blobs deleted"
        )
        private final long count = 0;

        @Builder.Default
        @Schema(
            title = "The size of all blobs deleted"
        )
        private final long size = 0;
    }

}
