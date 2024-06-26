package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
            code = {
                "accessKeyId: \"<access-key>\"",
                "secretKeyId: \"<secret-key>\"",
                "region: \"eu-central-1\"",
                "bucket: \"my-bucket\"",
                "prefix: \"sub-dir\""
            }
        )
    }
)
@Schema(
    title = "Delete a list of keys on a bucket."
)
public class DeleteList extends AbstractMinioObject implements RunnableTask<DeleteList.Output> {

    @Schema(
        title = "Limits the response to keys that begin with the specified prefix."
    )
    @PluginProperty(dynamic = true)
    private String prefix;

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
    protected final List.Filter filter = List.Filter.BOTH;

    @Min(2)
    @Schema(
        title = "Number of concurrent parallels deletion"
    )
    @PluginProperty
    private Integer concurrent;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @PluginProperty
    @Builder.Default
    private final Boolean errorOnEmpty = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String bucket = runContext.render(this.bucket);

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

            if (errorOnEmpty && finalResult.getLeft() == 0) {
                throw new NoSuchElementException(
                    "Unable to find any files to delete on " +
                        runContext.render(this.bucket) + " " +
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
