package io.kestra.plugin.minio;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.minio.model.MinioObject;
import io.minio.MinioAsyncClient;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    title = "Downloads multiple files from a bucket."
)
public class Downloads extends AbstractMinioObject implements RunnableTask<Downloads.Output> {

    public enum Action {
        MOVE,
        DELETE,
        NONE
    }

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
    protected final io.kestra.plugin.minio.List.Filter filter = io.kestra.plugin.minio.List.Filter.BOTH;

    @Schema(
        title = "The action to perform on the retrieved files. If using 'NONE' make sure to handle the files inside your flow to avoid infinite triggering."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Action action;

    @Schema(
        title = "The destination bucket and key for `MOVE` action."
    )
    @PluginProperty(dynamic = true)
    private Copy.CopyObject moveTo;

    @Override
    public Output run(RunContext runContext) throws Exception {
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

        io.kestra.plugin.minio.List.Output run = task.run(runContext);

        try (MinioAsyncClient client = this.asyncClient(runContext)) {
            String bucket = runContext.render(this.bucket);

            List<MinioObject> list = run
                .getObjects()
                .stream()
                .map(throwFunction(object -> {
                            URI fileUri = MinioService.download(
                                    runContext,
                                    client,
                                    bucket,
                                    object.getKey(),
                                    null
                                )
                                .getLeft();

                            return object.withUri(fileUri);
                        }
                    )
                )
                .filter(object -> !object.getKey().endsWith("/"))
                .collect(Collectors.toList());

            Map<String, URI> outputFiles = list
                .stream()
                .map(object -> new AbstractMap.SimpleEntry<>(object.getKey(), object.getUri()))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            MinioService.performAction(runContext, list, action,bucket, this, moveTo);

            return Output
                .builder()
                .objects(list)
                .outputFiles(outputFiles)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @JsonInclude
        @Schema(
            title = "The list of objects."
        )
        private final java.util.List<MinioObject> objects;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }

}
