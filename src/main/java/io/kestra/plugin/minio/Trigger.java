package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.minio.model.MinioObject;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on a bucket.",
    description = "This trigger will list every `interval` a bucket. " +
        "You can search for all files in a bucket or directory in `from` or you can filter the files with a `regExp`. " +
        "The detection is atomic, internally we do a list and interact only with files listed.\n" +
        "Once a file is detected, we download the file on internal storage and processed with declared `action` " +
        "in order to move or delete the files from the bucket (to avoid double detection on new poll)."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of files on a bucket and iterate through the files.",
            full = true,
            code = """
                id: minio_listen
                namespace: company.team
                
                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.objects | jq('.[].uri') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"
                
                triggers:
                  - id: watch
                    type: io.kestra.plugin.minio.Trigger
                    interval: "PT5M"
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                    action: MOVE
                    moveTo: 
                      key: archive"
                """
        ),
        @Example(
            title = "Wait for a list of files on a bucket and iterate through the files. Delete files manually after processing to prevent infinite triggering.",
            full = true,
            code = """
                id: minio_listen
                namespace: company.team
                
                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.objects | jq('.[].key') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"
                      - id: delete
                        type: io.kestra.plugin.minio.Delete
                        accessKeyId: "<access-key>"
                        secretKeyId: "<secret-key>"
                        region: "eu-central-1"
                        bucket: "my-bucket"
                        key: "{{ taskrun.value }}"
                
                triggers:
                  - id: watch
                    type: io.kestra.plugin.minio.Trigger
                    interval: "PT5M"
                    accessKeyId: "<access-key>"
                    secretKeyId: "<secret-key>"
                    region: "eu-central-1"
                    bucket: "my-bucket"
                    prefix: "sub-dir"
                    action: NONE
                """
        ),
        @Example(
            title = "Wait for a list of files on a bucket on an S3-compatible storage — here, Spaces Object Storage from Digital Ocean. Iterate through those files, and move it to another folder.",
            full = true,
            code = """
              id: trigger_on_s3_compatible_storage
              namespace: company.team
              tasks:
                - id: each
                  type: io.kestra.plugin.core.flow.ForEach
                  values: "{{ trigger.objects | jq('.[].uri') }}"
                  tasks:
                    - id: return
                      type: io.kestra.plugin.core.debug.Return
                      format: "{{ taskrun.value }}"
              
              triggers:
                - id: watch
                  type: io.kestra.plugin.minio.Trigger
                  interval: "PT5M"
                  accessKeyId: "<access-key>"
                  secretKeyId: "<secret-key>"
                  endpoint: https://<region>>.digitaloceanspaces.com
                  bucket: "kestra-test-bucket"
                  prefix: "sub-dir"
                  action: MOVE
                  moveTo: 
                    key: archive
              """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, MinioConnectionInterface {

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String accessKeyId;

    protected String secretKeyId;

    protected String region;

    protected String endpoint;

    protected String bucket;

    private String prefix;

    private String delimiter;

    private String marker;

    @Builder.Default
    private Integer maxKeys = 1000;

    protected String regexp;

    @Builder.Default
    protected final List.Filter filter = List.Filter.BOTH;

    private Downloads.Action action;

    private Copy.CopyObject moveTo;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
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
        List.Output run = task.run(runContext);

        if (run.getObjects().isEmpty()) {
            return Optional.empty();
        }

        java.util.List<MinioObject> list = run
            .getObjects()
            .stream()
            .map(throwFunction(getMinioObject(runContext)))
            .collect(Collectors.toList());

        MinioService.performAction(
            runContext,
            run.getObjects(),
            this.action,
            this.bucket,
            this,
            this.moveTo
        );

        List.Output output = List.Output
            .builder()
            .objects(list)
            .build();

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }

    private Rethrow.FunctionChecked<MinioObject, MinioObject, Exception> getMinioObject(RunContext runContext) {
        return object -> {
            Download download = Download.builder()
                .id(this.id)
                .type(List.class.getName())
                .region(this.region)
                .endpoint(this.endpoint)
                .accessKeyId(this.accessKeyId)
                .secretKeyId(this.secretKeyId)
                .bucket(this.bucket)
                .key(object.getKey())
                .build();
            Download.Output downloadOutput = download.run(runContext);

            return object.withUri(downloadOutput.getUri());
        };
    }

}
