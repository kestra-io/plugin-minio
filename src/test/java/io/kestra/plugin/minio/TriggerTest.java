package io.kestra.plugin.minio;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.minio.model.MinioObject;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
public class TriggerTest extends AbstractMinIoTest {

    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void deleteAction() throws Exception {
        String bucket = "trigger-test";
        this.createBucket(bucket);
        List listTask = list().bucket(Property.ofValue(bucket)).build();

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals("listen")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        upload("trigger/", bucket);
        upload("trigger/", bucket);

        Path flowPath = Files.createTempFile("kestra-minio-listen-", ".yaml");
        String flow;
        try (
            var flowStream = Objects.requireNonNull(
                TriggerTest.class.getClassLoader()
                    .getResourceAsStream("flows/listen.yaml")
            )
        ) {
            flow = new String(flowStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        flow = flow.replace("http://localhost:9000", minIOContainer.getS3URL());
        Files.writeString(flowPath, flow, StandardCharsets.UTF_8);

        repositoryLoader.load(flowPath.toUri().toURL());

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        @SuppressWarnings("unchecked")
        java.util.List<MinioObject> trigger = (java.util.List<MinioObject>) last.get().getTrigger().getVariables().get("objects");

        assertThat(trigger.size(), is(2));

        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getObjects()
            .size();
        assertThat(remainingFilesOnBucket, is(0));
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        String bucket = "trigger-on-create";
        this.createBucket(bucket);

        Trigger trigger = Trigger.builder()
            .id("minio-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .region(Property.ofValue(minIOContainer.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/on-create"))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .action(Property.ofValue(Downloads.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        var key = upload("trigger/on-create", bucket);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue().context());

        assertThat(createExecution.isPresent(), is(true));

        Optional<Execution> updateExecution = trigger.evaluate(context.getKey(), context.getValue().context());

        assertThat(updateExecution.isPresent(), is(false));
    }

    @Test
    void shouldExecuteOnUpdate() throws Exception {
        String bucket = "trigger-on-update";
        this.createBucket(bucket);

        var key = upload("trigger/on-update", bucket);

        Trigger trigger = Trigger.builder()
            .id("minio-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .region(Property.ofValue(minIOContainer.getRegion()))
            .bucket(Property.ofValue(bucket))
            .prefix(Property.ofValue("trigger/on-update"))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .action(Property.ofValue(Downloads.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue().context());

        update(key, bucket);
        Thread.sleep(2000);

        Optional<Execution> updateExecution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(updateExecution.isPresent(), is(true));
    }
}
