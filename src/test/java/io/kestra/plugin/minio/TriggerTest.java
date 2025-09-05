package io.kestra.plugin.minio;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.minio.model.MinioObject;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TriggerTest extends AbstractMinIoTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void deleteAction() throws Exception {
        String bucket = "trigger-test";
        this.createBucket(bucket);
        List listTask = list().bucket(Property.ofValue(bucket)).build();

        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            )
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();

                if (execution.getFlowId().equals("listen")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });

            upload("trigger/", bucket);
            upload("trigger/", bucket);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/listen.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
            }

            @SuppressWarnings("unchecked")
            java.util.List<MinioObject> trigger = (java.util.List<MinioObject>) last.get().getTrigger().getVariables().get("objects");

            assertThat(trigger.size(), is(2));

            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getObjects()
                .size();
            assertThat(remainingFilesOnBucket, is(0));
        }
    }

}
