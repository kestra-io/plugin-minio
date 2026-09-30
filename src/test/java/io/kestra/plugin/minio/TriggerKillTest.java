package io.kestra.plugin.minio;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cancellation coverage for {@link Trigger#kill()}.
 *
 * <p>
 * These tests drive a local HTTP endpoint rather than a real object store because the behaviour under test only
 * appears when a request never gets answered, which a healthy server cannot reproduce. Hand-off is by latch on
 * the server side, never by sleeping, and every wait is bounded well below the MinIO SDK's 5 minute read
 * timeout so that a pass cannot be explained by that timeout expiring instead of by cancellation.
 */
@KestraTest
class TriggerKillTest {

    private static final String BUCKET = "trigger-kill-test";

    /**
     * Upper bound on how long a killed evaluation may take to unwind. Comfortably under the SDK's 5 minute
     * default read timeout: if cancellation regresses, these waits expire first and the test fails rather than
     * passing slowly for the wrong reason.
     */
    private static final Duration UNBLOCK_TIMEOUT = Duration.ofSeconds(30);

    /** How long a request is observed for to conclude it is still blocked. */
    private static final Duration STILL_BLOCKED_WINDOW = Duration.ofSeconds(2);

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldReleaseBlockedEvaluationWhenKilled() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.stalling()) {
            Trigger trigger = trigger(endpoint);

            try (BlockedEvaluation evaluation = startAndAwaitRequest(trigger, endpoint)) {
                trigger.kill();

                Throwable cause = evaluation.awaitFailure();

                assertThat(
                    "evaluation failed with " + cause + ", which is not the I/O abort a cancelled socket produces",
                    hasCauseOfType(cause, IOException.class),
                    is(true)
                );
                assertThat("the endpoint answered the request, so the test proved nothing", endpoint.respondedCount(), is(0));
            }
        }
    }

    @Test
    void shouldBeIdempotentWhenKilledRepeatedly() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.stalling()) {
            Trigger trigger = trigger(endpoint);

            try (BlockedEvaluation evaluation = startAndAwaitRequest(trigger, endpoint)) {
                assertDoesNotThrow(
                    () ->
                    {
                        trigger.kill();
                        trigger.kill();
                        trigger.stop();
                    },
                    "repeated kill()/stop() on a live evaluation must not throw"
                );

                evaluation.awaitFailure();

                assertDoesNotThrow(trigger::kill, "kill() after the operation already finished must stay harmless");
            }
        }
    }

    @Test
    void shouldNotCancelAnotherEvaluationsRequest() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.stalling()) {
            Trigger killed = trigger(endpoint);
            Trigger untouched = trigger(endpoint);

            try (
                BlockedEvaluation killedEvaluation = startAndAwaitRequest(killed, endpoint);
                BlockedEvaluation untouchedEvaluation = startAndAwaitRequest(untouched, endpoint)
            ) {
                killed.kill();

                killedEvaluation.awaitFailure();

                // Cancellation works by cancelling every call on a dispatcher, so it is only safe while each
                // evaluation owns its own HTTP client. If that ever regresses, this is the test that catches it.
                assertThat(
                    "killing one trigger also cancelled an unrelated evaluation's in-flight request",
                    untouchedEvaluation.isStillBlocked(STILL_BLOCKED_WINDOW),
                    is(true)
                );
            }
        }
    }

    @Test
    void shouldNotIssueRequestWhenKilledBeforeEvaluationStarts() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.stalling()) {
            Trigger trigger = trigger(endpoint);
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            trigger.kill();

            Optional<Execution> evaluation = trigger.evaluate(context.getKey(), context.getValue());

            assertThat("a killed trigger must not produce an execution", evaluation.isPresent(), is(false));
            assertThat(
                "the cycle reached the network despite being killed first, so it would have blocked on the stalling endpoint",
                endpoint.receivedCount(),
                is(0)
            );
        }
    }

    @Test
    void shouldAbortListTaskKilledBeforeItsClientIsPublished() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.stalling()) {
            List listTask = listTask(endpoint);

            // The kill lands before run() publishes its HTTP client, so there is nothing to cancel yet. Without
            // the sticky flag the signal would be dropped and the request would then block for the full timeout.
            listTask.kill();

            assertThrows(
                InterruptedIOException.class,
                () -> listTask.run(runContext(listTask)),
                "a List task killed before it published its client must refuse to issue the request"
            );
            assertThat("a killed List task issued its request anyway", endpoint.receivedCount(), is(0));
        }
    }

    @Test
    void shouldKeepPollingNormallyAcrossEvaluations() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.responding()) {
            Trigger trigger = trigger(endpoint);
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Two back-to-back cycles, each building its own client: proves the cancellation plumbing leaves the
            // normal path intact and carries no state between evaluations.
            assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
            assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

            assertThat("both polls should have completed against the endpoint", endpoint.respondedCount(), is(2));
        }
    }

    @Test
    void shouldNotPoisonLaterTriggersAfterAKill() throws Exception {
        try (
            StubS3Endpoint stalling = StubS3Endpoint.stalling();
            StubS3Endpoint healthy = StubS3Endpoint.responding()
        ) {
            Trigger killed = trigger(stalling);

            try (BlockedEvaluation evaluation = startAndAwaitRequest(killed, stalling)) {
                killed.kill();
                evaluation.awaitFailure();
            }

            // The worker deserializes a fresh trigger per evaluation, so a kill must not reach beyond the
            // instance it was delivered to.
            Trigger later = trigger(healthy);
            var laterContext = TestsUtils.mockTrigger(runContextFactory, later);

            assertThat(later.evaluate(laterContext.getKey(), laterContext.getValue()).isPresent(), is(false));
            assertThat("a later trigger failed to poll after an unrelated trigger was killed", healthy.respondedCount(), is(1));
        }
    }

    @Test
    void shouldSkipRemainingDownloadsWhenKilledDuringADownload() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.serving("a.txt", "b.txt")) {
            Trigger trigger = trigger(endpoint, Downloads.Action.DELETE);
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Downloads use their own client, so this kill only raises the flag: the first download completes
            // normally and the kill must be honoured before the next object is fetched.
            endpoint.onObjectRead(trigger::kill);

            assertThat(
                "a trigger killed mid-cycle must not produce an execution",
                trigger.evaluate(context.getKey(), context.getValue()).isPresent(),
                is(false)
            );
            assertThat(
                "the second object was fetched although the trigger had already been killed",
                endpoint.objectRequests().stream().noneMatch(request -> request.endsWith(" b.txt")),
                is(true)
            );
            assertThat(
                "the DELETE action ran for a killed evaluation",
                endpoint.objectRequests().stream().noneMatch(request -> request.startsWith("DELETE ")),
                is(true)
            );
        }
    }

    @Test
    void shouldNotRecordStateWhenKilledBeforeStateIsWritten() throws Exception {
        try (StubS3Endpoint endpoint = StubS3Endpoint.serving("a.txt")) {
            String stateKey = "minio-kill-state-" + IdUtils.create();
            Trigger killed = trigger(endpoint, Downloads.Action.DELETE, stateKey);
            // One context for every evaluation, so all three read and write the same state store.
            var context = TestsUtils.mockTrigger(runContextFactory, killed);

            // The only download completes, then the kill must stop the cycle before it writes state.
            endpoint.onObjectRead(killed::kill);

            assertThat(killed.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
            assertThat(
                "the DELETE action ran for a killed evaluation",
                endpoint.objectRequests().stream().noneMatch(request -> request.startsWith("DELETE ")),
                is(true)
            );

            endpoint.onObjectRead(() ->
            {
            });

            // Had the killed cycle written state, the object would already count as seen and not fire again.
            assertThat(
                "the killed evaluation recorded the object as seen, so it will never fire",
                trigger(endpoint, Downloads.Action.DELETE, stateKey).evaluate(context.getKey(), context.getValue()).isPresent(),
                is(true)
            );
            // Control: a completed cycle does write state, which is what makes the assertion above meaningful.
            assertThat(
                trigger(endpoint, Downloads.Action.DELETE, stateKey).evaluate(context.getKey(), context.getValue()).isPresent(),
                is(false)
            );
        }
    }

    /**
     * Starts {@code evaluate()} on another thread and returns once the endpoint confirms the list request is
     * genuinely in flight, so the caller can kill a request that has actually started.
     */
    private BlockedEvaluation startAndAwaitRequest(Trigger trigger, StubS3Endpoint endpoint) {
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Optional<Execution>> future = executor.submit(
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );

        if (!endpoint.awaitRequest(UNBLOCK_TIMEOUT)) {
            executor.shutdownNow();
            throw new AssertionError(
                "the endpoint never received a list request within " + UNBLOCK_TIMEOUT
                    + ", so no request was in flight and this test could not have proved anything"
            );
        }

        return new BlockedEvaluation(executor, future);
    }

    /** An {@code evaluate()} call running on its own thread, blocked on a request the endpoint will not answer. */
    private record BlockedEvaluation(ExecutorService executor, Future<Optional<Execution>> future) implements AutoCloseable {

        /** Asserts the evaluation terminates by failing, and returns the cause for further inspection. */
        Throwable awaitFailure() {
            ExecutionException thrown = assertThrows(
                ExecutionException.class,
                () -> future.get(UNBLOCK_TIMEOUT.toSeconds(), TimeUnit.SECONDS),
                () -> "evaluate() did not unwind within " + UNBLOCK_TIMEOUT
                    + " of kill(); the in-flight request was not cancelled"
            );
            return thrown.getCause();
        }

        boolean isStillBlocked(Duration window) {
            try {
                future.get(window.toMillis(), TimeUnit.MILLISECONDS);
                return false;
            } catch (TimeoutException e) {
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                return false;
            }
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }

    private Trigger trigger(StubS3Endpoint endpoint) {
        return triggerBuilder(endpoint).build();
    }

    private Trigger trigger(StubS3Endpoint endpoint, Downloads.Action action) {
        return triggerBuilder(endpoint).action(Property.ofValue(action)).build();
    }

    private Trigger trigger(StubS3Endpoint endpoint, Downloads.Action action, String stateKey) {
        return triggerBuilder(endpoint).action(Property.ofValue(action)).stateKey(Property.ofValue(stateKey)).build();
    }

    private Trigger.TriggerBuilder<?, ?> triggerBuilder(StubS3Endpoint endpoint) {
        return Trigger.builder()
            .id("minio-kill-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(endpoint.url()))
            // An explicit region stops the SDK resolving the bucket location first, so exactly one request is in
            // flight and the request-count assertions stay meaningful.
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-access-key"))
            .secretKeyId(Property.ofValue("test-secret-key"))
            .bucket(Property.ofValue(BUCKET))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .action(Property.ofValue(Downloads.Action.NONE))
            .interval(Duration.ofSeconds(60));
    }

    private List listTask(StubS3Endpoint endpoint) {
        return List.builder()
            .id("minio-kill-list-" + IdUtils.create())
            .type(List.class.getName())
            .endpoint(Property.ofValue(endpoint.url()))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-access-key"))
            .secretKeyId(Property.ofValue("test-secret-key"))
            .bucket(Property.ofValue(BUCKET))
            .build();
    }

    private RunContext runContext(List task) {
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }

    private static boolean hasCauseOfType(Throwable throwable, Class<? extends Throwable> type) {
        for (Throwable current = throwable; current != null && current != current.getCause(); current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A local stand-in for an S3 endpoint that either holds every request open forever, or serves a listing of
     * the given keys along with their contents and deletion.
     */
    private static final class StubS3Endpoint implements AutoCloseable {

        private static final String LISTING = """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>%s</Name>
              <Prefix></Prefix>
              <MaxKeys>1000</MaxKeys>
              <IsTruncated>false</IsTruncated>
            %s</ListVersionsResult>
            """;

        private static final String VERSION = """
              <Version>
                <Key>%s</Key>
                <VersionId>null</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2026-01-01T00:00:00.000Z</LastModified>
                <ETag>"%s"</ETag>
                <Size>%d</Size>
                <StorageClass>STANDARD</StorageClass>
              </Version>
            """;

        private static final byte[] CONTENT = "content".getBytes(StandardCharsets.UTF_8);

        private final HttpServer server;
        private final ExecutorService executor;
        /** One token per request received, so a test can await the n-th arrival without polling or sleeping. */
        private final BlockingQueue<String> arrivals = new LinkedBlockingQueue<>();
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger received = new AtomicInteger();
        private final AtomicInteger responded = new AtomicInteger();
        /** {@code METHOD key} for every request made against an object rather than the bucket. */
        private final java.util.List<String> objectRequests = new CopyOnWriteArrayList<>();
        /** Invoked on the server thread whenever an object is read, before the response is sent. */
        private final AtomicReference<Runnable> onObjectRead = new AtomicReference<>(() ->
        {
        });

        private StubS3Endpoint(boolean stall, java.util.List<String> keys) throws IOException {
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            this.executor = Executors.newCachedThreadPool();
            this.server.setExecutor(executor);
            this.server.createContext("/", exchange ->
            {
                received.incrementAndGet();
                arrivals.add(exchange.getRequestURI().toString());

                if (stall) {
                    try {
                        // Withhold the response so the client stays blocked until it is cancelled. The bound is
                        // only a backstop against a wedged run; tests never wait this long.
                        release.await(2, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.close();
                    return;
                }

                String objectPrefix = "/" + BUCKET + "/";
                String path = exchange.getRequestURI().getPath();
                if (path.startsWith(objectPrefix) && path.length() > objectPrefix.length()) {
                    String key = path.substring(objectPrefix.length());
                    String method = exchange.getRequestMethod();
                    objectRequests.add(method + " " + key);

                    if (method.equals("DELETE")) {
                        exchange.sendResponseHeaders(204, -1);
                        exchange.close();
                        return;
                    }

                    onObjectRead.get().run();

                    exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().set("ETag", "\"" + key + "\"");
                    exchange.getResponseHeaders().set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT");
                    if (method.equals("HEAD")) {
                        exchange.getResponseHeaders().set("Content-Length", String.valueOf(CONTENT.length));
                        exchange.sendResponseHeaders(200, -1);
                        exchange.close();
                        return;
                    }
                    exchange.sendResponseHeaders(200, CONTENT.length);
                    try (var out = exchange.getResponseBody()) {
                        out.write(CONTENT);
                    }
                    return;
                }

                String versions = keys.stream()
                    .map(key -> VERSION.formatted(key, key, CONTENT.length))
                    .collect(Collectors.joining());
                byte[] body = LISTING.formatted(BUCKET, versions).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                exchange.sendResponseHeaders(200, body.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(body);
                }
                responded.incrementAndGet();
            });
            this.server.start();
        }

        static StubS3Endpoint stalling() throws IOException {
            return new StubS3Endpoint(true, java.util.List.of());
        }

        static StubS3Endpoint responding() throws IOException {
            return new StubS3Endpoint(false, java.util.List.of());
        }

        static StubS3Endpoint serving(String... keys) throws IOException {
            return new StubS3Endpoint(false, java.util.List.of(keys));
        }

        void onObjectRead(Runnable hook) {
            onObjectRead.set(hook);
        }

        java.util.List<String> objectRequests() {
            return java.util.List.copyOf(objectRequests);
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        /** Waits for one request to arrive that no earlier call has already consumed. */
        boolean awaitRequest(Duration timeout) {
            try {
                return arrivals.poll(timeout.toMillis(), TimeUnit.MILLISECONDS) != null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        int receivedCount() {
            return received.get();
        }

        int respondedCount() {
            return responded.get();
        }

        @Override
        public void close() {
            release.countDown();
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
