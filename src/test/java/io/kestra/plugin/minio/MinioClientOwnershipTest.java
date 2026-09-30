package io.kestra.plugin.minio;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Guards the HTTP client ownership rules that {@link AbstractMinio#cancellableClient} relies on.
 *
 * <p>
 * Building the HTTP client in the plugin rather than letting the SDK do it is what makes cancellation
 * possible, but it also makes the plugin responsible for tearing that client down. These tests pin that every
 * HTTP client the plugin builds, whether defaulted in or derived from TLS configuration, is closed with the
 * MinIO client.
 */
@KestraTest
class MinioClientOwnershipTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldCloseTheHttpClientItDefaultedIn() throws Exception {
        List task = task().build();

        var cancellableClient = task.cancellableClient(runContext(task));
        assertThat(cancellableClient.httpClient().dispatcher().executorService().isShutdown(), is(false));

        cancellableClient.close();

        // No custom client was configured, so the SDK would have created and owned one: closing must still
        // release it, exactly as before this class started supplying it.
        assertThat(cancellableClient.httpClient().dispatcher().executorService().isShutdown(), is(true));
    }

    @Test
    void shouldCloseTheHttpClientItBuiltFromTlsConfiguration() throws Exception {
        List task = task()
            .ssl(SslOptions.builder().insecureTrustAllCertificates(Property.ofValue(true)).build())
            .build();

        var cancellableClient = task.cancellableClient(runContext(task));
        assertThat(cancellableClient.httpClient().dispatcher().executorService().isShutdown(), is(false));

        cancellableClient.close();

        // The TLS client is built by the plugin for this client alone, so nothing else can release it: leaving it
        // open would leak its dispatcher threads and pooled connections on every evaluation.
        assertThat(cancellableClient.httpClient().dispatcher().executorService().isShutdown(), is(true));
    }

    @Test
    void shouldGiveEachClientItsOwnDispatcher() throws Exception {
        List task = task().build();

        var first = task.cancellableClient(runContext(task));
        var second = task.cancellableClient(runContext(task));

        try {
            // Cancellation works through the dispatcher, so sharing one between two clients would let a kill on
            // one operation abort another's requests.
            assertThat(first.httpClient().dispatcher(), not(sameInstance(second.httpClient().dispatcher())));
        } finally {
            first.close();
            second.close();
        }
    }

    private List.ListBuilder<?, ?> task() {
        return List.builder()
            .id("minio-ownership-" + IdUtils.create())
            .type(List.class.getName())
            .endpoint(Property.ofValue("http://127.0.0.1:9000"))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-access-key"))
            .secretKeyId(Property.ofValue("test-secret-key"))
            .bucket(Property.ofValue("ownership-test"));
    }

    private RunContext runContext(List task) {
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }
}
