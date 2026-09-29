package io.kestra.plugin.minio;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;

import javax.net.ssl.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.ssl.SSLContexts;

import io.kestra.core.runners.RunContext;

import io.minio.Http;
import io.minio.MinioAsyncClient;
import io.minio.MinioClient;
import okhttp3.OkHttpClient;

public interface AbstractMinio extends MinioConnectionInterface {

    /**
     * A {@link MinioClient} paired with the {@link OkHttpClient} that executes its requests.
     *
     * <p>
     * MinIO never retains the {@code okhttp3.Call} it enqueues, so this HTTP client's dispatcher is the
     * only supported handle for aborting a request that is already in flight. Tasks that must stay
     * interruptible hold this pair for the duration of the operation and call
     * {@link #cancelInFlightRequests()} from their {@code kill()} method.
     */
    record CancellableClient(MinioClient client, OkHttpClient httpClient) implements AutoCloseable {

        /**
         * Aborts every request currently queued or running on this client.
         *
         * <p>
         * Safe to call at any point: it is a no-op when nothing is in flight, and since each client is
         * built with its own dispatcher it can never abort another operation's requests.
         */
        public void cancelInFlightRequests() {
            httpClient.dispatcher().cancelAll();
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }

    default MinioClient client(final RunContext runContext) throws Exception {
        return cancellableClient(runContext).client();
    }

    /**
     * Builds a client whose in-flight requests can be cancelled.
     *
     * <p>
     * Callers that do not need cancellation should use {@link #client(RunContext)}.
     */
    default CancellableClient cancellableClient(final RunContext runContext) throws Exception {
        MinioConnection.MinioClientConfig minioClientConfig = minioClientConfig(runContext);

        MinioClient.Builder clientBuilder = MinioClient.builder();

        if (
            StringUtils.isNotEmpty(minioClientConfig.accessKeyId()) &&
                StringUtils.isNotEmpty(minioClientConfig.secretKeyId())
        ) {
            clientBuilder.credentials(minioClientConfig.accessKeyId(), minioClientConfig.secretKeyId());
        }

        if (StringUtils.isNotEmpty(minioClientConfig.endpoint())) {
            clientBuilder.endpoint(minioClientConfig.endpoint());
        }

        if (StringUtils.isNotEmpty(minioClientConfig.region())) {
            clientBuilder.region(minioClientConfig.region());
        }

        OkHttpClient customHttpClient = buildHttpClient(minioClientConfig, runContext);

        // The HTTP client must be built here rather than left to the SDK, otherwise there is no reference to
        // cancel through. Falling back to the SDK's own factory keeps timeouts, protocols, interceptors and
        // SSL_CERT_FILE/SSL_CERT_DIR handling identical to what the SDK would have built for itself.
        OkHttpClient httpClient = customHttpClient != null ? customHttpClient : Http.newDefaultClient();

        // Ownership must mirror the SDK's own rule, since MinioClient.close() only tears down an HTTP client
        // the SDK created: a client we defaulted in is closed as before, and a user-configured one is left
        // alone exactly as it is today.
        clientBuilder.httpClient(httpClient, customHttpClient == null);

        return new CancellableClient(clientBuilder.build(), httpClient);
    }

    default MinioAsyncClient asyncClient(final RunContext runContext) throws Exception {
        MinioConnection.MinioClientConfig minioClientConfig = minioClientConfig(runContext);

        MinioAsyncClient.Builder clientBuilder = MinioAsyncClient.builder();

        if (
            StringUtils.isNotEmpty(minioClientConfig.accessKeyId()) &&
                StringUtils.isNotEmpty(minioClientConfig.secretKeyId())
        ) {
            clientBuilder.credentials(minioClientConfig.accessKeyId(), minioClientConfig.secretKeyId());
        }

        if (StringUtils.isNotEmpty(minioClientConfig.endpoint())) {
            clientBuilder.endpoint(minioClientConfig.endpoint());
        }

        if (StringUtils.isNotEmpty(minioClientConfig.region())) {
            clientBuilder.region(minioClientConfig.region());
        }

        OkHttpClient httpClient = buildHttpClient(minioClientConfig, runContext);
        if (httpClient != null) {
            clientBuilder.httpClient(httpClient);
        }

        return clientBuilder.build();
    }

    private static OkHttpClient buildHttpClient(MinioConnection.MinioClientConfig config, RunContext runContext) throws Exception {
        if (config.sslOptions() != null && runContext.render(config.sslOptions().getInsecureTrustAllCertificates()).as(Boolean.class).orElse(false)) {
            runContext.logger().warn(
                "MinIO client is configured with 'ssl.insecureTrustAllCertificates=true': TLS certificate validation " +
                    "and hostname verification are disabled. This makes the connection vulnerable to man-in-the-middle " +
                    "attacks and should only be used for local development or testing against trusted networks."
            );

            SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(null, (chain, authType) -> true)
                .build();

            return new OkHttpClient.Builder()
                .sslSocketFactory(sslContext.getSocketFactory(), CustomTrustManager.INSTANCE)
                .hostnameVerifier((h, s) -> true)
                .build();
        }

        if (config.clientPem() != null || config.caPem() != null) {
            return MinioClientUtils.withPemCertificate(
                config.clientPem() != null
                    ? new ByteArrayInputStream(config.clientPem().getBytes(StandardCharsets.UTF_8))
                    : null,
                config.caPem() != null
                    ? new ByteArrayInputStream(config.caPem().getBytes(StandardCharsets.UTF_8))
                    : null
            );
        }

        return null;
    }

    class CustomTrustManager implements X509TrustManager {
        static final CustomTrustManager INSTANCE = new CustomTrustManager();

        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
