package io.kestra.plugin.minio;

import io.kestra.core.runners.RunContext;
import io.minio.MinioAsyncClient;
import io.minio.MinioClient;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.ssl.SSLContexts;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;

public interface AbstractMinio extends MinioConnectionInterface {

    default MinioClient client(final RunContext runContext) throws Exception {
        MinioConnection.MinioClientConfig minioClientConfig = minioClientConfig(runContext);

        MinioClient.Builder clientBuilder = MinioClient.builder();

        if (StringUtils.isNotEmpty(minioClientConfig.accessKeyId()) &&
            StringUtils.isNotEmpty(minioClientConfig.secretKeyId())) {
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

    default MinioAsyncClient asyncClient(final RunContext runContext) throws Exception {
        MinioConnection.MinioClientConfig minioClientConfig = minioClientConfig(runContext);

        MinioAsyncClient.Builder clientBuilder = MinioAsyncClient.builder();

        if (StringUtils.isNotEmpty(minioClientConfig.accessKeyId()) &&
            StringUtils.isNotEmpty(minioClientConfig.secretKeyId())) {
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
