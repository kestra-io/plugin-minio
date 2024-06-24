package io.kestra.plugin.minio;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.minio.MinioAsyncClient;
import io.minio.MinioClient;
import org.apache.commons.lang3.StringUtils;

public interface AbstractMinio extends MinioConnectionInterface {

    default MinioClient client(final RunContext runContext) throws IllegalVariableEvaluationException {
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

        return clientBuilder.build();
    }

    default MinioAsyncClient asyncClient(final RunContext runContext) throws IllegalVariableEvaluationException {
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

        return clientBuilder.build();
    }

}
