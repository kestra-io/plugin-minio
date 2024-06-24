package io.kestra.plugin.minio;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

public interface MinioConnectionInterface {

    @Schema(
        title = "URL to S3 service."
    )
    @PluginProperty(dynamic = true)
    String getEndpoint();

    @Schema(
        title = "Access Key Id in order to connect to S3 service."
    )
    @PluginProperty(dynamic = true)
    String getAccessKeyId();

    @Schema(
        title = "Secret Key Id in order to connect to S3 service."
    )
    @PluginProperty(dynamic = true)
    String getSecretKeyId();

    @Schema(
        title = "MinIO region with which the SDK should communicate."
    )
    @PluginProperty(dynamic = true)
    String getRegion();

    default MinioConnection.MinioClientConfig minioClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new MinioConnection.MinioClientConfig(
            runContext.render(this.getAccessKeyId()),
            runContext.render(this.getSecretKeyId()),
            runContext.render(this.getRegion()),
            runContext.render(this.getEndpoint())
        );
    }

}
