package io.kestra.plugin.minio;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

public interface MinioConnectionInterface {

    @Schema(
        title = "URL to the MinIO endpoint."
    )
    Property<String> getEndpoint();

    @Schema(
        title = "Access Key Id for authentication."
    )
    Property<String> getAccessKeyId();

    @Schema(
        title = "Secret Key Id for authentication."
    )
    Property<String> getSecretKeyId();

    @Schema(
        title = "MinIO region with which the SDK should communicate."
    )
    Property<String> getRegion();

    default MinioConnection.MinioClientConfig minioClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new MinioConnection.MinioClientConfig(
            runContext.render(this.getAccessKeyId()).as(String.class).orElse(null),
            runContext.render(this.getSecretKeyId()).as(String.class).orElse(null),
            runContext.render(this.getRegion()).as(String.class).orElse(null),
            runContext.render(this.getEndpoint()).as(String.class).orElse(null)
        );
    }

}
