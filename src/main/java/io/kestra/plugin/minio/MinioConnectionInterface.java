package io.kestra.plugin.minio;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;

public interface MinioConnectionInterface {

    @Schema(
        title = "URL to the MinIO endpoint."
    )
    @PluginProperty(group = "connection")
    Property<String> getEndpoint();

    @Schema(
        title = "Access Key Id for authentication."
    )
    @PluginProperty(group = "advanced")
    Property<String> getAccessKeyId();

    @Schema(
        title = "Secret Key Id for authentication."
    )
    @PluginProperty(group = "advanced")
    Property<String> getSecretKeyId();

    @Schema(
        title = "MinIO region with which the SDK should communicate."
    )
    @PluginProperty(group = "connection")
    Property<String> getRegion();

    @Schema(
        title = "Client PEM certificate content",
        description = "PEM client certificate as text, used to authenticate the connection to MinIO (mTLS)."
    )
    @PluginProperty(group = "advanced")
    Property<String> getClientPem();

    @Schema(
        title = "CA PEM certificate content",
        description = "CA certificate as text, used to verify SSL/TLS connections to custom MinIO endpoints."
    )
    @PluginProperty(group = "advanced")
    Property<String> getCaPem();

    @Schema(
        title = "SSL/TLS configuration options"
    )
    @PluginProperty(group = "connection")
    SslOptions getSsl();

    default MinioConnection.MinioClientConfig minioClientConfig(final RunContext runContext) throws IllegalVariableEvaluationException {
        return new MinioConnection.MinioClientConfig(
            runContext.render(this.getAccessKeyId()).as(String.class).orElse(null),
            runContext.render(this.getSecretKeyId()).as(String.class).orElse(null),
            runContext.render(this.getRegion()).as(String.class).orElse(null),
            runContext.render(this.getEndpoint()).as(String.class).orElse(null),
            runContext.render(this.getClientPem()).as(String.class).orElse(null),
            runContext.render(this.getCaPem()).as(String.class).orElse(null),
            this.getSsl()
        );
    }

}
