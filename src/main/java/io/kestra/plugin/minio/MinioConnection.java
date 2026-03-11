package io.kestra.plugin.minio;

import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;

import jakarta.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class MinioConnection extends Task implements MinioConnectionInterface {

    protected Property<String> region;

    protected Property<String> accessKeyId;
    protected Property<String> secretKeyId;

    protected Property<String> endpoint;

    private Property<String> clientPem;

    private Property<String> caPem;

    protected SslOptions ssl;

    public record MinioClientConfig(
        @Nullable String accessKeyId,
        @Nullable String secretKeyId,
        @Nullable String region,
        @Nullable String endpoint,
        @Nullable String clientPem,
        @Nullable String caPem,
        @Nullable SslOptions sslOptions) {
    }

}
