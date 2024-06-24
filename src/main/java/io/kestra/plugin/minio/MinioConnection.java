package io.kestra.plugin.minio;

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

    protected String region;

    protected String accessKeyId;
    protected String secretKeyId;

    protected String endpoint;

    public record MinioClientConfig(
        @Nullable String accessKeyId,
        @Nullable String secretKeyId,
        @Nullable String region,
        @Nullable String endpoint
    ) { }

}
