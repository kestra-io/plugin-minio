package io.kestra.plugin.minio;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
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
public abstract class AbstractMinioObject extends MinioConnection implements AbstractMinio {

    @Schema(
        title = "The S3 bucket name."
    )
    @PluginProperty(dynamic = true)
    protected String bucket;

}
