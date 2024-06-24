package io.kestra.plugin.minio.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
public class ObjectOutput {

    @Schema(
        title = "An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL."
    )
    private String eTag;

    @Schema(
        title = "The version of the object."
    )
    private String versionId;

}
