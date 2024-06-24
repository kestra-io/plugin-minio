package io.kestra.plugin.minio.model;

import io.minio.messages.Item;
import lombok.Builder;
import lombok.Data;
import lombok.With;

import java.net.URI;
import java.time.Instant;

@Data
@Builder
public class MinioObject {

    @With
    URI uri;

    String key;

    String etag;

    Long size;

    Instant lastModified;

    Owner owner;

    public static MinioObject of(Item object) {
        return MinioObject.builder()
            .key(object.objectName())
            .etag(object.etag())
            .size(object.size())
            .lastModified(object.lastModified().toInstant())
            .owner(Owner.of(object.owner()))
            .build();
    }

}
