package io.kestra.plugin.minio.model;

import java.net.URI;
import java.time.Instant;

import io.minio.messages.Item;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@Builder
public class MinioObject {

    @With
    URI uri;

    String key;

    String versionId;

    Boolean isLatest;

    Boolean isDeleteMarker;

    String etag;

    Long size;

    Instant lastModified;

    Owner owner;

    public static MinioObject of(Item object) {
        return MinioObject.builder()
            .key(object.objectName())
            .versionId(object.versionId())
            .isLatest(object.versionId() == null ? null : object.isLatest())
            .isDeleteMarker(object.versionId() == null ? null : object.isDeleteMarker())
            .etag(object.etag())
            .size(object.size())
            .lastModified(object.lastModified().toInstant())
            .owner(Owner.of(object.owner()))
            .build();
    }

}
