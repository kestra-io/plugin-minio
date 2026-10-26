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
        // unversioned listings report isLatest/isDeleteMarker as false, keep them null instead
        boolean versioned = object.versionId() != null;

        // common-prefix (directory) entries from a non-recursive listing carry no last-modified timestamp
        var lastModified = object.lastModified();

        return MinioObject.builder()
            .key(object.objectName())
            .versionId(object.versionId())
            .isLatest(versioned ? object.isLatest() : null)
            .isDeleteMarker(versioned ? object.isDeleteMarker() : null)
            .etag(object.etag())
            .size(object.isDir() ? null : object.size())
            .lastModified(lastModified == null ? null : lastModified.toInstant())
            .owner(Owner.of(object.owner()))
            .build();
    }

}
