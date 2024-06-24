package io.kestra.plugin.minio.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Owner {

    private String id;

    private String displayName;

    public static Owner of(io.minio.messages.Owner owner) {
        return Owner
            .builder()
            .id(owner.id())
            .displayName(owner.displayName())
            .build();
    }

}
