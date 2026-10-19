package io.kestra.plugin.minio.model;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import io.minio.messages.Item;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MinioObjectTest {

    @Test
    void includesVersionMetadata() {
        Item item = mock(Item.class);
        when(item.objectName()).thenReturn("object.txt");
        when(item.lastModified()).thenReturn(ZonedDateTime.now(ZoneOffset.UTC));
        when(item.versionId()).thenReturn("version-1");
        when(item.isLatest()).thenReturn(true);
        when(item.isDeleteMarker()).thenReturn(false);

        MinioObject object = MinioObject.of(item);

        assertThat(object.getVersionId(), is("version-1"));
        assertThat(object.getIsLatest(), is(true));
        assertThat(object.getIsDeleteMarker(), is(false));
    }

    @Test
    void leavesVersionMetadataNullForUnversionedObject() {
        Item item = mock(Item.class);
        when(item.objectName()).thenReturn("object.txt");
        when(item.lastModified()).thenReturn(ZonedDateTime.now(ZoneOffset.UTC));

        MinioObject object = MinioObject.of(item);

        assertThat(object.getVersionId(), is(nullValue()));
        assertThat(object.getIsLatest(), is(nullValue()));
        assertThat(object.getIsDeleteMarker(), is(nullValue()));
    }
}
