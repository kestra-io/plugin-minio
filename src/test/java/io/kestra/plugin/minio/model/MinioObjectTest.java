package io.kestra.plugin.minio.model;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import io.minio.messages.Contents;
import io.minio.messages.Item;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MinioObjectTest {

    @Test
    void mapsDirectoryEntryWithoutLastModified() {
        // the SDK turns each CommonPrefixes entry of a non-recursive listing into a Contents
        // holding only the prefix name, so timestamp, etag and owner are all absent
        MinioObject object = MinioObject.of(new Contents("reports/"));

        assertThat(object.getKey(), is("reports/"));
        assertThat(object.getLastModified(), is(nullValue()));
        assertThat(object.getEtag(), is(nullValue()));
        assertThat(object.getOwner(), is(nullValue()));
        assertThat(object.getVersionId(), is(nullValue()));
    }

    @Test
    void mapsLastModifiedWhenPresent() {
        ZonedDateTime modified = ZonedDateTime.of(2026, 10, 21, 12, 0, 0, 0, ZoneOffset.UTC);
        Item item = mock(Item.class);
        when(item.objectName()).thenReturn("reports/one.txt");
        when(item.lastModified()).thenReturn(modified);

        MinioObject object = MinioObject.of(item);

        assertThat(object.getKey(), is("reports/one.txt"));
        assertThat(object.getLastModified(), is(modified.toInstant()));
    }
}
