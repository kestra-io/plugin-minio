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
        // A non-recursive listing returns each directory as a Contents with only its prefix name
        var object = MinioObject.of(new Contents("reports/"));

        assertThat(object.getKey(), is("reports/"));
        assertThat(object.getLastModified(), is(nullValue()));
        assertThat(object.getSize(), is(nullValue()));
        assertThat(object.getEtag(), is(nullValue()));
        assertThat(object.getOwner(), is(nullValue()));
        assertThat(object.getVersionId(), is(nullValue()));
    }

    @Test
    void mapsLastModifiedWhenPresent() {
        var modified = ZonedDateTime.of(2026, 10, 21, 12, 0, 0, 0, ZoneOffset.UTC);
        var item = mock(Item.class);
        when(item.objectName()).thenReturn("reports/one.txt");
        when(item.lastModified()).thenReturn(modified);

        var object = MinioObject.of(item);

        assertThat(object.getKey(), is("reports/one.txt"));
        assertThat(object.getLastModified(), is(modified.toInstant()));
    }
}
