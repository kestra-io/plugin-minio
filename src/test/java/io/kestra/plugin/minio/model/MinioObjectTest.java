package io.kestra.plugin.minio.model;

import org.junit.jupiter.api.Test;

import io.minio.messages.Item;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MinioObjectTest {

    @Test
    void mapsPrefixItemWithoutOptionalMetadata() {
        Item prefix = mock(Item.class);
        when(prefix.objectName()).thenReturn("reports/");
        when(prefix.lastModified()).thenReturn(null);
        when(prefix.etag()).thenReturn(null);
        when(prefix.owner()).thenReturn(null);

        MinioObject object = MinioObject.of(prefix);

        assertThat(object.getKey(), is("reports/"));
        assertThat(object.getSize(), is(0L));
        assertThat(object.getLastModified(), nullValue());
        assertThat(object.getEtag(), nullValue());
        assertThat(object.getOwner(), nullValue());
    }
}
