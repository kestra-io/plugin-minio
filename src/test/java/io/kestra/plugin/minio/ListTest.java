package io.kestra.plugin.minio;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ListTest extends AbstractMinIoTest {

    @Test
    void run() throws Exception {
        this.createBucket();

        String dir = IdUtils.create();
        String lastFileName = null;

        for (int i = 0; i < 5; i++) {
            lastFileName = upload("/tasks/%s".formatted(dir));
        }
        upload("/tasks/%s/sub".formatted(dir));

        List task = list().build();
        List.Output output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(6));

        task = list()
            .filter(Property.ofValue(List.Filter.FILES))
            .prefix(Property.ofValue("tasks/" + dir + "/"))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(6));

        task = list()
            .filter(Property.ofValue(List.Filter.FILES))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(6));

        task = list()
            .prefix(Property.ofValue("tasks/%s/sub".formatted(dir)))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(1));

        task = list()
            .regexp(Property.ofValue("tasks/.*/" + StringUtils.substringAfterLast(lastFileName, "/")))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(1));
    }

    @Test
    void nonRecursiveListingReturnsDirectoryEntries() throws Exception {
        this.createBucket();

        var dir = IdUtils.create();
        upload("reports/%s".formatted(dir));
        upload("reports/%s".formatted(dir));

        var task = list()
            .prefix(Property.ofValue("reports/"))
            .delimiter(Property.ofValue("/"))
            .recursive(Property.ofValue(false))
            .includeVersions(Property.ofValue(false))
            .build();
        var output = task.run(runContext(task));

        assertThat(output.getObjects().size(), is(1));
        var directory = output.getObjects().getFirst();
        assertThat(directory.getKey(), is("reports/%s/".formatted(dir)));
        assertThat(directory.getLastModified(), is(nullValue()));
        assertThat(directory.getSize(), is(nullValue()));

        task = list()
            .prefix(Property.ofValue("reports/"))
            .delimiter(Property.ofValue("/"))
            .recursive(Property.ofValue(false))
            .includeVersions(Property.ofValue(false))
            .filter(Property.ofValue(List.Filter.FILES))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(0));
    }

}
