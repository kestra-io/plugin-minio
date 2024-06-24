package io.kestra.plugin.minio;

import io.kestra.core.utils.IdUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
            .filter(List.Filter.FILES)
            .prefix("tasks/"+dir+"/")
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(6));

        task = list()
            .filter(List.Filter.FILES)
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(6));

        task = list()
            .prefix("tasks/%s/sub".formatted(dir))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(1));

        task = list()
            .regexp("tasks/.*/" + StringUtils.substringAfterLast(lastFileName, "/"))
            .build();
        output = task.run(runContext(task));
        assertThat(output.getObjects().size(), is(1));
    }

}
