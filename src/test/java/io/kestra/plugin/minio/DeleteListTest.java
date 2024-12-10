package io.kestra.plugin.minio;

import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class DeleteListTest extends AbstractMinIoTest {

    @Test
    void run() throws Exception {
        this.createBucket();

        for (int i = 0; i < 10; i++) {
            upload("/tasks/s3");
        }

        DeleteList task = DeleteList.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpoint(Property.of(minIOContainer.getS3URL()))
            .accessKeyId(Property.of(minIOContainer.getUserName()))
            .secretKeyId(Property.of(minIOContainer.getPassword()))
            .concurrent(5)
            .build();

        DeleteList.Output run = task.run(runContext(task));
        assertThat(run.getCount(), is(10L));
        assertThat(run.getSize(), greaterThan(1000L));
    }

}
