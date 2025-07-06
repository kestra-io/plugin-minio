package io.kestra.plugin.minio;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CopyTest extends AbstractMinIoTest {

    void run(Boolean delete) throws Exception {
        this.createBucket();

        String upload = upload("tasks/%s/sub".formatted(IdUtils.create()));
        String move = upload("tasks/%s/sub".formatted(IdUtils.create()));

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .from(
                Copy.CopyObjectFrom
                    .builder()
                    .bucket(Property.ofValue(this.BUCKET))
                    .key(Property.ofValue(upload))
                    .build()
            )
            .to(
                Copy.CopyObject
                    .builder()
                    .key(Property.ofValue(move))
                    .build()
            )
            .delete(Property.ofValue(delete))
            .build();

        Copy.Output copyOutput = task.run(runContext(task));
        assertThat(copyOutput.getKey(), is(move));

        List list = list().prefix(Property.ofValue(move)).build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        list = list().prefix(Property.ofValue(upload)).build();

        listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(delete ? 0 : 1));
    }

    @Test
    void withoutDeletingOriginal() throws Exception {
        this.run(false);
    }

    @Test
    void deleteOriginal() throws Exception {
        this.run(true);
    }

}
