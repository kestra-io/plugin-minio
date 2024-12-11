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
            .endpoint(Property.of(minIOContainer.getS3URL()))
            .accessKeyId(Property.of(minIOContainer.getUserName()))
            .secretKeyId(Property.of(minIOContainer.getPassword()))
            .from(
                Copy.CopyObjectFrom
                    .builder()
                    .bucket(Property.of(this.BUCKET))
                    .key(Property.of(upload))
                    .build()
            )
            .to(
                Copy.CopyObject
                    .builder()
                    .key(Property.of(move))
                    .build()
            )
            .delete(Property.of(delete))
            .build();

        Copy.Output copyOutput = task.run(runContext(task));
        assertThat(copyOutput.getKey(), is(move));

        List list = list().prefix(move).build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        list = list().prefix(upload).build();

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
