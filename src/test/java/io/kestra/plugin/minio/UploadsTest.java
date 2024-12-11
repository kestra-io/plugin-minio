package io.kestra.plugin.minio;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class UploadsTest extends AbstractMinIoTest {

    @Test
    void run() throws Exception {
        this.createBucket();

        URI source1 = storagePut("1.yml");
        URI source2 = storagePut("2.yml");
        URI source3 = storagePut("3.yml");
        URI source4 = storagePut("4.yml");

        Upload upload = Upload
            .builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpoint(Property.of(minIOContainer.getS3URL()))
            .accessKeyId(Property.of(minIOContainer.getUserName()))
            .secretKeyId(Property.of(minIOContainer.getPassword()))
            .from(java.util.List.of(source1.toString(), source2.toString(), source3.toString(), source4.toString()))
            .key(Property.of(IdUtils.create() + "/"))
            .build();
        var result = upload.run(runContext(upload));

        List list = List
            .builder()
            .id(AllTest.class.getSimpleName())
            .type(List.class.getName())
            .bucket(Property.of(this.BUCKET))
            .endpoint(Property.of(minIOContainer.getS3URL()))
            .accessKeyId(Property.of(minIOContainer.getUserName()))
            .secretKeyId(Property.of(minIOContainer.getPassword()))
            .prefix(Property.of(result.getKey()))
            .build();

        List.Output output = list.run(runContext(list));
        assertThat(output.getObjects().size(), is(4));
        assertThat(output.getObjects().stream().filter(object -> object.getKey().contains("1.yml")).count(), is(1L));
    }

}
