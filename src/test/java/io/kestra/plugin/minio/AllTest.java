package io.kestra.plugin.minio;

import com.google.common.io.CharStreams;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.minio.errors.ErrorResponseException;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AllTest extends AbstractMinIoTest {

    @Test
    void run() throws Exception {
        this.createBucket();

        String key = upload("tasks/upload");

        List list = List
            .builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .prefix(Property.ofValue("tasks/upload/"))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getObjects().size(), is(1));

        Download download = Download
            .builder()
            .id(AllTest.class.getSimpleName())
            .type(Download.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .key(Property.ofValue(key))
            .build();

        Download.Output downloadOutput = download.run(runContext(download));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, downloadOutput.getUri());
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file()))))
        );

        Delete delete = Delete
            .builder()
            .id(AllTest.class.getSimpleName())
            .type(Delete.class.getName())
            .bucket(Property.ofValue(this.BUCKET))
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .key(Property.ofValue(key))
            .build();

        Delete.Output deleteOutput = delete.run(runContext(delete));
        assertThat(deleteOutput.getBucket(), is(notNullValue()));
        assertThat(deleteOutput.getKey(), is(notNullValue()));

        // delete missing
        ExecutionException exp = assertThrows(
            ExecutionException.class,
            () -> download.run(runContext(download))
        );
        assertThat(exp.getCause(), instanceOf(CompletionException.class));
        CompletionException cause = (CompletionException) exp.getCause();
        ErrorResponseException response = ((ErrorResponseException) cause.getCause());

        assertThat(response.response().code(), is(404));
        assertThat(response.errorResponse().code(), is("NoSuchKey"));
    }
}
