package io.kestra.plugin.minio;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import io.minio.errors.ErrorResponseException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CopyTest extends AbstractMinIoTest {
    private static String caPem;
    private static String clientPem;

    @BeforeAll
    static void loadCerts() throws Exception {
        Path certDir = Path.of("src/test/resources/mtls");
        caPem = Files.readString(certDir.resolve("ca-cert.pem"));
        clientPem = Files.readString(certDir.resolve("client-cert-key.pem"));
    }

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
            .region(Property.ofValue(minIOContainer.getRegion()))
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
    void testWithTlsContainer_andClientPem_shouldUseMtls() throws Exception {
        var tlsContainer = new VersityContainer(DockerImageName.parse(VERSITY_IMAGE))
            .withUserName("testuser")
            .withPassword("testpassword")
            .withCopyToContainer(
                MountableFile.forHostPath("src/test/resources/mtls"),
                "/root/.minio/certs"
            )
            .withCommand(
                "--access", "testuser",
                "--secret", "testpassword",
                "--port", ":9000",
                "--cert", "/root/.minio/certs/server-cert.pem",
                "--key", "/root/.minio/certs/server-key.pem",
                "--region", "us-east-2",
                "posix", "/data",
                "--quiet"
            )
            .waitingFor(Wait.forListeningPort())
            .withExposedPorts(9000);

        try {
            tlsContainer.start();

            var endpoint = "https://localhost:" + tlsContainer.getMappedPort(9000);

            var createBucket = CreateBucket.builder()
                .id("tls-create-bucket" + IdUtils.create())
                .type(CreateBucket.class.getName())
                .endpoint(Property.ofValue(endpoint))
                .accessKeyId(Property.ofValue(tlsContainer.getUserName()))
                .secretKeyId(Property.ofValue(tlsContainer.getPassword()))
                .bucket(Property.ofValue("tls-bucket"))
                .region(Property.ofValue("us-east-2"))
                .caPem(Property.ofValue(caPem))
                .clientPem(Property.ofValue(clientPem))
                .build();

            createBucket.run(runContext(createBucket));

            var source = storagePut("application.yml");

            var uploadTask = Upload.builder()
                .id("tls-upload-" + IdUtils.create())
                .type(Upload.class.getName())
                .endpoint(Property.ofValue(endpoint))
                .accessKeyId(Property.ofValue(tlsContainer.getUserName()))
                .secretKeyId(Property.ofValue(tlsContainer.getPassword()))
                .region(Property.ofValue("us-east-2"))
                .bucket(Property.ofValue("tls-bucket"))
                .from(source.toString())
                .key(Property.ofValue("tls.txt"))
                .caPem(Property.ofValue(caPem))
                .clientPem(Property.ofValue(clientPem))
                .build();

            uploadTask.run(runContext(uploadTask));

            var copyTask = Copy.builder()
                .id("copy-" + IdUtils.create())
                .type(Copy.class.getName())
                .endpoint(Property.ofValue(endpoint))
                .accessKeyId(Property.ofValue(tlsContainer.getUserName()))
                .secretKeyId(Property.ofValue(tlsContainer.getPassword()))
                .region(Property.ofValue("us-east-2"))
                .caPem(Property.ofValue(caPem))
                .clientPem(Property.ofValue(clientPem))
                .from(
                    Copy.CopyObjectFrom.builder()
                        .bucket(Property.ofValue("tls-bucket"))
                        .key(Property.ofValue("tls.txt"))
                        .build()
                )
                .to(
                    Copy.CopyObject.builder()
                        .key(Property.ofValue("copy.txt"))
                        .build()
                )
                .build();

            Copy.Output copy = copyTask.run(runContext(copyTask));
            assertThat(copy.getKey(), is(notNullValue()));
        } finally {
            tlsContainer.stop();
        }
    }

    @Test
    void withoutDeletingOriginal() throws Exception {
        this.run(false);
    }

    @Test
    void deleteOriginal() throws Exception {
        this.run(true);
    }

    @Test
    void copyWhenMatchETagMatches() throws Exception {
        this.createBucket();

        var sourceKey = upload("tasks/match");
        var listTask = list().prefix(Property.ofValue(sourceKey)).build();
        var etag = listTask.run(runContext(listTask)).getObjects().getFirst().getEtag();

        var destKey = "tasks/%s/copy".formatted(IdUtils.create());

        var copy = Copy.builder()
            .id(IdUtils.create())
            .type(Copy.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .region(Property.ofValue(minIOContainer.getRegion()))
            .from(
                Copy.CopyObjectFrom.builder()
                    .bucket(Property.ofValue(this.BUCKET))
                    .key(Property.ofValue(sourceKey))
                    .matchETag(Property.ofValue(etag))
                    .build()
            )
            .to(Copy.CopyObject.builder().key(Property.ofValue(destKey)).build())
            .build();

        var output = copy.run(runContext(copy));

        assertThat(output.getKey(), is(destKey));
        assertThat(output.getETag(), is(notNullValue()));

        var listDest = list().prefix(Property.ofValue(destKey)).build();
        assertThat(listDest.run(runContext(listDest)).getObjects().size(), is(1));
    }

    @Test
    void copyWhenMatchETagMismatches() throws Exception {
        this.createBucket();

        var sourceKey = upload("tasks/mismatch");
        var destKey = "tasks/%s/copy".formatted(IdUtils.create());

        var copy = Copy.builder()
            .id(IdUtils.create())
            .type(Copy.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .region(Property.ofValue(minIOContainer.getRegion()))
            .from(
                Copy.CopyObjectFrom.builder()
                    .bucket(Property.ofValue(this.BUCKET))
                    .key(Property.ofValue(sourceKey))
                    .matchETag(Property.ofValue("\"00000000000000000000000000000000-0\""))
                    .build()
            )
            .to(Copy.CopyObject.builder().key(Property.ofValue(destKey)).build())
            .build();

        var error = assertThrows(ErrorResponseException.class, () -> copy.run(runContext(copy)));
        assertThat(error.response().code(), is(412));
        assertThat(error.errorResponse().code(), is("PreconditionFailed"));

        var listDest = list().prefix(Property.ofValue(destKey)).build();
        assertThat(listDest.run(runContext(listDest)).getObjects().size(), is(0));
    }

    @Test
    void copyWhenModifiedSinceRenderedFromExpression() throws Exception {
        this.createBucket();

        var sourceKey = upload("tasks/modified-since");
        var destKey = "tasks/%s/copy".formatted(IdUtils.create());

        var copy = Copy.builder()
            .id(IdUtils.create())
            .type(Copy.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .region(Property.ofValue(minIOContainer.getRegion()))
            .from(
                Copy.CopyObjectFrom.builder()
                    .bucket(Property.ofValue(this.BUCKET))
                    .key(Property.ofValue(sourceKey))
                    .modifiedSince(Property.ofExpression("{{ '2024-06-13T12:00:00Z' }}"))
                    .build()
            )
            .to(Copy.CopyObject.builder().key(Property.ofValue(destKey)).build())
            .build();

        var output = copy.run(runContext(copy));
        assertThat(output.getKey(), is(destKey));
    }

}
