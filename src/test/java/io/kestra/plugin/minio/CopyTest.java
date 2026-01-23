package io.kestra.plugin.minio;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
                .caPem(Property.ofValue(caPem))
                .clientPem(Property.ofValue(clientPem))
                .from(Copy.CopyObjectFrom.builder()
                    .bucket(Property.ofValue("tls-bucket"))
                    .key(Property.ofValue("tls.txt"))
                    .build())
                .to(Copy.CopyObject.builder()
                    .key(Property.ofValue("copy.txt"))
                    .build())
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

}
