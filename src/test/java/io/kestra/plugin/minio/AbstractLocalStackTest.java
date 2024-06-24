package io.kestra.plugin.minio;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Objects;

@KestraTest
@Testcontainers
//@Disabled("Disabled as for testing purposes are uses MinIOContainer which can be replaced by LocalStackContainer")
public class AbstractLocalStackTest {
    public static String LOCALSTACK_VERSION = "localstack/localstack:3.4.0";

    protected static LocalStackContainer localstack;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse(LOCALSTACK_VERSION));

        localstack.setPortBindings(Collections.singletonList("4566:4566"));
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    @Inject
    protected final String BUCKET = IdUtils.create().toLowerCase();

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    protected static File file() throws URISyntaxException {
        return new File(Objects.requireNonNull(AbstractLocalStackTest.class.getClassLoader()
                .getResource("application.yml"))
            .toURI());
    }

    protected String createBucket() throws Exception {
        return this.createBucket(this.BUCKET);
    }

    protected String createBucket(String bucket) throws Exception {
        CreateBucket createBucket = CreateBucket.builder()
            .id(AllTest.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .endpoint(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .bucket(bucket)
            .build();

        CreateBucket.Output createOutput = createBucket.run(runContext(createBucket));

        return createOutput.getBucket();
    }

    protected String upload(String dir) throws Exception {
        return upload(dir, this.BUCKET);
    }

    protected URI storagePut(String path) throws URISyntaxException, IOException {
        return storageInterface.put(
            null,
            new URI("/" + (path != null ? path : IdUtils.create())),
            new FileInputStream(file())
        );
    }

    protected String upload(String dir, String bucket) throws Exception {
        String out = IdUtils.create();
        URI source = storagePut(null);

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .endpoint(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .bucket(bucket)
            .from(source.toString())
            .key(dir + "/" + out + ".yml")
            .build();

        Upload.Output output = upload.run(runContext(upload));

        return output.getKey();
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .accessKeyId(localstack.getAccessKey())
            .secretKeyId(localstack.getSecretKey())
            .region(localstack.getRegion())
            .bucket(this.BUCKET)
            .includeVersions(true);
    }

    protected RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }

}
