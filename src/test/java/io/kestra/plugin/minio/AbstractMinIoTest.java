package io.kestra.plugin.minio;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MinIOContainer;
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
public class AbstractMinIoTest {
    public static String MINIO_IMAGE = "minio/minio:RELEASE.2023-09-04T19-57-37Z";

    protected static MinIOContainer minIOContainer;

    @BeforeAll
    static void startMinIo() {
        minIOContainer = new MinIOContainer(DockerImageName.parse(MINIO_IMAGE))
            .withUserName("testuser")
            .withPassword("testpassword");

        minIOContainer.setPortBindings(Collections.singletonList("9000:9000"));
        minIOContainer.start();
    }

    @AfterAll
    static void stopMinIo() {
        if (minIOContainer != null) {
            minIOContainer.stop();
        }
    }

    @Inject
    protected final String BUCKET = IdUtils.create().toLowerCase();

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    protected static File file() throws URISyntaxException {
        return new File(Objects.requireNonNull(AbstractMinIoTest.class.getClassLoader()
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
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .bucket(Property.ofValue(bucket))
            .build();

        CreateBucket.Output createOutput = createBucket.run(runContext(createBucket));

        return createOutput.getBucket();
    }

    protected String upload(String dir) throws Exception {
        return upload(dir, this.BUCKET);
    }

    protected URI storagePut(String path) throws URISyntaxException, IOException {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
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
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .bucket(Property.ofValue(bucket))
            .from(source.toString())
            .key(Property.ofValue(dir + "/" + out + ".yml"))
            .build();

        Upload.Output output = upload.run(runContext(upload));

        return output.getKey();
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(minIOContainer.getS3URL()))
            .accessKeyId(Property.ofValue(minIOContainer.getUserName()))
            .secretKeyId(Property.ofValue(minIOContainer.getPassword()))
            .bucket(Property.ofValue(this.BUCKET))
            .includeVersions(Property.ofValue(true));
    }

    protected RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }

}
