package io.kestra.plugin.minio;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class VersityContainer extends GenericContainer<VersityContainer> {
    private static final int S3_PORT = 9000;
    private static final String ROOT_PATH = "/data";
    private static final String REGION = "us-east-2";

    private String userName;
    private String password;

    public VersityContainer(DockerImageName imageName) {
        super(imageName);
        this.userName = "testuser";
        this.password = "testpassword";

        withTmpFs(Map.of(ROOT_PATH, "rw"));
        withExposedPorts(S3_PORT);
        waitingFor(Wait.forListeningPort());
        applyCredentials();
    }

    public VersityContainer withUserName(String userName) {
        this.userName = userName;
        applyCredentials();
        return this;
    }

    public VersityContainer withPassword(String password) {
        this.password = password;
        applyCredentials();
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getS3URL() {
        return "http://" + getHost() + ":" + getMappedPort(S3_PORT);
    }

    public String getRegion() {
        return REGION;
    }

    private void applyCredentials() {
        withEnv("ROOT_ACCESS_KEY_ID", userName);
        withEnv("ROOT_ACCESS_KEY", userName);
        withEnv("ROOT_SECRET_ACCESS_KEY", password);
        withEnv("ROOT_SECRET_KEY", password);
        withCommand(
            "--access", userName,
            "--secret", password,
            "--port", ":" + S3_PORT,
            "--region", REGION,
            "posix", ROOT_PATH,
            "--quiet"
        );
    }
}
