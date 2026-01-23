package io.kestra.plugin.minio;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class VersityContainer extends GenericContainer<VersityContainer> {
    private static final int S3_PORT = 9000;
    private static final String ROOT_PATH = "/data";

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

    private void applyCredentials() {
        withEnv("ROOT_ACCESS_KEY_ID", userName);
        withEnv("ROOT_ACCESS_KEY", userName);
        withEnv("ROOT_SECRET_ACCESS_KEY", password);
        withEnv("ROOT_SECRET_KEY", password);
        withCommand(
            "--access", userName,
            "--secret", password,
            "--port", ":" + S3_PORT,
            "posix", ROOT_PATH,
            "--quiet"
        );
    }
}
