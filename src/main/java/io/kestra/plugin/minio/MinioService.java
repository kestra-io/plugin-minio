package io.kestra.plugin.minio;

import java.io.File;
import java.net.URI;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.minio.model.MinioObject;

import io.minio.DownloadObjectArgs;
import io.minio.MinioAsyncClient;

public class MinioService {

    public static void performAction(
        RunContext runContext,
        List<MinioObject> list,
        Downloads.Action action,
        String bucket,
        MinioConnectionInterface minioConnection,
        Copy.CopyObject moveTo) throws Exception {
        if (action == Downloads.Action.DELETE) {
            for (MinioObject object : list) {
                Delete delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .region(minioConnection.getRegion())
                    .accessKeyId(minioConnection.getAccessKeyId())
                    .secretKeyId(minioConnection.getSecretKeyId())
                    .key(Property.ofValue(object.getKey()))
                    .bucket(Property.ofValue(bucket))
                    .endpoint(minioConnection.getEndpoint())
                    .build();
                delete.run(runContext);
            }
        } else if (action == Downloads.Action.MOVE) {
            if (moveTo == null) {
                throw new IllegalArgumentException("Action 'MOVE' requires 'moveTo' to be configured.");
            }
            for (MinioObject object : list) {
                String targetKey = destinationKey(runContext, moveTo.getKey(), object.getKey());
                Copy copy = Copy.builder()
                    .id("archive")
                    .type(Copy.class.getName())
                    .region(minioConnection.getRegion())
                    .endpoint(minioConnection.getEndpoint())
                    .accessKeyId(minioConnection.getAccessKeyId())
                    .secretKeyId(minioConnection.getSecretKeyId())
                    .from(
                        Copy.CopyObjectFrom
                            .builder()
                            .bucket(Property.ofValue(bucket))
                            .key(Property.ofValue(object.getKey()))
                            .build()
                    )
                    .to(
                        moveTo.toBuilder()
                            .key(Property.ofValue(targetKey))
                            .build()
                    )
                    .delete(Property.ofValue(true))
                    .build();
                copy.run(runContext);
            }
        }
    }

    static String destinationKey(RunContext runContext, Property<String> moveToKey, String sourceKey) throws Exception {
        String renderedKey = moveToKey == null ? null : runContext.render(moveToKey).as(String.class).orElse(null);
        String filename = FilenameUtils.getName(sourceKey);
        if (StringUtils.isNotBlank(renderedKey)) {
            String prefix = StringUtils.stripEnd(renderedKey.trim(), "/");
            if (prefix.isEmpty() == false) {
                return "%s/%s".formatted(prefix, filename);
            }
        }
        return filename;
    }

    public static Pair<URI, Long> download(RunContext runContext, MinioAsyncClient client, String bucket, String key, String versionId) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(key)).toFile();
        boolean deleted = tempFile.delete();

        DownloadObjectArgs.Builder requestBuilder = DownloadObjectArgs.builder()
            .bucket(bucket)
            .object(key)
            .filename(tempFile.getPath());

        if (versionId != null) {
            requestBuilder.versionId(runContext.render(versionId));
        }

        client.downloadObject(requestBuilder.build()).get();

        URI uri = runContext.storage().putFile(tempFile);

        runContext.metric(Counter.of("file.size", tempFile.length()));

        return Pair.of(uri, tempFile.length());
    }

}
