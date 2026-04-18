# Kestra Minio Plugin

## What

- Provides plugin components under `io.kestra.plugin.minio`.
- Includes classes such as `Delete`, `Upload`, `List`, `DeleteList`.

## Why

- This plugin integrates Kestra with MinIO.
- It provides tasks that interact with MinIO S3-compatible object storage.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `minio`

### Key Plugin Classes

- `io.kestra.plugin.minio.Copy`
- `io.kestra.plugin.minio.CreateBucket`
- `io.kestra.plugin.minio.Delete`
- `io.kestra.plugin.minio.DeleteList`
- `io.kestra.plugin.minio.Download`
- `io.kestra.plugin.minio.Downloads`
- `io.kestra.plugin.minio.List`
- `io.kestra.plugin.minio.Trigger`
- `io.kestra.plugin.minio.Upload`

### Project Structure

```
plugin-minio/
├── src/main/java/io/kestra/plugin/minio/model/
├── src/test/java/io/kestra/plugin/minio/model/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
