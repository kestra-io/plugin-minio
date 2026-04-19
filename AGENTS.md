# Kestra Minio Plugin

## What

- Provides plugin components under `io.kestra.plugin.minio`.
- Includes classes such as `Delete`, `Upload`, `List`, `DeleteList`.

## Why

- What user problem does this solve? Teams need to interact with MinIO S3-compatible object storage from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps MinIO steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on MinIO.

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
