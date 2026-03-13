# Kestra Minio Plugin

## What

Plugin MinIO for Kestra Exposes 9 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with MinIO, allowing orchestration of MinIO-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
