package io.kestra.plugin.minio;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class MinioServiceTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldUseFilenameWhenKeyIsNull() throws Exception {
        RunContext runContext = runContextFactory.of();

        String destination = MinioService.destinationKey(runContext, null, "incoming/data.csv");

        assertThat(destination).isEqualTo("data.csv");
    }

    @Test
    void shouldUseFilenameWhenKeyIsEmptyOrOnlySlashes() throws Exception {
        RunContext runContext = runContextFactory.of();

        String empty = MinioService.destinationKey(runContext, Property.ofValue(""), "incoming/data.csv");
        String rootSlash = MinioService.destinationKey(runContext, Property.ofValue("/"), "incoming/data.csv");
        String multipleSlashes = MinioService.destinationKey(runContext, Property.ofValue("///"), "incoming/data.csv");

        assertThat(empty).isEqualTo("data.csv");
        assertThat(rootSlash).isEqualTo("data.csv");
        assertThat(multipleSlashes).isEqualTo("data.csv");
    }

    @Test
    void shouldPrefixFilenameWhenKeyIsProvided() throws Exception {
        RunContext runContext = runContextFactory.of();

        String withoutTrailingSlash = MinioService.destinationKey(runContext, Property.ofValue("archive"), "in/orders.parquet");
        String withTrailingSlash = MinioService.destinationKey(runContext, Property.ofValue("archive/"), "in/orders.parquet");
        String nestedDirectory = MinioService.destinationKey(runContext, Property.ofValue("backup/2026/"), "in/orders.parquet");

        assertThat(withoutTrailingSlash).isEqualTo("archive/orders.parquet");
        assertThat(withTrailingSlash).isEqualTo("archive/orders.parquet");
        assertThat(nestedDirectory).isEqualTo("backup/2026/orders.parquet");
    }

    @Test
    void shouldThrowExceptionWhenMoveActionHasNoMoveToConfigured() {
        RunContext runContext = runContextFactory.of();

        assertThatThrownBy(() -> MinioService.performAction(
            runContext,
            Collections.emptyList(),
            Downloads.Action.MOVE,
            "source-bucket",
            null,
            null
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Action 'MOVE' requires 'moveTo' to be configured.");
    }
}
