/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hoglake;

import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeFileFormats
{
    @Test
    void testParquetTableDefaultsRemainSupported()
    {
        assertThatCode(() -> HoglakeFileFormats.checkTableProperties(Map.of(), "test.events"))
                .doesNotThrowAnyException();
        assertThatCode(() -> HoglakeFileFormats.checkTableProperties(
                Map.of(HoglakeFileFormats.WRITE_FORMAT_DEFAULT_PROPERTY, HoglakeFileFormats.PARQUET),
                "test.events"))
                .doesNotThrowAnyException();
        assertThatCode(() -> HoglakeFileFormats.checkTableProperties(
                Map.of(HoglakeFileFormats.WRITE_FORMAT_DEFAULT_PROPERTY, "PARQUET"),
                "test.events"))
                .doesNotThrowAnyException();
    }

    @Test
    void testPackedTableIsRejectedEvenWhenEmpty()
    {
        HoglakeDtos.Table table = new HoglakeDtos.Table(
                "events",
                "test",
                "00000000-0000-0000-0000-000000000001",
                List.of(new HoglakeDtos.Column(1, 0, "id", "long", Map.of(), true)),
                0,
                0,
                0,
                null,
                null,
                null,
                Map.of(HoglakeFileFormats.WRITE_FORMAT_DEFAULT_PROPERTY, HoglakeFileFormats.CLICKHOUSE_MERGETREE_PACKED));

        assertThatThrownBy(() -> HoglakeFileFormats.checkReadableTable(table))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("test.events")
                .hasMessageContaining("clickhouse-mergetree-packed")
                .hasMessageContaining("supports only 'parquet'");
    }

    @Test
    void testWriterFragmentWithUnsupportedFormatIsAnInvalidResponse()
    {
        assertThatThrownBy(() -> HoglakeFileFormats.checkWriterFile(
                HoglakeFileFormats.CLICKHOUSE_MERGETREE_PACKED,
                "memory:///data.packed"))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(HOGLAKE_INVALID_RESPONSE.toErrorCode()))
                .hasMessageContaining("memory:///data.packed")
                .hasMessageContaining("clickhouse-mergetree-packed")
                .hasMessageContaining("supports only 'parquet'");
    }

    @Test
    void testWorkerRejectsPackedSplitBeforeAccessingStorage()
    {
        HoglakePageSourceProvider provider = new HoglakePageSourceProvider(_ -> {
            throw new AssertionError("unsupported files must not reach object storage");
        });
        HoglakeSplit split = new HoglakeSplit(
                1,
                "memory:///data.packed",
                HoglakeFileFormats.CLICKHOUSE_MERGETREE_PACKED,
                100,
                10,
                Optional.empty(),
                0,
                Optional.empty(),
                0,
                100,
                SplitWeight.standard(),
                OptionalLong.empty(),
                Optional.empty());
        HoglakeTableHandle table = new HoglakeTableHandle("test", "events", 1, "00000000-0000-0000-0000-000000000001", List.of());

        assertThatThrownBy(() -> provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                table,
                Optional.empty(),
                List.of(),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("memory:///data.packed")
                .hasMessageContaining("clickhouse-mergetree-packed")
                .hasMessageContaining("supports only 'parquet'");
    }
}
