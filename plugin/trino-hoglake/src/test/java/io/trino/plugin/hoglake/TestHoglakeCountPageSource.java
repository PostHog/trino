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

import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_INVALID;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeCountPageSource
{
    private final HoglakePageSourceProvider provider = new HoglakePageSourceProvider(_ -> {
        throw new AssertionError("Object storage accessed");
    });

    @Test
    void testEmptyAndLargeCounts()
            throws IOException
    {
        for (long count : new long[] {0, 17, Integer.MAX_VALUE + 123L}) {
            try (ConnectorPageSource source = createPageSource(count, Optional.empty(), List.of())) {
                long rows = 0;
                while (!source.isFinished()) {
                    SourcePage page = source.getNextSourcePage();
                    assertThat(page.getChannelCount()).isZero();
                    assertThat(page.getPositionCount()).isPositive();
                    rows += page.getPositionCount();
                }
                assertThat(rows).isEqualTo(count);
                assertThat(source.getNextSourcePage()).isNull();
                assertThat(source.getCompletedBytes()).isZero();
                assertThat(source.getReadTimeNanos()).isZero();
            }
        }
    }

    @Test
    void testClose()
    {
        HoglakeCountPageSource source = new HoglakeCountPageSource(Long.MAX_VALUE);
        assertThat(source.isFinished()).isFalse();
        source.close();
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextSourcePage()).isNull();
    }

    /**
     * The catalog-only count path still has to account for deletes: a file's
     * physical record count is not its visible row count once a deletion
     * vector exists.
     */
    @Test
    void testCountsSubtractDeletedRows()
            throws IOException
    {
        HoglakePageSourceProvider provider = providerWithVector("memory:///counts.dv", PuffinDeletionVectorFixtures.deletionVector("s3://test-bucket/counts.parquet", 1L, 2L, 3L, 4L));

        try (ConnectorPageSource source = createPageSource(provider, 17, Optional.of("memory:///counts.dv"), 4, List.of())) {
            long rows = 0;
            while (!source.isFinished()) {
                rows += source.getNextSourcePage().getPositionCount();
            }
            assertThat(rows).isEqualTo(13);
        }
    }

    @Test
    void testCountWithEmptyDeletionVectorIsUnchanged()
            throws IOException
    {
        HoglakePageSourceProvider provider = providerWithVector("memory:///counts.dv", PuffinDeletionVectorFixtures.deletionVector("s3://test-bucket/counts.parquet"));

        try (ConnectorPageSource source = createPageSource(provider, 17, Optional.of("memory:///counts.dv"), 0, List.of())) {
            long rows = 0;
            while (!source.isFinished()) {
                rows += source.getNextSourcePage().getPositionCount();
            }
            assertThat(rows).isEqualTo(17);
        }
    }

    @Test
    void testCountRequiresObjectStorageOnlyImageWhenTheVectorIsMissing()
    {
        // The count-only path touches object storage exactly because the
        // file has a deletion vector it must read and validate.
        assertThatThrownBy(() -> createPageSource(17, Optional.of("memory:///counts.dv"), List.of()))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Object storage accessed");
    }

    @Test
    void testCountFailsWhenTheDeletionVectorIsMissing()
    {
        HoglakePageSourceProvider provider = providerWithVector("memory:///other.dv", PuffinDeletionVectorFixtures.deletionVector());

        assertThatThrownBy(() -> createPageSource(provider, 17, Optional.of("memory:///counts.dv"), 0, List.of()))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()))
                .hasMessageContaining("memory:///counts.dv");
    }

    @Test
    void testCountFailsWhenTheDeletionVectorDisagreesWithTheCatalog()
    {
        HoglakePageSourceProvider provider = providerWithVector("memory:///counts.dv", PuffinDeletionVectorFixtures.deletionVector("s3://test-bucket/counts.parquet", 1L, 2L, 3L, 4L));

        // The catalog says one row is deleted; the vector deletes four.
        assertThatThrownBy(() -> createPageSource(provider, 17, Optional.of("memory:///counts.dv"), 1, List.of()))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("the catalog reports 1 deleted rows but the vector deletes 4");
    }

    @Test
    void testCountFailsWhenTheDeletionVectorIsCorrupt()
    {
        byte[] corrupt = PuffinDeletionVectorFixtures.deletionVector("s3://test-bucket/counts.parquet", 1L);
        corrupt[20] = (byte) (corrupt[20] ^ 0x40);
        HoglakePageSourceProvider provider = providerWithVector("memory:///counts.dv", corrupt);

        assertThatThrownBy(() -> createPageSource(provider, 17, Optional.of("memory:///counts.dv"), 1, List.of()))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()));
    }

    private static HoglakePageSourceProvider providerWithVector(String path, byte[] vector)
    {
        return new HoglakePageSourceProvider(ConnectorTestFixtures.memoryFileSystem(Map.of(path, vector)));
    }

    @Test
    void testUnknownCountAndProjectedColumnsRequireStorage()
    {
        assertThatThrownBy(() -> createPageSource(-1, Optional.empty(), List.of()))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Object storage accessed");
        assertThatThrownBy(() -> createPageSource(17, Optional.empty(), List.of(new HoglakeColumnHandle("value", 1, BIGINT, true))))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Object storage accessed");
    }

    @Test
    void testPredicateWithNoProjectedColumnsRequiresStorage()
    {
        HoglakeColumnHandle column = new HoglakeColumnHandle("value", 1, BIGINT, true);
        TupleDomain<HoglakeColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(BIGINT, 1L)));
        assertThatThrownBy(() -> createPageSource(17, Optional.empty(), List.of(), predicate))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Object storage accessed");
    }

    @Test
    void testNonePredicateProducesNoRowsWithoutStorage()
            throws IOException
    {
        try (ConnectorPageSource source = createPageSource(17, Optional.empty(), List.of(), TupleDomain.none())) {
            assertThat(source.isFinished()).isTrue();
            assertThat(source.getCompletedBytes()).isZero();
        }
    }

    private ConnectorPageSource createPageSource(long count, Optional<String> deletePath, List<ColumnHandle> columns)
    {
        return createPageSource(provider, count, deletePath, 0, columns, TupleDomain.all());
    }

    private ConnectorPageSource createPageSource(HoglakePageSourceProvider provider, long count, Optional<String> deletePath, long deleteCount, List<ColumnHandle> columns)
    {
        return createPageSource(provider, count, deletePath, deleteCount, columns, TupleDomain.all());
    }

    private ConnectorPageSource createPageSource(long count, Optional<String> deletePath, List<ColumnHandle> columns, TupleDomain<HoglakeColumnHandle> predicate)
    {
        return createPageSource(provider, count, deletePath, 0, columns, predicate);
    }

    private ConnectorPageSource createPageSource(HoglakePageSourceProvider provider, long count, Optional<String> deletePath, long deleteCount, List<ColumnHandle> columns, TupleDomain<HoglakeColumnHandle> predicate)
    {
        return provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                new HoglakeSplit("s3://test-bucket/counts.parquet", 100, count, deletePath, deleteCount, Optional.of("puffin-dv")),
                new HoglakeTableHandle("test", "counts", 7, "synthetic-table", List.of(), predicate),
                Optional.empty(),
                columns,
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);
    }
}
