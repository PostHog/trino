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
package io.trino.plugin.ducklake.metastore;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_CHANGE_TYPE;
import static io.trino.plugin.ducklake.metastore.DuckLakeSnapshotChanges.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rules deciding whether a commit may land on top of another one. They are DuckLake's, so each
 * case here is one a DuckDB writer against the same catalog would decide the same way.
 */
final class TestDuckLakeSnapshotChanges
{
    @Test
    void testUnrelatedTablesNeverConflict()
    {
        assertThat(parse("inserted_into_table:1").conflictWith(parse("altered_table:2,dropped_table:3,deleted_from_table:4"))).isEmpty();
        assertThat(parse("deleted_from_table:1").conflictWith(parse("inserted_into_table:2,merge_adjacent:3"))).isEmpty();
        assertThat(parse("altered_table:1").conflictWith(parse("altered_table:2"))).isEmpty();
    }

    @Test
    void testConcurrentInsertsIntoOneTableAreAllowed()
    {
        // both writers only add rows, and the rows of one are visible beside the rows of the other
        assertThat(parse("inserted_into_table:1").conflictWith(parse("inserted_into_table:1"))).isEmpty();
        assertThat(parse("inserted_into_table:1").conflictWith(parse("inlined_insert:1"))).isEmpty();
    }

    @Test
    void testInsertConflictsWithChangesToTheTableItWrites()
    {
        // the data files were written against the columns the table had, so an alter invalidates them
        assertThat(parse("inserted_into_table:1").conflictWith(parse("altered_table:1")))
                .contains("this statement inserted into object 1, but another transaction altered it");
        assertThat(parse("inserted_into_table:1").conflictWith(parse("dropped_table:1")))
                .contains("this statement inserted into object 1, but another transaction dropped it");
        assertThat(parse("inserted_into_table:1").conflictWith(parse("deleted_from_table:1")))
                .contains("this statement inserted into object 1, but another transaction deleted from it");
        assertThat(parse("inserted_into_table:1").conflictWith(parse("inlined_delete:1")))
                .contains("this statement inserted into object 1, but another transaction deleted from it");
    }

    @Test
    void testDeleteConflictsWithChangesToTheTableItRewrites()
    {
        assertThat(parse("deleted_from_table:1").conflictWith(parse("dropped_table:1"))).isPresent();
        assertThat(parse("deleted_from_table:1").conflictWith(parse("altered_table:1"))).isPresent();
        assertThat(parse("deleted_from_table:1").conflictWith(parse("inserted_into_table:1")))
                .contains("this statement deleted from object 1, but another transaction inserted into it");
        assertThat(parse("deleted_from_table:1").conflictWith(parse("merge_adjacent:1")))
                .contains("this statement deleted from object 1, but another transaction compacted it");
        assertThat(parse("deleted_from_table:1").conflictWith(parse("rewrite_delete:1"))).isPresent();
        assertThat(parse("deleted_from_table:1").conflictWith(parse("compacted_table:1"))).isPresent();
    }

    @Test
    void testAlterAndDropConflictWithThemselves()
    {
        assertThat(parse("altered_table:1").conflictWith(parse("altered_table:1"))).isPresent();
        assertThat(parse("altered_table:1").conflictWith(parse("dropped_table:1"))).isPresent();
        assertThat(parse("dropped_table:1").conflictWith(parse("dropped_table:1")))
                .contains("this statement dropped object 1, but another transaction dropped it");
        // a view carries an identifier from the same counter, whichever word the writer used for it
        assertThat(parse("altered_table:1").conflictWith(parse("dropped_view:1"))).isPresent();
        assertThat(parse("dropped_view:1").conflictWith(parse("dropped_table:1"))).isPresent();
        assertThat(parse("altered_view:1").conflictWith(parse("altered_table:1"))).isPresent();

        // dropping something another writer altered is not a conflict: the drop removes it either way
        assertThat(parse("dropped_table:1").conflictWith(parse("altered_table:1"))).isEmpty();
    }

    /**
     * Creating a schema, table or view is recorded by name. A commit that creates one re-resolves
     * the name when it is replayed against the newer catalog and fails there, so these changes
     * carry no rule of their own.
     */
    @Test
    void testChangesRecordedByNameCarryNoRule()
    {
        assertThat(parse("created_table:\"main\".\"orders\"").isEmpty()).isTrue();
        assertThat(parse("created_schema:\"main\",dropped_schema:7").isEmpty()).isTrue();
        assertThat(parse("created_view:\"main\".\"v\"").conflictWith(parse("created_view:\"main\".\"v\""))).isEmpty();
    }

    @Test
    void testFlushingInlinedRowsCarriesNoRule()
    {
        // the flush writes a new data file; it changes none of the files a statement read
        assertThat(parse("inserted_into_table:1").conflictWith(parse("flushed_inlined:1"))).isEmpty();
        assertThat(parse("deleted_from_table:1").conflictWith(parse("inline_flush:1"))).isEmpty();
    }

    @Test
    void testNamesHoldingACommaAreOneEntry()
    {
        assertThat(parse("created_table:\"main\".\"a,b\",inserted_into_table:1").conflictWith(parse("altered_table:1"))).isPresent();
    }

    @Test
    void testEmptyChanges()
    {
        assertThat(parse("").isEmpty()).isTrue();
        assertThat(parse("inserted_into_table:1").conflictWith(parse(""))).isEmpty();
    }

    /**
     * A change type this connector does not know was written by a newer DuckLake version. Ignoring
     * it could drop the work of the writer that made it, so it is rejected instead, and named so
     * that the remedy is obvious.
     */
    @Test
    void testUnknownChangeTypeIsRejected()
    {
        assertThatThrownBy(() -> parse("teleported_table:1"))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_UNSUPPORTED_CHANGE_TYPE.toErrorCode()))
                .hasMessageContaining("recorded the DuckLake change type 'teleported_table'")
                .hasMessageContaining("Upgrade the DuckLake connector");
    }

    /**
     * A row that is not shaped like a change at all is corruption rather than a newer writer, and
     * says so with a different error code.
     */
    @Test
    void testCorruptChangeEntryIsInvalidMetadata()
    {
        assertThatThrownBy(() -> parse("dropped_table:not_a_number"))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_INVALID_METADATA.toErrorCode()))
                .hasMessageContaining("Malformed DuckLake object identifier: not_a_number");
        assertThatThrownBy(() -> parse("dropped_table"))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_INVALID_METADATA.toErrorCode()))
                .hasMessageContaining("Malformed DuckLake change entry: dropped_table");
    }
}
