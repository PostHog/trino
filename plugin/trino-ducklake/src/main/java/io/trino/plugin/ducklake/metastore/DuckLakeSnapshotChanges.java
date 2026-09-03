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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_CHANGE_TYPE;
import static java.util.Objects.requireNonNull;

/**
 * What one or more snapshots changed, read from {@code ducklake_snapshot_changes}.
 * <p>
 * DuckLake decides whether two commits may coexist from these records alone: a commit is allowed
 * to land on top of another one unless the two touched the same object. The vocabulary is shared
 * with DuckDB, which writes the same strings and applies the same rules, so a statement is
 * accepted or rejected here exactly as it would be there.
 * <p>
 * Only the changes keyed by object identifier are kept. The ones keyed by name — creating a
 * schema, table or view — need no record, because a commit that creates something by name
 * re-resolves that name against the newer catalog when it is replayed, and fails there if the
 * name has been taken or its schema has gone.
 */
public final class DuckLakeSnapshotChanges
{
    private final Set<Long> droppedRelations;
    private final Set<Long> alteredRelations;
    private final Set<Long> insertedIntoTables;
    private final Set<Long> deletedFromTables;
    private final Set<Long> compactedTables;

    /**
     * Reads the changes from the comma-separated form DuckLake stores them in.
     * <p>
     * Two things can go wrong, and they mean different things to whoever reads the error. A change
     * type this connector does not know is a catalog written by a newer DuckLake than this
     * connector understands, and it fails with {@code DUCKLAKE_UNSUPPORTED_CHANGE_TYPE}: the
     * connector cannot tell whether committing on top of it is safe, and ignoring it could drop the
     * other writer's work. An entry that is not shaped like a change at all is a corrupt row, and
     * fails as invalid metadata. Neither is retried, because attempting the commit again reads the
     * same row and reaches the same conclusion.
     */
    public static DuckLakeSnapshotChanges parse(String changesMade)
    {
        Builder builder = new Builder();
        for (String change : splitChanges(changesMade)) {
            int separator = change.indexOf(':');
            if (separator < 0) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Malformed DuckLake change entry: " + change);
            }
            builder.add(change.substring(0, separator), change.substring(separator + 1));
        }
        return builder.build();
    }

    /**
     * Splits on the commas separating entries, which are the ones outside a quoted name. A created
     * schema, table or view is recorded under its quoted name, and a name may hold a comma.
     */
    private static List<String> splitChanges(String changesMade)
    {
        List<String> changes = new ArrayList<>();
        boolean quoted = false;
        int start = 0;
        for (int i = 0; i < changesMade.length(); i++) {
            char character = changesMade.charAt(i);
            if (character == '"') {
                quoted = !quoted;
            }
            else if (character == ',' && !quoted) {
                changes.add(changesMade.substring(start, i));
                start = i + 1;
            }
        }
        if (start < changesMade.length()) {
            changes.add(changesMade.substring(start));
        }
        return changes;
    }

    private DuckLakeSnapshotChanges(
            Set<Long> droppedRelations,
            Set<Long> alteredRelations,
            Set<Long> insertedIntoTables,
            Set<Long> deletedFromTables,
            Set<Long> compactedTables)
    {
        this.droppedRelations = requireNonNull(droppedRelations, "droppedRelations is null");
        this.alteredRelations = requireNonNull(alteredRelations, "alteredRelations is null");
        this.insertedIntoTables = requireNonNull(insertedIntoTables, "insertedIntoTables is null");
        this.deletedFromTables = requireNonNull(deletedFromTables, "deletedFromTables is null");
        this.compactedTables = requireNonNull(compactedTables, "compactedTables is null");
    }

    public boolean isEmpty()
    {
        return droppedRelations.isEmpty()
                && alteredRelations.isEmpty()
                && insertedIntoTables.isEmpty()
                && deletedFromTables.isEmpty()
                && compactedTables.isEmpty();
    }

    /**
     * Describes the first reason these changes cannot land on top of {@code other}, or an empty
     * value when they can.
     * <p>
     * The rules are DuckLake's. Two commits that touched different tables never conflict, and
     * neither do two inserts into the same table: rows added by one are simply visible beside the
     * rows added by the other. What conflicts is a commit whose result depends on the state
     * another commit changed — inserting into a table whose columns were altered, deleting rows
     * another writer already removed or rewrote, altering a table that was dropped.
     */
    public Optional<String> conflictWith(DuckLakeSnapshotChanges other)
    {
        return firstConflict(droppedRelations, other.droppedRelations, "dropped", "dropped it")
                .or(() -> firstConflict(insertedIntoTables, other.droppedRelations, "inserted into", "dropped it"))
                .or(() -> firstConflict(insertedIntoTables, other.alteredRelations, "inserted into", "altered it"))
                .or(() -> firstConflict(insertedIntoTables, other.deletedFromTables, "inserted into", "deleted from it"))
                .or(() -> firstConflict(deletedFromTables, other.droppedRelations, "deleted from", "dropped it"))
                .or(() -> firstConflict(deletedFromTables, other.alteredRelations, "deleted from", "altered it"))
                .or(() -> firstConflict(deletedFromTables, other.insertedIntoTables, "deleted from", "inserted into it"))
                .or(() -> firstConflict(deletedFromTables, other.compactedTables, "deleted from", "compacted it"))
                .or(() -> firstConflict(alteredRelations, other.droppedRelations, "altered", "dropped it"))
                .or(() -> firstConflict(alteredRelations, other.alteredRelations, "altered", "altered it"));
    }

    private static Optional<String> firstConflict(Set<Long> mine, Set<Long> theirs, String action, String otherAction)
    {
        return intersection(mine, theirs).stream()
                .min(Long::compare)
                .map(objectId -> "this statement %s object %s, but another transaction %s".formatted(action, objectId, otherAction));
    }

    private static final class Builder
    {
        private final ImmutableSet.Builder<Long> droppedRelations = ImmutableSet.builder();
        private final ImmutableSet.Builder<Long> alteredRelations = ImmutableSet.builder();
        private final ImmutableSet.Builder<Long> insertedIntoTables = ImmutableSet.builder();
        private final ImmutableSet.Builder<Long> deletedFromTables = ImmutableSet.builder();
        private final ImmutableSet.Builder<Long> compactedTables = ImmutableSet.builder();

        void add(String type, String value)
        {
            switch (type) {
                // Tables and views are numbered from one counter, so an identifier names exactly
                // one of them. Keeping them in one set makes a rule hold however the writer
                // spelled the change, which DuckDB and this connector do not always do alike.
                case "dropped_table", "dropped_view" -> droppedRelations.add(objectId(value));
                case "altered_table", "altered_view" -> alteredRelations.add(objectId(value));
                // Inlined rows live in the catalog database rather than a data file, but they are
                // rows of the table either way and conflict with the same statements.
                case "inserted_into_table", "inlined_insert" -> insertedIntoTables.add(objectId(value));
                case "deleted_from_table", "inlined_delete" -> deletedFromTables.add(objectId(value));
                case "compacted_table", "merge_adjacent", "rewrite_delete" -> compactedTables.add(objectId(value));
                // Creating something by name, dropping a schema, and flushing inlined rows into a
                // data file are recorded but need no rule here; see the class comment for creates,
                // and a flush adds a file without touching the ones a statement already read.
                case "created_schema", "created_table", "created_view", "created_scalar_macro", "created_table_macro",
                     "dropped_schema", "dropped_scalar_macro", "dropped_table_macro",
                     "flushed_inlined", "inline_flush" -> {}
                default -> throw new TrinoException(
                        DUCKLAKE_UNSUPPORTED_CHANGE_TYPE,
                        ("Another writer recorded the DuckLake change type '%s', which this connector does not understand, " +
                                "so it cannot tell whether committing on top of that snapshot is safe. " +
                                "Upgrade the DuckLake connector to a version that knows this change type.").formatted(type));
            }
        }

        private static long objectId(String value)
        {
            try {
                return Long.parseLong(value);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Malformed DuckLake object identifier: " + value, e);
            }
        }

        DuckLakeSnapshotChanges build()
        {
            return new DuckLakeSnapshotChanges(
                    droppedRelations.build(),
                    alteredRelations.build(),
                    insertedIntoTables.build(),
                    deletedFromTables.build(),
                    compactedTables.build());
        }
    }
}
