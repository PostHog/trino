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
package io.trino.hogql;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public record HogQlPhysicalCatalog(
        int protocolVersion,
        int schemaVersion,
        Identifier catalog,
        String catalogHandleVersion,
        List<Table> tables)
{
    public static final int PROTOCOL_VERSION = 1;
    public static final int SCHEMA_VERSION = 1;

    private static final Pattern UNDELIMITED_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public HogQlPhysicalCatalog
    {
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported protocolVersion");
        }
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schemaVersion");
        }
        requireNonNull(catalog, "catalog is null");
        requireNonNull(catalogHandleVersion, "catalogHandleVersion is null");
        tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
    }

    public static Identifier identifier(String value)
    {
        requireNonNull(value, "value is null");
        boolean delimited = !UNDELIMITED_IDENTIFIER.matcher(value).matches() || !value.equals(value.toLowerCase(Locale.ENGLISH));
        return new Identifier(value, delimited);
    }

    public record Identifier(String value, boolean delimited)
    {
        public Identifier
        {
            requireNonNull(value, "value is null");
        }
    }

    public record Table(Identifier schema, Identifier table, List<Column> columns)
    {
        public Table
        {
            requireNonNull(schema, "schema is null");
            requireNonNull(table, "table is null");
            columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        }
    }

    public record Column(
            Identifier name,
            int ordinal,
            String type,
            boolean nullable,
            boolean hidden,
            boolean starVisible)
    {
        public Column
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            if (ordinal < 1) {
                throw new IllegalArgumentException("ordinal must be positive");
            }
            if (hidden == starVisible) {
                throw new IllegalArgumentException("hidden and starVisible must be opposites");
            }
        }
    }
}
