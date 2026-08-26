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
package io.trino.plugin.ducklake.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.DuckLakeWriteColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * The Parquet schema of a DuckLake data file.
 * <p>
 * DuckLake files are read by matching the {@code field_id} of each Parquet field to the column
 * identifier in the catalog, so the schema mirrors the column tree exactly, including the fields
 * of nested types. The physical encodings are the ones DuckDB writes, so that the files this
 * connector produces and the ones DuckDB produces are indistinguishable to any reader.
 * <p>
 * The message name and the placement of identifiers on group types follow DuckDB: the wrapper
 * groups the Parquet list and map encodings introduce carry no identifier of their own.
 */
public final class DuckLakeParquetSchema
{
    private static final PrimitiveType.PrimitiveTypeName DOUBLE_PRIMITIVE = PrimitiveType.PrimitiveTypeName.DOUBLE;
    private static final PrimitiveType.PrimitiveTypeName BOOLEAN_PRIMITIVE = PrimitiveType.PrimitiveTypeName.BOOLEAN;

    private static final String MESSAGE_NAME = "duckdb_schema";
    private static final String LIST_GROUP_NAME = "list";
    private static final String LIST_ELEMENT_NAME = "element";
    private static final String MAP_GROUP_NAME = "key_value";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_VALUE_NAME = "value";

    private final MessageType messageType;
    private final Map<List<String>, Type> primitiveTypes;
    private final List<Type> fileColumnTypes;
    private final List<String> fileColumnNames;
    private final Map<String, LeafField> leafFieldsByPath;

    private DuckLakeParquetSchema(
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            Map<String, LeafField> leafFieldsByPath)
    {
        this.messageType = messageType;
        this.primitiveTypes = primitiveTypes;
        this.fileColumnTypes = fileColumnTypes;
        this.fileColumnNames = fileColumnNames;
        this.leafFieldsByPath = leafFieldsByPath;
    }

    public static DuckLakeParquetSchema create(List<DuckLakeWriteColumn> columns)
    {
        requireNonNull(columns, "columns is null");
        ImmutableMap.Builder<List<String>, Type> primitiveTypes = ImmutableMap.builder();
        ImmutableMap.Builder<String, LeafField> leafFields = ImmutableMap.builder();
        Types.MessageTypeBuilder message = Types.buildMessage();
        for (DuckLakeWriteColumn column : columns) {
            message.addField(convert(column, ImmutableList.of(), OPTIONAL, primitiveTypes, leafFields));
        }
        return new DuckLakeParquetSchema(
                message.named(MESSAGE_NAME),
                primitiveTypes.buildOrThrow(),
                // the writer reads a top-level column as its declared type; the physical encoding
                // it uses for each value lives on the leaf entries of the primitive type map
                columns.stream().map(DuckLakeWriteColumn::type).collect(toImmutableList()),
                columns.stream().map(DuckLakeWriteColumn::name).collect(toImmutableList()),
                leafFields.buildOrThrow());
    }

    public MessageType messageType()
    {
        return messageType;
    }

    public Map<List<String>, Type> primitiveTypes()
    {
        return primitiveTypes;
    }

    /**
     * The types the Parquet writer uses for the top-level columns. They describe the physical
     * encoding rather than the column's declared type: a {@code TIMESTAMP(0)} column, for
     * instance, is written with microsecond values like every other DuckLake timestamp.
     */
    public List<Type> fileColumnTypes()
    {
        return fileColumnTypes;
    }

    public List<String> fileColumnNames()
    {
        return fileColumnNames;
    }

    /**
     * Resolves a Parquet column chunk, identified by its path in the file schema, to the DuckLake
     * column it holds the values of.
     */
    public Optional<LeafField> leafField(List<String> pathInSchema)
    {
        return Optional.ofNullable(leafFieldsByPath.get(join(".", pathInSchema)));
    }

    private static org.apache.parquet.schema.Type convert(
            DuckLakeWriteColumn column,
            List<String> parentPath,
            org.apache.parquet.schema.Type.Repetition repetition,
            ImmutableMap.Builder<List<String>, Type> primitiveTypes,
            ImmutableMap.Builder<String, LeafField> leafFields)
    {
        List<String> path = ImmutableList.<String>builder().addAll(parentPath).add(column.name()).build();
        return switch (normalize(column.duckLakeType())) {
            case "list" -> {
                DuckLakeWriteColumn element = onlyChild(column);
                List<String> groupPath = ImmutableList.<String>builder().addAll(path).add(LIST_GROUP_NAME).build();
                yield Types.buildGroup(repetition)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(Types.repeatedGroup()
                                .addField(convert(element, groupPath, OPTIONAL, primitiveTypes, leafFields))
                                .named(LIST_GROUP_NAME))
                        .id(toIntId(column))
                        .named(column.name());
            }
            case "map" -> {
                List<DuckLakeWriteColumn> entries = column.children();
                if (entries.size() != 2) {
                    throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Map column '%s' must have a key and a value field".formatted(column.name()));
                }
                List<String> groupPath = ImmutableList.<String>builder().addAll(path).add(MAP_GROUP_NAME).build();
                yield Types.buildGroup(repetition)
                        .as(LogicalTypeAnnotation.mapType())
                        .addField(Types.repeatedGroup()
                                .addField(convert(entries.getFirst(), groupPath, REQUIRED, primitiveTypes, leafFields))
                                .addField(convert(entries.getLast(), groupPath, OPTIONAL, primitiveTypes, leafFields))
                                .named(MAP_GROUP_NAME))
                        .id(toIntId(column))
                        .named(column.name());
            }
            case "struct" -> {
                if (column.children().isEmpty()) {
                    throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Struct column '%s' must have at least one field".formatted(column.name()));
                }
                Types.GroupBuilder<GroupType> group = Types.buildGroup(repetition);
                for (DuckLakeWriteColumn field : column.children()) {
                    group.addField(convert(field, path, OPTIONAL, primitiveTypes, leafFields));
                }
                yield group.id(toIntId(column)).named(column.name());
            }
            default -> {
                WriterType writerType = writerType(column);
                primitiveTypes.put(path, writerType.type());
                leafFields.put(join(".", path), new LeafField(column.columnId(), writerType.type(), writerType.statisticsSupported()));
                yield writerType.parquetType(column.name(), repetition, toIntId(column));
            }
        };
    }

    private static DuckLakeWriteColumn onlyChild(DuckLakeWriteColumn column)
    {
        if (column.children().size() != 1) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "List column '%s' must have exactly one element field".formatted(column.name()));
        }
        return column.children().getFirst();
    }

    private static int toIntId(DuckLakeWriteColumn column)
    {
        long columnId = column.columnId();
        if (columnId <= 0 || columnId > Integer.MAX_VALUE) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Column '%s' has an identifier outside the range Parquet field ids can hold: %s".formatted(column.name(), columnId));
        }
        return (int) columnId;
    }

    /**
     * The physical encoding of a leaf column: the Parquet type it is written as and the Trino type
     * the Parquet writer interprets the block with.
     */
    private static WriterType writerType(DuckLakeWriteColumn column)
    {
        String duckLakeType = normalize(column.duckLakeType());
        Type type = column.type();
        return switch (duckLakeType) {
            case "boolean", "bool" -> primitive(BOOLEAN_PRIMITIVE, null, BOOLEAN);
            case "int8", "tinyint" -> primitive(INT32, LogicalTypeAnnotation.intType(8, true), TINYINT);
            case "int16", "smallint" -> primitive(INT32, LogicalTypeAnnotation.intType(16, true), SMALLINT);
            case "int32", "integer", "int", "int4" -> primitive(INT32, LogicalTypeAnnotation.intType(32, true), INTEGER);
            case "int64", "bigint", "long" -> primitive(INT64, LogicalTypeAnnotation.intType(64, true), BIGINT);
            // Unsigned columns are only ever created by DuckDB. Trino reads them as the next wider
            // signed type, and the writer stores that value back into the narrower physical type,
            // which is exact for every value the unsigned column can hold.
            case "uint8", "utinyint" -> primitive(INT32, LogicalTypeAnnotation.intType(8, false), SMALLINT, false);
            case "uint16", "usmallint" -> primitive(INT32, LogicalTypeAnnotation.intType(16, false), INTEGER, false);
            case "float32", "float", "real" -> primitive(FLOAT, null, REAL);
            case "float64", "double" -> primitive(DOUBLE_PRIMITIVE, null, DOUBLE);
            case "varchar", "text", "string", "json" -> primitive(BINARY, LogicalTypeAnnotation.stringType(), type);
            case "blob", "bytea", "varbinary", "binary" -> primitive(BINARY, null, VARBINARY);
            case "uuid" -> fixedLength(16, LogicalTypeAnnotation.uuidType(), UUID);
            case "date" -> primitive(INT32, LogicalTypeAnnotation.dateType(), DATE);
            case "time" -> primitive(INT64, LogicalTypeAnnotation.timeType(false, MICROS), TIME_MICROS);
            // DuckDB writes second-precision timestamps as microseconds, and Trino's second
            // precision timestamps already hold microseconds, so the value passes through.
            case "timestamp_s" -> primitive(INT64, LogicalTypeAnnotation.timestampType(false, MICROS), TIMESTAMP_MICROS);
            case "timestamp_ms" -> primitive(INT64, LogicalTypeAnnotation.timestampType(false, MILLIS), TIMESTAMP_MILLIS);
            case "timestamp", "timestamp_us", "datetime" -> primitive(INT64, LogicalTypeAnnotation.timestampType(false, MICROS), TIMESTAMP_MICROS);
            case "timestamp_ns" -> primitive(INT64, LogicalTypeAnnotation.timestampType(false, NANOS), TIMESTAMP_NANOS);
            case "timestamptz", "timestamp with time zone" -> timestampWithTimeZone(type);
            default -> {
                if (duckLakeType.startsWith("decimal(") || duckLakeType.startsWith("numeric(")) {
                    yield decimal(type);
                }
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Writing to a column of DuckLake type '%s' is not supported".formatted(column.duckLakeType()));
            }
        };
    }

    private static WriterType timestampWithTimeZone(Type type)
    {
        if (!(type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType)) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Expected a timestamp with time zone, found " + type);
        }
        // Trino carries millisecond precision values in a different representation from the wider
        // ones, so the file records the precision the values actually have. Readers widen it to
        // the microsecond precision the DuckLake type declares.
        if (timestampWithTimeZoneType.getPrecision() <= 3) {
            return primitive(INT64, LogicalTypeAnnotation.timestampType(true, MILLIS), TIMESTAMP_TZ_MILLIS);
        }
        if (timestampWithTimeZoneType.getPrecision() <= 6) {
            return primitive(INT64, LogicalTypeAnnotation.timestampType(true, MICROS), TIMESTAMP_TZ_MICROS);
        }
        return primitive(INT64, LogicalTypeAnnotation.timestampType(true, NANOS), TIMESTAMP_TZ_NANOS);
    }

    private static WriterType decimal(Type type)
    {
        if (!(type instanceof DecimalType decimalType)) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Expected a decimal, found " + type);
        }
        LogicalTypeAnnotation annotation = LogicalTypeAnnotation.decimalType(decimalType.getScale(), decimalType.getPrecision());
        if (decimalType.getPrecision() <= 9) {
            return primitive(INT32, annotation, decimalType);
        }
        if (decimalType.getPrecision() <= 18) {
            return primitive(INT64, annotation, decimalType);
        }
        return fixedLength(16, annotation, decimalType);
    }

    private static WriterType primitive(PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation annotation, Type type)
    {
        return primitive(primitiveTypeName, annotation, type, true);
    }

    private static WriterType primitive(PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation annotation, Type type, boolean statisticsSupported)
    {
        return new WriterType(type, statisticsSupported, (name, repetition, id) -> {
            Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(primitiveTypeName, repetition);
            if (annotation != null) {
                builder = builder.as(annotation);
            }
            return builder.id(id).named(name);
        });
    }

    private static WriterType fixedLength(int length, LogicalTypeAnnotation annotation, Type type)
    {
        return new WriterType(type, true, (name, repetition, id) -> Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                .length(length)
                .as(annotation)
                .id(id)
                .named(name));
    }

    private static String normalize(String duckLakeType)
    {
        return duckLakeType.trim().toLowerCase(Locale.ENGLISH);
    }

    /**
     * A column of the file that holds values, with the DuckLake column its statistics belong to.
     */
    public record LeafField(long columnId, Type type, boolean statisticsSupported)
    {
        public LeafField
        {
            requireNonNull(type, "type is null");
        }
    }

    private record WriterType(Type type, boolean statisticsSupported, ParquetTypeFactory parquetTypeFactory)
    {
        org.apache.parquet.schema.Type parquetType(String name, org.apache.parquet.schema.Type.Repetition repetition, int id)
        {
            return parquetTypeFactory.create(name, repetition, id);
        }
    }

    @FunctionalInterface
    private interface ParquetTypeFactory
    {
        org.apache.parquet.schema.Type create(String name, org.apache.parquet.schema.Type.Repetition repetition, int id);
    }

    /**
     * Names DuckDB gives the fields of nested types, so that the catalog rows describing them
     * stay consistent with the Parquet schema the files are written with.
     */
    public static String listElementName()
    {
        return LIST_ELEMENT_NAME;
    }

    public static String mapKeyName()
    {
        return MAP_KEY_NAME;
    }

    public static String mapValueName()
    {
        return MAP_VALUE_NAME;
    }
}
