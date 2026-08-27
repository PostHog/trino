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
package io.trino.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.trino.hogql.compiler.HogQlCompilationResult;
import io.trino.hogql.compiler.HogQlTypedValue;
import io.trino.hogql.compiler.HogQlTypedValue.ArrayValue;
import io.trino.hogql.compiler.HogQlTypedValue.BooleanValue;
import io.trino.hogql.compiler.HogQlTypedValue.NullValue;
import io.trino.hogql.compiler.HogQlTypedValue.NumberValue;
import io.trino.hogql.compiler.HogQlTypedValue.ObjectValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.compiler.HogQlTypedValue.Value;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TypeParameter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_BINDING_ERROR;
import static io.trino.spi.type.CharType.MAX_LENGTH;
import static io.trino.type.DateTimes.extractTimePrecision;
import static io.trino.type.DateTimes.extractTimestampPrecision;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.DateTimes.timeHasTimeZone;
import static io.trino.type.DateTimes.timestampHasTimeZone;
import static io.trino.util.DateTimeUtils.parseDate;
import static java.lang.Float.isFinite;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class HogQlParameterDecoder
{
    private static final Set<String> INTEGRAL_TYPES = Set.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT");
    private static final Set<String> STRING_TYPES = Set.of("VARCHAR", "CHAR", "DATE", "UUID", "IPADDRESS", "VARBINARY");

    private final SqlParser sqlParser;

    HogQlParameterDecoder(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    List<Expression> decode(HogQlCompilationResult result, Map<String, HogQlTypedValue> bindings)
    {
        requireNonNull(result, "result is null");
        requireNonNull(bindings, "bindings is null");
        List<Parameter> parameters = ParameterExtractor.extractParameters(result.statement());
        if (parameters.size() != result.parameterNames().size()) {
            throw new IllegalStateException("HogQL compiler returned inconsistent parameter metadata");
        }

        ImmutableList.Builder<Expression> values = ImmutableList.builderWithExpectedSize(parameters.size());
        for (int index = 0; index < parameters.size(); index++) {
            String name = result.parameterNames().get(index);
            NodeLocation location = parameters.get(index).getLocation().orElseThrow();
            try {
                HogQlTypedValue binding = requireNonNull(bindings.get(name), "binding is null");
                DataType type = sqlParser.createType(binding.type());
                values.add(decode(binding.value(), type, location));
            }
            catch (RuntimeException _) {
                throw bindingError(name, location);
            }
        }
        return values.build();
    }

    private static Expression decode(Value value, DataType type, NodeLocation location)
    {
        if (value instanceof NullValue) {
            validateSupportedType(type);
            return new Cast(location, new NullLiteral(location), type);
        }
        if (type instanceof DateTimeDataType dateTimeType) {
            return new Cast(location, decodeDateTime(value, dateTimeType, location), type);
        }
        if (type instanceof RowDataType rowType) {
            return decodeRow(value, rowType, location);
        }
        if (!(type instanceof GenericDataType genericType)) {
            throw new IllegalArgumentException("unsupported type");
        }

        String typeName = genericType.getName().getCanonicalValue();
        if (typeName.equals("JSON")) {
            requireNoArguments(genericType);
            return new GenericLiteral(location, "JSON", toJson(value).toString());
        }
        if (typeName.equals("ARRAY")) {
            return decodeArray(value, genericType, location);
        }
        if (typeName.equals("MAP")) {
            return decodeMap(value, genericType, location);
        }

        Expression scalar = switch (value) {
            case BooleanValue booleanValue -> decodeBoolean(booleanValue, genericType, location);
            case NumberValue numberValue -> decodeNumber(numberValue, genericType, location);
            case StringValue stringValue -> decodeString(stringValue, genericType, location);
            case ArrayValue _, ObjectValue _ -> throw new IllegalArgumentException("container value requires a container type");
            case NullValue _ -> throw new IllegalStateException("null handled above");
        };
        return new Cast(location, scalar, type);
    }

    private static Expression decodeBoolean(BooleanValue value, GenericDataType type, NodeLocation location)
    {
        requireType(type, "BOOLEAN");
        return new BooleanLiteral(location, Boolean.toString(value.value()));
    }

    private static Expression decodeNumber(NumberValue value, GenericDataType type, NodeLocation location)
    {
        String typeName = type.getName().getCanonicalValue();
        BigDecimal number = new BigDecimal(value.value());
        if (INTEGRAL_TYPES.contains(typeName)) {
            requireNoArguments(type);
            BigInteger integer = number.toBigIntegerExact();
            validateIntegerRange(typeName, integer);
            return new LongLiteral(location, integer.toString());
        }
        if (typeName.equals("REAL") || typeName.equals("DOUBLE")) {
            requireNoArguments(type);
            double doubleValue = number.doubleValue();
            if (!Double.isFinite(doubleValue) || (typeName.equals("REAL") && !isFinite(number.floatValue()))) {
                throw new IllegalArgumentException("floating-point value is out of range");
            }
            return new DoubleLiteral(location, value.value());
        }
        if (typeName.equals("DECIMAL")) {
            List<Long> arguments = validateDecimalType(type);
            int precision = toIntExact(arguments.get(0));
            int scale = toIntExact(arguments.get(1));
            BigDecimal scaled = number.setScale(scale, RoundingMode.UNNECESSARY);
            if (scaled.precision() > precision) {
                throw new IllegalArgumentException("decimal value is out of range");
            }
            return new DecimalLiteral(location, value.value());
        }
        throw new IllegalArgumentException("number value has an incompatible type");
    }

    private static Expression decodeString(StringValue value, GenericDataType type, NodeLocation location)
    {
        String typeName = type.getName().getCanonicalValue();
        if (!STRING_TYPES.contains(typeName)) {
            throw new IllegalArgumentException("string value has an incompatible type");
        }

        return switch (typeName) {
            case "VARCHAR" -> {
                validateStringLength(value.value(), type, false);
                yield new StringLiteral(location, value.value());
            }
            case "CHAR" -> {
                validateStringLength(value.value(), type, true);
                yield new StringLiteral(location, value.value());
            }
            case "DATE" -> {
                requireNoArguments(type);
                parseDate(utf8Slice(value.value()));
                yield new GenericLiteral(location, "DATE", value.value());
            }
            case "UUID" -> {
                requireNoArguments(type);
                UUID.fromString(value.value());
                if (value.value().length() != 36) {
                    throw new IllegalArgumentException("UUID value is not canonical");
                }
                yield new GenericLiteral(location, "UUID", value.value());
            }
            case "IPADDRESS" -> {
                requireNoArguments(type);
                InetAddresses.forString(value.value());
                yield new GenericLiteral(location, "IPADDRESS", value.value());
            }
            case "VARBINARY" -> {
                requireNoArguments(type);
                yield new BinaryLiteral(location, value.value());
            }
            default -> throw new IllegalArgumentException("unsupported string type");
        };
    }

    private static Expression decodeDateTime(Value value, DateTimeDataType type, NodeLocation location)
    {
        if (!(value instanceof StringValue stringValue)) {
            throw new IllegalArgumentException("date-time value must be a string");
        }
        validateDateTimePrecision(type);
        String literal = stringValue.value();
        if (type.getType() == DateTimeDataType.Type.TIMESTAMP) {
            int precision = extractTimestampPrecision(literal);
            validateLiteralPrecision(precision);
            if (timestampHasTimeZone(literal) != type.isWithTimeZone()) {
                throw new IllegalArgumentException("timestamp time zone does not match type");
            }
            if (type.isWithTimeZone()) {
                parseTimestampWithTimeZone(precision, literal);
            }
            else {
                parseTimestamp(precision, literal);
            }
        }
        else {
            int precision = extractTimePrecision(literal);
            validateLiteralPrecision(precision);
            if (timeHasTimeZone(literal) != type.isWithTimeZone()) {
                throw new IllegalArgumentException("time zone does not match type");
            }
            if (type.isWithTimeZone()) {
                parseTimeWithTimeZone(precision, literal);
            }
            else {
                parseTime(literal);
            }
        }
        return new GenericLiteral(location, type.getType().name(), literal);
    }

    private static Expression decodeArray(Value value, GenericDataType type, NodeLocation location)
    {
        if (!(value instanceof ArrayValue arrayValue)) {
            throw new IllegalArgumentException("array value is required");
        }
        DataType elementType = typeArgument(type, 0, 1);
        List<Expression> elements = arrayValue.value().stream()
                .map(element -> decode(element, elementType, location))
                .toList();
        return new Cast(location, new Array(location, elements), type);
    }

    private static Expression decodeMap(Value value, GenericDataType type, NodeLocation location)
    {
        if (!(value instanceof ObjectValue objectValue)) {
            throw new IllegalArgumentException("object value is required");
        }
        DataType keyType = typeArgument(type, 0, 2);
        DataType valueType = typeArgument(type, 1, 2);
        if (!(keyType instanceof GenericDataType genericKeyType) || !genericKeyType.getName().getCanonicalValue().equals("VARCHAR")) {
            throw new IllegalArgumentException("object keys require varchar map keys");
        }
        validateStringType(genericKeyType, false);

        List<Map.Entry<String, Value>> entries = objectValue.value().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();
        FunctionCall map = entries.isEmpty()
                ? new FunctionCall(location, QualifiedName.of("map"), List.of())
                : new FunctionCall(
                location,
                QualifiedName.of("map"),
                List.of(
                        new Array(location, entries.stream()
                                            .map(entry -> decode(new StringValue(entry.getKey()), keyType, location))
                                            .toList()),
                        new Array(location, entries.stream()
                                            .map(entry -> decode(entry.getValue(), valueType, location))
                                            .toList())));
        return new Cast(location, map, type);
    }

    private static Expression decodeRow(Value value, RowDataType type, NodeLocation location)
    {
        List<Value> fieldValues;
        if (value instanceof ObjectValue objectValue) {
            if (type.getFields().stream().anyMatch(field -> field.getName().isEmpty())) {
                throw new IllegalArgumentException("object values require named row fields");
            }
            Set<String> fieldNames = type.getFields().stream()
                    .map(field -> field.getName().orElseThrow().getValue())
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            if (!fieldNames.equals(objectValue.value().keySet())) {
                throw new IllegalArgumentException("row fields do not match");
            }
            fieldValues = type.getFields().stream()
                    .map(field -> objectValue.value().get(field.getName().orElseThrow().getValue()))
                    .toList();
        }
        else if (value instanceof ArrayValue arrayValue && arrayValue.value().size() == type.getFields().size()) {
            fieldValues = arrayValue.value();
        }
        else {
            throw new IllegalArgumentException("row value has an incompatible shape");
        }

        List<Row.Field> fields = new ArrayList<>(fieldValues.size());
        for (int index = 0; index < fieldValues.size(); index++) {
            fields.add(new Row.Field(location, Optional.empty(), decode(fieldValues.get(index), type.getFields().get(index).getType(), location)));
        }
        return new Cast(location, new Row(location, fields), type);
    }

    private static JsonNode toJson(Value value)
    {
        return switch (value) {
            case NullValue _ -> NullNode.getInstance();
            case BooleanValue booleanValue -> BooleanNode.valueOf(booleanValue.value());
            case NumberValue numberValue -> DecimalNode.valueOf(new BigDecimal(numberValue.value()));
            case StringValue stringValue -> TextNode.valueOf(stringValue.value());
            case ArrayValue arrayValue -> {
                ArrayNode array = JsonNodeFactory.instance.arrayNode();
                arrayValue.value().forEach(element -> array.add(toJson(element)));
                yield array;
            }
            case ObjectValue objectValue -> {
                ObjectNode object = JsonNodeFactory.instance.objectNode();
                objectValue.value().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> object.set(entry.getKey(), toJson(entry.getValue())));
                yield object;
            }
        };
    }

    private static void validateSupportedType(DataType type)
    {
        if (type instanceof DateTimeDataType dateTimeType) {
            validateDateTimePrecision(dateTimeType);
            return;
        }
        if (type instanceof RowDataType rowType) {
            rowType.getFields().forEach(field -> validateSupportedType(field.getType()));
            return;
        }
        if (!(type instanceof GenericDataType genericType)) {
            throw new IllegalArgumentException("unsupported type");
        }
        String typeName = genericType.getName().getCanonicalValue();
        if (Set.of("BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DATE", "UUID", "IPADDRESS", "VARBINARY", "JSON").contains(typeName)) {
            requireNoArguments(genericType);
            return;
        }
        if (typeName.equals("VARCHAR")) {
            validateStringType(genericType, false);
            return;
        }
        if (typeName.equals("CHAR")) {
            validateStringType(genericType, true);
            return;
        }
        if (typeName.equals("DECIMAL")) {
            validateDecimalType(genericType);
            return;
        }
        if (typeName.equals("ARRAY")) {
            validateSupportedType(typeArgument(genericType, 0, 1));
            return;
        }
        if (typeName.equals("MAP")) {
            validateSupportedType(typeArgument(genericType, 0, 2));
            validateSupportedType(typeArgument(genericType, 1, 2));
            return;
        }
        throw new IllegalArgumentException("unsupported type");
    }

    private static void validateIntegerRange(String type, BigInteger value)
    {
        BigInteger minimum;
        BigInteger maximum;
        switch (type) {
            case "TINYINT" -> {
                minimum = BigInteger.valueOf(Byte.MIN_VALUE);
                maximum = BigInteger.valueOf(Byte.MAX_VALUE);
            }
            case "SMALLINT" -> {
                minimum = BigInteger.valueOf(Short.MIN_VALUE);
                maximum = BigInteger.valueOf(Short.MAX_VALUE);
            }
            case "INTEGER" -> {
                minimum = BigInteger.valueOf(Integer.MIN_VALUE);
                maximum = BigInteger.valueOf(Integer.MAX_VALUE);
            }
            case "BIGINT" -> {
                minimum = BigInteger.valueOf(Long.MIN_VALUE);
                maximum = BigInteger.valueOf(Long.MAX_VALUE);
            }
            default -> throw new IllegalArgumentException("unsupported integral type");
        }
        if (value.compareTo(minimum) < 0 || value.compareTo(maximum) > 0) {
            throw new IllegalArgumentException("integral value is out of range");
        }
    }

    private static void validateStringLength(String value, GenericDataType type, boolean lengthRequired)
    {
        validateStringType(type, lengthRequired);
        if (!type.getArguments().isEmpty()) {
            long maximumLength = numericArguments(type, 1).getFirst();
            if (value.codePointCount(0, value.length()) > maximumLength) {
                throw new IllegalArgumentException("string value is too long");
            }
        }
    }

    private static void validateStringType(GenericDataType type, boolean lengthRequired)
    {
        int argumentCount = type.getArguments().size();
        if ((lengthRequired && argumentCount != 1) || (!lengthRequired && argumentCount > 1)) {
            throw new IllegalArgumentException("invalid string type");
        }
        if (argumentCount == 1) {
            long length = numericArguments(type, 1).getFirst();
            long maximumLength = type.getName().getCanonicalValue().equals("CHAR") ? MAX_LENGTH : io.trino.spi.type.VarcharType.MAX_LENGTH;
            if (length < 0 || length > maximumLength) {
                throw new IllegalArgumentException("invalid string length");
            }
        }
    }

    private static List<Long> validateDecimalType(GenericDataType type)
    {
        List<Long> arguments = numericArguments(type, 2);
        long precision = arguments.get(0);
        long scale = arguments.get(1);
        if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
            throw new IllegalArgumentException("invalid decimal type");
        }
        return arguments;
    }

    private static void validateDateTimePrecision(DateTimeDataType type)
    {
        type.getPrecision().ifPresent(precision -> {
            if (!(precision instanceof NumericParameter numericPrecision) || numericPrecision.getParsedValue() < 0 || numericPrecision.getParsedValue() > 12) {
                throw new IllegalArgumentException("invalid date-time precision");
            }
        });
    }

    private static void validateLiteralPrecision(int precision)
    {
        if (precision > 12) {
            throw new IllegalArgumentException("date-time literal precision is out of range");
        }
    }

    private static void requireType(GenericDataType type, String expected)
    {
        if (!type.getName().getCanonicalValue().equals(expected)) {
            throw new IllegalArgumentException("value has an incompatible type");
        }
        requireNoArguments(type);
    }

    private static void requireNoArguments(GenericDataType type)
    {
        if (!type.getArguments().isEmpty()) {
            throw new IllegalArgumentException("type does not accept arguments");
        }
    }

    private static DataType typeArgument(GenericDataType type, int index, int expectedCount)
    {
        if (type.getArguments().size() != expectedCount || !(type.getArguments().get(index) instanceof TypeParameter typeParameter)) {
            throw new IllegalArgumentException("invalid container type");
        }
        return typeParameter.getValue();
    }

    private static List<Long> numericArguments(GenericDataType type, int expectedCount)
    {
        if (type.getArguments().size() != expectedCount) {
            throw new IllegalArgumentException("invalid type parameters");
        }
        return type.getArguments().stream()
                .map(HogQlParameterDecoder::numericArgument)
                .toList();
    }

    private static long numericArgument(DataTypeParameter parameter)
    {
        if (!(parameter instanceof NumericParameter numericParameter)) {
            throw new IllegalArgumentException("numeric type parameter is required");
        }
        return numericParameter.getParsedValue();
    }

    private static TrinoException bindingError(String name, NodeLocation location)
    {
        return new TrinoException(
                HOGQL_BINDING_ERROR,
                Optional.of(new Location(location.getLineNumber(), location.getColumnNumber())),
                "Invalid HogQL parameter binding: " + name,
                null);
    }
}
