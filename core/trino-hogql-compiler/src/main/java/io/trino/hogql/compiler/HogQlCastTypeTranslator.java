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
package io.trino.hogql.compiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

final class HogQlCastTypeTranslator
{
    private HogQlCastTypeTranslator() {}

    public static String translate(String type)
    {
        Translation translation = translateType(requireNonNull(type, "type is null").trim());
        return translation.sql();
    }

    private static Translation translateType(String type)
    {
        if (type.isEmpty()) {
            throw unsupported("empty type");
        }
        if (type.endsWith("[]")) {
            return composite("array(" + translateType(type.substring(0, type.length() - 2)).sql() + ")");
        }
        if (type.matches(".*\\[\\s*\\d+\\s*]$")) {
            throw unsupported("fixed-size array syntax");
        }

        Optional<Call> call = parseCall(type);
        String name = call.map(Call::name).orElse(type).trim().toLowerCase(Locale.ENGLISH);
        List<String> arguments = call.map(Call::arguments).orElseGet(List::of);
        String suffix = call.map(Call::suffix).orElse("").trim().toLowerCase(Locale.ENGLISH);

        if (name.matches("u?int(8|16|32|64|128|256)")) {
            if (name.startsWith("u")) {
                throw unsupported("unsigned integer range");
            }
            return switch (name) {
                case "int8" -> scalar("tinyint");
                case "int16" -> scalar("smallint");
                case "int32" -> scalar("integer");
                case "int64" -> scalar("bigint");
                default -> throw unsupported("integer width exceeds Trino bigint");
            };
        }
        if (name.matches("decimal(32|64|128|256)")) {
            requireArity(name, arguments, 1);
            int precision = switch (name) {
                case "decimal32" -> 9;
                case "decimal64" -> 18;
                case "decimal128" -> 38;
                default -> throw unsupported("decimal precision exceeds Trino decimal(38, s)");
            };
            return decimal(precision, parseUnsigned(arguments.getFirst(), "decimal scale"));
        }

        return switch (name) {
            case "int", "integer" -> noArguments(name, arguments, scalar("integer"));
            case "bigint" -> noArguments(name, arguments, scalar("bigint"));
            case "smallint" -> noArguments(name, arguments, scalar("smallint"));
            case "tinyint" -> noArguments(name, arguments, scalar("tinyint"));
            case "float", "real" -> noArguments(name, arguments, scalar("real"));
            case "float64", "double", "double precision" -> noArguments(name, arguments, scalar("double"));
            case "float32" -> noArguments(name, arguments, scalar("real"));
            case "string", "text" -> noArguments(name, arguments, scalar("varchar"));
            case "varchar", "char" -> characterType(name, arguments);
            case "bool", "boolean" -> noArguments(name, arguments, scalar("boolean"));
            case "date" -> noArguments(name, arguments, scalar("date"));
            case "uuid" -> noArguments(name, arguments, scalar("uuid"));
            case "json" -> noArguments(name, arguments, scalar("json"));
            case "decimal", "numeric" -> decimal(arguments);
            case "nullable" -> nullable(arguments);
            case "array" -> unaryComposite("array", arguments);
            case "map" -> map(arguments);
            case "tuple", "row" -> row(arguments);
            case "timestamp", "time" -> temporal(name, arguments, suffix);
            case "timestamp with time zone" -> noArguments(name, arguments, scalar("timestamp(3) with time zone"));
            case "time with time zone" -> noArguments(name, arguments, scalar("time(3) with time zone"));
            case "timestamp with local time zone", "time with local time zone" -> throw unsupported("WITH LOCAL TIME ZONE semantics differ from Trino WITH TIME ZONE");
            case "interval day to second", "interval year to month" -> noArguments(name, arguments, scalar(name));
            case "fixedstring" -> throw unsupported("FixedString padding semantics");
            case "date32" -> throw unsupported("Date32 range");
            case "datetime", "datetime64", "timestamptz" -> throw unsupported("ClickHouse time-zone and range semantics");
            case "interval" -> throw unsupported("interval qualifier is required");
            default -> throw unsupported("unknown type family");
        };
    }

    private static Translation characterType(String name, List<String> arguments)
    {
        if (arguments.isEmpty()) {
            return scalar(name);
        }
        requireArity(name, arguments, 1);
        int length = parseUnsigned(arguments.getFirst(), name + " length");
        if (length < 1) {
            throw unsupported(name + " length must be positive");
        }
        return scalar(name + "(" + length + ")");
    }

    private static Translation decimal(List<String> arguments)
    {
        requireArity("decimal", arguments, 2);
        return decimal(
                parseUnsigned(arguments.getFirst(), "decimal precision"),
                parseUnsigned(arguments.get(1), "decimal scale"));
    }

    private static Translation decimal(int precision, int scale)
    {
        if (precision < 1 || precision > 38 || scale > precision) {
            throw unsupported("decimal requires 1 <= precision <= 38 and 0 <= scale <= precision");
        }
        return scalar("decimal(" + precision + ", " + scale + ")");
    }

    private static Translation nullable(List<String> arguments)
    {
        requireArity("nullable", arguments, 1);
        Translation nested = translateType(arguments.getFirst());
        if (nested.composite()) {
            throw unsupported("ClickHouse Nullable cannot wrap Array, Map, Tuple, or Row");
        }
        return nested;
    }

    private static Translation unaryComposite(String name, List<String> arguments)
    {
        requireArity(name, arguments, 1);
        return composite(name + "(" + translateType(arguments.getFirst()).sql() + ")");
    }

    private static Translation map(List<String> arguments)
    {
        requireArity("map", arguments, 2);
        return composite("map(" + translateType(arguments.getFirst()).sql() + ", " + translateType(arguments.get(1)).sql() + ")");
    }

    private static Translation row(List<String> arguments)
    {
        if (arguments.isEmpty()) {
            throw unsupported("row requires at least one field");
        }
        return composite("row(" + String.join(", ", arguments.stream().map(HogQlCastTypeTranslator::rowField).toList()) + ")");
    }

    private static String rowField(String argument)
    {
        try {
            return translateType(argument).sql();
        }
        catch (IllegalArgumentException _) {
            int separator = topLevelWhitespace(argument);
            if (separator < 0) {
                throw unsupported("invalid tuple or row field");
            }
            String fieldName = argument.substring(0, separator).trim();
            String fieldType = argument.substring(separator).trim();
            if (!fieldName.matches("[A-Za-z_][A-Za-z0-9_]*|\"(?:\"\"|[^\"])+\"")) {
                throw unsupported("invalid tuple or row field name");
            }
            return fieldName + " " + translateType(fieldType).sql();
        }
    }

    private static Translation temporal(String name, List<String> arguments, String suffix)
    {
        if (arguments.size() > 1) {
            throw unsupported(name + " accepts at most one precision");
        }
        int precision = arguments.isEmpty() ? 3 : parseUnsigned(arguments.getFirst(), name + " precision");
        if (precision > 12) {
            throw unsupported(name + " precision exceeds 12");
        }
        if (suffix.equals("with local time zone")) {
            throw unsupported("WITH LOCAL TIME ZONE semantics differ from Trino WITH TIME ZONE");
        }
        if (!suffix.isEmpty() && !suffix.equals("with time zone")) {
            throw unsupported("invalid time-zone qualifier");
        }
        String qualifier = suffix.isEmpty() ? "" : " with time zone";
        return scalar(name + "(" + precision + ")" + qualifier);
    }

    private static Optional<Call> parseCall(String type)
    {
        int opening = type.indexOf('(');
        if (opening < 0) {
            return Optional.empty();
        }
        int closing = matchingParenthesis(type, opening);
        if (closing < 0) {
            throw unsupported("unbalanced parentheses");
        }
        String suffix = type.substring(closing + 1);
        if (suffix.contains("(") || suffix.contains(")")) {
            throw unsupported("invalid type suffix");
        }
        return Optional.of(new Call(
                type.substring(0, opening).trim(),
                splitArguments(type.substring(opening + 1, closing)),
                suffix));
    }

    private static List<String> splitArguments(String input)
    {
        if (input.isBlank()) {
            return List.of();
        }
        List<String> arguments = new ArrayList<>();
        int depth = 0;
        boolean quoted = false;
        int start = 0;
        for (int index = 0; index < input.length(); index++) {
            char character = input.charAt(index);
            if (character == '\'' || character == '"') {
                quoted = !quoted;
            }
            else if (!quoted && character == '(') {
                depth++;
            }
            else if (!quoted && character == ')') {
                depth--;
            }
            else if (!quoted && depth == 0 && character == ',') {
                arguments.add(nonEmpty(input.substring(start, index)));
                start = index + 1;
            }
        }
        if (depth != 0 || quoted) {
            throw unsupported("unbalanced nested type");
        }
        arguments.add(nonEmpty(input.substring(start)));
        return List.copyOf(arguments);
    }

    private static int matchingParenthesis(String input, int opening)
    {
        int depth = 0;
        boolean quoted = false;
        for (int index = opening; index < input.length(); index++) {
            char character = input.charAt(index);
            if (character == '\'' || character == '"') {
                quoted = !quoted;
            }
            else if (!quoted && character == '(') {
                depth++;
            }
            else if (!quoted && character == ')' && --depth == 0) {
                return index;
            }
        }
        return -1;
    }

    private static int topLevelWhitespace(String input)
    {
        int depth = 0;
        boolean quoted = false;
        for (int index = 0; index < input.length(); index++) {
            char character = input.charAt(index);
            if (character == '"') {
                quoted = !quoted;
            }
            else if (!quoted && character == '(') {
                depth++;
            }
            else if (!quoted && character == ')') {
                depth--;
            }
            else if (!quoted && depth == 0 && Character.isWhitespace(character)) {
                return index;
            }
        }
        return -1;
    }

    private static int parseUnsigned(String value, String description)
    {
        String normalized = value.trim();
        if (!normalized.matches("\\d+")) {
            throw unsupported(description + " must be an unsigned integer");
        }
        try {
            return parseInt(normalized);
        }
        catch (NumberFormatException _) {
            throw unsupported(description + " is too large");
        }
    }

    private static Translation noArguments(String name, List<String> arguments, Translation translation)
    {
        requireArity(name, arguments, 0);
        return translation;
    }

    private static void requireArity(String name, List<String> arguments, int expected)
    {
        if (arguments.size() != expected) {
            throw unsupported(name + " requires " + expected + " type argument(s)");
        }
    }

    private static String nonEmpty(String value)
    {
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw unsupported("empty type argument");
        }
        return normalized;
    }

    private static Translation scalar(String sql)
    {
        return new Translation(sql, false);
    }

    private static Translation composite(String sql)
    {
        return new Translation(sql, true);
    }

    private static IllegalArgumentException unsupported(String reason)
    {
        return new IllegalArgumentException(reason);
    }

    private record Call(String name, List<String> arguments, String suffix) {}

    private record Translation(String sql, boolean composite) {}
}
