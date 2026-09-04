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

import io.trino.Session;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.compiler.HogQlErrorCode;
import io.trino.hogql.compiler.HogQlTypedValue;
import io.trino.hogql.compiler.HogQlTypedValue.ArrayValue;
import io.trino.hogql.compiler.HogQlTypedValue.BooleanValue;
import io.trino.hogql.compiler.HogQlTypedValue.NullValue;
import io.trino.hogql.compiler.HogQlTypedValue.NumberValue;
import io.trino.hogql.compiler.HogQlTypedValue.ObjectValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QuerySubmission.hogQl;
import static io.trino.execution.QuerySubmission.trino;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final QueryPreparer QUERY_PREPARER = new QueryPreparer(SQL_PARSER);
    private static final QueryPreparer HOGQL_QUERY_PREPARER = new QueryPreparer(SQL_PARSER, new HogQlCompiler());

    @Test
    public void testSelectStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "SELECT * FROM foo");
        PreparedQuery submittedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, trino("SELECT * FROM foo"));
        assertThat(preparedQuery.getStatement()).isEqualTo(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
        assertThat(submittedQuery.getStatement()).isEqualTo(preparedQuery.getStatement());
        assertThat(preparedQuery.getSessionPropertyOverrides()).isEmpty();
        assertThat(submittedQuery.getSessionPropertyOverrides()).isEmpty();
    }

    @Test
    public void testHogQlStatement()
    {
        HogQlCompileEnvelope envelope = envelope("SELECT 1");
        QuerySubmission submission = hogQl(envelope);
        PreparedQuery preparedQuery = HOGQL_QUERY_PREPARER.prepareQuery(TEST_SESSION, submission);
        assertThat(preparedQuery.getStatement()).isInstanceOf(Query.class);
        assertThat(preparedQuery.getSessionPropertyOverrides()).isEmpty();
        assertThat(submission.hogQlEnvelope()).containsSame(envelope);
    }

    @Test
    public void testHogQlTypedParametersUseThePreparedValueBoundary()
    {
        Map<String, HogQlTypedValue> parameters = new LinkedHashMap<>();
        parameters.put("json_value", typedValue("json", new ObjectValue(Map.of("enabled", new BooleanValue(true), "items", new ArrayValue(List.of(new NumberValue("1"), NullValue.NULL))))));
        parameters.put("row_value", typedValue("row(label varchar, count bigint)", new ObjectValue(Map.of("count", new NumberValue("2"), "label", new StringValue("synthetic-row")))));
        parameters.put("map_value", typedValue("map(varchar, bigint)", new ObjectValue(Map.of("second", NullValue.NULL, "first", new NumberValue("1")))));
        parameters.put("array_value", typedValue("array(bigint)", new ArrayValue(List.of(new NumberValue("1"), NullValue.NULL, new NumberValue("2")))));
        parameters.put("uuid_value", typedValue("uuid", new StringValue("018f6b9d-89f4-7e8a-8f5d-4c621e5d4a33")));
        parameters.put("timestamp_value", typedValue("timestamp(3)", new StringValue("2026-08-27 12:34:56.789")));
        parameters.put("date_value", typedValue("date", new StringValue("2026-08-27")));
        parameters.put("varchar_value", typedValue("varchar", new StringValue("synthetic-text")));
        parameters.put("decimal_value", typedValue("decimal(4, 2)", new NumberValue("12.30")));
        parameters.put("double_value", typedValue("double", new NumberValue("1.25")));
        parameters.put("bigint_value", typedValue("bigint", new NumberValue("42")));
        parameters.put("boolean_value", typedValue("boolean", new BooleanValue(true)));

        PreparedQuery preparedQuery = HOGQL_QUERY_PREPARER.prepareQuery(
                TEST_SESSION,
                hogQl(envelope(
                        "SELECT {boolean_value}, {bigint_value}, {double_value}, {decimal_value}, {varchar_value}, {date_value}, {timestamp_value}, {uuid_value}, {array_value}, {map_value}, {row_value}, {json_value}, {varchar_value}",
                        parameters)));

        assertThat(preparedQuery.getParameters()).hasSize(13);
        assertThat(preparedQuery.getParameters())
                .extracting(expression -> expression.getLocation())
                .containsExactlyElementsOf(ParameterExtractor.extractParameters(preparedQuery.getStatement()).stream()
                        .map(parameter -> parameter.getLocation())
                        .toList());
        assertThat(preparedQuery.getParameters().subList(0, 11)).allMatch(Cast.class::isInstance);
        assertThat(preparedQuery.getParameters().subList(0, 11))
                .extracting(expression -> ((Cast) expression).getType())
                .containsExactly(
                        SQL_PARSER.createType("boolean"),
                        SQL_PARSER.createType("bigint"),
                        SQL_PARSER.createType("double"),
                        SQL_PARSER.createType("decimal(4, 2)"),
                        SQL_PARSER.createType("varchar"),
                        SQL_PARSER.createType("date"),
                        SQL_PARSER.createType("timestamp(3)"),
                        SQL_PARSER.createType("uuid"),
                        SQL_PARSER.createType("array(bigint)"),
                        SQL_PARSER.createType("map(varchar, bigint)"),
                        SQL_PARSER.createType("row(label varchar, count bigint)"));
        assertThat(((Cast) preparedQuery.getParameters().get(8)).getExpression()).isInstanceOf(Array.class);
        assertThat(((Cast) preparedQuery.getParameters().get(9)).getExpression()).isInstanceOf(FunctionCall.class);
        assertThat(((Cast) preparedQuery.getParameters().get(10)).getExpression()).isInstanceOf(Row.class);
        assertThat(preparedQuery.getParameters().get(11))
                .isInstanceOfSatisfying(GenericLiteral.class, json -> assertThat(json.getValue()).isEqualTo("{\"enabled\":true,\"items\":[1,null]}"));
        assertThat(preparedQuery.getParameters().get(12)).isEqualTo(preparedQuery.getParameters().get(4));
        assertThat(SqlFormatter.formatSql(preparedQuery.getStatement()))
                .doesNotContain("synthetic-text", "synthetic-row", "018f6b9d")
                .contains("?");
    }

    @ParameterizedTest
    @MethodSource("validHogQlLiteralParameters")
    public void testHogQlLiteralParametersPreserveStockAstValues(HogQlTypedValue typedValue, Class<? extends Expression> literalType, String expectedValue)
    {
        PreparedQuery preparedQuery = HOGQL_QUERY_PREPARER.prepareQuery(
                TEST_SESSION,
                hogQl(envelope("SELECT {input}", Map.of("input", typedValue))));

        assertThat(preparedQuery.getParameters()).hasSize(1);
        Expression parameter = preparedQuery.getParameters().getFirst();
        Expression literal;
        if (typedValue.type().equalsIgnoreCase("json")) {
            literal = parameter;
        }
        else {
            assertThat(parameter).isInstanceOf(Cast.class);
            Cast cast = (Cast) parameter;
            assertThat(cast.getType()).isEqualTo(SQL_PARSER.createType(typedValue.type()));
            literal = cast.getExpression();
        }
        assertThat(literal).isInstanceOf(literalType);
        String literalValue = switch (literal) {
            case GenericLiteral genericLiteral -> genericLiteral.getValue();
            case BinaryLiteral binaryLiteral -> binaryLiteral.toHexString();
            default -> throw new AssertionError("unexpected literal type");
        };
        assertThat(literalValue).isEqualTo(expectedValue);
        assertThat(SqlFormatter.formatSql(preparedQuery.getStatement()))
                .contains("?")
                .doesNotContain(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("invalidHogQlTypedParameters")
    public void testHogQlTypedParameterErrorsAreStableAndRedacted(HogQlTypedValue typedValue, String redactedFragment)
    {
        assertTrinoExceptionThrownBy(() -> HOGQL_QUERY_PREPARER.prepareQuery(
                TEST_SESSION,
                hogQl(envelope("SELECT {input}", Map.of("input", typedValue)))))
                .hasErrorCode(HogQlErrorCode.HOGQL_BINDING_ERROR)
                .hasMessage("line 1:8: Invalid HogQL parameter binding: input")
                .satisfies(exception -> assertThat(exception.getMessage()).doesNotContain(redactedFragment));
    }

    @Test
    public void testHogQlSubmissionFailsClosedWithoutCompiler()
    {
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, hogQl(envelope("SELECT 1"))))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("HogQL query submission is disabled");
    }

    private static HogQlCompileEnvelope envelope(String query)
    {
        return envelope(query, Map.of());
    }

    private static HogQlCompileEnvelope envelope(String query, Map<String, HogQlTypedValue> parameters)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                parameters,
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.empty());
    }

    private static Stream<Arguments> invalidHogQlTypedParameters()
    {
        String sensitiveValue = "sensitive-binding-value";
        return Stream.of(
                arguments(typedValue(sensitiveValue, new StringValue(sensitiveValue)), sensitiveValue),
                arguments(typedValue("boolean", new StringValue(sensitiveValue)), sensitiveValue),
                arguments(typedValue("tinyint", new NumberValue("128")), "128"),
                arguments(typedValue("decimal(3, 1)", new NumberValue("123.4")), "123.4"),
                arguments(typedValue("uuid", new StringValue(sensitiveValue)), sensitiveValue),
                arguments(typedValue("array(bigint)", new ArrayValue(List.of(new StringValue(sensitiveValue)))), sensitiveValue),
                arguments(typedValue("map(bigint, varchar)", new ObjectValue(Map.of("field", new StringValue(sensitiveValue)))), sensitiveValue),
                arguments(typedValue("row(label varchar)", new ObjectValue(Map.of("unexpected", new StringValue(sensitiveValue)))), sensitiveValue),
                arguments(typedValue("time(13)", new StringValue("12:34:56.123456789012")), "12:34:56.123456789012"),
                arguments(typedValue("time(12) with time zone", new StringValue("12:34:56.123456789012+15:00")), "+15:00"),
                arguments(typedValue("time(12) with time zone", new StringValue("12:34:56.123456789012 America/Toronto")), "America/Toronto"),
                arguments(typedValue("timestamp(12) with time zone", new StringValue("2026-03-08 02:30:00.123456789012 America/Toronto")), "America/Toronto"),
                arguments(typedValue("timestamp(12) with time zone", new StringValue("2026-08-27 12:34:56.123456789012 Invalid/Zone")), "Invalid/Zone"),
                arguments(typedValue("timestamp(12)", new StringValue("2026-08-27 12:34:56.1234567890123")), "1234567890123"),
                arguments(typedValue("date", new StringValue("2026-02-29")), "2026-02-29"),
                arguments(typedValue("uuid", new StringValue("1-1-1-1-1")), "1-1-1-1-1"),
                arguments(typedValue("ipaddress", new StringValue("host.example.com")), "host.example.com"),
                arguments(typedValue("varbinary", new StringValue("ABC")), "ABC"));
    }

    private static Stream<Arguments> validHogQlLiteralParameters()
    {
        return Stream.of(
                arguments(typedValue("time(0)", new StringValue("00:00")), GenericLiteral.class, "00:00"),
                arguments(typedValue("time(10)", new StringValue("12:34:56.1234567890")), GenericLiteral.class, "12:34:56.1234567890"),
                arguments(typedValue("time(12) with time zone", new StringValue("12:34:56.123456789012 +05:45")), GenericLiteral.class, "12:34:56.123456789012 +05:45"),
                arguments(typedValue("timestamp(0)", new StringValue("2026-08-27 12:34:56")), GenericLiteral.class, "2026-08-27 12:34:56"),
                arguments(typedValue("timestamp(10)", new StringValue("2026-08-27 12:34:56.1234567890")), GenericLiteral.class, "2026-08-27 12:34:56.1234567890"),
                arguments(typedValue("timestamp(11) with time zone", new StringValue("2026-08-27 12:34:56.12345678901 +05:45")), GenericLiteral.class, "2026-08-27 12:34:56.12345678901 +05:45"),
                arguments(typedValue("timestamp(12) with time zone", new StringValue("2026-08-27 12:34:56.123456789012 America/Toronto")), GenericLiteral.class, "2026-08-27 12:34:56.123456789012 America/Toronto"),
                arguments(typedValue("date", new StringValue("2000-02-29")), GenericLiteral.class, "2000-02-29"),
                arguments(typedValue("uuid", new StringValue("00000000-0000-0000-0000-000000000000")), GenericLiteral.class, "00000000-0000-0000-0000-000000000000"),
                arguments(typedValue("ipaddress", new StringValue("64:ff9b::10.0.0.1")), GenericLiteral.class, "64:ff9b::10.0.0.1"),
                arguments(typedValue("varbinary", new StringValue("00 ff 10")), BinaryLiteral.class, "00FF10"),
                arguments(typedValue("json", new ObjectValue(Map.of("escaped", new StringValue("line\n\u2603"), "number", new NumberValue("12345678901234567890.000000000001"), "null", NullValue.NULL))), GenericLiteral.class, "{\"escaped\":\"line\\n☃\",\"null\":null,\"number\":12345678901234567890.000000000001}"));
    }

    private static HogQlTypedValue typedValue(String type, HogQlTypedValue.Value value)
    {
        return new HogQlTypedValue(type, value);
    }

    @Test
    public void testExecuteStatement()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo")
                .build();
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query");
        assertThat(preparedQuery.getStatement()).isEqualTo(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteImmediateStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT * FROM foo'");
        assertThat(preparedQuery.getStatement()).isEqualTo(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "execute my_query"))
                .hasErrorCode(NOT_FOUND);
    }

    @Test
    public void testExecuteImmediateInvalidStatement()
    {
        assertThatThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT FROM'"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:27: mismatched input 'FROM'. Expecting: .*");
    }

    @Test
    public void testExecuteImmediateInvalidMultilineStatement()
    {
        assertThatThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE\nIMMEDIATE 'SELECT\n FROM'"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 3:2: mismatched input 'FROM'. Expecting: .*");
    }

    @Test
    public void testTooManyParameters()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo where col1 = ?")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1,2"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT * FROM foo where col1 = ?' USING 1,2"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testTooFewParameters()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo where col1 = ?")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo where col1 = ?' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testParameterMismatchWithOffset()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo OFFSET ? ROWS")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo OFFSET ? ROWS' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo OFFSET ? ROWS' USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testParameterMismatchWithLimit()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo LIMIT ?")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo LIMIT ?' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo LIMIT ?' USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testParameterMismatchWithFetchFirst()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo FETCH FIRST ? ROWS ONLY")
                .build();

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo FETCH FIRST ? ROWS ONLY' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo FETCH FIRST ? ROWS ONLY' USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }
}
