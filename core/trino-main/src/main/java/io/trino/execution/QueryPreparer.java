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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static io.trino.execution.ParameterExtractor.getParameterCount;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.util.StatementUtils.getQueryType;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class QueryPreparer
{
    private final SqlParser sqlParser;

    @Inject
    public QueryPreparer(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public PreparedQuery prepareQuery(Session session, String query)
            throws ParsingException, TrinoException
    {
        Statement wrappedStatement = sqlParser.createStatement(query);
        return prepareQuery(session, wrappedStatement);
    }

    public PreparedQuery prepareQuery(Session session, Statement wrappedStatement)
            throws ParsingException, TrinoException
    {
        Statement statement = wrappedStatement;
        Optional<String> prepareSql = Optional.empty();
        if (statement instanceof Execute executeStatement) {
            prepareSql = Optional.of(session.getPreparedStatementFromExecute(executeStatement));
            statement = sqlParser.createStatement(prepareSql.get());
        }
        else if (statement instanceof ExecuteImmediate executeImmediateStatement) {
            statement = sqlParser.createStatement(
                    executeImmediateStatement.getStatement().getValue(),
                    executeImmediateStatement.getStatement().getLocation().orElseThrow());
        }
        else if (statement instanceof ExplainAnalyze explainAnalyzeStatement) {
            Statement innerStatement = explainAnalyzeStatement.getStatement();
            Optional<QueryType> innerQueryType = getQueryType(innerStatement);
            if (innerQueryType.isEmpty() || innerQueryType.get() == QueryType.DATA_DEFINITION) {
                throw new TrinoException(NOT_SUPPORTED, "EXPLAIN ANALYZE doesn't support statement type: " + innerStatement.getClass().getSimpleName());
            }
        }

        // Checked on the statement that runs, so EXECUTE IMMEDIATE cannot wrap the PREPARE past it.
        // A prepared statement travels in session headers and query info as plain text
        if (statement instanceof Prepare prepareStatement && CatalogPropertyRedaction.definesCatalogProperties(prepareStatement.getStatement(), sqlParser)) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE CATALOG cannot be used in a prepared statement");
        }

        List<Expression> parameters = ImmutableList.of();
        if (wrappedStatement instanceof Execute executeStatement) {
            parameters = executeStatement.getParameters();
        }
        else if (wrappedStatement instanceof ExecuteImmediate executeImmediateStatement) {
            parameters = executeImmediateStatement.getParameters();
        }
        validateParameters(statement, parameters);

        return new PreparedQuery(wrappedStatement, statement, parameters, prepareSql);
    }

    /**
     * The text to record for a prepared query: the original text, unless the query defines catalog
     * properties, in which case their values are masked. The query itself is executed unchanged.
     */
    public String recordedQueryText(PreparedQuery preparedQuery, String query)
    {
        return CatalogPropertyRedaction.redactedSql(preparedQuery.getWrappedStatement(), sqlParser).orElse(query);
    }

    /**
     * Whether any of the statements is, or wraps, a {@code CREATE CATALOG}. Text that cannot be
     * parsed is checked lexically.
     */
    public boolean definesCatalogProperties(Iterable<String> statements)
    {
        for (String sql : statements) {
            if (CatalogPropertyRedaction.definesCatalogProperties(sql, sqlParser)) {
                return true;
            }
        }
        return false;
    }

    private static void validateParameters(Statement node, List<Expression> parameterValues)
    {
        int parameterCount = getParameterCount(node);
        if (parameterValues.size() != parameterCount) {
            throw semanticException(INVALID_PARAMETER_USAGE, node, "Incorrect number of parameters: expected %s but found %s", parameterCount, parameterValues.size());
        }
        for (Expression expression : parameterValues) {
            verifyExpressionIsConstant(emptySet(), expression);
        }
    }

    public static class PreparedQuery
    {
        private final Statement wrappedStatement;
        private final Statement statement;
        private final List<Expression> parameters;
        private final Optional<String> prepareSql;

        public PreparedQuery(Statement statement, List<Expression> parameters, Optional<String> prepareSql)
        {
            this(statement, statement, parameters, prepareSql);
        }

        public PreparedQuery(Statement wrappedStatement, Statement statement, List<Expression> parameters, Optional<String> prepareSql)
        {
            this.wrappedStatement = requireNonNull(wrappedStatement, "wrappedStatement is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
            this.prepareSql = requireNonNull(prepareSql, "prepareSql is null");
        }

        /**
         * The statement as submitted, before {@code EXECUTE} or {@code EXECUTE IMMEDIATE} was resolved.
         */
        public Statement getWrappedStatement()
        {
            return wrappedStatement;
        }

        public Statement getStatement()
        {
            return statement;
        }

        public List<Expression> getParameters()
        {
            return parameters;
        }

        public Optional<String> getPrepareSql()
        {
            return prepareSql;
        }
    }
}
