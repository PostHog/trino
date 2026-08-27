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
import io.trino.hogql.HogQlCompilationEvent.Dimensions;
import io.trino.hogql.HogQlCompilationObserver;
import io.trino.hogql.HogQlCompilationTracker;
import io.trino.hogql.compiler.HogQlCompilationResult;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.compiler.HogQlSemanticCatalogContext;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static io.trino.execution.ParameterExtractor.getParameterCount;
import static io.trino.hogql.HogQlCatalogIdentifiers.physicalCatalog;
import static io.trino.hogql.HogQlCompilationEvent.Phase.COMPILATION;
import static io.trino.hogql.HogQlCompilationEvent.Phase.PARAMETER_BINDING;
import static io.trino.hogql.HogQlCompilationObserver.NOOP;
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
    private final Optional<HogQlCompiler> hogQlCompiler;
    private final HogQlParameterDecoder hogQlParameterDecoder;
    private final HogQlCompilationObserver hogQlCompilationObserver;
    private final Optional<HogQlSemanticCatalogSnapshotProvider> hogQlSemanticCatalogSnapshotProvider;

    public QueryPreparer(SqlParser sqlParser)
    {
        this(sqlParser, Optional.empty(), NOOP, Optional.empty());
    }

    public QueryPreparer(SqlParser sqlParser, HogQlCompiler hogQlCompiler)
    {
        this(sqlParser, Optional.of(requireNonNull(hogQlCompiler, "hogQlCompiler is null")), NOOP, Optional.empty());
    }

    public QueryPreparer(SqlParser sqlParser, HogQlCompiler hogQlCompiler, HogQlCompilationObserver hogQlCompilationObserver)
    {
        this(sqlParser, Optional.of(requireNonNull(hogQlCompiler, "hogQlCompiler is null")), hogQlCompilationObserver, Optional.empty());
    }

    @Inject
    public QueryPreparer(
            SqlParser sqlParser,
            HogQlCompiler hogQlCompiler,
            HogQlCompilationObserver hogQlCompilationObserver,
            Optional<HogQlSemanticCatalogSnapshotProvider> hogQlSemanticCatalogSnapshotProvider)
    {
        this(sqlParser,
                Optional.of(requireNonNull(hogQlCompiler, "hogQlCompiler is null")),
                hogQlCompilationObserver,
                hogQlSemanticCatalogSnapshotProvider);
    }

    private QueryPreparer(
            SqlParser sqlParser,
            Optional<HogQlCompiler> hogQlCompiler,
            HogQlCompilationObserver hogQlCompilationObserver,
            Optional<HogQlSemanticCatalogSnapshotProvider> hogQlSemanticCatalogSnapshotProvider)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.hogQlCompiler = requireNonNull(hogQlCompiler, "hogQlCompiler is null");
        this.hogQlParameterDecoder = new HogQlParameterDecoder(sqlParser);
        this.hogQlCompilationObserver = requireNonNull(hogQlCompilationObserver, "hogQlCompilationObserver is null");
        this.hogQlSemanticCatalogSnapshotProvider = requireNonNull(hogQlSemanticCatalogSnapshotProvider, "hogQlSemanticCatalogSnapshotProvider is null");
    }

    public PreparedQuery prepareQuery(Session session, String query)
            throws ParsingException, TrinoException
    {
        return prepareQuery(session, QuerySubmission.trino(query));
    }

    public PreparedQuery prepareQuery(Session session, QuerySubmission submission)
            throws ParsingException, TrinoException
    {
        requireNonNull(submission, "submission is null");
        return switch (submission.language()) {
            case TRINO -> prepareQuery(session, sqlParser.createStatement(submission.originalText()));
            case HOGQL -> {
                HogQlCompileEnvelope envelope = submission.hogQlEnvelope().orElseThrow();
                HogQlCompilationTracker tracker = new HogQlCompilationTracker(hogQlCompilationObserver, Dimensions.fromEnvelope(envelope));
                PreparedQuery preparedQuery;
                try {
                    HogQlCompilationResult result = tracker.observe(COMPILATION, () -> hogQlCompiler
                            .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "HogQL query submission is disabled"))
                            .compile(envelope, semanticCatalogContext(session)));
                    Statement statement = explain(result.statement(), submission.hogQlExplain());
                    preparedQuery = tracker.observe(PARAMETER_BINDING, () -> prepareQuery(
                            session,
                            statement,
                            Optional.of(hogQlParameterDecoder.decode(result, envelope.parameters()))));
                }
                catch (RuntimeException | Error failure) {
                    tracker.failed(failure);
                    throw failure;
                }
                tracker.succeeded();
                yield preparedQuery;
            }
        };
    }

    private static Statement explain(Statement statement, Optional<QuerySubmission.HogQlExplain> explain)
    {
        if (explain.isEmpty()) {
            return statement;
        }
        NodeLocation location = statement.getLocation().orElseThrow();
        QuerySubmission.HogQlExplain options = explain.orElseThrow();
        return new Explain(
                location,
                statement,
                List.of(
                        new ExplainType(location, options.type()),
                        new ExplainFormat(location, options.format())));
    }

    private Optional<HogQlSemanticCatalogContext> semanticCatalogContext(Session session)
    {
        if (hogQlSemanticCatalogSnapshotProvider.isEmpty() || session.getCatalog().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new HogQlSemanticCatalogContext(
                physicalCatalog(session.getCatalog().orElseThrow()),
                hogQlSemanticCatalogSnapshotProvider.orElseThrow()));
    }

    public PreparedQuery prepareQuery(Session session, Statement wrappedStatement)
            throws ParsingException, TrinoException
    {
        return prepareQuery(session, wrappedStatement, Optional.empty());
    }

    private PreparedQuery prepareQuery(Session session, Statement wrappedStatement, Optional<List<Expression>> suppliedParameters)
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

        List<Expression> parameters;
        if (suppliedParameters.isPresent()) {
            parameters = suppliedParameters.orElseThrow();
        }
        else if (wrappedStatement instanceof Execute executeStatement) {
            parameters = executeStatement.getParameters();
        }
        else if (wrappedStatement instanceof ExecuteImmediate executeImmediateStatement) {
            parameters = executeImmediateStatement.getParameters();
        }
        else {
            parameters = ImmutableList.of();
        }
        validateParameters(statement, parameters);

        return new PreparedQuery(statement, parameters, prepareSql);
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
        private final Statement statement;
        private final List<Expression> parameters;
        private final Optional<String> prepareSql;

        public PreparedQuery(Statement statement, List<Expression> parameters, Optional<String> prepareSql)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
            this.prepareSql = requireNonNull(prepareSql, "prepareSql is null");
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
