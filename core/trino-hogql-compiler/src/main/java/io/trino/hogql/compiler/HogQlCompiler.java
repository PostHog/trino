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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinRequest;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.hogql.parser.HogQlLanguageVersion;
import io.trino.hogql.parser.HogQlParser;
import io.trino.hogql.parser.HogQlParsingException;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Statement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_BINDING_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_SYNTAX_ERROR;
import static java.util.Objects.requireNonNull;

public final class HogQlCompiler
{
    private final HogQlParser parser;

    public HogQlCompiler()
    {
        this(new HogQlParser());
    }

    HogQlCompiler(HogQlParser parser)
    {
        this.parser = requireNonNull(parser, "parser is null");
    }

    public Statement compile(String hogql)
    {
        return compile(hogql, Map.of()).statement();
    }

    public HogQlCompilationResult compile(HogQlCompileEnvelope envelope)
    {
        return compile(envelope, Optional.empty());
    }

    public HogQlCompilationResult compile(HogQlCompileEnvelope envelope, Optional<HogQlSemanticCatalogContext> catalogContext)
    {
        requireNonNull(envelope, "envelope is null");
        requireNonNull(catalogContext, "catalogContext is null");
        return compile(
                parse(envelope.query()),
                envelope.parameters(),
                catalogContext,
                envelope.languageVersion(),
                envelope.catalogGeneration());
    }

    public HogQlCompilationResult compile(String hogql, Map<String, HogQlTypedValue> parameters)
    {
        requireNonNull(hogql, "hogql is null");
        requireNonNull(parameters, "parameters is null");
        return compile(parse(hogql), parameters, Optional.empty(), HogQlLanguageContract.current().languageVersion(), OptionalLong.empty());
    }

    private HogQlQuery parse(String hogql)
    {
        requireNonNull(hogql, "hogql is null");
        try {
            return parser.parseStatement(hogql);
        }
        catch (HogQlParsingException exception) {
            throw new TrinoException(
                    HOGQL_SYNTAX_ERROR,
                    Optional.of(new Location(exception.getLineNumber(), exception.getColumnNumber())),
                    exception.getErrorMessage(),
                    exception);
        }
    }

    private static HogQlCompilationResult compile(
            HogQlQuery query,
            Map<String, HogQlTypedValue> parameters,
            Optional<HogQlSemanticCatalogContext> catalogContext,
            HogQlLanguageVersion languageVersion,
            OptionalLong expectedCatalogGeneration)
    {
        validateParameters(query, parameters);
        validateQuery(query);

        List<Placeholder> placeholders = new ArrayList<>();
        collectPlaceholders(query, placeholders);
        placeholders.sort(Comparator.comparingInt(placeholder -> placeholder.span().startOffset()));

        List<String> missing = placeholders.stream()
                .map(Placeholder::name)
                .filter(name -> !parameters.containsKey(name))
                .distinct()
                .toList();
        if (!missing.isEmpty()) {
            Placeholder firstMissing = placeholders.stream()
                    .filter(placeholder -> missing.contains(placeholder.name()))
                    .findFirst()
                    .orElseThrow();
            throw bindingError(firstMissing.span(), "Missing HogQL parameter bindings: " + String.join(", ", missing));
        }

        Set<String> placeholderNames = new HashSet<>();
        placeholders.stream()
                .map(Placeholder::name)
                .forEach(placeholderNames::add);
        List<String> extra = parameters.keySet().stream()
                .filter(name -> !placeholderNames.contains(name))
                .sorted()
                .toList();
        if (!extra.isEmpty()) {
            throw bindingError(query.span(), "Unused HogQL parameter bindings: " + String.join(", ", extra));
        }

        Map<SourceSpan, Integer> parameterIds = new HashMap<>();
        for (int index = 0; index < placeholders.size(); index++) {
            parameterIds.put(placeholders.get(index).span(), index);
        }
        ResolvedQuery resolved = resolveQuery(query, catalogContext, languageVersion, expectedCatalogGeneration);
        Statement statement = TrinoAstFactory.createStatement(resolved.query(), parameterIds);
        return new HogQlCompilationResult(statement, placeholders.stream()
                .map(Placeholder::name)
                .toList(), resolved.catalogGeneration());
    }

    private static void collectPlaceholders(HogQlQuery query, List<Placeholder> placeholders)
    {
        query.with().forEach(commonTable -> collectPlaceholders(commonTable.query(), placeholders));
        query.projections().forEach(projection -> {
            if (projection instanceof HogQlQuery.ExpressionProjection expressionProjection) {
                collectPlaceholders(expressionProjection.expression(), placeholders);
            }
        });
        query.from().ifPresent(relation -> collectPlaceholders(relation, placeholders));
        query.where().ifPresent(expression -> collectPlaceholders(expression, placeholders));
        query.groupBy().forEach(expression -> collectPlaceholders(expression, placeholders));
        query.having().ifPresent(expression -> collectPlaceholders(expression, placeholders));
        query.orderBy().forEach(sortItem -> collectPlaceholders(sortItem.expression(), placeholders));
        query.limit().ifPresent(expression -> collectPlaceholders(expression, placeholders));
        query.offset().ifPresent(expression -> collectPlaceholders(expression, placeholders));
    }

    private static ResolvedQuery resolveQuery(
            HogQlQuery query,
            Optional<HogQlSemanticCatalogContext> catalogContext,
            HogQlLanguageVersion languageVersion,
            OptionalLong expectedCatalogGeneration)
    {
        if (!containsSemanticCandidate(query) || catalogContext.isEmpty()) {
            return new ResolvedQuery(query, OptionalLong.empty());
        }
        HogQlSemanticCatalogContext context = catalogContext.orElseThrow();
        PinnedSnapshot pinned = context.snapshotProvider().pin(new PinRequest(
                context.catalog(),
                requireNonNull(languageVersion, "languageVersion is null"),
                expectedCatalogGeneration));
        HogQlQuery resolved = HogQlSemanticResolver.resolve(pinned, query)
                .map(HogQlSemanticResolver.ResolvedQuery::query)
                .orElse(query);
        return new ResolvedQuery(resolved, OptionalLong.of(pinned.generation()));
    }

    private static boolean containsSemanticCandidate(HogQlQuery query)
    {
        return query.with().stream().anyMatch(commonTable -> containsSemanticCandidate(commonTable.query())) ||
                query.from().map(HogQlCompiler::containsSemanticCandidate).orElse(false);
    }

    private static boolean containsSemanticCandidate(Relation relation)
    {
        return switch (relation) {
            case AliasedRelation alias -> containsSemanticCandidate(alias.relation());
            case CommonTableReference _ -> false;
            case JoinRelation join -> containsSemanticCandidate(join.left()) || containsSemanticCandidate(join.right());
            case SubqueryRelation subquery -> containsSemanticCandidate(subquery.query());
            case TablePlaceholder _ -> false;
            case HogQlQuery.TableReference table -> table.parts().size() == 1;
        };
    }

    private record ResolvedQuery(HogQlQuery query, OptionalLong catalogGeneration)
    {
        private ResolvedQuery
        {
            query = requireNonNull(query, "query is null");
            catalogGeneration = requireNonNull(catalogGeneration, "catalogGeneration is null");
        }
    }

    private static void validateParameters(HogQlQuery query, Map<String, HogQlTypedValue> parameters)
    {
        for (Map.Entry<String, HogQlTypedValue> parameter : parameters.entrySet()) {
            if (parameter.getKey() == null || parameter.getKey().isBlank()) {
                throw bindingError(query.span(), "HogQL parameter binding name is empty");
            }
            if (parameter.getValue() == null) {
                throw bindingError(query.span(), "HogQL parameter binding has no typed value: " + parameter.getKey());
            }
        }
    }

    private static void validateQuery(HogQlQuery query)
    {
        query.with().forEach(commonTable -> validateQuery(commonTable.query()));
        query.from().ifPresent(HogQlCompiler::validateRelation);
    }

    private static void collectPlaceholders(Expression expression, List<Placeholder> placeholders)
    {
        switch (expression) {
            case ArrayExpression array -> array.values().forEach(value -> collectPlaceholders(value, placeholders));
            case BetweenExpression between -> {
                collectPlaceholders(between.value(), placeholders);
                collectPlaceholders(between.min(), placeholders);
                collectPlaceholders(between.max(), placeholders);
            }
            case BinaryExpression binary -> {
                collectPlaceholders(binary.left(), placeholders);
                collectPlaceholders(binary.right(), placeholders);
            }
            case CaseExpression caseExpression -> {
                caseExpression.operand().ifPresent(operand -> collectPlaceholders(operand, placeholders));
                caseExpression.whenClauses().forEach(when -> {
                    collectPlaceholders(when.operand(), placeholders);
                    collectPlaceholders(when.result(), placeholders);
                });
                caseExpression.defaultValue().ifPresent(value -> collectPlaceholders(value, placeholders));
            }
            case CastExpression cast -> collectPlaceholders(cast.value(), placeholders);
            case ColumnReference _ -> {}
            case FunctionCall function -> {
                function.arguments().forEach(argument -> collectPlaceholders(argument, placeholders));
                function.orderBy().forEach(sortItem -> collectPlaceholders(sortItem.expression(), placeholders));
                function.filter().ifPresent(filter -> collectPlaceholders(filter, placeholders));
            }
            case InExpression in -> {
                collectPlaceholders(in.value(), placeholders);
                in.values().forEach(value -> collectPlaceholders(value, placeholders));
            }
            case IsNullExpression isNull -> collectPlaceholders(isNull.value(), placeholders);
            case Literal _ -> {}
            case Placeholder placeholder -> placeholders.add(placeholder);
            case TupleExpression tuple -> tuple.values().forEach(value -> collectPlaceholders(value, placeholders));
            case UnaryExpression unary -> collectPlaceholders(unary.operand(), placeholders);
        }
    }

    private static void validateRelation(Relation relation)
    {
        switch (relation) {
            case AliasedRelation alias -> validateRelation(alias.relation());
            case CommonTableReference _ -> {}
            case JoinRelation join -> {
                validateRelation(join.left());
                validateRelation(join.right());
            }
            case SubqueryRelation subquery -> validateQuery(subquery.query());
            case TablePlaceholder tablePlaceholder -> throw bindingError(
                    tablePlaceholder.span(),
                    "HogQL parameter placeholders are not supported in table positions: " + tablePlaceholder.placeholder().name());
            case HogQlQuery.TableReference _ -> {}
        }
    }

    private static void collectPlaceholders(Relation relation, List<Placeholder> placeholders)
    {
        switch (relation) {
            case AliasedRelation alias -> collectPlaceholders(alias.relation(), placeholders);
            case CommonTableReference _ -> {}
            case JoinRelation join -> {
                collectPlaceholders(join.left(), placeholders);
                collectPlaceholders(join.right(), placeholders);
                join.criteria().ifPresent(criteria -> {
                    if (criteria instanceof JoinOn on) {
                        collectPlaceholders(on.expression(), placeholders);
                    }
                });
            }
            case SubqueryRelation subquery -> collectPlaceholders(subquery.query(), placeholders);
            case TablePlaceholder _ -> {}
            case HogQlQuery.TableReference _ -> {}
        }
    }

    private static TrinoException bindingError(SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_BINDING_ERROR,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }
}
