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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException.Failure;
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
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.InCohortExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.LambdaExpression;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnnestRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Window;
import io.trino.hogql.parser.tree.HogQlQuery.WindowDefinition;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
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
                envelope,
                envelope.modifiers(),
                catalogContext,
                envelope.languageVersion(),
                envelope.catalogGeneration(),
                false);
    }

    public HogQlCompilationResult compileV0(HogQlCompileEnvelope envelope, Optional<HogQlSemanticCatalogContext> catalogContext)
    {
        requireNonNull(envelope, "envelope is null");
        requireNonNull(catalogContext, "catalogContext is null");
        if (!envelope.modifiers().isEmpty()) {
            throw unsupportedError(parse(envelope.query()).span(), "Modifiers are outside the HogQL v0 profile");
        }
        return compile(
                parse(envelope.query()),
                envelope,
                Map.of(),
                catalogContext,
                envelope.languageVersion(),
                envelope.catalogGeneration(),
                true);
    }

    public HogQlCompilationResult compile(String hogql, Map<String, HogQlTypedValue> parameters)
    {
        requireNonNull(hogql, "hogql is null");
        requireNonNull(parameters, "parameters is null");
        return compile(
                parse(hogql),
                new HogQlCompileEnvelope(
                        hogql,
                        HogQlCompileEnvelope.PROTOCOL_VERSION,
                        HogQlLanguageContract.current().languageVersion(),
                        parameters,
                        Map.of(),
                        Map.of(),
                        Map.of(),
                        OptionalLong.empty()),
                Map.of(),
                Optional.empty(),
                HogQlLanguageContract.current().languageVersion(),
                OptionalLong.empty(),
                false);
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
            HogQlCompileEnvelope envelope,
            Map<String, HogQlTypedValue> modifiers,
            Optional<HogQlSemanticCatalogContext> catalogContext,
            HogQlLanguageVersion languageVersion,
            OptionalLong expectedCatalogGeneration,
            boolean v0Profile)
    {
        validateParameters(query, envelope.parameters());
        validateQuery(query);

        List<Placeholder> placeholders = new ArrayList<>();
        collectPlaceholders(query, placeholders);
        placeholders.sort(Comparator.comparingInt(placeholder -> placeholder.span().startOffset()));

        List<String> missing = placeholders.stream()
                .map(Placeholder::name)
                .filter(name -> envelope.bindingForPlaceholder(name).isEmpty())
                .distinct()
                .toList();
        if (!missing.isEmpty()) {
            Placeholder firstMissing = placeholders.stream()
                    .filter(placeholder -> missing.contains(placeholder.name()))
                    .findFirst()
                    .orElseThrow();
            throw bindingError(firstMissing.span(), "Missing HogQL parameter bindings: " + String.join(", ", missing));
        }

        Set<String> parameterPlaceholderNames = new HashSet<>();
        placeholders.stream()
                .map(Placeholder::name)
                .filter(name -> !name.contains("."))
                .forEach(parameterPlaceholderNames::add);
        List<String> extra = envelope.parameters().keySet().stream()
                .filter(name -> !parameterPlaceholderNames.contains(name))
                .sorted()
                .toList();
        if (!extra.isEmpty()) {
            throw bindingError(query.span(), "Unused HogQL parameter bindings: " + String.join(", ", extra));
        }

        Map<SourceSpan, Integer> parameterIds = new HashMap<>();
        for (int index = 0; index < placeholders.size(); index++) {
            parameterIds.put(placeholders.get(index).span(), index);
        }
        if (v0Profile) {
            HogQlV0ProfileValidator.validate(query, Optional.empty());
        }
        ResolvedQuery resolved = resolveQuery(query, catalogContext, languageVersion, expectedCatalogGeneration, !modifiers.isEmpty(), v0Profile);
        Statement statement = TrinoAstFactory.createStatement(resolved.query(), parameterIds);
        List<HogQlModifierBinding> modifierBindings = resolved.pinnedSnapshot()
                .map(snapshot -> HogQlModifierResolver.resolve(snapshot, modifiers, query.span()))
                .orElseGet(List::of);
        return new HogQlCompilationResult(
                statement,
                placeholders.stream()
                        .map(Placeholder::name)
                        .toList(),
                modifierBindings,
                resolved.catalogGeneration());
    }

    private static void collectPlaceholders(HogQlQuery query, List<Placeholder> placeholders)
    {
        query.with().forEach(commonTable -> collectPlaceholders(commonTable.query(), placeholders));
        switch (query.body()) {
            case SelectQueryBody select -> {
                select.projections().forEach(projection -> {
                    switch (projection) {
                        case ColumnsList columns -> columns.expressions().forEach(expression -> collectPlaceholders(expression, placeholders));
                        case ColumnsRegex _ -> {}
                        case ExpressionProjection expression -> collectPlaceholders(expression.expression(), placeholders);
                        case Star star -> star.replacements().forEach(replacement -> collectPlaceholders(replacement.expression(), placeholders));
                    }
                });
                select.from().ifPresent(relation -> collectPlaceholders(relation, placeholders));
                select.where().ifPresent(expression -> collectPlaceholders(expression, placeholders));
                select.groupBy().forEach(expression -> collectPlaceholders(expression, placeholders));
                select.having().ifPresent(expression -> collectPlaceholders(expression, placeholders));
                select.windows().forEach(window -> collectPlaceholders(window.specification(), placeholders));
                select.limitBy().ifPresent(limitBy -> {
                    collectPlaceholders(limitBy.limit(), placeholders);
                    limitBy.offset().ifPresent(expression -> collectPlaceholders(expression, placeholders));
                    limitBy.partitionBy().forEach(expression -> collectPlaceholders(expression, placeholders));
                });
            }
            case SetOperation setOperation -> {
                collectPlaceholders(setOperation.left(), placeholders);
                collectPlaceholders(setOperation.right(), placeholders);
            }
        }
        query.orderBy().forEach(sortItem -> collectPlaceholders(sortItem.expression(), placeholders));
        query.limit().ifPresent(expression -> collectPlaceholders(expression, placeholders));
        query.offset().ifPresent(expression -> collectPlaceholders(expression, placeholders));
    }

    private static ResolvedQuery resolveQuery(
            HogQlQuery query,
            Optional<HogQlSemanticCatalogContext> catalogContext,
            HogQlLanguageVersion languageVersion,
            OptionalLong expectedCatalogGeneration,
            boolean modifiersRequireSnapshot,
            boolean v0Profile)
    {
        boolean semanticCandidate = containsSemanticCandidate(query);
        if (!semanticCandidate && !modifiersRequireSnapshot) {
            return new ResolvedQuery(query, Optional.empty());
        }
        if (catalogContext.isEmpty()) {
            if (modifiersRequireSnapshot) {
                throw new HogQlSemanticCatalogException(Failure.UNAVAILABLE, "HogQL semantic catalog snapshot is required for modifiers");
            }
            return new ResolvedQuery(containsFunctionCall(query) ? HogQlFunctionResolver.resolve(query) : query, Optional.empty());
        }
        HogQlSemanticCatalogContext context = catalogContext.orElseThrow();
        PinnedSnapshot pinned = context.snapshotProvider().pin(new PinRequest(
                context.catalog(),
                requireNonNull(languageVersion, "languageVersion is null"),
                expectedCatalogGeneration));
        if (v0Profile) {
            HogQlV0ProfileValidator.validate(query, Optional.of(pinned.snapshot()));
        }
        HogQlQuery resolved = query;
        if (semanticCandidate) {
            HogQlQuery functionsResolved = v0Profile ? HogQlFunctionResolver.resolveV0(query) : HogQlFunctionResolver.resolve(pinned, query);
            if (hasSemanticDefinitions(pinned.snapshot())) {
                resolved = HogQlSemanticResolver.resolve(pinned, functionsResolved)
                        .map(HogQlSemanticResolver.ResolvedQuery::query)
                        .orElse(functionsResolved);
            }
            else {
                resolved = functionsResolved;
            }
        }
        return new ResolvedQuery(resolved, Optional.of(pinned));
    }

    private static boolean hasSemanticDefinitions(io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot snapshot)
    {
        return !snapshot.logicalTables().isEmpty() ||
                !snapshot.virtualTables().isEmpty() ||
                !snapshot.savedQueries().isEmpty() ||
                !snapshot.materializedViews().isEmpty() ||
                !snapshot.expressionFields().isEmpty() ||
                !snapshot.actions().isEmpty() ||
                !snapshot.cohorts().isEmpty();
    }

    private static boolean containsSemanticCandidate(HogQlQuery query)
    {
        return containsFunctionCall(query) ||
                query.with().stream().anyMatch(commonTable -> containsSemanticCandidate(commonTable.query())) ||
                switch (query.body()) {
                    case SelectQueryBody select -> select.from().map(HogQlCompiler::containsSemanticCandidate).orElse(false);
                    case SetOperation setOperation -> containsSemanticCandidate(setOperation.left()) || containsSemanticCandidate(setOperation.right());
                };
    }

    private static boolean containsSemanticCandidate(Relation relation)
    {
        return switch (relation) {
            case AliasedRelation alias -> containsSemanticCandidate(alias.relation());
            case CommonTableReference _ -> false;
            case JoinRelation join -> containsSemanticCandidate(join.left()) || containsSemanticCandidate(join.right());
            case HogQlQuery.PivotRelation pivot -> containsSemanticCandidate(pivot.input()) ||
                    pivot.aggregations().stream().anyMatch(aggregation -> containsFunctionCall(aggregation.expression()));
            case SubqueryRelation subquery -> containsSemanticCandidate(subquery.query());
            case TablePlaceholder _ -> false;
            case HogQlQuery.TableReference table -> table.parts().size() == 1;
            case UnnestRelation unnest -> unnest.expressions().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case ValuesRelation _ -> false;
        };
    }

    private static boolean containsFunctionCall(HogQlQuery query)
    {
        return query.with().stream().anyMatch(commonTable -> containsFunctionCall(commonTable.query())) ||
                switch (query.body()) {
                    case SelectQueryBody select -> select.projections().stream().anyMatch(HogQlCompiler::containsFunctionCall) ||
                            select.from().map(HogQlCompiler::containsFunctionCall).orElse(false) ||
                            select.where().map(HogQlCompiler::containsFunctionCall).orElse(false) ||
                            select.groupBy().stream().anyMatch(HogQlCompiler::containsFunctionCall) ||
                            select.having().map(HogQlCompiler::containsFunctionCall).orElse(false) ||
                            select.windows().stream().map(WindowDefinition::specification).anyMatch(HogQlCompiler::containsFunctionCall) ||
                            select.limitBy().map(limitBy ->
                                    containsFunctionCall(limitBy.limit()) ||
                                            containsFunctionCall(limitBy.offset()) ||
                                            limitBy.partitionBy().stream().anyMatch(HogQlCompiler::containsFunctionCall)).orElse(false);
                    case SetOperation set -> containsFunctionCall(set.left()) || containsFunctionCall(set.right());
                } ||
                query.orderBy().stream().anyMatch(sortItem -> containsFunctionCall(sortItem.expression())) ||
                query.limit().map(HogQlCompiler::containsFunctionCall).orElse(false) ||
                query.offset().map(HogQlCompiler::containsFunctionCall).orElse(false);
    }

    private static boolean containsFunctionCall(Relation relation)
    {
        return switch (relation) {
            case AliasedRelation alias -> containsFunctionCall(alias.relation());
            case CommonTableReference _ -> false;
            case JoinRelation join -> containsFunctionCall(join.left()) ||
                    containsFunctionCall(join.right()) ||
                    join.criteria().filter(JoinOn.class::isInstance)
                            .map(JoinOn.class::cast)
                            .map(JoinOn::expression)
                            .map(HogQlCompiler::containsFunctionCall)
                            .orElse(false);
            case HogQlQuery.PivotRelation pivot -> containsFunctionCall(pivot.input()) ||
                    pivot.aggregations().stream().anyMatch(aggregation -> containsFunctionCall(aggregation.expression())) ||
                    pivot.pivotColumns().stream().anyMatch(HogQlCompiler::containsFunctionCall) ||
                    pivot.valueGroups().stream()
                            .flatMap(group -> group.values().stream())
                            .anyMatch(HogQlCompiler::containsFunctionCall) ||
                    pivot.groupBy().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case SubqueryRelation subquery -> containsFunctionCall(subquery.query());
            case TablePlaceholder _, HogQlQuery.TableReference _ -> false;
            case UnnestRelation unnest -> unnest.expressions().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case ValuesRelation values -> values.rows().stream()
                    .flatMap(List::stream)
                    .anyMatch(HogQlCompiler::containsFunctionCall);
        };
    }

    private static boolean containsFunctionCall(Projection projection)
    {
        return switch (projection) {
            case ColumnsList columns -> columns.expressions().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case ColumnsRegex _ -> false;
            case ExpressionProjection expression -> containsFunctionCall(expression.expression());
            case Star star -> star.replacements().stream().anyMatch(replacement -> containsFunctionCall(replacement.expression()));
        };
    }

    private static boolean containsFunctionCall(Expression expression)
    {
        return switch (expression) {
            case ArrayExpression array -> array.values().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case BetweenExpression between -> containsFunctionCall(between.value()) || containsFunctionCall(between.min()) || containsFunctionCall(between.max());
            case BinaryExpression binary -> containsFunctionCall(binary.left()) || containsFunctionCall(binary.right());
            case CaseExpression caseExpression -> caseExpression.operand().map(HogQlCompiler::containsFunctionCall).orElse(false) ||
                    caseExpression.whenClauses().stream().anyMatch(when -> containsFunctionCall(when.operand()) || containsFunctionCall(when.result())) ||
                    caseExpression.defaultValue().map(HogQlCompiler::containsFunctionCall).orElse(false);
            case CastExpression cast -> containsFunctionCall(cast.value());
            case ColumnReference _, Literal _, Placeholder _ -> false;
            case FunctionCall _ -> true;
            case InCohortExpression _ -> true;
            case InExpression in -> containsFunctionCall(in.value()) || in.values().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case InSubqueryExpression in -> containsFunctionCall(in.value()) || containsFunctionCall(in.query());
            case IntervalExpression interval -> containsFunctionCall(interval.value());
            case IsNullExpression isNull -> containsFunctionCall(isNull.value());
            case LambdaExpression lambda -> containsFunctionCall(lambda.body());
            case MemberAccessExpression memberAccess -> containsFunctionCall(memberAccess.base());
            case ScalarSubqueryExpression subquery -> containsSemanticCandidate(subquery.query());
            case SubscriptExpression subscript -> containsFunctionCall(subscript.base()) || containsFunctionCall(subscript.index());
            case TupleExpression tuple -> tuple.values().stream().anyMatch(HogQlCompiler::containsFunctionCall);
            case UnaryExpression unary -> containsFunctionCall(unary.operand());
        };
    }

    private static boolean containsFunctionCall(Window window)
    {
        return switch (window) {
            case WindowReference _ -> false;
            case WindowSpecification specification -> specification.partitionBy().stream().anyMatch(HogQlCompiler::containsFunctionCall) ||
                    specification.orderBy().stream().anyMatch(sortItem -> containsFunctionCall(sortItem.expression())) ||
                    specification.frame().map(frame -> containsFunctionCall(frame.start().value()) ||
                            frame.end().map(bound -> containsFunctionCall(bound.value())).orElse(false)).orElse(false);
        };
    }

    private static boolean containsFunctionCall(Optional<Expression> expression)
    {
        return expression.map(HogQlCompiler::containsFunctionCall).orElse(false);
    }

    private record ResolvedQuery(HogQlQuery query, Optional<PinnedSnapshot> pinnedSnapshot)
    {
        private ResolvedQuery
        {
            query = requireNonNull(query, "query is null");
            pinnedSnapshot = requireNonNull(pinnedSnapshot, "pinnedSnapshot is null");
        }

        public OptionalLong catalogGeneration()
        {
            return pinnedSnapshot
                    .map(snapshot -> OptionalLong.of(snapshot.generation()))
                    .orElseGet(OptionalLong::empty);
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
        switch (query.body()) {
            case SelectQueryBody select -> select.from().ifPresent(HogQlCompiler::validateRelation);
            case SetOperation setOperation -> {
                validateQuery(setOperation.left());
                validateQuery(setOperation.right());
            }
        }
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
                function.window().ifPresent(window -> collectPlaceholders(window, placeholders));
            }
            case InCohortExpression in -> {
                collectPlaceholders(in.value(), placeholders);
                collectPlaceholders(in.cohort(), placeholders);
            }
            case InExpression in -> {
                collectPlaceholders(in.value(), placeholders);
                in.values().forEach(value -> collectPlaceholders(value, placeholders));
            }
            case InSubqueryExpression in -> {
                collectPlaceholders(in.value(), placeholders);
                collectPlaceholders(in.query(), placeholders);
            }
            case IntervalExpression interval -> collectPlaceholders(interval.value(), placeholders);
            case IsNullExpression isNull -> collectPlaceholders(isNull.value(), placeholders);
            case LambdaExpression lambda -> collectPlaceholders(lambda.body(), placeholders);
            case Literal _ -> {}
            case MemberAccessExpression memberAccess -> collectPlaceholders(memberAccess.base(), placeholders);
            case Placeholder placeholder -> placeholders.add(placeholder);
            case ScalarSubqueryExpression subquery -> collectPlaceholders(subquery.query(), placeholders);
            case SubscriptExpression subscript -> {
                collectPlaceholders(subscript.base(), placeholders);
                collectPlaceholders(subscript.index(), placeholders);
            }
            case TupleExpression tuple -> tuple.values().forEach(value -> collectPlaceholders(value, placeholders));
            case UnaryExpression unary -> collectPlaceholders(unary.operand(), placeholders);
        }
    }

    private static void collectPlaceholders(Window window, List<Placeholder> placeholders)
    {
        switch (window) {
            case WindowReference _ -> {}
            case WindowSpecification specification -> {
                specification.partitionBy().forEach(expression -> collectPlaceholders(expression, placeholders));
                specification.orderBy().forEach(sortItem -> collectPlaceholders(sortItem.expression(), placeholders));
                specification.frame().ifPresent(frame -> {
                    frame.start().value().ifPresent(value -> collectPlaceholders(value, placeholders));
                    frame.end().flatMap(HogQlQuery.FrameBound::value).ifPresent(value -> collectPlaceholders(value, placeholders));
                });
            }
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
            case HogQlQuery.PivotRelation pivot -> validateRelation(pivot.input());
            case SubqueryRelation subquery -> validateQuery(subquery.query());
            case TablePlaceholder tablePlaceholder -> throw bindingError(
                    tablePlaceholder.span(),
                    "HogQL parameter placeholders are not supported in table positions: " + tablePlaceholder.placeholder().name());
            case HogQlQuery.TableReference _ -> {}
            case UnnestRelation _ -> {}
            case ValuesRelation _ -> {}
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
            case HogQlQuery.PivotRelation pivot -> {
                collectPlaceholders(pivot.input(), placeholders);
                pivot.aggregations().forEach(aggregation -> collectPlaceholders(aggregation.expression(), placeholders));
                pivot.pivotColumns().forEach(expression -> collectPlaceholders(expression, placeholders));
                pivot.valueGroups().forEach(group -> group.values().forEach(expression -> collectPlaceholders(expression, placeholders)));
                pivot.groupBy().forEach(expression -> collectPlaceholders(expression, placeholders));
            }
            case SubqueryRelation subquery -> collectPlaceholders(subquery.query(), placeholders);
            case TablePlaceholder _ -> {}
            case HogQlQuery.TableReference _ -> {}
            case UnnestRelation unnest -> unnest.expressions().forEach(expression -> collectPlaceholders(expression, placeholders));
            case ValuesRelation values -> values.rows().forEach(row -> row.forEach(expression -> collectPlaceholders(expression, placeholders)));
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

    private static TrinoException unsupportedError(SourceSpan span, String message)
    {
        return new TrinoException(
                io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }
}
