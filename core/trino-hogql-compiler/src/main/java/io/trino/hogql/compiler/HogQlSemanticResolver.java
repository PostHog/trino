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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinUsing;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static java.util.Objects.requireNonNull;

final class HogQlSemanticResolver
{
    private final PinnedSnapshot snapshot;
    private List<TableBinding> bindings = List.of();
    private boolean allRelationsLogical;

    private HogQlSemanticResolver(PinnedSnapshot snapshot)
    {
        this.snapshot = requireNonNull(snapshot, "snapshot is null");
    }

    public static Optional<ResolvedQuery> resolve(PinnedSnapshot snapshot, HogQlQuery query)
    {
        requireNonNull(snapshot, "snapshot is null");
        requireNonNull(query, "query is null");
        HogQlSemanticResolver resolver = new HogQlSemanticResolver(snapshot);
        HogQlQuery resolved = resolver.resolveNestedQuery(query);
        return resolved.equals(query) ? Optional.empty() : Optional.of(new ResolvedQuery(resolved));
    }

    private HogQlQuery resolveNestedQuery(HogQlQuery query)
    {
        List<CommonTableExpression> commonTables = query.with().stream()
                .map(commonTable -> new CommonTableExpression(
                        commonTable.name(),
                        commonTable.columnAliases(),
                        new HogQlSemanticResolver(snapshot).resolveNestedQuery(commonTable.query()),
                        commonTable.span()))
                .toList();
        Optional<ResolvedRelation> relation = query.from().map(this::resolveRelation);
        bindings = relation.map(ResolvedRelation::bindings).orElse(List.of());
        allRelationsLogical = relation.map(ResolvedRelation::allLogical).orElse(false);
        if (bindings.isEmpty()) {
            return new HogQlQuery(
                    commonTables,
                    query.distinct(),
                    query.projections(),
                    relation.map(ResolvedRelation::relation),
                    query.where(),
                    query.groupBy(),
                    query.having(),
                    query.orderBy(),
                    query.limit(),
                    query.offset(),
                    query.span());
        }
        return resolveQuery(query, commonTables, relation.orElseThrow().relation());
    }

    private HogQlQuery resolveQuery(HogQlQuery query, List<CommonTableExpression> commonTables, Relation relation)
    {
        List<Projection> projections = new ArrayList<>();
        query.projections().forEach(projection -> projections.addAll(resolveProjection(projection)));
        HogQlQuery resolved = new HogQlQuery(
                commonTables,
                query.distinct(),
                projections,
                Optional.of(relation),
                query.where().map(this::resolveExpression),
                query.groupBy().stream().map(this::resolveExpression).toList(),
                query.having().map(this::resolveExpression),
                resolveSortItems(query.orderBy()),
                query.limit().map(this::resolveExpression),
                query.offset().map(this::resolveExpression),
                query.span());
        return resolved;
    }

    private List<Projection> resolveProjection(Projection projection)
    {
        return switch (projection) {
            case Star star -> {
                if (!allRelationsLogical) {
                    yield List.of(star);
                }
                yield bindings.stream()
                        .flatMap(binding -> binding.logicalTable().fields().stream()
                                .filter(LogicalFieldDefinition::starVisible)
                                .map(field -> new ExpressionProjection(
                                        physicalColumn(field, binding.starQualifier(bindings.size()), star.span()),
                                        Optional.of(new Identifier(field.name(), true, star.span())))))
                        .map(Projection.class::cast)
                        .toList();
            }
            case ExpressionProjection expressionProjection -> {
                Expression resolved = resolveExpression(expressionProjection.expression());
                Optional<Identifier> alias = expressionProjection.alias();
                if (alias.isEmpty() && expressionProjection.expression() instanceof ColumnReference reference) {
                    alias = Optional.of(reference.parts().getLast());
                }
                yield List.of(new ExpressionProjection(resolved, alias));
            }
        };
    }

    private ResolvedRelation resolveRelation(Relation relation)
    {
        return switch (relation) {
            case AliasedRelation alias -> {
                ResolvedRelation child = resolveRelation(alias.relation());
                List<TableBinding> aliasedBindings = child.bindings().stream()
                        .map(binding -> binding.withAlias(alias.alias()))
                        .toList();
                yield new ResolvedRelation(
                        new AliasedRelation(child.relation(), alias.alias(), alias.span()),
                        aliasedBindings,
                        child.allLogical());
            }
            case CommonTableReference commonTable -> new ResolvedRelation(commonTable, List.of(), false);
            case JoinRelation join -> {
                ResolvedRelation left = resolveRelation(join.left());
                ResolvedRelation right = resolveRelation(join.right());
                List<TableBinding> joinBindings = new ArrayList<>(left.bindings());
                joinBindings.addAll(right.bindings());
                List<TableBinding> previousBindings = bindings;
                boolean previousAllRelationsLogical = allRelationsLogical;
                bindings = List.copyOf(joinBindings);
                allRelationsLogical = left.allLogical() && right.allLogical();
                Optional<HogQlQuery.JoinCriteria> criteria = join.criteria().map(value -> switch (value) {
                    case JoinOn on -> new JoinOn(resolveExpression(on.expression()), on.span());
                    case JoinUsing using -> resolveJoinUsing(using, left.bindings(), right.bindings());
                });
                bindings = previousBindings;
                allRelationsLogical = previousAllRelationsLogical;
                yield new ResolvedRelation(
                        new JoinRelation(join.type(), left.relation(), right.relation(), criteria, join.span()),
                        joinBindings,
                        left.allLogical() && right.allLogical());
            }
            case SubqueryRelation subquery -> new ResolvedRelation(
                    new SubqueryRelation(new HogQlSemanticResolver(snapshot).resolveNestedQuery(subquery.query()), subquery.span()),
                    List.of(),
                    false);
            case TablePlaceholder placeholder -> new ResolvedRelation(placeholder, List.of(), false);
            case TableReference table -> resolveTable(table);
        };
    }

    private JoinUsing resolveJoinUsing(JoinUsing using, List<TableBinding> leftBindings, List<TableBinding> rightBindings)
    {
        List<Identifier> columns = using.columns().stream()
                .map(column -> {
                    List<LogicalFieldDefinition> leftFields = matchingFields(leftBindings, column.value());
                    List<LogicalFieldDefinition> rightFields = matchingFields(rightBindings, column.value());
                    if (leftFields.size() != 1 || rightFields.size() != 1) {
                        throw resolutionError(new ColumnReference(List.of(column), column.span()), column.value());
                    }
                    PhysicalIdentifier left = leftFields.getFirst().physicalColumn();
                    PhysicalIdentifier right = rightFields.getFirst().physicalColumn();
                    if (!left.equals(right)) {
                        throw incompatibleUsingResolutionError(column);
                    }
                    return new Identifier(left.value(), left.delimited(), column.span());
                })
                .toList();
        return new JoinUsing(columns, using.span());
    }

    private static List<LogicalFieldDefinition> matchingFields(List<TableBinding> tableBindings, String name)
    {
        return tableBindings.stream()
                .map(TableBinding::fields)
                .map(fields -> fields.get(canonical(name)))
                .filter(field -> field != null)
                .toList();
    }

    private ResolvedRelation resolveTable(TableReference table)
    {
        if (table.parts().size() != 1) {
            return new ResolvedRelation(table, List.of(), false);
        }
        Optional<LogicalTableDefinition> logicalTable = snapshot.logicalTable(table.parts().getFirst().value());
        if (logicalTable.isEmpty()) {
            return new ResolvedRelation(table, List.of(), false);
        }
        LogicalTableDefinition definition = logicalTable.orElseThrow();
        TableReference physicalTable = new TableReference(
                List.of(
                        identifier(definition.physicalTable().catalog(), table),
                        identifier(definition.physicalTable().schema(), table),
                        identifier(definition.physicalTable().table(), table)),
                table.span());
        return new ResolvedRelation(
                physicalTable,
                List.of(new TableBinding(
                        definition,
                        canonical(definition.name()),
                        definition.physicalTable().table(),
                        false)),
                true);
    }

    private List<SortItem> resolveSortItems(List<SortItem> sortItems)
    {
        return sortItems.stream()
                .map(sortItem -> new SortItem(resolveExpression(sortItem.expression()), sortItem.direction(), sortItem.nullPlacement(), sortItem.span()))
                .toList();
    }

    private Expression resolveExpression(Expression expression)
    {
        return switch (expression) {
            case ArrayExpression array -> new ArrayExpression(array.values().stream().map(this::resolveExpression).toList(), array.span());
            case BetweenExpression between -> new BetweenExpression(
                    resolveExpression(between.value()),
                    resolveExpression(between.min()),
                    resolveExpression(between.max()),
                    between.negated(),
                    between.predicateSpan(),
                    between.span());
            case BinaryExpression binary -> new BinaryExpression(
                    binary.operator(),
                    resolveExpression(binary.left()),
                    resolveExpression(binary.right()),
                    binary.span());
            case CaseExpression caseExpression -> new CaseExpression(
                    caseExpression.operand().map(this::resolveExpression),
                    caseExpression.whenClauses().stream()
                            .map(when -> new CaseWhen(resolveExpression(when.operand()), resolveExpression(when.result()), when.span()))
                            .toList(),
                    caseExpression.defaultValue().map(this::resolveExpression),
                    caseExpression.span());
            case CastExpression cast -> new CastExpression(resolveExpression(cast.value()), cast.type(), cast.safe(), cast.span());
            case ColumnReference reference -> resolveColumn(reference);
            case FunctionCall function -> new FunctionCall(
                    function.name(),
                    function.arguments().stream().map(this::resolveExpression).toList(),
                    function.distinct(),
                    resolveSortItems(function.orderBy()),
                    function.filter().map(this::resolveExpression),
                    function.span());
            case InExpression in -> new InExpression(
                    resolveExpression(in.value()),
                    in.values().stream().map(this::resolveExpression).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    resolveExpression(isNull.value()),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case Literal literal -> literal;
            case Placeholder placeholder -> placeholder;
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(this::resolveExpression).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), resolveExpression(unary.operand()), unary.span());
        };
    }

    private ColumnReference resolveColumn(ColumnReference reference)
    {
        List<Identifier> parts = reference.parts();
        if (parts.size() == 2) {
            Optional<TableBinding> binding = bindings.stream()
                    .filter(candidate -> candidate.qualifier().equals(canonical(parts.getFirst().value())))
                    .findFirst();
            if (binding.isEmpty()) {
                if (allRelationsLogical) {
                    throw resolutionError(reference, parts.getLast().value());
                }
                return reference;
            }
            LogicalFieldDefinition field = binding.orElseThrow().fields().get(canonical(parts.getLast().value()));
            if (field == null) {
                throw resolutionError(reference, parts.getLast().value());
            }
            return physicalColumn(field, Optional.of(binding.orElseThrow().outputQualifier()), reference.span());
        }
        if (parts.size() > 2) {
            if (allRelationsLogical) {
                throw resolutionError(reference, parts.getLast().value());
            }
            return reference;
        }
        String logicalName = parts.getLast().value();
        List<LogicalFieldDefinition> matches = bindings.stream()
                .map(TableBinding::fields)
                .map(fields -> fields.get(canonical(logicalName)))
                .filter(field -> field != null)
                .toList();
        if (matches.size() == 1 && (allRelationsLogical || bindings.size() == 1)) {
            return physicalColumn(matches.getFirst(), Optional.empty(), reference.span());
        }
        if (matches.size() > 1) {
            throw ambiguousResolutionError(reference, logicalName);
        }
        if (allRelationsLogical) {
            throw resolutionError(reference, logicalName);
        }
        return reference;
    }

    private static ColumnReference physicalColumn(LogicalFieldDefinition field, Optional<PhysicalIdentifier> qualifier, HogQlQuery.SourceSpan span)
    {
        List<Identifier> parts = new ArrayList<>();
        qualifier.ifPresent(identifier -> parts.add(new Identifier(identifier.value(), identifier.delimited(), span)));
        parts.add(new Identifier(field.physicalColumn().value(), field.physicalColumn().delimited(), span));
        return new ColumnReference(parts, span);
    }

    private static Identifier identifier(PhysicalIdentifier identifier, TableReference source)
    {
        return new Identifier(identifier.value(), identifier.delimited(), source.span());
    }

    private static String canonical(String value)
    {
        return value.toLowerCase(Locale.ENGLISH);
    }

    private static TrinoException resolutionError(ColumnReference reference, String name)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(reference.span().startLine(), reference.span().startColumn())),
                "Unknown HogQL field: " + name,
                null);
    }

    private static TrinoException ambiguousResolutionError(ColumnReference reference, String name)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(reference.span().startLine(), reference.span().startColumn())),
                "Ambiguous HogQL field: " + name,
                null);
    }

    private static TrinoException incompatibleUsingResolutionError(Identifier identifier)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(identifier.span().startLine(), identifier.span().startColumn())),
                "HogQL USING field maps to different physical columns: " + identifier.value(),
                null);
    }

    private record ResolvedRelation(Relation relation, List<TableBinding> bindings, boolean allLogical)
    {
        private ResolvedRelation
        {
            relation = requireNonNull(relation, "relation is null");
            bindings = List.copyOf(requireNonNull(bindings, "bindings is null"));
        }
    }

    private record TableBinding(
            LogicalTableDefinition logicalTable,
            String qualifier,
            PhysicalIdentifier outputQualifier,
            boolean aliased,
            Map<String, LogicalFieldDefinition> fields)
    {
        private TableBinding(LogicalTableDefinition logicalTable, String qualifier, PhysicalIdentifier outputQualifier, boolean aliased)
        {
            this(logicalTable, qualifier, outputQualifier, aliased, fieldMap(logicalTable));
        }

        private TableBinding
        {
            logicalTable = requireNonNull(logicalTable, "logicalTable is null");
            qualifier = requireNonNull(qualifier, "qualifier is null");
            outputQualifier = requireNonNull(outputQualifier, "outputQualifier is null");
            fields = Map.copyOf(requireNonNull(fields, "fields is null"));
        }

        private TableBinding withAlias(Identifier alias)
        {
            return new TableBinding(
                    logicalTable,
                    canonical(alias.value()),
                    new PhysicalIdentifier(alias.value(), alias.delimited()),
                    true,
                    fields);
        }

        private Optional<PhysicalIdentifier> starQualifier(int relationCount)
        {
            return aliased || relationCount > 1 ? Optional.of(outputQualifier) : Optional.empty();
        }

        private static Map<String, LogicalFieldDefinition> fieldMap(LogicalTableDefinition table)
        {
            Map<String, LogicalFieldDefinition> fields = new HashMap<>();
            table.fields().forEach(field -> fields.put(canonical(field.name()), field));
            return fields;
        }
    }

    record ResolvedQuery(HogQlQuery query)
    {
        ResolvedQuery
        {
            query = requireNonNull(query, "query is null");
        }
    }
}
