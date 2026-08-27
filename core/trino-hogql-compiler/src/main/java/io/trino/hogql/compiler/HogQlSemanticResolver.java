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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CastRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCallRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.MaterializedViewReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ReferencedField;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SavedQueryReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticOperator;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.VirtualTableDefinition;
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
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Window;
import io.trino.hogql.parser.tree.HogQlQuery.WindowDefinition;
import io.trino.hogql.parser.tree.HogQlQuery.WindowFrame;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_COMPILER_LIMIT_EXCEEDED;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;

final class HogQlSemanticResolver
{
    private final PinnedSnapshot snapshot;
    private final ExpansionBudget expansionBudget;
    private List<TableBinding> bindings = List.of();
    private boolean allRelationsLogical;

    private HogQlSemanticResolver(PinnedSnapshot snapshot)
    {
        this(snapshot, new ExpansionBudget());
    }

    private HogQlSemanticResolver(PinnedSnapshot snapshot, ExpansionBudget expansionBudget)
    {
        this.snapshot = requireNonNull(snapshot, "snapshot is null");
        this.expansionBudget = requireNonNull(expansionBudget, "expansionBudget is null");
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
                        new HogQlSemanticResolver(snapshot, expansionBudget).resolveNestedQuery(commonTable.query()),
                        commonTable.span()))
                .toList();
        if (query.body() instanceof SetOperation setOperation) {
            SetOperation resolved = new SetOperation(
                    setOperation.type(),
                    setOperation.distinct(),
                    new HogQlSemanticResolver(snapshot, expansionBudget).resolveNestedQuery(setOperation.left()),
                    new HogQlSemanticResolver(snapshot, expansionBudget).resolveNestedQuery(setOperation.right()),
                    setOperation.leftParenthesized(),
                    setOperation.rightParenthesized(),
                    setOperation.operatorSpan(),
                    setOperation.span());
            return new HogQlQuery(commonTables, resolved, query.orderBy(), query.limit(), query.offset(), query.span());
        }
        SelectQueryBody select = (SelectQueryBody) query.body();
        Optional<ResolvedRelation> relation = select.from().map(this::resolveRelation);
        bindings = relation.map(ResolvedRelation::bindings).orElse(List.of());
        allRelationsLogical = relation.map(ResolvedRelation::allLogical).orElse(false);
        if (bindings.isEmpty()) {
            return new HogQlQuery(
                    commonTables,
                    select.distinct(),
                    select.projections(),
                    relation.map(ResolvedRelation::relation),
                    select.where(),
                    select.groupBy(),
                    select.having(),
                    select.windows(),
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
                query.windows().stream().map(this::resolveWindowDefinition).toList(),
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
                        .flatMap(binding -> binding.orderedFields().stream()
                                .filter(BoundField::starVisible)
                                .map(field -> new ExpressionProjection(
                                        resolveBoundField(binding, field, binding.starQualifier(bindings.size()), star.span(), expansionBudget),
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
                        new AliasedRelation(child.relation(), alias.alias(), alias.columnAliases(), alias.span()),
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
                    new SubqueryRelation(new HogQlSemanticResolver(snapshot, expansionBudget).resolveNestedQuery(subquery.query()), subquery.span()),
                    List.of(),
                    false);
            case TablePlaceholder placeholder -> new ResolvedRelation(placeholder, List.of(), false);
            case TableReference table -> resolveTable(table);
            case ValuesRelation values -> new ResolvedRelation(values, List.of(), false);
        };
    }

    private JoinUsing resolveJoinUsing(JoinUsing using, List<TableBinding> leftBindings, List<TableBinding> rightBindings)
    {
        List<Identifier> columns = using.columns().stream()
                .map(column -> {
                    List<BoundField> leftFields = matchingFields(leftBindings, column.value());
                    List<BoundField> rightFields = matchingFields(rightBindings, column.value());
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

    private static List<BoundField> matchingFields(List<TableBinding> tableBindings, String name)
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
        String name = table.parts().getFirst().value();
        return resolveSemanticRelation(name, table.span(), new RelationExpansionBudget())
                .orElseGet(() -> new ResolvedRelation(table, List.of(), false));
    }

    private Optional<ResolvedRelation> resolveSemanticRelation(String name, HogQlQuery.SourceSpan span, RelationExpansionBudget budget)
    {
        budget.enter(span);
        try {
            Optional<LogicalTableDefinition> logicalTable = snapshot.logicalTable(name);
            if (logicalTable.isPresent()) {
                return Optional.of(resolveLogicalTable(logicalTable.orElseThrow(), span));
            }
            Optional<MaterializedViewReference> materializedView = snapshot.snapshot().materializedViews().stream()
                    .filter(view -> canonical(view.name()).equals(canonical(name)))
                    .findFirst();
            if (materializedView.isPresent()) {
                return Optional.of(resolveMaterializedView(materializedView.orElseThrow(), span));
            }
            Optional<VirtualTableDefinition> virtualTable = snapshot.snapshot().virtualTables().stream()
                    .filter(table -> canonical(table.name()).equals(canonical(name)))
                    .findFirst();
            if (virtualTable.isPresent()) {
                return Optional.of(resolveVirtualTable(virtualTable.orElseThrow(), span, budget));
            }
            Optional<SavedQueryReference> savedQuery = snapshot.snapshot().savedQueries().stream()
                    .filter(query -> canonical(query.name()).equals(canonical(name)))
                    .findFirst();
            if (savedQuery.isPresent()) {
                return Optional.of(resolveSavedQuery(savedQuery.orElseThrow(), span, budget));
            }
            return Optional.empty();
        }
        finally {
            budget.exit();
        }
    }

    private ResolvedRelation resolveLogicalTable(LogicalTableDefinition definition, HogQlQuery.SourceSpan span)
    {
        TableReference physicalTable = new TableReference(
                List.of(
                        identifier(definition.physicalTable().catalog(), span),
                        identifier(definition.physicalTable().schema(), span),
                        identifier(definition.physicalTable().table(), span)),
                span);
        List<BoundField> fields = new ArrayList<>();
        definition.fields().forEach(field -> fields.add(new BoundField(field.name(), field.physicalColumn(), field.starVisible(), Optional.empty())));
        snapshot.snapshot().expressionFields().stream()
                .filter(field -> canonical(field.table()).equals(canonical(definition.name())))
                .forEach(field -> fields.add(new BoundField(
                        field.name(),
                        new PhysicalIdentifier(field.name(), true),
                        field.starVisible(),
                        Optional.of(field))));
        return new ResolvedRelation(
                physicalTable,
                List.of(tableBinding(
                        definition,
                        canonical(definition.name()),
                        definition.physicalTable().table(),
                        false,
                        fields)),
                true);
    }

    private ResolvedRelation resolveMaterializedView(MaterializedViewReference definition, HogQlQuery.SourceSpan span)
    {
        TableReference physicalTable = new TableReference(
                List.of(
                        identifier(definition.physicalView().catalog(), span),
                        identifier(definition.physicalView().schema(), span),
                        identifier(definition.physicalView().table(), span)),
                span);
        List<BoundField> fields = definition.fields().stream()
                .map(field -> referencedField(field, Optional.empty()))
                .toList();
        return new ResolvedRelation(
                physicalTable,
                List.of(new TableBinding(
                        definition.name(),
                        canonical(definition.name()),
                        new PhysicalIdentifier(definition.physicalView().table().value(), definition.physicalView().table().delimited()),
                        false,
                        fields,
                        TableBinding.fieldMap(fields))),
                true);
    }

    private ResolvedRelation resolveVirtualTable(VirtualTableDefinition definition, HogQlQuery.SourceSpan span, RelationExpansionBudget budget)
    {
        ResolvedRelation source = resolveRelationReference(definition.source(), span, budget);
        List<ProjectedField> projections = definition.projections().stream()
                .map(projection -> new ProjectedField(projection.name(), projection.sourceField(), projection.starVisible()))
                .toList();
        return projectRelation(definition.name(), source, projections, span);
    }

    private ResolvedRelation resolveSavedQuery(SavedQueryReference definition, HogQlQuery.SourceSpan span, RelationExpansionBudget budget)
    {
        ResolvedRelation source = resolveRelationReference(definition.target(), span, budget);
        List<ProjectedField> projections = definition.fields().stream()
                .map(field -> new ProjectedField(field.name(), field.name(), field.starVisible()))
                .toList();
        return projectRelation(definition.name(), source, projections, span);
    }

    private ResolvedRelation resolveRelationReference(RelationReference reference, HogQlQuery.SourceSpan span, RelationExpansionBudget budget)
    {
        ResolvedRelation relation = resolveSemanticRelation(reference.name(), span, budget)
                .orElseThrow(() -> expansionError(span, "HogQL semantic relation references an unavailable target"));
        RelationKind actualKind = semanticRelationKind(reference.name());
        if (actualKind != reference.kind()) {
            throw expansionError(span, "HogQL semantic relation target kind does not match the catalog");
        }
        return relation;
    }

    private RelationKind semanticRelationKind(String name)
    {
        if (snapshot.logicalTable(name).isPresent()) {
            return RelationKind.LOGICAL_TABLE;
        }
        if (snapshot.snapshot().virtualTables().stream().anyMatch(table -> canonical(table.name()).equals(canonical(name)))) {
            return RelationKind.VIRTUAL_TABLE;
        }
        if (snapshot.snapshot().savedQueries().stream().anyMatch(query -> canonical(query.name()).equals(canonical(name)))) {
            return RelationKind.SAVED_QUERY;
        }
        return RelationKind.MATERIALIZED_VIEW;
    }

    private ResolvedRelation projectRelation(String relationName, ResolvedRelation source, List<ProjectedField> projectedFields, HogQlQuery.SourceSpan span)
    {
        List<TableBinding> previousBindings = bindings;
        boolean previousAllRelationsLogical = allRelationsLogical;
        bindings = source.bindings();
        allRelationsLogical = source.allLogical();
        List<Projection> projections;
        try {
            projections = projectedFields.stream()
                    .map(field -> new ExpressionProjection(
                            resolveColumn(new ColumnReference(List.of(new Identifier(field.sourceField(), true, span)), span)),
                            Optional.of(new Identifier(field.name(), true, span))))
                    .map(Projection.class::cast)
                    .toList();
        }
        finally {
            bindings = previousBindings;
            allRelationsLogical = previousAllRelationsLogical;
        }
        HogQlQuery projectedQuery = new HogQlQuery(
                List.of(),
                false,
                projections,
                Optional.of(source.relation()),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                span);
        List<BoundField> fields = projectedFields.stream()
                .map(field -> new BoundField(
                        field.name(),
                        new PhysicalIdentifier(field.name(), true),
                        field.starVisible(),
                        Optional.empty()))
                .toList();
        return new ResolvedRelation(
                new SubqueryRelation(projectedQuery, span),
                List.of(new TableBinding(
                        relationName,
                        canonical(relationName),
                        new PhysicalIdentifier(relationName, true),
                        false,
                        fields,
                        TableBinding.fieldMap(fields))),
                true);
    }

    private static BoundField referencedField(ReferencedField field, Optional<ExpressionFieldDefinition> expression)
    {
        return new BoundField(field.name(), new PhysicalIdentifier(field.name(), true), field.starVisible(), expression);
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
                    function.nameParts(),
                    function.arguments().stream().map(this::resolveExpression).toList(),
                    function.distinct(),
                    resolveSortItems(function.orderBy()),
                    function.filter().map(this::resolveExpression),
                    function.nullTreatment(),
                    function.window().map(this::resolveWindow),
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
            case MemberAccessExpression memberAccess -> new MemberAccessExpression(
                    resolveExpression(memberAccess.base()),
                    memberAccess.member(),
                    memberAccess.span());
            case Placeholder placeholder -> placeholder;
            case SubscriptExpression subscript -> new SubscriptExpression(
                    resolveExpression(subscript.base()),
                    resolveExpression(subscript.index()),
                    subscript.span());
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(this::resolveExpression).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), resolveExpression(unary.operand()), unary.span());
        };
    }

    private WindowDefinition resolveWindowDefinition(WindowDefinition definition)
    {
        return new WindowDefinition(definition.name(), (WindowSpecification) resolveWindow(definition.specification()), definition.span());
    }

    private Window resolveWindow(Window window)
    {
        return switch (window) {
            case WindowReference reference -> reference;
            case WindowSpecification specification -> new WindowSpecification(
                    specification.partitionBy().stream().map(this::resolveExpression).toList(),
                    resolveSortItems(specification.orderBy()),
                    specification.frame().map(this::resolveWindowFrame),
                    specification.span());
        };
    }

    private WindowFrame resolveWindowFrame(WindowFrame frame)
    {
        return new WindowFrame(
                frame.type(),
                new HogQlQuery.FrameBound(
                        frame.start().type(),
                        frame.start().value().map(this::resolveExpression),
                        frame.start().span()),
                frame.end().map(bound -> new HogQlQuery.FrameBound(
                        bound.type(),
                        bound.value().map(this::resolveExpression),
                        bound.span())),
                frame.span());
    }

    private Expression resolveColumn(ColumnReference reference)
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
            BoundField field = binding.orElseThrow().fields().get(canonical(parts.getLast().value()));
            if (field == null) {
                throw resolutionError(reference, parts.getLast().value());
            }
            return resolveBoundField(binding.orElseThrow(), field, Optional.of(binding.orElseThrow().outputQualifier()), reference.span(), expansionBudget);
        }
        if (parts.size() > 2) {
            if (allRelationsLogical) {
                throw resolutionError(reference, parts.getLast().value());
            }
            return reference;
        }
        String logicalName = parts.getLast().value();
        List<FieldMatch> matches = bindings.stream()
                .map(binding -> new FieldMatch(binding, binding.fields().get(canonical(logicalName))))
                .filter(match -> match.field() != null)
                .toList();
        if (matches.size() == 1 && (allRelationsLogical || bindings.size() == 1)) {
            FieldMatch match = matches.getFirst();
            Optional<PhysicalIdentifier> qualifier = bindings.size() > 1 ? Optional.of(match.binding().outputQualifier()) : Optional.empty();
            return resolveBoundField(match.binding(), match.field(), qualifier, reference.span(), expansionBudget);
        }
        if (matches.size() > 1) {
            throw ambiguousResolutionError(reference, logicalName);
        }
        if (allRelationsLogical) {
            throw resolutionError(reference, logicalName);
        }
        return reference;
    }

    private Expression resolveBoundField(
            TableBinding binding,
            BoundField field,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget)
    {
        budget.add(span);
        return field.expression()
                .<Expression>map(expression -> expandRecipe(binding, expression.recipe(), qualifier, span, budget))
                .orElseGet(() -> physicalColumn(field, qualifier, span));
    }

    private static ColumnReference physicalColumn(BoundField field, Optional<PhysicalIdentifier> qualifier, HogQlQuery.SourceSpan span)
    {
        List<Identifier> parts = new ArrayList<>();
        qualifier.ifPresent(identifier -> parts.add(new Identifier(identifier.value(), identifier.delimited(), span)));
        parts.add(new Identifier(field.physicalColumn().value(), field.physicalColumn().delimited(), span));
        return new ColumnReference(parts, span);
    }

    private Expression expandRecipe(
            TableBinding binding,
            ExpressionRecipe recipe,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget)
    {
        budget.enter(span);
        try {
            return switch (recipe) {
                case FieldReferenceRecipe reference -> {
                    BoundField field = binding.fields().get(canonical(reference.field()));
                    if (field == null) {
                        throw expansionError(span, "HogQL expression recipe references an unavailable field");
                    }
                    yield resolveBoundField(binding, field, qualifier, span, budget);
                }
                case LiteralRecipe literal -> typedLiteral(literal.literal(), span);
                case FunctionCallRecipe function -> expandFunction(binding, function, qualifier, span, budget);
                case OperatorRecipe operator -> expandOperator(binding, operator, qualifier, span, budget);
                case CastRecipe cast -> new CastExpression(
                        expandRecipe(binding, cast.expression(), qualifier, span, budget),
                        new Identifier(cast.targetTypeSignature(), false, span),
                        false,
                        span);
            };
        }
        finally {
            budget.exit();
        }
    }

    private Expression expandFunction(
            TableBinding binding,
            FunctionCallRecipe function,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget)
    {
        FunctionCapabilityDefinition capability = snapshot.snapshot().functions().stream()
                .filter(candidate -> canonical(candidate.name()).equals(canonical(function.name())))
                .findFirst()
                .orElseThrow(() -> expansionError(span, "HogQL expression recipe references an unavailable function"));
        if (capability.implementation() == FunctionImplementation.REWRITE || capability.trinoName().isEmpty()) {
            throw unsupportedExpansion(span, "HogQL function recipe requires an unavailable compiler rewrite");
        }
        boolean supportedArity = capability.signatures().stream()
                .anyMatch(signature -> signature.variadic()
                        ? function.arguments().size() >= Math.max(0, signature.argumentTypes().size() - 1)
                        : function.arguments().size() == signature.argumentTypes().size());
        if (!supportedArity) {
            throw expansionError(span, "HogQL function recipe does not match a declared signature");
        }
        return new FunctionCall(
                capability.trinoName().stream()
                        .map(name -> new Identifier(name.value(), name.delimited(), span))
                        .toList(),
                function.arguments().stream()
                        .map(argument -> expandRecipe(binding, argument, qualifier, span, budget))
                        .toList(),
                false,
                List.of(),
                Optional.empty(),
                span);
    }

    private Expression expandOperator(
            TableBinding binding,
            OperatorRecipe operator,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget)
    {
        List<Expression> arguments = operator.arguments().stream()
                .map(argument -> expandRecipe(binding, argument, qualifier, span, budget))
                .toList();
        return switch (operator.operator()) {
            case NOT -> new UnaryExpression(HogQlQuery.UnaryOperator.NOT, arguments.getFirst(), span);
            case NEGATE -> new UnaryExpression(HogQlQuery.UnaryOperator.NEGATE, arguments.getFirst(), span);
            case IS_NULL -> new IsNullExpression(arguments.getFirst(), false, span, span);
            case IS_NOT_NULL -> new IsNullExpression(arguments.getFirst(), true, span, span);
            default -> new BinaryExpression(binaryOperator(operator.operator()), arguments.getFirst(), arguments.getLast(), span);
        };
    }

    private static HogQlQuery.BinaryOperator binaryOperator(SemanticOperator operator)
    {
        return switch (operator) {
            case ADD -> HogQlQuery.BinaryOperator.ADD;
            case SUBTRACT -> HogQlQuery.BinaryOperator.SUBTRACT;
            case MULTIPLY -> HogQlQuery.BinaryOperator.MULTIPLY;
            case DIVIDE -> HogQlQuery.BinaryOperator.DIVIDE;
            case MODULUS -> HogQlQuery.BinaryOperator.MODULO;
            case EQUAL -> HogQlQuery.BinaryOperator.EQUAL;
            case NOT_EQUAL -> HogQlQuery.BinaryOperator.NOT_EQUAL;
            case LESS_THAN -> HogQlQuery.BinaryOperator.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> HogQlQuery.BinaryOperator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN -> HogQlQuery.BinaryOperator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> HogQlQuery.BinaryOperator.GREATER_THAN_OR_EQUAL;
            case AND -> HogQlQuery.BinaryOperator.AND;
            case OR -> HogQlQuery.BinaryOperator.OR;
            case NOT, NEGATE, IS_NULL, IS_NOT_NULL -> throw new IllegalArgumentException("unary operator cannot be lowered as binary");
        };
    }

    private static Expression typedLiteral(TypedLiteral literal, HogQlQuery.SourceSpan span)
    {
        Expression value = switch (literal.encoding()) {
            case NULL -> new Literal(HogQlQuery.LiteralKind.NULL, "", span);
            case BOOLEAN -> new Literal(HogQlQuery.LiteralKind.BOOLEAN, literal.value(), span);
            case INTEGER -> new Literal(HogQlQuery.LiteralKind.INTEGER, literal.value(), span);
            case STRING, DECIMAL, FLOAT, JSON -> new Literal(HogQlQuery.LiteralKind.STRING, literal.value(), span);
            case BASE64 -> new FunctionCall(
                    new Identifier("from_base64", false, span),
                    List.of(new Literal(HogQlQuery.LiteralKind.STRING, literal.value(), span)),
                    false,
                    List.of(),
                    Optional.empty(),
                    span);
        };
        return new CastExpression(value, new Identifier(literal.typeSignature(), false, span), false, span);
    }

    private static TrinoException expansionError(HogQlQuery.SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }

    private static TrinoException unsupportedExpansion(HogQlQuery.SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_UNSUPPORTED_FEATURE,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }

    private static TrinoException limitError(HogQlQuery.SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_COMPILER_LIMIT_EXCEEDED,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }

    private static Identifier identifier(PhysicalIdentifier identifier, TableReference source)
    {
        return new Identifier(identifier.value(), identifier.delimited(), source.span());
    }

    private static Identifier identifier(PhysicalIdentifier identifier, HogQlQuery.SourceSpan span)
    {
        return new Identifier(identifier.value(), identifier.delimited(), span);
    }

    private static TableBinding tableBinding(
            LogicalTableDefinition definition,
            String qualifier,
            PhysicalIdentifier outputQualifier,
            boolean aliased,
            List<BoundField> fields)
    {
        return new TableBinding(
                definition.name(),
                qualifier,
                outputQualifier,
                aliased,
                fields,
                TableBinding.fieldMap(fields));
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
            String relationName,
            String qualifier,
            PhysicalIdentifier outputQualifier,
            boolean aliased,
            List<BoundField> orderedFields,
            Map<String, BoundField> fields)
    {
        private TableBinding(LogicalTableDefinition logicalTable, String qualifier, PhysicalIdentifier outputQualifier, boolean aliased)
        {
            this(logicalTable.name(), qualifier, outputQualifier, aliased, boundFields(logicalTable), fieldMap(boundFields(logicalTable)));
        }

        private TableBinding
        {
            relationName = requireNonNull(relationName, "relationName is null");
            qualifier = requireNonNull(qualifier, "qualifier is null");
            outputQualifier = requireNonNull(outputQualifier, "outputQualifier is null");
            orderedFields = List.copyOf(requireNonNull(orderedFields, "orderedFields is null"));
            fields = Map.copyOf(requireNonNull(fields, "fields is null"));
        }

        private TableBinding withAlias(Identifier alias)
        {
            return new TableBinding(
                    relationName,
                    canonical(alias.value()),
                    new PhysicalIdentifier(alias.value(), alias.delimited()),
                    true,
                    orderedFields,
                    fields);
        }

        private Optional<PhysicalIdentifier> starQualifier(int relationCount)
        {
            return aliased || relationCount > 1 ? Optional.of(outputQualifier) : Optional.empty();
        }

        private static List<BoundField> boundFields(LogicalTableDefinition table)
        {
            return table.fields().stream()
                    .map(field -> new BoundField(field.name(), field.physicalColumn(), field.starVisible(), Optional.empty()))
                    .toList();
        }

        private static Map<String, BoundField> fieldMap(List<BoundField> orderedFields)
        {
            Map<String, BoundField> fields = new HashMap<>();
            orderedFields.forEach(field -> fields.put(canonical(field.name()), field));
            return fields;
        }
    }

    private record BoundField(String name, PhysicalIdentifier physicalColumn, boolean starVisible, Optional<ExpressionFieldDefinition> expression)
    {
        private BoundField
        {
            name = requireNonNull(name, "name is null");
            physicalColumn = requireNonNull(physicalColumn, "physicalColumn is null");
            expression = requireNonNull(expression, "expression is null");
        }
    }

    private record FieldMatch(TableBinding binding, BoundField field) {}

    private record ProjectedField(String name, String sourceField, boolean starVisible)
    {
        private ProjectedField
        {
            name = requireNonNull(name, "name is null");
            sourceField = requireNonNull(sourceField, "sourceField is null");
        }
    }

    private static final class ExpansionBudget
    {
        private static final int MAXIMUM_NODES = 10_000;
        private static final int MAXIMUM_DEPTH = 64;
        private int nodes;
        private int depth;

        private void add(HogQlQuery.SourceSpan span)
        {
            if (++nodes > MAXIMUM_NODES) {
                throw limitError(span, "HogQL semantic expansion exceeded node limit");
            }
        }

        private void enter(HogQlQuery.SourceSpan span)
        {
            add(span);
            if (++depth > MAXIMUM_DEPTH) {
                throw limitError(span, "HogQL semantic expansion exceeded depth limit");
            }
        }

        private void exit()
        {
            depth--;
        }
    }

    private static final class RelationExpansionBudget
    {
        private static final int MAXIMUM_DEPTH = 64;
        private int depth;

        private void enter(HogQlQuery.SourceSpan span)
        {
            if (++depth > MAXIMUM_DEPTH) {
                throw limitError(span, "HogQL semantic relation expansion exceeded depth limit");
            }
        }

        private void exit()
        {
            depth--;
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
