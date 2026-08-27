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

import io.trino.hogql.compiler.HogQlProjectionDemand.RequiredOutputs;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ActionReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ArgumentReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CastRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CohortReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionArgument;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCallRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyProjectionDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.MaterializedViewReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PredicateRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyLookupRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ReferencedField;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipJoinSide;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SavedQueryReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ScopedFieldReferenceRecipe;
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
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InCohortExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinUsing;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.StarReplacement;
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
import io.trino.re2j.Pattern;
import io.trino.re2j.PatternSyntaxException;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_COMPILER_LIMIT_EXCEEDED;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;

final class HogQlSemanticResolver
{
    private static final String MATCHES_ACTION = "matchesaction";

    private final PinnedSnapshot snapshot;
    private final ExpansionBudget expansionBudget;
    private final Optional<RequiredOutputs> requiredOutputs;
    private final Map<RelationshipPathKey, TableBinding> relationshipPaths = new LinkedHashMap<>();
    private List<TableBinding> bindings = List.of();
    private boolean allRelationsLogical;
    private Optional<Relation> expandedRelation = Optional.empty();
    private int generatedRelationId;

    private HogQlSemanticResolver(PinnedSnapshot snapshot)
    {
        this(snapshot, new ExpansionBudget(), Optional.empty());
    }

    private HogQlSemanticResolver(PinnedSnapshot snapshot, ExpansionBudget expansionBudget)
    {
        this(snapshot, expansionBudget, Optional.empty());
    }

    private HogQlSemanticResolver(PinnedSnapshot snapshot, ExpansionBudget expansionBudget, Optional<RequiredOutputs> requiredOutputs)
    {
        this.snapshot = requireNonNull(snapshot, "snapshot is null");
        this.expansionBudget = requireNonNull(expansionBudget, "expansionBudget is null");
        this.requiredOutputs = requireNonNull(requiredOutputs, "requiredOutputs is null");
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
        HogQlProjectionDemand projectionDemand = HogQlProjectionDemand.collect(query);
        Optional<ResolvedRelation> relation = select.from().map(value -> resolveRelation(value, projectionDemand));
        bindings = relation.map(ResolvedRelation::bindings).orElse(List.of());
        allRelationsLogical = relation.map(ResolvedRelation::allLogical).orElse(false);
        expandedRelation = relation.map(ResolvedRelation::relation);
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
        return resolveQuery(query, commonTables);
    }

    private HogQlQuery resolveQuery(HogQlQuery query, List<CommonTableExpression> commonTables)
    {
        List<Projection> projections = new ArrayList<>();
        query.projections().forEach(projection -> projections.addAll(resolveProjection(projection)));
        if (projections.isEmpty() && requiredOutputs.isPresent()) {
            projections.add(new ExpressionProjection(
                    new Literal(HogQlQuery.LiteralKind.INTEGER, "1", query.span()),
                    Optional.of(new Identifier("__hogql_pruned", true, query.span()))));
        }
        Optional<Expression> where = query.where().map(this::resolveExpression);
        List<Expression> groupBy = query.groupBy().stream().map(this::resolveExpression).toList();
        Optional<Expression> having = query.having().map(this::resolveExpression);
        List<WindowDefinition> windows = query.windows().stream().map(this::resolveWindowDefinition).toList();
        List<SortItem> orderBy = resolveSortItems(query.orderBy());
        Optional<Expression> limit = query.limit().map(this::resolveExpression);
        Optional<Expression> offset = query.offset().map(this::resolveExpression);
        return new HogQlQuery(
                commonTables,
                query.distinct(),
                projections,
                expandedRelation,
                where,
                groupBy,
                having,
                windows,
                orderBy,
                limit,
                offset,
                query.span());
    }

    private List<Projection> resolveProjection(Projection projection)
    {
        return switch (projection) {
            case ColumnsList columns -> resolveColumnsList(columns);
            case ColumnsRegex columns -> resolveColumnsRegex(columns);
            case Star star -> resolveStar(star);
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

    private List<Projection> resolveColumnsList(ColumnsList columns)
    {
        return columns.expressions().stream()
                .flatMap(expression -> resolveProjection(new ExpressionProjection(expression, Optional.empty())).stream())
                .toList();
    }

    private List<Projection> resolveColumnsRegex(ColumnsRegex columns)
    {
        if (!allRelationsLogical) {
            throw unsupportedColumns(columns.span());
        }
        Pattern pattern;
        try {
            pattern = Pattern.compile(columns.pattern());
        }
        catch (PatternSyntaxException _) {
            throw semanticEntityError(HOGQL_RESOLUTION_ERROR, columns.patternSpan(), "Invalid HogQL COLUMNS regex: " + columns.pattern());
        }
        List<Projection> projections = bindings.stream()
                .flatMap(binding -> binding.orderedFields().stream()
                        .filter(BoundField::starVisible)
                        .filter(field -> pattern.find(utf8Slice(field.name())))
                        .map(field -> new ExpressionProjection(
                                resolveBoundField(binding, field, binding.starQualifier(bindings.size()), columns.span(), expansionBudget),
                                Optional.of(new Identifier(field.name(), true, columns.span())))))
                .map(Projection.class::cast)
                .toList();
        if (projections.isEmpty()) {
            throw semanticEntityError(HOGQL_RESOLUTION_ERROR, columns.patternSpan(), "No HogQL fields matched COLUMNS regex: " + columns.pattern());
        }
        return projections;
    }

    private List<Projection> resolveStar(Star star)
    {
        Optional<List<Projection>> lazyStar = resolveLazyStar(star);
        if (lazyStar.isPresent()) {
            return lazyStar.orElseThrow();
        }

        List<TableBinding> starBindings;
        boolean qualified = !star.qualifier().isEmpty();
        if (qualified) {
            starBindings = bindings.stream()
                    .filter(binding -> matchesStarQualifier(binding, star.qualifier()))
                    .toList();
            if (starBindings.size() > 1) {
                throw starResolutionError(star.qualifier().getFirst(), "Ambiguous HogQL star qualifier: " + starQualifier(star));
            }
            if (starBindings.isEmpty()) {
                if (allRelationsLogical) {
                    throw starResolutionError(star.qualifier().getFirst(), "Unknown HogQL star qualifier: " + starQualifier(star));
                }
                return List.of(star);
            }
        }
        else {
            if (!allRelationsLogical) {
                return List.of(star);
            }
            starBindings = bindings;
        }

        Set<StarField> exclusions = resolveStarExclusions(star, starBindings);
        Map<StarField, StarReplacement> replacements = resolveStarReplacements(star, starBindings, exclusions);
        return starBindings.stream()
                .flatMap(binding -> binding.orderedFields().stream()
                        .filter(BoundField::starVisible)
                        .filter(field -> !exclusions.contains(new StarField(binding, field)))
                        .filter(field -> projectionDemanded(field.name()))
                        .map(field -> {
                            StarReplacement replacement = replacements.get(new StarField(binding, field));
                            Expression expression = replacement == null
                                    ? resolveBoundField(
                                    binding,
                                    field,
                                    qualified ? Optional.of(binding.outputQualifier()) : binding.starQualifier(bindings.size()),
                                    star.span(),
                                    expansionBudget)
                                    : resolveExpression(replacement.expression());
                            return new ExpressionProjection(expression, Optional.of(new Identifier(field.name(), true, star.span())));
                        }))
                .map(Projection.class::cast)
                .toList();
    }

    private Optional<List<Projection>> resolveLazyStar(Star star)
    {
        if (star.qualifier().isEmpty() || star.qualifier().size() > 2) {
            return Optional.empty();
        }

        List<LazyStar> matches;
        if (star.qualifier().size() == 1) {
            String lazyName = star.qualifier().getFirst().value();
            matches = bindings.stream()
                    .flatMap(binding -> lazyTable(binding, lazyName)
                            .map(definition -> java.util.stream.Stream.of(new LazyStar(binding, definition)))
                            .orElseGet(java.util.stream.Stream::empty))
                    .toList();
        }
        else {
            Identifier owner = star.qualifier().getFirst();
            String lazyName = star.qualifier().getLast().value();
            matches = bindings.stream()
                    .filter(binding -> matchesStarQualifier(binding, List.of(owner)))
                    .flatMap(binding -> lazyTable(binding, lazyName)
                            .map(definition -> java.util.stream.Stream.of(new LazyStar(binding, definition)))
                            .orElseGet(java.util.stream.Stream::empty))
                    .toList();
        }
        if (matches.isEmpty()) {
            return Optional.empty();
        }
        if (matches.size() > 1) {
            throw starResolutionError(star.qualifier().getFirst(), "Ambiguous HogQL star qualifier: " + starQualifier(star));
        }

        LazyStar lazyStar = matches.getFirst();
        List<LazyProjectionDefinition> visible = lazyStar.definition().projections().stream()
                .filter(LazyProjectionDefinition::starVisible)
                .toList();
        Set<String> exclusions = new HashSet<>();
        for (ColumnReference exclusion : star.exclusions()) {
            Identifier name = exclusion.parts().getLast();
            LazyProjectionDefinition matched = visible.stream()
                    .filter(projection -> matchesIdentifier(name, projection.name()))
                    .findFirst()
                    .orElseThrow(() -> starResolutionError(exclusion.span(), "Unknown HogQL star exclusion: " + identifierPath(exclusion.parts())));
            if (!exclusions.add(canonical(matched.name()))) {
                throw starResolutionError(exclusion.span(), "Duplicate HogQL star exclusion: " + identifierPath(exclusion.parts()));
            }
        }
        Map<String, StarReplacement> replacements = new HashMap<>();
        for (StarReplacement replacement : star.replacements()) {
            LazyProjectionDefinition matched = visible.stream()
                    .filter(projection -> !exclusions.contains(canonical(projection.name())))
                    .filter(projection -> matchesIdentifier(replacement.target(), projection.name()))
                    .findFirst()
                    .orElseThrow(() -> starResolutionError(replacement.target(), "Unknown HogQL star replacement: " + replacement.target().value()));
            if (replacements.putIfAbsent(canonical(matched.name()), replacement) != null) {
                throw starResolutionError(replacement.target(), "Duplicate HogQL star replacement: " + replacement.target().value());
            }
        }

        return Optional.of(visible.stream()
                .filter(projection -> !exclusions.contains(canonical(projection.name())))
                .filter(projection -> projectionDemanded(projection.name()))
                .map(projection -> {
                    StarReplacement replacement = replacements.get(canonical(projection.name()));
                    Expression expression = replacement == null
                            ? resolveLazyProjection(lazyStar.binding(), lazyStar.definition(), projection, star.span())
                            : resolveExpression(replacement.expression());
                    return new ExpressionProjection(expression, Optional.of(new Identifier(projection.name(), true, star.span())));
                })
                .map(Projection.class::cast)
                .toList());
    }

    private Expression resolveLazyProjection(
            TableBinding owner,
            LazyTableDefinition definition,
            LazyProjectionDefinition projection,
            HogQlQuery.SourceSpan span)
    {
        TableBinding terminal = ensureRelationshipPath(owner, definition.relationshipPath(), span);
        return expandRecipe(
                terminal,
                projection.recipe(),
                Optional.of(terminal.outputQualifier()),
                span,
                expansionBudget);
    }

    private boolean projectionDemanded(String name)
    {
        return requiredOutputs.map(outputs -> outputs.includes(name)).orElse(true);
    }

    private static Map<StarField, StarReplacement> resolveStarReplacements(Star star, List<TableBinding> starBindings, Set<StarField> exclusions)
    {
        Map<StarField, StarReplacement> replacements = new HashMap<>();
        for (StarReplacement replacement : star.replacements()) {
            List<StarField> matchedFields = starBindings.stream()
                    .flatMap(binding -> binding.orderedFields().stream()
                            .filter(BoundField::starVisible)
                            .map(field -> new StarField(binding, field)))
                    .filter(field -> !exclusions.contains(field))
                    .filter(field -> matchesIdentifier(replacement.target(), field.field().name()))
                    .distinct()
                    .toList();
            if (matchedFields.isEmpty()) {
                throw starResolutionError(replacement.target(), "Unknown HogQL star replacement: " + replacement.target().value());
            }
            if (matchedFields.stream().anyMatch(replacements::containsKey)) {
                throw starResolutionError(replacement.target(), "Duplicate HogQL star replacement: " + replacement.target().value());
            }
            matchedFields.forEach(field -> replacements.put(field, replacement));
        }
        return replacements;
    }

    private static Set<StarField> resolveStarExclusions(Star star, List<TableBinding> starBindings)
    {
        Set<StarField> exclusions = new HashSet<>();
        for (ColumnReference exclusion : star.exclusions()) {
            List<Identifier> parts = exclusion.parts();
            Identifier fieldName = parts.getLast();
            List<TableBinding> exclusionBindings = parts.size() == 1 ? starBindings : starBindings.stream()
                                                                                      .filter(binding -> matchesStarQualifier(binding, parts.subList(0, parts.size() - 1)))
                                                                                      .toList();
            if (parts.size() > 1 && exclusionBindings.size() > 1) {
                throw starResolutionError(exclusion.span(), "Ambiguous HogQL star exclusion qualifier: " + identifierPath(parts.subList(0, parts.size() - 1)));
            }
            List<StarField> matchedFields = exclusionBindings.stream()
                    .flatMap(binding -> binding.orderedFields().stream()
                            .filter(BoundField::starVisible)
                            .filter(field -> matchesIdentifier(fieldName, field.name()))
                            .map(field -> new StarField(binding, field)))
                    .distinct()
                    .toList();
            if (matchedFields.isEmpty()) {
                throw starResolutionError(exclusion.span(), "Unknown HogQL star exclusion: " + identifierPath(parts));
            }
            if (matchedFields.stream().anyMatch(exclusions::contains)) {
                throw starResolutionError(exclusion.span(), "Duplicate HogQL star exclusion: " + identifierPath(parts));
            }
            exclusions.addAll(matchedFields);
        }
        return exclusions;
    }

    private static boolean matchesStarQualifier(TableBinding binding, List<Identifier> qualifier)
    {
        if (binding.aliased()) {
            return qualifier.size() == 1 && matchesIdentifier(qualifier.getFirst(), binding.outputQualifier());
        }
        if (qualifier.size() == 1 && canonicalIdentifier(qualifier.getFirst().value(), qualifier.getFirst().delimited()).equals(canonical(binding.relationName()))) {
            return true;
        }
        if (qualifier.size() > binding.physicalQualifier().size()) {
            return false;
        }
        int offset = binding.physicalQualifier().size() - qualifier.size();
        for (int index = 0; index < qualifier.size(); index++) {
            if (!matchesIdentifier(qualifier.get(index), binding.physicalQualifier().get(offset + index))) {
                return false;
            }
        }
        return !qualifier.isEmpty();
    }

    private static boolean matchesIdentifier(Identifier identifier, String value)
    {
        return identifier.delimited() ? identifier.value().equals(value) : canonical(identifier.value()).equals(canonical(value));
    }

    private static boolean matchesIdentifier(Identifier identifier, PhysicalIdentifier value)
    {
        return canonicalIdentifier(identifier.value(), identifier.delimited()).equals(canonicalIdentifier(value.value(), value.delimited()));
    }

    private static String canonicalIdentifier(String value, boolean delimited)
    {
        return delimited ? value : canonical(value);
    }

    private static String starQualifier(Star star)
    {
        return identifierPath(star.qualifier());
    }

    private static String identifierPath(List<Identifier> identifiers)
    {
        return String.join(".", identifiers.stream().map(Identifier::value).toList());
    }

    private ResolvedRelation resolveRelation(Relation relation)
    {
        return resolveRelation(relation, HogQlProjectionDemand.preserveAll());
    }

    private ResolvedRelation resolveRelation(Relation relation, HogQlProjectionDemand projectionDemand)
    {
        return switch (relation) {
            case AliasedRelation alias -> {
                ResolvedRelation child;
                if (alias.relation() instanceof SubqueryRelation subquery && alias.columnAliases().isEmpty()) {
                    child = resolveSubquery(subquery, projectionDemand.forAlias(alias.alias()));
                }
                else {
                    child = resolveRelation(alias.relation(), HogQlProjectionDemand.preserveAll());
                }
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
                ResolvedRelation left = resolveRelation(join.left(), projectionDemand);
                ResolvedRelation right = resolveRelation(join.right(), projectionDemand);
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
            case SubqueryRelation subquery -> resolveSubquery(subquery, projectionDemand.unqualified());
            case TablePlaceholder placeholder -> new ResolvedRelation(placeholder, List.of(), false);
            case TableReference table -> resolveTable(table);
            case ValuesRelation values -> new ResolvedRelation(values, List.of(), false);
        };
    }

    private ResolvedRelation resolveSubquery(SubqueryRelation subquery, RequiredOutputs requiredOutputs)
    {
        return new ResolvedRelation(
                new SubqueryRelation(
                        new HogQlSemanticResolver(snapshot, expansionBudget, Optional.of(requiredOutputs)).resolveNestedQuery(subquery.query()),
                        subquery.span()),
                List.of(),
                false);
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
                        List.of(definition.physicalTable().catalog(), definition.physicalTable().schema(), definition.physicalTable().table()),
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
                        List.of(definition.physicalView().catalog(), definition.physicalView().schema(), definition.physicalView().table()),
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
                        List.of(),
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
            case CastExpression cast -> new CastExpression(resolveExpression(cast.value()), cast.type(), cast.safe(), cast.typeDialect(), cast.span());
            case ColumnReference reference -> resolveColumn(reference);
            case FunctionCall function -> resolveFunctionExpression(function);
            case InCohortExpression in -> resolveCohortExpression(in);
            case InExpression in -> new InExpression(
                    resolveExpression(in.value()),
                    in.values().stream().map(this::resolveExpression).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case InSubqueryExpression in -> new InSubqueryExpression(
                    resolveExpression(in.value()),
                    new HogQlSemanticResolver(snapshot, expansionBudget).resolveNestedQuery(in.query()),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case IntervalExpression interval -> new IntervalExpression(resolveExpression(interval.value()), interval.unit(), interval.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    resolveExpression(isNull.value()),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case Literal literal -> literal;
            case MemberAccessExpression memberAccess -> resolveMemberAccess(memberAccess);
            case Placeholder placeholder -> placeholder;
            case ScalarSubqueryExpression subquery -> new ScalarSubqueryExpression(
                    new HogQlSemanticResolver(snapshot, expansionBudget).resolveNestedQuery(subquery.query()),
                    subquery.span());
            case SubscriptExpression subscript -> resolveSubscript(subscript);
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(this::resolveExpression).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), resolveExpression(unary.operand()), unary.span());
        };
    }

    private Expression resolveFunctionExpression(FunctionCall function)
    {
        if (function.nameParts().size() == 1 && canonical(function.name().value()).equals(MATCHES_ACTION)) {
            return resolveActionExpression(function);
        }
        return new FunctionCall(
                function.nameParts(),
                function.arguments().stream().map(this::resolveExpression).toList(),
                function.distinct(),
                resolveSortItems(function.orderBy()),
                function.filter().map(this::resolveExpression),
                function.nullTreatment(),
                function.window().map(this::resolveWindow),
                function.span());
    }

    private Expression resolveActionExpression(FunctionCall function)
    {
        if (function.arguments().size() != 1) {
            throw semanticEntityError(HOGQL_RESOLUTION_ERROR, function.span(), "HogQL matchesAction requires exactly one argument");
        }
        if (function.distinct() || !function.orderBy().isEmpty() || function.filter().isPresent() || function.nullTreatment().isPresent() || function.window().isPresent()) {
            throw semanticEntityError(HOGQL_UNSUPPORTED_FEATURE, function.span(), "HogQL matchesAction does not support invocation modifiers");
        }
        EntityLookup lookup = entityLookup(function.arguments().getFirst(), "action");
        List<ActionReference> matches = snapshot.snapshot().actions().stream()
                .filter(action -> lookup.matches(action.name(), action.actionId()))
                .toList();
        ActionReference action = requireEntity(matches, "action", function.span());
        TableBinding binding = requireEntityBinding(action.table(), "action", function.span());
        return switch (action.representation()) {
            case PredicateRepresentation predicate -> expandRecipe(
                    binding,
                    predicate.predicate(),
                    Optional.of(binding.outputQualifier()),
                    function.span(),
                    expansionBudget);
            case RelationMembershipRepresentation membership -> membershipExpression(binding, membership.relation(), false, function.span());
        };
    }

    private Expression resolveCohortExpression(InCohortExpression in)
    {
        EntityLookup lookup = entityLookup(in.cohort(), "cohort");
        List<CohortReference> matches = snapshot.snapshot().cohorts().stream()
                .filter(cohort -> lookup.matches(cohort.name(), cohort.cohortId()))
                .toList();
        CohortReference cohort = requireEntity(matches, "cohort", in.span());
        TableBinding binding = requireEntityBinding(cohort.table(), "cohort", in.span());
        return switch (cohort.representation()) {
            case PredicateRepresentation predicate -> {
                requireCohortSource(in.value(), binding, in.span());
                Expression expanded = expandRecipe(
                        binding,
                        predicate.predicate(),
                        Optional.of(binding.outputQualifier()),
                        in.span(),
                        expansionBudget);
                yield in.negated() ? new UnaryExpression(HogQlQuery.UnaryOperator.NOT, expanded, in.span()) : expanded;
            }
            case RelationMembershipRepresentation membership -> {
                requireMembershipSource(in.value(), binding, membership.relation(), in.span());
                yield membershipExpression(binding, membership.relation(), in.negated(), in.span());
            }
        };
    }

    private void requireCohortSource(Expression value, TableBinding binding, HogQlQuery.SourceSpan span)
    {
        if (!(value instanceof ColumnReference reference)) {
            throw unsupportedExpansion(span, "HogQL cohort membership source must be a declared field reference");
        }
        List<Identifier> parts = reference.parts();
        boolean qualified = parts.size() == 2 && binding.qualifier().equals(canonical(parts.getFirst().value()));
        boolean matches = (parts.size() == 1 || qualified) && binding.fields().containsKey(canonical(parts.getLast().value()));
        if (!matches) {
            throw unsupportedExpansion(span, "HogQL cohort membership source does not match the catalog table");
        }
    }

    private InSubqueryExpression membershipExpression(
            TableBinding sourceBinding,
            RelationMembershipRecipe membership,
            boolean negated,
            HogQlQuery.SourceSpan span)
    {
        expansionBudget.enter(span);
        try {
            BoundField sourceField = Optional.ofNullable(sourceBinding.fields().get(canonical(membership.sourceField())))
                    .orElseThrow(() -> expansionError(span, "HogQL semantic entity references an unavailable source field"));
            Expression source = resolveBoundField(
                    sourceBinding,
                    sourceField,
                    sourceBinding.starQualifier(bindings.size()),
                    span,
                    expansionBudget);
            ResolvedRelation target = resolveRelationReference(membership.relation(), span, new RelationExpansionBudget());
            if (target.bindings().size() != 1) {
                throw unsupportedExpansion(span, "HogQL semantic entity membership requires a single target relation");
            }
            TableBinding targetBinding = target.bindings().getFirst();
            BoundField targetField = Optional.ofNullable(targetBinding.fields().get(canonical(membership.targetField())))
                    .orElseThrow(() -> expansionError(span, "HogQL semantic entity references an unavailable target field"));
            Expression targetValue = resolveBoundField(
                    targetBinding,
                    targetField,
                    targetBinding.starQualifier(1),
                    span,
                    expansionBudget);
            HogQlQuery membershipQuery = new HogQlQuery(
                    List.of(),
                    true,
                    List.of(new ExpressionProjection(targetValue, Optional.empty())),
                    Optional.of(target.relation()),
                    Optional.empty(),
                    List.of(),
                    Optional.empty(),
                    List.of(),
                    Optional.empty(),
                    Optional.empty(),
                    span);
            return new InSubqueryExpression(source, membershipQuery, negated, span, span);
        }
        finally {
            expansionBudget.exit();
        }
    }

    private void requireMembershipSource(Expression value, TableBinding binding, RelationMembershipRecipe membership, HogQlQuery.SourceSpan span)
    {
        if (!(value instanceof ColumnReference reference)) {
            throw unsupportedExpansion(span, "HogQL cohort membership source must be a declared field reference");
        }
        List<Identifier> parts = reference.parts();
        boolean matches = switch (parts.size()) {
            case 1 -> canonical(parts.getFirst().value()).equals(canonical(membership.sourceField()));
            case 2 -> binding.qualifier().equals(canonical(parts.getFirst().value())) &&
                    canonical(parts.getLast().value()).equals(canonical(membership.sourceField()));
            default -> false;
        };
        if (!matches) {
            throw unsupportedExpansion(span, "HogQL cohort membership source does not match the catalog");
        }
    }

    private TableBinding requireEntityBinding(String tableName, String kind, HogQlQuery.SourceSpan span)
    {
        List<TableBinding> matches = bindings.stream()
                .filter(binding -> canonical(binding.relationName()).equals(canonical(tableName)))
                .toList();
        if (matches.size() != 1) {
            throw semanticEntityError(
                    HOGQL_RESOLUTION_ERROR,
                    span,
                    "HogQL " + kind + " requires exactly one " + tableName + " relation in scope");
        }
        return matches.getFirst();
    }

    private EntityLookup entityLookup(Expression expression, String kind)
    {
        if (!(expression instanceof Literal literal) || (literal.kind() != HogQlQuery.LiteralKind.STRING && literal.kind() != HogQlQuery.LiteralKind.INTEGER)) {
            throw semanticEntityError(
                    HOGQL_UNSUPPORTED_FEATURE,
                    expression.span(),
                    "HogQL " + kind + " reference must be a string or integer literal");
        }
        return new EntityLookup(literal.value(), literal.kind() == HogQlQuery.LiteralKind.INTEGER);
    }

    private static <T> T requireEntity(List<T> matches, String kind, HogQlQuery.SourceSpan span)
    {
        if (matches.isEmpty()) {
            throw semanticEntityError(HOGQL_RESOLUTION_ERROR, span, "Unknown HogQL " + kind);
        }
        if (matches.size() > 1) {
            throw semanticEntityError(HOGQL_RESOLUTION_ERROR, span, "Ambiguous HogQL " + kind);
        }
        return matches.getFirst();
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
        Optional<Expression> semanticPath = resolveSemanticPath(reference);
        if (semanticPath.isPresent()) {
            return semanticPath.orElseThrow();
        }
        Optional<Expression> propertyAccess = resolveDottedPropertyAccess(reference);
        if (propertyAccess.isPresent()) {
            return propertyAccess.orElseThrow();
        }
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

    private Optional<Expression> resolveSemanticPath(ColumnReference reference)
    {
        List<Identifier> parts = reference.parts();
        if (parts.size() < 2) {
            return Optional.empty();
        }

        List<PathCandidate> candidates = new ArrayList<>();
        List<TableBinding> qualifiedBindings = bindings.stream()
                .filter(binding -> binding.qualifier().equals(canonical(parts.getFirst().value())))
                .toList();
        if (!qualifiedBindings.isEmpty()) {
            if (qualifiedBindings.size() == 1 && isSemanticPathMember(qualifiedBindings.getFirst(), parts.get(1).value())) {
                candidates.add(new PathCandidate(qualifiedBindings.getFirst(), parts.subList(1, parts.size())));
            }
        }
        else {
            bindings.stream()
                    .filter(binding -> isSemanticPathMember(binding, parts.getFirst().value()))
                    .map(binding -> new PathCandidate(binding, parts))
                    .forEach(candidates::add);
        }
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        if (candidates.size() > 1) {
            throw ambiguousResolutionError(reference, parts.getFirst().value());
        }
        if (expandedRelation.isEmpty()) {
            throw unsupportedExpansion(reference.span(), "HogQL relationship paths are not supported inside explicit join criteria");
        }
        return Optional.of(resolveSemanticPath(reference, candidates.getFirst()));
    }

    private boolean isSemanticPathMember(TableBinding binding, String name)
    {
        return relationship(binding, name).isPresent() || lazyTable(binding, name).isPresent();
    }

    private Expression resolveSemanticPath(ColumnReference reference, PathCandidate candidate)
    {
        TableBinding owner = candidate.owner();
        List<Identifier> path = candidate.path();
        Optional<LazyTableDefinition> lazyTable = lazyTable(owner, path.getFirst().value());
        if (lazyTable.isPresent()) {
            if (path.size() != 2) {
                throw resolutionError(reference, path.getLast().value());
            }
            LazyTableDefinition definition = lazyTable.orElseThrow();
            LazyProjectionDefinition projection = definition.projections().stream()
                    .filter(candidateProjection -> canonical(candidateProjection.name()).equals(canonical(path.getLast().value())))
                    .findFirst()
                    .orElseThrow(() -> resolutionError(reference, path.getLast().value()));
            TableBinding terminal = ensureRelationshipPath(owner, definition.relationshipPath(), reference.span());
            return expandRecipe(
                    terminal,
                    projection.recipe(),
                    Optional.of(terminal.outputQualifier()),
                    reference.span(),
                    expansionBudget);
        }

        LogicalTableDefinition currentTable = snapshot.logicalTable(owner.relationName()).orElseThrow();
        List<String> relationshipPath = new ArrayList<>();
        int index = 0;
        while (index < path.size() - 1) {
            Optional<RelationshipDefinition> relationship = relationship(currentTable, path.get(index).value());
            if (relationship.isEmpty()) {
                break;
            }
            RelationshipDefinition definition = relationship.orElseThrow();
            relationshipPath.add(definition.name());
            currentTable = snapshot.logicalTable(definition.targetTable())
                    .orElseThrow(() -> expansionError(reference.span(), "HogQL relationship references an unavailable target table"));
            index++;
        }
        if (relationshipPath.isEmpty()) {
            return reference;
        }

        TableBinding terminal = ensureRelationshipPath(owner, relationshipPath, reference.span());
        List<Identifier> remaining = path.subList(index, path.size());
        if (remaining.size() == 1) {
            BoundField field = terminal.fields().get(canonical(remaining.getFirst().value()));
            if (field == null) {
                throw resolutionError(reference, remaining.getFirst().value());
            }
            return resolveBoundField(terminal, field, Optional.of(terminal.outputQualifier()), reference.span(), expansionBudget);
        }
        if (remaining.size() == 2) {
            Optional<PropertyDefinition> property = properties(terminal).stream()
                    .filter(candidateProperty -> canonical(candidateProperty.name()).equals(canonical(remaining.getFirst().value())))
                    .findFirst();
            if (property.isPresent()) {
                return expandProperty(
                        terminal,
                        property.orElseThrow(),
                        new Literal(HogQlQuery.LiteralKind.STRING, remaining.getLast().value(), remaining.getLast().span()),
                        Optional.of(terminal.outputQualifier()),
                        reference.span(),
                        expansionBudget);
            }
        }
        throw resolutionError(reference, remaining.getLast().value());
    }

    private TableBinding ensureRelationshipPath(TableBinding owner, List<String> path, HogQlQuery.SourceSpan span)
    {
        TableBinding source = owner;
        List<String> prefix = new ArrayList<>();
        for (String relationshipName : path) {
            prefix.add(canonical(relationshipName));
            RelationshipPathKey key = new RelationshipPathKey(owner.qualifier(), prefix);
            TableBinding cached = relationshipPaths.get(key);
            if (cached != null) {
                source = cached;
                continue;
            }
            RelationshipDefinition relationship = relationship(source, relationshipName)
                    .orElseThrow(() -> expansionError(span, "HogQL lazy table references an unavailable relationship"));
            LogicalTableDefinition target = snapshot.logicalTable(relationship.targetTable())
                    .orElseThrow(() -> expansionError(span, "HogQL relationship references an unavailable target table"));
            source = addRelationshipJoin(source, target, relationship, span);
            relationshipPaths.put(key, source);
        }
        return source;
    }

    private TableBinding addRelationshipJoin(
            TableBinding source,
            LogicalTableDefinition target,
            RelationshipDefinition relationship,
            HogQlQuery.SourceSpan span)
    {
        expansionBudget.add(span);
        ResolvedRelation targetRelation = resolveLogicalTable(target, span);
        Identifier alias = nextGeneratedRelationAlias(span);
        TableBinding targetBinding = targetRelation.bindings().getFirst().withAlias(alias);
        Relation aliasedTarget = new AliasedRelation(targetRelation.relation(), alias, span);

        List<Expression> predicates = relationship.joinKeys().stream()
                .map(joinKey -> joinKeyPredicate(source, targetBinding, joinKey, span))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        relationship.joinPredicate().ifPresent(predicate -> predicates.add(expandRecipe(
                source,
                predicate,
                Optional.of(source.outputQualifier()),
                span,
                expansionBudget,
                Map.of(),
                Map.of(RelationshipJoinSide.SOURCE, source, RelationshipJoinSide.TARGET, targetBinding))));
        Expression criteria = predicates.stream()
                .reduce((left, right) -> new BinaryExpression(HogQlQuery.BinaryOperator.AND, left, right, span))
                .orElseThrow();
        expandedRelation = Optional.of(new JoinRelation(
                HogQlQuery.JoinType.LEFT,
                expandedRelation.orElseThrow(),
                aliasedTarget,
                Optional.of(new JoinOn(criteria, span)),
                span));
        return targetBinding;
    }

    private Expression joinKeyPredicate(TableBinding source, TableBinding target, JoinKey joinKey, HogQlQuery.SourceSpan span)
    {
        BoundField sourceField = Optional.ofNullable(source.fields().get(canonical(joinKey.sourceField())))
                .orElseThrow(() -> expansionError(span, "HogQL relationship references an unavailable source field"));
        BoundField targetField = Optional.ofNullable(target.fields().get(canonical(joinKey.targetField())))
                .orElseThrow(() -> expansionError(span, "HogQL relationship references an unavailable target field"));
        return new BinaryExpression(
                HogQlQuery.BinaryOperator.EQUAL,
                physicalColumn(sourceField, Optional.of(source.outputQualifier()), span),
                physicalColumn(targetField, Optional.of(target.outputQualifier()), span),
                span);
    }

    private Identifier nextGeneratedRelationAlias(HogQlQuery.SourceSpan span)
    {
        while (true) {
            Identifier candidate = new Identifier("__hogql_lazy_" + ++generatedRelationId, true, span);
            boolean used = bindings.stream().anyMatch(binding -> binding.qualifier().equals(canonical(candidate.value()))) ||
                    relationshipPaths.values().stream().anyMatch(binding -> binding.qualifier().equals(canonical(candidate.value())));
            if (!used) {
                return candidate;
            }
        }
    }

    private Optional<RelationshipDefinition> relationship(TableBinding binding, String name)
    {
        return snapshot.logicalTable(binding.relationName()).flatMap(table -> relationship(table, name));
    }

    private static Optional<RelationshipDefinition> relationship(LogicalTableDefinition table, String name)
    {
        return table.relationships().stream()
                .filter(candidate -> canonical(candidate.name()).equals(canonical(name)))
                .findFirst();
    }

    private Optional<LazyTableDefinition> lazyTable(TableBinding binding, String name)
    {
        return snapshot.snapshot().lazyTables().stream()
                .filter(candidate -> canonical(candidate.table()).equals(canonical(binding.relationName())))
                .filter(candidate -> canonical(candidate.name()).equals(canonical(name)))
                .findFirst();
    }

    private Expression resolveMemberAccess(MemberAccessExpression memberAccess)
    {
        if (memberAccess.base() instanceof ColumnReference reference) {
            Optional<Expression> propertyAccess = resolvePropertyAccess(
                    reference,
                    new Literal(HogQlQuery.LiteralKind.STRING, memberAccess.member().value(), memberAccess.member().span()),
                    memberAccess.span());
            if (propertyAccess.isPresent()) {
                return propertyAccess.orElseThrow();
            }
        }
        return new MemberAccessExpression(
                resolveExpression(memberAccess.base()),
                memberAccess.member(),
                memberAccess.span());
    }

    private Expression resolveSubscript(SubscriptExpression subscript)
    {
        if (subscript.base() instanceof ColumnReference reference) {
            Optional<Expression> propertyAccess = resolvePropertyAccess(
                    reference,
                    subscript.index(),
                    subscript.span());
            if (propertyAccess.isPresent()) {
                return propertyAccess.orElseThrow();
            }
        }
        return new SubscriptExpression(
                resolveExpression(subscript.base()),
                resolveExpression(subscript.index()),
                subscript.span());
    }

    private Optional<Expression> resolveDottedPropertyAccess(ColumnReference reference)
    {
        List<Identifier> parts = reference.parts();
        if (parts.size() == 2 && bindings.stream().noneMatch(binding -> binding.qualifier().equals(canonical(parts.getFirst().value())))) {
            return resolvePropertyAccess(
                    new ColumnReference(List.of(parts.getFirst()), parts.getFirst().span()),
                    new Literal(HogQlQuery.LiteralKind.STRING, parts.getLast().value(), parts.getLast().span()),
                    reference.span());
        }
        if (parts.size() == 3) {
            return resolvePropertyAccess(
                    new ColumnReference(parts.subList(0, 2), reference.span()),
                    new Literal(HogQlQuery.LiteralKind.STRING, parts.getLast().value(), parts.getLast().span()),
                    reference.span());
        }
        return Optional.empty();
    }

    private Optional<Expression> resolvePropertyAccess(
            ColumnReference propertyReference,
            Expression key,
            HogQlQuery.SourceSpan span)
    {
        List<Identifier> parts = propertyReference.parts();
        List<TableBinding> candidateBindings;
        String propertyName;
        boolean qualified;
        if (parts.size() == 1) {
            candidateBindings = bindings;
            propertyName = parts.getFirst().value();
            qualified = false;
        }
        else if (parts.size() == 2) {
            candidateBindings = bindings.stream()
                    .filter(binding -> binding.qualifier().equals(canonical(parts.getFirst().value())))
                    .toList();
            propertyName = parts.getLast().value();
            qualified = true;
        }
        else {
            return Optional.empty();
        }

        List<PropertyMatch> propertyMatches = candidateBindings.stream()
                .flatMap(binding -> properties(binding).stream()
                        .filter(property -> canonical(property.name()).equals(canonical(propertyName)))
                        .map(property -> new PropertyMatch(binding, property)))
                .toList();
        if (propertyMatches.isEmpty()) {
            return Optional.empty();
        }
        List<TableBinding> matchedBindings = propertyMatches.stream()
                .map(PropertyMatch::binding)
                .distinct()
                .toList();
        if (matchedBindings.size() > 1) {
            throw ambiguousPropertyResolutionError(span, propertyName);
        }

        PropertyDefinition property = propertyMatches.getFirst().property();
        TableBinding binding = matchedBindings.getFirst();
        Optional<PhysicalIdentifier> qualifier = qualified || bindings.size() > 1
                ? Optional.of(binding.outputQualifier())
                : Optional.empty();
        return Optional.of(expandProperty(binding, property, resolveExpression(key), qualifier, span, expansionBudget));
    }

    private List<PropertyDefinition> properties(TableBinding binding)
    {
        return snapshot.logicalTable(binding.relationName())
                .map(LogicalTableDefinition::properties)
                .orElse(List.of());
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
        return expandRecipe(binding, recipe, qualifier, span, budget, Map.of(), Map.of());
    }

    private Expression expandRecipe(
            TableBinding binding,
            ExpressionRecipe recipe,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget,
            Map<ExpressionArgument, Expression> arguments)
    {
        return expandRecipe(binding, recipe, qualifier, span, budget, arguments, Map.of());
    }

    private Expression expandRecipe(
            TableBinding binding,
            ExpressionRecipe recipe,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget,
            Map<ExpressionArgument, Expression> arguments,
            Map<RelationshipJoinSide, TableBinding> scopedBindings)
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
                case FunctionCallRecipe function -> expandFunction(binding, function, qualifier, span, budget, arguments, scopedBindings);
                case OperatorRecipe operator -> expandOperator(binding, operator, qualifier, span, budget, arguments, scopedBindings);
                case CastRecipe cast -> new CastExpression(
                        expandRecipe(binding, cast.expression(), qualifier, span, budget, arguments, scopedBindings),
                        new Identifier(cast.targetTypeSignature(), false, span),
                        false,
                        span);
                case ArgumentReferenceRecipe reference -> {
                    Expression argument = arguments.get(reference.argument());
                    if (argument == null) {
                        throw unsupportedExpansion(span, "HogQL recipe argument is unavailable in this expansion context");
                    }
                    yield argument;
                }
                case ScopedFieldReferenceRecipe reference -> {
                    TableBinding scopedBinding = scopedBindings.get(reference.side());
                    if (scopedBinding == null) {
                        throw unsupportedExpansion(span, "HogQL scoped field recipe is only valid inside a relationship predicate");
                    }
                    BoundField field = scopedBinding.fields().get(canonical(reference.field()));
                    if (field == null) {
                        throw expansionError(span, "HogQL relationship predicate references an unavailable field");
                    }
                    yield resolveBoundField(
                            scopedBinding,
                            field,
                            Optional.of(scopedBinding.outputQualifier()),
                            span,
                            budget);
                }
                case PropertyLookupRecipe lookup -> expandPropertyLookup(binding, lookup, qualifier, span, budget, arguments, scopedBindings);
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
            ExpansionBudget budget,
            Map<ExpressionArgument, Expression> arguments,
            Map<RelationshipJoinSide, TableBinding> scopedBindings)
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
                        .map(argument -> expandRecipe(binding, argument, qualifier, span, budget, arguments, scopedBindings))
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
            ExpansionBudget budget,
            Map<ExpressionArgument, Expression> recipeArguments,
            Map<RelationshipJoinSide, TableBinding> scopedBindings)
    {
        List<Expression> arguments = operator.arguments().stream()
                .map(argument -> expandRecipe(binding, argument, qualifier, span, budget, recipeArguments, scopedBindings))
                .toList();
        return switch (operator.operator()) {
            case NOT -> new UnaryExpression(HogQlQuery.UnaryOperator.NOT, arguments.getFirst(), span);
            case NEGATE -> new UnaryExpression(HogQlQuery.UnaryOperator.NEGATE, arguments.getFirst(), span);
            case IS_NULL -> new IsNullExpression(arguments.getFirst(), false, span, span);
            case IS_NOT_NULL -> new IsNullExpression(arguments.getFirst(), true, span, span);
            case SUBSCRIPT -> new SubscriptExpression(arguments.getFirst(), arguments.getLast(), span);
            default -> new BinaryExpression(binaryOperator(operator.operator()), arguments.getFirst(), arguments.getLast(), span);
        };
    }

    private Expression expandPropertyLookup(
            TableBinding binding,
            PropertyLookupRecipe lookup,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget,
            Map<ExpressionArgument, Expression> arguments,
            Map<RelationshipJoinSide, TableBinding> scopedBindings)
    {
        TableBinding lookupBinding = canonical(binding.relationName()).equals(canonical(lookup.table()))
                ? binding
                : scopedBindings.values().stream()
                  .filter(candidate -> canonical(candidate.relationName()).equals(canonical(lookup.table())))
                  .findFirst()
                  .orElseThrow(() -> expansionError(span, "HogQL property lookup recipe references an unavailable table"));
        Optional<PhysicalIdentifier> lookupQualifier = lookupBinding == binding
                ? qualifier
                : Optional.of(lookupBinding.outputQualifier());
        PropertyDefinition property = properties(lookupBinding).stream()
                .filter(candidate -> canonical(candidate.name()).equals(canonical(lookup.property())))
                .findFirst()
                .orElseThrow(() -> expansionError(span, "HogQL property lookup recipe references an unavailable property"));
        Expression key = expandRecipe(binding, lookup.key(), qualifier, span, budget, arguments, scopedBindings);
        return expandProperty(lookupBinding, property, key, lookupQualifier, span, budget);
    }

    private Expression expandProperty(
            TableBinding binding,
            PropertyDefinition property,
            Expression key,
            Optional<PhysicalIdentifier> qualifier,
            HogQlQuery.SourceSpan span,
            ExpansionBudget budget)
    {
        budget.enter(span);
        try {
            ExpressionRecipe recipe = property.lookupRecipe()
                    .orElseThrow(() -> unsupportedExpansion(span, "HogQL property lookup has no declared compiler recipe"));
            String keyType = property.keyTypeSignature()
                    .orElseThrow(() -> unsupportedExpansion(span, "HogQL property lookup has no declared key type"));
            String valueType = property.valueTypeSignature()
                    .orElseThrow(() -> unsupportedExpansion(span, "HogQL property lookup has no declared value type"));
            BoundField source = binding.fields().get(canonical(property.sourceField()));
            if (source == null) {
                throw expansionError(span, "HogQL property lookup references an unavailable source field");
            }
            Expression sourceExpression = resolveBoundField(binding, source, qualifier, span, budget);
            Expression typedKey = new CastExpression(key, new Identifier(keyType, false, key.span()), false, key.span());
            Expression value = expandRecipe(
                    binding,
                    recipe,
                    qualifier,
                    span,
                    budget,
                    Map.of(
                            ExpressionArgument.PROPERTY_SOURCE, sourceExpression,
                            ExpressionArgument.PROPERTY_KEY, typedKey));
            return new CastExpression(value, new Identifier(valueType, false, span), false, span);
        }
        finally {
            budget.exit();
        }
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
            case NOT, NEGATE, IS_NULL, IS_NOT_NULL, SUBSCRIPT -> throw new IllegalArgumentException("operator cannot be lowered as binary");
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

    private static TrinoException semanticEntityError(HogQlErrorCode errorCode, HogQlQuery.SourceSpan span, String message)
    {
        return new TrinoException(
                errorCode,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }

    private static TrinoException unsupportedColumns(HogQlQuery.SourceSpan span)
    {
        return semanticEntityError(
                HOGQL_UNSUPPORTED_FEATURE,
                span,
                "HogQL COLUMNS requires a logical relation from the semantic catalog");
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
            List<PhysicalIdentifier> physicalQualifier,
            boolean aliased,
            List<BoundField> fields)
    {
        return new TableBinding(
                definition.name(),
                qualifier,
                outputQualifier,
                physicalQualifier,
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

    private static TrinoException ambiguousPropertyResolutionError(HogQlQuery.SourceSpan span, String name)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                "Ambiguous HogQL property source: " + name,
                null);
    }

    private static TrinoException starResolutionError(Identifier identifier, String message)
    {
        return starResolutionError(identifier.span(), message);
    }

    private static TrinoException starResolutionError(HogQlQuery.SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
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
            List<PhysicalIdentifier> physicalQualifier,
            boolean aliased,
            List<BoundField> orderedFields,
            Map<String, BoundField> fields)
    {
        private TableBinding(LogicalTableDefinition logicalTable, String qualifier, PhysicalIdentifier outputQualifier, List<PhysicalIdentifier> physicalQualifier, boolean aliased)
        {
            this(logicalTable.name(), qualifier, outputQualifier, physicalQualifier, aliased, boundFields(logicalTable), fieldMap(boundFields(logicalTable)));
        }

        private TableBinding
        {
            relationName = requireNonNull(relationName, "relationName is null");
            qualifier = requireNonNull(qualifier, "qualifier is null");
            outputQualifier = requireNonNull(outputQualifier, "outputQualifier is null");
            physicalQualifier = List.copyOf(requireNonNull(physicalQualifier, "physicalQualifier is null"));
            orderedFields = List.copyOf(requireNonNull(orderedFields, "orderedFields is null"));
            fields = Map.copyOf(requireNonNull(fields, "fields is null"));
        }

        private TableBinding withAlias(Identifier alias)
        {
            return new TableBinding(
                    relationName,
                    canonical(alias.value()),
                    new PhysicalIdentifier(alias.value(), alias.delimited()),
                    physicalQualifier,
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

    private record StarField(TableBinding binding, BoundField field) {}

    private record LazyStar(TableBinding binding, LazyTableDefinition definition) {}

    private record PropertyMatch(TableBinding binding, PropertyDefinition property) {}

    private record PathCandidate(TableBinding owner, List<Identifier> path)
    {
        private PathCandidate
        {
            owner = requireNonNull(owner, "owner is null");
            path = List.copyOf(requireNonNull(path, "path is null"));
        }
    }

    private record RelationshipPathKey(String ownerQualifier, List<String> path)
    {
        private RelationshipPathKey
        {
            ownerQualifier = requireNonNull(ownerQualifier, "ownerQualifier is null");
            path = List.copyOf(requireNonNull(path, "path is null"));
        }
    }

    private record ProjectedField(String name, String sourceField, boolean starVisible)
    {
        private ProjectedField
        {
            name = requireNonNull(name, "name is null");
            sourceField = requireNonNull(sourceField, "sourceField is null");
        }
    }

    private record EntityLookup(String value, boolean id)
    {
        private EntityLookup
        {
            value = requireNonNull(value, "value is null");
        }

        private boolean matches(String name, String entityId)
        {
            return id ? value.equals(entityId) : canonical(value).equals(canonical(name));
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
