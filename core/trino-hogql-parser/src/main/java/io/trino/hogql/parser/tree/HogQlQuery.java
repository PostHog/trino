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
package io.trino.hogql.parser.tree;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record HogQlQuery(
        List<CommonTableExpression> with,
        QueryBody body,
        List<SortItem> orderBy,
        Optional<Expression> limit,
        Optional<Expression> offset,
        SourceSpan span)
{
    public HogQlQuery
    {
        with = List.copyOf(requireNonNull(with, "with is null"));
        body = requireNonNull(body, "body is null");
        orderBy = List.copyOf(requireNonNull(orderBy, "orderBy is null"));
        limit = requireNonNull(limit, "limit is null");
        offset = requireNonNull(offset, "offset is null");
        span = requireNonNull(span, "span is null");
    }

    public HogQlQuery(
            List<CommonTableExpression> with,
            boolean distinct,
            List<Projection> projections,
            Optional<Relation> from,
            Optional<Expression> where,
            List<Expression> groupBy,
            Optional<Expression> having,
            List<WindowDefinition> windows,
            List<SortItem> orderBy,
            Optional<Expression> limit,
            Optional<Expression> offset,
            SourceSpan span)
    {
        this(with, new SelectQueryBody(distinct, projections, from, where, groupBy, having, windows, span), orderBy, limit, offset, span);
    }

    public HogQlQuery(
            List<CommonTableExpression> with,
            boolean distinct,
            List<Projection> projections,
            Optional<Relation> from,
            Optional<Expression> where,
            List<Expression> groupBy,
            Optional<Expression> having,
            List<SortItem> orderBy,
            Optional<Expression> limit,
            Optional<Expression> offset,
            SourceSpan span)
    {
        this(with, distinct, projections, from, where, groupBy, having, List.of(), orderBy, limit, offset, span);
    }

    public boolean distinct()
    {
        return selectBody().distinct();
    }

    public List<Projection> projections()
    {
        return selectBody().projections();
    }

    public Optional<Relation> from()
    {
        return selectBody().from();
    }

    public Optional<Expression> where()
    {
        return selectBody().where();
    }

    public List<Expression> groupBy()
    {
        return selectBody().groupBy();
    }

    public Optional<Expression> having()
    {
        return selectBody().having();
    }

    public List<WindowDefinition> windows()
    {
        return selectBody().windows();
    }

    private SelectQueryBody selectBody()
    {
        if (body instanceof SelectQueryBody select) {
            return select;
        }
        throw new IllegalStateException("query body is not a SELECT");
    }

    public sealed interface QueryBody
            permits SelectQueryBody, SetOperation
    {
        SourceSpan span();
    }

    public record SelectQueryBody(
            boolean distinct,
            List<Projection> projections,
            Optional<Relation> from,
            Optional<Expression> where,
            List<Expression> groupBy,
            Optional<Expression> having,
            List<WindowDefinition> windows,
            SourceSpan span)
            implements QueryBody
    {
        public SelectQueryBody
        {
            projections = List.copyOf(requireNonNull(projections, "projections is null"));
            from = requireNonNull(from, "from is null");
            where = requireNonNull(where, "where is null");
            groupBy = List.copyOf(requireNonNull(groupBy, "groupBy is null"));
            having = requireNonNull(having, "having is null");
            windows = List.copyOf(requireNonNull(windows, "windows is null"));
            span = requireNonNull(span, "span is null");
        }

        public SelectQueryBody(
                boolean distinct,
                List<Projection> projections,
                Optional<Relation> from,
                Optional<Expression> where,
                List<Expression> groupBy,
                Optional<Expression> having,
                SourceSpan span)
        {
            this(distinct, projections, from, where, groupBy, having, List.of(), span);
        }
    }

    public record SetOperation(
            SetOperationType type,
            boolean distinct,
            HogQlQuery left,
            HogQlQuery right,
            boolean leftParenthesized,
            boolean rightParenthesized,
            SourceSpan operatorSpan,
            SourceSpan span)
            implements QueryBody
    {
        public SetOperation
        {
            type = requireNonNull(type, "type is null");
            left = requireNonNull(left, "left is null");
            right = requireNonNull(right, "right is null");
            operatorSpan = requireNonNull(operatorSpan, "operatorSpan is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public enum SetOperationType
    {
        EXCEPT,
        INTERSECT,
        UNION,
    }

    public record CommonTableExpression(Identifier name, List<Identifier> columnAliases, HogQlQuery query, SourceSpan span)
    {
        public CommonTableExpression
        {
            name = requireNonNull(name, "name is null");
            columnAliases = List.copyOf(requireNonNull(columnAliases, "columnAliases is null"));
            query = requireNonNull(query, "query is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record SortItem(Expression expression, SortDirection direction, NullPlacement nullPlacement, SourceSpan span)
    {
        public SortItem
        {
            expression = requireNonNull(expression, "expression is null");
            direction = requireNonNull(direction, "direction is null");
            nullPlacement = requireNonNull(nullPlacement, "nullPlacement is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public enum SortDirection
    {
        ASCENDING,
        DESCENDING,
    }

    public enum NullPlacement
    {
        FIRST,
        LAST,
        UNDEFINED,
    }

    public sealed interface Projection
            permits ExpressionProjection, Star
    {
        SourceSpan span();
    }

    public record Star(SourceSpan span)
            implements Projection
    {
        public Star
        {
            span = requireNonNull(span, "span is null");
        }
    }

    public record ExpressionProjection(Expression expression, Optional<Identifier> alias)
            implements Projection
    {
        public ExpressionProjection
        {
            expression = requireNonNull(expression, "expression is null");
            alias = requireNonNull(alias, "alias is null");
        }

        @Override
        public SourceSpan span()
        {
            return expression.span();
        }
    }

    public sealed interface Expression
            permits ArrayExpression,
                    BetweenExpression,
                    BinaryExpression,
                    CaseExpression,
                    CastExpression,
                    ColumnReference,
                    FunctionCall,
                    InExpression,
                    IsNullExpression,
                    Literal,
                    MemberAccessExpression,
                    Placeholder,
                    SubscriptExpression,
                    TupleExpression,
                    UnaryExpression
    {
        SourceSpan span();
    }

    public record CaseExpression(Optional<Expression> operand, List<CaseWhen> whenClauses, Optional<Expression> defaultValue, SourceSpan span)
            implements Expression
    {
        public CaseExpression
        {
            operand = requireNonNull(operand, "operand is null");
            whenClauses = List.copyOf(requireNonNull(whenClauses, "whenClauses is null"));
            if (whenClauses.isEmpty()) {
                throw new IllegalArgumentException("whenClauses is empty");
            }
            defaultValue = requireNonNull(defaultValue, "defaultValue is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record CaseWhen(Expression operand, Expression result, SourceSpan span)
    {
        public CaseWhen
        {
            operand = requireNonNull(operand, "operand is null");
            result = requireNonNull(result, "result is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record CastExpression(Expression value, Identifier type, boolean safe, SourceSpan span)
            implements Expression
    {
        public CastExpression
        {
            value = requireNonNull(value, "value is null");
            type = requireNonNull(type, "type is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record ArrayExpression(List<Expression> values, SourceSpan span)
            implements Expression
    {
        public ArrayExpression
        {
            values = List.copyOf(requireNonNull(values, "values is null"));
            span = requireNonNull(span, "span is null");
        }
    }

    public record TupleExpression(List<Expression> values, SourceSpan span)
            implements Expression
    {
        public TupleExpression
        {
            values = List.copyOf(requireNonNull(values, "values is null"));
            if (values.isEmpty()) {
                throw new IllegalArgumentException("values is empty");
            }
            span = requireNonNull(span, "span is null");
        }
    }

    public record SubscriptExpression(Expression base, Expression index, SourceSpan span)
            implements Expression
    {
        public SubscriptExpression
        {
            base = requireNonNull(base, "base is null");
            index = requireNonNull(index, "index is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record MemberAccessExpression(Expression base, Identifier member, SourceSpan span)
            implements Expression
    {
        public MemberAccessExpression
        {
            base = requireNonNull(base, "base is null");
            member = requireNonNull(member, "member is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record BetweenExpression(Expression value, Expression min, Expression max, boolean negated, SourceSpan predicateSpan, SourceSpan span)
            implements Expression
    {
        public BetweenExpression
        {
            value = requireNonNull(value, "value is null");
            min = requireNonNull(min, "min is null");
            max = requireNonNull(max, "max is null");
            predicateSpan = requireNonNull(predicateSpan, "predicateSpan is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record InExpression(Expression value, List<Expression> values, boolean negated, SourceSpan predicateSpan, SourceSpan span)
            implements Expression
    {
        public InExpression
        {
            value = requireNonNull(value, "value is null");
            values = List.copyOf(requireNonNull(values, "values is null"));
            if (values.isEmpty()) {
                throw new IllegalArgumentException("values is empty");
            }
            predicateSpan = requireNonNull(predicateSpan, "predicateSpan is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record IsNullExpression(Expression value, boolean negated, SourceSpan predicateSpan, SourceSpan span)
            implements Expression
    {
        public IsNullExpression
        {
            value = requireNonNull(value, "value is null");
            predicateSpan = requireNonNull(predicateSpan, "predicateSpan is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record UnaryExpression(UnaryOperator operator, Expression operand, SourceSpan span)
            implements Expression
    {
        public UnaryExpression
        {
            operator = requireNonNull(operator, "operator is null");
            operand = requireNonNull(operand, "operand is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record BinaryExpression(BinaryOperator operator, Expression left, Expression right, SourceSpan span)
            implements Expression
    {
        public BinaryExpression
        {
            operator = requireNonNull(operator, "operator is null");
            left = requireNonNull(left, "left is null");
            right = requireNonNull(right, "right is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record FunctionCall(
            List<Identifier> nameParts,
            List<Expression> arguments,
            boolean distinct,
            List<SortItem> orderBy,
            Optional<Expression> filter,
            Optional<NullTreatment> nullTreatment,
            Optional<Window> window,
            SourceSpan span)
            implements Expression
    {
        public FunctionCall
        {
            nameParts = List.copyOf(requireNonNull(nameParts, "nameParts is null"));
            if (nameParts.isEmpty()) {
                throw new IllegalArgumentException("nameParts is empty");
            }
            arguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
            orderBy = List.copyOf(requireNonNull(orderBy, "orderBy is null"));
            filter = requireNonNull(filter, "filter is null");
            nullTreatment = requireNonNull(nullTreatment, "nullTreatment is null");
            window = requireNonNull(window, "window is null");
            span = requireNonNull(span, "span is null");
        }

        public FunctionCall(
                List<Identifier> nameParts,
                List<Expression> arguments,
                boolean distinct,
                List<SortItem> orderBy,
                Optional<Expression> filter,
                SourceSpan span)
        {
            this(nameParts, arguments, distinct, orderBy, filter, Optional.empty(), Optional.empty(), span);
        }

        public FunctionCall(
                Identifier name,
                List<Expression> arguments,
                boolean distinct,
                List<SortItem> orderBy,
                Optional<Expression> filter,
                SourceSpan span)
        {
            this(List.of(name), arguments, distinct, orderBy, filter, Optional.empty(), Optional.empty(), span);
        }

        public Identifier name()
        {
            return nameParts.getLast();
        }
    }

    public enum NullTreatment
    {
        IGNORE
    }

    public record WindowDefinition(Identifier name, WindowSpecification specification, SourceSpan span)
    {
        public WindowDefinition
        {
            name = requireNonNull(name, "name is null");
            specification = requireNonNull(specification, "specification is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public sealed interface Window
            permits WindowReference, WindowSpecification
    {
        SourceSpan span();
    }

    public record WindowReference(Identifier name, SourceSpan span)
            implements Window
    {
        public WindowReference
        {
            name = requireNonNull(name, "name is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record WindowSpecification(
            List<Expression> partitionBy,
            List<SortItem> orderBy,
            Optional<WindowFrame> frame,
            SourceSpan span)
            implements Window
    {
        public WindowSpecification
        {
            partitionBy = List.copyOf(requireNonNull(partitionBy, "partitionBy is null"));
            orderBy = List.copyOf(requireNonNull(orderBy, "orderBy is null"));
            frame = requireNonNull(frame, "frame is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record WindowFrame(FrameType type, FrameBound start, Optional<FrameBound> end, SourceSpan span)
    {
        public WindowFrame
        {
            type = requireNonNull(type, "type is null");
            start = requireNonNull(start, "start is null");
            end = requireNonNull(end, "end is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public enum FrameType
    {
        RANGE,
        ROWS,
    }

    public record FrameBound(FrameBoundType type, Optional<Expression> value, SourceSpan span)
    {
        public FrameBound
        {
            type = requireNonNull(type, "type is null");
            value = requireNonNull(value, "value is null");
            span = requireNonNull(span, "span is null");
            boolean valueRequired = type == FrameBoundType.PRECEDING || type == FrameBoundType.FOLLOWING;
            if (value.isPresent() != valueRequired) {
                throw new IllegalArgumentException("window frame bound value does not match bound type");
            }
        }
    }

    public enum FrameBoundType
    {
        CURRENT_ROW,
        FOLLOWING,
        PRECEDING,
        UNBOUNDED_FOLLOWING,
        UNBOUNDED_PRECEDING,
    }

    public enum UnaryOperator
    {
        NEGATE,
        NOT,
        POSITIVE,
    }

    public enum BinaryOperator
    {
        ADD,
        AND,
        DIVIDE,
        EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        MODULO,
        MULTIPLY,
        NOT_EQUAL,
        OR,
        SUBTRACT,
    }

    public record ColumnReference(List<Identifier> parts, SourceSpan span)
            implements Expression
    {
        public ColumnReference
        {
            parts = List.copyOf(requireNonNull(parts, "parts is null"));
            if (parts.isEmpty()) {
                throw new IllegalArgumentException("parts is empty");
            }
            span = requireNonNull(span, "span is null");
        }
    }

    public record Placeholder(String name, SourceSpan span)
            implements Expression
    {
        public Placeholder
        {
            name = requireNonNull(name, "name is null");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("name is empty");
            }
            span = requireNonNull(span, "span is null");
        }
    }

    public record Literal(LiteralKind kind, String value, SourceSpan span)
            implements Expression
    {
        public Literal
        {
            kind = requireNonNull(kind, "kind is null");
            value = requireNonNull(value, "value is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public enum LiteralKind
    {
        BOOLEAN,
        INTEGER,
        NULL,
        STRING,
    }

    public sealed interface Relation
            permits AliasedRelation,
                    CommonTableReference,
                    JoinRelation,
                    SubqueryRelation,
                    TablePlaceholder,
                    TableReference,
                    ValuesRelation
    {
        SourceSpan span();
    }

    public record AliasedRelation(Relation relation, Identifier alias, List<Identifier> columnAliases, SourceSpan span)
            implements Relation
    {
        public AliasedRelation(Relation relation, Identifier alias, SourceSpan span)
        {
            this(relation, alias, List.of(), span);
        }

        public AliasedRelation
        {
            relation = requireNonNull(relation, "relation is null");
            alias = requireNonNull(alias, "alias is null");
            columnAliases = List.copyOf(requireNonNull(columnAliases, "columnAliases is null"));
            span = requireNonNull(span, "span is null");
        }
    }

    public record CommonTableReference(Identifier name, SourceSpan span)
            implements Relation
    {
        public CommonTableReference
        {
            name = requireNonNull(name, "name is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record SubqueryRelation(HogQlQuery query, SourceSpan span)
            implements Relation
    {
        public SubqueryRelation
        {
            query = requireNonNull(query, "query is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record ValuesRelation(List<List<Expression>> rows, SourceSpan span)
            implements Relation
    {
        public ValuesRelation
        {
            rows = requireNonNull(rows, "rows is null").stream()
                    .map(row -> List.copyOf(requireNonNull(row, "row is null")))
                    .toList();
            if (rows.isEmpty()) {
                throw new IllegalArgumentException("rows is empty");
            }
            int columnCount = rows.getFirst().size();
            if (columnCount == 0 || rows.stream().anyMatch(row -> row.size() != columnCount)) {
                throw new IllegalArgumentException("VALUES rows must have the same non-zero column count");
            }
            span = requireNonNull(span, "span is null");
        }

        public int columnCount()
        {
            return rows.getFirst().size();
        }
    }

    public enum JoinType
    {
        CROSS,
        INNER,
        LEFT,
        RIGHT,
        FULL,
    }

    public sealed interface JoinCriteria
            permits JoinOn, JoinUsing
    {
        SourceSpan span();
    }

    public record JoinOn(Expression expression, SourceSpan span)
            implements JoinCriteria
    {
        public JoinOn
        {
            expression = requireNonNull(expression, "expression is null");
            span = requireNonNull(span, "span is null");
        }
    }

    public record JoinUsing(List<Identifier> columns, SourceSpan span)
            implements JoinCriteria
    {
        public JoinUsing
        {
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("columns is empty");
            }
            span = requireNonNull(span, "span is null");
        }
    }

    public record JoinRelation(JoinType type, Relation left, Relation right, Optional<JoinCriteria> criteria, SourceSpan span)
            implements Relation
    {
        public JoinRelation
        {
            type = requireNonNull(type, "type is null");
            left = requireNonNull(left, "left is null");
            right = requireNonNull(right, "right is null");
            criteria = requireNonNull(criteria, "criteria is null");
            span = requireNonNull(span, "span is null");
            if ((type == JoinType.CROSS) == criteria.isPresent()) {
                throw new IllegalArgumentException("cross joins must omit criteria and qualified joins must provide criteria");
            }
        }
    }

    public record TableReference(List<Identifier> parts, SourceSpan span)
            implements Relation
    {
        public TableReference
        {
            parts = List.copyOf(requireNonNull(parts, "parts is null"));
            if (parts.isEmpty()) {
                throw new IllegalArgumentException("parts is empty");
            }
            span = requireNonNull(span, "span is null");
        }
    }

    public record TablePlaceholder(Placeholder placeholder)
            implements Relation
    {
        public TablePlaceholder
        {
            placeholder = requireNonNull(placeholder, "placeholder is null");
        }

        @Override
        public SourceSpan span()
        {
            return placeholder.span();
        }
    }

    public record Identifier(String value, boolean delimited, SourceSpan span)
    {
        public Identifier
        {
            value = requireNonNull(value, "value is null");
            if (value.isEmpty()) {
                throw new IllegalArgumentException("value is empty");
            }
            span = requireNonNull(span, "span is null");
        }
    }

    public record SourceSpan(int startOffset, int endOffset, int startLine, int startColumn, int endLine, int endColumn)
    {
        public SourceSpan
        {
            if (startOffset < 0) {
                throw new IllegalArgumentException("startOffset is negative");
            }
            if (endOffset < startOffset) {
                throw new IllegalArgumentException("endOffset is before startOffset");
            }
            if (startLine < 1) {
                throw new IllegalArgumentException("startLine must be positive");
            }
            if (startColumn < 1) {
                throw new IllegalArgumentException("startColumn must be positive");
            }
            if (endLine < startLine) {
                throw new IllegalArgumentException("endLine is before startLine");
            }
            if (endColumn < 1) {
                throw new IllegalArgumentException("endColumn must be positive");
            }
        }
    }
}
