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
    public HogQlQuery
    {
        projections = List.copyOf(requireNonNull(projections, "projections is null"));
        from = requireNonNull(from, "from is null");
        where = requireNonNull(where, "where is null");
        groupBy = List.copyOf(requireNonNull(groupBy, "groupBy is null"));
        having = requireNonNull(having, "having is null");
        orderBy = List.copyOf(requireNonNull(orderBy, "orderBy is null"));
        limit = requireNonNull(limit, "limit is null");
        offset = requireNonNull(offset, "offset is null");
        span = requireNonNull(span, "span is null");
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
                    Placeholder,
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
            Identifier name,
            List<Expression> arguments,
            boolean distinct,
            List<SortItem> orderBy,
            Optional<Expression> filter,
            SourceSpan span)
            implements Expression
    {
        public FunctionCall
        {
            name = requireNonNull(name, "name is null");
            arguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
            orderBy = List.copyOf(requireNonNull(orderBy, "orderBy is null"));
            filter = requireNonNull(filter, "filter is null");
            span = requireNonNull(span, "span is null");
        }
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
                    JoinRelation,
                    TablePlaceholder,
                    TableReference
    {
        SourceSpan span();
    }

    public record AliasedRelation(Relation relation, Identifier alias, SourceSpan span)
            implements Relation
    {
        public AliasedRelation
        {
            relation = requireNonNull(relation, "relation is null");
            alias = requireNonNull(alias, "alias is null");
            span = requireNonNull(span, "span is null");
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
