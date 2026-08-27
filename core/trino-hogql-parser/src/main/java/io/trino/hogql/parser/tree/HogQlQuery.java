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

public record HogQlQuery(List<Projection> projections, Optional<TableReference> from, Optional<Expression> where, SourceSpan span)
{
    public HogQlQuery
    {
        projections = List.copyOf(requireNonNull(projections, "projections is null"));
        from = requireNonNull(from, "from is null");
        where = requireNonNull(where, "where is null");
        span = requireNonNull(span, "span is null");
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
                    ColumnReference,
                    FunctionCall,
                    InExpression,
                    IsNullExpression,
                    Literal,
                    TupleExpression,
                    UnaryExpression
    {
        SourceSpan span();
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

    public record FunctionCall(Identifier name, List<Expression> arguments, SourceSpan span)
            implements Expression
    {
        public FunctionCall
        {
            name = requireNonNull(name, "name is null");
            arguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
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

    public record TableReference(List<Identifier> parts, SourceSpan span)
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
