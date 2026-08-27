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

public record HogQlQuery(List<Projection> projections, Optional<TableReference> from, SourceSpan span)
{
    public HogQlQuery
    {
        projections = List.copyOf(requireNonNull(projections, "projections is null"));
        from = requireNonNull(from, "from is null");
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

    public record ExpressionProjection(Expression expression)
            implements Projection
    {
        public ExpressionProjection
        {
            expression = requireNonNull(expression, "expression is null");
        }

        @Override
        public SourceSpan span()
        {
            return expression.span();
        }
    }

    public sealed interface Expression
            permits ColumnReference, Literal
    {
        SourceSpan span();
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

    public record SourceSpan(int startOffset, int endOffset, int line, int column)
    {
        public SourceSpan
        {
            if (startOffset < 0) {
                throw new IllegalArgumentException("startOffset is negative");
            }
            if (endOffset < startOffset) {
                throw new IllegalArgumentException("endOffset is before startOffset");
            }
            if (line < 1) {
                throw new IllegalArgumentException("line must be positive");
            }
            if (column < 1) {
                throw new IllegalArgumentException("column must be positive");
            }
        }
    }
}
