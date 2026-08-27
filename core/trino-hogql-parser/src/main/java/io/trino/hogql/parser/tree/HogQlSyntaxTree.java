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

public record HogQlSyntaxTree(LanguageClass languageClass, Node root)
{
    public HogQlSyntaxTree
    {
        languageClass = requireNonNull(languageClass, "languageClass is null");
        root = requireNonNull(root, "root is null");
    }

    public enum LanguageClass
    {
        READ_ONLY_QUERY,
        HOGQLX,
    }

    public sealed interface Element
            permits Node, Token
    {
        HogQlQuery.SourceSpan span();
    }

    public record Node(String rule, Optional<String> alternative, List<Element> children, HogQlQuery.SourceSpan span)
            implements Element
    {
        public Node
        {
            rule = requireNonNull(rule, "rule is null");
            alternative = requireNonNull(alternative, "alternative is null");
            children = List.copyOf(requireNonNull(children, "children is null"));
            span = requireNonNull(span, "span is null");
            if (rule.isBlank() || alternative.stream().anyMatch(String::isBlank)) {
                throw new IllegalArgumentException("syntax node identity is empty");
            }
        }
    }

    public record Token(String type, String text, HogQlQuery.SourceSpan span)
            implements Element
    {
        public Token
        {
            type = requireNonNull(type, "type is null");
            text = requireNonNull(text, "text is null");
            span = requireNonNull(span, "span is null");
            if (type.isBlank()) {
                throw new IllegalArgumentException("token type is empty");
            }
        }
    }
}
