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

import io.trino.hogql.parser.HogQlParser;
import io.trino.hogql.parser.HogQlParsingException;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.Statement;

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
        try {
            return TrinoAstFactory.createStatement(parser.parseStatement(hogql));
        }
        catch (HogQlParsingException exception) {
            throw new ParsingException(
                    exception.getErrorMessage(),
                    null,
                    exception.getLineNumber(),
                    exception.getColumnNumber());
        }
    }
}
