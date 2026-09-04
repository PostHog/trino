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
package io.trino.hogql.parser;

public final class HogQlParsingException
        extends RuntimeException
{
    private final int line;
    private final int column;

    public HogQlParsingException(String message, Throwable cause, int line, int column)
    {
        super(message, cause);
        if (line < 1) {
            throw new IllegalArgumentException("line must be positive");
        }
        if (column < 1) {
            throw new IllegalArgumentException("column must be positive");
        }
        this.line = line;
        this.column = column;
    }

    public int getLineNumber()
    {
        return line;
    }

    public int getColumnNumber()
    {
        return column;
    }

    public String getErrorMessage()
    {
        return super.getMessage();
    }

    @Override
    public String getMessage()
    {
        return "line %s:%s: %s".formatted(line, column, getErrorMessage());
    }
}
