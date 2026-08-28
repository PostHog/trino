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

public record HogQlParserLimits(int maxTokens, int maxParseDepth, int maxParseTreeNodes)
{
    private static final HogQlParserLimits DEFAULTS = new HogQlParserLimits(200_000, 256, 500_000);

    public HogQlParserLimits
    {
        if (maxTokens < 1) {
            throw new IllegalArgumentException("maxTokens must be positive");
        }
        if (maxParseDepth < 1) {
            throw new IllegalArgumentException("maxParseDepth must be positive");
        }
        if (maxParseTreeNodes < 1) {
            throw new IllegalArgumentException("maxParseTreeNodes must be positive");
        }
    }

    public static HogQlParserLimits defaults()
    {
        return DEFAULTS;
    }
}
