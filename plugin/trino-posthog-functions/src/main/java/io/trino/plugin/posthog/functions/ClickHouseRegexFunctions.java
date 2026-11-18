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
package io.trino.plugin.posthog.functions;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * ClickHouse's {@code multiMatchAnyIndex}, so that HogQL compiled to Trino returns the same values.
 * <p>
 * ClickHouse returns the pattern whose match Hyperscan reports first, which is the one whose
 * match is confirmed earliest in the haystack, not the first matching pattern in the array:
 * {@code multiMatchAnyIndex('abc', ['c', 'a', 'b'])} is 2. See {@link ClickHouseMultiMatch}
 * for the exact order and {@link HyperscanPatternTranslator} for the pattern dialect.
 * <p>
 * The function is an instance method so that each compiled expression keeps the compiled
 * patterns of the last array it saw. A constant array is compiled once, not once per row.
 */
public final class ClickHouseRegexFunctions
{
    // replaced as a whole, and the record and its matcher are immutable, so a racing reader sees a consistent pair
    private CompiledPatterns compiled;

    public ClickHouseRegexFunctions() {}

    @Description("ClickHouse multiMatchAnyIndex: index of the pattern whose match Hyperscan reports first, or 0")
    @ScalarFunction("ch_multimatchanyindex")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public long multiMatchAnyIndex(@SqlType("varchar(x)") Slice haystack, @SqlType("array(varchar)") Block patterns)
    {
        return compiledPatterns(patterns).find(haystack);
    }

    private ClickHouseMultiMatch compiledPatterns(Block patterns)
    {
        CompiledPatterns current = compiled;
        if (current != null && current.block() == patterns) {
            return current.matcher();
        }
        List<Slice> values = patternValues(patterns);
        ClickHouseMultiMatch matcher;
        if (current != null && current.matcher().patterns().equals(values)) {
            matcher = current.matcher();
        }
        else {
            matcher = ClickHouseMultiMatch.compile(values);
        }
        compiled = new CompiledPatterns(patterns, matcher);
        return matcher;
    }

    private static List<Slice> patternValues(Block patterns)
    {
        List<Slice> values = new ArrayList<>(patterns.getPositionCount());
        for (int position = 0; position < patterns.getPositionCount(); position++) {
            if (patterns.isNull(position)) {
                // ClickHouse rejects Array(Nullable(String)) as an argument type
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "ch_multimatchanyindex patterns must not contain NULL");
            }
            values.add(VARCHAR.getSlice(patterns, position));
        }
        return values;
    }

    private record CompiledPatterns(Block block, ClickHouseMultiMatch matcher) {}
}
