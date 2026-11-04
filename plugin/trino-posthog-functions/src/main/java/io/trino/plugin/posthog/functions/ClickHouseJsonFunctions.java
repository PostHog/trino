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
import io.trino.plugin.posthog.functions.ClickHouseJsonKeysAndValues.Target;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.plugin.posthog.functions.ClickHouseJsonKeysAndValues.extract;

/**
 * ClickHouse's {@code JSONExtractKeysAndValues}. A Trino function cannot choose its return type from
 * the value of an argument, so each ClickHouse value type family has its own function. Each takes the
 * JSON text, a path as a JSON array of keys and 1-based indexes (negative counts from the end), and
 * whether a JSON null is kept as a NULL value, as ClickHouse does for a Nullable type.
 */
public final class ClickHouseJsonFunctions
{
    private static final String DESCRIPTION = "ClickHouse JSONExtractKeysAndValues: the members of the JSON object at a path, with their values converted";

    private ClickHouseJsonFunctions() {}

    @Description(DESCRIPTION)
    @ScalarFunction("ch_json_extract_keys_and_values_float64")
    @SqlType("array(row(varchar,double))")
    public static Block float64(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice path, @SqlType(StandardTypes.BOOLEAN) boolean keepNulls)
    {
        return extract(json, path, keepNulls, Target.FLOAT64, 0, 0);
    }

    @Description(DESCRIPTION)
    @ScalarFunction("ch_json_extract_keys_and_values_float32")
    @SqlType("array(row(varchar,real))")
    public static Block float32(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice path, @SqlType(StandardTypes.BOOLEAN) boolean keepNulls)
    {
        return extract(json, path, keepNulls, Target.FLOAT32, 0, 0);
    }

    @Description(DESCRIPTION + ", for an integer type whose range is [low, high]")
    @ScalarFunction("ch_json_extract_keys_and_values_int")
    @SqlType("array(row(varchar,bigint))")
    public static Block integer(
            @SqlType(StandardTypes.VARCHAR) Slice json,
            @SqlType(StandardTypes.VARCHAR) Slice path,
            @SqlType(StandardTypes.BOOLEAN) boolean keepNulls,
            @SqlType(StandardTypes.BIGINT) long low,
            @SqlType(StandardTypes.BIGINT) long high)
    {
        return extract(json, path, keepNulls, Target.INTEGER, low, high);
    }

    @Description(DESCRIPTION)
    @ScalarFunction("ch_json_extract_keys_and_values_uint64")
    @SqlType("array(row(varchar,decimal(20,0)))")
    public static Block uint64(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice path, @SqlType(StandardTypes.BOOLEAN) boolean keepNulls)
    {
        return extract(json, path, keepNulls, Target.UINT64, 0, 0);
    }

    @Description(DESCRIPTION)
    @ScalarFunction("ch_json_extract_keys_and_values_bool")
    @SqlType("array(row(varchar,boolean))")
    public static Block bool(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice path, @SqlType(StandardTypes.BOOLEAN) boolean keepNulls)
    {
        return extract(json, path, keepNulls, Target.BOOL, 0, 0);
    }

    @Description(DESCRIPTION)
    @ScalarFunction("ch_json_extract_keys_and_values_string")
    @SqlType("array(row(varchar,varchar))")
    public static Block string(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice path, @SqlType(StandardTypes.BOOLEAN) boolean keepNulls)
    {
        return extract(json, path, keepNulls, Target.STRING, 0, 0);
    }
}
