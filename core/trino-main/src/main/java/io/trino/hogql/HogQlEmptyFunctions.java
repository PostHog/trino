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
package io.trino.hogql;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class HogQlEmptyFunctions
{
    public static final String NAME = "hogql_empty";

    private HogQlEmptyFunctions() {}

    @ScalarFunction(value = NAME, hidden = true, neverFails = true)
    @SqlType(BOOLEAN)
    public static boolean varcharIsEmpty(@SqlType(VARCHAR) Slice value)
    {
        return value.length() == 0;
    }

    @ScalarFunction(value = NAME, hidden = true, neverFails = true)
    @TypeParameter("E")
    @SqlType(BOOLEAN)
    public static boolean arrayIsEmpty(@SqlType("array(E)") Block value)
    {
        return value.getPositionCount() == 0;
    }

    @ScalarFunction(value = NAME, hidden = true, neverFails = true)
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(BOOLEAN)
    public static boolean mapIsEmpty(@SqlType("map(K,V)") SqlMap value)
    {
        return value.getSize() == 0;
    }
}
