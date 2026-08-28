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
package io.trino.plugin.ducklake.function;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;

public final class CityHash64Function
{
    private CityHash64Function() {}

    @Description("Returns the ClickHouse CityHash 1.0.2 hash of a string")
    @ScalarFunction("cityhash64")
    @LiteralParameters("x")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64(@SqlType("varchar(x)") Slice value)
    {
        return Int128.valueOf(0, ClickHouseCityHash64.hash(value));
    }
}
