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

import io.trino.spi.type.DecimalType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestClickHouseHashFunctions
{
    private static final DecimalType UINT64 = createDecimalType(20, 0);

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new PostHogFunctionsPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testStringsMatchClickHouse()
            throws IOException
    {
        // Each line is the hex of a UTF-8 input and ClickHouse 26.9's cityHash64 of it. The
        // inputs cover every length branch (0, 1-3, 4-8, 9-16, 17-32, 33-64) and 1 to 15 turns
        // of the 64-byte loop, multi-byte UTF-8 and NUL bytes.
        List<String> lines;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                requireNonNull(getClass().getResourceAsStream("/cityhash64-clickhouse.tsv")), UTF_8))) {
            lines = reader.lines().toList();
        }
        assertThat(lines).hasSize(33);

        for (String line : lines) {
            String[] fields = line.split("\t", -1);
            String binary = "from_hex('" + fields[0] + "')";
            assertThat(assertions.function("ch_cityhash64", binary))
                    .describedAs("varbinary %s", fields[0])
                    .isEqualTo(decimal(fields[1], UINT64));
            assertThat(assertions.function("ch_cityhash64", "from_utf8(" + binary + ")"))
                    .describedAs("varchar %s", fields[0])
                    .isEqualTo(decimal(fields[1], UINT64));
        }
    }

    @Test
    public void testIntegersMatchClickHouseIntHash64()
    {
        assertThat(assertions.function("ch_cityhash64", "BIGINT '0'"))
                .isEqualTo(decimal("4761183170873013810", UINT64));
        assertThat(assertions.function("ch_cityhash64", "5"))
                .isEqualTo(decimal("15228578409069794350", UINT64));
        assertThat(assertions.function("ch_cityhash64", "BIGINT '4294967295'"))
                .isEqualTo(decimal("9168733277332772950", UINT64));
        assertThat(assertions.function("ch_cityhash64", "BIGINT '9223372036854775807'"))
                .isEqualTo(decimal("16054072765933828340", UINT64));
    }

    @Test
    public void testIntegersHashAtTheirOwnWidth()
    {
        assertThat(assertions.function("ch_cityhash64", "TINYINT '0'"))
                .isEqualTo(decimal("4761183170873013810", UINT64));
        assertThat(assertions.function("ch_cityhash64", "TINYINT '127'"))
                .isEqualTo(decimal("6947037243239945742", UINT64));
        assertThat(assertions.function("ch_cityhash64", "TINYINT '-1'"))
                .isEqualTo(decimal("8055013221972926055", UINT64));
        assertThat(assertions.function("ch_cityhash64", "TINYINT '-128'"))
                .isEqualTo(decimal("10573636647639147473", UINT64));
        assertThat(assertions.function("ch_cityhash64", "SMALLINT '-1'"))
                .isEqualTo(decimal("6547939410751295652", UINT64));
        assertThat(assertions.function("ch_cityhash64", "SMALLINT '-32768'"))
                .isEqualTo(decimal("11349277977595110966", UINT64));
        assertThat(assertions.function("ch_cityhash64", "INTEGER '-1'"))
                .isEqualTo(decimal("9168733277332772950", UINT64));
        assertThat(assertions.function("ch_cityhash64", "INTEGER '-2147483648'"))
                .isEqualTo(decimal("6300953685602397207", UINT64));
        assertThat(assertions.function("ch_cityhash64", "BIGINT '-1'"))
                .isEqualTo(decimal("14600443904207254319", UINT64));
        assertThat(assertions.function("ch_cityhash64", "BIGINT '-9223372036854775808'"))
                .isEqualTo(decimal("7378616448805205431", UINT64));
    }

    @Test
    public void testResultIsUnsigned()
    {
        // 15228578409069794350 is above 2^63; a signed bigint would make this bucket negative
        assertThat(assertions.expression("CAST(ch_cityhash64(5) % 10 AS bigint)"))
                .isEqualTo(0L);
        assertThat(assertions.expression("CAST(ch_cityhash64(5) AS varchar)"))
                .isEqualTo("15228578409069794350");
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.function("ch_cityhash64", "CAST(NULL AS varchar)")).isNull(UINT64);
        assertThat(assertions.function("ch_cityhash64", "CAST(NULL AS varbinary)")).isNull(UINT64);
        assertThat(assertions.function("ch_cityhash64", "CAST(NULL AS bigint)")).isNull(UINT64);
        assertThat(assertions.function("ch_cityhash64", "CAST(NULL AS tinyint)")).isNull(UINT64);
    }
}
