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
package io.trino.plugin.hoglake;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakePartitioning
{
    @Test
    void crossClientTransformVectors()
    {
        // Values from pyhoglake.transforms (Iceberg murmur3 x86_32, long LE).
        var integer = new HoglakeColumnHandle("id", 1, BIGINT, true);
        assertValue(integer, 34L, "bucket", 16, "3");
        var date = new HoglakeColumnHandle("d", 2, DATE, true);
        assertValue(date, -1L, "day", null, "-1");
        assertValue(date, -1L, "month", null, "-1");
        assertValue(date, -1L, "year", null, "-1");
        assertValue(date, -1L, "identity", null, "1969-12-31");
        var timestamp = new HoglakeColumnHandle("ts", 3, TIMESTAMP_MICROS, true);
        assertValue(timestamp, -1L, "hour", null, "-1");
        assertValue(timestamp, -1L, "day", null, "-1");
        assertValue(timestamp, -1L, "identity", null, "1969-12-31T23:59:59.999999");
        assertValue(timestamp, 1000L, "identity", null, "1970-01-01T00:00:00.001000");
        assertThatThrownBy(() -> HoglakePartitioning.parse(List.of("truncate(id, 4)"), List.of(integer)))
                .hasMessageContaining("Unsupported partition expression");
        assertThatThrownBy(() -> HoglakePartitioning.parse(List.of("hour(d)"), List.of(date)))
                .hasMessageContaining("Unsupported temporal partition");
    }

    @Test
    void pythonFloatingPartitionSpelling()
            throws Exception
    {
        var column = new HoglakeColumnHandle("f", 1, DoubleType.DOUBLE, true);
        try (var input = getClass().getResourceAsStream("/partition-float-vectors.json")) {
            var vectors = new ObjectMapper().readTree(input);
            for (var vector : vectors) {
                double value = Double.longBitsToDouble(Long.parseUnsignedLong(vector.get("bits").asText(), 16));
                var block = DoubleType.DOUBLE.createBlockBuilder(null, 1);
                DoubleType.DOUBLE.writeDouble(block, value);
                assertThat(HoglakePartitioning.value(new HoglakeDtos.PartitionField(1, "identity", null), column, block.build(), 0))
                        .as("bits %s", vector.get("bits").asText()).isEqualTo(vector.get("wire").asText());
            }
        }
    }

    private static void assertValue(HoglakeColumnHandle column, long value, String transform, Integer param, String expected)
    {
        var block = column.type().createBlockBuilder(null, 1);
        column.type().writeLong(block, value);
        assertThat(HoglakePartitioning.value(new HoglakeDtos.PartitionField(column.fieldId(), transform, param), column, block.build(), 0))
                .isEqualTo(expected);
    }
}
