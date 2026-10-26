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
package io.trino.plugin.ducklake;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.ducklake.DuckLakeFileImporter.decodePercentEscapes;
import static io.trino.plugin.ducklake.DuckLakeFileImporter.hiveValues;
import static io.trino.plugin.ducklake.DuckLakeFileImporter.validateLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckLakeFileImporter
{
    @Test
    void testPercentEscapesAreDecodedLikeDuckDb()
    {
        assertThat(decodePercentEscapes("a%2Fb%20c+d")).isEqualTo("a/b c+d");
        assertThat(decodePercentEscapes("%C3%A9%e2%82%ac")).isEqualTo("é€");
        // a '%' that does not start an escape is kept as it is
        assertThat(decodePercentEscapes("50%")).isEqualTo("50%");
        assertThat(decodePercentEscapes("%zz")).isEqualTo("%zz");
        assertThat(decodePercentEscapes("%2")).isEqualTo("%2");
        assertThat(decodePercentEscapes("50%off")).isEqualTo("50%off");
    }

    @Test
    void testHiveValuesFollowDuckDb()
    {
        // DuckDB takes the first directory naming a key, decodes values only, and ignores the file name
        assertThat(hiveValues("s3://bucket/lake/dt=a/x/dt=b/f=1.parquet")).isEqualTo(Map.of("dt", "a"));
        assertThat(hiveValues("s3://bucket/lake/pct=50%/enc=a%2Fb/%41b=x%2fy/f.parquet"))
                .isEqualTo(Map.of("pct", "50%", "enc", "a/b", "%41b", "x/y"));
        assertThat(hiveValues("s3://bucket/lake/Region=West/empty=/f.parquet")).isEqualTo(Map.of("region", "West", "empty", ""));
    }

    @Test
    void testLocationValidation()
    {
        validateLocation("s3://bucket/lake/", "s3://bucket/lake/imports/50%off.parquet");
        validateLocation("s3://bucket/lake", "s3://bucket/lake/imports/a+b%zz.parquet");
        assertThatThrownBy(() -> validateLocation("s3://bucket/lake/", "s3://bucket/lake/imports/%2e%2e/secret.parquet"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("traversal");
        assertThatThrownBy(() -> validateLocation("s3://bucket/lake/", "s3://bucket/lake/imports/a%5Cb.parquet"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("catalog data path");
        assertThatThrownBy(() -> validateLocation("s3://bucket/lake/", "s3://bucket/other/f.parquet"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("catalog data path");
        assertThatThrownBy(() -> validateLocation("s3://bucket/lake/", "s3://bucket/lake/imports/f.parquet#fragment"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("catalog data path");
    }
}
