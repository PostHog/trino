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
package io.trino.plugin.ducklake.util;

import io.trino.plugin.ducklake.metastore.DuckLakeTableEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestPathResolver
{
    @Test
    void testAbsolutePath()
    {
        assertThat(PathResolver.resolve("s3://bucket/base/", "s3://other/file.parquet", false))
                .isEqualTo("s3://other/file.parquet");
    }

    @Test
    void testRelativePath()
    {
        assertThat(PathResolver.resolve("s3://bucket/base/", "main/", true))
                .isEqualTo("s3://bucket/base/main/");
        assertThat(PathResolver.resolve("s3://bucket/base", "main/", true))
                .isEqualTo("s3://bucket/base/main/");
        assertThat(PathResolver.resolve("local:///", "file.parquet", true))
                .isEqualTo("local:///file.parquet");
    }

    @Test
    void testEmptyPaths()
    {
        assertThat(PathResolver.resolve("s3://bucket/base/", "", true))
                .isEqualTo("s3://bucket/base/");
        assertThat(PathResolver.resolve("", "main/", true))
                .isEqualTo("main/");
    }

    @Test
    void testTableLocation()
    {
        DuckLakeTableEntry relativeEverywhere = new DuckLakeTableEntry(1, 0, "main", "orders", "orders/", true, "main/", true);
        assertThat(PathResolver.tableLocation("s3://bucket/lake/", relativeEverywhere))
                .isEqualTo("s3://bucket/lake/main/orders/");

        DuckLakeTableEntry absoluteTable = new DuckLakeTableEntry(1, 0, "main", "orders", "s3://elsewhere/orders/", false, "main/", true);
        assertThat(PathResolver.tableLocation("s3://bucket/lake/", absoluteTable))
                .isEqualTo("s3://elsewhere/orders/");

        DuckLakeTableEntry absoluteSchema = new DuckLakeTableEntry(1, 0, "main", "orders", "orders/", true, "s3://elsewhere/main/", false);
        assertThat(PathResolver.tableLocation("s3://bucket/lake/", absoluteSchema))
                .isEqualTo("s3://elsewhere/main/orders/");
    }
}
