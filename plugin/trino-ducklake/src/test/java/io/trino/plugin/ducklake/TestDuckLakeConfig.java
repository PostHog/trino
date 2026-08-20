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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

final class TestDuckLakeConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DuckLakeConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setMetadataSchema("public")
                .setDataPath(null)
                .setFileStatisticsPruningEnabled(true)
                .setMaxSplitSize(DataSize.of(64, MEGABYTE)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ducklake.metadata.connection-url", "jdbc:postgresql://example.net:5432/lake")
                .put("ducklake.metadata.connection-user", "alice")
                .put("ducklake.metadata.connection-password", "secret")
                .put("ducklake.metadata.schema", "ducklake")
                .put("ducklake.data-path", "s3://bucket/prefix/")
                .put("ducklake.file-statistics-pruning.enabled", "false")
                .put("ducklake.max-split-size", "32MB")
                .buildOrThrow();

        DuckLakeConfig expected = new DuckLakeConfig()
                .setConnectionUrl("jdbc:postgresql://example.net:5432/lake")
                .setConnectionUser("alice")
                .setConnectionPassword("secret")
                .setMetadataSchema("ducklake")
                .setDataPath("s3://bucket/prefix/")
                .setFileStatisticsPruningEnabled(false)
                .setMaxSplitSize(DataSize.of(32, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
