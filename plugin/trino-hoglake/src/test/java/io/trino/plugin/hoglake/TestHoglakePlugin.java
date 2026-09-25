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

import io.airlift.units.DataSize;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.Map;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHoglakePlugin
{
    @Test
    void testCreateAndShutdownConnector()
    {
        ConnectorFactory factory = new HoglakePlugin().getConnectorFactories().iterator().next();
        assertThat(factory.getName()).isEqualTo("hoglake");
        factory.create("test", Map.of(
                "hoglake.uri", "http://localhost:8080",
                "hoglake.catalog", "test",
                "hoglake.s3.region", "us-east-1"), new TestingConnectorContext()).shutdown();
    }

    @Test
    void testMaxSplitSizeSessionPropertyDefaultsToTheCatalogConfig()
    {
        ConnectorFactory factory = new HoglakePlugin().getConnectorFactories().iterator().next();
        Connector connector = factory.create("test", Map.of(
                "hoglake.uri", "http://localhost:8080",
                "hoglake.catalog", "test",
                "hoglake.s3.region", "us-east-1",
                "hoglake.max-split-size", "512MB"), new TestingConnectorContext());
        try {
            assertThat(connector.getSessionProperties())
                    .filteredOn(property -> property.getName().equals("max_split_size"))
                    .singleElement()
                    .satisfies(property -> assertThat(property.getDefaultValue()).isEqualTo(DataSize.of(512, MEGABYTE)));
        }
        finally {
            connector.shutdown();
        }
    }

    @Test
    void testFooterCacheStatisticsAreExportedPerCatalog()
            throws Exception
    {
        String catalogName = "hoglake_jmx_" + randomNameSuffix();
        ConnectorFactory factory = new HoglakePlugin().getConnectorFactories().iterator().next();
        Connector connector = factory.create(catalogName, Map.of(
                "hoglake.uri", "http://localhost:8080",
                "hoglake.catalog", "test",
                "hoglake.s3.region", "us-east-1"), new TestingConnectorContext());
        try {
            MBeanServer mbeanServer = getPlatformMBeanServer();
            ObjectName name = new ObjectName("io.trino.plugin.hoglake:type=HoglakeParquetFooterCache,name=" + catalogName);
            assertThat(mbeanServer.isRegistered(name)).isTrue();
            assertThat(mbeanServer.getMBeanInfo(name).getAttributes())
                    .extracting(MBeanAttributeInfo::getName)
                    .contains("CacheStats.HitRate", "CacheStats.MissRate", "CacheStats.LoadCount", "CacheStats.RequestCount");
            assertThat(mbeanServer.getAttribute(name, "CacheStats.RequestCount")).isEqualTo(0L);
        }
        finally {
            connector.shutdown();
        }
    }
}
