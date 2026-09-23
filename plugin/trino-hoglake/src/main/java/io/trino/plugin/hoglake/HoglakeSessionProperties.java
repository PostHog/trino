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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMinDataSize;

public class HoglakeSessionProperties
        implements SessionPropertiesProvider
{
    private static final DataSize MINIMUM_MAX_SPLIT_SIZE = DataSize.of(1, MEGABYTE);

    private static final String MAX_SPLIT_SIZE = "max_split_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HoglakeSessionProperties(HoglakeConfig config)
    {
        sessionProperties = ImmutableList.of(
                dataSizeProperty(
                        MAX_SPLIT_SIZE,
                        "Largest byte range of one Parquet file assigned to a single split",
                        config.getMaxSplitSize(),
                        value -> validateMinDataSize(MAX_SPLIT_SIZE, value, MINIMUM_MAX_SPLIT_SIZE),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }
}
