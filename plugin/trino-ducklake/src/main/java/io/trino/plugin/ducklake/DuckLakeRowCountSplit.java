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

import io.trino.spi.connector.ConnectorSplit;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;

/**
 * The single split of a scan whose {@code count(*)} was answered from the catalog. It names no
 * file: the row count it carries is the whole result, and the scan reads nothing.
 */
public record DuckLakeRowCountSplit(long rowCount)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeRowCountSplit.class));

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
