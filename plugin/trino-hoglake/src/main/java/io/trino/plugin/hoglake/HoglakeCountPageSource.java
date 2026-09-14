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

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

/**
 * Emits the catalog's row cardinality without opening the data file.
 */
final class HoglakeCountPageSource
        implements ConnectorPageSource
{
    // Bound downstream work per page even when a single file has more than Integer.MAX_VALUE rows.
    private static final int MAX_PAGE_POSITIONS = 1_048_576;

    private long remainingRows;

    public HoglakeCountPageSource(long recordCount)
    {
        if (recordCount < 0) {
            throw new IllegalArgumentException("recordCount is negative");
        }
        remainingRows = recordCount;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return remainingRows == 0;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }
        int positions = (int) Math.min(remainingRows, MAX_PAGE_POSITIONS);
        remainingRows -= positions;
        return SourcePage.create(positions);
    }

    @Override
    public void close()
    {
        remainingRows = 0;
    }
}
