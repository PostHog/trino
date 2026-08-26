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

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Serves pages a count was answered with from the DuckLake catalog, either the rows of a data file
 * counted without reading it or the row count of a whole table. It touches no file, and so reports
 * no bytes read: a query answered this way costs nothing to read, and its statistics say so.
 */
public class DuckLakeCountPageSource
        implements ConnectorPageSource
{
    private final Iterator<Page> pages;
    private boolean closed;

    public DuckLakeCountPageSource(List<Page> pages)
    {
        this.pages = requireNonNull(pages, "pages is null").iterator();
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
        return closed || !pages.hasNext();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }
        return SourcePage.create(pages.next());
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
