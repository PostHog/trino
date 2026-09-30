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
package io.trino.filesystem.cache;

import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TestCatalogVersionedCacheFile
{
    @Test
    void testVersionedKeysRetainInvalidationPrefix()
            throws IOException
    {
        MemoryFileSystem delegate = new MemoryFileSystem();
        Location location = Location.of("memory:///data/file");
        delegate.newOutputFile(location).createOrOverwrite(new byte[] {1});
        List<CacheKey> reads = new ArrayList<>();
        List<CacheKey> invalidations = new ArrayList<>();
        BlobCache cache = new BlobCache()
        {
            @Override
            public Optional<Blob> get(CacheKey key, BlobSource source)
            {
                reads.add(key);
                return Optional.empty();
            }

            @Override
            public void tryInvalidate(CacheKey key)
            {
                invalidations.add(key);
            }
        };
        CacheFileSystem fileSystem = new CacheFileSystem(delegate, cache, new DefaultCacheKeyProvider());
        for (String version : List.of("1", "2")) {
            try (var input = fileSystem.newInputFile(location, 1, CacheKey.of("catalog", version)).newInput()) {
                assertThat(input.readTail(1).getBytes()).containsExactly((byte) 1);
            }
        }
        fileSystem.deleteFile(location);
        fileSystem.deleteDirectory(Location.of("memory:///data"));
        assertThat(reads).doesNotHaveDuplicates();
        assertThat(invalidations).hasSize(2);
        for (CacheKey key : reads) {
            assertThat(key.startsWith(invalidations.get(0))).isTrue();
            assertThat(key.startsWith(invalidations.get(1))).isTrue();
        }
    }
}
