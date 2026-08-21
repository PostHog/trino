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
package io.trino.plugin.catalogstore.posthog;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;

import java.util.Map;

public final class CatalogVersions
{
    private CatalogVersions() {}

    /**
     * Computes the version of a catalog as a deterministic hash of its name, connector name and
     * properties, so that every coordinator in a cell derives the same version for the same
     * catalog.
     * <p>
     * This mirrors the scheme of {@code io.trino.connector.FileCatalogStore#computeCatalogVersion},
     * which is package private in the engine and therefore cannot be reused from a plugin. Like
     * that method, this is not a generic, universal or stable version computation: it only has to
     * agree with itself across coordinators and restarts of the same Trino version.
     */
    public static CatalogVersion computeCatalogVersion(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putUnencodedChars("catalog-hash");
        hashLengthPrefixedString(hasher, catalogName.toString());
        hashLengthPrefixedString(hasher, connectorName.toString());
        hasher.putInt(properties.size());
        ImmutableSortedMap.copyOf(properties).forEach((key, value) -> {
            hashLengthPrefixedString(hasher, key);
            hashLengthPrefixedString(hasher, value);
        });
        return new CatalogVersion(hasher.hash().toString());
    }

    private static void hashLengthPrefixedString(Hasher hasher, String value)
    {
        hasher.putInt(value.length());
        hasher.putUnencodedChars(value);
    }
}
