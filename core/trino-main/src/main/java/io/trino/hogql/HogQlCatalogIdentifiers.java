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
package io.trino.hogql;

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;

import java.util.Locale;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public final class HogQlCatalogIdentifiers
{
    private static final Pattern UNDELIMITED_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private HogQlCatalogIdentifiers() {}

    public static PhysicalIdentifier physicalCatalog(String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        boolean delimited = !UNDELIMITED_IDENTIFIER.matcher(catalog).matches() || !catalog.equals(catalog.toLowerCase(Locale.ENGLISH));
        return new PhysicalIdentifier(catalog, delimited);
    }
}
