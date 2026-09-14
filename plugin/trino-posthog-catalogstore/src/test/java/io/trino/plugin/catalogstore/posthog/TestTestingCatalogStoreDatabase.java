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

import org.junit.jupiter.api.Test;

import static io.trino.plugin.catalogstore.posthog.TestingCatalogStoreDatabase.validateExternalUrl;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTestingCatalogStoreDatabase
{
    @Test
    void testExternalDatabaseIsLocalAndCannotOverrideOwnedSchema()
    {
        assertThatCode(() -> validateExternalUrl("jdbc:postgresql://127.0.0.1:5432/catalogs")).doesNotThrowAnyException();
        assertThatCode(() -> validateExternalUrl("jdbc:postgresql://[::1]:5432/catalogs")).doesNotThrowAnyException();
        for (String url : new String[] {
                "jdbc:postgresql://database.example.test:5432/catalogs",
                "jdbc:postgresql://localhost/catalogs?currentSchema=public",
                "jdbc:postgresql://localhost/catalogs?user=other",
                "jdbc:postgresql://user:password@localhost/catalogs",
                "jdbc:postgresql://localhost/catalogs#fragment",
                "jdbc:postgresql://localhost/first/second",
                "jdbc:postgresql://localhost/catalogs?password=invalid value",
                "jdbc:other://localhost/catalogs",
        }) {
            assertThatThrownBy(() -> validateExternalUrl(url))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageNotContaining(url);
        }
    }
}
