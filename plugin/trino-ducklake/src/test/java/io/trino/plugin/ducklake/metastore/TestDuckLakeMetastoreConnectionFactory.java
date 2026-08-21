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
package io.trino.plugin.ducklake.metastore;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.ducklake.metastore.DuckLakeMetastoreConnectionFactory.trimTrailingNewlines;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckLakeMetastoreConnectionFactory
{
    @Test
    void testTrimTrailingNewlines()
    {
        assertThat(trimTrailingNewlines("secret")).isEqualTo("secret");
        assertThat(trimTrailingNewlines("secret\n")).isEqualTo("secret");
        assertThat(trimTrailingNewlines("secret\r\n")).isEqualTo("secret");
        assertThat(trimTrailingNewlines("secret\n\n")).isEqualTo("secret");
        assertThat(trimTrailingNewlines("")).isEmpty();
        assertThat(trimTrailingNewlines("\n")).isEmpty();

        // only line terminators are removed, every other character is part of the password
        assertThat(trimTrailingNewlines("secret ")).isEqualTo("secret ");
        assertThat(trimTrailingNewlines("secret \n")).isEqualTo("secret ");
        assertThat(trimTrailingNewlines("\nsecret")).isEqualTo("\nsecret");
        assertThat(trimTrailingNewlines("sec\nret\n")).isEqualTo("sec\nret");
    }
}
