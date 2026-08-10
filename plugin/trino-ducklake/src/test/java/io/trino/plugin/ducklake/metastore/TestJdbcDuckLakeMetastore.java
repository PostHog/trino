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

import java.sql.SQLException;

import static io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore.isUndefinedTable;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJdbcDuckLakeMetastore
{
    @Test
    void testIsUndefinedTable()
    {
        assertThat(isUndefinedTable(new SQLException("relation does not exist", "42P01"))).isTrue();
        assertThat(isUndefinedTable(new RuntimeException(new SQLException("relation does not exist", "42P01")))).isTrue();
        assertThat(isUndefinedTable(new RuntimeException(new RuntimeException(new SQLException("nested", "42P01"))))).isTrue();

        assertThat(isUndefinedTable(new SQLException("syntax error", "42601"))).isFalse();
        assertThat(isUndefinedTable(new SQLException("no sql state", (String) null))).isFalse();
        assertThat(isUndefinedTable(new RuntimeException("no sql exception in the cause chain"))).isFalse();
        assertThat(isUndefinedTable(new RuntimeException(new SQLException("wrong state", "08006")))).isFalse();
    }
}
