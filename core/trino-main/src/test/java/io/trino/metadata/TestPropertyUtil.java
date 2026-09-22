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
package io.trino.metadata;

import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestPropertyUtil
{
    @Test
    void testMapPropertiesRenderKeysAndValuesInMatchingOrder()
    {
        PropertyMetadata<Map> property = new PropertyMetadata<>("custom", "custom map", new MapType(VARCHAR, VARCHAR, new TypeOperators()), Map.class, Map.of(), false, value -> (Map) value, value -> value);
        Map<String, String> values = new LinkedHashMap<>();
        values.put("z", "apostrophe's");
        values.put("a", "second");
        assertThat(PropertyUtil.toSqlProperties("test", INVALID_TABLE_PROPERTY, Map.of("custom", values), List.of(property)))
                .containsExactly(new Property(new Identifier("custom"), new FunctionCall(QualifiedName.of("map"), List.of(
                        new Array(List.of(new StringLiteral("z"), new StringLiteral("a"))),
                        new Array(List.of(new StringLiteral("apostrophe's"), new StringLiteral("second")))))));
        assertThat(PropertyUtil.toSqlProperties("test", INVALID_TABLE_PROPERTY, Map.of("custom", Map.of()), List.of(property)))
                .containsExactly(new Property(new Identifier("custom"), new FunctionCall(QualifiedName.of("map"), List.of(new Array(List.of()), new Array(List.of())))));
    }
}
