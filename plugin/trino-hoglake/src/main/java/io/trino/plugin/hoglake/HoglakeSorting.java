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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

final class HoglakeSorting
{
    private static final Pattern EXPRESSION = Pattern.compile("(?i)([a-z_][a-z_0-9]*(?:\\.[a-z_][a-z_0-9]*)*)(?:\\s+(ASC|DESC))?(?:\\s+NULLS\\s+(FIRST|LAST))?");

    private HoglakeSorting() {}

    static List<HoglakeDtos.SortField> read(Map<String, Object> spec)
    {
        if (spec == null) {
            return List.of();
        }
        if (!(spec.get("fields") instanceof List<?> fields)) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Hoglake sort spec");
        }
        ObjectMapper mapper = new ObjectMapper();
        return fields.stream().map(field -> mapper.convertValue(field, HoglakeDtos.SortField.class)).toList();
    }

    static List<HoglakeDtos.SortField> parse(List<String> expressions, List<HoglakeColumnHandle> columns)
    {
        List<HoglakeDtos.SortField> fields = new ArrayList<>();
        for (String expression : expressions) {
            var matcher = EXPRESSION.matcher(expression.trim());
            if (!matcher.matches()) {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported sort expression: " + expression);
            }
            var source = HoglakePartitioning.parse(List.of(matcher.group(1)), columns).getFirst();
            fields.add(new HoglakeDtos.SortField(
                    source.sourceFieldId(),
                    matcher.group(2) == null ? "asc" : matcher.group(2).toLowerCase(Locale.ROOT),
                    matcher.group(3) == null ? "nulls_last" : "nulls_" + matcher.group(3).toLowerCase(Locale.ROOT)));
        }
        validate(fields, columns);
        return List.copyOf(fields);
    }

    static void validate(List<HoglakeDtos.SortField> fields, List<HoglakeColumnHandle> columns)
    {
        HashSet<Long> seen = new HashSet<>();
        for (var field : fields) {
            var chain = HoglakePartitioning.find(columns, field.sourceFieldId());
            if (chain.isEmpty() || !chain.getLast().children().isEmpty() || !chain.getLast().type().isOrderable() || chain.getLast().hoglakeType().equals("variant")) {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported Hoglake sort source: " + field.sourceFieldId());
            }
            if (!seen.add(field.sourceFieldId())) {
                throw new TrinoException(NOT_SUPPORTED, "Duplicate Hoglake sort source: " + field.sourceFieldId());
            }
            order(field);
        }
    }

    static List<String> expressions(List<HoglakeDtos.SortField> fields, List<HoglakeColumnHandle> columns)
    {
        return fields.stream().map(field -> {
            var chain = HoglakePartitioning.find(columns, field.sourceFieldId());
            if (chain.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Unknown sort source: " + field.sourceFieldId());
            }
            return String.join(".", chain.stream().map(HoglakeColumnHandle::name).toList()) + " " + field.direction().toUpperCase(Locale.ROOT) +
                    " " + field.nullOrder().replace('_', ' ').toUpperCase(Locale.ROOT);
        }).toList();
    }

    private static SortOrder order(HoglakeDtos.SortField field)
    {
        if ((!"asc".equals(field.direction()) && !"desc".equals(field.direction())) ||
                (!"nulls_first".equals(field.nullOrder()) && !"nulls_last".equals(field.nullOrder()))) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Hoglake sort direction or null order");
        }
        return SortOrder.valueOf((field.direction() + "_" + field.nullOrder()).toUpperCase(Locale.ROOT));
    }

    static Iterator<Page> sort(PageSorter sorter, List<Page> pages, List<HoglakeColumnHandle> columns, List<HoglakeDtos.SortField> fields)
    {
        List<List<HoglakeColumnHandle>> chains = fields.stream().map(field -> HoglakePartitioning.find(columns, field.sourceFieldId())).toList();
        List<Type> types = new ArrayList<>(columns.stream().map(HoglakeColumnHandle::type).toList());
        chains.forEach(chain -> types.add(chain.getLast().type()));
        List<Page> augmented = new ArrayList<>();
        int positions = 0;
        for (Page page : pages) {
            Block[] blocks = new Block[types.size()];
            for (int channel = 0; channel < columns.size(); channel++) {
                blocks[channel] = page.getBlock(channel);
            }
            for (int key = 0; key < chains.size(); key++) {
                var chain = chains.get(key);
                Type type = chain.getLast().type();
                var values = type.createBlockBuilder(null, page.getPositionCount());
                for (int position = 0; position < page.getPositionCount(); position++) {
                    Block block = page.getBlock(columns.indexOf(chain.getFirst()));
                    int offset = position;
                    for (int depth = 1; depth < chain.size() && !block.isNull(offset); depth++) {
                        var parent = chain.get(depth - 1);
                        var row = ((RowType) parent.type()).getObject(block, offset);
                        block = row.getRawFieldBlock(parent.children().indexOf(chain.get(depth)));
                        offset = row.getRawIndex();
                    }
                    if (block.isNull(offset)) {
                        values.appendNull();
                    }
                    else {
                        values.appendBlockRange(block, offset, 1);
                    }
                }
                blocks[columns.size() + key] = values.build();
            }
            augmented.add(new Page(page.getPositionCount(), blocks));
            positions = Math.addExact(positions, page.getPositionCount());
        }
        return sorter.sort(
                types,
                augmented,
                IntStream.range(columns.size(), types.size()).boxed().toList(),
                fields.stream().map(HoglakeSorting::order).toList(),
                positions);
    }
}
