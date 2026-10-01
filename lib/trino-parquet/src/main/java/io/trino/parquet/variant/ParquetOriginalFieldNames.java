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
package io.trino.parquet.variant;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.format.SchemaElement;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// The field names of a Parquet schema, as the file writes them.
///
/// The schema of {@link io.trino.parquet.metadata.ParquetMetadata#getFileMetaData()}
/// has lowercase field names. The field names of a shredded VARIANT object are
/// variant keys, which are case-sensitive, so a reader takes them from the Thrift
/// schema. The tree has the same structure and field order as the Parquet schema.
public final class ParquetOriginalFieldNames
{
    private final String name;
    private final String path;
    private final List<ParquetOriginalFieldNames> children;

    private ParquetOriginalFieldNames(String name, String path, List<ParquetOriginalFieldNames> children)
    {
        this.name = requireNonNull(name, "name is null");
        this.path = requireNonNull(path, "path is null");
        this.children = ImmutableList.copyOf(children);
    }

    /// Builds the tree from a Thrift schema, which lists the elements depth first.
    public static ParquetOriginalFieldNames fromSchema(List<SchemaElement> schema)
    {
        Iterator<SchemaElement> elements = schema.iterator();
        checkArgument(elements.hasNext(), "Parquet schema is empty");
        SchemaElement root = elements.next();
        // Paths start below the root group, like Parquet column paths
        ImmutableList.Builder<ParquetOriginalFieldNames> children = ImmutableList.builder();
        for (int child = 0; child < root.getNum_children(); child++) {
            children.add(readElement(elements, ""));
        }
        checkArgument(!elements.hasNext(), "Parquet schema has elements after the root group");
        return new ParquetOriginalFieldNames(root.getName(), root.getName(), children.build());
    }

    private static ParquetOriginalFieldNames readElement(Iterator<SchemaElement> elements, String parentPath)
    {
        checkArgument(elements.hasNext(), "Parquet schema ends before all children of a group");
        SchemaElement element = elements.next();
        String path = parentPath.isEmpty() ? element.getName() : parentPath + "." + element.getName();
        ImmutableList.Builder<ParquetOriginalFieldNames> children = ImmutableList.builder();
        for (int child = 0; child < element.getNum_children(); child++) {
            children.add(readElement(elements, path));
        }
        return new ParquetOriginalFieldNames(element.getName(), path, children.build());
    }

    public String name()
    {
        return name;
    }

    /// The dotted path of the field below the root group, in its original case.
    public String path()
    {
        return path;
    }

    public List<ParquetOriginalFieldNames> children()
    {
        return children;
    }

    @Override
    public String toString()
    {
        return path;
    }
}
