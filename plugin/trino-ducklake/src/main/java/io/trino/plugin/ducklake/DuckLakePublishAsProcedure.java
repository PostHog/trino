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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.TableProcedureMetadata;

import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

/**
 * Publishes a table under another name of its schema, replacing the table holding that name, in
 * one snapshot: {@code ALTER TABLE staging EXECUTE publish_as(target => 'live')}.
 */
public final class DuckLakePublishAsProcedure
{
    public static final String NAME = "PUBLISH_AS";

    private DuckLakePublishAsProcedure() {}

    public static TableProcedureMetadata metadata()
    {
        return new TableProcedureMetadata(
                NAME,
                coordinatorOnly(),
                ImmutableList.of(stringProperty("target", "Unqualified name to publish the table as, replacing the table holding it", null, false)));
    }
}
