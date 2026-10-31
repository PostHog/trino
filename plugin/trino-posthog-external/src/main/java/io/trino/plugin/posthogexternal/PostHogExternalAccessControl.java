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
package io.trino.plugin.posthogexternal;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.security.ReadOnlyAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;

import java.util.Map;
import java.util.Set;

import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;

/**
 * Read-only access, plus the catalog operations that change only registrations: create and drop schema,
 * drop table, and the register and unregister procedures. Data writes, ALTER, ANALYZE and table procedures stay denied.
 * <p>
 * The create table check is allowed because {@code register_table} runs it.
 * {@link ReadOnlyConnectorMetadata} refuses CREATE TABLE and CTAS themselves.
 */
public class PostHogExternalAccessControl
        extends ReadOnlyAccessControl
{
    static final Set<SchemaRoutineName> ALLOWED_PROCEDURES = ImmutableSet.of(
            new SchemaRoutineName("system", "register_table"),
            new SchemaRoutineName("system", "unregister_table"));

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName, Map<String, Object> properties) {}

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName) {}

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties) {}

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName) {}

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
        if (!ALLOWED_PROCEDURES.contains(procedure)) {
            denyExecuteProcedure(procedure.toString());
        }
    }
}
