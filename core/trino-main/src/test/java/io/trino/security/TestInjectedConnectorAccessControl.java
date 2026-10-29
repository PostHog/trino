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
package io.trino.security;

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInjectedConnectorAccessControl
{
    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, InjectedConnectorAccessControl.class);
    }

    @Test
    public void testRenameTableChecksTheNewName()
    {
        List<String> checkedRenames = new ArrayList<>();
        AccessControl accessControl = new AllowAllAccessControl()
        {
            @Override
            public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
            {
                checkedRenames.add(tableName + " -> " + newTableName);
            }
        };
        SecurityContext securityContext = new SecurityContext(TransactionId.create(), Identity.ofUser("user"), QueryId.valueOf("query"), Instant.now());
        ConnectorAccessControl connectorAccessControl = new InjectedConnectorAccessControl(accessControl, securityContext, "example_catalog");

        connectorAccessControl.checkCanRenameTable(null, new SchemaTableName("example_schema", "source"), new SchemaTableName("example_schema", "target"));

        assertThat(checkedRenames).containsExactly("example_catalog.example_schema.source -> example_catalog.example_schema.target");
    }
}
