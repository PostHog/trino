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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.hash.Hashing.sha256;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A catalog's identity member group lets a group its users already hold satisfy the catalog's
 * identity binding, so a new catalog is usable before its identity group reaches every
 * coordinator's group provider. It must never widen access beyond the instance that names it.
 */
public class TestCatalogIdentityMemberGroup
{
    private static final String CATALOG = "archive";
    private static final String OWNER_PROPERTY = "catalog.required-identity-group";
    private static final String MEMBER_PROPERTY = "catalog.identity-member-group";
    private static final String FIRST_OWNER = nameClaim(CATALOG, "first");
    private static final String SECOND_OWNER = nameClaim(CATALOG, "second");
    private static final String SELECT_VALUE = "SELECT value FROM archive.data.records";

    @Test
    public void testTheMemberGroupSatisfiesTheBinding()
    {
        try (StandaloneQueryRunner queryRunner = queryRunner()) {
            CoordinatorDynamicCatalogManager catalogManager = catalogManager(queryRunner);
            publish(catalogManager, "first", FIRST_OWNER, "org_first", 101);

            // A session that holds the warehouse group but not yet the identity group
            Session member = session(ImmutableSet.of("org_first"));
            assertThat(queryRunner.execute(member, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
            // The identity group alone still works
            assertThat(queryRunner.execute(session(ImmutableSet.of(FIRST_OWNER)), SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
        }
    }

    @Test
    public void testWithoutAMemberGroupTheIdentityGroupIsRequired()
    {
        try (StandaloneQueryRunner queryRunner = queryRunner()) {
            publish(catalogManager(queryRunner), "first", FIRST_OWNER, "", 101);
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(session(ImmutableSet.of("org_first")), SELECT_VALUE))
                    .hasErrorCode(PERMISSION_DENIED);
        }
    }

    @Test
    public void testAnotherGroupDoesNotSatisfyTheBinding()
    {
        try (StandaloneQueryRunner queryRunner = queryRunner()) {
            publish(catalogManager(queryRunner), "first", FIRST_OWNER, "org_first", 101);
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(session(ImmutableSet.of("org_second")), SELECT_VALUE))
                    .hasErrorCode(PERMISSION_DENIED);
        }
    }

    @Test
    public void testAReusedNameDoesNotInheritTheRetainedInstancesMemberGroup()
    {
        try (StandaloneQueryRunner queryRunner = queryRunner()) {
            CoordinatorDynamicCatalogManager catalogManager = catalogManager(queryRunner);
            publish(catalogManager, "first", FIRST_OWNER, "org_first", 101);
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session first = Session.builder(session(ImmutableSet.of("org_first"))).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(first, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);

                // Another warehouse takes the name
                publish(catalogManager, "second", SECOND_OWNER, "org_second", 202);
                Session second = session(ImmutableSet.of("org_second"));
                assertThat(queryRunner.execute(second, SELECT_VALUE).getOnlyValue()).isEqualTo(202L);

                // Its member group never reaches the instance the first warehouse's transaction retains
                Session reused = Session.builder(second).setTransactionId(transactionId).build();
                assertTrinoExceptionThrownBy(() -> queryRunner.execute(reused, SELECT_VALUE))
                        .hasErrorCode(PERMISSION_DENIED);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    @Test
    public void testInvalidMemberGroupsFailBeforeReplacement()
    {
        try (StandaloneQueryRunner queryRunner = queryRunner()) {
            CoordinatorDynamicCatalogManager catalogManager = catalogManager(queryRunner);
            publish(catalogManager, "first", FIRST_OWNER, "org_first", 101);

            assertThatThrownBy(() -> publish(catalogManager, "unbound", "", "org_first", 202))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Catalog identity member group requires a catalog identity group");
            assertThatThrownBy(() -> publish(catalogManager, "borrowed", FIRST_OWNER, nameClaim("other", "first"), 202))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Catalog identity member group must not be an identity group");
            assertThatThrownBy(() -> publish(catalogManager, "blank", FIRST_OWNER, " ", 202))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Catalog identity member group is empty");

            assertThat(queryRunner.execute(session(ImmutableSet.of("org_first")), SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
        }
    }

    private static StandaloneQueryRunner queryRunner()
    {
        // Access control admits every requester, so the identity binding alone decides
        AllowAllSystemAccessControl authorization = new AllowAllSystemAccessControl()
        {
            @Override
            public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
            {
                return true;
            }
        };
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION, builder -> builder.setSystemAccessControl(authorization));
        queryRunner.installPlugin(new MockConnectorPlugin(new IdentityFixtureFactory()));
        return queryRunner;
    }

    private static CoordinatorDynamicCatalogManager catalogManager(StandaloneQueryRunner queryRunner)
    {
        return queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
    }

    private static Session session(Set<String> groups)
    {
        return Session.builder(TEST_SESSION)
                .setIdentity(Identity.forUser("synthetic_reader")
                        .withPrincipal(new BasicPrincipal("synthetic_reader"))
                        .withGroups(groups)
                        .build())
                .build();
    }

    private static String nameClaim(String catalog, String owner)
    {
        return "external_identity_" + sha256().hashString(catalog, UTF_8).toString().substring(0, 32) +
                "_" + sha256().hashString(owner, UTF_8).toString().substring(0, 32);
    }

    private static void publish(CoordinatorDynamicCatalogManager catalogManager, String version, String owner, String member, long value)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder().put("fixture.value", Long.toString(value));
        if (!owner.isEmpty()) {
            properties.put(OWNER_PROPERTY, owner);
        }
        if (!member.isEmpty()) {
            properties.put(MEMBER_PROPERTY, member);
        }
        catalogManager.applyPublishedCatalog(new CatalogProperties(
                new CatalogName(CATALOG),
                new CatalogVersion(version),
                new ConnectorName("postgresql"),
                properties.buildOrThrow()));
    }

    private static class IdentityFixtureFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "postgresql";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            assertThat(config).doesNotContainKeys(OWNER_PROPERTY, MEMBER_PROPERTY);
            long value = Long.parseLong(config.get("fixture.value"));
            return MockConnectorFactory.builder()
                    .withName("postgresql")
                    .withListSchemaNames(_ -> ImmutableList.of("data"))
                    .withListTables((_, _) -> ImmutableList.of("records"))
                    .withGetTableHandle((_, table) -> new MockConnectorTableHandle(table))
                    .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("value", BIGINT)))
                    .withData(_ -> ImmutableList.of(ImmutableList.of(value)))
                    .build()
                    .create(catalogName, config, context);
        }
    }
}
