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
import io.trino.connector.TestingTableFunctions.ConstantFunction.ConstantFunctionHandle;
import io.trino.connector.TestingTableFunctions.ConstantFunction.ConstantFunctionProcessorProvider;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.hash.Hashing.sha256;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.TestingTableFunctions.ConstantFunction.getConstantFunctionSplitSource;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCatalogIdentityTableFunctions
{
    private static final String CATALOG = "archive";
    private static final String OWNER_PROPERTY = "catalog.required-identity-group";
    private static final String OLD_OWNER = nameClaim("first");
    private static final String NEW_OWNER = nameClaim("second");
    private static final String SELECT_VALUE = "SELECT value FROM TABLE(archive.data.catalog_value())";

    @ParameterizedTest
    @MethodSource("ownershipChanges")
    public void testNewOwnerCannotReadRetainedTableFunctionValues(String connector, boolean previousBound)
    {
        String previousGroup = previousBound ? OLD_OWNER : "legacy_permission";
        AtomicReference<String> authorizedGroup = new AtomicReference<>(previousGroup);
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", previousBound ? OLD_OWNER : "", 101);
            Identity previousOwner = identity(previousGroup);
            Identity newOwner = identity(NEW_OWNER);
            assertThat(newOwner.getUser()).isEqualTo(previousOwner.getUser());
            assertThat(newOwner.getPrincipal()).isEqualTo(previousOwner.getPrincipal());
            assertThat(newOwner.getGroups()).doesNotContainAnyElementsOf(previousOwner.getGroups());

            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previousSession = Session.builder(TEST_SESSION).setIdentity(previousOwner).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previousSession, SELECT_VALUE).getOnlyValue()).isEqualTo(101);
                var previousHandle = queryRunner.getTransactionManager().getCatalogHandle(transactionId, CATALOG).orElseThrow();

                publish(catalogManager, connector, "second", NEW_OWNER, 202);
                authorizedGroup.set(NEW_OWNER);
                Session replacementSession = Session.builder(TEST_SESSION).setIdentity(newOwner).build();
                assertThat(queryRunner.execute(replacementSession, SELECT_VALUE).getOnlyValue()).isEqualTo(202);
                assertThat(queryRunner.getTransactionManager().getCatalogHandle(transactionId, CATALOG)).contains(previousHandle);

                Session retainedSession = Session.builder(replacementSession).setTransactionId(transactionId).build();
                assertTrinoExceptionThrownBy(() -> {
                    Object exposedValue = queryRunner.execute(retainedSession, SELECT_VALUE).getOnlyValue();
                    throw new AssertionError("A different canonical owner reads the retained table-function value: " + exposedValue);
                }).hasErrorCode(PERMISSION_DENIED);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    private static Stream<Arguments> ownershipChanges()
    {
        return Stream.of("iceberg", "postgresql")
                .flatMap(connector -> Stream.of(true, false).map(previousBound -> Arguments.of(connector, previousBound)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"iceberg", "postgresql"})
    public void testSameOwnerKeepsRetainedTableFunctionValues(String connector)
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>("generation_first");
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", OLD_OWNER, 101);
            Identity previousIdentity = Identity.from(identity(OLD_OWNER)).withAdditionalGroups(ImmutableSet.of("generation_first")).build();
            Identity rotatedIdentity = Identity.from(identity(OLD_OWNER)).withAdditionalGroups(ImmutableSet.of("generation_second")).build();
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previousSession = Session.builder(TEST_SESSION).setIdentity(previousIdentity).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previousSession, SELECT_VALUE).getOnlyValue()).isEqualTo(101);
                var previousHandle = queryRunner.getTransactionManager().getCatalogHandle(transactionId, CATALOG).orElseThrow();

                publish(catalogManager, connector, "second", OLD_OWNER, 202);
                authorizedGroup.set("generation_second");
                Session rotatedSession = Session.builder(TEST_SESSION).setIdentity(rotatedIdentity).build();
                assertThat(queryRunner.execute(rotatedSession, SELECT_VALUE).getOnlyValue()).isEqualTo(202);
                assertThat(queryRunner.getTransactionManager().getCatalogHandle(transactionId, CATALOG)).contains(previousHandle);
                assertThat(queryRunner.execute(Session.builder(rotatedSession).setTransactionId(transactionId).build(), SELECT_VALUE).getOnlyValue()).isEqualTo(101);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    private static StandaloneQueryRunner queryRunner(String connector, AtomicReference<String> authorizedGroup)
    {
        AllowAllSystemAccessControl authorization = new AllowAllSystemAccessControl()
        {
            @Override
            public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
            {
                return !catalogName.equals(CATALOG) || context.getIdentity().getGroups().contains(authorizedGroup.get());
            }
        };
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION, builder -> builder.setSystemAccessControl(authorization));
        queryRunner.installPlugin(new MockConnectorPlugin(new TableFunctionFixtureFactory(connector)));
        return queryRunner;
    }

    private static Identity identity(String group)
    {
        return Identity.forUser("synthetic_reader")
                .withPrincipal(new BasicPrincipal("synthetic_reader"))
                .withGroups(ImmutableSet.of(group))
                .build();
    }

    private static String nameClaim(String owner)
    {
        return "external_identity_" + sha256().hashString(CATALOG, UTF_8).toString().substring(0, 32) +
                "_" + sha256().hashString(owner, UTF_8).toString().substring(0, 32);
    }

    private static void publish(CoordinatorDynamicCatalogManager catalogManager, String connector, String version, String owner, long value)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder().put("fixture.value", Long.toString(value));
        if (!owner.isEmpty()) {
            properties.put(OWNER_PROPERTY, owner);
        }
        catalogManager.applyPublishedCatalog(new CatalogProperties(
                new CatalogName(CATALOG),
                new CatalogVersion(version),
                new ConnectorName(connector),
                properties.buildOrThrow()));
    }

    private static class TableFunctionFixtureFactory
            implements ConnectorFactory
    {
        private final String name;

        public TableFunctionFixtureFactory(String name)
        {
            this.name = name;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            assertThat(config).doesNotContainKey(OWNER_PROPERTY);
            long value = Long.parseLong(config.get("fixture.value"));
            return MockConnectorFactory.builder()
                    .withName(name)
                    .withTableFunctions(ImmutableSet.of(new CatalogValueFunction(value)))
                    .withFunctionProvider(Optional.of(new FunctionProvider()
                    {
                        @Override
                        public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
                        {
                            assertThat(functionHandle).isInstanceOf(ConstantFunctionHandle.class);
                            return new ConstantFunctionProcessorProvider();
                        }
                    }))
                    .withTableFunctionSplitSources(handle -> getConstantFunctionSplitSource((ConstantFunctionHandle) handle))
                    .build()
                    .create(catalogName, config, context);
        }
    }

    private static class CatalogValueFunction
            extends AbstractConnectorTableFunction
    {
        private final long value;

        public CatalogValueFunction(long value)
        {
            super("data",
                    "catalog_value",
                    ImmutableList.of(),
                    new DescribedTable(Descriptor.descriptor(ImmutableList.of("value"), ImmutableList.of(INTEGER))),
                    "Returns the synthetic catalog instance value");
            this.value = value;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new ConstantFunctionHandle(value, 1))
                    .build();
        }
    }
}
