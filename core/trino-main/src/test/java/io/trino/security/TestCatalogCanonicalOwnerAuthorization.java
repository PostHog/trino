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
import io.trino.NotInTransactionException;
import io.trino.Session;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.server.CatalogSyncResource;
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
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.hash.Hashing.sha256;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ROOT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCatalogCanonicalOwnerAuthorization
{
    private static final String CATALOG = "archive";

    private static final String OWNER_PROPERTY = "catalog.required-identity-group";
    private static final String OLD_OWNER = nameClaim(CATALOG, "first");
    private static final String NEW_OWNER = nameClaim(CATALOG, "second");
    private static final String SELECT_VALUE = "SELECT value FROM archive.data.records";

    @ParameterizedTest
    @MethodSource("ownershipChanges")
    public void testRecreatedOwnerCannotReadThePreviousOwnersRetainedConnector(String connector, boolean previousBound, boolean sameUsername)
    {
        String previousGroup = previousBound ? OLD_OWNER : "legacy_permission";
        AtomicReference<String> authorizedOwner = new AtomicReference<>(previousGroup);
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedOwner)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", previousBound ? OLD_OWNER : "", 101);

            Identity previousOwner = identity(previousGroup);
            Identity recreatedOwner = identity(NEW_OWNER);
            if (sameUsername) {
                assertThat(recreatedOwner.getUser()).isEqualTo(previousOwner.getUser());
                assertThat(recreatedOwner.getPrincipal()).isEqualTo(previousOwner.getPrincipal());
            }
            else {
                recreatedOwner = Identity.from(recreatedOwner).withUser("synthetic_other_reader").withPrincipal(new BasicPrincipal("synthetic_other_reader")).build();
            }
            assertThat(recreatedOwner.getExtraCredentials()).isEqualTo(previousOwner.getExtraCredentials());
            assertThat(recreatedOwner.getGroups()).doesNotContainAnyElementsOf(previousOwner.getGroups());

            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previousSession = Session.builder(TEST_SESSION).setIdentity(previousOwner).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previousSession, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
                var previousHandle = queryRunner.getTransactionManager().getCatalogHandle(transactionId, CATALOG).orElseThrow();

                publish(catalogManager, connector, "second", NEW_OWNER, 202);
                authorizedOwner.set(NEW_OWNER);
                Session recreatedSession = Session.builder(TEST_SESSION).setIdentity(recreatedOwner).build();
                assertThat(queryRunner.execute(recreatedSession, SELECT_VALUE).getOnlyValue()).isEqualTo(202L);
                assertThat(queryRunner.getTransactionManager().getCatalogHandle(transactionId, CATALOG)).contains(previousHandle);

                Session reusedTransaction = Session.builder(recreatedSession).setTransactionId(transactionId).build();
                assertTrinoExceptionThrownBy(() -> {
                    Object exposedValue = queryRunner.execute(reusedTransaction, SELECT_VALUE).getOnlyValue();
                    throw new AssertionError("A different canonical owner reads the retained connector value: " + exposedValue);
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
                .flatMap(connector -> Stream.of(true, false)
                        .flatMap(previousBound -> Stream.of(true, false)
                                .map(sameUsername -> Arguments.of(connector, previousBound, sameUsername))));
    }

    @ParameterizedTest
    @MethodSource("metadataChanges")
    public void testMetadataDiscoveryCannotAdoptAnOldInstance(String connector, boolean previousBound)
    {
        String previousGroup = previousBound ? OLD_OWNER : "legacy_permission";
        AtomicReference<String> authorizedOwner = new AtomicReference<>(previousGroup);
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedOwner)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", previousBound ? OLD_OWNER : "", 101);
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previousSession = Session.builder(TEST_SESSION).setIdentity(identity(previousGroup)).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previousSession, "SHOW CATALOGS").getOnlyColumnAsSet()).contains(CATALOG);
                // Discovery pins the mapping without starting this connector's transaction.
                assertThat(queryRunner.getTransactionManager().getActiveCatalogs(transactionId))
                        .noneMatch(catalog -> catalog.catalogName().equals(CATALOG));
                publish(catalogManager, connector, "second", NEW_OWNER, 202);
                authorizedOwner.set(NEW_OWNER);
                Session recreatedSession = Session.builder(TEST_SESSION).setIdentity(identity(NEW_OWNER)).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(recreatedSession, "SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain(CATALOG);
                assertThat(queryRunner.execute(recreatedSession, "SELECT count(*) FROM system.jdbc.columns WHERE table_cat = 'archive'").getOnlyValue()).isEqualTo(0L);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    private static Stream<Arguments> metadataChanges()
    {
        return Stream.of("iceberg", "postgresql")
                .flatMap(connector -> Stream.of(true, false).map(previousBound -> Arguments.of(connector, previousBound)));
    }

    @ParameterizedTest
    @MethodSource("legacyRemovalChanges")
    public void testRemovingABindingCannotReauthorizeARetainedUnboundInstance(String connector, boolean discoveryOnly)
    {
        AtomicReference<String> authorizedOwner = new AtomicReference<>("legacy_permission");
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedOwner)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", "", 101);
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previous = Session.builder(TEST_SESSION).setIdentity(identity("legacy_permission")).setTransactionId(transactionId).build();
                if (discoveryOnly) {
                    assertThat(queryRunner.execute(previous, "SHOW CATALOGS").getOnlyColumnAsSet()).contains(CATALOG);
                }
                else {
                    assertThat(queryRunner.execute(previous, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
                }
                publish(catalogManager, connector, "second", NEW_OWNER, 202);
                authorizedOwner.set(NEW_OWNER);
                Session recreated = Session.builder(TEST_SESSION).setIdentity(identity(NEW_OWNER)).build();
                assertThat(queryRunner.execute(recreated, SELECT_VALUE).getOnlyValue()).isEqualTo(202L);

                // No statement in the old transaction observes the bound instance.
                assertThat(catalogManager.removePublishedCatalog(new CatalogName(CATALOG))).isTrue();
                publish(catalogManager, connector, "third", "", 303);
                authorizedOwner.set("ordinary_permission");
                Session ordinary = Session.builder(TEST_SESSION).setIdentity(identity("ordinary_permission")).build();
                assertThat(queryRunner.execute(ordinary, SELECT_VALUE).getOnlyValue()).isEqualTo(303L);
                authorizedOwner.set(NEW_OWNER);

                Session retained = Session.builder(recreated).setTransactionId(transactionId).build();
                assertTrinoExceptionThrownBy(() -> {
                    Object exposedValue = queryRunner.execute(retained, SELECT_VALUE).getOnlyValue();
                    throw new AssertionError("Removing the binding exposes the previous owner's connector value: " + exposedValue);
                }).hasErrorCode(PERMISSION_DENIED);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    private static Stream<Arguments> legacyRemovalChanges()
    {
        return Stream.of("iceberg", "postgresql")
                .flatMap(connector -> Stream.of(true, false).map(discoveryOnly -> Arguments.of(connector, discoveryOnly)));
    }

    @ParameterizedTest
    @MethodSource("staleNameClaims")
    public void testAStaleExternalGrantCannotReadAFreshUnboundReplacement(String connector, String staleClaim)
    {
        AtomicReference<String> authorizedOwner = new AtomicReference<>(NEW_OWNER);
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedOwner)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "bound", NEW_OWNER, 202);
            Session owner = Session.builder(TEST_SESSION).setIdentity(identity(NEW_OWNER)).build();
            assertThat(queryRunner.execute(owner, SELECT_VALUE).getOnlyValue()).isEqualTo(202L);
            assertThat(catalogManager.removePublishedCatalog(new CatalogName(CATALOG))).isTrue();
            publish(catalogManager, connector, "ordinary", "", 303);
            authorizedOwner.set(staleClaim);
            Session stale = Session.builder(TEST_SESSION).setIdentity(identity(staleClaim)).build();
            assertTrinoExceptionThrownBy(() -> {
                Object exposedValue = queryRunner.execute(stale, SELECT_VALUE).getOnlyValue();
                throw new AssertionError("A stale external grant reads another owner's unbound connector value: " + exposedValue);
            }).hasErrorCode(PERMISSION_DENIED);
            assertThat(queryRunner.execute(stale, "SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain(CATALOG);
            assertThat(queryRunner.execute(stale, "SELECT count(*) FROM system.jdbc.columns WHERE table_cat = 'archive'").getOnlyValue()).isEqualTo(0L);
        }
    }

    private static Stream<Arguments> staleNameClaims()
    {
        return Stream.of("iceberg", "postgresql")
                .flatMap(connector -> Stream.of(OLD_OWNER, NEW_OWNER).map(owner -> Arguments.of(connector, owner)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"iceberg", "postgresql"})
    public void testUnrelatedNameClaimsDoNotBlockOrdinaryNameReuse(String connector)
    {
        String otherCatalog = "neighbor";
        String otherClaim = nameClaim(otherCatalog, "second");
        AtomicReference<String> authorizedGroup = new AtomicReference<>("ordinary_permission");
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "bound", NEW_OWNER, 202);
            catalogManager.removePublishedCatalog(new CatalogName(CATALOG));
            publish(catalogManager, connector, "ordinary", "", 303);
            catalogManager.applyPublishedCatalog(new CatalogProperties(
                    new CatalogName(otherCatalog),
                    new CatalogVersion("neighbor"),
                    new ConnectorName(connector),
                    ImmutableMap.of(OWNER_PROPERTY, otherClaim, "fixture.value", "909")));
            Identity ordinaryOwner = Identity.from(identity(otherClaim)).withAdditionalGroups(ImmutableSet.of("ordinary_permission")).build();
            Session ordinary = Session.builder(TEST_SESSION).setIdentity(ordinaryOwner).build();
            assertThat(queryRunner.execute(ordinary, "SELECT value FROM neighbor.data.records").getOnlyValue()).isEqualTo(909L);
            assertThat(queryRunner.execute(ordinary, SELECT_VALUE).getOnlyValue()).isEqualTo(303L);
            assertThat(queryRunner.execute(ordinary, "SHOW CATALOGS").getOnlyColumnAsSet()).contains(CATALOG, otherCatalog);
        }
    }

    @Test
    public void testExtraCredentialsDoNotSupplyNameClaims()
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>("ordinary_permission");
        try (StandaloneQueryRunner queryRunner = queryRunner("iceberg", authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, "iceberg", "ordinary", "", 303);
            Identity owner = Identity.from(identity("ordinary_permission"))
                    .withExtraCredentials(ImmutableMap.of("groups", NEW_OWNER, OWNER_PROPERTY, NEW_OWNER))
                    .build();
            assertThat(queryRunner.execute(Session.builder(TEST_SESSION).setIdentity(owner).build(), SELECT_VALUE).getOnlyValue()).isEqualTo(303L);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"iceberg", "postgresql"})
    public void testSameOwnerCredentialRotationKeepsRetainedTransactions(String connector)
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>("generation_first");
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", OLD_OWNER, 101);
            Identity previousCredentials = Identity.from(identity(OLD_OWNER)).withAdditionalGroups(ImmutableSet.of("generation_first")).build();
            Identity rotatedCredentials = Identity.from(identity(OLD_OWNER))
                    .withAdditionalGroups(ImmutableSet.of("generation_second"))
                    .withExtraCredentials(ImmutableMap.of("password", "synthetic-rotated-password"))
                    .build();
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previousSession = Session.builder(TEST_SESSION).setIdentity(previousCredentials).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previousSession, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
                publish(catalogManager, connector, "second", OLD_OWNER, 202);
                authorizedGroup.set("generation_second");
                Session rotatedSession = Session.builder(TEST_SESSION).setIdentity(rotatedCredentials).build();
                assertThat(queryRunner.execute(rotatedSession, SELECT_VALUE).getOnlyValue()).isEqualTo(202L);
                assertThat(queryRunner.execute(Session.builder(rotatedSession).setTransactionId(transactionId).build(), SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"iceberg", "postgresql"})
    public void testUnboundCatalogsKeepTheirExistingBehavior(String connector)
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>("ordinary_first");
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", "", 101);
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previousSession = Session.builder(TEST_SESSION).setIdentity(identity("ordinary_first")).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previousSession, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
                publish(catalogManager, connector, "second", "", 202);
                authorizedGroup.set("ordinary_second");
                Session replacement = Session.builder(TEST_SESSION).setIdentity(identity("ordinary_second")).build();
                assertThat(queryRunner.execute(replacement, SELECT_VALUE).getOnlyValue()).isEqualTo(202L);
                assertThat(queryRunner.execute(Session.builder(replacement).setTransactionId(transactionId).build(), SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"iceberg", "postgresql"})
    public void testRevocationStillOverridesTheCanonicalOwner(String connector)
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>(OLD_OWNER);
        try (StandaloneQueryRunner queryRunner = queryRunner(connector, authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, connector, "first", OLD_OWNER, 101);
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session session = Session.builder(TEST_SESSION).setIdentity(identity(OLD_OWNER)).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(session, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
                authorizedGroup.set("revoked");
                assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, SELECT_VALUE)).hasErrorCode(PERMISSION_DENIED);
            }
            finally {
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ȀȀȀ"})
    public void testPolicyOnlyNamesKeepTheirOrdinaryAuthorization(String name)
    {
        try (StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactions = queryRunner.getTransactionManager();
            TransactionId transactionId = transactions.beginTransaction(false);
            try {
                assertThat(transactions.isCatalogIdentityAuthorized(transactionId, name, ImmutableSet.of())).isTrue();
                transactions.fail(transactionId);
                assertTrinoExceptionThrownBy(() -> transactions.isCatalogIdentityAuthorized(transactionId, name, ImmutableSet.of()))
                        .hasErrorCode(TRANSACTION_ALREADY_ABORTED);
            }
            finally {
                getFutureValue(transactions.asyncAbort(transactionId));
            }
            assertThatThrownBy(() -> transactions.isCatalogIdentityAuthorized(transactionId, name, ImmutableSet.of()))
                    .isInstanceOf(NotInTransactionException.class);
        }
    }

    @Test
    public void testAnEmptyOwnerBindingFailsBeforeReplacement()
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>(OLD_OWNER);
        try (StandaloneQueryRunner queryRunner = queryRunner("iceberg", authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, "iceberg", "first", OLD_OWNER, 101);
            assertThatThrownBy(() -> publish(catalogManager, "iceberg", "second", " ", 202))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Catalog identity group is empty");
            Session session = Session.builder(TEST_SESSION).setIdentity(identity(OLD_OWNER)).build();
            assertThat(queryRunner.execute(session, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidReservedBindings")
    public void testMalformedReservedBindingsFailBeforeReplacement(String invalidBinding)
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>(OLD_OWNER);
        try (StandaloneQueryRunner queryRunner = queryRunner("iceberg", authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, "iceberg", "first", OLD_OWNER, 101);
            assertThatThrownBy(() -> publish(catalogManager, "iceberg", "invalid", invalidBinding, 202))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Catalog identity group does not match the catalog name");
            assertThat(queryRunner.execute(Session.builder(TEST_SESSION).setIdentity(identity(OLD_OWNER)).build(), SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
        }
    }

    private static Stream<String> invalidReservedBindings()
    {
        return Stream.of("external_identity", "external_identity_", "external_identity_invalid", nameClaim("neighbor", "first"), OLD_OWNER.toUpperCase(ROOT).replace("EXTERNAL_IDENTITY", "external_identity"));
    }

    @Test
    public void testOwnershipCheckFollowsThePolicyDuringAdoption()
            throws Exception
    {
        CountDownLatch policyEntered = new CountDownLatch(1);
        CountDownLatch policyReleased = new CountDownLatch(1);
        AtomicBoolean adoptionStarts = new AtomicBoolean();
        AllowAllSystemAccessControl authorization = new AllowAllSystemAccessControl()
        {
            @Override
            public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
            {
                if (catalogName.equals(CATALOG) && adoptionStarts.get()) {
                    policyEntered.countDown();
                    assertThat(awaitUninterruptibly(policyReleased, 10, SECONDS)).isTrue();
                }
                return true;
            }
        };
        try (StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION, builder -> builder.setSystemAccessControl(authorization));
                ExecutorService executor = Executors.newSingleThreadExecutor()) {
            queryRunner.installPlugin(new MockConnectorPlugin(new IdentityFixtureFactory("iceberg")));
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, "iceberg", "first", "", 101);
            TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(false);
            try {
                Session previous = Session.builder(TEST_SESSION).setIdentity(identity("ordinary_permission")).setTransactionId(transactionId).build();
                assertThat(queryRunner.execute(previous, SELECT_VALUE).getOnlyValue()).isEqualTo(101L);
                adoptionStarts.set(true);
                Session recreated = Session.builder(TEST_SESSION).setIdentity(identity("ordinary_permission")).setTransactionId(transactionId).build();
                Future<?> query = executor.submit(() -> assertTrinoExceptionThrownBy(() -> {
                    Object value = queryRunner.execute(recreated, SELECT_VALUE).getOnlyValue();
                    throw new AssertionError("The policy race exposes the old connector value: " + value);
                }).hasErrorCode(PERMISSION_DENIED));
                assertThat(policyEntered.await(10, SECONDS)).isTrue();
                publish(catalogManager, "iceberg", "second", NEW_OWNER, 202);
                policyReleased.countDown();
                query.get(20, SECONDS);
            }
            finally {
                policyReleased.countDown();
                getFutureValue(queryRunner.getTransactionManager().asyncAbort(transactionId));
            }
        }
    }

    @Test
    public void testPublicationCannotReplaceAProcessOnlyCatalog()
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>(OLD_OWNER);
        try (StandaloneQueryRunner queryRunner = queryRunner("iceberg", authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            var systemHandle = catalogManager.getCatalog(new CatalogName("system")).orElseThrow().getCatalogHandle();
            assertTrinoExceptionThrownBy(() -> catalogManager.applyPublishedCatalog(new CatalogProperties(
                    new CatalogName("system"),
                    new CatalogVersion("replacement"),
                    new ConnectorName("iceberg"),
                    ImmutableMap.of(OWNER_PROPERTY, OLD_OWNER, "fixture.value", "101"))))
                    .hasErrorCode(ALREADY_EXISTS);
            assertThat(catalogManager.getCatalog(new CatalogName("system")).orElseThrow().getCatalogHandle()).isEqualTo(systemHandle);
            assertThat(catalogManager.storeCatalogNames()).doesNotContain(new CatalogName("system"));
        }
    }

    @Test
    public void testLiveReceiptReportsTheActualBoundInstance()
    {
        AtomicReference<String> authorizedGroup = new AtomicReference<>(OLD_OWNER);
        try (StandaloneQueryRunner queryRunner = queryRunner("iceberg", authorizedGroup)) {
            CoordinatorDynamicCatalogManager catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
            publish(catalogManager, "iceberg", "first", OLD_OWNER, 101);
            publish(catalogManager, "iceberg", "second", NEW_OWNER, 202);
            CatalogSyncResource.CatalogSyncStatus status = queryRunner.getCoordinator().getInstance(Key.get(CatalogSyncResource.class)).catalogSyncStatus();
            assertThat(status.catalogIdentityVersion()).isEqualTo(1);
            assertThat(status.catalogInstances()).anySatisfy(instance -> {
                assertThat(instance.name()).isEqualTo(CATALOG);
                assertThat(instance.version()).isEqualTo("second");
                assertThat(instance.connector()).isEqualTo("iceberg");
                assertThat(instance.requiredIdentityGroup()).isEqualTo(NEW_OWNER);
                assertThat(instance.published()).isTrue();
            });
            assertThat(status.catalogInstances()).anySatisfy(instance -> {
                assertThat(instance.name()).isEqualTo("system");
                assertThat(instance.requiredIdentityGroup()).isNull();
                assertThat(instance.published()).isFalse();
            });
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
        queryRunner.installPlugin(new MockConnectorPlugin(new IdentityFixtureFactory(connector)));
        return queryRunner;
    }

    private static Identity identity(String owner)
    {
        return Identity.forUser("synthetic_reader")
                .withPrincipal(new BasicPrincipal("synthetic_reader"))
                .withGroups(ImmutableSet.of(owner))
                .withExtraCredentials(ImmutableMap.of("password", "synthetic-fixture-password"))
                .build();
    }

    private static String nameClaim(String catalog, String owner)
    {
        return "external_identity_" + sha256().hashString(catalog, UTF_8).toString().substring(0, 32) +
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

    private static class IdentityFixtureFactory
            implements ConnectorFactory
    {
        private final String name;

        public IdentityFixtureFactory(String name)
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
