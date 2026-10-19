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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.connector.CatalogStoreManager;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.connector.InMemoryCatalogStore;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.catalog.DelegatedCatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestDelegatedCatalogTasks
{
    private static final String CATALOG = "ext_tenant__warehouse";
    private static final List<Property> PROPERTIES = ImmutableList.of(
            new Property(new NodeLocation(1, 1), new Identifier("tpch.partitioning-enabled"), new StringLiteral("false")),
            new Property(new NodeLocation(1, 1), new Identifier("tpch.splits-per-node"), new StringLiteral("4")));

    private QueryRunner queryRunner;
    private FakeDelegatedCatalogStore store;
    private CoordinatorDynamicCatalogManager catalogManager;
    private CreateCatalogTask createTask;
    private DropCatalogTask dropTask;
    private ScheduledExecutorService publisher;

    @BeforeEach
    public void setUp()
    {
        store = new FakeDelegatedCatalogStore();
        queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new DelegatedCatalogStoreModule(store))
                        .addProperty("catalog.store", "delegated"));
        queryRunner.installPlugin(new TpchPlugin());
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(new Key<>() {});
        createTask = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        dropTask = (DropCatalogTask) tasks.get(DropCatalog.class);
        catalogManager = queryRunner.getCoordinator().getInstance(Key.get(CoordinatorDynamicCatalogManager.class));
        publisher = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    public void tearDown()
    {
        publisher.shutdownNow();
        if (queryRunner != null) {
            queryRunner.close();
        }
        queryRunner = null;
    }

    @Test
    public void testCreateIsDelegatedWithIdentityAndWaitsForPublication()
    {
        store.onCreate = request -> publisher.schedule(() -> publish(request.catalogName()), 1200, MILLISECONDS);

        QueryStateMachine stateMachine = stateMachine();
        ListenableFuture<Void> future = createTask.execute(createCatalog(false), stateMachine, emptyList(), WarningCollector.NOOP);
        // Accepted, but not here yet
        assertThat(future.isDone()).isFalse();
        getFutureValue(future);

        assertThat(store.requests).hasSize(1);
        Request request = store.requests.getFirst();
        assertThat(request.operation()).isEqualTo("create");
        assertThat(request.identity().getUser()).isEqualTo(stateMachine.getSession().getIdentity().getUser());
        assertThat(request.catalogName()).isEqualTo(new CatalogName(CATALOG));
        assertThat(request.connectorName()).isEqualTo(new ConnectorName("tpch"));
        // The delegate receives the real values, only the recorded query text is masked
        assertThat(request.properties()).isEqualTo(ImmutableMap.of("tpch.partitioning-enabled", "false", "tpch.splits-per-node", "4"));
        assertThat(request.flag()).isFalse();
        assertThat(catalogManager.getCatalog(new CatalogName(CATALOG))).isPresent();
        // Nothing went through the store's own mutation path
        assertThat(store.delegate.getCatalogs()).isEmpty();
    }

    @Test
    public void testCreateIfNotExistsShortCircuitsWhenActive()
    {
        publish(new CatalogName(CATALOG));

        QueryStateMachine stateMachine = stateMachine();
        getFutureValue(createTask.execute(createCatalog(true), stateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(store.requests).isEmpty();

        // Without IF NOT EXISTS the control plane decides
        store.onCreate = _ -> {};
        getFutureValue(createTask.execute(createCatalog(false), stateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(store.requests).hasSize(1);
    }

    @Test
    public void testCreateTimesOutWithRetryableError()
    {
        store.visibilityTimeout = Duration.ofSeconds(1);
        store.onCreate = _ -> {};

        assertTrinoExceptionThrownBy(() -> getFutureValue(createTask.execute(createCatalog(true), stateMachine(), emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(CATALOG_NOT_AVAILABLE)
                .hasMessage("Catalog '%s' was accepted but is not yet available on this coordinator; retry the statement shortly", CATALOG);
        assertThat(store.requests).hasSize(1);
        assertThat(store.requests.getFirst().flag()).isTrue();
    }

    @Test
    public void testCancelledQueryStopsWaiting()
    {
        store.onCreate = _ -> {};
        QueryStateMachine stateMachine = stateMachine();
        ListenableFuture<Void> future = createTask.execute(createCatalog(false), stateMachine, emptyList(), WarningCollector.NOOP);
        assertThat(future.isDone()).isFalse();
        stateMachine.transitionToCanceled();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    public void testDelegateFailureFailsTheStatement()
    {
        store.onCreate = _ -> {
            throw new TrinoException(NOT_SUPPORTED, "rejected by the control plane");
        };
        assertTrinoExceptionThrownBy(() -> getFutureValue(createTask.execute(createCatalog(false), stateMachine(), emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("rejected by the control plane");
    }

    @Test
    public void testInvalidPropertyValueIsNotEchoed()
    {
        List<Property> properties = ImmutableList.of(new Property(new NodeLocation(1, 1), new Identifier("connection-password"), new LongLiteral(new NodeLocation(1, 1), "12345678901234")));
        CreateCatalog statement = new CreateCatalog(new NodeLocation(1, 1), new Identifier(CATALOG), false, new Identifier("tpch"), properties, Optional.empty(), Optional.empty());
        assertTrinoExceptionThrownBy(() -> getFutureValue(createTask.execute(statement, stateMachine(), emptyList(), WarningCollector.NOOP)))
                .hasMessageContaining("Invalid value for catalog property 'connection-password'")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("12345678901234"))
                .satisfies(e -> assertThat(e.getCause()).isNull());
        assertThat(store.requests).isEmpty();
    }

    @Test
    public void testDropIsDelegatedAndWaitsForRemoval()
    {
        CatalogName catalogName = new CatalogName(CATALOG);
        publish(catalogName);
        store.onDrop = request -> publisher.schedule(() -> catalogManager.removePublishedCatalog(request.catalogName()), 1200, MILLISECONDS);

        QueryStateMachine stateMachine = stateMachine();
        ListenableFuture<Void> future = dropTask.execute(new DropCatalog(new NodeLocation(1, 1), new Identifier(CATALOG), true, false), stateMachine, emptyList(), WarningCollector.NOOP);
        assertThat(future.isDone()).isFalse();
        getFutureValue(future);

        assertThat(store.requests).hasSize(1);
        Request request = store.requests.getFirst();
        assertThat(request.operation()).isEqualTo("drop");
        assertThat(request.identity().getUser()).isEqualTo(stateMachine.getSession().getIdentity().getUser());
        assertThat(request.flag()).isTrue();
        assertThat(catalogManager.getCatalog(catalogName)).isEmpty();
    }

    @Test
    public void testDropTimesOut()
    {
        publish(new CatalogName(CATALOG));
        store.visibilityTimeout = Duration.ofSeconds(1);
        assertTrinoExceptionThrownBy(() -> getFutureValue(dropTask.execute(new DropCatalog(new NodeLocation(1, 1), new Identifier(CATALOG), false, false), stateMachine(), emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(CATALOG_NOT_AVAILABLE)
                .hasMessage("Catalog '%s' was accepted for removal but is still present on this coordinator; retry the statement shortly", CATALOG);
    }

    @Test
    public void testDropOfAbsentCatalogCompletesImmediately()
    {
        getFutureValue(dropTask.execute(new DropCatalog(new NodeLocation(1, 1), new Identifier(CATALOG), true, false), stateMachine(), emptyList(), WarningCollector.NOOP));
        assertThat(store.requests).hasSize(1);
    }

    @Test
    public void testWaitKeepsPollingUntilTheCallerIsAuthorized()
    {
        CatalogName catalogName = new CatalogName(CATALOG);
        publish(catalogName);
        AtomicInteger checks = new AtomicInteger();
        // Two failing access checks, as from an unreachable policy service, then two that do not
        // yet admit the caller: none of them may end the wait
        AccessControl accessControl = new AllowAllAccessControl()
        {
            @Override
            public Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs)
            {
                int check = checks.incrementAndGet();
                if (check <= 2) {
                    throw new RuntimeException("policy service unavailable");
                }
                return check <= 4 ? ImmutableSet.of() : catalogs;
            }
        };
        DelegatedCatalogMutations mutations = new DelegatedCatalogMutations(Optional.empty(), catalogManager, accessControl, Duration.ofMillis(20));
        try {
            getFutureValue(mutations.awaitCreated(stateMachine(), catalogName, true, Duration.ofSeconds(30)));
            assertThat(checks.get()).isGreaterThanOrEqualTo(5);
        }
        finally {
            mutations.stop();
        }
    }

    @Test
    public void testActiveCatalogTheCallerCannotSeeIsNotAvailable()
    {
        CatalogName catalogName = new CatalogName(CATALOG);
        publish(catalogName);
        AccessControl hidden = new AllowAllAccessControl()
        {
            @Override
            public Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs)
            {
                return ImmutableSet.of();
            }
        };
        DelegatedCatalogMutations mutations = new DelegatedCatalogMutations(Optional.empty(), catalogManager, hidden, Duration.ofMillis(20));
        try {
            // IF NOT EXISTS must not succeed locally for another login's catalog
            assertThat(mutations.isActive(catalogName)).isTrue();
            assertThat(mutations.isAvailable(stateMachine().getSession(), catalogName)).isFalse();
        }
        finally {
            mutations.stop();
        }
    }

    private void publish(CatalogName catalogName)
    {
        catalogManager.applyPublishedCatalog(new CatalogProperties(catalogName, new CatalogVersion("v1"), new ConnectorName("tpch"), ImmutableMap.of()));
    }

    private static CreateCatalog createCatalog(boolean notExists)
    {
        return new CreateCatalog(new NodeLocation(1, 1), new Identifier(CATALOG), notExists, new Identifier("tpch"), PROPERTIES, Optional.empty(), Optional.empty());
    }

    private QueryStateMachine stateMachine()
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                queryRunner.getDefaultSession(),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl(),
                directExecutor(),
                queryRunner.getPlannerContext().getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new ExchangeMetricsCollector(ImmutableList::of, Duration.ofMillis(1)),
                Optional.empty(),
                true,
                Optional.empty(),
                new NodeVersion("test"));
    }

    private record Request(String operation, Identity identity, CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean flag) {}

    private interface RequestHandler
    {
        void handle(Request request);
    }

    private static class FakeDelegatedCatalogStore
            implements CatalogStore, DelegatedCatalogStore
    {
        private final CatalogStore delegate = new InMemoryCatalogStore();
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private volatile RequestHandler onCreate = _ -> {};
        private volatile RequestHandler onDrop = _ -> {};
        private volatile Duration visibilityTimeout = Duration.ofSeconds(30);

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            return delegate.getCatalogs();
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            throw new UnsupportedOperationException("mutations are delegated");
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            throw new UnsupportedOperationException("mutations are delegated");
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            throw new UnsupportedOperationException("mutations are delegated");
        }

        @Override
        public CatalogRequestResult createCatalog(Identity identity, CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean ifNotExists)
        {
            Request request = new Request("create", identity, catalogName, connectorName, ImmutableMap.copyOf(properties), ifNotExists);
            requests.add(request);
            onCreate.handle(request);
            return new CatalogRequestResult(catalogName, true);
        }

        @Override
        public CatalogRequestResult dropCatalog(Identity identity, CatalogName catalogName, boolean ifExists)
        {
            Request request = new Request("drop", identity, catalogName, null, ImmutableMap.of(), ifExists);
            requests.add(request);
            onDrop.handle(request);
            return new CatalogRequestResult(catalogName, true);
        }

        @Override
        public Duration visibilityTimeout()
        {
            return visibilityTimeout;
        }
    }

    private record DelegatedCatalogStoreModule(FakeDelegatedCatalogStore store)
            implements Module
    {
        private DelegatedCatalogStoreModule
        {
            requireNonNull(store, "store is null");
        }

        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        public CatalogStoreFactory createCatalogStoreFactory(CatalogStoreManager catalogStoreManager)
        {
            CatalogStoreFactory factory = new CatalogStoreFactory()
            {
                @Override
                public String getName()
                {
                    return "delegated";
                }

                @Override
                public CatalogStore create(Map<String, String> config)
                {
                    return store;
                }
            };
            catalogStoreManager.addCatalogStoreFactory(factory);
            return factory;
        }
    }
}
