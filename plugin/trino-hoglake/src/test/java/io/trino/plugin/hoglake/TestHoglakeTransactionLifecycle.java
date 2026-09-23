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

import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeTransactionLifecycle
{
    @Test
    void testCommitReleasesStateForSuccessConflictAndUnknownOutcome()
    {
        for (int outcome = 0; outcome < 3; outcome++) {
            RuntimeException failure = switch (outcome) {
                case 0 -> null;
                case 1 -> new TrinoException(TRANSACTION_CONFLICT, "conflict");
                default -> new TrinoException(HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE, "outcome is unknown");
            };
            AtomicInteger rollbacks = new AtomicInteger();
            try (HoglakeClient client = new HoglakeClient("http://localhost:1", "unused")) {
                HoglakeMetadata metadata = new HoglakeMetadata(client)
                {
                    @Override
                    public HoglakeMetadata newTransaction(boolean autoCommit)
                    {
                        return this;
                    }

                    @Override
                    public void commit()
                    {
                        if (failure != null) {
                            throw failure;
                        }
                    }

                    @Override
                    public void rollback()
                    {
                        rollbacks.incrementAndGet();
                    }
                };
                var storage = new MemoryFileSystemFactory();
                var connector = new HoglakeConnector(
                        metadata,
                        new HoglakeSplitManager(client),
                        new HoglakePageSourceProvider(storage),
                        new HoglakePageSinkProvider(storage, "test"),
                        new Bootstrap().quiet().initialize().getInstance(LifeCycleManager.class),
                        Set.of());
                try {
                    var handle = connector.beginTransaction(REPEATABLE_READ, false, false);
                    assertThat(connector.getMetadata(ConnectorTestFixtures.session(), handle)).isSameAs(metadata);
                    if (failure == null) {
                        connector.commit(handle);
                    }
                    else {
                        assertThatThrownBy(() -> connector.commit(handle)).isSameAs(failure);
                    }
                    assertThatThrownBy(() -> connector.getMetadata(ConnectorTestFixtures.session(), handle))
                            .isInstanceOf(NullPointerException.class).hasMessage("Unknown transaction");
                    connector.rollback(handle);
                    assertThat(rollbacks.get()).isZero();
                }
                finally {
                    connector.shutdown();
                }
            }
        }
    }
}
