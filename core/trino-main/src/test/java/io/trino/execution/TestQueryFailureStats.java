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

import com.google.common.collect.ImmutableMap;
import io.trino.dispatcher.FailedDispatchQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executors;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static javax.management.MBeanServerFactory.newMBeanServer;
import static org.assertj.core.api.Assertions.assertThat;

class TestQueryFailureStats
{
    private static final ErrorCode STORAGE_ERROR = new ErrorCode(1234, "TEST_STORAGE_ERROR", EXTERNAL);

    @Test
    void testFailuresAreSeparatedByUserCodeAndType()
            throws Exception
    {
        MBeanServer server = newMBeanServer();
        var stats = new QueryFailureStats(new MBeanExporter(server));
        try {
            stats.recordFailure("tenant_alpha.reader", STORAGE_ERROR);
            stats.recordFailure("tenant_alpha.reader", STORAGE_ERROR);
            stats.recordFailure("tenant_beta.reader", STORAGE_ERROR);
            stats.recordFailure("tenant_alpha.reader", new ErrorCode(1235, "TEST_OTHER_ERROR", EXTERNAL));
            stats.recordFailure("tenant_alpha.reader", new ErrorCode(1234, "TEST_STORAGE_ERROR", INTERNAL_ERROR));
            stats.recordFailure("tenant_alpha.reader", new ErrorCode(1236, "TEST_RESOURCE_ERROR", INSUFFICIENT_RESOURCES));
            stats.recordFailure("tenant_alpha.reader", new ErrorCode(1237, "TEST_USER_ERROR", USER_ERROR));

            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_STORAGE_ERROR", EXTERNAL, false)).isEqualTo(2);
            assertThat(totalCount(server, "tenant_beta.reader", "TEST_STORAGE_ERROR", EXTERNAL, false)).isEqualTo(1);
            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_OTHER_ERROR", EXTERNAL, false)).isEqualTo(1);
            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_STORAGE_ERROR", INTERNAL_ERROR, false)).isEqualTo(1);
            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_RESOURCE_ERROR", INSUFFICIENT_RESOURCES, false)).isEqualTo(1);
            assertThat(server.getMBeanCount()).isEqualTo(6);
        }
        finally {
            stats.destroy();
        }
        assertThat(server.getMBeanCount()).isEqualTo(1);
        stats.recordFailure("tenant_alpha.reader", STORAGE_ERROR);
        assertThat(server.getMBeanCount()).isEqualTo(1);
    }

    @Test
    void testOverflowPreservesCountsAndTypes()
            throws Exception
    {
        MBeanServer server = newMBeanServer();
        var stats = new QueryFailureStats(new MBeanExporter(server), 1);
        try {
            stats.recordFailure("tenant_alpha.reader", STORAGE_ERROR);
            stats.recordFailure("tenant_beta.reader", STORAGE_ERROR);
            stats.recordFailure("tenant_gamma.reader", STORAGE_ERROR);
            stats.recordFailure("tenant_beta.reader", new ErrorCode(1235, "TEST_INTERNAL_ERROR", INTERNAL_ERROR));
            stats.recordFailure("tenant_beta.reader", new ErrorCode(1236, "TEST_RESOURCE_ERROR", INSUFFICIENT_RESOURCES));
            stats.recordFailure("tenant_alpha.reader", STORAGE_ERROR);

            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_STORAGE_ERROR", EXTERNAL, false)).isEqualTo(2);
            assertThat(totalCount(server, "__other__", "__other__", EXTERNAL, true)).isEqualTo(2);
            assertThat(totalCount(server, "__other__", "__other__", INTERNAL_ERROR, true)).isEqualTo(1);
            assertThat(totalCount(server, "__other__", "__other__", INSUFFICIENT_RESOURCES, true)).isEqualTo(1);
            assertThat(server.getMBeanCount()).isEqualTo(5);
        }
        finally {
            stats.destroy();
        }
    }

    @Test
    void testObjectNameEscapesUser()
            throws Exception
    {
        MBeanServer server = newMBeanServer();
        var stats = new QueryFailureStats(new MBeanExporter(server));
        try {
            String user = "tenant_alpha,reader=\"*?\\\n";
            stats.recordFailure(user, STORAGE_ERROR);
            assertThat(totalCount(server, user, "TEST_STORAGE_ERROR", EXTERNAL, false)).isEqualTo(1);
        }
        finally {
            stats.destroy();
        }
    }

    @Test
    void testConcurrentFailureCounts()
            throws Exception
    {
        MBeanServer server = newMBeanServer();
        var stats = new QueryFailureStats(new MBeanExporter(server));
        try (var executor = Executors.newFixedThreadPool(4)) {
            for (int index = 0; index < 1000; index++) {
                executor.submit(() -> stats.recordFailure("tenant_alpha.reader", STORAGE_ERROR));
            }
        }
        try {
            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_STORAGE_ERROR", EXTERNAL, false)).isEqualTo(1000);
        }
        finally {
            stats.destroy();
        }
    }

    @Test
    void testQueryCompletionCountsOnce()
            throws Exception
    {
        MBeanServer server = newMBeanServer();
        var failureStats = new QueryFailureStats(new MBeanExporter(server));
        var stats = new QueryManagerStats(failureStats);
        var identity = Identity.ofUser("tenant_alpha.reader");
        var session = testSessionBuilder().setIdentity(identity).setOriginalIdentity(identity).build();
        var query = new FailedDispatchQuery(
                session,
                "SELECT 1",
                Optional.empty(),
                URI.create("http://localhost/query"),
                Optional.empty(),
                new TrinoException(() -> STORAGE_ERROR, "Synthetic storage failure"),
                Runnable::run,
                new NodeVersion("test"))
        {
            @Override
            public void addStateChangeListener(StateChangeListener<QueryState> listener)
            {
                listener.stateChanged(QueryState.FAILED);
                listener.stateChanged(QueryState.FAILED);
            }
        };
        try {
            stats.trackQueryStats(query);
            assertThat(stats.getCompletedQueries().getTotalCount()).isEqualTo(1);
            assertThat(stats.getExternalFailures().getTotalCount()).isEqualTo(1);
            assertThat(totalCount(server, "tenant_alpha.reader", "TEST_STORAGE_ERROR", EXTERNAL, false)).isEqualTo(1);
        }
        finally {
            failureStats.destroy();
        }
    }

    @Test
    void testExportFailureDoesNotInterruptQueryAccounting()
    {
        MBeanServer server = newMBeanServer();
        var exporter = new MBeanExporter(server);
        exporter.exportWithGeneratedName(new QueryFailureStats.FailureCounter(), QueryFailureStats.class, ImmutableMap.of(
                "name", "QueryFailureStats",
                "user", "tenant_alpha.reader",
                "errorCode", "TEST_STORAGE_ERROR",
                "errorType", "EXTERNAL",
                "overflow", "false"));
        var failureStats = new QueryFailureStats(exporter);
        var stats = new QueryManagerStats(failureStats);
        var identity = Identity.ofUser("tenant_alpha.reader");
        var query = new FailedDispatchQuery(
                testSessionBuilder().setIdentity(identity).setOriginalIdentity(identity).build(),
                "SELECT 1",
                Optional.empty(),
                URI.create("http://localhost/query"),
                Optional.empty(),
                new TrinoException(() -> STORAGE_ERROR, "Synthetic storage failure"),
                Runnable::run,
                new NodeVersion("test"));
        try {
            stats.trackQueryStats(query);
            assertThat(stats.getCompletedQueries().getTotalCount()).isEqualTo(1);
            assertThat(stats.getExternalFailures().getTotalCount()).isEqualTo(1);
        }
        finally {
            failureStats.destroy();
            exporter.destroy();
        }
    }

    private static long totalCount(MBeanServer server, String user, String errorCode, ErrorType errorType, boolean overflow)
            throws Exception
    {
        String name = ObjectNames.builder(QueryFailureStats.class).withProperties(ImmutableMap.of(
                "user", user,
                "errorCode", errorCode,
                "errorType", errorType.name(),
                "overflow", Boolean.toString(overflow))).build();
        return (long) server.getAttribute(new ObjectName(name), "TotalCount");
    }
}
