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
import com.google.common.io.Closer;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.trino.testing.QueryRunner;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

/**
 * Measures how long {@code add_files} takes to register many small Parquet files, and how the time
 * splits between reading the files and committing them to the catalog.
 * <p>
 * Every round registers fresh copies of one Parquet file into a new table. It then registers the
 * same files again under another import, which reads every file and fails at the start of the
 * commit because the files are already registered: that second call measures the reading, and the
 * commit takes the rest of the first. The catalog database is the PostgreSQL container the tests
 * use, and the files are on the local file system.
 * <p>
 * Run it with {@code -Dfiles=1000 -Dcolumns=28 -Drows=200 -Drounds=5}. A database next to the
 * benchmark answers much sooner than one across a network, and the commit spends most of its time
 * waiting for answers: {@code -Dround-trip=400us} sends the connector's connections to the catalog
 * database through a proxy that delays them by that much. A system property naming a connector
 * property, such as {@code -Dducklake.commit.max-retries=3}, is passed to the catalog.
 */
public final class BenchmarkDuckLakeAddFiles
{
    private BenchmarkDuckLakeAddFiles() {}

    public static void main(String[] args)
            throws Exception
    {
        int files = Integer.getInteger("files", 1000);
        int columns = Integer.getInteger("columns", 28);
        int rows = Integer.getInteger("rows", 200);
        int rounds = Integer.getInteger("rounds", 5);
        // logging takes over the standard output, which the results are printed to
        PrintStream out = System.out;
        Logging.initialize().setLevel("io.trino", Level.WARN);

        Duration roundTrip = Duration.valueOf(System.getProperty("round-trip", "0s"));
        try (Closer closer = Closer.create()) {
            TestingDuckLakeCatalog catalog = closer.register(new TestingDuckLakeCatalog());
            catalog.executeInDuckDb("CREATE TABLE _bootstrap (x INTEGER)", "DROP TABLE _bootstrap");
            String connectionUrl = catalog.jdbcUrl();
            if (roundTrip.roundTo(NANOSECONDS) > 0) {
                connectionUrl = closer.register(new DelayingProxy(catalog.jdbcUrl(), roundTrip)).jdbcUrl();
            }
            out.printf("catalog database round trip: %.0f us%n", measureRoundTrip(connectionUrl) / 1e3);
            DuckLakeQueryRunner.Builder builder = DuckLakeQueryRunner.builder(catalog)
                    .addConnectorProperty("ducklake.metadata.connection-url", connectionUrl)
                    .addConnectorProperty("ducklake.add-files-procedure.enabled", "true");
            System.getProperties().stringPropertyNames().stream()
                    .filter(name -> name.startsWith("ducklake."))
                    .forEach(name -> builder.addConnectorProperty(name, System.getProperty(name)));
            try (QueryRunner queryRunner = builder.build()) {
                Path template = catalog.dataPath().resolve("template.parquet");
                catalog.executeInDuckDb("COPY (%s) TO '%s' (FORMAT PARQUET)".formatted(templateQuery(columns, rows), template));
                out.printf("%s files of %s bytes, %s columns, %s rows each%n", files, Files.size(template), columns, rows);

                List<Long> totals = new ArrayList<>();
                List<Long> reads = new ArrayList<>();
                // the first round warms up the engine and is not counted
                for (int round = 0; round <= rounds; round++) {
                    String table = "benchmark_" + round;
                    queryRunner.execute("CREATE TABLE %s (%s)".formatted(table, IntStream.range(0, columns).mapToObj(column -> "c%s %s".formatted(column, trinoType(column))).collect(joining(", "))));
                    List<String> locations = copies(catalog, template, table, files);

                    long start = System.nanoTime();
                    queryRunner.execute(addFiles(table, "first", locations));
                    long total = System.nanoTime() - start;

                    start = System.nanoTime();
                    try {
                        queryRunner.execute(addFiles(table, "again", locations));
                        throw new IllegalStateException("Registering the files again succeeded");
                    }
                    catch (RuntimeException e) {
                        verify(e.getMessage().contains("already registered"), "Unexpected failure: %s", e.getMessage());
                    }
                    long read = System.nanoTime() - start;

                    out.printf("round %s: total %s ms, reading %s ms, commit %s ms%n", round, millis(total), millis(read), millis(total - read));
                    if (round > 0) {
                        totals.add(total);
                        reads.add(read);
                    }
                }
                long total = median(totals);
                long read = median(reads);
                out.printf("median: total %s ms, reading %s ms, commit %s ms (%.2f ms per file)%n", millis(total), millis(read), millis(total - read), (total - read) / 1e6 / files);
            }
        }
    }

    /**
     * The mean time a statement doing no work takes to run in the catalog database, in nanoseconds.
     */
    private static double measureRoundTrip(String connectionUrl)
            throws SQLException
    {
        int statements = 200;
        try (Connection connection = DriverManager.getConnection(connectionUrl, TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            long start = System.nanoTime();
            for (int index = 0; index < statements; index++) {
                statement.execute("SELECT 1");
            }
            return (System.nanoTime() - start) / (double) statements;
        }
    }

    /**
     * A query producing rows of every type the table declares, with values that differ from row to
     * row, so that every column carries statistics.
     */
    private static String templateQuery(int columns, int rows)
    {
        String values = IntStream.range(0, columns)
                .mapToObj(column -> switch (column % 7) {
                    case 0 -> "i::BIGINT * %s".formatted(column + 1);
                    case 1 -> "md5((i * %s)::VARCHAR)".formatted(column + 1);
                    case 2 -> "i / %s.0::DOUBLE".formatted(column + 1);
                    case 3 -> "TIMESTAMP '2024-01-01' + to_seconds(i * %s)".formatted(column + 1);
                    case 4 -> "i %% %s = 0".formatted(column + 2);
                    case 5 -> "DATE '2024-01-01' + (i %% %s)::INTEGER".formatted(column + 1);
                    default -> "(i * %s)::INTEGER".formatted(column + 1);
                } + " AS c" + column)
                .collect(joining(", "));
        return "SELECT %s FROM range(%s) t(i)".formatted(values, rows);
    }

    private static String trinoType(int column)
    {
        return switch (column % 7) {
            case 0 -> "BIGINT";
            case 1 -> "VARCHAR";
            case 2 -> "DOUBLE";
            case 3 -> "TIMESTAMP(6)";
            case 4 -> "BOOLEAN";
            case 5 -> "DATE";
            default -> "INTEGER";
        };
    }

    private static List<String> copies(TestingDuckLakeCatalog catalog, Path template, String table, int files)
            throws Exception
    {
        Path directory = catalog.dataPath().resolve("main").resolve(table).resolve("imports");
        Files.createDirectories(directory);
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        for (int file = 0; file < files; file++) {
            Path copy = directory.resolve("part-%05d.parquet".formatted(file));
            Files.copy(template, copy);
            locations.add("local:///" + catalog.dataPath().relativize(copy));
        }
        return locations.build();
    }

    private static String addFiles(String table, String importId, List<String> locations)
    {
        return "ALTER TABLE %s EXECUTE add_files(files => ARRAY[%s], import_id => '%s')".formatted(
                table,
                locations.stream().map(location -> "'" + location + "'").collect(joining(", ")),
                importId);
    }

    private static long median(List<Long> values)
    {
        List<Long> sorted = values.stream().sorted().toList();
        return sorted.get(sorted.size() / 2);
    }

    private static long millis(long nanos)
    {
        return nanos / 1_000_000;
    }

    /**
     * Forwards connections to the catalog database, holding back what is sent either way for half
     * the round trip, so that a database running next to the benchmark answers as late as one
     * across a network. What is sent in a burst arrives in a burst: only its arrival is delayed.
     */
    private static final class DelayingProxy
            implements Closeable
    {
        private final URI target;
        private final long delayNanos;
        private final ServerSocket serverSocket;
        private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("delaying-proxy-%s"));

        public DelayingProxy(String jdbcUrl, Duration roundTrip)
                throws IOException
        {
            target = URI.create(jdbcUrl.substring("jdbc:".length()));
            delayNanos = roundTrip.roundTo(NANOSECONDS) / 2;
            serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            executor.execute(this::accept);
        }

        public String jdbcUrl()
        {
            return "jdbc:postgresql://%s:%s%s".formatted(serverSocket.getInetAddress().getHostAddress(), serverSocket.getLocalPort(), target.getPath());
        }

        private void accept()
        {
            while (!serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    Socket server = new Socket(target.getHost(), target.getPort());
                    client.setTcpNoDelay(true);
                    server.setTcpNoDelay(true);
                    forward(client, server);
                    forward(server, client);
                }
                catch (IOException _) {
                    // the proxy was closed
                    return;
                }
            }
        }

        private void forward(Socket from, Socket to)
        {
            BlockingQueue<Chunk> chunks = new LinkedBlockingQueue<>();
            executor.execute(() -> {
                byte[] buffer = new byte[64 * 1024];
                try {
                    InputStream input = from.getInputStream();
                    for (int length = input.read(buffer); length >= 0; length = input.read(buffer)) {
                        chunks.add(new Chunk(System.nanoTime() + delayNanos, Arrays.copyOf(buffer, length)));
                    }
                }
                catch (IOException _) {
                    // the connection was closed
                }
                // an empty chunk ends the connection
                chunks.add(new Chunk(System.nanoTime() + delayNanos, new byte[0]));
            });
            executor.execute(() -> {
                try (from; to) {
                    OutputStream output = to.getOutputStream();
                    for (Chunk chunk = chunks.take(); chunk.bytes().length > 0; chunk = chunks.take()) {
                        for (long wait = chunk.due() - System.nanoTime(); wait > 0; wait = chunk.due() - System.nanoTime()) {
                            LockSupport.parkNanos(wait);
                        }
                        output.write(chunk.bytes());
                        output.flush();
                    }
                }
                catch (IOException _) {
                    // the connection was closed
                }
                catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        @Override
        public void close()
                throws IOException
        {
            serverSocket.close();
            executor.shutdownNow();
        }

        private record Chunk(long due, byte[] bytes) {}
    }
}
