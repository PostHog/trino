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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.HoglakeSharedSessionContext.StorageSession;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Reads tables of remote team schemas for the callers of a shared catalog. Each
 * transaction reads with its caller's own verified token: the token is the bearer of
 * every catalog request, and its storage session is the only credential a file is
 * read with. Nothing here is shared between callers but the HTTP connection pool.
 */
final class HoglakeSharedReadPath
        implements AutoCloseable
{
    // The requests a read sends: the namespace, one of its tables, and that table's scan
    private static final Pattern ROUTE = Pattern.compile("/namespaces/([^/]+)(/tables/[^/]+(/scan)?)?");

    private final HoglakeSharedSessionVerifier verifier;
    private final String baseUri;
    private final String basePath;
    private final Duration requestTimeout;
    private final HttpClient httpClient;
    private final TrinoFileSystemFactory sessionFileSystems;
    private final ConnectorPageSourceProvider pageSources;
    private final Map<ConnectorTransactionHandle, Reader> readers = new ConcurrentHashMap<>();

    HoglakeSharedReadPath(HoglakeSharedConfig config, HoglakeSharedSessionVerifier verifier, TrinoFileSystemFactory fileSystemFactory)
    {
        this.verifier = requireNonNull(verifier, "verifier is null");
        baseUri = HoglakeClient.validateBaseUri(config.getUri().toASCIIString()).toString();
        String path = URI.create(baseUri).getRawPath();
        basePath = path == null ? "" : path;
        requestTimeout = Duration.ofNanos(config.getRequestTimeout().roundTo(NANOSECONDS));
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        sessionFileSystems = new SessionFileSystemFactory(verifier, requireNonNull(fileSystemFactory, "fileSystemFactory is null"));
        // No footer cache and no data cache: nothing read for one caller is kept for another
        pageSources = new HoglakePageSourceProvider(sessionFileSystems);
    }

    /**
     * The transaction's reader, made for the first caller that reads in it. A later
     * call with another token is refused: a transaction reads for one caller.
     */
    Reader reader(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        HoglakeSharedSessionContext context = verifier.verify(session);
        if (context.storage().isEmpty()) {
            throw denied();
        }
        Reader reader = readers.computeIfAbsent(transaction, _ -> newReader(context));
        if (!reader.token().equals(context.token())) {
            throw denied();
        }
        return reader;
    }

    /**
     * Whether this caller may read this table: a table of a remote team schema that
     * the caller's verified token holds.
     */
    boolean reads(ConnectorSession session, ConnectorTableHandle table)
    {
        return table instanceof HoglakeTableHandle handle && handle.remote() && verifier.verify(session).readsTeamSchema(handle.schemaName());
    }

    ConnectorSplitManager splitManager(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return new HoglakeSplitManager(reader(session, transaction).client(), HoglakeSessionProperties::getMaxSplitSize);
    }

    ConnectorPageSourceProvider pageSources()
    {
        return pageSources;
    }

    TrinoFileSystemFactory sessionFileSystems()
    {
        return sessionFileSystems;
    }

    void end(ConnectorTransactionHandle transaction)
    {
        Reader reader = readers.remove(transaction);
        if (reader != null) {
            reader.client().close();
        }
    }

    @Override
    public void close()
    {
        readers.clear();
        httpClient.shutdownNow();
    }

    private Reader newReader(HoglakeSharedSessionContext context)
    {
        String catalogPath = basePath + "/v1/catalogs/" + context.catalog();
        HoglakeClient client = new HoglakeClient(
                baseUri,
                context.catalog(),
                requestTimeout,
                new HoglakeTokenSource.Fixed(context.token()),
                httpClient,
                (method, uri) -> {
                    String path = uri.getRawPath();
                    if (!method.equals("GET") || path == null || !path.startsWith(catalogPath)) {
                        throw denied();
                    }
                    Matcher route = ROUTE.matcher(path.substring(catalogPath.length()));
                    if (!route.matches() || !context.readsTeamSchema(route.group(1))) {
                        throw denied();
                    }
                });
        return new Reader(context.token(), client, new HoglakeMetadata(client));
    }

    private static TrinoException denied()
    {
        return new TrinoException(PERMISSION_DENIED, "Shared Hoglake session authority does not allow this read");
    }

    record Reader(String token, HoglakeClient client, HoglakeMetadata metadata)
    {
        @Override
        public String toString()
        {
            return "Reader{redacted}";
        }
    }

    /**
     * File systems whose only credential is the storage session of the caller's
     * verified token. The token is verified again on the process that reads, and an
     * identity without a storage session gets no file system at all: no static key,
     * role or default credential chain stands in for it.
     */
    private static final class SessionFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final HoglakeSharedSessionVerifier verifier;
        private final TrinoFileSystemFactory delegate;

        private SessionFileSystemFactory(HoglakeSharedSessionVerifier verifier, TrinoFileSystemFactory delegate)
        {
            this.verifier = verifier;
            this.delegate = delegate;
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            StorageSession storage = verifier.verify(identity).storage().orElseThrow(HoglakeSharedReadPath::denied);
            // The storage identity carries the session and nothing of the caller
            return delegate.create(ConnectorIdentity.forUser("hoglake-session")
                    .withExtraCredentials(ImmutableMap.of(
                            EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, storage.accessKeyId(),
                            EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, storage.secretAccessKey(),
                            EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, storage.sessionToken()))
                    .build());
        }
    }
}
