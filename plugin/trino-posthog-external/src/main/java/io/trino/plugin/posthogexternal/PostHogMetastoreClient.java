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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.trino.metastore.SchemaAlreadyExistsException;
import io.trino.metastore.TableAlreadyExistsException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.GENERIC_EXTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Calls the PostHog external metastore API for one catalog.
 * The bearer token is read from its file on every call, so a rotated token takes effect without a restart.
 */
public class PostHogMetastoreClient
{
    // the contract's name rule; checking it here also keeps names from changing the request path
    private static final Pattern NAME = Pattern.compile("[a-z_][a-z0-9_]{0,127}");
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectMapper mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private final HttpClient httpClient;
    private final String catalogUri;
    private final Path tokenFile;

    public PostHogMetastoreClient(URI metastoreUri, Path tokenFile, String catalogId)
    {
        this(HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).followRedirects(HttpClient.Redirect.NEVER).build(), metastoreUri, tokenFile, catalogId);
    }

    PostHogMetastoreClient(HttpClient httpClient, URI metastoreUri, Path tokenFile, String catalogId)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        String base = metastoreUri.toString().replaceAll("/+$", "");
        this.catalogUri = base + "/trino/external-metastore/v1/catalogs/" + URLEncoder.encode(catalogId, UTF_8);
        this.tokenFile = requireNonNull(tokenFile, "tokenFile is null");
    }

    public static boolean isValidName(String name)
    {
        return NAME.matcher(name).matches();
    }

    public List<String> listSchemas(Optional<String> user)
    {
        Response response = send(user, "GET", "/schemas", Optional.empty());
        throwUnlessOk(response);
        return parse(response, SchemaList.class).schemas();
    }

    public boolean schemaExists(Optional<String> user, String schema)
    {
        if (!isValidName(schema)) {
            return false;
        }
        Response response = send(user, "GET", "/schemas/" + schema, Optional.empty());
        if (response.isError("not_found", 404)) {
            return false;
        }
        throwUnlessOk(response);
        return true;
    }

    public void createSchema(Optional<String> user, String schema)
    {
        checkName(schema);
        Response response = send(user, "PUT", "/schemas/" + schema, Optional.of(new SchemaBody(schema)));
        if (response.isError("already_exists", 409)) {
            throw new SchemaAlreadyExistsException(schema);
        }
        throwUnlessOk(response);
    }

    public void dropSchema(Optional<String> user, String schema)
    {
        if (!isValidName(schema)) {
            throw new SchemaNotFoundException(schema);
        }
        Response response = send(user, "DELETE", "/schemas/" + schema, Optional.empty());
        if (response.isError("not_found", 404)) {
            throw new SchemaNotFoundException(schema);
        }
        if (response.isError("not_empty", 409)) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema '%s' still has tables".formatted(schema));
        }
        throwUnlessOk(response);
    }

    public List<String> listTables(Optional<String> user, String schema)
    {
        if (!isValidName(schema)) {
            return ImmutableList.of();
        }
        Response response = send(user, "GET", "/schemas/" + schema + "/tables", Optional.empty());
        if (response.isError("not_found", 404)) {
            return ImmutableList.of();
        }
        throwUnlessOk(response);
        return parse(response, TableList.class).tables();
    }

    public Optional<ExternalTable> getTable(Optional<String> user, String schema, String table)
    {
        if (!isValidName(schema) || !isValidName(table)) {
            return Optional.empty();
        }
        Response response = send(user, "GET", "/schemas/" + schema + "/tables/" + table, Optional.empty());
        if (response.isError("not_found", 404)) {
            return Optional.empty();
        }
        throwUnlessOk(response);
        return Optional.of(parse(response, ExternalTable.class));
    }

    public void createTable(Optional<String> user, String schema, ExternalTable table)
    {
        checkName(schema);
        checkName(table.name());
        Response response = send(user, "PUT", "/schemas/" + schema + "/tables/" + table.name(), Optional.of(table));
        if (response.isError("already_exists", 409)) {
            throw new TableAlreadyExistsException(new SchemaTableName(schema, table.name()));
        }
        if (response.isError("not_found", 404)) {
            throw new SchemaNotFoundException(schema);
        }
        throwUnlessOk(response);
    }

    public void dropTable(Optional<String> user, String schema, String table)
    {
        if (!isValidName(schema) || !isValidName(table)) {
            throw new TableNotFoundException(new SchemaTableName(schema, table));
        }
        Response response = send(user, "DELETE", "/schemas/" + schema + "/tables/" + table, Optional.empty());
        if (response.isError("not_found", 404)) {
            throw new TableNotFoundException(new SchemaTableName(schema, table));
        }
        throwUnlessOk(response);
    }

    private static void checkName(String name)
    {
        if (!isValidName(name)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Invalid name '%s': names must match %s".formatted(name, NAME.pattern()));
        }
    }

    private Response send(Optional<String> user, String method, String path, Optional<Object> body)
    {
        String token = readToken();
        BodyPublisher publisher = body.map(value -> BodyPublishers.ofString(toJson(value))).orElseGet(BodyPublishers::noBody);
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(catalogUri + path))
                .timeout(REQUEST_TIMEOUT)
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/json")
                .method(method, publisher);
        body.ifPresent(_ -> request.header("Content-Type", "application/json"));
        user.ifPresent(name -> request.header("X-Trino-User", name));
        try {
            HttpResponse<String> response = httpClient.send(request.build(), BodyHandlers.ofString(UTF_8));
            return new Response(method, path, response.statusCode(), response.body(), parseError(response.body()), token);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_EXTERNAL_ERROR, "Failed to reach the PostHog metastore for %s %s".formatted(method, path), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_EXTERNAL_ERROR, "Interrupted calling the PostHog metastore", e);
        }
    }

    private String readToken()
    {
        String token;
        try {
            token = Files.readString(tokenFile, UTF_8).strip();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot read the PostHog metastore token file " + tokenFile, e);
        }
        if (token.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "The PostHog metastore token file is empty: " + tokenFile);
        }
        return token;
    }

    private Optional<ErrorBody> parseError(String body)
    {
        if (body == null || body.isBlank()) {
            return Optional.empty();
        }
        try {
            ErrorBody error = mapper.readValue(body, ErrorBody.class);
            return Optional.ofNullable(error.error()).map(_ -> error);
        }
        catch (JsonProcessingException _) {
            return Optional.empty();
        }
    }

    private static void throwUnlessOk(Response response)
    {
        if (response.status() / 100 == 2) {
            return;
        }
        String code = response.error().map(ErrorBody::error).orElse("");
        String message = response.safeMessage();
        String request = response.method() + " " + response.path();
        throw switch (code) {
            case "invalid", "limit_exceeded", "already_exists", "not_found", "not_empty" -> new TrinoException(GENERIC_USER_ERROR, "PostHog metastore refused %s (%s): %s".formatted(request, code, message));
            default -> switch (response.status()) {
                case 401, 403 -> new TrinoException(GENERIC_INTERNAL_ERROR, "PostHog metastore rejected this catalog's service credentials (HTTP %s) for %s".formatted(response.status(), request));
                default -> new TrinoException(GENERIC_EXTERNAL_ERROR, "PostHog metastore failed %s with HTTP %s: %s".formatted(request, response.status(), message));
            };
        };
    }

    private <T> T parse(Response response, Class<T> type)
    {
        try {
            return mapper.readValue(response.body(), type);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_EXTERNAL_ERROR, "Invalid response from the PostHog metastore for %s %s".formatted(response.method(), response.path()), e);
        }
    }

    private String toJson(Object value)
    {
        try {
            return mapper.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot encode a PostHog metastore request", e);
        }
    }

    private record Response(String method, String path, int status, String body, Optional<ErrorBody> error, String token)
    {
        boolean isError(String code, int expectedStatus)
        {
            return status == expectedStatus && error.map(ErrorBody::error).map(code::equals).orElse(false);
        }

        String safeMessage()
        {
            String message = error.map(ErrorBody::message).orElse("");
            if (message.length() > 500) {
                message = message.substring(0, 500);
            }
            return message.replace(token, "***");
        }

        @Override
        public String toString()
        {
            return method + " " + path + " -> " + status;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ErrorBody(String error, String message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record SchemaList(List<String> schemas)
    {
        SchemaList
        {
            schemas = schemas == null ? ImmutableList.of() : ImmutableList.copyOf(schemas);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record TableList(List<String> tables)
    {
        TableList
        {
            tables = tables == null ? ImmutableList.of() : ImmutableList.copyOf(tables);
        }
    }

    record SchemaBody(String name) {}
}
