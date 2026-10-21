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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * In-memory implementation of the PostHog external metastore REST contract, for one catalog.
 */
final class FakePostHogMetastore
        implements Closeable
{
    private static final Pattern NAME = Pattern.compile("[a-z_][a-z0-9_]{0,127}");
    private static final String ID = "[^/]+";
    private static final Pattern SCHEMAS = Pattern.compile("/schemas");
    private static final Pattern SCHEMA = Pattern.compile("/schemas/(" + ID + ")");
    private static final Pattern TABLES = Pattern.compile("/schemas/(" + ID + ")/tables");
    private static final Pattern TABLE = Pattern.compile("/schemas/(" + ID + ")/tables/(" + ID + ")");

    record Request(String method, String path, Optional<String> user) {}

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpServer server;
    private final String catalogPrefix;
    private final Map<String, Map<String, JsonNode>> schemas = new ConcurrentHashMap<>();
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private volatile String token;
    private volatile Optional<Integer> failWith = Optional.empty();
    private volatile Optional<String> failMessage = Optional.empty();

    FakePostHogMetastore(String catalogId, String token)
            throws IOException
    {
        this.catalogPrefix = "/trino/external-metastore/v1/catalogs/" + catalogId;
        this.token = token;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    URI uri()
    {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    void setToken(String token)
    {
        this.token = token;
    }

    void failNextRequests(int status, String message)
    {
        failWith = Optional.of(status);
        failMessage = Optional.of(message);
    }

    void stopFailing()
    {
        failWith = Optional.empty();
    }

    List<Request> requests()
    {
        return ImmutableList.copyOf(requests);
    }

    Optional<JsonNode> table(String schema, String table)
    {
        return Optional.ofNullable(schemas.get(schema)).map(tables -> tables.get(table));
    }

    @Override
    public void close()
    {
        server.stop(0);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String rawPath = exchange.getRequestURI().getRawPath();
            requests.add(new Request(method, rawPath, Optional.ofNullable(exchange.getRequestHeaders().getFirst("X-Trino-User"))));
            if (!("Bearer " + token).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                error(exchange, 401, "unauthorized", "bad token");
                return;
            }
            if (failWith.isPresent()) {
                error(exchange, failWith.get(), "internal", failMessage.orElse(""));
                return;
            }
            if (!rawPath.startsWith(catalogPrefix)) {
                error(exchange, 404, "not_found", "unknown catalog");
                return;
            }
            route(exchange, method, rawPath.substring(catalogPrefix.length()));
        }
    }

    private synchronized void route(HttpExchange exchange, String method, String path)
            throws IOException
    {
        Matcher matcher;
        if (SCHEMAS.matcher(path).matches() && method.equals("GET")) {
            respond(exchange, 200, ImmutableMap.of("schemas", ImmutableList.copyOf(new TreeMap<>(schemas).keySet())));
        }
        else if ((matcher = SCHEMA.matcher(path)).matches()) {
            String schema = matcher.group(1);
            switch (method) {
                case "GET" -> {
                    if (schemas.containsKey(schema)) {
                        respond(exchange, 200, ImmutableMap.of("name", schema));
                    }
                    else {
                        error(exchange, 404, "not_found", "no schema " + schema);
                    }
                }
                case "PUT" -> {
                    if (!NAME.matcher(schema).matches()) {
                        error(exchange, 400, "invalid", "bad name");
                    }
                    else if (schemas.putIfAbsent(schema, new ConcurrentHashMap<>()) != null) {
                        error(exchange, 409, "already_exists", "schema exists");
                    }
                    else {
                        respond(exchange, 200, ImmutableMap.of("name", schema));
                    }
                }
                case "DELETE" -> {
                    Map<String, JsonNode> tables = schemas.get(schema);
                    if (tables == null) {
                        error(exchange, 404, "not_found", "no schema " + schema);
                    }
                    else if (!tables.isEmpty()) {
                        error(exchange, 409, "not_empty", "schema has tables");
                    }
                    else {
                        schemas.remove(schema);
                        respond(exchange, 204, null);
                    }
                }
                default -> error(exchange, 400, "invalid", "method");
            }
        }
        else if ((matcher = TABLES.matcher(path)).matches() && method.equals("GET")) {
            Map<String, JsonNode> tables = schemas.get(matcher.group(1));
            if (tables == null) {
                error(exchange, 404, "not_found", "no schema");
            }
            else {
                respond(exchange, 200, ImmutableMap.of("tables", ImmutableList.copyOf(new TreeMap<>(tables).keySet())));
            }
        }
        else if ((matcher = TABLE.matcher(path)).matches()) {
            Map<String, JsonNode> tables = schemas.get(matcher.group(1));
            String name = matcher.group(2);
            if (tables == null) {
                error(exchange, 404, "not_found", "no schema");
                return;
            }
            switch (method) {
                case "GET" -> {
                    JsonNode table = tables.get(name);
                    if (table == null) {
                        error(exchange, 404, "not_found", "no table");
                    }
                    else {
                        respond(exchange, 200, table);
                    }
                }
                case "PUT" -> {
                    JsonNode body = mapper.readTree(exchange.getRequestBody());
                    String location = body.path("location").asText();
                    if (!NAME.matcher(name).matches() || !name.equals(body.path("name").asText())
                            || !body.path("format").asText().equals("delta")
                            || !location.startsWith("s3://") || location.endsWith("/")) {
                        error(exchange, 400, "invalid", "invalid table");
                    }
                    else if (tables.putIfAbsent(name, body) != null) {
                        error(exchange, 409, "already_exists", "table exists");
                    }
                    else {
                        respond(exchange, 200, body);
                    }
                }
                case "DELETE" -> {
                    if (tables.remove(name) == null) {
                        error(exchange, 404, "not_found", "no table");
                    }
                    else {
                        respond(exchange, 204, null);
                    }
                }
                default -> error(exchange, 400, "invalid", "method");
            }
        }
        else {
            error(exchange, 404, "not_found", "no route");
        }
    }

    private void error(HttpExchange exchange, int status, String code, String message)
            throws IOException
    {
        respond(exchange, status, ImmutableMap.of("error", code, "message", message));
    }

    private void respond(HttpExchange exchange, int status, Object body)
            throws IOException
    {
        if (body == null) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        byte[] bytes = mapper.writeValueAsString(body).getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
