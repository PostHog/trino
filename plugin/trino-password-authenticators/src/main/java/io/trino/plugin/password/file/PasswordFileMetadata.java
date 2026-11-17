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
package io.trino.plugin.password.file;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;
import io.trino.spi.security.AuthenticatedPasswordPrincipal;
import io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.DUCKLAKE;
import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.HOGLAKE;

record PasswordFileMetadata(List<String> credentialLines, Map<String, AuthenticatedPasswordPrincipal> classifications)
{
    private static final String HEADER_PREFIX = "# trufflehog-auth:";
    private static final String HEADER = HEADER_PREFIX + "1";
    private static final String TARGET_PREFIX = "# trufflehog-target ";
    private static final String OPERATOR_PREFIX = "# trufflehog-operator ";
    private static final String OPERATOR = "__trino_ops";
    private static final JsonCodec<JsonRecord> RECORD_CODEC = jsonCodec(JsonRecord.class);

    PasswordFileMetadata
    {
        credentialLines = ImmutableList.copyOf(credentialLines);
        classifications = ImmutableMap.copyOf(classifications);
    }

    static PasswordFileMetadata parse(List<String> lines)
    {
        var credentials = ImmutableList.<String>builder();
        Map<String, AuthenticatedPasswordPrincipal> classifications = new HashMap<>();
        boolean firstNonemptyLine = true;
        boolean headerPresent = false;
        for (int index = 0; index < lines.size(); index++) {
            String line = lines.get(index);
            boolean header = line.startsWith(HEADER_PREFIX);
            boolean target = line.startsWith(TARGET_PREFIX);
            boolean operator = line.startsWith(OPERATOR_PREFIX);
            if (header) {
                if (!firstNonemptyLine || headerPresent || !line.equals(HEADER)) {
                    throw invalidMetadata(index + 1);
                }
                headerPresent = true;
            }
            else if (target || operator) {
                if (!headerPresent) {
                    throw invalidMetadata(index + 1);
                }
                AuthenticatedPasswordPrincipal principal = parseRecord(line, target, index + 1);
                if (classifications.putIfAbsent(principal.getName(), principal) != null) {
                    throw invalidMetadata(index + 1);
                }
            }
            if (header || target || operator) {
                // Empty placeholders preserve the credential parser's line numbers.
                credentials.add("");
            }
            else {
                credentials.add(line);
            }
            if (!line.trim().isEmpty()) {
                firstNonemptyLine = false;
            }
        }
        return new PasswordFileMetadata(credentials.build(), classifications);
    }

    private static AuthenticatedPasswordPrincipal parseRecord(String line, boolean target, int lineNumber)
    {
        JsonRecord record;
        try {
            record = RECORD_CODEC.fromJson(line.substring(target ? TARGET_PREFIX.length() : OPERATOR_PREFIX.length()));
        }
        catch (IllegalArgumentException _) {
            // JSON errors can contain the source line.
            throw invalidMetadata(lineNumber);
        }
        if (record == null) {
            throw invalidMetadata(lineNumber);
        }
        Map<String, String> fields = record.fields;
        String principal = fields.get("principal");
        if (principal == null || principal.isEmpty()) {
            throw invalidMetadata(lineNumber);
        }
        if (!target) {
            if (!fields.keySet().equals(Set.of("principal")) || !principal.equals(OPERATOR)) {
                throw invalidMetadata(lineNumber);
            }
            return AuthenticatedPasswordPrincipal.operator(principal);
        }
        if (!fields.keySet().equals(Set.of("principal", "backend")) || principal.equals(OPERATOR)) {
            throw invalidMetadata(lineNumber);
        }
        Backend backend = switch (fields.get("backend")) {
            case "hoglake" -> HOGLAKE;
            case "ducklake" -> DUCKLAKE;
            default -> throw invalidMetadata(lineNumber);
        };
        return AuthenticatedPasswordPrincipal.tenant(principal, backend);
    }

    private static TrinoException invalidMetadata(int lineNumber)
    {
        return new TrinoException(CONFIGURATION_INVALID, "Error in password file line %s: Invalid authentication metadata".formatted(lineNumber));
    }

    public static final class JsonRecord
    {
        private final Map<String, String> fields = new HashMap<>();

        @JsonAnySetter
        public void addField(String name, Object value)
        {
            if ((!name.equals("principal") && !name.equals("backend")) ||
                    !(value instanceof String text) ||
                    fields.putIfAbsent(name, text) != null) {
                throw new IllegalArgumentException("Invalid authentication metadata field");
            }
        }
    }
}
