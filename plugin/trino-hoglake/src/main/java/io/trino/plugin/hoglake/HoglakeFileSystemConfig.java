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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compatibility for catalogs written before Hoglake used the shared filesystem module.
 */
final class HoglakeFileSystemConfig
{
    private static final Map<String, String> LEGACY_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("hoglake.s3.endpoint", "s3.endpoint")
            .put("hoglake.s3.region", "s3.region")
            .put("hoglake.s3.access-key", "s3.aws-access-key")
            .put("hoglake.s3.secret-key", "s3.aws-secret-key")
            .put("hoglake.s3.path-style", "s3.path-style-access")
            .buildOrThrow();

    private HoglakeFileSystemConfig() {}

    public static Map<String, String> normalize(Map<String, String> properties)
    {
        Map<String, String> result = new HashMap<>(properties);
        LEGACY_PROPERTIES.forEach((legacy, standard) -> {
            if (!result.containsKey(legacy)) {
                return;
            }
            String value = normalizeValue(standard, result.remove(legacy));
            if (result.containsKey(standard)) {
                if (!Objects.equals(value, normalizeValue(standard, result.get(standard)))) {
                    throw new IllegalArgumentException("Conflicting configuration properties: %s and %s".formatted(legacy, standard));
                }
            }
            if (value == null) {
                result.remove(standard);
            }
            else {
                result.put(standard, value);
            }
        });
        return ImmutableMap.copyOf(result);
    }

    public static Map<String, String> defaults(Map<String, String> properties)
    {
        // Do not make the default modern spelling conflict with an explicit legacy alias.
        if (properties.containsKey("fs.s3.enabled") || properties.containsKey("fs.native-s3.enabled")) {
            return ImmutableMap.of("s3.region", "us-east-1");
        }
        return ImmutableMap.of("fs.s3.enabled", "true", "s3.region", "us-east-1");
    }

    private static String normalizeValue(String property, String value)
    {
        if ((property.equals("s3.endpoint") || property.equals("s3.aws-access-key") || property.equals("s3.aws-secret-key")) && value.isBlank()) {
            return null;
        }
        if (property.equals("s3.path-style-access") && (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))) {
            return Boolean.toString(Boolean.parseBoolean(value));
        }
        return value;
    }
}
