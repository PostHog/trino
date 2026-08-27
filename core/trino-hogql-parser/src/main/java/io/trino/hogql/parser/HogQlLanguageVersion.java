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
package io.trino.hogql.parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record HogQlLanguageVersion(int major, int minor, int patch)
{
    private static final Pattern VERSION_PATTERN = Pattern.compile("(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)");

    public HogQlLanguageVersion
    {
        if (major < 0 || minor < 0 || patch < 0) {
            throw new IllegalArgumentException("language version components must be non-negative");
        }
    }

    @JsonCreator
    public static HogQlLanguageVersion valueOf(String value)
    {
        Matcher matcher = VERSION_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid HogQL language version: " + value);
        }
        return new HogQlLanguageVersion(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)));
    }

    @Override
    @JsonValue
    public String toString()
    {
        return "%s.%s.%s".formatted(major, minor, patch);
    }
}
