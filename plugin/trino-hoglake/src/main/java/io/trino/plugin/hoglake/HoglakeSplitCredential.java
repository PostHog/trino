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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static java.util.Objects.requireNonNull;

/**
 * One team's S3 read credential, attached to every split of the team's files,
 * for the objects under {@code prefix} ({@code s3://<bucket>/<key prefix>/}).
 * {@link #toString()}, which the split's {@code toString()} uses, omits the secrets.
 */
public record HoglakeSplitCredential(
        @JsonProperty("teamId") long teamId,
        @JsonProperty("prefix") String prefix,
        @JsonProperty("accessKeyId") String accessKeyId,
        @JsonProperty("secretAccessKey") String secretAccessKey,
        @JsonProperty("sessionToken") String sessionToken,
        @JsonProperty("expirationEpochMillis") long expirationEpochMillis)
{
    private static final int INSTANCE_SIZE = instanceSize(HoglakeSplitCredential.class);
    private static final Pattern ENCODED_DOT_OR_SLASH = Pattern.compile("%2[eEfF]");

    @JsonCreator
    public HoglakeSplitCredential
    {
        checkArgument(prefix.endsWith("/"), "prefix does not end with /: %s", prefix);
        requireNonNull(accessKeyId, "accessKeyId is null");
        requireNonNull(secretAccessKey, "secretAccessKey is null");
        requireNonNull(sessionToken, "sessionToken is null");
    }

    // The S3 filesystem signs every request with these instead of the catalog's or the pod's credentials.
    public Map<String, String> extraCredentials()
    {
        return Map.of(
                EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, accessKeyId,
                EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, secretAccessKey,
                EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionToken);
    }

    // Fails unless the split's data file and its deletion vector, if any, are under the prefix.
    public void checkCovers(HoglakeSplit split)
    {
        checkCovers(split.path());
        split.deleteFilePath().ifPresent(this::checkCovers);
    }

    private void checkCovers(String path)
    {
        if (!pathStaysUnder(path, prefix)) {
            throw new TrinoException(HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, "%s is not under the prefix %s of team %d".formatted(path, prefix, teamId));
        }
    }

    /**
     * An exact port of hoglake's {@code pathStaysUnder} ({@code ObjectPrefix.kt}),
     * tested against its shared {@code containment_cases}: a bare {@code startsWith}
     * would accept {@code team_id=4/../team_id=42/a}. Kotlin's {@code isWhitespace}
     * is Java's whitespace or space character, and its {@code split} keeps trailing
     * empty segments.
     */
    static boolean pathStaysUnder(String path, String prefix)
    {
        if (!path.startsWith(prefix)) {
            return false;
        }
        if (path.chars().anyMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c) || Character.isISOControl(c) || c == '\\')) {
            return false;
        }
        if (ENCODED_DOT_OR_SLASH.matcher(path).find()) {
            return false;
        }
        String relative = path.substring(prefix.length());
        if (relative.isEmpty()) {
            return false;
        }
        return Arrays.stream(relative.split("/", -1)).noneMatch(segment -> segment.isEmpty() || segment.equals(".") || segment.equals(".."));
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(prefix) + estimatedSizeOf(accessKeyId) + estimatedSizeOf(secretAccessKey) + estimatedSizeOf(sessionToken);
    }

    @Override
    public String toString()
    {
        return "HoglakeSplitCredential[teamId=%d, prefix=%s, expiration=%s]".formatted(teamId, prefix, Instant.ofEpochMilli(expirationEpochMillis));
    }
}
