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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static java.util.Objects.requireNonNull;

/**
 * One team's S3 session for the files of the posthog schema: read access to
 * the objects under {@code prefix} ({@code s3://<bucket>/<key prefix>/}, which
 * ends in {@code team_id=<teamId>/}) until {@code expirationEpochMillis}.
 *
 * <p>{@link #toString()}, which the split's {@code toString()} uses, prints
 * the team, the prefix and the expiry only. No message names a session value.
 */
public record HoglakeStorageSession(
        @JsonProperty("teamId") long teamId,
        @JsonProperty("prefix") String prefix,
        @JsonProperty("accessKeyId") String accessKeyId,
        @JsonProperty("secretAccessKey") String secretAccessKey,
        @JsonProperty("sessionToken") String sessionToken,
        @JsonProperty("expirationEpochMillis") long expirationEpochMillis)
{
    private static final int INSTANCE_SIZE = instanceSize(HoglakeStorageSession.class);
    private static final String S3_SCHEME = "s3://";
    private static final Pattern ENCODED_DOT_OR_SLASH = Pattern.compile("%2[eEfF]");

    @JsonCreator
    public HoglakeStorageSession
    {
        requireNonNull(prefix, "prefix is null");
        requireNonNull(accessKeyId, "accessKeyId is null");
        requireNonNull(secretAccessKey, "secretAccessKey is null");
        requireNonNull(sessionToken, "sessionToken is null");
    }

    /**
     * The extra credentials that make the S3 file system sign every request
     * with this session instead of the catalog's credentials.
     */
    public Map<String, String> extraCredentials()
    {
        return ImmutableMap.of(
                EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, accessKeyId,
                EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, secretAccessKey,
                EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionToken);
    }

    public boolean isExpired(long nowEpochMillis)
    {
        return nowEpochMillis >= expirationEpochMillis;
    }

    /**
     * Fails unless the prefix is this team's and the split's data file and
     * its deletion vector, if any, are both under it.
     */
    public void checkCovers(HoglakeSplit split)
    {
        if (!isTeamPrefix(prefix, teamId)
                || !pathStaysUnder(split.path(), prefix)
                || !split.deleteFilePath().map(path -> pathStaysUnder(path, prefix)).orElse(true)) {
            throw new TrinoException(HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, "A file of the posthog schema is outside its team's storage prefix");
        }
    }

    /**
     * Whether {@code prefix} is an S3 location whose last segment is
     * {@code team_id=<teamId>}, with no empty, {@code .} or {@code ..} segment
     * and nothing {@link #pathStaysUnder} refuses.
     */
    public static boolean isTeamPrefix(String prefix, long teamId)
    {
        if (teamId <= 0 || !prefix.endsWith("/team_id=" + teamId + "/")) {
            return false;
        }
        return pathStaysUnder(prefix.substring(0, prefix.length() - 1), S3_SCHEME);
    }

    /**
     * A port of hoglake's {@code pathStaysUnder} ({@code ObjectPrefix.kt}),
     * tested against its shared {@code containment_cases}: a bare
     * {@code startsWith} would accept {@code team_id=4/../team_id=42/a}.
     * Kotlin's {@code isWhitespace} is Java's whitespace or space character,
     * and its {@code split} keeps trailing empty segments.
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
        return "HoglakeStorageSession[teamId=%d, prefix=%s, expiration=%s]".formatted(teamId, prefix, Instant.ofEpochMilli(expirationEpochMillis));
    }
}
