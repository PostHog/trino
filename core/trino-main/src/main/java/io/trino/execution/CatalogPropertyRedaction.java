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

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.SqlFormatter.formatSql;

/**
 * Keeps catalog property values out of the query text Trino records. A {@code CREATE CATALOG}
 * statement carries connector configuration, typically including credentials, and the recorded
 * text of a query is shown in query listings, the web UI, {@code system.runtime.queries} and every
 * event listener. The statement that executes is never changed; only the text kept about it is.
 */
public final class CatalogPropertyRedaction
{
    public static final String REDACTED_VALUE = "***";
    public static final String REDACTED_STATEMENT = "<statement redacted: it may define catalog properties>";

    private static final NodeLocation UNKNOWN_LOCATION = new NodeLocation(1, 1);

    // CREATE and CATALOG as whole words separated only by whitespace. Comments in between are found
    // by the scan in mayDefineCatalogProperties: a pattern that alternates over comments backtracks
    // exponentially on crafted text, and every client can send text to this check
    private static final Pattern CREATE_CATALOG = Pattern.compile(
            "(?<![\\p{Alnum}_])CREATE\\s+CATALOG(?![\\p{Alnum}_])",
            Pattern.CASE_INSENSITIVE);

    private CatalogPropertyRedaction() {}

    /**
     * A lexical check for text that could not be parsed, or has not been yet: true if the text
     * might contain a {@code CREATE CATALOG} statement anywhere, including inside a string that
     * would be executed. False positives are acceptable, they only hide harmless text.
     */
    public static boolean mayDefineCatalogProperties(String sql)
    {
        // Applied to the whole text, string literals included, so that it also sees a statement
        // that is executed from a string, and it errs on the side of matching
        return CREATE_CATALOG.matcher(sql).find() || createCatalogAcrossComments(sql);
    }

    /**
     * One linear pass over words, whitespace and comments: true if a CREATE word is followed by a
     * CATALOG word with only whitespace and comments between them.
     */
    private static boolean createCatalogAcrossComments(String sql)
    {
        int length = sql.length();
        boolean afterCreate = false;
        int position = 0;
        while (position < length) {
            char current = sql.charAt(position);
            if (Character.isWhitespace(current)) {
                position++;
            }
            else if (sql.startsWith("/*", position)) {
                int end = sql.indexOf("*/", position + 2);
                position = end < 0 ? length : end + 2;
            }
            else if (sql.startsWith("--", position)) {
                int end = sql.indexOf('\n', position + 2);
                position = end < 0 ? length : end + 1;
            }
            else if (isWordCharacter(current)) {
                int start = position;
                while (position < length && isWordCharacter(sql.charAt(position))) {
                    position++;
                }
                int wordLength = position - start;
                if (afterCreate && wordLength == 7 && sql.regionMatches(true, start, "CATALOG", 0, 7)) {
                    return true;
                }
                afterCreate = wordLength == 6 && sql.regionMatches(true, start, "CREATE", 0, 6);
            }
            else {
                afterCreate = false;
                position++;
            }
        }
        return false;
    }

    private static boolean isWordCharacter(char character)
    {
        return character == '_' || (character < 128 && Character.isLetterOrDigit(character));
    }

    /**
     * The recorded text for a parsed statement that is, or wraps, a {@code CREATE CATALOG}, with
     * every property value masked. Empty when the statement defines no catalog properties, in which
     * case the original text is recorded unchanged.
     */
    public static Optional<String> redactedSql(Statement statement, SqlParser sqlParser)
    {
        return redact(statement, sqlParser).map(redacted -> formatSql(redacted));
    }

    /**
     * Whether a parsed statement is, or wraps, a {@code CREATE CATALOG}.
     */
    public static boolean definesCatalogProperties(Statement statement, SqlParser sqlParser)
    {
        return redact(statement, sqlParser).isPresent();
    }

    /**
     * Whether text, parsed if possible and checked lexically if not, defines catalog properties.
     */
    public static boolean definesCatalogProperties(String sql, SqlParser sqlParser)
    {
        if (!mayDefineCatalogProperties(sql)) {
            // The lexical check is a superset, so only text that passes it is worth parsing
            return false;
        }
        try {
            return definesCatalogProperties(sqlParser.createStatement(sql), sqlParser);
        }
        catch (ParsingException | IllegalArgumentException _) {
            return mayDefineCatalogProperties(sql);
        }
    }

    private static Optional<Statement> redact(Statement statement, SqlParser sqlParser)
    {
        return switch (statement) {
            case CreateCatalog createCatalog -> Optional.of(redact(createCatalog));
            case Explain explain -> redact(explain.getStatement(), sqlParser)
                    .map(inner -> new Explain(location(explain), inner, explain.getOptions()));
            case Prepare prepare -> redact(prepare.getStatement(), sqlParser)
                    .map(inner -> new Prepare(location(prepare), prepare.getName(), inner));
            case ExecuteImmediate executeImmediate -> redact(executeImmediate, sqlParser);
            default -> Optional.empty();
        };
    }

    private static Optional<Statement> redact(ExecuteImmediate executeImmediate, SqlParser sqlParser)
    {
        String innerSql = executeImmediate.getStatement().getValue();
        String redactedInner;
        try {
            Optional<Statement> inner = redact(sqlParser.createStatement(innerSql), sqlParser);
            if (inner.isEmpty()) {
                return Optional.empty();
            }
            redactedInner = formatSql(inner.get());
        }
        catch (ParsingException | IllegalArgumentException _) {
            if (!mayDefineCatalogProperties(innerSql)) {
                return Optional.empty();
            }
            redactedInner = REDACTED_STATEMENT;
        }
        // The parameters are the values of the inner statement's placeholders, so they are masked too
        List<Expression> parameters = executeImmediate.getParameters().stream()
                .map(_ -> (Expression) new StringLiteral(UNKNOWN_LOCATION, REDACTED_VALUE))
                .collect(toImmutableList());
        return Optional.of(new ExecuteImmediate(
                location(executeImmediate),
                new StringLiteral(executeImmediate.getStatement().getLocation().orElse(UNKNOWN_LOCATION), redactedInner),
                parameters));
    }

    private static CreateCatalog redact(CreateCatalog createCatalog)
    {
        List<Property> properties = createCatalog.getProperties().stream()
                .map(property -> property.isSetToDefault()
                        ? property
                        : new Property(location(property.getLocation()), property.getName(), new StringLiteral(location(property.getLocation()), REDACTED_VALUE)))
                .collect(toImmutableList());
        return new CreateCatalog(
                location(createCatalog),
                createCatalog.getCatalogName(),
                createCatalog.isNotExists(),
                createCatalog.getConnectorName(),
                properties,
                createCatalog.getPrincipal(),
                createCatalog.getComment());
    }

    private static NodeLocation location(Statement statement)
    {
        return location(statement.getLocation());
    }

    private static NodeLocation location(Optional<NodeLocation> location)
    {
        return location.orElse(UNKNOWN_LOCATION);
    }
}
