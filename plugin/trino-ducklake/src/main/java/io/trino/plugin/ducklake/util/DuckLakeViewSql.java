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
package io.trino.plugin.ducklake.util;

/**
 * Puts the query text of a view into the form DuckLake stores it in, where the catalog the view
 * reads is named by the {@code {DUCKLAKE_CATALOG}} placeholder rather than literally.
 * <p>
 * Every engine attaches a DuckLake catalog under a name of its own choosing, so the name one
 * engine knows it by means nothing to the next: a view body that named its catalog literally
 * would bind only in the engine that wrote it. DuckDB writes the placeholder for this reason and
 * resolves it to its own attach name on read, whatever dialect the row records, so a view written
 * this way is one DuckDB can read.
 */
public final class DuckLakeViewSql
{
    private static final String CATALOG_PLACEHOLDER = "{DUCKLAKE_CATALOG}";

    private DuckLakeViewSql() {}

    /**
     * Replaces every reference to {@code catalogName} as the catalog of a qualified name with the
     * placeholder, leaving the rest of the query, string literals included, as it was.
     * <p>
     * Only a name of three parts or more is rewritten. That is the shape a catalog-qualified
     * reference has, and requiring it keeps a table alias or column that happens to share the
     * catalog's name — which reaches only two parts — out of the substitution.
     */
    public static String withCatalogPlaceholder(String sql, String catalogName)
    {
        StringBuilder rewritten = new StringBuilder(sql.length());
        int position = 0;
        while (position < sql.length()) {
            int end = endOfToken(sql, position);
            if (namesCatalog(sql, position, end, catalogName) && qualifiesQualifiedName(sql, end)) {
                rewritten.append(CATALOG_PLACEHOLDER);
            }
            else {
                rewritten.append(sql, position, end);
            }
            position = end;
        }
        return rewritten.toString();
    }

    /**
     * The position just past the token starting at {@code position}: a quoted string or
     * identifier, an unquoted identifier, or the single character that is neither.
     */
    private static int endOfToken(String sql, int position)
    {
        char first = sql.charAt(position);
        if (first == '\'' || first == '"') {
            int end = position + 1;
            while (end < sql.length()) {
                if (sql.charAt(end) == first) {
                    // a doubled quote stands for the quote itself and leaves the token open
                    if (end + 1 < sql.length() && sql.charAt(end + 1) == first) {
                        end += 2;
                        continue;
                    }
                    return end + 1;
                }
                end++;
            }
            // an unterminated quote runs to the end of the query
            return sql.length();
        }
        if (isIdentifierStart(first)) {
            int end = position + 1;
            while (end < sql.length() && isIdentifierPart(sql.charAt(end))) {
                end++;
            }
            return end;
        }
        return position + 1;
    }

    /**
     * Whether the token spanning {@code [start, end)} names the catalog. A quoted identifier names
     * it exactly, an unquoted one regardless of case, and a string literal never does.
     */
    private static boolean namesCatalog(String sql, int start, int end, String catalogName)
    {
        if (sql.charAt(start) == '\'') {
            return false;
        }
        if (sql.charAt(start) == '"') {
            // an unterminated quote is not a name at all, so it names nothing
            if (end - start < 2 || sql.charAt(end - 1) != '"') {
                return false;
            }
            return sql.substring(start + 1, end - 1).replace("\"\"", "\"").equals(catalogName);
        }
        return end - start == catalogName.length() && sql.regionMatches(true, start, catalogName, 0, catalogName.length());
    }

    /**
     * Whether a name of at least two further parts follows the token ending at {@code position},
     * which is what makes the token the catalog of a qualified name rather than a table alias.
     */
    private static boolean qualifiesQualifiedName(String sql, int position)
    {
        if (position + 1 >= sql.length() || sql.charAt(position) != '.') {
            return false;
        }
        int afterFirstPart = endOfToken(sql, position + 1);
        return afterFirstPart < sql.length() && sql.charAt(afterFirstPart) == '.';
    }

    private static boolean isIdentifierStart(char character)
    {
        return Character.isLetter(character) || character == '_';
    }

    private static boolean isIdentifierPart(char character)
    {
        return Character.isLetterOrDigit(character) || character == '_';
    }
}
