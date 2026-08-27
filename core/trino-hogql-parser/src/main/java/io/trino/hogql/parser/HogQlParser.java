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

import io.trino.hogql.parser.antlr.HogQlBaseBaseVisitor;
import io.trino.hogql.parser.antlr.HogQlBaseLexer;
import io.trino.hogql.parser.antlr.HogQlBaseParser;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.LiteralKind;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;
import java.util.Optional;

import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.BOOLEAN;
import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.INTEGER;
import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.NULL;
import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.STRING;
import static java.util.Objects.requireNonNull;

public final class HogQlParser
{
    private static final HogQlLanguageVersion CURRENT_LANGUAGE_VERSION = HogQlLanguageContract.current().languageVersion();

    private static final ANTLRErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException cause)
        {
            throw new HogQlParsingException(message, cause, line, charPositionInLine + 1);
        }
    };

    public HogQlQuery parseStatement(String hogql)
    {
        return parseStatement(hogql, CURRENT_LANGUAGE_VERSION);
    }

    public HogQlQuery parseStatement(String hogql, HogQlLanguageVersion languageVersion)
    {
        requireNonNull(hogql, "hogql is null");
        requireNonNull(languageVersion, "languageVersion is null");
        if (!languageVersion.equals(CURRENT_LANGUAGE_VERSION)) {
            throw new IllegalArgumentException("unsupported HogQL language version: " + languageVersion);
        }

        try {
            HogQlBaseLexer lexer = new HogQlBaseLexer(CharStreams.fromString(hogql));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            HogQlBaseParser parser = new HogQlBaseParser(tokenStream);

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);
            parser.removeErrorListeners();

            HogQlBaseParser.SingleStatementContext tree;
            try {
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                parser.setErrorHandler(new BailErrorStrategy());
                tree = parser.singleStatement();
            }
            catch (ParseCancellationException _) {
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                parser.setErrorHandler(new DefaultErrorStrategy());
                parser.addErrorListener(ERROR_LISTENER);
                tree = parser.singleStatement();
            }

            return new AstBuilder().build(tree.query());
        }
        catch (StackOverflowError _) {
            throw new HogQlParsingException("statement is too large", null, 1, 1);
        }
    }

    private static final class AstBuilder
            extends HogQlBaseBaseVisitor<Object>
    {
        public HogQlQuery build(HogQlBaseParser.QueryContext context)
        {
            List<Projection> projections = context.projection().stream()
                    .map(this::buildProjection)
                    .toList();
            Optional<TableReference> from = Optional.ofNullable(context.qualifiedName())
                    .map(this::buildTableReference);
            return new HogQlQuery(projections, from, sourceSpan(context));
        }

        private Projection buildProjection(HogQlBaseParser.ProjectionContext context)
        {
            if (context.ASTERISK() != null) {
                return new Star(sourceSpan(context));
            }
            return new ExpressionProjection(buildExpression(context.expression()));
        }

        private Expression buildExpression(HogQlBaseParser.ExpressionContext context)
        {
            if (context.literal() != null) {
                return buildLiteral(context.literal());
            }
            return new ColumnReference(buildIdentifiers(context.qualifiedName()), sourceSpan(context));
        }

        private Literal buildLiteral(HogQlBaseParser.LiteralContext context)
        {
            String value = context.getText();
            LiteralKind kind;
            if (context.INTEGER_VALUE() != null) {
                kind = INTEGER;
            }
            else if (context.STRING() != null) {
                kind = STRING;
                value = decodeString(value);
            }
            else if (context.NULL() != null) {
                kind = NULL;
            }
            else {
                kind = BOOLEAN;
            }
            return new Literal(kind, value, sourceSpan(context));
        }

        private TableReference buildTableReference(HogQlBaseParser.QualifiedNameContext context)
        {
            return new TableReference(buildIdentifiers(context), sourceSpan(context));
        }

        private List<Identifier> buildIdentifiers(HogQlBaseParser.QualifiedNameContext context)
        {
            return context.identifier().stream()
                    .map(this::buildIdentifier)
                    .toList();
        }

        private Identifier buildIdentifier(HogQlBaseParser.IdentifierContext context)
        {
            String value = context.getText();
            boolean delimited = context.IDENTIFIER() == null;
            if (context.QUOTED_IDENTIFIER() != null) {
                value = unquoteAndUnescape(value, '"');
            }
            else if (context.BACKQUOTED_IDENTIFIER() != null) {
                value = unquoteAndUnescape(value, '`');
            }
            return new Identifier(value, delimited, sourceSpan(context));
        }

        private static String decodeString(String value)
        {
            return value.substring(1, value.length() - 1).replace("''", "'");
        }

        private static String unquoteAndUnescape(String value, char quote)
        {
            String quoted = value.substring(1, value.length() - 1);
            return quoted.replace(String.valueOf(quote).repeat(2), String.valueOf(quote));
        }

        private static SourceSpan sourceSpan(ParserRuleContext context)
        {
            Token start = context.getStart();
            Token stop = context.getStop();
            return new SourceSpan(start.getStartIndex(), stop.getStopIndex() + 1, start.getLine(), start.getCharPositionInLine() + 1);
        }
    }
}
