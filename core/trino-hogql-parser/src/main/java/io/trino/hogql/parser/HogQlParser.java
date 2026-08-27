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

import io.trino.hogql.parser.canonical.HogQLLexer;
import io.trino.hogql.parser.canonical.HogQLParser;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlSyntaxTree;
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
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.ADD;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.AND;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.DIVIDE;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.GREATER_THAN;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.LESS_THAN;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.MODULO;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.MULTIPLY;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.NOT_EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.OR;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.SUBTRACT;
import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.INTEGER;
import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.NULL;
import static io.trino.hogql.parser.tree.HogQlQuery.LiteralKind.STRING;
import static io.trino.hogql.parser.tree.HogQlQuery.UnaryOperator.NEGATE;
import static io.trino.hogql.parser.tree.HogQlQuery.UnaryOperator.NOT;
import static io.trino.hogql.parser.tree.HogQlQuery.UnaryOperator.POSITIVE;
import static java.lang.Character.digit;
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
        try {
            ParsedQuery parsed = parseQuery(hogql, languageVersion);
            return new AstBuilder(parsed.source()).build(parsed.tree());
        }
        catch (StackOverflowError _) {
            throw new HogQlParsingException("statement is too large", null, 1, 1);
        }
    }

    public HogQlSyntaxTree parseSyntax(String hogql)
    {
        return parseSyntax(hogql, CURRENT_LANGUAGE_VERSION);
    }

    public HogQlSyntaxTree parseSyntax(String hogql, HogQlLanguageVersion languageVersion)
    {
        try {
            ParsedQuery parsed = parseQuery(hogql, languageVersion);
            return new SyntaxTreeBuilder(parsed.source()).build(parsed.tree());
        }
        catch (StackOverflowError _) {
            throw new HogQlParsingException("statement is too large", null, 1, 1);
        }
    }

    private static ParsedQuery parseQuery(String hogql, HogQlLanguageVersion languageVersion)
    {
        requireNonNull(hogql, "hogql is null");
        requireNonNull(languageVersion, "languageVersion is null");
        if (!languageVersion.equals(CURRENT_LANGUAGE_VERSION)) {
            throw new IllegalArgumentException("unsupported HogQL language version: " + languageVersion);
        }

        HogQLLexer lexer = new HogQLLexer(CharStreams.fromString(hogql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        HogQLParser parser = new HogQLParser(tokenStream);

        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);
        parser.removeErrorListeners();

        HogQLParser.SelectContext tree;
        try {
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            parser.setErrorHandler(new BailErrorStrategy());
            tree = parser.select();
        }
        catch (ParseCancellationException _) {
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            parser.setErrorHandler(new DefaultErrorStrategy());
            parser.addErrorListener(ERROR_LISTENER);
            tree = parser.select();
        }
        return new ParsedQuery(hogql, tree);
    }

    private record ParsedQuery(String source, HogQLParser.SelectContext tree) {}

    private static final class SyntaxTreeBuilder
    {
        private final SourcePositions sourcePositions;

        private SyntaxTreeBuilder(String source)
        {
            sourcePositions = new SourcePositions(source);
        }

        public HogQlSyntaxTree build(HogQLParser.SelectContext context)
        {
            HogQlSyntaxTree.LanguageClass languageClass = context.hogqlxTagElement() == null ?
                    HogQlSyntaxTree.LanguageClass.READ_ONLY_QUERY :
                    HogQlSyntaxTree.LanguageClass.HOGQLX;
            return new HogQlSyntaxTree(languageClass, buildNode(context));
        }

        private HogQlSyntaxTree.Node buildNode(ParserRuleContext context)
        {
            String rule = HogQLParser.ruleNames[context.getRuleIndex()];
            String contextName = context.getClass().getSimpleName();
            String baseContextName = Character.toUpperCase(rule.charAt(0)) + rule.substring(1) + "Context";
            Optional<String> alternative = contextName.equals(baseContextName) ?
                    Optional.empty() :
                    Optional.of(contextName.substring(0, contextName.length() - "Context".length()));

            List<HogQlSyntaxTree.Element> children = new ArrayList<>();
            for (int index = 0; index < context.getChildCount(); index++) {
                ParseTree child = context.getChild(index);
                if (child instanceof ParserRuleContext ruleChild) {
                    children.add(buildNode(ruleChild));
                }
                else if (child instanceof TerminalNode terminal && terminal.getSymbol().getType() != Token.EOF) {
                    children.add(buildToken(terminal.getSymbol()));
                }
            }
            return new HogQlSyntaxTree.Node(rule, alternative, children, sourceSpan(context));
        }

        private HogQlSyntaxTree.Token buildToken(Token token)
        {
            String type = HogQLLexer.VOCABULARY.getSymbolicName(token.getType());
            if (type == null) {
                type = HogQLLexer.VOCABULARY.getDisplayName(token.getType());
            }
            return new HogQlSyntaxTree.Token(type, token.getText(), sourceSpan(token, token));
        }

        private SourceSpan sourceSpan(ParserRuleContext context)
        {
            return sourceSpan(context.getStart(), context.getStop());
        }

        private SourceSpan sourceSpan(Token start, Token stop)
        {
            int startOffset = Math.max(0, start.getStartIndex());
            int endOffset = Math.max(startOffset, stop.getStopIndex() + 1);
            return new SourceSpan(
                    startOffset,
                    endOffset,
                    sourcePositions.line(startOffset),
                    sourcePositions.column(startOffset),
                    sourcePositions.line(endOffset),
                    sourcePositions.column(endOffset));
        }
    }

    private static final class AstBuilder
    {
        private final SourcePositions sourcePositions;

        private AstBuilder(String source)
        {
            sourcePositions = new SourcePositions(source);
        }

        public HogQlQuery build(HogQLParser.SelectContext context)
        {
            HogQLParser.SelectStmtContext select = extractPlainSelect(context);
            rejectUnsupportedClauses(select);

            List<Projection> projections = selectColumns(select.selectColumnExprListBeforeFrom()).stream()
                    .map(this::buildProjection)
                    .toList();
            Optional<TableReference> from = Optional.ofNullable(select.fromClause())
                    .map(HogQLParser.FromClauseContext::joinExpr)
                    .map(this::buildTableReference);
            Optional<Expression> where = Optional.ofNullable(select.whereClause())
                    .map(HogQLParser.WhereClauseContext::columnExpr)
                    .map(this::buildExpression);
            return new HogQlQuery(projections, from, where, sourceSpan(select));
        }

        private List<HogQLParser.SelectColumnExprContext> selectColumns(HogQLParser.SelectColumnExprListBeforeFromContext context)
        {
            if (context instanceof HogQLParser.SelectColumnExprListBeforeFromTrailingCommaContext trailingComma) {
                return trailingComma.selectColumnExpr();
            }
            if (context instanceof HogQLParser.SelectColumnExprListBeforeFromPlainContext plain) {
                return plain.selectColumnExprList().selectColumnExpr();
            }
            throw unsupported(context, "select list");
        }

        private HogQLParser.SelectStmtContext extractPlainSelect(HogQLParser.SelectContext context)
        {
            if (context.selectStmt() != null) {
                return context.selectStmt();
            }
            HogQLParser.SelectSetStmtContext set = context.selectSetStmt();
            if (set == null || !set.subsequentSelectSetClause().isEmpty() || set.orderByClause() != null || set.limitAndOffsetClauseOptional() != null) {
                throw unsupported(context, "set query");
            }
            HogQLParser.SelectStmtWithParensContext wrapped = set.selectStmtWithParens();
            if (wrapped.selectStmt() == null) {
                throw unsupported(wrapped, "parenthesized or placeholder query");
            }
            return wrapped.selectStmt();
        }

        private void rejectUnsupportedClauses(HogQLParser.SelectStmtContext context)
        {
            List<ParserRuleContext> clauses = new ArrayList<>();
            clauses.add(context.withClause());
            clauses.add(context.topClause());
            clauses.add(context.arrayJoinClause());
            clauses.add(context.prewhereClause());
            clauses.addAll(context.sampleClause());
            clauses.add(context.groupByClause());
            clauses.add(context.havingClause());
            clauses.add(context.qualifyClause());
            clauses.add(context.windowClause());
            clauses.add(context.orderByClause());
            clauses.add(context.limitByClause());
            clauses.add(context.limitAndOffsetClause());
            clauses.add(context.offsetOnlyClause());
            clauses.add(context.settingsClause());
            Optional<ParserRuleContext> firstClause = clauses.stream()
                    .filter(requireNonNullClause -> requireNonNullClause != null)
                    .min((left, right) -> Integer.compare(left.getStart().getStartIndex(), right.getStart().getStartIndex()));
            if (firstClause.isPresent()) {
                throw unsupported(firstClause.orElseThrow(), "query clause");
            }
            if (context.DISTINCT() != null) {
                throw unsupported(context, "distinct query");
            }
        }

        private Projection buildProjection(HogQLParser.SelectColumnExprContext context)
        {
            HogQLParser.ColumnExprContext expression;
            Optional<Identifier> alias = Optional.empty();
            if (context instanceof HogQLParser.ColumnExprAliasBeforeContext aliased) {
                expression = aliased.columnExpr();
                alias = Optional.of(buildIdentifier(aliased.identifier()));
            }
            else if (context instanceof HogQLParser.ColumnExprAliasImplicitContext aliased) {
                expression = aliased.columnExpr();
                alias = Optional.of(buildIdentifier(aliased.implicitAlias().getText(), aliased.implicitAlias()));
            }
            else if (context instanceof HogQLParser.ColumnExprSelectValueContext selected) {
                expression = selected.columnExpr();
                if (expression instanceof HogQLParser.ColumnExprAliasContext aliased) {
                    expression = aliased.columnExpr();
                    if (aliased.identifier() != null) {
                        alias = Optional.of(buildIdentifier(aliased.identifier()));
                    }
                    else {
                        alias = Optional.of(new Identifier(decodeQuoted(aliased.STRING_LITERAL().getText()), true, sourceSpan(aliased)));
                    }
                }
            }
            else {
                throw unsupported(context, "projection");
            }

            if (expression instanceof HogQLParser.ColumnExprValuePassthroughContext passthrough && passthrough.columnExprValue() instanceof HogQLParser.ColumnExprAsteriskContext asterisk) {
                if (alias.isPresent() || asterisk.tableIdentifier() != null || asterisk.EXCLUDE() != null) {
                    throw unsupported(asterisk, "qualified or transformed star");
                }
                return new Star(sourceSpan(context));
            }
            return new ExpressionProjection(buildExpression(expression), alias);
        }

        private Expression buildExpression(HogQLParser.ColumnExprContext context)
        {
            if (context instanceof HogQLParser.ColumnExprValuePassthroughContext passthrough) {
                return buildExpression(passthrough.columnExprValue());
            }
            if (context instanceof HogQLParser.ColumnExprAndContext binary) {
                return binary(AND, binary.columnExpr(0), binary.columnExpr(1), binary);
            }
            if (context instanceof HogQLParser.ColumnExprOrContext binary) {
                return binary(OR, binary.columnExpr(0), binary.columnExpr(1), binary);
            }
            throw unsupported(context, "expression " + context.getClass().getSimpleName());
        }

        private Expression buildExpression(HogQLParser.ColumnExprValueContext context)
        {
            if (context instanceof HogQLParser.ColumnExprLiteralContext literal) {
                return buildLiteral(literal.literal());
            }
            if (context instanceof HogQLParser.ColumnExprCaseContext caseExpression) {
                return buildCaseExpression(caseExpression);
            }
            if (context instanceof HogQLParser.ColumnExprCastContext cast) {
                return new CastExpression(buildExpression(cast.columnExpr()), buildSimpleType(cast.columnTypeExpr()), false, sourceSpan(cast));
            }
            if (context instanceof HogQLParser.ColumnExprTryCastContext cast) {
                return new CastExpression(buildExpression(cast.columnExpr()), buildSimpleType(cast.columnTypeExpr()), true, sourceSpan(cast));
            }
            if (context instanceof HogQLParser.ColumnExprIdentifierContext identifier) {
                return buildColumnReference(identifier.columnIdentifier());
            }
            if (context instanceof HogQLParser.ColumnExprParensContext parens) {
                return buildExpression(parens.columnExpr());
            }
            if (context instanceof HogQLParser.ColumnExprArrayContext array) {
                List<Expression> values = array.columnExprList() == null ? List.of() : buildExpressions(array.columnExprList());
                return new ArrayExpression(values, sourceSpan(array));
            }
            if (context instanceof HogQLParser.ColumnExprTupleContext tuple) {
                return new TupleExpression(buildExpressions(tuple.columnExprList()), sourceSpan(tuple));
            }
            if (context instanceof HogQLParser.ColumnExprNegateContext negate) {
                return new UnaryExpression(NEGATE, buildExpression(negate.columnExprValue()), sourceSpan(negate));
            }
            if (context instanceof HogQLParser.ColumnExprNotContext not) {
                return new UnaryExpression(NOT, buildExpression(not.columnExprValue()), sourceSpan(not));
            }
            if (context instanceof HogQLParser.ColumnExprPrecedence1Context binary) {
                BinaryOperator operator = switch (binary.operator.getType()) {
                    case HogQLParser.ASTERISK -> MULTIPLY;
                    case HogQLParser.SLASH -> DIVIDE;
                    case HogQLParser.PERCENT -> MODULO;
                    default -> throw unsupported(binary, "multiplicative operator");
                };
                return binary(operator, binary.left, binary.right, binary);
            }
            if (context instanceof HogQLParser.ColumnExprPrecedence2Context binary) {
                BinaryOperator operator = switch (binary.operator.getType()) {
                    case HogQLParser.PLUS -> ADD;
                    case HogQLParser.DASH -> SUBTRACT;
                    default -> throw unsupported(binary, "additive operator");
                };
                return binary(operator, binary.left, binary.right, binary);
            }
            if (context instanceof HogQLParser.ColumnExprPrecedence3Context binary) {
                if (binary.IN() != null && binary.COHORT() == null) {
                    return new InExpression(
                            buildExpression(binary.left),
                            buildInValues(binary.right),
                            binary.NOT() != null,
                            sourceSpan(binary.NOT() == null ? binary.IN().getSymbol() : binary.NOT().getSymbol(), binary.getStop()),
                            sourceSpan(binary));
                }
                if (binary.operator == null) {
                    throw unsupported(binary, "comparison operator");
                }
                BinaryOperator operator = switch (binary.operator.getType()) {
                    case HogQLParser.EQ_DOUBLE, HogQLParser.EQ_SINGLE -> EQUAL;
                    case HogQLParser.NOT_EQ -> NOT_EQUAL;
                    case HogQLParser.LT -> LESS_THAN;
                    case HogQLParser.LT_EQ -> LESS_THAN_OR_EQUAL;
                    case HogQLParser.GT -> GREATER_THAN;
                    case HogQLParser.GT_EQ -> GREATER_THAN_OR_EQUAL;
                    default -> throw unsupported(binary, "comparison operator");
                };
                return binary(operator, binary.left, binary.right, binary);
            }
            if (context instanceof HogQLParser.ColumnExprIsNullContext isNull) {
                return new IsNullExpression(
                        buildExpression(isNull.columnExprValue()),
                        isNull.NOT() != null,
                        sourceSpan(isNull.IS().getSymbol(), isNull.getStop()),
                        sourceSpan(isNull));
            }
            if (context instanceof HogQLParser.ColumnExprBetweenContext between) {
                return new BetweenExpression(
                        buildExpression(between.columnExprValue(0)),
                        buildExpression(between.columnExprValue(1)),
                        buildExpression(between.columnExprValue(2)),
                        between.NOT() != null,
                        sourceSpan(between.NOT() == null ? between.BETWEEN().getSymbol() : between.NOT().getSymbol(), between.getStop()),
                        sourceSpan(between));
            }
            if (context instanceof HogQLParser.ColumnExprFunctionContext function) {
                return buildFunction(function);
            }
            throw unsupported(context, "expression " + context.getClass().getSimpleName());
        }

        private CaseExpression buildCaseExpression(HogQLParser.ColumnExprCaseContext context)
        {
            List<HogQLParser.ColumnExprContext> expressions = context.columnExpr();
            int index = 0;
            Optional<Expression> operand = Optional.empty();
            if (context.caseExpr != null) {
                operand = Optional.of(buildExpression(expressions.get(index++)));
            }

            List<CaseWhen> whenClauses = new ArrayList<>();
            for (int whenIndex = 0; whenIndex < context.WHEN().size(); whenIndex++) {
                HogQLParser.ColumnExprContext when = expressions.get(index++);
                HogQLParser.ColumnExprContext result = expressions.get(index++);
                whenClauses.add(new CaseWhen(
                        buildExpression(when),
                        buildExpression(result),
                        sourceSpan(context.WHEN(whenIndex).getSymbol(), result.getStop())));
            }
            Optional<Expression> defaultValue = context.ELSE() == null ? Optional.empty() : Optional.of(buildExpression(expressions.get(index)));
            return new CaseExpression(operand, whenClauses, defaultValue, sourceSpan(context));
        }

        private Identifier buildSimpleType(HogQLParser.ColumnTypeExprContext context)
        {
            if (context instanceof HogQLParser.ColumnTypeExprSimpleContext simple) {
                return buildIdentifier(simple.identifier());
            }
            throw unsupported(context, "complex cast type");
        }

        private List<Expression> buildExpressions(HogQLParser.ColumnExprListContext context)
        {
            return context.columnExpr().stream()
                    .map(this::buildExpression)
                    .toList();
        }

        private List<Expression> buildInValues(HogQLParser.ColumnExprValueContext context)
        {
            if (context instanceof HogQLParser.ColumnExprTupleContext tuple) {
                return buildExpressions(tuple.columnExprList());
            }
            if (context instanceof HogQLParser.ColumnExprParensContext parens) {
                return List.of(buildExpression(parens.columnExpr()));
            }
            throw unsupported(context, "IN value list");
        }

        private BinaryExpression binary(BinaryOperator operator, HogQLParser.ColumnExprContext left, HogQLParser.ColumnExprContext right, ParserRuleContext context)
        {
            return new BinaryExpression(operator, buildExpression(left), buildExpression(right), sourceSpan(context));
        }

        private BinaryExpression binary(BinaryOperator operator, HogQLParser.ColumnExprValueContext left, HogQLParser.ColumnExprValueContext right, ParserRuleContext context)
        {
            return new BinaryExpression(operator, buildExpression(left), buildExpression(right), sourceSpan(context));
        }

        private FunctionCall buildFunction(HogQLParser.ColumnExprFunctionContext context)
        {
            if (context.columnExprs != null || context.DISTINCT() != null || context.orderExprList() != null || context.filterExpr != null) {
                throw unsupported(context, "parametric, distinct, ordered, or filtered function");
            }
            List<Expression> arguments = context.columnArgList == null ? List.of() : context.columnArgList.columnExpr().stream()
                                                                                     .map(this::buildExpression)
                                                                                     .toList();
            return new FunctionCall(buildIdentifier(context.identifier()), arguments, sourceSpan(context));
        }

        private Expression buildLiteral(HogQLParser.LiteralContext context)
        {
            if (context.NULL_SQL() != null) {
                return new Literal(NULL, "null", sourceSpan(context));
            }
            if (context.STRING_LITERAL() != null) {
                return new Literal(STRING, decodeQuoted(context.getText()), sourceSpan(context));
            }

            String value = context.numberLiteral().getText();
            if (!value.matches("[+-]?[0-9]+")) {
                throw unsupported(context, "numeric literal");
            }
            if (value.startsWith("-")) {
                return new Literal(INTEGER, "-" + normalizeInteger(value.substring(1)), sourceSpan(context));
            }
            if (value.startsWith("+")) {
                Literal magnitude = new Literal(INTEGER, normalizeInteger(value.substring(1)), sourceSpan(context));
                return new UnaryExpression(POSITIVE, magnitude, sourceSpan(context));
            }
            return new Literal(INTEGER, normalizeInteger(value), sourceSpan(context));
        }

        private Expression buildColumnReference(HogQLParser.ColumnIdentifierContext context)
        {
            if (context.placeholder() != null) {
                throw unsupported(context, "placeholder");
            }
            List<Identifier> parts = new ArrayList<>();
            if (context.tableIdentifier() != null) {
                parts.addAll(buildIdentifiers(context.tableIdentifier()));
            }
            parts.addAll(buildIdentifiers(context.nestedIdentifier()));
            if (parts.size() == 1 && !parts.getFirst().delimited()) {
                if (parts.getFirst().value().equalsIgnoreCase("true")) {
                    return new Literal(HogQlQuery.LiteralKind.BOOLEAN, "true", sourceSpan(context));
                }
                if (parts.getFirst().value().equalsIgnoreCase("false")) {
                    return new Literal(HogQlQuery.LiteralKind.BOOLEAN, "false", sourceSpan(context));
                }
            }
            return new ColumnReference(parts, sourceSpan(context));
        }

        private TableReference buildTableReference(HogQLParser.JoinExprContext context)
        {
            if (!(context instanceof HogQLParser.JoinExprTableContext table) || table.FINAL() != null || table.sampleClause() != null || !(table.tableExpr() instanceof HogQLParser.TableExprIdentifierContext identifier)) {
                throw unsupported(context, "table expression or join");
            }
            return new TableReference(buildIdentifiers(identifier.tableIdentifier()), sourceSpan(context));
        }

        private List<Identifier> buildIdentifiers(HogQLParser.TableIdentifierContext context)
        {
            List<Identifier> parts = new ArrayList<>();
            if (context.databaseIdentifier() != null) {
                parts.add(buildIdentifier(context.databaseIdentifier().identifier()));
            }
            parts.addAll(buildIdentifiers(context.nestedIdentifier()));
            return List.copyOf(parts);
        }

        private List<Identifier> buildIdentifiers(HogQLParser.NestedIdentifierContext context)
        {
            return context.identifier().stream()
                    .map(this::buildIdentifier)
                    .toList();
        }

        private Identifier buildIdentifier(HogQLParser.IdentifierContext context)
        {
            return buildIdentifier(context.getText(), context);
        }

        private Identifier buildIdentifier(String text, ParserRuleContext context)
        {
            boolean delimited = text.startsWith("`") || text.startsWith("\"");
            return new Identifier(delimited ? decodeQuoted(text) : text, delimited, sourceSpan(context));
        }

        private SourceSpan sourceSpan(ParserRuleContext context)
        {
            return sourceSpan(context.getStart(), context.getStop());
        }

        private SourceSpan sourceSpan(Token start, Token stop)
        {
            int startOffset = start.getStartIndex();
            int endOffset = stop.getStopIndex() + 1;
            return new SourceSpan(
                    startOffset,
                    endOffset,
                    sourcePositions.line(startOffset),
                    sourcePositions.column(startOffset),
                    sourcePositions.line(endOffset),
                    sourcePositions.column(endOffset));
        }

        private HogQlParsingException unsupported(ParserRuleContext context, String feature)
        {
            SourceSpan span = sourceSpan(context);
            return new HogQlParsingException("HogQL feature is not lowered yet: " + feature, null, span.startLine(), span.startColumn());
        }

        private static String normalizeInteger(String value)
        {
            int firstNonZero = 0;
            while (firstNonZero < value.length() - 1 && value.charAt(firstNonZero) == '0') {
                firstNonZero++;
            }
            return value.substring(firstNonZero);
        }

        private static String decodeQuoted(String text)
        {
            char quote = text.charAt(0);
            StringBuilder decoded = new StringBuilder(text.length() - 2);
            for (int index = 1; index < text.length() - 1; index++) {
                char character = text.charAt(index);
                if (character == quote && text.charAt(index + 1) == quote) {
                    decoded.append(quote);
                    index++;
                }
                else if (character != '\\') {
                    decoded.append(character);
                }
                else {
                    char escaped = text.charAt(++index);
                    if (escaped == 'x') {
                        decoded.append((char) ((digit(text.charAt(++index), 16) << 4) | digit(text.charAt(++index), 16)));
                    }
                    else {
                        decoded.append(switch (escaped) {
                            case '0' -> '\0';
                            case 'a' -> '\u0007';
                            case 'b' -> '\b';
                            case 'f' -> '\f';
                            case 'n' -> '\n';
                            case 'r' -> '\r';
                            case 't' -> '\t';
                            case 'v' -> '\u000B';
                            default -> escaped;
                        });
                    }
                }
            }
            return decoded.toString();
        }
    }

    private static final class SourcePositions
    {
        private final int[] lines;
        private final int[] columns;

        private SourcePositions(String source)
        {
            int length = source.codePointCount(0, source.length());
            lines = new int[length + 1];
            columns = new int[length + 1];
            lines[0] = 1;
            columns[0] = 1;

            int line = 1;
            int column = 1;
            int codePointOffset = 0;
            boolean previousWasCarriageReturn = false;
            for (int charOffset = 0; charOffset < source.length(); ) {
                int codePoint = source.codePointAt(charOffset);
                charOffset += Character.charCount(codePoint);
                codePointOffset++;
                if (codePoint == '\n' && previousWasCarriageReturn) {
                    previousWasCarriageReturn = false;
                }
                else if (codePoint == '\n' || codePoint == '\r') {
                    line++;
                    column = 1;
                    previousWasCarriageReturn = codePoint == '\r';
                }
                else {
                    column++;
                    previousWasCarriageReturn = false;
                }
                lines[codePointOffset] = line;
                columns[codePointOffset] = column;
            }
        }

        public int line(int offset)
        {
            return lines[offset];
        }

        public int column(int offset)
        {
            return columns[offset];
        }
    }
}
