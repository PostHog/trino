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
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CastTypeDialect;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FrameBound;
import io.trino.hogql.parser.tree.HogQlQuery.FrameBoundType;
import io.trino.hogql.parser.tree.HogQlQuery.FrameType;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InCohortExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalUnit;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinCriteria;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinType;
import io.trino.hogql.parser.tree.HogQlQuery.JoinUsing;
import io.trino.hogql.parser.tree.HogQlQuery.LambdaExpression;
import io.trino.hogql.parser.tree.HogQlQuery.LimitBy;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.NullPlacement;
import io.trino.hogql.parser.tree.HogQlQuery.NullTreatment;
import io.trino.hogql.parser.tree.HogQlQuery.PivotAggregation;
import io.trino.hogql.parser.tree.HogQlQuery.PivotRelation;
import io.trino.hogql.parser.tree.HogQlQuery.PivotValueGroup;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperationType;
import io.trino.hogql.parser.tree.HogQlQuery.SortDirection;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.StarReplacement;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnnestRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Window;
import io.trino.hogql.parser.tree.HogQlQuery.WindowDefinition;
import io.trino.hogql.parser.tree.HogQlQuery.WindowFrame;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
import io.trino.hogql.parser.tree.HogQlSyntaxTree;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.ADD;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.AND;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.CONCAT;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.DIVIDE;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.GREATER_THAN;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.ILIKE;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.LESS_THAN;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.LIKE;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.MODULO;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.MULTIPLY;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.NOT_ILIKE;
import static io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator.NOT_LIKE;
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
import static java.util.stream.Collectors.joining;

public final class HogQlParser
{
    private static final HogQlLanguageVersion CURRENT_LANGUAGE_VERSION = HogQlLanguageContract.current().languageVersion();

    private final HogQlParserLimits limits;

    private static final ANTLRErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException cause)
        {
            throw new HogQlParsingException(message, cause, line, charPositionInLine + 1);
        }
    };

    public HogQlParser()
    {
        this(HogQlParserLimits.defaults());
    }

    HogQlParser(HogQlParserLimits limits)
    {
        this.limits = requireNonNull(limits, "limits is null");
    }

    public HogQlQuery parseStatement(String hogql)
    {
        return parseStatement(hogql, CURRENT_LANGUAGE_VERSION);
    }

    public HogQlQuery parseStatement(String hogql, HogQlLanguageVersion languageVersion)
    {
        try {
            ParsedSyntax parsed = parse(hogql, languageVersion, EntryPoint.QUERY);
            return new AstBuilder(parsed.source()).build((HogQLParser.SelectContext) parsed.tree());
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
            ParsedSyntax parsed = parse(hogql, languageVersion, EntryPoint.QUERY);
            return new SyntaxTreeBuilder(parsed.source()).build((HogQLParser.SelectContext) parsed.tree());
        }
        catch (StackOverflowError _) {
            throw new HogQlParsingException("statement is too large", null, 1, 1);
        }
    }

    public HogQlSyntaxTree parseExpressionSyntax(String hogql)
    {
        return parseExpressionSyntax(hogql, CURRENT_LANGUAGE_VERSION);
    }

    public HogQlSyntaxTree parseExpressionSyntax(String hogql, HogQlLanguageVersion languageVersion)
    {
        try {
            ParsedSyntax parsed = parse(hogql, languageVersion, EntryPoint.EXPRESSION);
            return new SyntaxTreeBuilder(parsed.source()).build(parsed.tree());
        }
        catch (StackOverflowError _) {
            throw new HogQlParsingException("expression is too large", null, 1, 1);
        }
    }

    private ParsedSyntax parse(String hogql, HogQlLanguageVersion languageVersion, EntryPoint entryPoint)
    {
        requireNonNull(hogql, "hogql is null");
        requireNonNull(languageVersion, "languageVersion is null");
        if (!languageVersion.equals(CURRENT_LANGUAGE_VERSION)) {
            throw new IllegalArgumentException("unsupported HogQL language version: " + languageVersion);
        }

        HogQLLexer lexer = new HogQLLexer(CharStreams.fromString(hogql));
        CommonTokenStream tokenStream = new CommonTokenStream(new BoundedTokenSource(lexer, limits.maxTokens()));
        HogQLParser parser = new HogQLParser(tokenStream);

        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);
        parser.removeErrorListeners();

        ParserRuleContext tree;
        try {
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            parser.setErrorHandler(new BailErrorStrategy());
            parser.addParseListener(new ParseBudgetListener(limits));
            tree = entryPoint.parse(parser);
        }
        catch (ParseCancellationException _) {
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            parser.setErrorHandler(new DefaultErrorStrategy());
            parser.addErrorListener(ERROR_LISTENER);
            parser.removeParseListeners();
            parser.addParseListener(new ParseBudgetListener(limits));
            tree = entryPoint.parse(parser);
        }
        if (parser.getCurrentToken().getType() != Token.EOF) {
            Token trailing = parser.getCurrentToken();
            throw new HogQlParsingException("unexpected trailing input", null, trailing.getLine(), trailing.getCharPositionInLine() + 1);
        }
        return new ParsedSyntax(hogql, tree);
    }

    private record ParsedSyntax(String source, ParserRuleContext tree) {}

    private enum EntryPoint
    {
        QUERY {
            @Override
            public ParserRuleContext parse(HogQLParser parser)
            {
                return parser.select();
            }
        },
        EXPRESSION {
            @Override
            public ParserRuleContext parse(HogQLParser parser)
            {
                return parser.expression();
            }
        };

        public abstract ParserRuleContext parse(HogQLParser parser);
    }

    private static final class BoundedTokenSource
            implements TokenSource
    {
        private final TokenSource delegate;
        private final int maxTokens;
        private int tokenCount;

        private BoundedTokenSource(TokenSource delegate, int maxTokens)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.maxTokens = maxTokens;
        }

        @Override
        public Token nextToken()
        {
            Token token = delegate.nextToken();
            if (token.getType() != Token.EOF && ++tokenCount > maxTokens) {
                throw new HogQlParsingException("token limit exceeded", null, token.getLine(), token.getCharPositionInLine() + 1);
            }
            return token;
        }

        @Override
        public int getLine()
        {
            return delegate.getLine();
        }

        @Override
        public int getCharPositionInLine()
        {
            return delegate.getCharPositionInLine();
        }

        @Override
        public CharStream getInputStream()
        {
            return delegate.getInputStream();
        }

        @Override
        public String getSourceName()
        {
            return delegate.getSourceName();
        }

        @Override
        public void setTokenFactory(TokenFactory<?> factory)
        {
            delegate.setTokenFactory(factory);
        }

        @Override
        public TokenFactory<?> getTokenFactory()
        {
            return delegate.getTokenFactory();
        }
    }

    private static final class ParseBudgetListener
            implements ParseTreeListener
    {
        private final HogQlParserLimits limits;
        private int depth;
        private int nodes;

        private ParseBudgetListener(HogQlParserLimits limits)
        {
            this.limits = requireNonNull(limits, "limits is null");
        }

        @Override
        public void visitTerminal(TerminalNode node)
        {
            addNode(node.getSymbol());
        }

        @Override
        public void visitErrorNode(ErrorNode node)
        {
            addNode(node.getSymbol());
        }

        @Override
        public void enterEveryRule(ParserRuleContext context)
        {
            depth++;
            if (depth > limits.maxParseDepth()) {
                throw limitExceeded("parse depth limit exceeded", context.getStart());
            }
            addNode(context.getStart());
        }

        @Override
        public void exitEveryRule(ParserRuleContext context)
        {
            depth--;
        }

        private void addNode(Token token)
        {
            if (++nodes > limits.maxParseTreeNodes()) {
                throw limitExceeded("parse tree node limit exceeded", token);
            }
        }

        private static HogQlParsingException limitExceeded(String message, Token token)
        {
            return new HogQlParsingException(message, null, token.getLine(), token.getCharPositionInLine() + 1);
        }
    }

    private static final class SyntaxTreeBuilder
    {
        private final SourcePositions sourcePositions;

        private SyntaxTreeBuilder(String source)
        {
            sourcePositions = new SourcePositions(source);
        }

        public HogQlSyntaxTree build(HogQLParser.SelectContext context)
        {
            HogQlSyntaxTree.LanguageClass languageClass;
            if (context.hogqlxTagElement() != null) {
                languageClass = HogQlSyntaxTree.LanguageClass.HOGQLX;
            }
            else if (containsRule(context, HogQLParser.RULE_block)) {
                languageClass = HogQlSyntaxTree.LanguageClass.PROCEDURAL;
            }
            else {
                languageClass = HogQlSyntaxTree.LanguageClass.READ_ONLY_QUERY;
            }
            return new HogQlSyntaxTree(languageClass, buildNode(context));
        }

        public HogQlSyntaxTree build(ParserRuleContext context)
        {
            HogQlSyntaxTree.LanguageClass languageClass = containsRule(context, HogQLParser.RULE_block) ?
                    HogQlSyntaxTree.LanguageClass.PROCEDURAL :
                    HogQlSyntaxTree.LanguageClass.READ_ONLY_QUERY;
            return new HogQlSyntaxTree(languageClass, buildNode(context));
        }

        private static boolean containsRule(ParserRuleContext context, int ruleIndex)
        {
            if (context.getRuleIndex() == ruleIndex) {
                return true;
            }
            for (int index = 0; index < context.getChildCount(); index++) {
                if (context.getChild(index) instanceof ParserRuleContext child && containsRule(child, ruleIndex)) {
                    return true;
                }
            }
            return false;
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
        private final String source;
        private final SourcePositions sourcePositions;
        private final Deque<Set<String>> commonTableScopes = new ArrayDeque<>();
        private final Deque<Set<String>> prohibitedCommonTableScopes = new ArrayDeque<>();
        private final Deque<Map<String, Expression>> withExpressionScopes = new ArrayDeque<>();

        private AstBuilder(String source)
        {
            this.source = requireNonNull(source, "source is null");
            sourcePositions = new SourcePositions(source);
        }

        public HogQlQuery build(HogQLParser.SelectContext context)
        {
            HogQlQuery query;
            if (context.selectStmt() != null) {
                query = buildQuery(new SelectedQuery(context.selectStmt(), null, null));
            }
            else if (context.selectSetStmt() != null) {
                query = buildSetQuery(context.selectSetStmt());
            }
            else {
                throw unsupported(context, "query");
            }
            validateUncorrelatedSubqueries(query);
            return query;
        }

        private HogQlQuery buildSetQuery(HogQLParser.SelectSetStmtContext context)
        {
            commonTableScopes.push(new LinkedHashSet<>());
            prohibitedCommonTableScopes.push(new LinkedHashSet<>());
            try {
                QueryOperand first = buildSetOperand(context.selectStmtWithParens());
                if (context.subsequentSelectSetClause().isEmpty()) {
                    return attachSetLevelClauses(first.query(), context);
                }

                List<CommonTableExpression> commonTables = List.of();
                if (!first.parenthesized() && !first.query().with().isEmpty()) {
                    commonTables = first.query().with();
                    commonTables.stream()
                            .map(CommonTableExpression::name)
                            .map(HogQlParser.AstBuilder::canonicalName)
                            .forEach(commonTableScopes.getFirst()::add);
                    first = first.withQuery(copyQuery(List.of(), first.query().body(), first.query().orderBy(), first.query().limit(), first.query().offset(), first.query().span()));
                }

                List<QueryOperand> operands = new ArrayList<>();
                List<SetOperator> operators = new ArrayList<>();
                operands.add(first);
                for (HogQLParser.SubsequentSelectSetClauseContext clause : context.subsequentSelectSetClause()) {
                    SetOperator operator = buildSetOperator(clause);
                    QueryOperand operand = buildSetOperand(clause.selectStmtWithParens());
                    if (!operand.parenthesized() && !operand.query().with().isEmpty()) {
                        throw unsupported(clause.selectStmtWithParens(), "unparenthesized WITH set operand");
                    }
                    operators.add(operator);
                    operands.add(operand);
                }

                List<SortItem> orderBy = buildOrderBy(context.orderByClause());
                Pagination pagination = context.limitAndOffsetClauseOptional() == null
                        ? new Pagination(Optional.empty(), Optional.empty())
                        : buildPagination(context.limitAndOffsetClauseOptional());
                QueryOperand last = operands.getLast();
                if (!last.parenthesized() && hasQueryClauses(last.query())) {
                    if (!orderBy.isEmpty() && !last.query().orderBy().isEmpty()) {
                        throw unsupported(context.orderByClause(), "multiple ORDER BY clauses");
                    }
                    orderBy = orderBy.isEmpty() ? last.query().orderBy() : orderBy;
                    pagination = mergePagination(
                            new Pagination(last.query().limit(), last.query().offset()),
                            pagination,
                            context);
                    operands.set(
                            operands.size() - 1,
                            last.withQuery(copyQuery(
                                    last.query().with(),
                                    last.query().body(),
                                    List.of(),
                                    Optional.empty(),
                                    Optional.empty(),
                                    last.query().span())));
                }
                QueryOperand operation = applySetOperationPrecedence(operands, operators);
                return copyQuery(
                        commonTables,
                        operation.query().body(),
                        orderBy,
                        pagination.limit(),
                        pagination.offset(),
                        sourceSpan(context));
            }
            finally {
                prohibitedCommonTableScopes.pop();
                commonTableScopes.pop();
            }
        }

        private static boolean hasQueryClauses(HogQlQuery query)
        {
            return !query.orderBy().isEmpty() || query.limit().isPresent() || query.offset().isPresent();
        }

        private QueryOperand buildSetOperand(HogQLParser.SelectStmtWithParensContext context)
        {
            if (context.selectStmt() != null) {
                return new QueryOperand(buildQuery(new SelectedQuery(context.selectStmt(), null, null)), false, sourceSpan(context));
            }
            if (context.withClause() != null) {
                throw unsupported(context, "non-standard WITH query wrapper");
            }
            if (context.selectSetStmt() != null) {
                return new QueryOperand(buildSetQuery(context.selectSetStmt()), true, sourceSpan(context));
            }
            throw unsupported(context, "placeholder query operand");
        }

        private SetOperator buildSetOperator(HogQLParser.SubsequentSelectSetClauseContext context)
        {
            if (context.BY() != null) {
                throw unsupported(context, "set operation BY NAME");
            }
            TerminalNode operator;
            SetOperationType type;
            if (context.UNION() != null) {
                operator = context.UNION();
                type = SetOperationType.UNION;
            }
            else if (context.INTERSECT() != null) {
                operator = context.INTERSECT();
                type = SetOperationType.INTERSECT;
            }
            else {
                operator = context.EXCEPT();
                type = SetOperationType.EXCEPT;
            }
            Token stop = context.ALL() != null
                    ? context.ALL().getSymbol()
                    : context.DISTINCT() != null ? context.DISTINCT().getSymbol() : operator.getSymbol();
            return new SetOperator(type, context.ALL() == null, sourceSpan(operator.getSymbol(), stop));
        }

        private QueryOperand applySetOperationPrecedence(List<QueryOperand> operands, List<SetOperator> operators)
        {
            List<QueryOperand> unionAndExceptOperands = new ArrayList<>();
            List<SetOperator> unionAndExceptOperators = new ArrayList<>();
            QueryOperand current = operands.getFirst();
            for (int index = 0; index < operators.size(); index++) {
                SetOperator operator = operators.get(index);
                QueryOperand right = operands.get(index + 1);
                if (operator.type() == SetOperationType.INTERSECT) {
                    current = combineSetOperation(current, operator, right);
                }
                else {
                    unionAndExceptOperands.add(current);
                    unionAndExceptOperators.add(operator);
                    current = right;
                }
            }
            unionAndExceptOperands.add(current);

            current = unionAndExceptOperands.getFirst();
            for (int index = 0; index < unionAndExceptOperators.size(); ) {
                SetOperator operator = unionAndExceptOperators.get(index);
                if (operator.type() != SetOperationType.UNION) {
                    current = combineSetOperation(current, operator, unionAndExceptOperands.get(index + 1));
                    index++;
                    continue;
                }

                int runEnd = index + 1;
                while (runEnd < unionAndExceptOperators.size()) {
                    SetOperator next = unionAndExceptOperators.get(runEnd);
                    if (next.type() != SetOperationType.UNION || next.distinct() != operator.distinct()) {
                        break;
                    }
                    runEnd++;
                }
                List<QueryOperand> runOperands = new ArrayList<>(runEnd - index + 1);
                runOperands.add(current);
                runOperands.addAll(unionAndExceptOperands.subList(index + 1, runEnd + 1));
                current = combineAssociativeSetOperations(
                        runOperands,
                        unionAndExceptOperators.subList(index, runEnd),
                        0,
                        runOperands.size());
                index = runEnd;
            }
            return current;
        }

        private QueryOperand combineAssociativeSetOperations(List<QueryOperand> operands, List<SetOperator> operators, int start, int end)
        {
            if (end - start == 1) {
                return operands.get(start);
            }
            int split = (start + end) / 2;
            QueryOperand left = combineAssociativeSetOperations(operands, operators, start, split);
            QueryOperand right = combineAssociativeSetOperations(operands, operators, split, end);
            return combineSetOperation(left, operators.get(split - 1), right);
        }

        private QueryOperand combineSetOperation(QueryOperand left, SetOperator operator, QueryOperand right)
        {
            SourceSpan span = enclosingSpan(left.span(), right.span());
            SetOperation body = new SetOperation(
                    operator.type(),
                    operator.distinct(),
                    left.query(),
                    right.query(),
                    left.parenthesized(),
                    right.parenthesized(),
                    operator.span(),
                    span);
            HogQlQuery query = new HogQlQuery(
                    List.of(),
                    body,
                    List.of(),
                    Optional.empty(),
                    Optional.empty(),
                    span);
            return new QueryOperand(query, false, span);
        }

        private HogQlQuery attachSetLevelClauses(HogQlQuery query, HogQLParser.SelectSetStmtContext context)
        {
            List<SortItem> outerOrderBy = buildOrderBy(context.orderByClause());
            if (!outerOrderBy.isEmpty() && !query.orderBy().isEmpty()) {
                throw unsupported(context.orderByClause(), "multiple ORDER BY clauses");
            }
            Pagination outerPagination = context.limitAndOffsetClauseOptional() == null
                    ? new Pagination(Optional.empty(), Optional.empty())
                    : buildPagination(context.limitAndOffsetClauseOptional());
            Pagination pagination = mergePagination(
                    new Pagination(query.limit(), query.offset()),
                    outerPagination,
                    context);
            return copyQuery(
                    query.with(),
                    query.body(),
                    outerOrderBy.isEmpty() ? query.orderBy() : outerOrderBy,
                    pagination.limit(),
                    pagination.offset(),
                    sourceSpan(context));
        }

        private static HogQlQuery copyQuery(
                List<CommonTableExpression> commonTables,
                HogQlQuery.QueryBody body,
                List<SortItem> orderBy,
                Optional<Expression> limit,
                Optional<Expression> offset,
                SourceSpan span)
        {
            return new HogQlQuery(commonTables, body, orderBy, limit, offset, span);
        }

        private static SourceSpan enclosingSpan(SourceSpan left, SourceSpan right)
        {
            return new SourceSpan(
                    left.startOffset(),
                    right.endOffset(),
                    left.startLine(),
                    left.startColumn(),
                    right.endLine(),
                    right.endColumn());
        }

        private record QueryOperand(HogQlQuery query, boolean parenthesized, SourceSpan span)
        {
            private QueryOperand withQuery(HogQlQuery query)
            {
                return new QueryOperand(query, parenthesized, span);
            }
        }

        private record SetOperator(SetOperationType type, boolean distinct, SourceSpan span) {}

        private HogQlQuery buildQuery(SelectedQuery selectedQuery)
        {
            HogQLParser.SelectStmtContext select = selectedQuery.statement();
            rejectUnsupportedClauses(select);
            commonTableScopes.push(new LinkedHashSet<>());
            prohibitedCommonTableScopes.push(new LinkedHashSet<>());
            withExpressionScopes.push(new LinkedHashMap<>());
            try {
                List<CommonTableExpression> with = buildCommonTables(select.withClause());

                List<Projection> projections = selectColumns(select.selectColumnExprListBeforeFrom()).stream()
                        .map(this::buildProjection)
                        .toList();
                Optional<Relation> from = Optional.ofNullable(select.fromClause())
                        .map(HogQLParser.FromClauseContext::joinExpr)
                        .map(this::buildRelation);
                if (select.arrayJoinClause() != null) {
                    from = Optional.of(buildArrayJoin(from, select.arrayJoinClause()));
                }
                Optional<Expression> where = Optional.ofNullable(select.whereClause())
                        .map(HogQLParser.WhereClauseContext::columnExpr)
                        .map(this::buildExpression);
                List<Expression> groupBy = buildGroupBy(select);
                Optional<Expression> having = Optional.ofNullable(select.havingClause())
                        .map(HogQLParser.HavingClauseContext::columnExpr)
                        .map(this::buildExpression);
                List<WindowDefinition> windows = buildWindowDefinitions(select.windowClause());
                List<SortItem> orderBy = buildOrderBy(selectedQuery.orderBy() != null ? selectedQuery.orderBy() : select.orderByClause());
                Optional<LimitBy> limitBy = Optional.ofNullable(select.limitByClause()).map(this::buildLimitBy);
                Pagination pagination = mergePagination(
                        buildPagination(select.limitAndOffsetClause(), select.offsetOnlyClause()),
                        selectedQuery.pagination() == null ? new Pagination(Optional.empty(), Optional.empty()) : buildPagination(selectedQuery.pagination()),
                        select);
                return new HogQlQuery(
                        with,
                        new HogQlQuery.SelectQueryBody(
                                select.DISTINCT() != null,
                                projections,
                                from,
                                where,
                                groupBy,
                                having,
                                windows,
                                limitBy,
                                sourceSpan(select)),
                        orderBy,
                        pagination.limit(),
                        pagination.offset(),
                        sourceSpan(select));
            }
            finally {
                withExpressionScopes.pop();
                prohibitedCommonTableScopes.pop();
                commonTableScopes.pop();
            }
        }

        private List<CommonTableExpression> buildCommonTables(HogQLParser.WithClauseContext context)
        {
            if (context == null) {
                return List.of();
            }
            if (context.RECURSIVE() != null) {
                throw unsupported(context, "recursive CTE");
            }
            List<CommonTableExpression> commonTables = new ArrayList<>();
            Set<String> localNames = commonTableScopes.getFirst();
            for (HogQLParser.WithExprContext expression : context.withExprList().withExpr()) {
                if (expression instanceof HogQLParser.WithExprColumnContext column) {
                    Identifier name = buildIdentifier(column.identifier());
                    String canonicalName = canonicalName(name);
                    if (!localNames.add(canonicalName)) {
                        throw unsupported(column, "duplicate WITH name");
                    }
                    withExpressionScopes.getFirst().put(canonicalName, buildExpression(column.columnExpr()));
                    continue;
                }
                HogQLParser.WithExprSubqueryContext subquery = (HogQLParser.WithExprSubqueryContext) expression;
                if (subquery.USING() != null) {
                    throw unsupported(subquery, "CTE USING KEY");
                }
                if (subquery.MATERIALIZED() != null) {
                    throw unsupported(subquery, "materialized CTE");
                }
                Identifier name = buildIdentifier(subquery.identifier());
                String canonicalName = canonicalName(name);
                if (localNames.contains(canonicalName)) {
                    throw unsupported(subquery, "duplicate CTE name");
                }
                Set<String> prohibitedNames = prohibitedCommonTableScopes.getFirst();
                prohibitedNames.add(canonicalName);
                HogQlQuery query;
                try {
                    query = buildSetQuery(subquery.selectSetStmt());
                }
                finally {
                    prohibitedNames.remove(canonicalName);
                }
                List<Identifier> columnAliases = subquery.withExprColumnNameList().isEmpty()
                        ? List.of()
                        : subquery.withExprColumnNameList().getFirst().identifier().stream()
                          .map(this::buildIdentifier)
                          .toList();
                commonTables.add(new CommonTableExpression(name, columnAliases, query, sourceSpan(subquery)));
                localNames.add(canonicalName);
            }
            return List.copyOf(commonTables);
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

        private record SelectedQuery(
                HogQLParser.SelectStmtContext statement,
                HogQLParser.OrderByClauseContext orderBy,
                HogQLParser.LimitAndOffsetClauseOptionalContext pagination) {}

        private void rejectUnsupportedClauses(HogQLParser.SelectStmtContext context)
        {
            List<ParserRuleContext> clauses = new ArrayList<>();
            clauses.add(context.topClause());
            clauses.add(context.prewhereClause());
            clauses.addAll(context.sampleClause());
            clauses.add(context.qualifyClause());
            clauses.add(context.settingsClause());
            Optional<ParserRuleContext> firstClause = clauses.stream()
                    .filter(requireNonNullClause -> requireNonNullClause != null)
                    .min((left, right) -> Integer.compare(left.getStart().getStartIndex(), right.getStart().getStartIndex()));
            if (firstClause.isPresent()) {
                throw unsupported(firstClause.orElseThrow(), "query clause");
            }
        }

        private LimitBy buildLimitBy(HogQLParser.LimitByClauseContext context)
        {
            List<HogQLParser.ColumnExprContext> limitExpressions = context.limitExpr().columnExpr();
            Expression limit = buildPaginationExpression(
                    limitExpressions.size() == 2 && context.limitExpr().COMMA() != null
                            ? limitExpressions.getLast()
                            : limitExpressions.getFirst());
            Optional<Expression> offset = limitExpressions.size() == 2
                    ? Optional.of(buildPaginationExpression(
                            context.limitExpr().COMMA() != null
                                    ? limitExpressions.getFirst()
                                    : limitExpressions.getLast()))
                    : Optional.empty();
            return new LimitBy(limit, offset, buildExpressions(context.columnExprList()), sourceSpan(context));
        }

        private Relation buildArrayJoin(Optional<Relation> from, HogQLParser.ArrayJoinClauseContext context)
        {
            List<Expression> expressions = new ArrayList<>();
            List<Identifier> columnAliases = new ArrayList<>();
            for (HogQLParser.ColumnExprContext expression : context.columnExprList().columnExpr()) {
                if (!(expression instanceof HogQLParser.ColumnExprAliasContext alias) || alias.identifier() == null) {
                    throw unsupported(expression, "ARRAY JOIN expression without an identifier alias");
                }
                expressions.add(buildExpression(alias.columnExpr()));
                columnAliases.add(buildIdentifier(alias.identifier()));
            }
            Identifier relationAlias = buildIdentifier("__hogql_array_join", context);
            UnnestRelation unnest = new UnnestRelation(expressions, relationAlias, columnAliases, sourceSpan(context));
            if (from.isEmpty()) {
                if (context.LEFT() != null) {
                    throw unsupported(context, "LEFT ARRAY JOIN without FROM");
                }
                return unnest;
            }
            JoinType type = context.LEFT() == null ? JoinType.CROSS : JoinType.LEFT;
            Optional<JoinCriteria> criteria = context.LEFT() == null
                    ? Optional.empty()
                    : Optional.of(new JoinOn(new Literal(HogQlQuery.LiteralKind.BOOLEAN, "true", sourceSpan(context)), sourceSpan(context)));
            return new JoinRelation(type, from.orElseThrow(), unnest, criteria, sourceSpan(context));
        }

        private List<SortItem> buildOrderBy(HogQLParser.OrderByClauseContext context)
        {
            if (context == null) {
                return List.of();
            }
            if (context.interpolateClause() != null) {
                throw unsupported(context.interpolateClause(), "ORDER BY interpolation");
            }
            return buildSortItems(context.orderExprList());
        }

        private List<SortItem> buildSortItems(HogQLParser.OrderExprListContext context)
        {
            return context.orderExpr().stream()
                    .map(this::buildSortItem)
                    .toList();
        }

        private List<Expression> buildGroupBy(HogQLParser.SelectStmtContext select)
        {
            HogQLParser.GroupByClauseContext groupBy = select.groupByClause();
            if (groupBy == null) {
                return List.of();
            }
            if (groupBy.columnExprList() == null ||
                    groupBy.ALL() != null ||
                    groupBy.CUBE() != null ||
                    groupBy.ROLLUP() != null ||
                    groupBy.GROUPING() != null ||
                    select.CUBE() != null ||
                    select.ROLLUP() != null ||
                    select.TOTALS() != null) {
                throw unsupported(groupBy, "advanced grouping");
            }
            return buildExpressions(groupBy.columnExprList());
        }

        private SortItem buildSortItem(HogQLParser.OrderExprContext context)
        {
            if (context.COLLATE() != null || context.withFillClause() != null) {
                throw unsupported(context, "ORDER BY collation or fill");
            }
            SortDirection direction = context.DESC() != null || context.DESCENDING() != null
                    ? SortDirection.DESCENDING
                    : SortDirection.ASCENDING;
            NullPlacement nullPlacement = context.FIRST() != null
                    ? NullPlacement.FIRST
                    : context.LAST() != null ? NullPlacement.LAST : NullPlacement.UNDEFINED;
            return new SortItem(buildExpression(context.columnExpr()), direction, nullPlacement, sourceSpan(context));
        }

        private Pagination buildPagination(
                HogQLParser.LimitAndOffsetClauseContext limitContext,
                HogQLParser.OffsetOnlyClauseContext offsetContext)
        {
            if (limitContext == null) {
                return new Pagination(Optional.empty(), Optional.ofNullable(offsetContext)
                        .map(HogQLParser.OffsetOnlyClauseContext::columnExpr)
                        .map(this::buildPaginationExpression));
            }
            if (limitContext.PERCENT() != null || limitContext.WITH() != null) {
                throw unsupported(limitContext, "percent limit or ties");
            }
            List<HogQLParser.ColumnExprContext> expressions = limitContext.columnExpr();
            if (limitContext.COMMA() != null) {
                return new Pagination(
                        Optional.of(buildPaginationExpression(expressions.get(1))),
                        Optional.of(buildPaginationExpression(expressions.getFirst())));
            }
            return new Pagination(
                    Optional.of(buildPaginationExpression(expressions.getFirst())),
                    expressions.size() == 2 ? Optional.of(buildPaginationExpression(expressions.get(1))) : Optional.empty());
        }

        private Pagination buildPagination(HogQLParser.LimitAndOffsetClauseOptionalContext context)
        {
            if (context.PERCENT() != null || context.WITH() != null) {
                throw unsupported(context, "percent limit or ties");
            }
            List<HogQLParser.ColumnExprContext> expressions = context.columnExpr();
            if (context.LIMIT() == null) {
                return new Pagination(Optional.empty(), Optional.of(buildPaginationExpression(expressions.getFirst())));
            }
            if (context.COMMA() != null) {
                return new Pagination(
                        Optional.of(buildPaginationExpression(expressions.get(1))),
                        Optional.of(buildPaginationExpression(expressions.getFirst())));
            }
            return new Pagination(
                    Optional.of(buildPaginationExpression(expressions.getFirst())),
                    expressions.size() == 2 ? Optional.of(buildPaginationExpression(expressions.get(1))) : Optional.empty());
        }

        private Pagination mergePagination(Pagination inner, Pagination outer, ParserRuleContext context)
        {
            if (inner.limit().isPresent() && outer.limit().isPresent() || inner.offset().isPresent() && outer.offset().isPresent()) {
                throw unsupported(context, "duplicate pagination");
            }
            return new Pagination(
                    outer.limit().or(() -> inner.limit()),
                    outer.offset().or(() -> inner.offset()));
        }

        private Expression buildPaginationExpression(HogQLParser.ColumnExprContext context)
        {
            Expression expression = buildExpression(context);
            if (expression instanceof Literal literal && literal.kind() == INTEGER || expression instanceof Placeholder) {
                return expression;
            }
            throw unsupported(context, "non-constant pagination");
        }

        private record Pagination(Optional<Expression> limit, Optional<Expression> offset) {}

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

            if (expression instanceof HogQLParser.ColumnExprValuePassthroughContext passthrough) {
                Projection columns = buildColumnsProjection(passthrough.columnExprValue(), alias);
                if (columns != null) {
                    return columns;
                }
            }
            return new ExpressionProjection(buildExpression(expression), alias);
        }

        private Projection buildColumnsProjection(HogQLParser.ColumnExprValueContext context, Optional<Identifier> alias)
        {
            if (context instanceof HogQLParser.ColumnExprAsteriskContext asterisk) {
                requireUnaliasedColumnsProjection(alias, asterisk);
                List<Identifier> qualifier = asterisk.tableIdentifier() == null ? List.of() : buildIdentifiers(asterisk.tableIdentifier());
                return new Star(qualifier, buildStarExclusions(asterisk.identifierList()), List.of(), sourceSpan(asterisk));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsRegexContext regex) {
                requireUnaliasedColumnsProjection(alias, regex);
                return new ColumnsRegex(decodeQuoted(regex.STRING_LITERAL().getText()), sourceSpan(regex.STRING_LITERAL().getSymbol(), regex.STRING_LITERAL().getSymbol()), sourceSpan(regex));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsListContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new ColumnsList(buildExpressions(columns.columnExprList()), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsAllContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(), List.of(), List.of(), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsExcludeContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(), buildStarExclusions(columns.identifierList()), List.of(), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsReplaceContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(), List.of(), buildStarReplacements(columns.columnsReplaceList()), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsExcludeReplaceContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(), buildStarExclusions(columns.identifierList()), buildStarReplacements(columns.columnsReplaceList()), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsQualifiedAllContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(buildIdentifier(columns.identifier())), List.of(), List.of(), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsQualifiedExcludeContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(buildIdentifier(columns.identifier())), buildStarExclusions(columns.identifierList()), List.of(), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsQualifiedReplaceContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(List.of(buildIdentifier(columns.identifier())), List.of(), buildStarReplacements(columns.columnsReplaceList()), sourceSpan(columns));
            }
            if (context instanceof HogQLParser.ColumnExprColumnsQualifiedExcludeReplaceContext columns) {
                requireUnaliasedColumnsProjection(alias, columns);
                return new Star(
                        List.of(buildIdentifier(columns.identifier())),
                        buildStarExclusions(columns.identifierList()),
                        buildStarReplacements(columns.columnsReplaceList()),
                        sourceSpan(columns));
            }
            return null;
        }

        private void requireUnaliasedColumnsProjection(Optional<Identifier> alias, ParserRuleContext context)
        {
            if (alias.isPresent()) {
                throw unsupported(context, "aliased columns projection");
            }
        }

        private List<ColumnReference> buildStarExclusions(HogQLParser.IdentifierListContext exclusions)
        {
            return exclusions == null ? List.of() : exclusions.nestedIdentifier().stream()
                                                    .map(identifier -> new ColumnReference(buildIdentifiers(identifier), sourceSpan(identifier)))
                                                    .toList();
        }

        private List<StarReplacement> buildStarReplacements(HogQLParser.ColumnsReplaceListContext replacements)
        {
            return replacements.columnsReplaceItem().stream()
                    .map(replacement -> new StarReplacement(
                            buildExpression(replacement.columnExpr()),
                            buildIdentifier(replacement.identifier()),
                            sourceSpan(replacement)))
                    .toList();
        }

        private Expression buildExpression(HogQLParser.ColumnExprContext context)
        {
            if (context instanceof HogQLParser.ColumnExprAliasContext alias) {
                return buildExpression(alias.columnExpr());
            }
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
                return new CastExpression(buildExpression(cast.columnExpr()), buildType(cast.columnTypeExpr()), false, CastTypeDialect.HOGQL, sourceSpan(cast));
            }
            if (context instanceof HogQLParser.ColumnExprTryCastContext cast) {
                return new CastExpression(buildExpression(cast.columnExpr()), buildType(cast.columnTypeExpr()), true, CastTypeDialect.HOGQL, sourceSpan(cast));
            }
            if (context instanceof HogQLParser.ColumnExprIntervalContext interval) {
                return new IntervalExpression(
                        buildExpression(interval.columnExpr()),
                        buildIntervalUnit(interval.interval().getText(), interval.interval()),
                        sourceSpan(interval));
            }
            if (context instanceof HogQLParser.ColumnExprIntervalStringContext interval) {
                return buildStringInterval(interval);
            }
            if (context instanceof HogQLParser.ColumnExprIdentifierContext identifier) {
                return buildColumnReference(identifier.columnIdentifier());
            }
            if (context instanceof HogQLParser.ColumnExprSubqueryContext subquery) {
                return new ScalarSubqueryExpression(buildSetQuery(subquery.selectSetStmt()), sourceSpan(subquery));
            }
            if (context instanceof HogQLParser.ColumnExprParensContext parens) {
                return buildExpression(parens.columnExpr());
            }
            if (context instanceof HogQLParser.ColumnExprArrayContext array) {
                List<Expression> values = array.columnExprList() == null ? List.of() : buildExpressions(array.columnExprList());
                return new ArrayExpression(values, sourceSpan(array));
            }
            if (context instanceof HogQLParser.ColumnExprTemplateStringContext template) {
                return buildTemplateString(template.templateString());
            }
            if (context instanceof HogQLParser.ColumnExprTagElementContext tag) {
                return buildHogQlXTag(tag.hogqlxTagElement());
            }
            if (context instanceof HogQLParser.ColumnExprTupleContext tuple) {
                return new TupleExpression(buildExpressions(tuple.columnExprList()), sourceSpan(tuple));
            }
            if (context instanceof HogQLParser.ColumnExprArrayAccessContext access) {
                return new SubscriptExpression(
                        buildExpression(access.columnExprValue()),
                        buildExpression(access.columnExpr()),
                        sourceSpan(access));
            }
            if (context instanceof HogQLParser.ColumnExprTupleAccessContext access) {
                if (access.DECIMAL_LITERAL().getText().equals("0")) {
                    throw unsupported(access, "tuple index zero");
                }
                return new SubscriptExpression(
                        buildExpression(access.columnExprValue()),
                        new Literal(INTEGER, access.DECIMAL_LITERAL().getText(), sourceSpan(access.DECIMAL_LITERAL().getSymbol(), access.DECIMAL_LITERAL().getSymbol())),
                        sourceSpan(access));
            }
            if (context instanceof HogQLParser.ColumnExprPropertyAccessContext access) {
                return new MemberAccessExpression(
                        buildExpression(access.columnExprValue()),
                        buildIdentifier(access.identifier()),
                        sourceSpan(access));
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
                    case HogQLParser.CONCAT -> CONCAT;
                    default -> throw unsupported(binary, "additive operator");
                };
                return binary(operator, binary.left, binary.right, binary);
            }
            if (context instanceof HogQLParser.ColumnExprPrecedence3Context binary) {
                if (binary.IN() != null) {
                    if (binary.right instanceof HogQLParser.ColumnExprSubqueryContext subquery) {
                        if (binary.COHORT() != null) {
                            throw unsupported(binary, "IN COHORT subquery");
                        }
                        return new InSubqueryExpression(
                                buildExpression(binary.left),
                                buildSetQuery(subquery.selectSetStmt()),
                                binary.NOT() != null,
                                sourceSpan(binary.NOT() == null ? binary.IN().getSymbol() : binary.NOT().getSymbol(), binary.getStop()),
                                sourceSpan(binary));
                    }
                    if (binary.COHORT() != null) {
                        return new InCohortExpression(
                                buildExpression(binary.left),
                                buildExpression(binary.right),
                                binary.NOT() != null,
                                sourceSpan(binary.NOT() == null ? binary.IN().getSymbol() : binary.NOT().getSymbol(), binary.getStop()),
                                sourceSpan(binary));
                    }
                    return new InExpression(
                            buildExpression(binary.left),
                            buildInValues(binary.right),
                            binary.NOT() != null,
                            sourceSpan(binary.NOT() == null ? binary.IN().getSymbol() : binary.NOT().getSymbol(), binary.getStop()),
                            sourceSpan(binary));
                }
                if (binary.LIKE() != null) {
                    return binary(binary.NOT() == null ? LIKE : NOT_LIKE, binary.left, binary.right, binary);
                }
                if (binary.ILIKE() != null) {
                    return binary(binary.NOT() == null ? ILIKE : NOT_ILIKE, binary.left, binary.right, binary);
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
            if (context instanceof HogQLParser.ColumnExprNullishContext nullish) {
                Token operator = nullish.NULLISH().getSymbol();
                return new FunctionCall(
                        new Identifier("ifNull", false, sourceSpan(operator, operator)),
                        List.of(buildExpression(nullish.columnExprValue(0)), buildExpression(nullish.columnExprValue(1))),
                        false,
                        List.of(),
                        Optional.empty(),
                        sourceSpan(nullish));
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
            if (context instanceof HogQLParser.ColumnExprIgnoreNullsContext ignoreNulls) {
                Expression expression = buildExpression(ignoreNulls.columnExprValue());
                if (!(expression instanceof FunctionCall function) || function.window().isEmpty()) {
                    throw unsupported(ignoreNulls, "IGNORE NULLS outside window function");
                }
                if (function.nullTreatment().isPresent()) {
                    throw unsupported(ignoreNulls, "duplicate window null treatment");
                }
                return new FunctionCall(
                        function.nameParts(),
                        function.arguments(),
                        function.distinct(),
                        function.orderBy(),
                        function.filter(),
                        Optional.of(NullTreatment.IGNORE),
                        function.window(),
                        sourceSpan(ignoreNulls));
            }
            if (context instanceof HogQLParser.ColumnExprFunctionContext function) {
                return buildFunction(function);
            }
            if (context instanceof HogQLParser.ColumnExprWinFunctionContext function) {
                return buildWindowFunction(
                        function,
                        function.identifier(),
                        function.columnExprs,
                        function.columnArgList,
                        function.filterExpr,
                        buildWindowSpecification(function.windowExpr()));
            }
            if (context instanceof HogQLParser.ColumnExprWinFunctionTargetContext function) {
                return buildWindowFunction(
                        function,
                        function.identifier(0),
                        function.columnExprs,
                        function.columnArgList,
                        function.filterExpr,
                        new WindowReference(buildIdentifier(function.identifier(1)), sourceSpan(function.identifier(1))));
            }
            if (context instanceof HogQLParser.ColumnExprLambdaContext lambda) {
                HogQLParser.ColumnLambdaExprContext lambdaExpression = lambda.columnLambdaExpr();
                if (lambdaExpression instanceof HogQLParser.ArrowLambdaContext arrow) {
                    if (arrow.block() != null) {
                        throw unsupported(arrow.block(), "lambda block");
                    }
                    return new LambdaExpression(
                            arrow.identifier().stream().map(this::buildIdentifier).toList(),
                            buildExpression(arrow.columnExpr()),
                            sourceSpan(arrow));
                }
                if (lambdaExpression instanceof HogQLParser.ColonLambdaContext colon) {
                    return new LambdaExpression(
                            colon.identifier().stream().map(this::buildIdentifier).toList(),
                            buildExpression(colon.columnExpr()),
                            sourceSpan(colon));
                }
                throw unsupported(lambdaExpression, "lambda expression");
            }
            if (context instanceof HogQLParser.ColumnExprColonLambdaContext lambda) {
                return new LambdaExpression(
                        lambda.identifier().stream().map(this::buildIdentifier).toList(),
                        buildExpression(lambda.columnExpr()),
                        sourceSpan(lambda));
            }
            throw unsupported(context, "expression " + context.getClass().getSimpleName());
        }

        private Expression buildTemplateString(HogQLParser.TemplateStringContext context)
        {
            List<Expression> parts = new ArrayList<>();
            boolean interpolated = false;
            for (HogQLParser.StringContentsContext contents : context.stringContents()) {
                if (contents.STRING_TEXT() != null) {
                    Token token = contents.STRING_TEXT().getSymbol();
                    parts.add(new Literal(STRING, decodeQuoted("'" + token.getText() + "'"), sourceSpan(token, token)));
                }
                else {
                    interpolated = true;
                    Expression value = buildExpression(contents.columnExpr());
                    parts.add(new FunctionCall(
                            new Identifier("toString", false, value.span()),
                            List.of(value),
                            false,
                            List.of(),
                            Optional.empty(),
                            value.span()));
                }
            }
            if (!interpolated) {
                String value = parts.stream()
                        .map(Literal.class::cast)
                        .map(Literal::value)
                        .collect(joining());
                return new Literal(STRING, value, sourceSpan(context));
            }
            if (parts.size() == 1) {
                return parts.getFirst();
            }
            return new FunctionCall(
                    new Identifier("concat", false, sourceSpan(context)),
                    parts,
                    false,
                    List.of(),
                    Optional.empty(),
                    sourceSpan(context));
        }

        private TupleExpression buildHogQlXTag(HogQLParser.HogqlxTagElementContext context)
        {
            HogQLParser.IdentifierContext openingIdentifier;
            List<HogQLParser.HogqlxTagAttributeContext> attributes;
            List<Expression> children = new ArrayList<>();
            if (context instanceof HogQLParser.HogqlxTagElementClosedContext closed) {
                openingIdentifier = closed.identifier();
                attributes = closed.hogqlxTagAttribute();
            }
            else if (context instanceof HogQLParser.HogqlxTagElementNestedContext nested) {
                openingIdentifier = nested.identifier(0);
                Identifier opening = buildIdentifier(openingIdentifier);
                Identifier closing = buildIdentifier(nested.identifier(1));
                if (!opening.value().equals(closing.value())) {
                    throw unsupported(nested.identifier(1), "mismatched HogQLX closing tag");
                }
                attributes = nested.hogqlxTagAttribute();
                for (HogQLParser.HogqlxChildElementContext child : nested.hogqlxChildElement()) {
                    if (child.hogqlxTagElement() != null) {
                        children.add(buildHogQlXTag(child.hogqlxTagElement()));
                    }
                    else if (child.hogqlxText() != null) {
                        String text = child.hogqlxText().getText();
                        if (!((text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) && text.isBlank())) {
                            children.add(new Literal(STRING, text, sourceSpan(child.hogqlxText())));
                        }
                    }
                    else {
                        children.add(buildExpression(child.columnExpr()));
                    }
                }
            }
            else {
                throw unsupported(context, "HogQLX tag");
            }

            List<Expression> tupleValues = new ArrayList<>();
            tupleValues.add(new Literal(STRING, "__hx_tag", sourceSpan(context)));
            tupleValues.add(new Literal(STRING, buildIdentifier(openingIdentifier).value(), sourceSpan(openingIdentifier)));
            boolean hasChildrenAttribute = false;
            for (HogQLParser.HogqlxTagAttributeContext attribute : attributes) {
                Identifier name = buildIdentifier(attribute.identifier());
                hasChildrenAttribute |= name.value().equals("children");
                tupleValues.add(new Literal(STRING, name.value(), sourceSpan(attribute.identifier())));
                tupleValues.add(buildHogQlXAttributeValue(attribute));
            }
            if (!children.isEmpty()) {
                if (hasChildrenAttribute) {
                    throw unsupported(context, "HogQLX tag with both nested children and a children attribute");
                }
                tupleValues.add(new Literal(STRING, "children", sourceSpan(context)));
                tupleValues.add(new TupleExpression(children, sourceSpan(context)));
            }
            return new TupleExpression(tupleValues, sourceSpan(context));
        }

        private Expression buildHogQlXAttributeValue(HogQLParser.HogqlxTagAttributeContext attribute)
        {
            if (attribute.string() != null) {
                HogQLParser.StringContext value = attribute.string();
                if (value.STRING_LITERAL() != null) {
                    return new Literal(STRING, decodeQuoted(value.STRING_LITERAL().getText()), sourceSpan(value));
                }
                return buildTemplateString(value.templateString());
            }
            if (attribute.columnExpr() != null) {
                return buildExpression(attribute.columnExpr());
            }
            return new Literal(HogQlQuery.LiteralKind.BOOLEAN, "true", sourceSpan(attribute));
        }

        private IntervalExpression buildStringInterval(HogQLParser.ColumnExprIntervalStringContext context)
        {
            String value = decodeQuoted(context.STRING_LITERAL().getText());
            int separator = value.indexOf(' ');
            if (separator < 0) {
                throw intervalError(context, "Unsupported interval type: must be in the format '<count> <unit>'");
            }

            String count = value.substring(0, separator);
            String unit = value.substring(separator + 1);
            if (!count.matches("[0-9]+")) {
                throw intervalError(context, "Unsupported interval count: '" + count + "' is not a valid integer");
            }
            try {
                Long.parseLong(count);
            }
            catch (NumberFormatException _) {
                throw intervalError(context, "Unsupported interval count: '" + count + "' is too large");
            }

            String singularUnit = unit.endsWith("s") ? unit.substring(0, unit.length() - 1) : unit;
            IntervalUnit intervalUnit;
            try {
                intervalUnit = IntervalUnit.valueOf(singularUnit.toUpperCase(Locale.ENGLISH));
            }
            catch (IllegalArgumentException _) {
                throw intervalError(context, "Unsupported interval unit: " + unit);
            }
            if (!unit.equals(singularUnit) && !unit.equals(singularUnit + "s")) {
                throw intervalError(context, "Unsupported interval unit: " + unit);
            }
            if (!unit.equals(unit.toLowerCase(Locale.ENGLISH))) {
                throw intervalError(context, "Unsupported interval unit: " + unit);
            }
            return new IntervalExpression(
                    new Literal(INTEGER, normalizeInteger(count), sourceSpan(context.STRING_LITERAL().getSymbol(), context.STRING_LITERAL().getSymbol())),
                    intervalUnit,
                    sourceSpan(context));
        }

        private IntervalUnit buildIntervalUnit(String value, ParserRuleContext context)
        {
            try {
                return IntervalUnit.valueOf(value.toUpperCase(Locale.ENGLISH));
            }
            catch (IllegalArgumentException _) {
                throw intervalError(context, "Unsupported interval unit: " + value);
            }
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

        private Identifier buildType(HogQLParser.ColumnTypeExprContext context)
        {
            SourceSpan span = sourceSpan(context);
            return new Identifier(source.substring(span.startOffset(), span.endOffset()), false, span);
        }

        private List<Expression> buildExpressions(HogQLParser.ColumnExprListContext context)
        {
            return context.columnExpr().stream()
                    .map(this::buildExpression)
                    .toList();
        }

        private List<Expression> buildInValues(HogQLParser.ColumnExprValueContext context)
        {
            if (context instanceof HogQLParser.ColumnExprArrayContext array) {
                return array.columnExprList() == null ? List.of() : buildExpressions(array.columnExprList());
            }
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
            Identifier name = buildIdentifier(context.identifier());
            if (context.columnExprs != null && !Set.of("quantile", "quantileexact", "quantileif", "grouparrayif").contains(canonicalName(name))) {
                throw unsupported(context, "parametric function");
            }
            List<Expression> arguments = new ArrayList<>(buildFunctionArguments(context));
            if (context.columnExprs != null) {
                arguments.addAll(buildExpressions(context.columnExprs));
            }
            List<SortItem> orderBy = context.orderExprList() == null ? List.of() : buildSortItems(context.orderExprList());
            Optional<Expression> filter = Optional.ofNullable(context.filterExpr).map(this::buildExpression);
            return new FunctionCall(
                    name,
                    List.copyOf(arguments),
                    context.DISTINCT() != null,
                    orderBy,
                    filter,
                    sourceSpan(context));
        }

        private FunctionCall buildWindowFunction(
                ParserRuleContext context,
                HogQLParser.IdentifierContext name,
                HogQLParser.ColumnExprListContext arguments,
                HogQLParser.ColumnExprListContext parametricArguments,
                HogQLParser.ColumnExprContext filter,
                Window window)
        {
            if (parametricArguments != null) {
                throw unsupported(context, "parametric window function");
            }
            List<Expression> functionArguments;
            if (arguments == null) {
                functionArguments = List.of();
            }
            else if (arguments.columnExpr().size() == 1 && isUnqualifiedStar(arguments.columnExpr().getFirst())) {
                functionArguments = List.of();
            }
            else {
                functionArguments = buildExpressions(arguments);
            }
            Optional<Expression> functionFilter = Optional.ofNullable(filter).map(this::buildExpression);
            return new FunctionCall(
                    List.of(buildIdentifier(name)),
                    functionArguments,
                    false,
                    List.of(),
                    functionFilter,
                    Optional.empty(),
                    Optional.of(window),
                    sourceSpan(context));
        }

        private List<WindowDefinition> buildWindowDefinitions(HogQLParser.WindowClauseContext context)
        {
            if (context == null) {
                return List.of();
            }
            List<WindowDefinition> definitions = new ArrayList<>();
            for (int index = 0; index < context.identifier().size(); index++) {
                Identifier name = buildIdentifier(context.identifier(index));
                WindowSpecification specification = buildWindowSpecification(context.windowExpr(index));
                definitions.add(new WindowDefinition(name, specification, enclosingSpan(name.span(), specification.span())));
            }
            return List.copyOf(definitions);
        }

        private WindowSpecification buildWindowSpecification(HogQLParser.WindowExprContext context)
        {
            List<Expression> partitionBy = context.winPartitionByClause() == null
                    ? List.of()
                    : buildExpressions(context.winPartitionByClause().columnExprList());
            List<SortItem> orderBy = context.winOrderByClause() == null
                    ? List.of()
                    : buildSortItems(context.winOrderByClause().orderExprList());
            Optional<WindowFrame> frame = Optional.ofNullable(context.winFrameClause()).map(this::buildWindowFrame);
            return new WindowSpecification(partitionBy, orderBy, frame, sourceSpan(context));
        }

        private WindowFrame buildWindowFrame(HogQLParser.WinFrameClauseContext context)
        {
            FrameType type = context.ROWS() != null ? FrameType.ROWS : FrameType.RANGE;
            if (context.winFrameExtend() instanceof HogQLParser.FrameStartContext start) {
                return new WindowFrame(type, buildFrameBound(start.winFrameBound()), Optional.empty(), sourceSpan(context));
            }
            HogQLParser.FrameBetweenContext between = (HogQLParser.FrameBetweenContext) context.winFrameExtend();
            return new WindowFrame(
                    type,
                    buildFrameBound(between.winFrameBound(0)),
                    Optional.of(buildFrameBound(between.winFrameBound(1))),
                    sourceSpan(context));
        }

        private FrameBound buildFrameBound(HogQLParser.WinFrameBoundContext context)
        {
            if (context.CURRENT() != null) {
                return new FrameBound(FrameBoundType.CURRENT_ROW, Optional.empty(), sourceSpan(context));
            }
            if (context.UNBOUNDED() != null) {
                FrameBoundType type = context.PRECEDING() != null
                        ? FrameBoundType.UNBOUNDED_PRECEDING
                        : FrameBoundType.UNBOUNDED_FOLLOWING;
                return new FrameBound(type, Optional.empty(), sourceSpan(context));
            }
            FrameBoundType type = context.PRECEDING() != null ? FrameBoundType.PRECEDING : FrameBoundType.FOLLOWING;
            return new FrameBound(type, Optional.of(buildExpression(context.columnExpr())), sourceSpan(context));
        }

        private List<Expression> buildFunctionArguments(HogQLParser.ColumnExprFunctionContext context)
        {
            if (context.columnArgList == null) {
                return List.of();
            }
            List<HogQLParser.ColumnExprContext> arguments = context.columnArgList.columnExpr();
            if (arguments.size() == 1 && isUnqualifiedStar(arguments.getFirst())) {
                if (context.DISTINCT() != null) {
                    throw unsupported(context, "DISTINCT star function");
                }
                return List.of();
            }
            return arguments.stream()
                    .map(this::buildExpression)
                    .toList();
        }

        private static boolean isUnqualifiedStar(HogQLParser.ColumnExprContext context)
        {
            return context instanceof HogQLParser.ColumnExprValuePassthroughContext passthrough &&
                    passthrough.columnExprValue() instanceof HogQLParser.ColumnExprAsteriskContext asterisk &&
                    asterisk.tableIdentifier() == null &&
                    asterisk.EXCLUDE() == null;
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
            if (value.matches("[+-]?[0-9]+")) {
                if (value.startsWith("-")) {
                    return new Literal(INTEGER, "-" + normalizeInteger(value.substring(1)), sourceSpan(context));
                }
                if (value.startsWith("+")) {
                    Literal magnitude = new Literal(INTEGER, normalizeInteger(value.substring(1)), sourceSpan(context));
                    return new UnaryExpression(POSITIVE, magnitude, sourceSpan(context));
                }
                return new Literal(INTEGER, normalizeInteger(value), sourceSpan(context));
            }
            if (!value.matches("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?")) {
                throw unsupported(context, "numeric literal");
            }
            if (value.startsWith("-")) {
                return new Literal(HogQlQuery.LiteralKind.FLOAT, value, sourceSpan(context));
            }
            if (value.startsWith("+")) {
                Literal magnitude = new Literal(HogQlQuery.LiteralKind.FLOAT, value.substring(1), sourceSpan(context));
                return new UnaryExpression(POSITIVE, magnitude, sourceSpan(context));
            }
            return new Literal(HogQlQuery.LiteralKind.FLOAT, value, sourceSpan(context));
        }

        private Expression buildColumnReference(HogQLParser.ColumnIdentifierContext context)
        {
            if (context.placeholder() != null) {
                return buildPlaceholder(context.placeholder());
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
            if (parts.size() == 1) {
                String name = canonicalName(parts.getFirst());
                for (Map<String, Expression> scope : withExpressionScopes) {
                    Expression expression = scope.get(name);
                    if (expression != null) {
                        return expression;
                    }
                }
            }
            return new ColumnReference(parts, sourceSpan(context));
        }

        private Placeholder buildPlaceholder(HogQLParser.PlaceholderContext context)
        {
            Expression expression = buildExpression(context.columnExpr());
            if (!(expression instanceof ColumnReference reference)) {
                throw unsupported(context, "non-name placeholder");
            }
            if (reference.parts().size() == 1) {
                return new Placeholder(reference.parts().getFirst().value(), sourceSpan(context));
            }
            if (reference.parts().size() == 2 &&
                    (reference.parts().getFirst().value().equalsIgnoreCase("variables") ||
                            reference.parts().getFirst().value().equalsIgnoreCase("filters"))) {
                return new Placeholder(
                        (reference.parts().getFirst().value().equalsIgnoreCase("variables") ? "variables." : "filters.") + reference.parts().getLast().value(),
                        sourceSpan(context));
            }
            throw unsupported(context, "non-name placeholder");
        }

        private Relation buildRelation(HogQLParser.JoinExprContext context)
        {
            if (context instanceof HogQLParser.JoinExprTableContext table) {
                if (table.FINAL() != null || table.sampleClause() != null) {
                    throw unsupported(context, "HogQL-specific table modifier");
                }
                return buildTableExpression(table.tableExpr());
            }
            if (context instanceof HogQLParser.JoinExprParensContext parens) {
                return buildRelation(parens.joinExpr());
            }
            if (context instanceof HogQLParser.JoinExprCrossOpContext cross) {
                if (cross.joinOpCross().COMMA() != null) {
                    throw unsupported(context, "implicit comma join");
                }
                return new JoinRelation(
                        JoinType.CROSS,
                        buildRelation(cross.joinExpr(0)),
                        buildRelation(cross.joinExpr(1)),
                        Optional.empty(),
                        sourceSpan(context));
            }
            if (context instanceof HogQLParser.JoinExprOpContext join) {
                if (join.NATURAL() != null) {
                    throw unsupported(context, "natural join");
                }
                if (join.joinConstraintClause() == null) {
                    throw unsupported(context, "join without ON or USING");
                }
                return new JoinRelation(
                        buildJoinType(join.joinOp()),
                        buildRelation(join.joinExpr(0)),
                        buildRelation(join.joinExpr(1)),
                        Optional.of(buildJoinCriteria(join.joinConstraintClause())),
                        sourceSpan(context));
            }
            if (context instanceof HogQLParser.JoinExprPivotContext pivot) {
                return buildPivot(
                        buildRelation(pivot.joinExpr()),
                        pivot.columnExprList(),
                        pivot.pivotColumnList(),
                        pivot.GROUP() != null,
                        context);
            }
            throw unsupported(context, "HogQL-specific join variant");
        }

        private Relation buildTableExpression(HogQLParser.TableExprContext context)
        {
            if (context instanceof HogQLParser.TableExprIdentifierContext identifier) {
                List<Identifier> parts = buildIdentifiers(identifier.tableIdentifier());
                if (parts.size() == 1) {
                    String name = canonicalName(parts.getFirst());
                    switch (resolveCommonTableName(name)) {
                        case PROHIBITED -> throw unsupported(context, "recursive CTE reference");
                        case VISIBLE -> {
                            return new CommonTableReference(parts.getFirst(), sourceSpan(context));
                        }
                        case ABSENT -> {}
                    }
                }
                return new TableReference(parts, sourceSpan(context));
            }
            if (context instanceof HogQLParser.TableExprFunctionContext function) {
                Identifier name = buildIdentifier(function.tableFunctionExpr().identifier());
                if (!canonicalName(name).equals("numbers")) {
                    throw unsupported(context, "table expression");
                }
                List<HogQLParser.ColumnExprContext> arguments = function.tableFunctionExpr().tableArgList() == null
                        ? List.of()
                        : function.tableFunctionExpr().tableArgList().columnExpr();
                if (arguments.size() != 1) {
                    throw unsupported(context, "numbers table function arity");
                }
                SourceSpan span = sourceSpan(context);
                FunctionCall range = new FunctionCall(
                        new Identifier("range", false, span),
                        List.of(buildExpression(arguments.getFirst())),
                        false,
                        List.of(),
                        Optional.empty(),
                        span);
                return new UnnestRelation(
                        List.of(range),
                        new Identifier("numbers", false, span),
                        List.of(new Identifier("number", false, span)),
                        span);
            }
            if (context instanceof HogQLParser.TableExprPlaceholderContext placeholder) {
                return new TablePlaceholder(buildPlaceholder(placeholder.placeholder()));
            }
            if (context instanceof HogQLParser.TableExprSubqueryContext subquery) {
                return new SubqueryRelation(buildSetQuery(subquery.selectSetStmt()), sourceSpan(context));
            }
            if (context instanceof HogQLParser.TableExprValuesContext values) {
                List<List<Expression>> rows = values.valuesClause().valuesRow().stream()
                        .map(row -> row.columnExpr().stream()
                                .map(this::buildExpression)
                                .toList())
                        .toList();
                int width = rows.getFirst().size();
                for (int index = 1; index < rows.size(); index++) {
                    if (rows.get(index).size() != width) {
                        throw unsupported(values.valuesClause().valuesRow(index), "VALUES rows with different column counts");
                    }
                }
                return new ValuesRelation(rows, sourceSpan(context));
            }
            if (context instanceof HogQLParser.TableExprPivotContext pivot) {
                return buildPivot(
                        buildTableExpression(pivot.tableExpr()),
                        pivot.columnExprList(),
                        pivot.pivotColumnList(),
                        pivot.GROUP() != null,
                        context);
            }
            if (context instanceof HogQLParser.TableExprAliasContext alias) {
                Relation relation = buildTableExpression(alias.tableExpr());
                Identifier identifier = alias.identifier() != null
                        ? buildIdentifier(alias.identifier())
                        : buildIdentifier(alias.alias().getText(), alias.alias());
                List<Identifier> columnAliases = alias.columnAliases() == null
                        ? List.of()
                        : alias.columnAliases().identifier().stream()
                          .map(this::buildIdentifier)
                          .toList();
                if (relation instanceof ValuesRelation values && !columnAliases.isEmpty() && columnAliases.size() != values.columnCount()) {
                    throw unsupported(alias.columnAliases(), "VALUES column alias count");
                }
                return new AliasedRelation(relation, identifier, columnAliases, sourceSpan(context));
            }
            throw unsupported(context, "table expression");
        }

        private PivotRelation buildPivot(
                Relation input,
                List<HogQLParser.ColumnExprListContext> expressionLists,
                HogQLParser.PivotColumnListContext pivotColumnList,
                boolean hasGroupBy,
                ParserRuleContext context)
        {
            List<PivotAggregation> aggregations = expressionLists.getFirst().columnExpr().stream()
                    .map(this::buildPivotAggregation)
                    .toList();
            List<HogQLParser.PivotColumnContext> pivotColumns = pivotColumnList.pivotColumn();
            if (pivotColumns.size() != 1) {
                throw unsupported(pivotColumns.get(1), "multiple PIVOT column clauses");
            }
            HogQLParser.PivotColumnContext pivotColumn = pivotColumns.getFirst();
            List<Expression> keys = buildPivotKeys(pivotColumn.columnExprTupleOrSingle());
            List<PivotValueGroup> valueGroups = pivotColumn.columnExprList().columnExpr().stream()
                    .map(this::buildPivotValueGroup)
                    .toList();
            List<Expression> groupBy = hasGroupBy
                    ? expressionLists.get(1).columnExpr().stream().map(this::buildExpression).toList()
                    : List.of();
            return new PivotRelation(input, aggregations, keys, valueGroups, groupBy, sourceSpan(context));
        }

        private PivotAggregation buildPivotAggregation(HogQLParser.ColumnExprContext context)
        {
            PivotExpression expression = buildPivotExpression(context);
            return new PivotAggregation(expression.expression(), expression.alias(), sourceSpan(context));
        }

        private List<Expression> buildPivotKeys(HogQLParser.ColumnExprTupleOrSingleContext context)
        {
            List<HogQLParser.ColumnExprContext> expressions = context.columnExprList() == null
                    ? List.of(context.columnExpr())
                    : context.columnExprList().columnExpr();
            return expressions.stream()
                    .map(expression -> {
                        Expression key = buildExpression(expression);
                        if (!(key instanceof ColumnReference)) {
                            throw unsupported(expression, "non-column PIVOT key");
                        }
                        return key;
                    })
                    .toList();
        }

        private PivotValueGroup buildPivotValueGroup(HogQLParser.ColumnExprContext context)
        {
            PivotExpression expression = buildPivotExpression(context);
            List<Expression> values = expression.expression() instanceof TupleExpression tuple
                    ? tuple.values()
                    : List.of(expression.expression());
            return new PivotValueGroup(values, expression.alias(), sourceSpan(context));
        }

        private PivotExpression buildPivotExpression(HogQLParser.ColumnExprContext context)
        {
            if (context instanceof HogQLParser.ColumnExprAliasContext alias) {
                Identifier identifier = alias.identifier() != null
                        ? buildIdentifier(alias.identifier())
                        : new Identifier(
                        decodeQuoted(alias.STRING_LITERAL().getText()),
                        true,
                        sourceSpan(alias.STRING_LITERAL().getSymbol(), alias.STRING_LITERAL().getSymbol()));
                return new PivotExpression(buildExpression(alias.columnExpr()), Optional.of(identifier));
            }
            return new PivotExpression(buildExpression(context), Optional.empty());
        }

        private record PivotExpression(Expression expression, Optional<Identifier> alias) {}

        private CommonTableNameResolution resolveCommonTableName(String name)
        {
            Iterator<Set<String>> visibleScopes = commonTableScopes.iterator();
            Iterator<Set<String>> prohibitedScopes = prohibitedCommonTableScopes.iterator();
            while (visibleScopes.hasNext()) {
                Set<String> visibleNames = visibleScopes.next();
                Set<String> prohibitedNames = prohibitedScopes.next();
                if (prohibitedNames.contains(name)) {
                    return CommonTableNameResolution.PROHIBITED;
                }
                if (visibleNames.contains(name)) {
                    return CommonTableNameResolution.VISIBLE;
                }
            }
            return CommonTableNameResolution.ABSENT;
        }

        private enum CommonTableNameResolution
        {
            ABSENT,
            PROHIBITED,
            VISIBLE,
        }

        private static String canonicalName(Identifier identifier)
        {
            return identifier.delimited() ? identifier.value() : identifier.value().toLowerCase(Locale.ENGLISH);
        }

        private void validateUncorrelatedSubqueries(HogQlQuery query)
        {
            validateQueryScope(query, Set.of());
        }

        private void validateQueryScope(HogQlQuery query, Set<String> forbiddenOuterRelations)
        {
            query.with().forEach(commonTable -> validateQueryScope(commonTable.query(), forbiddenOuterRelations));
            if (query.body() instanceof SetOperation setOperation) {
                query.orderBy().forEach(item -> validateExpressionScope(item.expression(), forbiddenOuterRelations, Set.of("__hogql_set_output")));
                query.limit().ifPresent(expression -> validateExpressionScope(expression, forbiddenOuterRelations, Set.of()));
                query.offset().ifPresent(expression -> validateExpressionScope(expression, forbiddenOuterRelations, Set.of()));
                validateQueryScope(setOperation.left(), forbiddenOuterRelations);
                validateQueryScope(setOperation.right(), forbiddenOuterRelations);
                return;
            }

            Set<String> localRelations = relationNames(query.from());
            boolean hasLocalRelation = query.from().isPresent();
            query.projections().forEach(projection -> {
                if (projection instanceof ExpressionProjection expression) {
                    validateExpressionScope(expression.expression(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                }
            });
            query.where().ifPresent(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
            query.groupBy().forEach(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
            query.having().ifPresent(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
            query.orderBy().forEach(item -> validateExpressionScope(item.expression(), forbiddenOuterRelations, localRelations, hasLocalRelation));
            query.limit().ifPresent(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
            query.offset().ifPresent(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
            query.from().ifPresent(relation -> validateRelationExpressionScopes(relation, forbiddenOuterRelations, localRelations, hasLocalRelation));

            Set<String> nestedForbiddenRelations = new LinkedHashSet<>(forbiddenOuterRelations);
            nestedForbiddenRelations.addAll(localRelations);
            query.from().ifPresent(relation -> validateNestedRelationScopes(relation, Set.copyOf(nestedForbiddenRelations)));
        }

        private void validateRelationExpressionScopes(Relation relation, Set<String> forbiddenOuterRelations, Set<String> localRelations, boolean hasLocalRelation)
        {
            switch (relation) {
                case AliasedRelation alias -> validateRelationExpressionScopes(alias.relation(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                case JoinRelation join -> {
                    validateRelationExpressionScopes(join.left(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateRelationExpressionScopes(join.right(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    join.criteria().ifPresent(criteria -> {
                        if (criteria instanceof JoinOn on) {
                            validateExpressionScope(on.expression(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                        }
                    });
                }
                case PivotRelation pivot -> {
                    validateRelationExpressionScopes(pivot.input(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    pivot.aggregations().forEach(aggregation ->
                            validateExpressionScope(aggregation.expression(), forbiddenOuterRelations, localRelations, hasLocalRelation));
                    pivot.pivotColumns().forEach(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
                    pivot.valueGroups().forEach(group -> group.values().forEach(expression ->
                            validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation)));
                    pivot.groupBy().forEach(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
                }
                case UnnestRelation unnest -> unnest.expressions().forEach(expression -> validateExpressionScope(expression, forbiddenOuterRelations, localRelations, hasLocalRelation));
                case CommonTableReference _, SubqueryRelation _, TablePlaceholder _, TableReference _, ValuesRelation _ -> {}
            }
        }

        private void validateNestedRelationScopes(Relation relation, Set<String> forbiddenOuterRelations)
        {
            switch (relation) {
                case AliasedRelation alias -> validateNestedRelationScopes(alias.relation(), forbiddenOuterRelations);
                case JoinRelation join -> {
                    validateNestedRelationScopes(join.left(), forbiddenOuterRelations);
                    validateNestedRelationScopes(join.right(), forbiddenOuterRelations);
                }
                case PivotRelation pivot -> validateNestedRelationScopes(pivot.input(), forbiddenOuterRelations);
                case SubqueryRelation subquery -> validateQueryScope(subquery.query(), forbiddenOuterRelations);
                case UnnestRelation _ -> {}
                case ValuesRelation values -> values.rows().forEach(row -> row.forEach(expression -> validateExpressionScope(expression, forbiddenOuterRelations, Set.of())));
                case CommonTableReference _, TablePlaceholder _, TableReference _ -> {}
            }
        }

        private Set<String> relationNames(Optional<Relation> relation)
        {
            Set<String> names = new LinkedHashSet<>();
            relation.ifPresent(value -> collectRelationNames(value, names));
            return Set.copyOf(names);
        }

        private void collectRelationNames(Relation relation, Set<String> names)
        {
            switch (relation) {
                case AliasedRelation alias -> names.add(canonicalName(alias.alias()));
                case CommonTableReference commonTable -> names.add(canonicalName(commonTable.name()));
                case JoinRelation join -> {
                    collectRelationNames(join.left(), names);
                    collectRelationNames(join.right(), names);
                }
                case PivotRelation pivot -> collectRelationNames(pivot.input(), names);
                case UnnestRelation unnest -> names.add(canonicalName(unnest.alias()));
                case SubqueryRelation _, TablePlaceholder _, ValuesRelation _ -> {}
                case TableReference table -> names.add(canonicalName(table.parts().getLast()));
            }
        }

        private void validateExpressionScope(Expression expression, Set<String> forbiddenOuterRelations, Set<String> localRelations)
        {
            validateExpressionScope(expression, forbiddenOuterRelations, localRelations, !localRelations.isEmpty());
        }

        private void validateExpressionScope(Expression expression, Set<String> forbiddenOuterRelations, Set<String> localRelations, boolean hasLocalRelation)
        {
            switch (expression) {
                case ArrayExpression array -> array.values().forEach(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                case BetweenExpression between -> {
                    validateExpressionScope(between.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateExpressionScope(between.min(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateExpressionScope(between.max(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                }
                case BinaryExpression binary -> {
                    validateExpressionScope(binary.left(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateExpressionScope(binary.right(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                }
                case CaseExpression caseExpression -> {
                    caseExpression.operand().ifPresent(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                    caseExpression.whenClauses().forEach(when -> {
                        validateExpressionScope(when.operand(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                        validateExpressionScope(when.result(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    });
                    caseExpression.defaultValue().ifPresent(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                }
                case CastExpression cast -> validateExpressionScope(cast.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                case ColumnReference reference -> {
                    if (reference.parts().size() == 1 && !forbiddenOuterRelations.isEmpty() && !hasLocalRelation) {
                        throw unsupported(reference.span(), "correlated relation subquery");
                    }
                    if (reference.parts().size() > 1) {
                        String qualifier = canonicalName(reference.parts().getFirst());
                        if (forbiddenOuterRelations.contains(qualifier) && !localRelations.contains(qualifier)) {
                            throw unsupported(reference.span(), "correlated relation subquery");
                        }
                    }
                }
                case FunctionCall function -> {
                    function.arguments().forEach(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                    function.orderBy().forEach(item -> validateExpressionScope(item.expression(), forbiddenOuterRelations, localRelations, hasLocalRelation));
                    function.filter().ifPresent(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                }
                case InExpression in -> {
                    validateExpressionScope(in.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    in.values().forEach(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                }
                case InCohortExpression in -> {
                    validateExpressionScope(in.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateExpressionScope(in.cohort(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                }
                case InSubqueryExpression in -> {
                    validateExpressionScope(in.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateQueryScope(in.query(), Set.of());
                }
                case IntervalExpression interval -> validateExpressionScope(interval.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                case IsNullExpression isNull -> validateExpressionScope(isNull.value(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                case LambdaExpression lambda -> validateExpressionScope(lambda.body(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                case Literal _, Placeholder _ -> {}
                case MemberAccessExpression memberAccess -> validateExpressionScope(memberAccess.base(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                case ScalarSubqueryExpression subquery -> validateQueryScope(subquery.query(), Set.of());
                case SubscriptExpression subscript -> {
                    validateExpressionScope(subscript.base(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                    validateExpressionScope(subscript.index(), forbiddenOuterRelations, localRelations, hasLocalRelation);
                }
                case TupleExpression tuple -> tuple.values().forEach(value -> validateExpressionScope(value, forbiddenOuterRelations, localRelations, hasLocalRelation));
                case UnaryExpression unary -> validateExpressionScope(unary.operand(), forbiddenOuterRelations, localRelations, hasLocalRelation);
            }
        }

        private JoinType buildJoinType(HogQLParser.JoinOpContext context)
        {
            if (context == null) {
                return JoinType.INNER;
            }
            if (context instanceof HogQLParser.JoinOpInnerContext inner) {
                if (inner.ANTI() != null || inner.SEMI() != null || inner.ASOF() != null || inner.ALL() != null) {
                    throw unsupported(context, "HogQL-specific inner join modifier");
                }
                return inner.ANY() == null ? JoinType.INNER : JoinType.INNER_ANY;
            }
            if (context instanceof HogQLParser.JoinOpLeftRightContext outer) {
                if (outer.ANTI() != null || outer.SEMI() != null || outer.ASOF() != null || outer.ALL() != null) {
                    throw unsupported(context, "HogQL-specific outer join modifier");
                }
                if (outer.ANY() != null) {
                    if (outer.LEFT() == null) {
                        throw unsupported(context, "RIGHT ANY JOIN");
                    }
                    return JoinType.LEFT_ANY;
                }
                return outer.LEFT() != null ? JoinType.LEFT : JoinType.RIGHT;
            }
            if (context instanceof HogQLParser.JoinOpFullContext full) {
                if (full.ASOF() != null || full.ALL() != null || full.ANY() != null) {
                    throw unsupported(context, "HogQL-specific full join modifier");
                }
                return JoinType.FULL;
            }
            throw unsupported(context, "join operator");
        }

        private JoinCriteria buildJoinCriteria(HogQLParser.JoinConstraintClauseContext context)
        {
            if (context.ON() != null) {
                List<HogQLParser.ColumnExprContext> expressions = context.columnExprList().columnExpr();
                if (expressions.size() != 1) {
                    throw unsupported(context, "multiple ON expressions");
                }
                return new JoinOn(buildExpression(expressions.getFirst()), sourceSpan(context));
            }
            List<Identifier> columns = context.columnExprList().columnExpr().stream()
                    .map(this::buildExpression)
                    .map(expression -> {
                        if (!(expression instanceof ColumnReference reference) || reference.parts().size() != 1) {
                            throw unsupported(context, "non-identifier USING column");
                        }
                        return reference.parts().getFirst();
                    })
                    .toList();
            return new JoinUsing(columns, sourceSpan(context));
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
            return unsupported(sourceSpan(context), feature);
        }

        private HogQlParsingException unsupported(SourceSpan span, String feature)
        {
            return new HogQlParsingException("HogQL feature is not lowered yet: " + feature, null, span.startLine(), span.startColumn());
        }

        private HogQlParsingException intervalError(ParserRuleContext context, String message)
        {
            SourceSpan span = sourceSpan(context);
            return new HogQlParsingException(message, null, span.startLine(), span.startColumn());
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
