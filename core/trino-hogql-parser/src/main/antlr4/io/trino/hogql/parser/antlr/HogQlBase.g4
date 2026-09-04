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

grammar HogQlBase;

options { caseInsensitive = true; }

singleStatement
    : query SEMICOLON? EOF
    ;

query
    : SELECT projection (COMMA projection)* (FROM qualifiedName)?
    ;

projection
    : ASTERISK
    | expression
    ;

expression
    : literal
    | qualifiedName
    ;

literal
    : INTEGER_VALUE
    | STRING
    | TRUE
    | FALSE
    | NULL
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

identifier
    : IDENTIFIER
    | QUOTED_IDENTIFIER
    | BACKQUOTED_IDENTIFIER
    ;

SELECT: 'SELECT';
FROM: 'FROM';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULL: 'NULL';

ASTERISK: '*';
COMMA: ',';
DOT: '.';
SEMICOLON: ';';

STRING
    : '\'' (~['\\] | '\'\'')* '\''
    ;

INTEGER_VALUE
    : '0'
    | [1-9] DIGIT*
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '$')*
    ;

QUOTED_IDENTIFIER
    : '"' (~'"' | '""')+ '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' (~'`' | '``')+ '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

WHITESPACE
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;
