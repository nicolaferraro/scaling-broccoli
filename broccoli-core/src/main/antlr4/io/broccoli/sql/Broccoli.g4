grammar Broccoli;

sqlFile
 : ( sqlStatementList | error )* EOF
 ;

error
 : UNEXPECTED_CHAR
   {
     throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
   }
 ;

sqlStatementList
 : SCOL* sqlStatement ( SCOL+ sqlStatement )* SCOL*
 ;

sqlStatement
 : (createTableStatement | createViewStatement)
 ;

createTableStatement
 : K_CREATE K_TABLE tableName OPEN_PAR columnDefinitionList CLOSE_PAR
 ;

columnDefinitionList
 : columnDefinition (COMMA columnDefinition )*
 ;

columnDefinition
 : columnName type
 ;

createViewStatement
 : K_CREATE (K_MATERIALIZED)? K_VIEW viewName K_AS OPEN_PAR selectStatement CLOSE_PAR
 ;

selectStatement
 : K_SELECT resultColumn ( ',' resultColumn )*
   ( K_FROM tableWithOptionalAlias ( ',' tableWithOptionalAlias )* )?
   ( K_WHERE expr )?
 ;

expr
 : literalValue                                                                     # ExprLiteral
 | BIND_PARAMETER                                                                   # ExprBindParam // TODO later (do not use it in definitions)
 | ( tableName '.' )? columnName                                                    # ExprColumn
 | unaryOperator expr                                                               # ExprUnary
 | expr ( STAR | DIV | MOD ) expr                                                   # ExprMultDivMod
 | expr ( PLUS | MINUS ) expr                                                       # ExprPlusMinus
 | expr ( LT | LT_EQ | GT | GT_EQ ) expr                                            # ExprComparison
 | expr ( ASSIGN | EQ | NOT_EQ1 | NOT_EQ2 | K_IS | K_IS K_NOT | K_LIKE ) expr       # ExprEquality
 | expr K_AND expr                                                                  # ExprAnd
 | expr K_OR expr                                                                   # ExprOr
 | OPEN_PAR expr CLOSE_PAR                                                          # ExprParenthesis
 | expr ( K_ISNULL | K_NOTNULL | K_NOT K_NULL )                                     # ExprNullCheck
 ;



resultColumn
 : '*'
 | tableName '.' '*'
 | expr ( K_AS? columnAlias )?
 ;



tableWithOptionalAlias
 : tableName ( K_AS? tableAlias )?
 ;

literalValue
 : NUMERIC_LITERAL
 | STRING_LITERAL
 | K_NULL
 ;

unaryOperator
 : MINUS
 | PLUS
 | K_NOT
 ;

type
 : T_INTEGER
 | T_VARCHAR
 ;

keyword
 : K_AND
 | K_AS
 | K_BETWEEN
 | K_BY
 | K_COLUMN
 | K_CREATE
 | K_DESC
 | K_DISTINCT
 | K_EXISTS
 | K_FROM
 | K_FULL
 | K_GROUP
 | K_HAVING
 | K_IN
 | K_INDEX
 | K_INNER
 | K_IS
 | K_ISNULL
 | K_JOIN
 | K_LEFT
 | K_LIKE
 | K_LIMIT
 | K_MATERIALIZED
 | K_NATURAL
 | K_NOT
 | K_NOTNULL
 | K_NULL
 | K_ON
 | K_OR
 | K_ORDER
 | K_OUTER
 | K_RIGHT
 | K_SELECT
 | K_TABLE
 | K_UNION
 | K_VIEW
 | K_VIRTUAL
 | K_WHERE
 ;


tableName
 : IDENTIFIER
 ;

viewName
 : IDENTIFIER
 ;

tableAlias
 : IDENTIFIER
 ;

columnName
 : IDENTIFIER
 ;

columnAlias
 : IDENTIFIER
 ;

//anyName
// : IDENTIFIER
// | keyword
// | STRING_LITERAL
// | '(' anyName ')'
// ;

T_INTEGER : I N T E G E R;
T_VARCHAR : V A R C H A R;


SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
ASSIGN : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
DIV : '/';
MOD : '%';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';

// keywords
K_AND : A N D;
K_AS : A S;
K_BETWEEN : B E T W E E N;
K_BY : B Y;
K_COLUMN : C O L U M N;
K_CREATE : C R E A T E;
K_DESC : D E S C;
K_DISTINCT : D I S T I N C T;
K_EXISTS : E X I S T S;
K_FROM : F R O M;
K_FULL : F U L L;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_IN : I N;
K_INDEX : I N D E X;
K_INNER : I N N E R;
K_IS : I S;
K_ISNULL : I S N U L L;
K_JOIN : J O I N;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_LIMIT : L I M I T;
K_MATERIALIZED : M A T E R I A L I Z E D;
K_NATURAL : N A T U R A L;
K_NOT : N O T;
K_NOTNULL : N O T N U L L;
K_NULL : N U L L;
K_ON : O N;
K_OR : O R;
K_ORDER : O R D E R;
K_OUTER : O U T E R;
K_RIGHT : R I G H T;
K_SELECT : S E L E C T;
K_TABLE : T A B L E;
K_UNION : U N I O N;
K_VIEW : V I E W;
K_VIRTUAL : V I R T U A L;
K_WHERE : W H E R E;

IDENTIFIER
 : [a-zA-Z_] [a-zA-Z_0-9]*
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

BIND_PARAMETER
 : '?' DIGIT*
 | [:] IDENTIFIER
 ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];