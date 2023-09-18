/*
   This is a lixical specification for expression scanner.
   Class 'ExpressionScanner' is generated at pre-compile phase and used then by 'ExpressionValidator' class.

   WARNING: Lexical rules part must be in synch with C++ flex specification file (bmqeval_simpleevaluatorscanner.l).

   For JFlex syntax see https://jflex.de/manual.html
*/

package com.bloomberg.bmq.impl.infr.util.expressionvalidator;

%%

%public
%class ExpressionScanner

%unicode

%type Token

%char

%%

"true" {
	return new Token(Token.Type.BOOL, yytext(), yychar);
}

"false" {
	return new Token(Token.Type.BOOL, yytext(), yychar);
}

[a-zA-Z][a-zA-Z0-9_]* {
	return new Token(Token.Type.PROPERTY, yytext(), yychar);
}

-?[0-9]+ {
	return new Token(Token.Type.INTEGER, yytext(), yychar);
}

\"([^\\\"]|\\.)*\" {
	return new Token(Token.Type.STRING, yytext(), yychar);
}

\( {
	return new Token(Token.Type.LPAR, yychar);
}

\) {
	return new Token(Token.Type.RPAR, yychar);
}

\|\| {
	return new Token(Token.Type.LOGICAL_OP, yytext(), yychar);
}

&& {
	return new Token(Token.Type.LOGICAL_OP, yytext(), yychar);
}

== {
	return new Token(Token.Type.COMPAR_OP, yytext(), yychar);
}

"!=" {
	return new Token(Token.Type.COMPAR_OP, yytext(), yychar);
}

"<" {
	return new Token(Token.Type.COMPAR_OP, yytext(), yychar);
}

"<=" {
	return new Token(Token.Type.COMPAR_OP, yytext(), yychar);
}

">" {
	return new Token(Token.Type.COMPAR_OP, yytext(), yychar);
}

">=" {
	return new Token(Token.Type.COMPAR_OP, yytext(), yychar);
}

[!~] {
	return new Token(Token.Type.LOGICAL_NOT_OP, yytext(), yychar);
}

"+" {
	return new Token(Token.Type.MATH_OP, yytext(), yychar);
}

"-" {
	return new Token(Token.Type.MATH_OP, yytext(), yychar);
}

"*" {
	return new Token(Token.Type.MATH_OP, yytext(), yychar);
}

"/" {
	return new Token(Token.Type.MATH_OP, yytext(), yychar);
}

"%" {
	return new Token(Token.Type.MATH_OP, yytext(), yychar);
}

[ \t\n] {
}

. {
	return new Token(Token.Type.INVALID, yytext(), yychar);
}

<<EOF>> {
	return new Token(Token.Type.END, yychar);
}
