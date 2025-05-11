/* parser.y */
%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ast.h" /* AST definitions and creation functions */


/* External declarations from Flex */
extern int yylex(void);
extern int yylineno;

/* Function to report errors */
void yyerror(const char *s);

/* Root of the AST */
JsonValue* ast_root = NULL;
%}

/* Define the types that semantic values ($$, $1, $2, etc.) can have */
%union {
    char* string_val;       /* For strings and numbers from lexer */
    int bool_val;           /* For true/false */
    JsonValue* json_value;  /* Generic JSON value node */
    struct {                /* For object members (key-value pair list) */
        char* key;
        JsonValue* value;
    } member_pair;
    JsonValue* elements_list; /* For array elements (actually just the array JsonValue itself) */
}

/* Bison's way to declare that location objects are used. */
%locations

/* Define tokens from the lexer */
%token TOKEN_LBRACE "{"
%token TOKEN_RBRACE "}"
%token TOKEN_LBRACKET "["
%token TOKEN_RBRACKET "]"
%token TOKEN_COMMA ","
%token TOKEN_COLON ":"
%token TOKEN_NULL "null"

/* Tokens with values (specify their type from the %union) */
%token <string_val> TOKEN_STRING
%token <string_val> TOKEN_NUMBER /* Numbers are passed as strings from lexer */
%token <bool_val> TOKEN_TRUE "true"
%token <bool_val> TOKEN_FALSE "false"

/* Define non-terminals that will have a value (typically AST nodes) */
%type <json_value> value
%type <json_value> object
%type <json_value> array
%type <member_pair> pair      /* A single key-value pair */

%type <json_value> members    /* <-- ADD THIS LINE */
%type <json_value> elements   /* <-- ADD THIS LINE */
/* No specific type for members or elements; they directly build object/array nodes */

/* Define the start symbol of the grammar */
%start json_document

%%

json_document: value
                { ast_root = $1; }
              ;

value: object
        { $$ = $1; }
     | array
        { $$ = $1; }
     | TOKEN_STRING
        { $$ = ast_create_string($1); /* $1 is char* (unescaped string from lexer) */ }
     | TOKEN_NUMBER
        { $$ = ast_create_number_from_string($1); free($1); /* $1 is char* from lexer */ }
     | TOKEN_TRUE
        { $$ = ast_create_boolean($1); /* $1 is yylval.bool_val */ }
     | TOKEN_FALSE
        { $$ = ast_create_boolean($1); /* $1 is yylval.bool_val */ }
     | TOKEN_NULL
        { $$ = ast_create_null(); }
     ;

object: "{" "}"
        { $$ = ast_create_object(); }
      | "{" members "}"
        { $$ = $2; /* members non-terminal returns the constructed object JsonValue* */ }
      ;

members: pair
        {
            $$ = ast_create_object();
            ast_object_add_member($$, $1.key, $1.value); /* $1.key ownership transferred */
        }
       | members "," pair
        {
            $$ = $1; /* $1 is the partially built object JsonValue* */
            ast_object_add_member($$, $3.key, $3.value); /* $3.key ownership transferred */
        }
       ;

pair: TOKEN_STRING ":" value
    {
        $$.key = $1;       /* $1 is char* (unescaped string from lexer for key) */
        $$.value = $3;     /* $3 is JsonValue* for the value */
    }
    ;

array: "[" "]"
        { $$ = ast_create_array(); }
     | "[" elements "]"
        { $$ = $2; /* elements non-terminal returns the constructed array JsonValue* */ }
     ;

elements: value
        {
            $$ = ast_create_array();
            ast_array_append($$, $1);
        }
        | elements "," value
        {
            $$ = $1; /* $1 is the partially built array JsonValue* */
            ast_array_append($$, $3);
        }
        ;

%%

/* Error reporting function */
void yyerror(const char *s) {
    fprintf(stderr, "Syntax Error: %s at line %d, column %d.\n",
            s, yylloc.first_line, yylloc.first_column);
    /* The assignment requires exiting on the first error. */
    /* ast_root might be partially built; main will handle freeing if needed */
    exit(1);
}