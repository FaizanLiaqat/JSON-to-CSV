// main.c
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h> // For basename

#include "ast.h"
#include "schema_csv.h" // For processing the AST
#include "parser.h"     // <--- ***** ADD THIS LINE ***** (For YYLTYPE, token definitions, etc.)

// External from parser.y (yyparse, ast_root are already effectively covered by including parser.h if it declares them,
// but explicit externs are fine too. YYLTYPE is the critical one from parser.h)
extern FILE *yyin;
// int yyparse(void); // Declaration might be in parser.h if bison generates it. Otherwise, keep.
extern JsonValue *ast_root; // <-- ADD THIS LINE
// In our parser.y, ast_root is global in parser.c, so extern is okay.
extern int yylineno;   // Provided by Flex
extern YYLTYPE yylloc; // Defined in parser.h when %locations is used

// External from scanner.l
extern int yycolumn; // To be reset for each file. Defined in scanner.l.

int main(int argc, char *argv[])
{
    char *input_filepath = NULL;
    char *output_dir = "."; // Default to current directory
    int print_ast_flag = 0;

    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <input.json> [--print-ast] [-out-dir DIR]\n", argv[0]);
        return EXIT_FAILURE;
    }
    input_filepath = argv[1];

    for (int i = 2; i < argc; i++)
    {
        if (strcmp(argv[i], "--print-ast") == 0)
        {
            print_ast_flag = 1;
        }
        else if (strcmp(argv[i], "-out-dir") == 0)
        {
            if (i + 1 < argc)
            {
                output_dir = argv[++i]; // Consume the directory argument
            }
            else
            {
                fprintf(stderr, "Error: -out-dir requires a directory argument.\n");
                return EXIT_FAILURE;
            }
        }
        else
        {
            fprintf(stderr, "Error: Unknown argument '%s'\n", argv[i]);
            fprintf(stderr, "Usage: %s <input.json> [--print-ast] [-out-dir DIR]\n", argv[0]);
            return EXIT_FAILURE;
        }
    }

    yyin = fopen(input_filepath, "r");
    if (!yyin)
    {
        perror(input_filepath);
        return EXIT_FAILURE;
    }

    // Initialize lexer location tracking
    yylineno = 1;
    yycolumn = 1; // From scanner.l global, reset for new input

    // ast_root is defined in parser.y (which becomes parser.c)
    // No need to redeclare here if it's properly externed from parser.h or directly from parser.c
    // If parser.h (from bison -d) declares `extern JsonValue* ast_root;`, it's fine.
    // If not, the `extern JsonValue* ast_root;` above is needed.
    // Our parser.y has `JsonValue* ast_root = NULL;` in the C declarations part, so it's global in parser.c.

    if (yyparse() != 0)
    {
        fprintf(stderr, "Parsing failed. Exiting.\n");
        fclose(yyin);
        if (ast_root)
            ast_free_value(ast_root);
        cleanup_schemas();
        return EXIT_FAILURE;
    }
    fclose(yyin);

    if (!ast_root)
    {
        fprintf(stderr, "Error: AST root is null after successful parsing (should not happen).\n");
        cleanup_schemas();
        return EXIT_FAILURE;
    }

    if (print_ast_flag)
    {
        printf("--- Abstract Syntax Tree ---\n");
        ast_print_value(ast_root, 0);
        printf("--------------------------\n\n");
    }

    char *input_filename_copy = strdup(input_filepath);
    if (!input_filename_copy)
    {
        perror("strdup for filename failed");
        ast_free_value(ast_root);
        cleanup_schemas();
        return EXIT_FAILURE;
    }
    char *temp_basename = basename(input_filename_copy); // basename might modify its argument or return pointer into it
    char input_filename_base[MAX_NAME_LEN];              // MAX_NAME_LEN from schema_csv.h
    strncpy(input_filename_base, temp_basename, sizeof(input_filename_base) - 1);
    input_filename_base[sizeof(input_filename_base) - 1] = '\0';
    free(input_filename_copy); // Free the copy after basename is done with it

    char *dot = strrchr(input_filename_base, '.');
    if (dot && !strchr(dot, '/'))
    {
        *dot = '\0';
    }

    printf("Processing JSON and generating CSVs into directory: %s\n", output_dir);
    process_json_to_csv(ast_root, output_dir, input_filename_base);

    printf("CSV generation process finished.\n");

    ast_free_value(ast_root);
    ast_root = NULL;
    cleanup_schemas();

    printf("Program finished successfully.\n");
    return EXIT_SUCCESS;
}