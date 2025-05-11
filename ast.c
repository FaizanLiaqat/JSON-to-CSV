// ast.c
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h> // For NAN, INFINITY if handling those for numbers
#include "ast.h"

// Helper for malloc with error checking
static void *safe_malloc(size_t size)
{
    void *ptr = malloc(size);
    if (!ptr)
    {
        perror("Error: malloc failed");
        exit(EXIT_FAILURE);
    }
    return ptr;
}

// Helper for strdup with error checking
static char *safe_strdup(const char *s)
{
    if (!s)
        return NULL;
    char *new_s = strdup(s);
    if (!new_s)
    {
        perror("Error: strdup failed");
        exit(EXIT_FAILURE);
    }
    return new_s;
}

JsonValue *ast_create_null()
{
    JsonValue *val = (JsonValue *)safe_malloc(sizeof(JsonValue));
    val->type = JSON_NULL_TYPE;
    return val;
}

JsonValue *ast_create_boolean(int b_val)
{
    JsonValue *val = (JsonValue *)safe_malloc(sizeof(JsonValue));
    val->type = JSON_BOOLEAN_TYPE;
    val->data.bool_val = b_val;
    return val;
}

JsonValue *ast_create_number_from_string(const char *s_val)
{
    JsonValue *val = (JsonValue *)safe_malloc(sizeof(JsonValue));
    val->type = JSON_NUMBER_TYPE;
    val->data.num_val = atof(s_val); // Handles int, float, scientific
    return val;
}

JsonValue *ast_create_string(char *str_val)
{ // Assumes str_val is already heap-allocated and unescaped
    JsonValue *val = (JsonValue *)safe_malloc(sizeof(JsonValue));
    val->type = JSON_STRING_TYPE;
    val->data.string_val = str_val; // Takes ownership
    return val;
}

JsonValue *ast_create_array()
{
    JsonValue *val = (JsonValue *)safe_malloc(sizeof(JsonValue));
    val->type = JSON_ARRAY_TYPE;
    val->data.array_val.head = NULL;
    val->data.array_val.num_elements = 0;
    return val;
}

JsonValue *ast_create_object()
{
    JsonValue *val = (JsonValue *)safe_malloc(sizeof(JsonValue));
    val->type = JSON_OBJECT_TYPE;
    val->data.object_val.head = NULL;
    val->data.object_val.num_members = 0;
    return val;
}

void ast_array_append(JsonValue *array_val, JsonValue *element_val)
{
    if (!array_val || array_val->type != JSON_ARRAY_TYPE)
        return;

    ValueNode *new_node = (ValueNode *)safe_malloc(sizeof(ValueNode));
    new_node->value = element_val;
    new_node->next = NULL;

    if (!array_val->data.array_val.head)
    {
        array_val->data.array_val.head = new_node;
    }
    else
    {
        ValueNode *current = array_val->data.array_val.head;
        while (current->next)
        {
            current = current->next;
        }
        current->next = new_node;
    }
    array_val->data.array_val.num_elements++;
}

void ast_object_add_member(JsonValue *object_val, char *key, JsonValue *member_val)
{ // key is from lexer, unescaped
    if (!object_val || object_val->type != JSON_OBJECT_TYPE)
        return;

    PairNode *new_node = (PairNode *)safe_malloc(sizeof(PairNode));
    new_node->data.key = key; // Takes ownership of unescaped key
    new_node->data.value = member_val;
    new_node->next = NULL;

    if (!object_val->data.object_val.head)
    {
        object_val->data.object_val.head = new_node;
    }
    else
    {
        PairNode *current = object_val->data.object_val.head;
        while (current->next)
        {
            current = current->next;
        }
        current->next = new_node;
    }
    object_val->data.object_val.num_members++;
}

void ast_free_value(JsonValue *val)
{
    if (!val)
        return;

    switch (val->type)
    {
    case JSON_STRING_TYPE:
        free(val->data.string_val);
        break;
    case JSON_ARRAY_TYPE:
    {
        ValueNode *current_element = val->data.array_val.head;
        while (current_element)
        {
            ValueNode *next_element = current_element->next;
            ast_free_value(current_element->value);
            free(current_element);
            current_element = next_element;
        }
        break;
    }
    case JSON_OBJECT_TYPE:
    {
        PairNode *current_member = val->data.object_val.head;
        while (current_member)
        {
            PairNode *next_member = current_member->next;
            free(current_member->data.key);
            ast_free_value(current_member->data.value);
            free(current_member);
            current_member = next_member;
        }
        break;
    }
    case JSON_NULL_TYPE:
    case JSON_BOOLEAN_TYPE:
    case JSON_NUMBER_TYPE:
        // No heap-allocated data within the union itself
        break;
    }
    free(val);
}

static void print_indent(int level)
{
    for (int i = 0; i < level; ++i)
        printf("  ");
}

void ast_print_value(const JsonValue *val, int indent_level)
{
    if (!val)
    {
        print_indent(indent_level);
        printf("(null_ast_node)\n");
        return;
    }

    print_indent(indent_level);
    switch (val->type)
    {
    case JSON_NULL_TYPE:
        printf("NULL\n");
        break;
    case JSON_BOOLEAN_TYPE:
        printf("BOOLEAN: %s\n", val->data.bool_val ? "true" : "false");
        break;
    case JSON_NUMBER_TYPE:
        printf("NUMBER: %g\n", val->data.num_val); // %g for general float format
        break;
    case JSON_STRING_TYPE:
        printf("STRING: \"%s\"\n", val->data.string_val);
        break;
    case JSON_ARRAY_TYPE:
        printf("ARRAY (%d elements):\n", val->data.array_val.num_elements);
        ValueNode *current_element = val->data.array_val.head;
        int i = 0;
        while (current_element)
        {
            print_indent(indent_level + 1);
            printf("[%d]:\n", i++);
            ast_print_value(current_element->value, indent_level + 2);
            current_element = current_element->next;
        }
        if (val->data.array_val.num_elements == 0)
        {
            print_indent(indent_level + 1);
            printf("(empty)\n");
        }
        break;
    case JSON_OBJECT_TYPE:
        printf("OBJECT (%d members):\n", val->data.object_val.num_members);
        PairNode *current_member = val->data.object_val.head;
        while (current_member)
        {
            print_indent(indent_level + 1);
            printf("\"%s\":\n", current_member->data.key);
            ast_print_value(current_member->data.value, indent_level + 2);
            current_member = current_member->next;
        }
        if (val->data.object_val.num_members == 0)
        {
            print_indent(indent_level + 1);
            printf("(empty)\n");
        }
        break;
    default:
        printf("Unknown Type\n");
    }
}

// Unescapes a JSON string. Input is yytext (including quotes). Length includes quotes.
// Returns a new heap-allocated string (unescaped, without outer quotes).
char *unescape_json_string(const char *input_str, int length_with_quotes)
{
    if (!input_str || length_with_quotes < 2 || input_str[0] != '"' || input_str[length_with_quotes - 1] != '"')
    {
        // Should not happen if lexer rule is correct
        return safe_strdup("");
    }

    // Max possible length is length_with_quotes - 2
    char *unescaped_str = (char *)safe_malloc(length_with_quotes); // A bit more for safety, then realloc if needed
    int s_idx = 0;                                                 // Index for unescaped_str

    for (int i = 1; i < length_with_quotes - 1; ++i)
    { // Skip outer quotes
        if (input_str[i] == '\\')
        {
            i++; // Move to the character after backslash
            if (i >= length_with_quotes - 1)
                break; // Should not happen in valid JSON

            switch (input_str[i])
            {
            case '"':
                unescaped_str[s_idx++] = '"';
                break;
            case '\\':
                unescaped_str[s_idx++] = '\\';
                break;
            case '/':
                unescaped_str[s_idx++] = '/';
                break;
            case 'b':
                unescaped_str[s_idx++] = '\b';
                break;
            case 'f':
                unescaped_str[s_idx++] = '\f';
                break;
            case 'n':
                unescaped_str[s_idx++] = '\n';
                break;
            case 'r':
                unescaped_str[s_idx++] = '\r';
                break;
            case 't':
                unescaped_str[s_idx++] = '\t';
                break;
            case 'u': // Handle \uXXXX Unicode escape
                if (i + 4 < length_with_quotes - 1)
                {
                    // For this assignment, we'll pass \uXXXX as a literal sequence.
                    // A full implementation would convert this to UTF-8 bytes.
                    unescaped_str[s_idx++] = '\\';
                    unescaped_str[s_idx++] = 'u';
                    unescaped_str[s_idx++] = input_str[i + 1];
                    unescaped_str[s_idx++] = input_str[i + 2];
                    unescaped_str[s_idx++] = input_str[i + 3];
                    unescaped_str[s_idx++] = input_str[i + 4];
                    i += 4;
                }
                else
                { // Malformed \u escape, treat as literal (or error)
                    unescaped_str[s_idx++] = '\\';
                    unescaped_str[s_idx++] = 'u';
                }
                break;
            default: // Unknown escape sequence, treat as literal backslash + char
                unescaped_str[s_idx++] = '\\';
                unescaped_str[s_idx++] = input_str[i];
                break;
            }
        }
        else
        {
            unescaped_str[s_idx++] = input_str[i];
        }
    }
    unescaped_str[s_idx] = '\0';

    // Optional: realloc to actual size if memory is critical
    // char* final_str = realloc(unescaped_str, s_idx + 1);
    // if (!final_str) { free(unescaped_str); perror("realloc failed"); exit(EXIT_FAILURE); }
    // return final_str;
    return unescaped_str;
}