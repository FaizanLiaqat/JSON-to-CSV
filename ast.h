// ast.h
#ifndef AST_H
#define AST_H

#include <stdio.h> // For FILE* in TableSchema, though it's more of a schema_csv concern

// Enum for JSON value types
typedef enum
{
    JSON_NULL_TYPE,
    JSON_BOOLEAN_TYPE,
    JSON_NUMBER_TYPE,
    JSON_STRING_TYPE,
    JSON_ARRAY_TYPE,
    JSON_OBJECT_TYPE
} JsonValueType;

// Forward declarations
struct JsonValue;
struct JsonObject;
struct JsonArray;
struct Pair;
struct PairNode;  // Linked list node for key-value pairs in an object
struct ValueNode; // Linked list node for values in an array

// Structure for a key-value pair in an object
typedef struct Pair
{
    char *key;               // String key (unescaped, null-terminated)
    struct JsonValue *value; // Value associated with the key
} Pair;

// Linked list node for pairs (members of an object)
typedef struct PairNode
{
    Pair data;
    struct PairNode *next;
} PairNode;

// Linked list node for values (elements of an array)
typedef struct ValueNode
{
    struct JsonValue *value;
    struct ValueNode *next;
} ValueNode;

// Structure for a JSON Object
typedef struct JsonObject
{
    PairNode *head; // Head of the linked list of pairs
    int num_members;
} JsonObject;

// Structure for a JSON Array
typedef struct JsonArray
{
    ValueNode *head; // Head of the linked list of values
    int num_elements;
} JsonArray;

// Generic JSON value structure
typedef struct JsonValue
{
    JsonValueType type;
    union
    {
        int bool_val;          // For JSON_BOOLEAN_TYPE
        double num_val;        // For JSON_NUMBER_TYPE
        char *string_val;      // For JSON_STRING_TYPE (unescaped, null-terminated)
        JsonArray array_val;   // For JSON_ARRAY_TYPE
        JsonObject object_val; // For JSON_OBJECT_TYPE
    } data;
} JsonValue;

// --- AST Node Creation Functions (Prototypes) ---
JsonValue *ast_create_null();
JsonValue *ast_create_boolean(int val);
JsonValue *ast_create_number(double val);
JsonValue *ast_create_number_from_string(const char *s_val);
JsonValue *ast_create_string(char *s_val); // Takes ownership of s_val (which should be heap-allocated and unescaped)
JsonValue *ast_create_array();
JsonValue *ast_create_object();

void ast_array_append(JsonValue *array_val, JsonValue *element_val);
void ast_object_add_member(JsonValue *object_val, char *key, JsonValue *member_val); // key is duplicated, member_val is adopted

// --- AST Utility Functions (Prototypes) ---
void ast_free_value(JsonValue *val);
void ast_print_value(const JsonValue *val, int indent_level);

// Helper for string unescaping (used by lexer or parser actions)
char *unescape_json_string(const char *input_str, int length_with_quotes);

#endif // AST_H