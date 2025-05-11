// schema_csv.c
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>   // For directory operations (checking if output_dir exists)
#include <sys/stat.h> // For mkdir
#include <errno.h>    // For errno
#include <assert.h>

#include "schema_csv.h"

TableSchema *G_all_schemas_head = NULL;
static char G_output_dir[MAX_NAME_LEN * 2]; // Store the output directory path

// Helper for malloc with error checking
static void *safe_csv_malloc(size_t size)
{
    void *ptr = malloc(size);
    if (!ptr)
    {
        perror("Error: schema_csv malloc failed");
        exit(EXIT_FAILURE);
    }
    return ptr;
}

// Helper for strdup with error checking
static char *safe_csv_strdup(const char *s)
{
    if (!s)
        return NULL;
    char *new_s = strdup(s);
    if (!new_s)
    {
        perror("Error: schema_csv strdup failed");
        exit(EXIT_FAILURE);
    }
    return new_s;
}

// Comparison function for qsort (for sorting keys to create shape signature)
static int compare_strings(const void *a, const void *b)
{
    return strcmp(*(const char **)a, *(const char **)b);
}

// Generates a unique signature for an object based on its sorted keys
static void generate_object_shape_signature(JsonObject *obj, char *signature_buffer, size_t buffer_len)
{
    if (!obj || obj->num_members == 0)
    {
        strncpy(signature_buffer, "{}", buffer_len - 1);
        signature_buffer[buffer_len - 1] = '\0';
        return;
    }

    const char *keys[obj->num_members];
    PairNode *current_member = obj->head;
    int i = 0;
    while (current_member)
    {
        keys[i++] = current_member->data.key;
        current_member = current_member->next;
    }

    qsort(keys, obj->num_members, sizeof(const char *), compare_strings);

    size_t current_len = 0;
    signature_buffer[0] = '\0';
    for (i = 0; i < obj->num_members; ++i)
    {
        strncat(signature_buffer, keys[i], buffer_len - current_len - 1);
        current_len = strlen(signature_buffer);
        if (i < obj->num_members - 1 && current_len < buffer_len - 2)
        {
            strncat(signature_buffer, ",", buffer_len - current_len - 1);
            current_len++;
        }
        if (current_len >= buffer_len - 1)
            break;
    }
}

// Forward declaration for recursive schema discovery
static void discover_schemas_recursive(JsonValue *current_json_node, const char *current_node_key_hint, TableSchema *parent_object_schema, const char *input_filename_base);

// Get existing or create a new table schema
static TableSchema *get_or_create_table(
    const char *desired_table_name_hint,
    const char *shape_sig,      // If object, its shape signature
    JsonObject *template_obj,   // If creating from an object, use its keys for columns
    TableSchema *parent_schema, // If this is a child of an array of objects (R2)
    int is_junction             // If this is a junction table for array of scalars (R3)
)
{
    // Rule R1: Reuse table if shape_sig matches (for objects)
    if (shape_sig && !is_junction && !parent_schema)
    { // Only for top-level objects or direct nested objects
        TableSchema *s = G_all_schemas_head;
        while (s)
        {
            if (strcmp(s->shape_signature, shape_sig) == 0 && !s->is_child_array_table && !s->is_junction_table)
            {
                return s; // Found existing table for this shape
            }
            s = s->next_schema;
        }
    }

    TableSchema *new_schema = (TableSchema *)safe_csv_malloc(sizeof(TableSchema));
    memset(new_schema, 0, sizeof(TableSchema));
    new_schema->current_pk_id = 0;

    // Generate a unique table name if needed
    char final_table_name[MAX_NAME_LEN];
    strncpy(final_table_name, desired_table_name_hint, MAX_NAME_LEN - 1);
    final_table_name[MAX_NAME_LEN - 1] = '\0';

    // Ensure unique table name (if multiple tables end up with same hint)
    int suffix = 1;
    TableSchema *check_s = G_all_schemas_head;
    while (check_s)
    {
        if (strcmp(check_s->name, final_table_name) == 0)
        {
            snprintf(final_table_name, MAX_NAME_LEN, "%s_%d", desired_table_name_hint, suffix++);
            check_s = G_all_schemas_head; // Restart check
            continue;
        }
        check_s = check_s->next_schema;
    }
    strncpy(new_schema->name, final_table_name, MAX_NAME_LEN - 1);

    if (shape_sig)
    {
        strncpy(new_schema->shape_signature, shape_sig, MAX_SHAPE_SIGNATURE_LEN - 1);
    }

    // Add 'id' column (Primary Key)
    strncpy(new_schema->columns[new_schema->num_columns++].name, "id", MAX_NAME_LEN - 1);

    if (parent_schema)
    { // Rule R2: Child table of array of objects
        new_schema->is_child_array_table = 1;
        snprintf(new_schema->parent_fk_column_name, MAX_NAME_LEN, "%s_id", parent_schema->name);
        strncpy(new_schema->columns[new_schema->num_columns++].name, new_schema->parent_fk_column_name, MAX_NAME_LEN - 1);
    }

    if (is_junction)
    { // Rule R3: Junction table for array of scalars
        new_schema->is_junction_table = 1;
        // Parent FK is already handled by parent_schema if R3 is inside an object.
        // If R3 is for a root array of scalars, parent_schema might be NULL,
        // then parent_fk_column_name needs to be set from desired_table_name_hint (e.g. root_values_id)
        if (parent_schema)
        { // This means the array of scalars was a field in an object
            snprintf(new_schema->parent_fk_column_name, MAX_NAME_LEN, "%s_id", parent_schema->name);
        }
        else
        {                                                                                                // This means the array of scalars was at the root or not directly tied to a parent object's row
            snprintf(new_schema->parent_fk_column_name, MAX_NAME_LEN, "%s_id", desired_table_name_hint); // e.g. "root_items_id"
        }
        strncpy(new_schema->columns[new_schema->num_columns++].name, new_schema->parent_fk_column_name, MAX_NAME_LEN - 1);
        strncpy(new_schema->columns[new_schema->num_columns++].name, "idx", MAX_NAME_LEN - 1);
        strncpy(new_schema->columns[new_schema->num_columns++].name, "value", MAX_NAME_LEN - 1);
    }
    else if (template_obj)
    { // Regular object table (R1) or child object table (R2 part)
        PairNode *member = template_obj->head;
        while (member)
        {
            if (new_schema->num_columns >= MAX_COLUMNS_PER_TABLE)
            {
                fprintf(stderr, "Warning: Max columns reached for table %s\n", new_schema->name);
                break;
            }
            // Scalar values form columns directly. Nested objects/arrays are handled by recursion.
            if (member->data.value->type == JSON_STRING_TYPE ||
                member->data.value->type == JSON_NUMBER_TYPE ||
                member->data.value->type == JSON_BOOLEAN_TYPE ||
                member->data.value->type == JSON_NULL_TYPE)
            {
                strncpy(new_schema->columns[new_schema->num_columns++].name, member->data.key, MAX_NAME_LEN - 1);
            }
            member = member->next;
        }
    }

    // Add to global list
    new_schema->next_schema = G_all_schemas_head;
    G_all_schemas_head = new_schema;
    return new_schema;
}

// First pass: discover all schemas
static void discover_schemas_recursive(JsonValue *current_json_node, const char *current_node_key_hint, TableSchema *parent_object_schema, const char *input_filename_base)
{
    if (!current_json_node)
        return;

    switch (current_json_node->type)
    {
    case JSON_OBJECT_TYPE:
    {
        char sig[MAX_SHAPE_SIGNATURE_LEN];
        generate_object_shape_signature(&(current_json_node->data.object_val), sig, sizeof(sig));

        TableSchema *current_object_table;
        if (parent_object_schema && parent_object_schema->is_child_array_table)
        {                                                // This object is an element of an array of objects (R2)
            current_object_table = parent_object_schema; // It uses the schema defined for the array elements
        }
        else
        {
            // If current_node_key_hint is null, means this is the root object
            current_object_table = get_or_create_table(
                current_node_key_hint ? current_node_key_hint : input_filename_base,
                sig,
                &(current_json_node->data.object_val),
                NULL, // Not a direct child of an array for schema *definition* purposes here, FK handled by parent_object_schema context
                0);
        }

        PairNode *member = current_json_node->data.object_val.head;
        while (member)
        {
            // Recursively discover schemas for nested structures
            discover_schemas_recursive(member->data.value, member->data.key, current_object_table, input_filename_base);
            member = member->next;
        }
        break;
    }
    case JSON_ARRAY_TYPE:
    {
        JsonArray *arr = &(current_json_node->data.array_val);
        if (arr->num_elements > 0)
        {
            JsonValue *first_element = arr->head->value;
            char child_table_name_hint[MAX_NAME_LEN];
            snprintf(child_table_name_hint, MAX_NAME_LEN, "%s_%s",
                     parent_object_schema ? parent_object_schema->name : (current_node_key_hint ? current_node_key_hint : input_filename_base),
                     current_node_key_hint ? current_node_key_hint : "items");

            if (first_element->type == JSON_OBJECT_TYPE)
            {                                      // R2: Array of objects
                char sig[MAX_SHAPE_SIGNATURE_LEN]; // Signature of objects within array
                generate_object_shape_signature(&(first_element->data.object_val), sig, sizeof(sig));

                TableSchema *child_obj_schema = get_or_create_table(child_table_name_hint, sig, &(first_element->data.object_val), parent_object_schema, 0);
                child_obj_schema->is_child_array_table = 1; // Mark it as an R2 table
                if (parent_object_schema)
                { // Set FK name based on actual parent table name
                    snprintf(child_obj_schema->parent_fk_column_name, MAX_NAME_LEN, "%s_id", parent_object_schema->name);
                    // Ensure FK column exists if not added by get_or_create_table's parent_schema logic
                    int fk_found = 0;
                    for (int k = 0; k < child_obj_schema->num_columns; ++k)
                    {
                        if (strcmp(child_obj_schema->columns[k].name, child_obj_schema->parent_fk_column_name) == 0)
                        {
                            fk_found = 1;
                            break;
                        }
                    }
                    if (!fk_found && child_obj_schema->num_columns < MAX_COLUMNS_PER_TABLE)
                    {
                        strncpy(child_obj_schema->columns[child_obj_schema->num_columns++].name, child_obj_schema->parent_fk_column_name, MAX_NAME_LEN - 1);
                    }
                }

                ValueNode *elem_node = arr->head;
                while (elem_node)
                {                                                                                                               // Discover schemas within each object of the array
                    discover_schemas_recursive(elem_node->value, current_node_key_hint, child_obj_schema, input_filename_base); // Pass child_obj_schema as its own "parent context" for R2
                    elem_node = elem_node->next;
                }
            }
            else if (first_element->type == JSON_STRING_TYPE || // R3: Array of scalars
                     first_element->type == JSON_NUMBER_TYPE ||
                     first_element->type == JSON_BOOLEAN_TYPE ||
                     first_element->type == JSON_NULL_TYPE)
            {
                TableSchema *junction_schema = get_or_create_table(child_table_name_hint, NULL, NULL, parent_object_schema, 1);
                // No further recursion needed for scalars
            }
        }
        break;
    }
    default: // Scalars, null, boolean - no schema generation from these directly beyond being columns
        break;
    }
}

// Helper to escape and quote string for CSV
static void write_csv_escaped_string(FILE *f, const char *str)
{
    if (!str || strlen(str) == 0)
    { // Handles JSON null or empty string
        fprintf(f, "");
        return;
    }

    int needs_quoting = 0;
    for (const char *p = str; *p; ++p)
    {
        if (*p == '"' || *p == ',' || *p == '\n' || *p == '\r')
        {
            needs_quoting = 1;
            break;
        }
    }

    if (needs_quoting)
        fprintf(f, "\"");
    for (const char *p = str; *p; ++p)
    {
        if (*p == '"')
        {
            fprintf(f, "\"\""); // Double quote
        }
        else
        {
            fprintf(f, "%c", *p);
        }
    }
    if (needs_quoting)
        fprintf(f, "\"");
}

// Second pass: populate CSV files
static void populate_csv_recursive(JsonValue *current_json_node, TableSchema *current_object_schema_context, long parent_pk_value)
{
    if (!current_json_node)
        return;

    switch (current_json_node->type)
    {
    case JSON_OBJECT_TYPE:
    {
        JsonObject *obj = &(current_json_node->data.object_val);
        char sig[MAX_SHAPE_SIGNATURE_LEN];
        generate_object_shape_signature(obj, sig, sizeof(sig));

        TableSchema *table_for_this_obj = NULL;
        if (current_object_schema_context && current_object_schema_context->is_child_array_table &&
            strcmp(current_object_schema_context->shape_signature, sig) == 0)
        { // This object is an element of an R2 array
            table_for_this_obj = current_object_schema_context;
        }
        else
        { // Find its schema by signature (R1 case or standalone object)
            TableSchema *s = G_all_schemas_head;
            while (s)
            {
                if (!s->is_child_array_table && !s->is_junction_table && strcmp(s->shape_signature, sig) == 0)
                {
                    table_for_this_obj = s;
                    break;
                }
                s = s->next_schema;
            }
        }

        if (!table_for_this_obj)
        {
            // This can happen if an object's shape wasn't properly schematized (e.g. root array of diverse objects - not standard relational)
            // Or if it's a nested object not part of an R2 array, its values become columns if scalar or handled by recursion.
            // For this assignment, we assume objects matching a schema are written to that schema.
            // Non-scalar members are handled by further recursion.
            PairNode *member = obj->head;
            while (member)
            {
                populate_csv_recursive(member->data.value, current_object_schema_context, parent_pk_value); // Pass current context
                member = member->next;
            }
            return;
        }

        long current_row_pk = ++(table_for_this_obj->current_pk_id);

        // Write PK
        fprintf(table_for_this_obj->file_ptr, "%ld", current_row_pk);

        // Write other columns
        for (int i = 1; i < table_for_this_obj->num_columns; ++i)
        { // Start from 1 to skip 'id'
            fprintf(table_for_this_obj->file_ptr, ",");
            const char *col_name = table_for_this_obj->columns[i].name;

            if (table_for_this_obj->is_child_array_table && strcmp(col_name, table_for_this_obj->parent_fk_column_name) == 0)
            {
                fprintf(table_for_this_obj->file_ptr, "%ld", parent_pk_value);
            }
            else
            {
                // Find corresponding key in JSON object
                JsonValue *member_val = NULL;
                PairNode *m = obj->head;
                while (m)
                {
                    if (strcmp(m->data.key, col_name) == 0)
                    {
                        member_val = m->data.value;
                        break;
                    }
                    m = m->next;
                }

                if (member_val)
                {
                    switch (member_val->type)
                    {
                    case JSON_STRING_TYPE:
                        write_csv_escaped_string(table_for_this_obj->file_ptr, member_val->data.string_val);
                        break;
                    case JSON_NUMBER_TYPE:
                        fprintf(table_for_this_obj->file_ptr, "%g", member_val->data.num_val);
                        break;
                    case JSON_BOOLEAN_TYPE:
                        fprintf(table_for_this_obj->file_ptr, "%s", member_val->data.bool_val ? "true" : "false");
                        break;
                    case JSON_NULL_TYPE:
                        fprintf(table_for_this_obj->file_ptr, "");
                        break; // R4
                    default:
                        fprintf(table_for_this_obj->file_ptr, ""); // Should not happen for direct columns
                    }
                }
                else
                {
                    fprintf(table_for_this_obj->file_ptr, ""); // Key not found in this instance, empty field
                }
            }
        }
        fprintf(table_for_this_obj->file_ptr, "\n");

        // Now recursively call for nested arrays/objects that form new tables
        PairNode *member = obj->head;
        while (member)
        {
            if (member->data.value->type == JSON_ARRAY_TYPE || member->data.value->type == JSON_OBJECT_TYPE)
            {
                // For arrays, context is this object. For nested objects not part of R2, context is also this object.
                populate_csv_recursive(member->data.value, table_for_this_obj, current_row_pk);
            }
            member = member->next;
        }
        break;
    }
    case JSON_ARRAY_TYPE:
    {
        JsonArray *arr = &(current_json_node->data.array_val);
        if (arr->num_elements == 0)
            break;

        JsonValue *first_element = arr->head->value;
        TableSchema *array_table_schema = NULL;

        // Find the schema for this array (either R2 child table or R3 junction table)
        // This relies on schemas being uniquely named and identifiable based on context
        // For R2, current_object_schema_context is the parent object's schema. The array elements form a child table.
        // For R3, current_object_schema_context is the parent object's schema. The array scalars form a junction table.

        // This part is tricky: we need to find the *specific* schema created for *this* array's elements.
        // The discovery phase should have created it. We need a way to retrieve it.
        // Let's iterate G_all_schemas_head and match based on hints if possible (e.g. parent name + key)
        // This simplified approach might require more robust schema identification.
        // For now, we assume `discover_schemas_recursive` correctly linked or allows lookup.
        // A better way: pass the key of this array to find its specific child/junction table.

        if (first_element->type == JSON_OBJECT_TYPE)
        { // R2
            char sig_first_elem[MAX_SHAPE_SIGNATURE_LEN];
            generate_object_shape_signature(&(first_element->data.object_val), sig_first_elem, sizeof(sig_first_elem));
            TableSchema *s_iter = G_all_schemas_head;
            while (s_iter)
            {
                if (s_iter->is_child_array_table && strcmp(s_iter->shape_signature, sig_first_elem) == 0)
                {
                    // Further check if this schema belongs to current_object_schema_context as parent
                    char expected_fk[MAX_NAME_LEN];
                    if (current_object_schema_context)
                    {
                        snprintf(expected_fk, MAX_NAME_LEN, "%s_id", current_object_schema_context->name);
                        if (strcmp(s_iter->parent_fk_column_name, expected_fk) == 0)
                        {
                            array_table_schema = s_iter;
                            break;
                        }
                    }
                }
                s_iter = s_iter->next_schema;
            }
            if (array_table_schema)
            {
                ValueNode *elem_node = arr->head;
                while (elem_node)
                {
                    populate_csv_recursive(elem_node->value, array_table_schema, parent_pk_value); // Pass array's table schema and parent's PK
                    elem_node = elem_node->next;
                }
            }
        }
        else
        { // R3: Array of scalars
            TableSchema *s_iter = G_all_schemas_head;
            while (s_iter)
            {
                if (s_iter->is_junction_table)
                {
                    // Check if this junction table is for the current parent context
                    char expected_fk[MAX_NAME_LEN];
                    if (current_object_schema_context)
                    {
                        snprintf(expected_fk, MAX_NAME_LEN, "%s_id", current_object_schema_context->name);
                        if (strcmp(s_iter->parent_fk_column_name, expected_fk) == 0)
                        {
                            array_table_schema = s_iter;
                            break;
                        }
                    }
                    else
                    { // Root array of scalars, match by name hint? (More complex)
                      // This scenario needs a robust naming for the junction table from discover phase.
                    }
                }
                s_iter = s_iter->next_schema;
            }

            if (array_table_schema)
            {
                ValueNode *elem_node = arr->head;
                int idx = 0;
                while (elem_node)
                {
                    fprintf(array_table_schema->file_ptr, "%ld", parent_pk_value); // Parent FK
                    fprintf(array_table_schema->file_ptr, ",%d,", idx++);          // Index
                    switch (elem_node->value->type)
                    {
                    case JSON_STRING_TYPE:
                        write_csv_escaped_string(array_table_schema->file_ptr, elem_node->value->data.string_val);
                        break;
                    case JSON_NUMBER_TYPE:
                        fprintf(array_table_schema->file_ptr, "%g", elem_node->value->data.num_val);
                        break;
                    case JSON_BOOLEAN_TYPE:
                        fprintf(array_table_schema->file_ptr, "%s", elem_node->value->data.bool_val ? "true" : "false");
                        break;
                    case JSON_NULL_TYPE:
                        fprintf(array_table_schema->file_ptr, "");
                        break;
                    default:
                        fprintf(array_table_schema->file_ptr, ""); // Should not happen
                    }
                    fprintf(array_table_schema->file_ptr, "\n");
                    elem_node = elem_node->next;
                }
            }
        }
        break;
    }
    default: // Scalars: Handled when processing object members
        break;
    }
}

void process_json_to_csv(JsonValue *root_json_value, const char *output_dir_path, const char *input_filename_base)
{
    if (!root_json_value)
        return;
    strncpy(G_output_dir, output_dir_path, sizeof(G_output_dir) - 1);
    G_output_dir[sizeof(G_output_dir) - 1] = '\0';

    // Create output directory if it doesn't exist
    struct stat st = {0};
    if (stat(G_output_dir, &st) == -1)
    {
        if (mkdir(G_output_dir, 0700) != 0 && errno != EEXIST)
        { // 0700: user rwx
            perror("Error creating output directory");
            exit(EXIT_FAILURE);
        }
    }

    // Pass 1: Discover Schemas
    discover_schemas_recursive(root_json_value, NULL, NULL, input_filename_base);

    // Open files and write headers
    TableSchema *s = G_all_schemas_head;
    if (!s)
    {
        printf("No tables generated for this JSON.\n");
        return;
    }
    while (s)
    {
        char file_path[MAX_NAME_LEN * 2 + 5]; // For path + .csv
        snprintf(file_path, sizeof(file_path), "%s/%s.csv", G_output_dir, s->name);
        s->file_ptr = fopen(file_path, "w");
        if (!s->file_ptr)
        {
            perror("Error opening CSV file for writing");
            fprintf(stderr, "Failed to open: %s\n", file_path);
            // Consider cleanup here before exiting
            exit(EXIT_FAILURE);
        }
        for (int i = 0; i < s->num_columns; ++i)
        {
            write_csv_escaped_string(s->file_ptr, s->columns[i].name);
            if (i < s->num_columns - 1)
                fprintf(s->file_ptr, ",");
        }
        fprintf(s->file_ptr, "\n");
        s = s->next_schema;
    }

    // Pass 2: Populate CSVs
    populate_csv_recursive(root_json_value, NULL, 0); // Root objects have no parent schema context / parent_pk
}

void cleanup_schemas()
{
    TableSchema *current = G_all_schemas_head;
    while (current)
    {
        TableSchema *next = current->next_schema;
        if (current->file_ptr)
        {
            fclose(current->file_ptr);
        }
        free(current);
        current = next;
    }
    G_all_schemas_head = NULL;
}