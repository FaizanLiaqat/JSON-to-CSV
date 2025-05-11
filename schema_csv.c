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

static int compare_strings(const void *a, const void *b)
{
    return strcmp(*(const char **)a, *(const char **)b);
}

static void generate_object_shape_signature(JsonObject *obj, char *signature_buffer, size_t buffer_len)
{
    if (!obj || obj->num_members == 0)
    {
        strncpy(signature_buffer, "{}", buffer_len - 1);
        signature_buffer[buffer_len - 1] = '\0';
        return;
    }
    if (obj->num_members > MAX_COLUMNS_PER_TABLE * 2)
    {
        fprintf(stderr, "Warning: Too many members for shape signature key array.\n");
        strncpy(signature_buffer, "{_too_many_keys_}", buffer_len - 1);
        signature_buffer[buffer_len - 1] = '\0';
        return;
    }
    const char *keys[obj->num_members];
    PairNode *current_member = obj->head;
    int i = 0;
    while (current_member && i < obj->num_members)
    {
        keys[i++] = current_member->data.key;
        current_member = current_member->next;
    }
    qsort(keys, i, sizeof(const char *), compare_strings);

    size_t current_len = 0;
    signature_buffer[0] = '\0';
    for (int j = 0; j < i; ++j)
    {
        strncat(signature_buffer, keys[j], buffer_len - current_len - 1);
        current_len = strlen(signature_buffer);
        if (j < i - 1 && current_len < buffer_len - 2)
        {
            strncat(signature_buffer, ",", buffer_len - current_len - 1);
            current_len++;
        }
        if (current_len >= buffer_len - 1)
            break;
    }
}

static void discover_schemas_recursive(JsonValue *current_json_node, const char *current_node_key_hint, TableSchema *parent_object_schema, const char *input_filename_base);
static void populate_csv_recursive(JsonValue *current_json_node, TableSchema *current_object_schema_context, long parent_pk_value, const char *json_key_of_current_node, const char *input_filename_base);

static TableSchema *get_or_create_table(
    const char *desired_table_name_hint,
    const char *shape_sig,
    JsonObject *template_obj,
    TableSchema *parent_schema,        // Parent object's schema, if this new table is for a nested structure
    int is_junction_table_flag,        // Is this an R3 junction table?
    int is_r2_array_element_table_flag // Is this a table for elements of an R2 array?
)
{
    if (shape_sig && !is_junction_table_flag && !is_r2_array_element_table_flag)
    { // R1 check for non-array-derived tables
        TableSchema *s = G_all_schemas_head;
        while (s)
        {
            if (!s->is_child_array_table && !s->is_junction_table && strcmp(s->shape_signature, shape_sig) == 0)
            {
                // If a parent_schema is provided (nested R1 object), this check might need to be more sophisticated
                // to ensure it's not grabbing an R1 table from a completely different context if shapes match.
                // For now, if shapes match and it's a base R1 type, reuse.
                if (parent_schema == NULL || (parent_schema && strcmp(s->name, desired_table_name_hint) != 0))
                { // Simple guard against reusing parent's own schema if names are too similar
                    return s;
                }
            }
            s = s->next_schema;
        }
    }

    TableSchema *new_schema = (TableSchema *)safe_csv_malloc(sizeof(TableSchema));
    memset(new_schema, 0, sizeof(TableSchema));
    new_schema->current_pk_id = 0;
    new_schema->is_junction_table = is_junction_table_flag;            // Set based on parameter
    new_schema->is_child_array_table = is_r2_array_element_table_flag; // Set based on parameter

    char final_table_name[MAX_NAME_LEN];
    strncpy(final_table_name, desired_table_name_hint, MAX_NAME_LEN - 1);
    final_table_name[MAX_NAME_LEN - 1] = '\0';

    int suffix = 1;
    TableSchema *check_s = G_all_schemas_head;
    while (check_s)
    {
        if (strcmp(check_s->name, final_table_name) == 0)
        {
            snprintf(final_table_name, MAX_NAME_LEN, "%s_%d", desired_table_name_hint, suffix++);
            check_s = G_all_schemas_head;
            continue;
        }
        check_s = check_s->next_schema;
    }
    strncpy(new_schema->name, final_table_name, MAX_NAME_LEN - 1);
    new_schema->name[MAX_NAME_LEN - 1] = '\0';

    if (shape_sig)
    {
        strncpy(new_schema->shape_signature, shape_sig, MAX_SHAPE_SIGNATURE_LEN - 1);
        new_schema->shape_signature[MAX_SHAPE_SIGNATURE_LEN - 1] = '\0';
    }

    strncpy(new_schema->columns[new_schema->num_columns++].name, "id", MAX_NAME_LEN - 1);

    if (parent_schema)
    { // If this table has a parent object it relates to
        snprintf(new_schema->parent_fk_column_name, MAX_NAME_LEN, "%s_id", parent_schema->name);
        new_schema->parent_fk_column_name[MAX_NAME_LEN - 1] = '\0';
        // Add the FK column, but only if it's not a junction table (junction tables add their own FK)
        if (!is_junction_table_flag)
        {
            strncpy(new_schema->columns[new_schema->num_columns++].name, new_schema->parent_fk_column_name, MAX_NAME_LEN - 1);
        }
    }

    if (is_junction_table_flag)
    { // R3 junction table columns
        // Parent FK name should have been set above if parent_schema was provided.
        // If parent_schema was NULL (e.g. root array of scalars), parent_fk_column_name needs a default.
        if (parent_schema == NULL)
        { // Root array of scalars
            snprintf(new_schema->parent_fk_column_name, MAX_NAME_LEN, "%s_parent_id", desired_table_name_hint);
            new_schema->parent_fk_column_name[MAX_NAME_LEN - 1] = '\0';
        }
        // Add the actual FK column for junction table
        strncpy(new_schema->columns[new_schema->num_columns++].name, new_schema->parent_fk_column_name, MAX_NAME_LEN - 1);
        strncpy(new_schema->columns[new_schema->num_columns++].name, "idx", MAX_NAME_LEN - 1);
        strncpy(new_schema->columns[new_schema->num_columns++].name, "value", MAX_NAME_LEN - 1);
    }
    else if (template_obj)
    { // For R1 objects or R2 object elements
        PairNode *member = template_obj->head;
        while (member)
        {
            if (new_schema->num_columns >= MAX_COLUMNS_PER_TABLE)
            {
                fprintf(stderr, "Warning: Max columns for table %s, key %s\n", new_schema->name, member->data.key);
                break;
            }
            if (member->data.value->type == JSON_STRING_TYPE ||
                member->data.value->type == JSON_NUMBER_TYPE ||
                member->data.value->type == JSON_BOOLEAN_TYPE ||
                member->data.value->type == JSON_NULL_TYPE)
            {
                int col_exists = 0;
                for (int k = 0; k < new_schema->num_columns; ++k)
                {
                    if (strcmp(new_schema->columns[k].name, member->data.key) == 0)
                    {
                        col_exists = 1;
                        break;
                    }
                }
                if (!col_exists)
                {
                    strncpy(new_schema->columns[new_schema->num_columns++].name, member->data.key, MAX_NAME_LEN - 1);
                }
            }
            member = member->next;
        }
    }

    new_schema->next_schema = G_all_schemas_head;
    G_all_schemas_head = new_schema;
    return new_schema;
}

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
        TableSchema *table_for_this_object;

        if (parent_object_schema && parent_object_schema->is_child_array_table &&
            strcmp(parent_object_schema->shape_signature, sig) == 0)
        {
            // This object is an element of an R2 array; it uses the schema already defined for array elements.
            table_for_this_object = parent_object_schema;
        }
        else
        {
            // This is an R1 object (root, or nested non-array element).
            // Determine its parent for FK purposes (if it's nested).
            TableSchema *actual_parent_for_fk = NULL;
            if (parent_object_schema && !parent_object_schema->is_child_array_table && !parent_object_schema->is_junction_table)
            {
                actual_parent_for_fk = parent_object_schema;
            }

            table_for_this_object = get_or_create_table(
                current_node_key_hint ? current_node_key_hint : input_filename_base,
                sig,
                &(current_json_node->data.object_val),
                actual_parent_for_fk, // Pass the true parent object's schema if this is a nested R1 object
                0,                    // Not a junction table
                0                     // Not an R2 array element table itself (its *parent* might be R2, but this obj is R1)
            );
        }

        PairNode *member = current_json_node->data.object_val.head;
        while (member)
        {
            discover_schemas_recursive(member->data.value, member->data.key, table_for_this_object, input_filename_base);
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
        char child_table_name_hint[MAX_NAME_LEN];
        const char *parent_name_for_hint = parent_object_schema ? parent_object_schema->name : input_filename_base;
        const char *array_key_for_hint = current_node_key_hint ? current_node_key_hint : "items";
        snprintf(child_table_name_hint, sizeof(child_table_name_hint), "%s_%s", parent_name_for_hint, array_key_for_hint);

        if (first_element->type == JSON_OBJECT_TYPE)
        { // R2: Array of objects
            char sig_first_obj[MAX_SHAPE_SIGNATURE_LEN];
            generate_object_shape_signature(&(first_element->data.object_val), sig_first_obj, sizeof(sig_first_obj));

            TableSchema *r2_elements_schema = get_or_create_table(
                child_table_name_hint,
                sig_first_obj,
                &(first_element->data.object_val),
                parent_object_schema, // The object containing this array is the parent
                0,                    // Not a junction table
                1                     // YES, this table is for R2 array elements
            );

            ValueNode *elem_node = arr->head;
            while (elem_node)
            {
                discover_schemas_recursive(elem_node->value, current_node_key_hint, r2_elements_schema, input_filename_base);
                elem_node = elem_node->next;
            }
        }
        else
        { // R3: Array of scalars (implicit due to previous checks)
            get_or_create_table(
                child_table_name_hint,
                NULL,
                NULL,
                parent_object_schema, // The object containing this array is the parent
                1,                    // YES, this is a junction table
                0                     // Not an R2 array element table
            );
        }
        break;
    }
    default:
        break;
    }
}

static void write_csv_escaped_string(FILE *f, const char *str)
{
    if (str == NULL)
    {
        fprintf(f, "");
        return;
    }
    if (strlen(str) == 0)
    {
        fprintf(f, "\"\"");
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
            fprintf(f, "\"\"");
        else
            fprintf(f, "%c", *p);
    }
    if (needs_quoting)
        fprintf(f, "\"");
}

static void populate_csv_recursive(JsonValue *current_json_node, TableSchema *current_object_schema_context, long parent_pk_value, const char *json_key_of_current_node, const char *input_filename_base)
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
        {
            table_for_this_obj = current_object_schema_context; // It's an R2 element, use the passed context
        }
        else
        { // It's an R1 object (root or nested). Find its schema.
            TableSchema *s_iter = G_all_schemas_head;
            char expected_table_name[MAX_NAME_LEN]; // For nested R1, name might be hint + key
            if (json_key_of_current_node)
            {   // If it has a key, it might be a nested R1 object like "author"
                // Its table name during discovery was likely based on this key.
                strncpy(expected_table_name, json_key_of_current_node, MAX_NAME_LEN - 1);
                expected_table_name[MAX_NAME_LEN - 1] = '\0';
            }
            else
            { // Root object, name based on input_filename_base
                strncpy(expected_table_name, input_filename_base, MAX_NAME_LEN - 1);
                expected_table_name[MAX_NAME_LEN - 1] = '\0';
            }

            while (s_iter)
            {
                // R1 tables are not child_array_tables and not junction_tables.
                // And their shape must match.
                // Also, their name should align with what discover_schemas_recursive would have named it.
                if (!s_iter->is_child_array_table && !s_iter->is_junction_table &&
                    strcmp(s_iter->shape_signature, sig) == 0)
                {
                    // This check might need to be stronger if multiple R1 tables can have same shape
                    // For now, if current_json_node has a key, we prefer a schema whose name matches that key.
                    // Otherwise, the first R1 table with matching signature.
                    if (json_key_of_current_node && strcmp(s_iter->name, json_key_of_current_node) == 0)
                    {
                        table_for_this_obj = s_iter;
                        break;
                    }
                    else if (!json_key_of_current_node && strcmp(s_iter->name, input_filename_base) == 0)
                    { // Root object
                        table_for_this_obj = s_iter;
                        break;
                    }
                    else if (!table_for_this_obj)
                    { // Fallback to first signature match if name specific match fails
                        table_for_this_obj = s_iter;
                        // Don't break yet if json_key_of_current_node, might find better name match
                        if (!json_key_of_current_node)
                            break;
                    }
                }
                s_iter = s_iter->next_schema;
            }
        }

        if (!table_for_this_obj)
        {
            // This object does not form its own table directly (e.g. it's a complex field whose parts are handled recursively)
            // or schema lookup failed.
            // For "author", if its schema has is_child_array_table=0, this lookup should find it.
            // If not found, it's an error or complex unhandled case.
            fprintf(stderr, "Warning: No table schema found for object with key '%s' and signature '%s'. Data may not be written.\n",
                    json_key_of_current_node ? json_key_of_current_node : "(root object)", sig);
            PairNode *member_recurse = obj->head;
            while (member_recurse)
            { // Still recurse for its children that might form tables
                populate_csv_recursive(member_recurse->data.value, current_object_schema_context, parent_pk_value, member_recurse->data.key, input_filename_base);
                member_recurse = member_recurse->next;
            }
            return;
        }

        long current_row_pk = ++(table_for_this_obj->current_pk_id);
        fprintf(table_for_this_obj->file_ptr, "%ld", current_row_pk);

        for (int i = 1; i < table_for_this_obj->num_columns; ++i)
        {
            fprintf(table_for_this_obj->file_ptr, ",");
            const char *col_name = table_for_this_obj->columns[i].name;

            // Check if this column is the defined parent_fk_column_name for this table
            if (strlen(table_for_this_obj->parent_fk_column_name) > 0 &&
                strcmp(col_name, table_for_this_obj->parent_fk_column_name) == 0)
            {
                fprintf(table_for_this_obj->file_ptr, "%ld", parent_pk_value);
            }
            else
            {
                JsonValue *member_val = NULL;
                PairNode *m_iter = obj->head;
                while (m_iter)
                {
                    if (strcmp(m_iter->data.key, col_name) == 0)
                    {
                        member_val = m_iter->data.value;
                        break;
                    }
                    m_iter = m_iter->next;
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
                        break;
                    default:
                        fprintf(table_for_this_obj->file_ptr, "");
                        break;
                    }
                }
                else
                {
                    fprintf(table_for_this_obj->file_ptr, "");
                }
            }
        }
        fprintf(table_for_this_obj->file_ptr, "\n");

        PairNode *member = obj->head;
        while (member)
        {
            if (member->data.value->type == JSON_ARRAY_TYPE || member->data.value->type == JSON_OBJECT_TYPE)
            {
                populate_csv_recursive(member->data.value, table_for_this_obj, current_row_pk, member->data.key, input_filename_base);
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
        char target_element_table_name[MAX_NAME_LEN];
        const char *parent_name_for_lookup = current_object_schema_context ? current_object_schema_context->name : input_filename_base;
        const char *array_key_for_lookup = json_key_of_current_node ? json_key_of_current_node : "items";
        snprintf(target_element_table_name, sizeof(target_element_table_name), "%s_%s", parent_name_for_lookup, array_key_for_lookup);

        TableSchema *s_iter = G_all_schemas_head;
        while (s_iter)
        {
            if (strcmp(s_iter->name, target_element_table_name) == 0)
            {
                if (first_element->type == JSON_OBJECT_TYPE && s_iter->is_child_array_table)
                { // R2 check
                    array_table_schema = s_iter;
                    break;
                }
                else if (s_iter->is_junction_table)
                { // R3 check (scalar types already implicitly checked by first_element type)
                    if (first_element->type != JSON_OBJECT_TYPE && first_element->type != JSON_ARRAY_TYPE)
                    { // Ensure it's scalar
                        array_table_schema = s_iter;
                        break;
                    }
                }
            }
            s_iter = s_iter->next_schema;
        }

        if (!array_table_schema)
        {
            fprintf(stderr, "Warning: populate_csv: Could not find schema for array elements of key '%s' (expected table name '%s').\n",
                    json_key_of_current_node ? json_key_of_current_node : "(root_array)", target_element_table_name);
            break;
        }

        if (first_element->type == JSON_OBJECT_TYPE)
        { // R2
            ValueNode *elem_node = arr->head;
            while (elem_node)
            {
                populate_csv_recursive(elem_node->value, array_table_schema, parent_pk_value, NULL, input_filename_base);
                elem_node = elem_node->next;
            }
        }
        else
        { // R3
            ValueNode *elem_node = arr->head;
            int idx = 0;
            while (elem_node)
            {
                long junction_row_pk = ++(array_table_schema->current_pk_id);
                fprintf(array_table_schema->file_ptr, "%ld", junction_row_pk);
                fprintf(array_table_schema->file_ptr, ",%ld", parent_pk_value);
                fprintf(array_table_schema->file_ptr, ",%d", idx++);
                fprintf(array_table_schema->file_ptr, ",");
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
                    fprintf(array_table_schema->file_ptr, "");
                    break;
                }
                fprintf(array_table_schema->file_ptr, "\n");
                elem_node = elem_node->next;
            }
        }
        break;
    }
    default:
        break;
    }
}

void process_json_to_csv(JsonValue *root_json_value, const char *output_dir_path, const char *input_filename_base)
{
    if (!root_json_value)
        return;
    strncpy(G_output_dir, output_dir_path, sizeof(G_output_dir) - 1);
    G_output_dir[sizeof(G_output_dir) - 1] = '\0';
    struct stat st = {0};
    if (stat(G_output_dir, &st) == -1)
    {
        if (mkdir(G_output_dir, 0700) != 0 && errno != EEXIST)
        {
            perror("Error creating output directory");
            exit(EXIT_FAILURE);
        }
    }

    discover_schemas_recursive(root_json_value, NULL, NULL, input_filename_base);
    if (!G_all_schemas_head)
    {
        printf("No tables generated for this JSON (no schemas discovered).\n");
        return;
    }

    TableSchema *s = G_all_schemas_head;
    while (s)
    {
        char file_path[MAX_NAME_LEN * 3];
        snprintf(file_path, sizeof(file_path), "%s/%s.csv", G_output_dir, s->name);
        s->file_ptr = fopen(file_path, "w");
        if (!s->file_ptr)
        {
            perror("Error opening CSV file for writing");
            fprintf(stderr, "Failed to open: %s\n", file_path);
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
    populate_csv_recursive(root_json_value, NULL, 0, input_filename_base, input_filename_base);
}

void cleanup_schemas()
{
    TableSchema *current = G_all_schemas_head;
    while (current)
    {
        TableSchema *next = current->next_schema;
        if (current->file_ptr)
            fclose(current->file_ptr);
        free(current);
        current = next;
    }
    G_all_schemas_head = NULL;
}