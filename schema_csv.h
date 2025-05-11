// schema_csv.h
#ifndef SCHEMA_CSV_H
#define SCHEMA_CSV_H

#include "ast.h"
#include <stdio.h> // For FILE*

#define MAX_NAME_LEN 512
#define MAX_COLUMNS_PER_TABLE 128
#define MAX_SHAPE_SIGNATURE_LEN 8192 // For object shape uniqueness

typedef struct ColumnInfo
{
    char name[MAX_NAME_LEN];
    // Could add type hint if needed, but CSV is typeless
} ColumnInfo;

typedef struct TableSchema
{
    char name[MAX_NAME_LEN]; // CSV file name (without .csv)
    ColumnInfo columns[MAX_COLUMNS_PER_TABLE];
    int num_columns;

    char shape_signature[MAX_SHAPE_SIGNATURE_LEN]; // Sorted unique keys string for R1

    FILE *file_ptr;     // Open file pointer for writing CSV
    long current_pk_id; // To generate unique primary keys for this table

    // For R2 (array of objects -> child table)
    int is_child_array_table;
    char parent_fk_column_name[MAX_NAME_LEN]; // e.g., "parent_id"

    // For R3 (array of scalars -> junction table)
    int is_junction_table; // True if this is a junction table for array of scalars

    struct TableSchema *next_schema; // For linked list of all schemas
} TableSchema;

// Global head of the linked list of discovered schemas
extern TableSchema *G_all_schemas_head;

void process_json_to_csv(JsonValue *root_json_value, const char *output_dir_path, const char *input_filename_base);
void cleanup_schemas(); // Frees all schema memory and closes files

#endif // SCHEMA_CSV_H