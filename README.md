# JSON to Relational CSV Converter (Compiler Construction Assignment 04)

This tool reads a JSON file, parses it, and converts its structure into a set of relational CSV files. It uses Flex for lexical analysis, Bison (Yacc) for parsing, and C for AST construction, schema discovery, and CSV generation.

**Roll Nos:** (22i-1197 || 22i-1321)
**Section:** ( C        || B)

## Features

* Parses valid JSON input.
* Builds an Abstract Syntax Tree (AST) representing the JSON structure.
* Implements conversion rules to map JSON to relational tables:
    * **R1 (Object to Table Row):** Objects with the same keys (shape) map to rows in a single table.
    * **R2 (Array of Objects to Child Table):** Arrays of objects become child tables with foreign keys to the parent.
    * **R3 (Array of Scalars to Junction Table):** Arrays of scalar values become junction tables.
    * **R4 (Nulls & Scalars):** JSON `null` values become empty fields in CSV. Scalar values form columns.
    * **R5 (Keys):** Assigns an integer primary key (`id`) to every row. Foreign keys are named `<parent_table_name>_id`.
* Generates one `.csv` file per discovered table, including a header row.
* Streams CSV output to handle potentially large files.
* Optional AST printing to `stdout`.
* Error reporting with line and column numbers for lexical and syntax errors.
* Memory-safe implementation.

## Build Instructions

1.  **Prerequisites:**
    * GCC (C Compiler)
    * Flex
    * Bison
    On Ubuntu/Debian, install with:
    ```bash
    sudo apt-get update
    sudo apt-get install build-essential flex bison
    ```

2.  **Compile:**
    Navigate to the project's root directory and run:
    ```bash
    make
    ```
    This will produce an executable named `json2relcsv`.

3.  **Clean:**
    To remove build files and generated CSVs from a default test run:
    ```bash
    make clean
    ```

## **Run Instructions**
  ## Run a single .json file
    
    ```bash
    ./json2relcsv <input.json> [--print-ast] [-out-dir DIR]
    '''
  ### **This command will:**

        Read .json test file.

        Run file through the json2relcsv executable.

        Store the resulting CSV in the output_csvs/ directory.


## OR 

## **Running All Tests**

    The project comes with a script to run all JSON test files and compare outputs.

    1. Ensure the script is executable:

        '''bash
        chmod +x run_all_tests.sh
        '''
    2. Then run the tests:

        '''bash
        ./run_all_tests.sh
        '''

### **This script will:**

        Read all .json test files inside the testcases/ folder.

        Run each file through the json2relcsv executable.

        Store the resulting CSVs in the output_csvs/ directory.

