# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh

## 1. Purpose & Scope
This migration job focuses on a KornShell script, `h_alis_date.ksh`, which provides a suite of utility functions for date calculations and validations. The script interacts with an Oracle database via `sqlplus` to perform some date operations, while others are handled natively within the shell. The primary purpose is to offer common date manipulation routines, such as calculating the previous month's end, checking date validity, comparing dates, determining date ranges, finding the last day of a month, counting days in a month, and adding days to a date. The scope of this migration is to convert this KornShell script and its dependent Oracle SQL scripts to equivalent BigQuery SQL stored procedures and functions.

## 2. Source Inventory

| File Name                                                               | Technology      | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                            |
| :---------------------------------------------------------------------- | :-------------- | :-------------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | KornShell Script | *Not analyzed*  | *Not analyzed*    | KornShell script providing utility functions for date calculations, often interacting with an Oracle database via sqlplus. It defines functions to check, compare, and calculate dates, including month-end and date addition. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/sql/d_alis_vormonat.sql` | Oracle SQL      | *Not analyzed*  | *Not analyzed*    | Oracle SQL script calculating the last day of the previous month based on `SYSDATE`.                                                                                                                                 |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/sql/d_alis_datum_zeitraum.sql` | Oracle SQL      | *Not analyzed*  | *Not analyzed*    | Oracle SQL script calculating a date range (start and end dates) based on an offset and a step (Day, Month, Year) from `SYSDATE`.                                                                                |

## 3. Target Architecture
The target platform is Google BigQuery. The KornShell script's functionalities will be refactored into BigQuery SQL stored procedures and user-defined functions (UDFs).

*   **BigQuery Stored Procedures**: Used for functions that involve control flow, variable assignments, and operations that might raise errors (e.g., `DWDate_Vormonat`, `DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`).
*   **BigQuery User-Defined Functions (UDFs)**: Used for functions that perform direct computations and return a single scalar value (e.g., `LetzterTagDesMonat`, `TageimMonat`, `AddiereDatum`).
*   **Dataset**: All procedures and functions will reside within a designated BigQuery dataset (e.g., `dataset`).
*   **Orchestration**: If the calling context of `h_alis_date.ksh` requires scheduling and execution coordination, Cloud Composer (Apache Airflow) or Workflows can be used to manage the sequence of BigQuery procedure calls.

## 4. Data Flow & Lineage
The original `h_alis_date.ksh` script orchestrates date operations. Functions like `DWDate_Vormonat`, `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` rely on `sqlplus` to execute Oracle SQL commands or external SQL scripts (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`). The results from these `sqlplus` calls are often spooled to temporary files, which are then read back into shell variables using `cat`, `grep`, and `cut`. Other functions like `LetzterTagDesMonat`, `TageimMonat`, and `AddiereDatum` perform date arithmetic purely within the KornShell environment.

**Migrated Data Flow in BigQuery:**
In BigQuery, the direct invocation of SQL scripts via `sqlplus` and the use of temporary files will be eliminated. Each function will be a self-contained BigQuery stored procedure or UDF.

*   **Input parameters**: Shell script parameters (`$1`, `$2`, etc.) will map directly to BigQuery procedure/function input parameters.
*   **Oracle `SYSDATE`**: Will be replaced by BigQuery's `CURRENT_DATE()` or `CURRENT_TIMESTAMP()`.
*   **Oracle `DUAL`**: Not needed in BigQuery; SQL queries can directly select expressions.
*   **Temporary files**: The mechanism of writing to and reading from temporary files will be replaced by direct assignment to BigQuery `DECLARE`d variables or output parameters (`OUT` parameters for stored procedures).
*   **Shell string processing (`grep`, `cut`, `tail`)**: Will be replaced by BigQuery string functions like `SPLIT`, `REGEXP_EXTRACT`, `SUBSTR`, or direct access to structured results.
*   **Shell arithmetic**: Will be replaced by BigQuery's native date arithmetic functions.

**Execution Order (Example for `DWDate_Vormonat`):**
1.  A calling routine invokes the BigQuery stored procedure `dataset.DWDate_Vormonat` with the required `v_dateformat` (and a variable to capture the `OUT` result).
2.  Inside `DWDate_Vormonat`, `CURRENT_DATE()` is used as the base, `DATE_SUB` calculates the previous month, and `LAST_DAY` gets the last day of that month. `FORMAT_DATE` formats the result.
3.  The formatted date is assigned to the `OUT` parameter `v_result`.

## 5. Transformation Logic

Each KornShell function will be transformed into a BigQuery SQL stored procedure or UDF.

### 5.1. `DWDate_Vormonat()`
*   **Legacy**: Calls `sqlplus` to execute `d_alis_vormonat.sql`, which uses `LAST_DAY(ADD_MONTHS(sysdate,-1))` to get the last day of the previous month. The result is spooled to a file and then read into a shell variable.
*   **BigQuery Target**: BigQuery Stored Procedure.
    ```sql
    CREATE OR REPLACE PROCEDURE dataset.DWDate_Vormonat(
      IN v_dateformat STRING,
      OUT v_result STRING
    )
    BEGIN
      SET v_result = FORMAT_DATE(
        v_dateformat,
        LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
      );
    END;
    ```

### 5.2. `DWDate_Datum_Check()`
*   **Legacy**: Uses an inline `sqlplus` call with `SELECT to_date('$wert','$format') FROM dual;` to validate a date. Returns `0` on success, `1` on failure (from `sqlplus` exit code).
*   **BigQuery Target**: BigQuery Stored Procedure.
    ```sql
    CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_Check(
      IN wert STRING,
      IN format STRING
    )
    BEGIN
      DECLARE parsed_date DATE;
      -- If PARSE_DATE fails due to invalid format, BigQuery raises an error automatically.
      SET parsed_date = PARSE_DATE(format, wert);
    END;
    ```

### 5.3. `DWDate_Datum_LE()`
*   **Legacy**: Uses an inline `sqlplus` call with a PL/SQL block to compare two dates (`datum1 <= datum2`). Raises Oracle application error `-20422` if `datum1 > datum2`.
*   **BigQuery Target**: BigQuery Stored Procedure.
    ```sql
    CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_LE(
      IN datum1 STRING,
      IN datum2 STRING
    )
    BEGIN
      DECLARE d1 DATE;
      DECLARE d2 DATE;

      SET d1 = PARSE_DATE('%Y%m%d', datum1);
      SET d2 = PARSE_DATE('%Y%m%d', datum2);

      IF d1 > d2 THEN
        RAISE USING MESSAGE = CONCAT('Datum ', datum1, ' ist groesser als ', datum2);
      END IF;
    END;
    ```

### 5.4. `DWDate_Gib_Zeitraum()`
*   **Legacy**: Calls `sqlplus` to execute `d_alis_datum_zeitraum.sql`, which calculates a date range using `SYSDATE`, offset, and step (`D`, `M`, `Y`). Results are spooled to a file, parsed with `grep` and `cut`, and assigned to shell variables via `eval`.
*   **BigQuery Target**: BigQuery Stored Procedure.
    ```sql
    CREATE OR REPLACE PROCEDURE dataset.DWDate_Gib_Zeitraum(
      IN offset_value INT64,
      IN stufe STRING,
      IN format STRING,
      OUT start_date STRING,
      OUT end_date STRING
    )
    BEGIN
      DECLARE base_start DATE;
      DECLARE base_end DATE;
      DECLARE calc_start DATE;
      DECLARE calc_end DATE;

      IF stufe = 'D' THEN
        SET base_start = CURRENT_DATE();
        SET base_end = CURRENT_DATE();
        SET calc_start = DATE_ADD(CURRENT_DATE(), INTERVAL offset_value DAY);
        SET calc_end = DATE_ADD(CURRENT_DATE(), INTERVAL offset_value DAY);

      ELSEIF stufe = 'M' THEN
        SET base_start = DATE_TRUNC(CURRENT_DATE(), MONTH);
        SET base_end = LAST_DAY(CURRENT_DATE());
        SET calc_start = DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL offset_value MONTH), MONTH);
        SET calc_end = LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL offset_value MONTH));

      ELSEIF stufe = 'Y' THEN
        SET base_start = DATE_TRUNC(CURRENT_DATE(), YEAR);
        SET base_end = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 12 MONTH), YEAR), INTERVAL 1 DAY);
        SET calc_start = DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL offset_value * 12 MONTH), YEAR);
        SET calc_end = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL (12 + offset_value * 12) MONTH), YEAR), INTERVAL 1 DAY);
      END IF;

      SET start_date = FORMAT_DATE(format, LEAST(base_start, calc_start));
      SET end_date = FORMAT_DATE(format, GREATEST(base_end, calc_end));
    END;
    ```

### 5.5. `LetzterTagDesMonat()`
*   **Legacy**: Shell script function performing arithmetic and array lookup to determine if a given `YYYYMMDD` date is the last day of its month, including leap year logic.
*   **BigQuery Target**: BigQuery UDF.
    ```sql
    CREATE OR REPLACE FUNCTION dataset.LetzterTagDesMonat(datum STRING)
    RETURNS BOOL
    AS (
      PARSE_DATE('%Y%m%d', datum) = LAST_DAY(PARSE_DATE('%Y%m%d', datum))
    );
    ```

### 5.6. `TageimMonat()`
*   **Legacy**: Shell script function performing arithmetic and array lookup to calculate the number of days in a given month and year, including leap year logic.
*   **BigQuery Target**: BigQuery UDF.
    ```sql
    CREATE OR REPLACE FUNCTION dataset.TageimMonat(jahr INT64, monat INT64)
    RETURNS INT64
    AS (
      EXTRACT(DAY FROM LAST_DAY(DATE(jahr, monat, 1)))
    );
    ```

### 5.7. `AddiereDatum()`
*   **Legacy**: Shell script function manually adding days to a `YYYYMMDD` date, handling month and year rollovers through loops and calls to `TageimMonat`.
*   **BigQuery Target**: BigQuery UDF.
    ```sql
    CREATE OR REPLACE FUNCTION dataset.AddiereDatum(datum STRING, tage INT64)
    RETURNS STRING
    AS (
      FORMAT_DATE(
        '%Y%m%d',
        DATE_ADD(PARSE_DATE('%Y%m%d', datum), INTERVAL tage DAY)
      )
    );
    ```

## 6. External Dependencies
*   **Oracle Database**: The original script heavily relies on an Oracle database (via `sqlplus` and the `DW_ORAUSER` environment variable) for date validation and calculation.
    *   **Replacement**: In BigQuery, all date operations will be performed using BigQuery's native date and time functions, eliminating the need for an external Oracle database. The logic previously executed on Oracle will be directly translated into BigQuery SQL within stored procedures and UDFs.
*   **Temporary Files (`/tmp`)**: Used by `DWDate_Vormonat` and `DWDate_Gib_Zeitraum` for spooling `sqlplus` output and then reading it back into shell variables.
    *   **Replacement**: This inter-process communication mechanism will be removed. BigQuery stored procedures can directly assign results to output parameters or declared variables, making temporary file I/O unnecessary.
*   **Shell Utilities (`grep`, `cut`, `rm`, `eval`)**: Used for parsing temporary files and dynamically assigning variables.
    *   **Replacement**: BigQuery SQL provides string manipulation functions (e.g., `SPLIT`, `REGEXP_EXTRACT`, `SUBSTR`) and explicit variable declaration and assignment, rendering these shell utilities obsolete in the target environment.

## 7. Unresolved / Risks
*   **Missing Complexity/Automation Metrics**: The initial analysis could not retrieve `tier` and `migration_bucket` for the source files. This suggests the migration effort might be underestimated without further manual review of file complexity.
*   **Dynamic Variable Names (`eval`)**: The `eval` command in `DWDate_Vormonat` and `DWDate_Gib_Zeitraum` is used for dynamic variable assignment. While BigQuery stored procedures support `OUT` parameters, ensuring exact dynamic behavior if complex naming conventions are involved needs careful consideration. The current BigQuery pseudocode uses direct `OUT` parameters, assuming a fixed set of target variable names. If the variable names themselves are dynamic, a different approach (e.g., passing result records) might be needed.
*   **Oracle-specific Date Semantics**: While BigQuery has rich date functions, subtle differences in how `ADD_MONTHS`, `LAST_DAY`, `TRUNC` handle edge cases (e.g., last day of month arithmetic, year rollovers) between Oracle and BigQuery should be thoroughly tested to ensure semantic equivalence. The generated BigQuery logic aims to match the Oracle behavior closely.
*   **Environment Variables (`DW_ORAUSER`, `DW_DIR_ROOT`)**: These are expected to be set in the KornShell environment. In BigQuery, these would typically be replaced by explicit dataset/project names or configuration parameters passed to the BigQuery procedures/functions.

## 8. Build Plan
The build plan involves creating the necessary BigQuery SQL objects (stored procedures and UDFs) and potentially an orchestration layer.

1.  **Create BigQuery Dataset**:
    *   Create a BigQuery dataset (e.g., `your_project.your_dataset`) to house the migrated date utility functions.
    *   Language: BigQuery DDL.

2.  **Generate BigQuery UDFs**:
    *   Translate `LetzterTagDesMonat()`, `TageimMonat()`, and `AddiereDatum()` into BigQuery SQL UDFs.
    *   Language: BigQuery SQL.

3.  **Generate BigQuery Stored Procedures**:
    *   Translate `DWDate_Vormonat()`, `DWDate_Datum_Check()`, `DWDate_Datum_LE()`, and `DWDate_Gib_Zeitraum()` into BigQuery SQL stored procedures.
    *   Language: BigQuery SQL.

4.  **Deployment Script**:
    *   Create a deployment script (e.g., a `.sql` file or a Python script using BigQuery client libraries) to apply the DDL for the dataset, UDFs, and stored procedures to the target BigQuery environment.
    *   Language: BigQuery SQL / Python.

5.  **Orchestration (Optional but Recommended)**:
    *   If the `h_alis_date.ksh` script is part of a larger workflow, consider creating an Apache Airflow DAG (using Cloud Composer) to manage the invocation and sequencing of these new BigQuery procedures. This would replace any previous scheduling mechanisms.
    *   Language: Python (for Airflow DAGs).

**Example Build Order:**
1.  `CREATE SCHEMA IF NOT EXISTS dataset;`
2.  `CREATE OR REPLACE FUNCTION dataset.LetzterTagDesMonat(...)`
3.  `CREATE OR REPLACE FUNCTION dataset.TageimMonat(...)`
4.  `CREATE OR REPLACE FUNCTION dataset.AddiereDatum(...)`
5.  `CREATE OR REPLACE PROCEDURE dataset.DWDate_Vormonat(...)`
6.  `CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_Check(...)`
7.  `CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_LE(...)`
8.  `CREATE OR REPLACE PROCEDURE dataset.DWDate_Gib_Zeitraum(...)`