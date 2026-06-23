# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_iccid_einzeln.ksh` to Google Cloud Platform, specifically BigQuery.

The primary purpose of this script is to act as a control script for a data processing workflow. It handles:
*   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   Performing date format validation on `p_Stichtag`.
*   Orchestrating the execution of an SQL script, identified as `d_ausd_bp_ta_iccid_einzeln.sql`.
*   Capturing the record count resulting from the SQL execution.
*   Potentially managing job metadata (currently commented out in the source).

The scope of this migration covers transforming the shell script's orchestration logic and parameter handling into a BigQuery-compatible format, and ensuring the SQL script it invokes can also be executed within the BigQuery environment.

## 2. Source Inventory
The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh`
    *   **Technology:** KornShell Script
    *   **Category:** `shell`
    *   **Tool:** `KornShell`
    *   **Summary:** Control script that prepares parameters, performs validation, and orchestrates the execution of an SQL script (`d_ausd_bp_ta_iccid_einzeln.sql`) for data processing.
    *   **Complexity Tier:** `medium`
    *   **Migration Flags:** `[]` (None identified)
    *   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery.

*   **Core Logic:** The orchestration and parameter handling currently performed by the KornShell script will be migrated to a BigQuery Stored Procedure. This stored procedure will encapsulate the parameter validation, date checks, and the invocation of the core data processing logic.
*   **Data Processing:** The SQL script `d_ausd_bp_ta_iccid_einzeln.sql` invoked by the shell script is assumed to be convertible to BigQuery SQL and will likely become another BigQuery Stored Procedure or a set of SQL statements within the main orchestration procedure.
*   **Error Logging:** Legacy error handling (`DWMSG_MeldeFehler`) will be replaced by inserts into a dedicated BigQuery error log table and potentially BigQuery's `RAISE` statements for immediate failure.
*   **Job Control/Metadata:** Any job status or metadata logging will be implemented by inserting records into a BigQuery `job_log` or `process_log` table.
*   **Temporary Data:** File-based temporary outputs (like `$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`) will be replaced by BigQuery temporary tables or variables.

**BigQuery Dataset and Naming Convention:**
*   A dedicated BigQuery dataset (e.g., `project.dataset`) will host the migrated stored procedures and logging tables.
*   The main orchestration stored procedure will be named `r_ausd_bp_ta_iccid_einzeln`.
*   The SQL script `d_ausd_bp_ta_iccid_einzeln.sql` will be migrated to a stored procedure `d_ausd_bp_ta_iccid_einzeln` or its logic integrated into `r_ausd_bp_ta_iccid_einzeln`.
*   Logging tables: `error_log`, `job_log`, `process_log` (example names, specific naming to be confirmed during implementation).

## 4. Data Flow & Lineage
The original script's flow:
1.  **Environment Setup:** Sources `$HOME/.dw_init` and various utility KornShell scripts.
2.  **Parameter Parsing:** Reads `j`, `f`, `s`, `l` parameters using `getopts`.
3.  **Parameter Validation:** Checks if `p_JobKennung`, `p_Stichtag`, `p_EintragsNr` are set. If not, logs an error and exits.
4.  **Date Validation:** Calls `DWDate_Datum_Check` to validate `p_Stichtag` format (DDMMYYYY).
5.  **Date Calculation:** Executes `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
6.  **SQL Execution:** Calls `starteSQLSkript` which is a wrapper to execute `d_ausd_bp_ta_iccid_einzeln.sql` with collected parameters.
7.  **Record Count:** Reads the record count from `$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`.
8.  **Job Logging:** (Commented out) `FOSJobErzeugeEintrag` would write job metadata.

**Migrated BigQuery Data Flow:**
The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_iccid_einzeln` will replicate this flow:
1.  **Parameter Declaration:** Declares input parameters corresponding to the shell script's arguments.
2.  **Variable Initialization:** Initializes internal variables for table names, record counts, and dates.
3.  **Parameter Validation:** Uses `IF` statements and `RAISE` to enforce required parameter presence. Errors are logged to `project.dataset.error_log`.
4.  **Date Validation:** Uses `SAFE.PARSE_DATE` to validate `p_Stichtag`. Errors are logged and raised.
5.  **Date Calculation:** Uses BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions to determine `v_datum_heute` and `v_datum_gestern`.
6.  **SQL Logic Execution:** Calls the migrated SQL processing logic (e.g., `CALL project.dataset.d_ausd_bp_ta_iccid_einzeln(...)`). This procedure will contain the transformations and data manipulation originally in the `.sql` file.
7.  **Record Count Capture:** `SELECT COUNT(*)` from the target result table will determine the processed record count, which is then assigned to a variable.
8.  **Job Logging:** `INSERT` statements will populate `project.dataset.job_log` and `project.dataset.process_log` with execution details and record counts.

## 5. Transformation Logic

**File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh`**

**Original Logic (KornShell):**
*   **Environment & Utilities:** Sources external `.ksh` files for error handling, date checks, parameter parsing, and SQL*Plus utilities. These need to be re-implemented using BigQuery scripting features.
*   **Parameter Parsing (`getopts`):** Reads `j`, `f`, `s`, `l` arguments. This will be replaced by direct input parameters to the BigQuery Stored Procedure.
*   **Validation (`pruefeParameterGesetzt`, `DWDate_Datum_Check`):** Checks for missing parameters and date format. This will be converted to `IF` conditions with `RAISE` for errors in BigQuery.
*   **Date Derivation (`gestern.ksh`):** Calls an external script to get yesterday's and today's dates. This will be replaced by BigQuery's date functions (`CURRENT_DATE()`, `DATE_SUB()`).
*   **SQL Execution (`starteSQLSkript`):** This is the core part, invoking `d_ausd_bp_ta_iccid_einzeln.sql`. In BigQuery, this will translate to a `CALL` statement to another BigQuery Stored Procedure containing the migrated SQL logic.
*   **Record Count (`cat $tmpFile`):** Reads the count from a temporary file. In BigQuery, this will be replaced by `SELECT COUNT(*)` from the output table into a `DECLARE`d variable.
*   **Job Entry (`FOSJobErzeugeEintrag`):** (Commented out) If this functionality is required, it will be an `INSERT` into a BigQuery logging table.
*   **Commented Post-Processing (`sed`, `sort`, `join`):** These sections indicate potential legacy file manipulation. If these steps are genuinely required for the data pipeline, they should be re-evaluated and implemented as BigQuery SQL transformations on staging tables. However, given they are commented out, they are assumed to be retired.

**Target Logic (BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_iccid_einzeln`):**

```sql
-- BigQuery Stored Procedure / Script Pseudocode (from MCP tool)

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart INT64 DEFAULT 0;

  -- Parameter defaults
  IF p_wiederanlaufWert IS NULL THEN
    SET v_restart = 0;
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Required parameter checks (equivalent to pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 193, 'Jobkennung fehlt');
    RAISE USING MESSAGE = 'FEHLER: 0 E 193 Jobkennung fehlt';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 193, 'Stichtag fehlt');
    RAISE USING MESSAGE = 'FEHLER: 0 E 193 Stichtag fehlt';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 193, 'EintragsNr fehlt');
    RAISE USING MESSAGE = 'FEHLER: 0 E 193 EintragsNr fehlt';
  END IF;

  -- Date validation: DDMMYYYY (equivalent to DWDate_Datum_Check)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 194, 'Ungueltiges Datum: ' || p_Stichtag);
    RAISE USING MESSAGE = 'FEHLER: 0 E 194 Ungueltiges Datum';
  END IF;

  -- Execute core SQL logic formerly in external SQL file
  -- This assumes d_ausd_bp_ta_iccid_einzeln.sql is migrated to a BQ Stored Procedure
  CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln`(
    p_EintragsNr,
    p_JobKennung,
    p_Stichtag, -- Or v_stichtag_date if the SQL proc expects DATE
    v_restart,
    v_datum_heute,
    v_datum_gestern
  );

  -- Capture record count from target/result table
  -- Replace `target_result_table` with the actual table name written by d_ausd_bp_ta_iccid_einzeln
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.target_result_table`
    WHERE stichtag = v_stichtag_date -- Assuming a 'stichtag' column exists
  );

  -- Persist job metadata / completion status (equivalent to FOSJobErzeugeEintrag if uncommented)
  INSERT INTO `project.dataset.job_log` (
    tab_name,
    job_status,
    job_type,
    stichtag,
    run_date,
    record_count,
    message,
    created_at
  )
  VALUES (
    v_TabName,
    'A',
    'I',
    v_stichtag_date,
    v_stichtag_date,
    v_records,
    'Initialbefuellung',
    CURRENT_TIMESTAMP()
  );

  -- Optional: write processing summary
  INSERT INTO `project.dataset.process_log` (
    job_kennung,
    eintrags_nr,
    stichtag,
    records,
    created_at
  )
  VALUES (
    p_JobKennung,
    p_EintragsNr,
    v_stichtag_date,
    v_records,
    CURRENT_TIMESTAMP()
  );

END;
```

## 6. External Dependencies
The original script has minimal external dependencies, primarily other shell scripts and an assumed database connection for the SQL script.

*   **`$HOME/.dw_init` (Environment File):** This file sets up environment variables. In BigQuery, environment variables are not directly supported. Their values should be managed either as explicit parameters to stored procedures, configuration tables, or by the orchestration layer (e.g., Airflow variables).
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error Handling):** This utility for error logging will be replaced by `INSERT` statements into a BigQuery error log table (`project.dataset.error_log`) and `RAISE` statements.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date Check):** This utility will be replaced by BigQuery's built-in date parsing and validation functions (`SAFE.PARSE_DATE`).
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter Parsing):** This functionality is absorbed by the BigQuery Stored Procedure's parameter declarations and internal validation logic.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus Wrapper):** This wrapper for executing SQL via SQL*Plus will be eliminated. The SQL content itself will be migrated to BigQuery SQL and executed directly within a BigQuery Stored Procedure.
*   **`${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (Date Calculation):** This external script will be replaced by BigQuery's date functions (`CURRENT_DATE()`, `DATE_SUB()`).
*   **`d_ausd_bp_ta_iccid_einzeln.sql` (Core SQL Logic):** This is a critical dependency. It will be migrated to a BigQuery Stored Procedure, `project.dataset.d_ausd_bp_ta_iccid_einzeln`, or its logic directly integrated into `project.dataset.r_ausd_bp_ta_iccid_einzeln`.
*   **Oracle Database (Implied by `sqlplus` and `.sql` extension):** The original SQL script likely interacts with an Oracle database. All Oracle SQL will need to be converted to BigQuery SQL. Data will need to be ingested into BigQuery from Oracle as a prerequisite.

## 7. Unresolved / Risks
*   **`d_ausd_bp_ta_iccid_einzeln.sql` Content:** The actual content of this SQL file is unknown. Its complexity, dependencies, and specific SQL dialect are crucial for successful migration. A separate analysis of this file will be required. If it contains complex procedural logic (e.g., PL/SQL), its migration might require more sophisticated BigQuery scripting or even Dataflow/PySpark.
*   **Commented-out Code:** The commented-out `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, `sed`, `sort`, `join` sections are assumed to be obsolete. If any of this functionality is still required, it must be explicitly re-implemented. The `FOSJobErzeugeEintrag` is partially addressed in the pseudocode.
*   **Dynamic Paths (`BERT_DIR_ROOT`, `DW_DIR_UTL`):** The exact values and resolution logic for these environment variables need to be understood. In BigQuery, these could become static configuration values or parameters.
*   **Error Handling (`DWMSG_MeldeFehler`):** While a basic `INSERT` into an error log is proposed, the full functionality and integration with the legacy monitoring system (if any) might need further investigation.
*   **`starteSQLSkript` function:** The exact implementation of this function (e.g., how it handles database connections, error codes from SQL*Plus, or temporary file management) is not fully clear from the provided shell script. This implicit behavior must be replicated correctly in BigQuery.
*   **`migration_bucket: semi_auto`:** This indicates that some manual intervention and review will be necessary, confirming the need for careful review of the generated design and the subsequent build.

## 8. Build Plan

The migration will involve the following ordered steps:

1.  **Migrate `d_ausd_bp_ta_iccid_einzeln.sql` to BigQuery SQL:**
    *   **Description:** Analyze the content of `d_ausd_bp_ta_iccid_einzeln.sql`. Convert its DML and DDL statements (if any) to BigQuery SQL syntax. Create this as a BigQuery Stored Procedure, `project.dataset.d_ausd_bp_ta_iccid_einzeln`.
    *   **Language:** BigQuery SQL (Stored Procedure)
2.  **Create BigQuery Logging and Metadata Tables:**
    *   **Description:** Create the necessary BigQuery tables for error logging, job status, and process tracking (e.g., `project.dataset.error_log`, `project.dataset.job_log`, `project.dataset.process_log`).
    *   **Language:** BigQuery DDL
3.  **Create BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_iccid_einzeln`:**
    *   **Description:** Implement the BigQuery Stored Procedure as detailed in Section 5, incorporating parameter validation, date logic, and calling `project.dataset.d_ausd_bp_ta_iccid_einzeln`.
    *   **Language:** BigQuery SQL (Stored Procedure)
4.  **Orchestration Layer Integration:**
    *   **Description:** Integrate the new BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_iccid_einzeln`) into the target platform's orchestration system (e.g., Airflow DAG) to schedule its execution and pass parameters.
    *   **Language:** Python (for Airflow DAG) or other relevant orchestration configuration.
5.  **Data Ingestion (Prerequisite):**
    *   **Description:** Ensure that all source data required by `d_ausd_bp_ta_iccid_einzeln.sql` (presumably from Oracle) is successfully ingested and available in BigQuery tables.
    *   **Language:** Google Cloud Data Transfer Service, Dataflow, or other ingestion tools.