# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_aurd_rechstan.ksh`.
The primary purpose of this script is to act as a control script for a data processing job. It is responsible for:
- Parsing and validating command-line parameters (job identifier, entry number, reference date, restart value).
- Initializing the execution environment by sourcing shared utility scripts.
- Performing date validation.
- Orchestrating the execution of an external SQL script, `d_aurd_rechstan.sql`, which performs the core data processing.
- Capturing the number of records processed by the SQL script.
- (Intended, though commented out) Recording job status and metrics into a job management system (likely a job table).
The script plays an ETL orchestration role, ensuring data readiness and initiating the main data load process.

## 2. Source Inventory
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh`
  - **Technology:** KornShell script
  - **Tool:** KornShell
  - **Summary:** This KornShell script acts as a control script for a data processing job. It parses parameters, validates dates, orchestrates the execution of an SQL script (`d_aurd_rechstan.sql`), and interacts with a job management system to record job status and metrics.
  - **File Purpose:** ETL orchestration, pipeline_orchestrator
  - **Complexity Tier:** (Information not available from `file_complexity` table)
  - **Migration Bucket:** (Information not available from `automation_rate` table)

## 3. Target Architecture
The migration will leverage Google Cloud Platform (GCP) services, primarily BigQuery for data processing and potentially Cloud Composer (Airflow) for orchestration.

-   **Orchestration Layer:** The current KornShell script logic, including parameter parsing, validation, and calling the SQL script, will be migrated to a BigQuery Stored Procedure. For higher-level scheduling and dependency management, if `k_aurd_rechstan.ksh` is part of a larger workflow, it will be integrated into a Cloud Composer DAG. The BigQuery Stored Procedure itself will be invoked by the orchestrator.
-   **Data Processing Layer:** The core SQL logic currently in `d_aurd_rechstan.sql` will be converted to BigQuery SQL and executed within the BigQuery Stored Procedure, likely using `EXECUTE IMMEDIATE` for dynamic SQL or directly as a series of DML statements.
-   **Job Monitoring/Logging:** The existing job-table updates and error logging will be replaced with dedicated BigQuery tables (e.g., `project.dataset.job_table`, `project.dataset.error_log`). Error reporting will use BigQuery's `RAISE USING MESSAGE` in conjunction with inserts into the error log table.
-   **Date Calculation:** `gestern.ksh` functionality will be replaced by BigQuery's native date functions like `CURRENT_DATE()` and `DATE_SUB()`.

## 4. Data Flow & Lineage
The original script `k_aurd_rechstan.ksh` acts as an orchestrator.

1.  **Input Parameters:** The script receives `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as command-line arguments.
2.  **Environment & Utilities:** It sources common utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and executes `gestern.ksh`.
3.  **Validation:** Parameters are validated (`pruefeParameterGesetzt`) and the `p_Stichtag` date format is checked (`DWDate_Datum_Check`).
4.  **SQL Script Execution:** The script invokes `d_aurd_rechstan.sql` via the `starteSQLSkript` function, passing various parameters.
    -   **Input to SQL:** The `d_aurd_rechstan.sql` script is expected to read data from various source tables (not directly identified for this specific job in `lineage_edges` but implied by typical ETL patterns).
    -   **Output from SQL:** `d_aurd_rechstan.sql` is expected to write to a temporary file (`tmpFile`) the count of processed records, and ultimately load data into `RKopfStan`.
5.  **Record Count & Job Logging:** The `k_aurd_rechstan.ksh` script reads `v_records` from the `tmpFile` and (intended) inserts this information into a job table (`FOSJobErzeugeEintrag`).

**Migrated Data Flow:**
-   **Input:** The BigQuery Stored Procedure will receive parameters directly.
-   **Processing:**
    -   Parameter and date validations will be implemented using BigQuery Scripting (`IF`, `SAFE.PARSE_DATE`).
    -   Date derivation will use `CURRENT_DATE()` and `DATE_SUB()`.
    -   The logic from `d_aurd_rechstan.sql` will be directly incorporated into the stored procedure (e.g., using `INSERT`, `MERGE`, `CREATE TABLE AS SELECT`).
    -   The `RKopfStan` target table will be the final destination for processed data.
-   **Output:**
    -   Job status and record counts will be inserted into a BigQuery `job_table`.
    -   Errors will be logged to a BigQuery `error_log` table.

## 5. Transformation Logic
The core transformation logic resides within the `d_aurd_rechstan.sql` file, which is orchestrated by `k_aurd_rechstan.ksh`. The `k_aurd_rechstan.ksh` script primarily handles orchestration, parameter management, and error handling.

**Original Logic (`k_aurd_rechstan.ksh`):**
-   **Parameter Parsing:** `getopts` is used to parse command-line arguments.
-   **Parameter Validation:** `pruefeParameterGesetzt` checks for mandatory parameters.
-   **Date Validation:** `DWDate_Datum_Check` ensures `p_Stichtag` is in `DDMMYYYY` format.
-   **Date Derivation:** `gestern.ksh` provides today's and yesterday's dates.
-   **SQL Execution:** `starteSQLSkript` function executes the SQL script `d_aurd_rechstan.sql`.
-   **Record Count:** `cat $tmpFile` reads the record count produced by the SQL script.
-   **Error Handling:** `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` manage error messages and exit codes.
-   **Job Status Update (Commented):** `FOSJobErzeugeEintrag` and `FOSJobDeaktivate` are intended for job tracking.

**Target Logic (BigQuery Stored Procedure):**
-   **Parameter Handling:**
    -   `DECLARE` statements for variables (e.g., `v_TabName`, `v_records`, `v_stichtag_date`).
    -   Input parameters will be defined directly in the `CREATE OR REPLACE PROCEDURE` signature.
    -   Default `p_wiederanlaufWert` will be set using `IF p_wiederanlaufWert IS NULL THEN SET v_restart = 0; END IF;`.
-   **Validation:**
    -   Mandatory parameter checks using `IF ... IS NULL OR TRIM(...) = '' THEN RAISE USING MESSAGE ...; END IF;`.
    -   Date format validation using `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and checking for `NULL`.
-   **Date Derivation:** `v_datum_heute DATE DEFAULT CURRENT_DATE();` and `v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`.
-   **SQL Execution:**
    -   The logic from `d_aurd_rechstan.sql` will be embedded directly. If dynamic parameters are needed, `EXECUTE IMMEDIATE` will be used.
    -   Record counting will be done via `SELECT COUNT(*)` from the target table after insertion.
-   **Error Handling:**
    -   `RAISE USING MESSAGE` for immediate errors.
    -   `INSERT INTO project.dataset.error_log (...)` for persistent error logging.
-   **Job Status Update:**
    -   `INSERT INTO project.dataset.job_table (...)` to record job metadata, including processed records.

## 6. External Dependencies
The original script has several dependencies:

1.  **Environment Initialization:** `$HOME/.dw_init`
    -   **Migration:** Environment variables will be explicitly defined or passed as parameters to the BigQuery Stored Procedure or set within the Cloud Composer environment.
2.  **Helper Shell Scripts:**
    -   `f_alis_msgerr.ksh` (Error messaging)
    -   `h_alis_date.ksh` (Date utilities)
    -   `h_alis_parameter.ksh` (Parameter parsing/validation)
    -   `h_alis_sqlplus.ksh` (SQL*Plus related utilities)
    -   `gestern.ksh` (Date derivation)
    -   **Migration:** All functionality from these scripts will be re-implemented directly in BigQuery Scripting using native functions (`SAFE.PARSE_DATE`, `CURRENT_DATE`, `DATE_SUB`), conditional logic (`IF`), and DML for logging. The `h_alis_sqlplus.ksh` will become obsolete as BigQuery SQL replaces SQL*Plus.
3.  **External SQL File:** `d_aurd_rechstan.sql`
    -   **Migration:** This script's content will be fully converted to BigQuery SQL and embedded within or called by the main BigQuery Stored Procedure.
4.  **Temporary File:** `$DW_DIR_UTL/bert_k_aurd_rechstan_$$.tmp`
    -   **Migration:** Replaced by BigQuery scripting variables (`DECLARE v_records INT64;`) or by directly querying the target table for row counts.
5.  **Job Management System/Table (Implicit):** `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`
    -   **Migration:** Replaced by `INSERT` statements into a BigQuery `job_table` with a defined schema for tracking job execution, status, and metrics.

## 7. Unresolved / Risks
-   **Commented-out Code:** The `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls are commented out in the source. This design assumes these are *intended* functionalities that should be enabled in the target BigQuery environment. If they are permanently disabled, the corresponding `INSERT` statements in the BigQuery Stored Procedure can be omitted.
-   **`d_aurd_rechstan.sql` Complexity:** The actual SQL logic within `d_aurd_rechstan.sql` is not available in this analysis. This design assumes it contains standard SQL that can be directly translated to BigQuery SQL. If `d_aurd_rechstan.sql` contains complex procedural logic (e.g., PL/SQL specific features, cursors, loops not directly translatable to BigQuery SQL), additional refactoring or use of BigQuery Scripting's advanced features may be required.
-   **Error Codes and Logging Standards:** The `ErrNr` and `ErrArg` structure should be mapped to a consistent BigQuery error handling and logging standard.
-   **Orchestration Context:** If `k_aurd_rechstan.ksh` is part of a larger, complex job stream (e.g., invoked by UC4), the overarching orchestration will need to be redesigned, likely using Cloud Composer (Airflow), with the BigQuery Stored Procedure as a task.

## 8. Build Plan
The migration will involve generating the following artifacts:

1.  **`r_aurd_rechstan_sp.sql` (BigQuery Stored Procedure):**
    -   **Language:** BigQuery SQL
    -   **Description:** This file will contain the complete BigQuery Stored Procedure `project.dataset.r_aurd_rechstan`. It will encapsulate:
        -   Parameter declarations.
        -   Validation logic (parameters, date format).
        -   Date derivation.
        -   The migrated core logic from `d_aurd_rechstan.sql` (converted to BQSQL).
        -   Record counting.
        -   Job status updates to `job_table`.
        -   Error logging to `error_log`.
2.  **`job_table_ddl.sql` (BigQuery Table DDL):**
    -   **Language:** BigQuery DDL
    -   **Description:** Defines the schema for the `job_table` to store job execution metadata.
3.  **`error_log_table_ddl.sql` (BigQuery Table DDL):**
    -   **Language:** BigQuery DDL
    -   **Description:** Defines the schema for the `error_log` table to capture runtime errors.
4.  **`orchestration_dag.py` (Cloud Composer DAG - Optional):**
    -   **Language:** Python (Airflow DAG)
    -   **Description:** If higher-level orchestration is required, this file will define a Cloud Composer DAG to schedule and invoke the BigQuery Stored Procedure. It will include tasks for calling the stored procedure and potentially other pre/post-processing steps.
5.  **`target_table_ddl.sql` (BigQuery Table DDL - for RKopfStan):**
    -   **Language:** BigQuery DDL
    -   **Description:** Defines the schema for the target table `RKopfStan` which `d_aurd_rechstan.sql` ultimately populates.

This build plan focuses on the core conversion to BigQuery and establishes the necessary supporting structures for robust operation in the target environment.