# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_msisdn.ksh` to Google Cloud Platform, specifically BigQuery.

**Purpose:** The original script `k_ausd_bp_ta_msisdn.ksh` acts as a control and orchestration wrapper. Its primary function is to handle parameter parsing, environment setup, error handling, and date validation before executing a core SQL script, `d_ausd_bp_ta_msisdn.sql`. It prepares the environment and parameters for the SQL execution, captures the output (e.g., record counts), and potentially performs job bookkeeping. The job's high-level purpose is described as "Job assembled from 1 component(s)". It involves processing data related to a table/object named `PoolBasisprodukt`.

**Scope:** The migration scope includes:
*   Translating the shell script's orchestration logic (parameter parsing, validation, date derivation) into BigQuery-compatible constructs.
*   Integrating the execution of the primary SQL logic (`d_ausd_bp_ta_msisdn.sql`) within the BigQuery environment.
*   Replicating the record counting mechanism and any job tracking functionalities.
*   Ensuring BigQuery best practices for data processing and error handling are adopted.

## 2. Source Inventory
The job consists of a single primary source file.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh`
*   **Technology:** KornShell (shell script)
*   **Summary:** This KornShell script acts as a control and orchestration wrapper for executing a SQL script, handling parameter parsing, environment setup, error handling, and date validation. It prepares and executes `d_ausd_bp_ta_msisdn.sql` and captures its output.
*   **Complexity Tier:** Information not available. This will need to be assessed manually or through further analysis.
*   **Automation Bucket:** semi_auto (B2) - Indicates that partial automation is expected, with some manual intervention or custom development required.

## 3. Target Architecture
The target architecture will leverage BigQuery's capabilities for scripting and stored procedures to encapsulate the orchestration logic.

*   **Primary Component:** A BigQuery Stored Procedure, e.g., `project.dataset.proc_ausd_bp_ta_msisdn`, will serve as the direct replacement for the `k_ausd_bp_ta_msisdn.ksh` script. This stored procedure will handle:
    *   Parameter parsing and validation (Jobkennung, Stichtag, EintragsNr, wiederanlaufWert).
    *   Date validation (DDMMYYYY format).
    *   Derivation of `heute` (today) and `gestern` (yesterday) dates.
    *   Calling another BigQuery Stored Procedure or directly embedding the translated SQL logic from `d_ausd_bp_ta_msisdn.sql`.
    *   Logging and error handling.
    *   Record counting.
    *   Optional job tracking.
*   **SQL Logic Component:** The core SQL logic from `d_ausd_bp_ta_msisdn.sql` will be migrated into a separate BigQuery Stored Procedure (e.g., `project.dataset.proc_d_ausd_bp_ta_msisdn`) or integrated directly into the orchestrating procedure, depending on its complexity and reusability.
*   **Configuration & Logging:**
    *   Environment variables (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`) will be replaced by dataset constants, configuration tables, or passed as procedure parameters.
    *   Error logging will be directed to a dedicated BigQuery logging table (`project.dataset.job_error_log`).
    *   Job tracking (if activated from the commented code) will use a BigQuery control table (`project.dataset.job_tracking`).
*   **Orchestration (Optional):** If external scheduling and orchestration are required beyond BigQuery's native capabilities, Cloud Composer (Airflow) or Cloud Workflows can be used to invoke the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script's lineage information was not explicitly available in the `lineage_edges` table. However, based on the source code and analysis, the data flow can be inferred:

1.  **Input Parameters:** The main BigQuery Stored Procedure receives `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert`.
2.  **Validation & Date Derivation:** The procedure validates these parameters and derives `v_datum_heute` and `v_datum_gestern` (equivalent to `gestern.ksh`).
3.  **SQL Execution:** The orchestrating procedure calls or embeds the translated logic of `d_ausd_bp_ta_msisdn.sql`, likely reading from source tables (e.g., implied "PoolBasisprodukt" or other tables accessed by the SQL).
4.  **Transformation/Processing:** The SQL logic performs data extraction, transformation, or loading operations.
5.  **Target Output:** The processed data is written to a target table (e.g., `project.dataset.target_table_or_result`).
6.  **Record Count & Logging:** The number of processed records is counted from the target table, and execution details, including errors or success messages, are logged to appropriate BigQuery tables.

**Inferred Dependencies from Script:**
*   `k_ausd_bp_ta_msisdn.ksh` INVOKES `d_ausd_bp_ta_msisdn.sql` (via `starteSQLSkript`).
*   `k_ausd_bp_ta_msisdn.ksh` INVOKES `gestern.ksh` (for date derivation).
*   `k_ausd_bp_ta_msisdn.ksh` DEPENDS_ON utility scripts: `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.
*   The overall job WRITES to a temporary file for record counts, which will be replaced by a `DECLARE` variable in BigQuery.
*   The SQL script `d_ausd_bp_ta_msisdn.sql` likely READS from source tables and WRITES to target tables.

## 5. Transformation Logic
The transformation logic will primarily involve converting KornShell scripting constructs and associated SQL calls into BigQuery SQL scripting and procedures.

**Shell Script to BigQuery Stored Procedure (`project.dataset.proc_ausd_bp_ta_msisdn`):**

*   **Parameter Handling:** `getopts` will be replaced by named parameters in the BigQuery Stored Procedure signature.
*   **Environment Sourcing:** `. $HOME/.dw_init` and `${BERT_DIR_ROOT}` references will be managed by either:
    *   Hardcoding relevant values as `DECLARE` constants within the procedure (if static).
    *   Fetching values from a BigQuery configuration table.
    *   Passing them as additional parameters to the procedure if dynamic.
*   **Utility Scripts:**
    *   `f_alis_msgerr.ksh`: Error reporting will be translated into `RAISE ERROR` or `SIGNAL SQLSTATE` in BigQuery, logging to `job_error_log`.
    *   `h_alis_date.ksh` (DWDate_Datum_Check): Date validation logic will use BigQuery's `SAFE.PARSE_DATE` and conditional `IF` statements.
    *   `h_alis_parameter.ksh` (pruefeParameterGesetzt): Parameter existence checks will use `IF parameter IS NULL OR TRIM(parameter) = ''`.
    *   `h_alis_sqlplus.ksh`: This wrapper for SQL*Plus will be entirely replaced by direct BigQuery SQL execution.
*   **Date Derivation (`gestern.ksh`):** The functionality of `gestern.ksh` (calculating today and yesterday) will be replaced by BigQuery's date functions: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Execution (`starteSQLSkript` / `d_ausd_bp_ta_msisdn.sql`):**
    *   The SQL logic within `d_ausd_bp_ta_msisdn.sql` will be translated into standard BigQuery SQL. This could involve `SELECT` statements, `INSERT`, `UPDATE`, `DELETE`, `MERGE` operations.
    *   This translated SQL will either be directly embedded within `proc_ausd_bp_ta_msisdn` or encapsulated in its own BigQuery Stored Procedure (`proc_d_ausd_bp_ta_msisdn`) and called.
*   **Record Counting:** The `eval "v_records=`cat $tmpFile`"` will be replaced by `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table_or_result`);`.
*   **Job Management (Commented):** The commented `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls indicate potential job tracking. If these functionalities are deemed necessary for migration, they will be implemented as `INSERT` or `UPDATE` statements against a `job_tracking` BigQuery table.
*   **Post-processing (Commented `sed`, `sort`, `join`):** The commented-out data manipulation (sed, sort, join) is not active and will not be migrated unless explicitly requested to reactivate this dormant logic. If reactivated, these would be translated to BigQuery SQL functions or potentially an external dataflow job if complex transformations are involved.

## 6. External Dependencies
The original job has the following external dependencies:

*   **Operating System/Shell Utilities:** `getopts`, `print`, `cat`, `eval`, `set`, `sed`, `sort`, `join`.
    *   **Replacement:** These will be replaced by BigQuery SQL scripting constructs, built-in functions, and native BigQuery capabilities.
*   **Filesystem-based configuration/utilities:**
    *   `$HOME/.dw_init`: An environment initialization file.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql`: The primary SQL script.
    *   `$DW_DIR_UTL/bert_k_ausd_bp_ta_msisdn.tmp`: Temporary file for record counts.
    *   **Replacement:** All these file-based dependencies will be internalized within BigQuery. Utility script logic will be directly coded as BigQuery SQL functions or sub-procedures. The SQL script's logic will become part of a BigQuery stored procedure. Temporary files will be replaced by BigQuery variables or temporary tables. Configuration paths will be managed via constants or configuration tables.
*   **Database (implied by SQL*Plus and SQL script):** An underlying database system where `d_ausd_bp_ta_msisdn.sql` was executed.
    *   **Replacement:** BigQuery itself will serve as the target database. All table references within `d_ausd_bp_ta_msisdn.sql` will be mapped to appropriate BigQuery datasets and tables.

No `external_systems` or `unresolved_targets` were found in the `lineage_assembled_jobs` record, suggesting no explicit external system integrations (like Oracle, SFTP, S3) are directly identified in the lineage for this job.

## 7. Unresolved / Risks
*   **File Complexity Information Missing:** The complexity tier and migration flags for `k_ausd_bp_ta_msisdn.ksh` were not found in `file_complexity`. This needs to be manually assessed to accurately estimate effort and identify specific migration challenges.
*   **Full `d_ausd_bp_ta_msisdn.sql` Analysis:** The actual content and complexity of `d_ausd_bp_ta_msisdn.sql` are critical. While this design covers the shell wrapper, a detailed analysis and migration plan for the SQL script itself are required. This script may contain proprietary functions, complex joins, or DML operations that require careful translation to BigQuery SQL.
*   **Commented-out Code:** The script contains significant commented-out sections (e.g., FOS job management, `sed`, `sort`, `join` operations). The current design assumes these are inactive. If any of this logic needs to be reactivated, it will add to the complexity and scope.
*   **`BERT_DIR_ROOT` and `DW_DIR_UTL` resolution:** The exact values and usage patterns of these environment variables need to be understood to correctly map them to BigQuery constants or configuration.
*   **`starteSQLSkript` Functionality:** The `starteSQLSkript` function is an abstraction. Its exact implementation details (e.g., how it handles connections, error codes, specific SQL*Plus commands) need to be known to ensure an accurate BigQuery equivalent.
*   **Error Handling Details:** The `DWMSG_MeldeFehler` implies a specific error reporting mechanism. The BigQuery error logging (`job_error_log` table) should capture equivalent details.

## 8. Build Plan
The migration will involve creating the following BigQuery components:

1.  **DDL for `job_error_log` table:** A BigQuery table to log errors and messages.
    *   **Language:** BigQuery DDL
2.  **DDL for `job_tracking` table (Optional):** A BigQuery table for job status and statistics, if the commented FOS job management needs to be implemented.
    *   **Language:** BigQuery DDL
3.  **BigQuery Stored Procedure for `d_ausd_bp_ta_msisdn.sql` logic:** Translate the content of `d_ausd_bp_ta_msisdn.sql` into a BigQuery Stored Procedure, e.g., `project.dataset.proc_d_ausd_bp_ta_msisdn`. This will be the core data processing unit.
    *   **Language:** BigQuery SQL (Stored Procedure)
4.  **BigQuery Stored Procedure for Orchestration:** Create `project.dataset.proc_ausd_bp_ta_msisdn` to replace the `k_ausd_bp_ta_msisdn.ksh` script. This procedure will:
    *   Define input parameters.
    *   Implement parameter and date validation.
    *   Derive `heute` and `gestern` dates.
    *   Call `project.dataset.proc_d_ausd_bp_ta_msisdn` (or embed its logic).
    *   Implement record counting.
    *   Handle error reporting to `job_error_log`.
    *   (Optional) Update `job_tracking`.
    *   **Language:** BigQuery SQL (Stored Procedure)
5.  **Deployment Script:** A script (e.g., using `bq` CLI or Cloud SDK) to deploy the DDL and stored procedures to BigQuery.
    *   **Language:** Shell Script or Python
6.  **Orchestration (Optional):** If required, an Airflow DAG in Cloud Composer or a Cloud Workflow definition to schedule and execute `project.dataset.proc_ausd_bp_ta_msisdn`.
    *   **Language:** Python (for Airflow) or YAML/JSON (for Cloud Workflows)