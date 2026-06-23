# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope
The KornShell script `k_ausd_v_ta_c_bfc.ksh` acts as an orchestration and control wrapper for a SQL script, `d_ausd_v_ta_c_bfc.sql`. Its primary purpose is to manage the execution of this SQL data processing job. Specifically, it handles:
*   **Parameter Validation:** Parses and validates command-line arguments `p_JobKennung` (job identifier) and `p_EintragsNr` (entry number).
*   **Job State Management:** It is designed to ignore currently active jobs with the same `p_JobKennung` and to deactivate old active jobs. This suggests interaction with a job control or metadata table.
*   **SQL Script Execution:** It invokes an external SQL script (`d_ausd_v_ta_c_bfc.sql`) to perform the core data processing.
*   **Error Handling and Reporting:** Implements a custom error handling mechanism using sourced utility scripts (`f_alis_msgerr.ksh`) and reports errors to the console.
*   **Record Count Capture:** After the SQL script execution, it reads a record count from a temporary file and makes it available.

The scope of this migration is to re-implement this orchestration logic and the underlying SQL processing within the BigQuery ecosystem, leveraging BigQuery stored procedures and SQL capabilities, or potentially an external orchestrator like Cloud Composer if complex external dependencies arise.

## 2. Source Inventory
The job is primarily composed of one KornShell script.

| File Name                                                 | Technology | Category | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| :-------------------------------------------------------- | :--------- | :------- | :-------------- | :---------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` | KornShell  | shell    | (not available) | semi_auto         | This is a control script that orchestrates the execution of a SQL script (`d_ausd_v_ta_c_bfc.sql`) for data processing. It handles parameter validation, job status checks, and error reporting, ensuring active jobs are managed and old ones deactivated. It sources several utility scripts for error handling, date functions, parameter parsing, and SQL*Plus execution. It sets a table name `ta_c_bfc` and captures a record count from a temporary file after SQL execution. |

## 3. Target Architecture
The primary target architecture for this job will be a BigQuery Stored Procedure.

*   **BigQuery Stored Procedure:** The core orchestration logic, parameter handling, validation, and error management currently in `k_ausd_v_ta_c_bfc.ksh` will be translated into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_ta_c_bfc`). This procedure will accept input parameters, execute the core data transformation, and manage job metadata.
*   **BigQuery Tables:**
    *   **Source Tables:** Data consumed by the underlying SQL logic will reside in existing BigQuery tables or views.
    *   **Target Tables:** The output of the data transformation (`ta_c_bfc`) will be a BigQuery table.
    *   **Job Metadata/Log Table:** A dedicated BigQuery table (e.g., `project.dataset.job_control_table` or `project.dataset.job_run_log`) will manage job activation status and log execution details and record counts, replacing the implicit job state management and temporary file usage.
    *   **Error Log Table:** A BigQuery table (e.g., `project.dataset.job_error_log`) will capture error details, replacing the `DWMSG_MeldeFehler` functionality.
*   **Underlying SQL Script (`d_ausd_v_ta_c_bfc.sql`):** The logic within this SQL script will be migrated into a BigQuery-compatible DML statement (e.g., `INSERT INTO ... SELECT ...`, `MERGE INTO`, `UPDATE`) or integrated as part of the main stored procedure logic or a separate, nested BigQuery stored procedure.
*   **Orchestration (Optional):** If there are external scheduling requirements or dependencies on other jobs, Cloud Composer (Airflow) could be used to call the BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The original data flow is as follows:

1.  **Environment Initialization:** `k_ausd_v_ta_c_bfc.ksh` sources `$HOME/.dw_init` for environment variables.
2.  **Utility Script Loading:** Several KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) are sourced for common functions.
3.  **Parameter Input:** The script receives `p_JobKennung` and `p_EintragsNr` as command-line parameters.
4.  **Parameter Validation:** Parameters are validated using `pruefeParameterGesetzt`.
5.  **Job State Check/Update:** Implied logic to check for active jobs and deactivate old ones, likely interacting with a job control system or table (e.g., `ta_c_bfc` based on variable `v_TabName`).
6.  **SQL Script Execution:** The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) invokes `d_ausd_v_ta_c_bfc.sql`. This SQL script is the core data transformation component, likely reading from source tables and writing to target tables, specifically `ta_c_bfc`.
7.  **Record Count Capture:** The number of processed records is written to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_c_bfc_$$.tmp`), which is then read back into the `v_records` variable.
8.  **Error Reporting:** Errors are logged and reported via `DWMSG_MeldeFehler`.

**Target BigQuery Data Flow:**

1.  **BigQuery Stored Procedure Call:** An external scheduler (e.g., Cloud Composer, Cloud Scheduler) or a user directly invokes the `project.dataset.r_ausd_ta_c_bfc` stored procedure, passing `p_JobKennung` and `p_EintragsNr` as arguments.
2.  **Parameter Validation (BQSP):** The stored procedure performs parameter validation using `IF` statements. If validation fails, an error is signaled, and an entry is made in `project.dataset.job_error_log`.
3.  **Job State Management (BQSP):** The stored procedure updates a `project.dataset.job_control_table` (or similar) to deactivate old jobs based on `p_JobKennung` and `p_EintragsNr`.
4.  **Core Data Transformation (BQSP):** The SQL logic from `d_ausd_v_ta_c_bfc.sql` is executed as a DML statement (e.g., `INSERT INTO project.dataset.ta_c_bfc SELECT ... FROM source_tables WHERE ...`) or via a nested stored procedure call.
5.  **Record Count Capture (BQSP):** The stored procedure captures the count of affected rows directly from the DML statement or via `SELECT COUNT(*)` on the target table. This count is then logged to `project.dataset.job_run_log`.
6.  **Logging (BQSP):** All significant events, including start, end, and record counts, are logged to `project.dataset.job_run_log`.

## 5. Transformation Logic

**Original KornShell (`k_ausd_v_ta_c_bfc.ksh`) to BigQuery Stored Procedure (`project.dataset.r_ausd_ta_c_bfc`) Mapping:**

*   **Environment Loading (`. $HOME/.dw_init`, etc.):** This will be replaced by the BigQuery environment context. Specific configurations (e.g., `BERT_DIR_ROOT`, `DW_DIR_UTL`) will be either hardcoded, passed as stored procedure parameters, or retrieved from BigQuery configuration tables.
*   **Parameter Parsing (`getopts`):** Replaced by input parameters to the BigQuery Stored Procedure (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`).
*   **Parameter Validation (`pruefeParameterGesetzt`):** Translated into `IF ... THEN ... ELSE ... END IF;` constructs within the BigQuery Stored Procedure. Missing parameters will trigger `SIGNAL SQLSTATE` and log to an error table.
*   **`v_TabName='ta_c_bfc'`:** This directly translates to the target BigQuery table name `project.dataset.ta_c_bfc`.
*   **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`):** Replaced by `INSERT` statements into a `job_error_log` table and `SIGNAL SQLSTATE` for procedure termination.
*   **SQL Script Path (`Name_SQLskript`):** The content of `d_ausd_v_ta_c_bfc.sql` will be embedded directly or called as a nested stored procedure within `project.dataset.r_ausd_ta_c_bfc`.
*   **Temporary File (`tmpFile`):** The use of a temporary file for record counting (`eval "v_records=`cat $tmpFile`"`) will be replaced by assigning the result of a `COUNT(*)` query or the `ROW_COUNT()` function (if applicable) to a `DECLARE`d variable within the BigQuery Stored Procedure, and then logging this to a `job_run_log` table.
*   **`starteSQLSkript` Function Call:** This function, which orchestrates SQL execution, will be replaced by direct DML/DDL execution within the BigQuery Stored Procedure.
*   **Job Deactivation Logic:** The comment "alte aktive Jobs werden einfach dekativiert" implies an `UPDATE` statement on a job control table, which will be implemented directly in BigQuery SQL.

**Underlying SQL Logic (`d_ausd_v_ta_c_bfc.sql`):**
The content of `d_ausd_v_ta_c_bfc.sql` (which is not available for inspection here) will be directly translated from its current SQL dialect (likely Oracle SQL based on context) to BigQuery Standard SQL. This may involve:
*   Rewriting specific functions (e.g., date functions, string manipulation).
*   Adjusting data types.
*   Replacing proprietary syntax with BigQuery equivalents.
*   Handling table/view references (`ta_c_bfc`).

## 6. External Dependencies

The following external dependencies were identified and their migration strategy defined:

*   **Sourced Utility Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date functions.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL execution wrapper (likely SQL*Plus).
    *   **Migration Strategy:** All these KornShell utility scripts will be replaced by native BigQuery Stored Procedure constructs, `DECLARE`d variables, or specific BigQuery functions. Error logging will be to a BigQuery table. The `starteSQLSkript` functionality will be subsumed by direct DML/DDL execution within the BigQuery Stored Procedure.
*   **Environment Variables:**
    *   `BERT_DIR_ROOT`, `DW_DIR_UTL`: These define directory structures.
    *   **Migration Strategy:** These will be replaced by BigQuery dataset/project references or configuration stored within BigQuery tables or passed as parameters. File paths for temporary files will be eliminated.
*   **Temporary File:**
    *   `$DW_DIR_UTL/bert_k_ausd_v_ta_c_bfc_$$.tmp`: Used to store record counts.
    *   **Migration Strategy:** Replaced by an `INT64` variable within the BigQuery Stored Procedure and persistent logging to a BigQuery table (`job_run_log`).
*   **Database (Implicit):**
    *   The script interacts with `ta_c_bfc` and potentially other job control tables.
    *   **Migration Strategy:** These will be migrated to BigQuery tables.

## 7. Unresolved / Risks

*   **Complexity of `d_ausd_v_ta_c_bfc.sql`:** The actual SQL logic within `d_ausd_v_ta_c_bfc.sql` is unknown. If it contains highly complex or proprietary SQL (e.g., Oracle-specific PL/SQL features, vendor-specific functions not directly translatable to BigQuery Standard SQL), this could significantly increase the migration effort and potentially necessitate a redesign of that specific component (B4 bucket). This is the biggest unknown.
*   **Job Deactivation Logic Details:** The exact mechanism and table schema for "deactivating old active jobs" are not fully clear from the script itself. This will need to be thoroughly investigated and replicated in BigQuery.
*   **Error Code Mapping:** The existing error codes (e.g., `ErrNr=193`, `ErrNr=192`) and their meanings need to be fully documented and mapped to an appropriate BigQuery error handling and logging strategy.
*   **SQL*Plus Specifics:** If `h_alis_sqlplus.ksh` leverages specific SQL*Plus features (e.g., spooling, variable substitution that goes beyond standard SQL), these will need to be re-engineered for BigQuery.
*   **Missing Complexity Tier:** The `file_complexity` table had no entry for this file, meaning its complexity tier is unknown. This introduces a risk that the `semi_auto` bucket might be optimistic if the script's internal logic (especially the SQL part) is very complex.

## 8. Build Plan

The migration will proceed with the following ordered build steps:

1.  **Analyze and Translate `d_ausd_v_ta_c_bfc.sql`:**
    *   **Action:** Obtain the content of `d_ausd_v_ta_c_bfc.sql`. Analyze its SQL dialect and features.
    *   **Output:** BigQuery Standard SQL DML/DDL statements for the core data transformation. This might result in a single complex query, several staged queries, or even a separate BigQuery Stored Procedure.
    *   **Tool/Language:** BigQuery Standard SQL.
2.  **Design and Create BigQuery Logging and Metadata Tables:**
    *   **Action:** Define schemas for `project.dataset.job_error_log`, `project.dataset.job_run_log`, and any necessary `project.dataset.job_control_table` to replicate the job management functionality.
    *   **Output:** `CREATE TABLE` DDL for BigQuery.
    *   **Tool/Language:** BigQuery Standard SQL.
3.  **Develop BigQuery Stored Procedure (`project.dataset.r_ausd_ta_c_bfc`):**
    *   **Action:** Translate the KornShell orchestration logic into a BigQuery Stored Procedure.
        *   Implement parameter handling (IN arguments).
        *   Implement parameter validation logic.
        *   Integrate error logging to `project.dataset.job_error_log`.
        *   Implement job state update logic (e.g., `UPDATE project.dataset.job_control_table`).
        *   Integrate the translated SQL from `d_ausd_v_ta_c_bfc.sql`.
        *   Implement record count capture and log to `project.dataset.job_run_log`.
    *   **Output:** `CREATE OR REPLACE PROCEDURE` DDL for BigQuery.
    *   **Tool/Language:** BigQuery Standard SQL.
4.  **Orchestration Integration (Optional but Recommended):**
    *   **Action:** If external scheduling is required, create a Cloud Composer (Airflow) DAG to invoke the `project.dataset.r_ausd_ta_c_bfc` BigQuery Stored Procedure.
    *   **Output:** Python script for an Airflow DAG.
    *   **Tool/Language:** Python (for Airflow).
5.  **Testing and Validation:**
    *   **Action:** Develop test cases covering valid parameters, invalid parameters, error conditions, and successful data processing scenarios. Compare results with the legacy system.
    *   **Output:** Test scripts, validation reports.
    *   **Tool/Language:** BigQuery Standard SQL, Python (for testing framework).