# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_discount.ksh` to Google BigQuery. The script serves as a control and orchestration wrapper for a core SQL script (`d_ausd_v_ta_discount.sql`). Its primary purpose is to:
*   Manage job execution, including ignoring currently active jobs.
*   Call a specific SQL script for data processing.
*   Register job execution status and details in a job table.
*   Deactivate older active job entries to maintain job state.
The scope of this migration design specifically covers the wrapper logic of `k_ausd_v_ta_discount.ksh`, its parameter handling, utility script integrations, and job control mechanisms. The detailed migration of the embedded SQL script `d_ausd_v_ta_discount.sql` will be a subsequent, dependent effort.

## 2. Source Inventory
*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh`
*   **Technology**: KornShell Script
*   **Purpose**: ETL orchestration, job control
*   **Complexity Tier**: Medium (based on job assembly note, no specific tier found in `file_complexity`)
*   **Automation Bucket**: Semi-automated (B2)
*   **Summary**: A shell script that initializes environment variables, parses command-line arguments (`p_JobKennung`, `p_EintragsNr`), performs parameter validation, and orchestrates the execution of an external SQL script (`d_ausd_v_ta_discount.sql`). It also manages job status records in a presumed job table and captures record counts via a temporary file.

## 3. Target Architecture
The migrated solution will primarily leverage Google BigQuery's capabilities, specifically:
*   **BigQuery Stored Procedure**: The core logic of `k_ausd_v_ta_discount.ksh` will be translated into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_v_ta_discount`). This procedure will handle parameter validation, job state management, and the invocation of the actual data processing logic.
*   **BigQuery Tables**:
    *   `project.dataset.job_table`: To store job execution status, mirroring the legacy job table used for `active_flag` and registration.
    *   `project.dataset.job_error_log`: For logging errors detected during parameter validation or execution.
    *   `project.dataset.job_run_control`: A control table or an alternative mechanism (e.g., an output parameter of the invoked SQL procedure) to capture processed record counts, replacing the temporary file approach.
*   **BigQuery Stored Procedure for SQL logic**: The external SQL script `d_ausd_v_ta_discount.sql` will be migrated into its own BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_discount`) or a set of BigQuery SQL statements. This will contain the actual business transformation logic.

## 4. Data Flow & Lineage
The data flow in the target BigQuery environment will be as follows:
1.  **Invocation**: The `r_ausd_v_ta_discount` BigQuery Stored Procedure is invoked, likely by a scheduler (e.g., Cloud Composer DAG, Cloud Scheduler) or another BigQuery job, passing `p_JobKennung` and `p_EintragsNr` as input parameters.
2.  **Parameter Validation**: The procedure validates the input parameters. If invalid, an entry is written to `job_error_log`, and the procedure exits.
3.  **Job Status Update**:
    *   Existing active entries in `job_table` for the given `p_JobKennung` are deactivated.
    *   A new active entry for the current job run is inserted into `job_table`.
4.  **SQL Logic Execution**: The `r_ausd_v_ta_discount` procedure calls the `d_ausd_v_ta_discount` BigQuery Stored Procedure (or executes the equivalent SQL) to perform the main data processing. Parameters relevant to the SQL script are passed during this invocation.
5.  **Record Count Capture**: Upon completion of the `d_ausd_v_ta_discount` procedure, the number of processed records is retrieved (e.g., from an output parameter of `d_ausd_v_ta_discount` or from `job_run_control` table) and assigned to a variable within `r_ausd_v_ta_discount`.
6.  **Completion**: The `r_ausd_v_ta_discount` procedure logs completion and returns the processed record count.

## 5. Transformation Logic
The transformation logic of the `k_ausd_v_ta_discount.ksh` wrapper script itself primarily involves control flow and parameter handling, not direct data transformations.
*   **Parameter Parsing & Validation**: The `getopts` logic for `j` (JobKennung) and `f` (EintragsNr) will be replaced by direct input parameters to the BigQuery Stored Procedure. Validation (checking for `NULL` or empty strings) will use `IF` statements and `ASSERT` or `RAISE` for error handling.
*   **Environment Initialization**: The sourcing of `.dw_init` and other utility scripts will be replaced by BigQuery's intrinsic environment (project, dataset context) and by incorporating necessary logic directly into the stored procedure or creating equivalent BigQuery helper functions/procedures.
*   **Job Management**: The script's logic to deactivate old active jobs and register the current job will be translated into `UPDATE` and `INSERT` DML statements against the `project.dataset.job_table`.
*   **SQL Script Execution**: The `starteSQLSkript` function call, which executes `d_ausd_v_ta_discount.sql`, will be replaced by a `CALL` statement to the corresponding BigQuery Stored Procedure `project.dataset.d_ausd_v_ta_discount`.
*   **Record Count Retrieval**: The temporary file (`tmpFile`) mechanism for `v_records` will be replaced by querying a control table (`project.dataset.job_run_control`) or by having the `d_ausd_v_ta_discount` procedure return the record count as an `OUT` parameter.
*   **Error Handling**: The `f_alis_msgerr.ksh` and custom error checks will be replaced by BigQuery's exception handling (e.g., `BEGIN ... EXCEPTION ... END` blocks), `ASSERT` statements, and logging to `project.dataset.job_error_log`.

## 6. External Dependencies
The original script has several dependencies which need to be addressed in the migration:
*   **Environment Initialization (`. $HOME/.dw_init`)**: This will be replaced by BigQuery project/dataset configuration and potentially initial `DECLARE` statements within the stored procedure.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: These shell-specific utility functions will be replaced by:
    *   **Error Reporting**: BigQuery error handling (`ASSERT`, `RAISE`), `INSERT` statements to a dedicated error log table (`project.dataset.job_error_log`).
    *   **Date Utilities**: BigQuery's native date and time functions (`CURRENT_DATE()`, `FORMAT_DATE()`, etc.).
    *   **Parameter Handling**: Handled intrinsically by the stored procedure's input parameters.
    *   **SQL Execution**: Replaced by direct `CALL` statements to other BigQuery procedures or `EXECUTE IMMEDIATE` for dynamic SQL if necessary.
*   **External SQL Script (`${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_discount.sql`)**: This is a critical dependency. Its content must be migrated to a BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_discount`) or equivalent BigQuery SQL. This migration is outside the direct scope of *this* wrapper design but is a necessary prerequisite for the complete job.
*   **Job Table (implicitly used by `starteSQLSkript`)**: A corresponding BigQuery table (`project.dataset.job_table`) will be created to manage job status.
*   **Temporary File (`tmpFile`)**: Replaced by a BigQuery variable or a specific control table entry for record counts.

## 7. Unresolved / Risks
*   **Content of `d_ausd_v_ta_discount.sql`**: The actual business logic and complexity of `d_ausd_v_ta_discount.sql` are unknown. This SQL script will need its own analysis and migration effort. This is the largest unknown and potential risk.
*   **Complexity Signals**: The `file_complexity` analysis did not return specific complexity signals, which means potential challenges within the `k_ausd_v_ta_discount.ksh` script might not have been fully identified by automated tools.
*   **Detailed `starteSQLSkript` Logic**: The exact implementation of `starteSQLSkript` and its interaction with the job table or error handling are abstracted. The BigQuery migration assumes a direct call to a SQL procedure and standard job table updates. Any complex behavior within `starteSQLSkript` (e.g., connection handling, retry mechanisms) would need to be re-implemented in BigQuery or an external orchestrator.
*   **DWMSG_MeldeFehler Implementation**: The full functionality of the legacy error reporting system (`DWMSG_MeldeFehler`) is not known. The proposed solution involves logging to a BigQuery table, which might not fully replicate all aspects of the original error reporting (e.g., notifications, integrations with other systems).

## 8. Build Plan
1.  **DDL Creation**:
    *   Define DDL for `project.dataset.job_table` (e.g., `job_kennung STRING`, `eintrags_nr STRING`, `table_name STRING`, `active_flag BOOL`, `start_ts TIMESTAMP`, `end_ts TIMESTAMP`, `script_name STRING`).
    *   Define DDL for `project.dataset.job_error_log` (e.g., `job_kennung STRING`, `eintrags_nr STRING`, `err_nr INT64`, `err_arg STRING`, `error_ts TIMESTAMP`, `script_name STRING`).
    *   Define DDL for `project.dataset.job_run_control` (e.g., `job_kennung STRING`, `eintrags_nr STRING`, `script_name STRING`, `records_processed INT64`, `update_ts TIMESTAMP`).
2.  **Migrate `d_ausd_v_ta_discount.sql`**:
    *   Analyze `d_ausd_v_ta_discount.sql` (if available).
    *   Translate `d_ausd_v_ta_discount.sql` into a BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_discount`). This procedure should ideally accept `p_EintragsNr` and `p_JobKennung` as input parameters and optionally return `records_processed` as an `OUT` parameter or insert it into `job_run_control`.
    *   (Language: BQSQL)
3.  **Create `r_ausd_v_ta_discount` Stored Procedure**:
    *   Implement the wrapper logic for `k_ausd_v_ta_discount.ksh` as a BigQuery Stored Procedure (`project.dataset.r_ausd_v_ta_discount`) based on the provided pseudocode.
    *   (Language: BQSQL)
4.  **Orchestration (Optional but recommended)**:
    *   Create a Cloud Composer DAG or Cloud Workflow to schedule and invoke the `project.dataset.r_ausd_v_ta_discount` stored procedure, passing the necessary parameters.
    *   (Language: Python for Airflow DAG, YAML for Cloud Workflow)