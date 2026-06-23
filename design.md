# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_c_bfc.ksh`. The script serves as a wrapper and orchestration component for updating a "Bindefristcache" table, likely named `ta_c_bfc`. Its primary responsibilities include:
*   Loading the necessary runtime environment.
*   Parsing command-line parameters (though currently only `-h` is explicitly handled).
*   Initializing job and error logging mechanisms.
*   Invoking a core processing script, `k_ausd_v_ta_c_bfc.ksh`, which contains the actual data update logic.
*   Managing job status (success or failure) and logging outcomes.
The script's business purpose is to ensure the controlled and logged execution of the `ta_c_bfc` table update process.

## 2. Source Inventory
The job consists of a single KornShell script:
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh`
*   **Technology:** KornShell (shell script)
*   **Complexity Tier:** Medium
*   **Automation Bucket:** Semi-automatic (B2)
*   **Purpose:** ETL Orchestrator / Wrapper Script

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery, leveraging its stored procedure capabilities for orchestration and standard tables for logging.
*   **BigQuery Stored Procedure:** The `r_ausd_v_ta_c_bfc.ksh` wrapper script will be re-engineered into a BigQuery Stored Procedure (e.g., `project.dataset.sp_bindefristcache_update`). This procedure will handle parameter validation, job metadata initialization, and the invocation of the core logic.
*   **BigQuery Audit/Log Tables:** The existing shell-based logging (`$LogDatei`, `DWMSG_...` functions) will be replaced by inserts into dedicated BigQuery tables (e.g., `project.dataset.job_audit_log` for job status and `project.dataset.job_error_log` for errors).
*   **Core Logic Migration:** The core processing logic currently in `k_ausd_v_ta_c_bfc.ksh` will also be migrated. This will likely involve converting it into another BigQuery stored procedure (e.g., `project.dataset.sp_ausd_v_ta_c_bfc`) if its logic is SQL-compatible. If it involves complex file system operations, external APIs, or non-SQL data manipulations, it may require migration to a Python-based Cloud Run job or a Dataflow pipeline, orchestrated by the main BigQuery stored procedure or an external workflow manager (e.g., Cloud Composer).

## 4. Data Flow & Lineage
The original script orchestrates the update of the `ta_c_bfc` table. The data flow can be summarized as:
1.  **Environment Setup:** The script sources several utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) to set up its execution context and error handling.
2.  **Parameter Handling:** It parses command-line arguments using `getopts`.
3.  **Logging Initialization:** It initializes job-specific logging (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`).
4.  **Core Script Execution:** The script then executes `k_ausd_v_ta_c_bfc.ksh`, passing job identifiers as parameters. This core script is responsible for the actual data reading, transformation, and writing to the `ta_c_bfc` table.
5.  **Status Update:** Upon completion of the core script, it updates the job status via `DWMSG_SetzeStatusOK`.

In the BigQuery target architecture, this flow will be:
1.  **Stored Procedure Invocation:** The main BigQuery stored procedure (`sp_bindefristcache_update`) is called, potentially with parameters corresponding to the original shell script's arguments.
2.  **Variable Declaration & Initialization:** Environment-like variables are declared within the stored procedure.
3.  **Logging Records:** Instead of sourcing shell functions for logging, the stored procedure will insert records into `job_audit_log` and `job_error_log` tables at different stages (start, error, success).
4.  **Core Logic Invocation:** The stored procedure will `CALL` the migrated core logic stored procedure (`sp_ausd_v_ta_c_bfc`).
5.  **Error Handling:** BigQuery's `EXCEPTION WHEN ERROR THEN ...` block will manage errors during the execution of the core logic, updating error logs and potentially re-raising errors.
6.  **Final Status Update:** On successful completion, the `job_audit_log` will be updated with an 'OK' status.

## 5. Transformation Logic
The `r_ausd_v_ta_c_bfc.ksh` script itself does not perform direct data transformations. Its logic is primarily control flow and orchestration. The migration of its "transformation logic" entails:
*   **Parameter Validation:** The `getopts` logic for parameter parsing and basic validation will be translated into `IF/THEN/ELSE` blocks within the BigQuery stored procedure, operating on the stored procedure's input parameters.
*   **Job Metadata:** The dynamic generation of `JobKennung`, `v_sysdate`, `DW_EintragsNr`, and `LogDatei` will be handled using BigQuery's `DECLARE` statements, `CURRENT_TIMESTAMP()`, `FORMAT_DATE()`, and `SELECT MAX(...) + 1` for sequence generation.
*   **Logging Functions:** Calls to `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` will be replaced by `INSERT` statements into the `job_audit_log` and `job_error_log` tables, capturing relevant metadata and messages.
*   **Execution Flow:** The sequential execution and conditional branching (`if [ ! $ErrNr -eq 0 ]`) will be directly translated into BigQuery SQL control flow statements.
*   **Core Script Invocation:** The shell command `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure for `k_ausd_v_ta_c_bfc.ksh`.

## 6. External Dependencies
The original script has several external dependencies:
*   **Sourced Utility Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
    *   **Migration Plan:** These utilities, where their functionality is essential, will be either inlined into the main BigQuery stored procedure or migrated as separate, smaller BigQuery UDFs (User Defined Functions) or stored procedures. Their shell-specific aspects (e.g., `trap` handling, file system operations) will be replaced by BigQuery's native error handling and table-based logging.
*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh`: This script holds the primary logic for updating the `ta_c_bfc` table.
    *   **Migration Plan:** This script will be analyzed separately. If its logic is primarily SQL-based, it will be migrated to a BigQuery stored procedure (e.g., `sp_ausd_v_ta_c_bfc`). If it involves complex logic that is not easily translated to SQL (e.g., extensive external file I/O, complex transformations requiring specific libraries), it may need to be migrated to a PySpark job on Dataproc, a Python script on Cloud Run, or a Dataflow pipeline. The calling BigQuery stored procedure (`sp_bindefristcache_update`) will then be modified to invoke this external BigQuery job or service.
*   **Log Files:** The script writes to dynamically generated log files.
    *   **Migration Plan:** Log file writes will be replaced by inserts into BigQuery audit and error log tables.

## 7. Unresolved / Risks
*   **Shell `trap` Handling:** The `trap` commands used for signal handling (`INT`, `ERR`) in the shell script do not have a direct equivalent in BigQuery SQL. While BigQuery stored procedures offer `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` for error handling, it's not a direct one-to-one mapping for asynchronous signals. This difference will be handled by BigQuery's native exception mechanisms, focusing on transactionality and error logging.
*   **Undefined CLI Options:** The `getopts` `ParamList` includes `s:l:`, but there's no visible code handling `-s` or `-l` parameters. It's assumed these are either unused or implicitly handled by the sourced scripts. In migration, these will be treated as unsupported unless further analysis of the sourced scripts reveals their purpose.
*   **Dynamic Source Path (`BERT_DIR_ROOT`):** The script uses `$BERT_DIR_ROOT` to locate other scripts. This environment-dependent path will need to be parameterized or hardcoded as appropriate within the BigQuery environment (e.g., using BigQuery constants or stored procedure variables).
*   **Core Script `k_ausd_v_ta_c_bfc.ksh`:** The actual complexity and nature of transformations within `k_ausd_v_ta_c_bfc.ksh` are currently unknown. Its migration path (BQ SQL SP, PySpark, Cloud Run) depends heavily on its internal logic. This represents the primary unknown risk in the overall migration.

## 8. Build Plan
The build plan will involve the following steps:

1.  **Define BigQuery Audit Tables:**
    *   Create `project.dataset.job_audit_log` table (e.g., for `entry_nr`, `job_kennung`, `script_name`, `log_file`, `stichtag`, `status`, `created_ts`, `message`).
    *   Create `project.dataset.job_error_log` table (e.g., for `job_kennung`, `err_nr`, `err_arg`, `created_ts`, `message`).
2.  **Migrate Core Logic (`k_ausd_v_ta_c_bfc.ksh`):**
    *   Analyze `k_ausd_v_ta_c_bfc.ksh` to determine its migration path (BigQuery stored procedure, Python, etc.).
    *   Develop and test the migrated core logic component (e.g., `project.dataset.sp_ausd_v_ta_c_bfc`).
3.  **Develop `sp_bindefristcache_update` Stored Procedure:**
    *   Write the BigQuery SQL for `project.dataset.sp_bindefristcache_update`.
    *   Implement parameter handling (e.g., for `-h`).
    *   Integrate logic for generating job metadata (`DW_EintragsNr`, `JobKennung`).
    *   Replace `DWMSG_...` calls with `INSERT` statements into the audit and error log tables.
    *   Implement BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` for error handling.
    *   Include a `CALL` statement to invoke the migrated core logic (`project.dataset.sp_ausd_v_ta_c_bfc` or an external mechanism).
4.  **Testing:**
    *   Unit test `project.dataset.sp_bindefristcache_update` and `project.dataset.sp_ausd_v_ta_c_bfc` independently.
    *   Integration test the full workflow, ensuring correct logging and error handling.
    *   Validate the updated `ta_c_bfc` table after successful runs.
5.  **Deployment:**
    *   Deploy the BigQuery tables and stored procedures to the target environment.
    *   Configure any external orchestration (e.g., Cloud Composer DAG) if the core logic requires it.