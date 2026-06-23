# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_discount.ksh` to Google BigQuery. The script serves as a wrapper or orchestration layer for a contract data reconciliation job related to the `ta_discount` table. Its primary functions include initializing the runtime environment, validating command-line parameters, setting up logging and error handling, invoking a core processing script, and recording the overall job status. The job was assembled from 1 component, with a medium stage distribution. The scope of this migration focuses on translating the orchestration, parameter handling, and logging aspects of this wrapper script, with the understanding that the core business logic resides in a separate, invoked script.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh`
    *   **Technology:** KornShell (KSH)
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Summary:** KornShell wrapper script for orchestrating the reconciliation of contract data for the 'ta_discount' table. It sets up the environment, handles parameters, and calls a core processing script.
    *   **Complexity Tier:** Not available in metadata.
    *   **Automation Bucket:** Not available in metadata.
    *   **Purpose:** ETL orchestrator/wrapper.

## 3. Target Architecture
The shell script's orchestration logic will be migrated to a BigQuery Stored Procedure. This stored procedure will handle parameter input, perform basic validation, manage job metadata, and log execution details to dedicated BigQuery audit tables. The invocation of the core processing script (`k_ausd_v_ta_discount.ksh`) will be represented as a call to another BigQuery Stored Procedure, `sp_k_ausd_v_ta_discount`, which would contain the translated core logic.

*   **BigQuery Components:**
    *   **Stored Procedures:**
        *   `project.dataset.sp_vertragsdatenabgleich_ta_discount`: Replaces the main wrapper script.
        *   `project.dataset.sp_k_ausd_v_ta_discount`: Placeholder for the core reconciliation logic (to be designed separately).
    *   **Audit Tables:**
        *   `project.dataset.job_control`: Stores job entry numbers, status (RUNNING, OK, ERROR), script names, log file names, and timestamps, replacing the original job control and log file functions.
        *   `project.dataset.job_error_log`: Records error details including job identifier, entry number, error message, and timestamp.
*   **Orchestration:** Cloud Composer (Airflow), Workflows, or Cloud Run might be used for external orchestration if the dependencies of `sp_k_ausd_v_ta_discount` or other parts of the overall job require non-SQL logic or external calls. For the immediate wrapper, the logic will be encapsulated within a BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original `r_ausd_v_ta_discount.ksh` script initiates the job. It sources several utility scripts (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`) for environment setup and common functions. The script then calls the core processing script `k_ausd_v_ta_discount.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`).

In BigQuery, this flow will be:
1.  **Invocation:** An external orchestrator (e.g., Cloud Composer DAG) or a direct call will execute `project.dataset.sp_vertragsdatenabgleich_ta_discount`.
2.  **Initialization:** The stored procedure will declare variables, determine the current date, and manage job entry numbers by querying and inserting into `project.dataset.job_control`.
3.  **Parameter Handling:** Input parameters will be passed directly to the stored procedure.
4.  **Core Logic Invocation:** The stored procedure will then execute `CALL project.dataset.sp_k_ausd_v_ta_discount(JobKennung, DW_EintragsNr)`, passing the necessary job identifier and entry number.
5.  **Logging & Error Handling:** All log messages, error details, and status updates will be written to `project.dataset.job_control` and `project.dataset.job_error_log` tables. Error conditions will be handled using BigQuery's `EXCEPTION WHEN ERROR` block.
6.  **Completion:** Upon successful completion or error, the status in `project.dataset.job_control` will be updated, and the procedure will terminate.

The current lineage edges do not show direct upstream/downstream dependencies for `r_ausd_v_ta_discount.ksh` within the `lineage_edges` table beyond what is inferred from the script's content. This suggests its role is purely orchestrational within this specific job.

## 5. Transformation Logic
The `r_ausd_v_ta_discount.ksh` script itself contains no direct data transformation or aggregation logic. Its "transformations" are limited to:
*   **Metadata Normalization:** Converting the job identifier to uppercase (`typeset -u JobKennung`) and formatting the current system date (`date +%d%m%Y`).
*   **Parameter Processing:** Using `getopts` for command-line argument parsing.
*   **Orchestration:** Invoking `k_ausd_v_ta_discount.ksh` with derived parameters.

These will be translated to equivalent BigQuery SQL functions and statements:
*   Uppercase conversion using `UPPER()`.
*   Date formatting using `FORMAT_DATE()`.
*   Parameter handling via stored procedure arguments and conditional logic (`IF` statements).
*   Stored procedure calls for orchestrating the core logic.

The actual data reconciliation logic is entirely delegated to `k_ausd_v_ta_discount.ksh`, which is assumed to be a separate transformation unit.

## 6. External Dependencies
The original script has the following dependencies:
*   **Sourced Environment Files and Utility Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging functions.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling functions.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling functions.
*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_discount.ksh`: Contains the main reconciliation logic.
*   **OS Level Tools:** `getopts`, `date`, `tee`, `trap`.

**Replacement Strategy for BigQuery:**
*   **Environment Initialization (`.dw_init`):** Environment variables will be replaced by BigQuery procedure parameters, configuration tables, or session variables within the BigQuery execution environment.
*   **Utility Scripts (DWMSG_*, h_alis_*):** These functions will be reimplemented as part of the BigQuery Stored Procedure, interacting with the `job_control` and `job_error_log` audit tables for logging and status management.
*   **Core Processing Script (`k_ausd_v_ta_discount.ksh`):** This will be migrated to a separate BigQuery Stored Procedure, `project.dataset.sp_k_ausd_v_ta_discount`. Its internal logic will require a separate design document.
*   **OS Level Tools:**
    *   `getopts`: Replaced by standard SQL parameter parsing within the stored procedure.
    *   `date`: Replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions.
    *   `tee`: Logging to console and file will be replaced by writing to `job_control` and `job_error_log` tables.
    *   `trap`: Replaced by BigQuery's SQL exception handling (`BEGIN ... EXCEPTION WHEN ERROR THEN ... END`).

There are no direct external system integrations (e.g., Oracle, SFTP, S3) identified for this specific wrapper script.

## 7. Unresolved / Risks
*   **Missing Core Logic:** The most significant gap is that the core data reconciliation logic within `k_ausd_v_ta_discount.ksh` is not part of this design. Its migration will be crucial and will likely involve more complex SQL transformations.
*   **Missing Metadata:** The `file_complexity` and `automation_rate` information was not available for this file, which means the initial assessment of effort and migration bucket for this specific component is unknown.
*   **Shell Traps:** Direct translation of shell traps (`INT`, `ERR`) to BigQuery SQL is not exact. BigQuery's `EXCEPTION WHEN ERROR` provides similar functionality for SQL errors, but OS-level signals might require a more sophisticated orchestration layer (e.g., Cloud Composer) to handle gracefully.
*   **File-based Logging:** The shell script's file-based logging is replaced by audit tables in BigQuery. While functional, the real-time tailing experience of a log file will be different.
*   **Environment Sourcing:** The sourcing of environment files (`. $HOME/.dw_init`) will require careful analysis to ensure all necessary environment variables and paths are correctly configured or replaced with BigQuery-native mechanisms.
*   **Dynamic Script Invocation:** If `k_ausd_v_ta_discount.ksh` is dynamically determined or its parameters are highly complex, the BigQuery stored procedure call might need to be adjusted or an external orchestrator might be necessary.

## 8. Build Plan
The migration build plan will focus on creating the BigQuery stored procedure and the necessary audit tables.

1.  **Create BigQuery Audit Tables:**
    *   `job_control`:
        ```sql
        CREATE TABLE project.dataset.job_control (
          entry_nr INT64,
          job_kennung STRING,
          script_name STRING,
          log_file STRING,
          status STRING,
          stichtag_info STRING,
          created_ts TIMESTAMP,
          finished_ts TIMESTAMP
        );
        ```
    *   `job_error_log`:
        ```sql
        CREATE TABLE project.dataset.job_error_log (
          job_kennung STRING,
          entry_nr INT64,
          error_nr INT64,
          error_arg STRING,
          error_message STRING,
          created_ts TIMESTAMP
        );
        ```
2.  **Develop BigQuery Stored Procedure for Wrapper:**
    *   Translate the KornShell script `r_ausd_v_ta_discount.ksh` into `project.dataset.sp_vertragsdatenabgleich_ta_discount` using the provided BigQuery SQL pseudocode as a starting point.
    *   Implement parameter handling, job metadata management, and logging to the new audit tables.
    *   Include the placeholder `CALL project.dataset.sp_k_ausd_v_ta_discount` for the core logic.
3.  **Develop BigQuery Stored Procedure for Core Logic (Placeholder):**
    *   Define a placeholder stored procedure `project.dataset.sp_k_ausd_v_ta_discount`. This will be populated after the detailed design of `k_ausd_v_ta_discount.ksh`.
4.  **Unit Testing:**
    *   Test `project.dataset.sp_vertragsdatenabgleich_ta_discount` with various parameters, including `-h` and invalid parameters, to ensure correct error handling and logging.
    *   Test successful execution paths and error scenarios (simulating errors from the core script).
5.  **Integration Testing:**
    *   Once `project.dataset.sp_k_ausd_v_ta_discount` is developed, integrate and test the end-to-end flow.
6.  **Deployment:**
    *   Deploy the BigQuery stored procedures and tables to the target BigQuery environment.
    *   Update any upstream orchestrators (e.g., Cloud Composer) to call the new BigQuery stored procedure.