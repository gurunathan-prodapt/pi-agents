# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

## 1. Purpose & Scope
The KornShell script `r_ausd_v_ta_cntrct_crs3.ksh` serves as a wrapper and orchestration component for a data synchronization process related to contract data. Its primary purpose is to prepare the execution environment, parse command-line parameters, manage logging, and orchestrate the execution of a core data processing script that targets the `ta_cntrct_crs3` table. It does not perform direct data transformations but rather manages the overall job flow and error handling.

## 2. Source Inventory
The job is comprised of a single main KornShell script, which then invokes other scripts.
*   **Main File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh`
    *   **Technology:** KornShell (`.ksh`)
    *   **Category:** Shell
    *   **Complexity Tier:** Medium (inferred from `lineage_assembled_jobs` complexity distribution)
    *   **Automation Bucket:** semi_auto (B2)
    *   **Summary:** Wrapper script for synchronizing contract data into the `ta_cntrct_crs3` table, handling parameter parsing, environment setup, error logging, and orchestrating the execution of a core data processing script.
*   **Implicitly Sourced Utilities:**
    *   `. $HOME/.dw_init` (environment initialization)
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error/message framework)
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling utilities)
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)
*   **Invoked Core Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh` (the actual data reconciliation logic is expected to reside here)

## 3. Target Architecture
The target architecture will leverage Google Cloud BigQuery for data processing and persistence, with Cloud Composer (Apache Airflow) for orchestration.

*   **Wrapper Logic:** The shell script's wrapper functionality (parameter parsing, logging, orchestration) will be migrated to a BigQuery Stored Procedure, named `project.dataset.sp_vertragsdatenabgleich`. This procedure will handle job metadata, status updates, and call the core data processing logic.
*   **Core Data Processing:** The core logic from `k_ausd_v_ta_cntrct_crs3.ksh` should ideally be migrated into a separate BigQuery Stored Procedure, e.g., `project.dataset.sp_k_ausd_v_ta_cntrct_crs3`. If the core script performs non-SQL operations (e.g., file system manipulation), it may need to be wrapped in a Cloud Function or Dataflow job, orchestrated by Cloud Composer.
*   **Logging and Auditing:** Dedicated BigQuery tables, such as `project.dataset.dw_job_log` and `project.dataset.dw_error_log`, will replace the file-based logging (`LogDatei`) and the `DWMSG_*` framework functions. These tables will store job execution status, parameters, log messages, and error details.
*   **Orchestration:** Cloud Composer (Airflow) will be used to schedule and manage the execution of the main BigQuery Stored Procedure. This will replace the legacy scheduler.

## 4. Data Flow & Lineage
The original `lineage_edges` analysis showed no direct relationships for this specific file, indicating its role as a top-level orchestrator. The migrated data flow will be as follows:

1.  **Cloud Composer DAG:** A scheduled DAG triggers the `sp_vertragsdatenabgleich` BigQuery Stored Procedure.
2.  **`sp_vertragsdatenabgleich` (BQSP):**
    *   Initializes job metadata (equivalent to `ProgName`, `ProgVersion`, `JobKennung`, `v_sysdate`).
    *   Parses and validates input parameters (`p_h`, `p_s`, `p_l`).
    *   Records job start entry into `project.dataset.dw_job_log`.
    *   Calls `project.dataset.sp_k_ausd_v_ta_cntrct_crs3` (or equivalent external process) to execute the core data reconciliation logic.
    *   Handles exceptions and records errors into `project.dataset.dw_error_log`.
    *   Updates job status (OK/ERROR) in `project.dataset.dw_job_log` upon completion.
3.  **`sp_k_ausd_v_ta_cntrct_crs3` (BQSP/External):** This component performs the actual data reading, transformation, and writing to the `ta_cntrct_crs3` table within BigQuery. Its detailed logic depends on the content of the original `k_ausd_v_ta_cntrct_crs3.ksh` script.

## 5. Transformation Logic
The transformation from KornShell to BigQuery SQL Stored Procedure will involve translating shell constructs into their BigQuery equivalents:

*   **Parameter Parsing:** The `getopts` logic in the shell script will be converted into `IN` parameters for the BigQuery Stored Procedure. Error conditions (`ErrNr = 192, 193`) will be handled by conditional logic and inserts into the error log table.
*   **Environment Variables & Utilities:** Sourcing of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` will be replaced by:
    *   BigQuery native functions (e.g., `CURRENT_DATE()` for date derivation).
    *   Explicit declarations within the stored procedure.
    *   Inserts into logging tables for error/message handling.
*   **Job Metadata:** Shell variables like `ProgName`, `ProgVersion`, `JobKennung`, `DW_EintragsNr` will become `DECLARE`d variables within the BigQuery Stored Procedure. The `typeset -u` for `JobKennung` translates to direct string assignment in BigQuery.
*   **Logging:** The `DWMSG_*` functions and `tee -a $LogDatei` operations will be replaced by `INSERT` statements into the `dw_job_log` and `dw_error_log` BigQuery tables.
*   **Error Handling (`trap`):** The shell's `trap` mechanism will be translated into BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to catch and handle runtime errors gracefully, logging them to `dw_error_log`.
*   **Core Script Invocation:** The `${Name_Kernskript}` execution will become a `CALL` to the `sp_k_ausd_v_ta_cntrct_crs3` stored procedure.

## 6. External Dependencies
The `lineage_assembled_jobs` indicated no external systems for this job. However, the source code reveals implicit file-based dependencies:

*   **Utility/Environment Scripts:** `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`. These are internal shell scripts providing common functionalities. Their logic needs to be either re-implemented using BigQuery SQL functions/stored procedures or if they provide environment setup, that setup needs to be recreated in the BigQuery execution context.
*   **Core Processing Script:** `k_ausd_v_ta_cntrct_crs3.ksh`. This is the most significant dependency. Its content must be analyzed. If it performs SQL-like operations, it should be migrated to a BigQuery Stored Procedure. If it involves complex file I/O or system commands, it might need migration to a Cloud Function (Python/Node.js) or a Dataflow job, executed as an external step orchestrated by Cloud Composer.
*   **File-based Logging:** The original script used a log file. This will be replaced by BigQuery logging tables (`dw_job_log`, `dw_error_log`).

## 7. Unresolved / Risks
*   **Content of `k_ausd_v_ta_cntrct_crs3.ksh`:** This is the primary unknown. The complexity and language of this core script will heavily influence its migration strategy and effort. Without its content, the full migration scope cannot be precisely defined.
*   **Environment Initialization (`.dw_init`):** The exact variables and configurations set by `.dw_init` need to be understood and replicated in the BigQuery execution environment (e.g., via Cloud Composer environment variables or explicit parameter passing).
*   **Detailed Logic of Utility Scripts:** The exact functionality within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` needs to be fully understood to accurately translate them to BigQuery SQL functions or procedures, or to confirm their irrelevance in the new environment.
*   **`semi_auto` Bucket:** The `semi_auto` migration bucket suggests that some manual intervention or review will be required, likely due to the need to interpret the original shell logic and design appropriate BigQuery constructs.

## 8. Build Plan
1.  **Define BigQuery Schemas (SQL):**
    *   Create `project.dataset.dw_job_log` table (for job metadata, status, log messages).
    *   Create `project.dataset.dw_error_log` table (for detailed error messages).
    *   (If applicable) Create schema for `ta_cntrct_crs3` and any other tables involved in `k_ausd_v_ta_cntrct_crs3.ksh`.
2.  **Migrate Utility Logic (SQL):**
    *   Analyze and re-implement necessary functionalities from `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` into BigQuery UDFs or helper stored procedures as needed.
3.  **Migrate Core Script `k_ausd_v_ta_cntrct_crs3.ksh` (SQL/Python):**
    *   **Analyze `k_ausd_v_ta_cntrct_crs3.ksh` content.**
    *   If primarily SQL-based: Design and implement `project.dataset.sp_k_ausd_v_ta_cntrct_crs3` BigQuery Stored Procedure.
    *   If non-SQL: Design and implement a Cloud Function or Dataflow job (e.g., Python) to encapsulate its logic.
4.  **Implement Wrapper Stored Procedure (SQL):**
    *   Create `project.dataset.sp_vertragsdatenabgleich` BigQuery Stored Procedure, translating the shell script's control flow, parameter handling, and logging to BigQuery SQL, incorporating the pseudocode provided by the migration tool.
    *   Ensure the `CALL` to the core script's migrated component is correctly implemented.
5.  **Develop Orchestration (Python/Airflow):**
    *   Create a Cloud Composer DAG in Python to:
        *   Define the schedule for the job.
        *   Trigger the `project.dataset.sp_vertragsdatenabgleich` BigQuery Stored Procedure, passing any necessary parameters.
6.  **Testing:**
    *   Unit test BigQuery Stored Procedures and logging mechanisms.
    *   Integrate test the Cloud Composer DAG with the BigQuery components.
    *   Perform end-to-end testing to ensure data reconciliation for `ta_cntrct_crs3` functions as expected.