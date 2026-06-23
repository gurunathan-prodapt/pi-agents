# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh

## 1. Purpose & Scope

This job, `r_ausd_v_ta_barrier_zusgf.ksh`, serves as a wrapper or orchestration script for the reconciliation of contract data, specifically targeting the `ta_barrier_zusgf` table. Its primary function is to prepare the environment, parse command-line parameters, initialize logging, handle errors, and then invoke a "kernel script" (`k_ausd_v_ta_barrier_zusgf.ksh`) which is expected to contain the actual business logic for data comparison and processing related to `ta_barrier_zusgf`. The scope of this migration document focuses on translating the wrapper script's functionality to Google Cloud Platform, primarily using BigQuery for its logic and potentially Cloud Composer or Cloud Workflows for orchestration.

## 2. Source Inventory

The job consists of a single KornShell script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh`
*   **Technology:** KornShell (ksh)
*   **Complexity Tier:** Medium (as per `lineage_assembled_jobs` stage distribution)
*   **Automation Bucket:** Semi-Automatic (`semi_auto`)
*   **Summary:** This is a control script responsible for job initialization, parameter handling, error trapping, and invoking the main processing script. It does not contain direct data transformation logic.

**Dependent Scripts/Libraries (sourced or invoked by this wrapper):**
*   `$HOME/.dw_init` (environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging framework)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling utilities)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh` (the "kernel script" containing core processing logic, invoked by this wrapper)

## 3. Target Architecture

The wrapper script's functionality will be migrated to a BigQuery Stored Procedure. This stored procedure will encapsulate the parameter parsing, logging initiation, and error handling logic. The invocation of the "kernel script" will be replaced by a call to another BigQuery Stored Procedure, which will represent the migrated `k_ausd_v_ta_barrier_zusgf.ksh`.

*   **Main Component:** BigQuery Stored Procedure (`sp_r_ausd_v_ta_barrier_zusgf`)
*   **Logging:** Dedicated BigQuery logging table (`project.dataset.job_log`) for audit and operational monitoring, replacing the file-based logging and `DWMSG_*` functions.
*   **Orchestration:** While the wrapper's direct execution will be a BQ Stored Procedure call, if multi-step execution and external dependencies of the overall job become complex, Cloud Composer (Airflow DAG) or Cloud Workflows could manage the sequence of BigQuery Stored Procedure calls and any external interactions.
*   **Core Logic:** A separate BigQuery Stored Procedure (`sp_k_ausd_v_ta_barrier_zusgf`) will eventually house the migrated logic from `k_ausd_v_ta_barrier_zusgf.ksh`.

## 4. Data Flow & Lineage

The current script acts as an orchestrator. Its data flow primarily involves control and metadata rather than direct data manipulation:

1.  **Input Parameters:** Command-line arguments (`-h`, `-s`, `-l`) for the wrapper script.
2.  **Environment Setup:** Sourcing `.dw_init` and other utility scripts for environment variables and functions.
3.  **Logging Initialization:** `DWMSG_*` functions are called to initialize log file names and job entries.
4.  **Kernel Script Invocation:** The wrapper executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh` passing `JobKennung` and `DW_EintragsNr` as parameters.
5.  **Output:** Log messages written to a runtime log file. Job status updated via `DWMSG_SetzeStatusOK`.

**Migrated Data Flow:**

1.  **Input Parameters:** BigQuery Stored Procedure parameters (`p_job_kennung`, `p_entry_number`).
2.  **Logging:** Inserts/updates to `project.dataset.job_log` table.
3.  **Core Logic Invocation:** Calls `CALL project.dataset.sp_k_ausd_v_ta_barrier_zusgf(...)`.
4.  **Target Impact:** The overall job's purpose is "reconciliation of contract data: table `ta_barrier_zusgf`". Therefore, the `sp_k_ausd_v_ta_barrier_zusgf` procedure is expected to read from and/or write to a BigQuery table named `ta_barrier_zusgf` (or its equivalent in the target schema).

## 5. Transformation Logic

The `r_ausd_v_ta_barrier_zusgf.ksh` wrapper script contains minimal data transformation logic. Its primary transformations are:

*   **Parameter Parsing:** Using `getopts` to interpret command-line arguments. In BigQuery, this will be handled by procedure parameters.
*   **Job Metadata Normalization:**
    *   `JobKennung` is converted to uppercase (`typeset -u`). In BigQuery, this is `UPPER()`.
    *   `v_sysdate` captures the current system date in `DDMMYYYY` format. In BigQuery, this is `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Logging and Error Handling:** These are handled by a custom `DWMSG_*` framework and shell `trap` commands. These will be replaced by standard BigQuery `BEGIN...EXCEPTION` blocks and inserts/updates to a BigQuery logging table.

The core data reconciliation and transformation logic is implicitly handled by the `k_ausd_v_ta_barrier_zusgf.ksh` script, which is outside the direct scope of *this* wrapper's migration but is a critical dependency for the overall job's functionality.

## 6. External Dependencies

The original script has several external dependencies:

*   **Environment Initialization (`. $HOME/.dw_init`):** Environment variables will be replaced by BigQuery Stored Procedure parameters or session variables where appropriate. Configuration tables can store static values.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** The functionality of these scripts, particularly logging and parameter processing, will be absorbed into the BigQuery Stored Procedure logic (e.g., direct SQL for logging, using procedure parameters).
*   **`DWMSG_*` Functions:** This custom logging and error handling framework will be replaced by:
    *   **Logging:** Inserts into a centralized BigQuery logging table (`project.dataset.job_log`).
    *   **Error Handling:** BigQuery's `BEGIN...EXCEPTION` blocks and `RAISE` statements.
    *   **Status Updates:** Updates to the `project.dataset.job_log` table.
*   **Operating System Commands (`date`, `tee`, `print`, `getopts`, `trap`):**
    *   `date`: Replaced by `CURRENT_DATE()` and `FORMAT_DATE()` in BigQuery.
    *   `tee`, `print`: Replaced by BigQuery `SELECT` statements (for output) and inserts into logging tables.
    *   `getopts`: Replaced by BigQuery Stored Procedure input parameters.
    *   `trap`: Replaced by BigQuery's `BEGIN...EXCEPTION` blocks.
*   **Invoked Kernel Script (`k_ausd_v_ta_barrier_zusgf.ksh`):** This script is a crucial dependency. It must be migrated separately, likely to its own BigQuery Stored Procedure (`sp_k_ausd_v_ta_barrier_zusgf`), which will then be called from the wrapper's migrated stored procedure.
*   **No other external systems (databases, APIs, filesystems) were directly identified as being used by *this specific wrapper script* in the lineage analysis.** Any such interactions would occur within the invoked kernel script.

## 7. Unresolved / Risks

*   **Migration of Kernel Script (`k_ausd_v_ta_barrier_zusgf.ksh`):** This is the most significant unresolved item. The wrapper's functionality is dependent on the core logic within this kernel script. A separate, detailed migration design and implementation are required for `k_ausd_v_ta_barrier_zusgf.ksh`. Without it, the overall job cannot be fully functional.
*   **Completeness of Sourced Utilities:** The exact functionality of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` needs to be fully understood during the migration of the kernel script, as some of their functionality might be directly relevant to the data processing.
*   **Runtime Environment Variables:** The original script relies on `HOME` and `BERT_DIR_ROOT`. These environment variables need to be correctly configured as BigQuery procedure parameters, external configuration, or derived values in the target environment.
*   **`semi_auto` Bucket:** The `semi_auto` classification indicates that some manual intervention and verification will be required during the migration process. This likely stems from the custom shell functions and the indirect invocation of the kernel script.

## 8. Build Plan

The migration will primarily involve creating BigQuery SQL objects.

1.  **Create BigQuery Logging Table (SQL):**
    *   **File:** `project.dataset.job_log.sql`
    *   **Language:** BigQuery SQL (DDL)
    *   **Content:**
        ```sql
        CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
          job_name STRING,
          job_number INT64,
          log_level STRING,
          error_code INT64,
          error_arg STRING,
          message STRING,
          status STRING,
          stichtag STRING,
          stichtag_format STRING,
          log_file_name STRING,
          script_name STRING,
          created_at TIMESTAMP,
          finished_at TIMESTAMP
        );
        ```

2.  **Create BigQuery Stored Procedure for Wrapper Script (SQL):**
    *   **File:** `sp_r_ausd_v_ta_barrier_zusgf.sql`
    *   **Language:** BigQuery SQL (DML/DDL)
    *   **Content:** Based on the provided pseudocode for the wrapper script logic, handling parameters, logging, and error traps. This will include the `CALL` to the kernel script's stored procedure.

3.  **Plan for Kernel Script Stored Procedure (SQL):**
    *   **File:** `sp_k_ausd_v_ta_barrier_zusgf.sql`
    *   **Language:** BigQuery SQL (DML/DDL)
    *   **Content:** This will be developed after a detailed analysis and migration of `k_ausd_v_ta_barrier_zusgf.ksh`. For now, a placeholder procedure should be created if needed for testing the wrapper.

4.  **Orchestration (Optional - depending on complexity):**
    *   **File:** `r_ausd_v_ta_barrier_zusgf_dag.py`
    *   **Language:** Python (Airflow DAG)
    *   **Content:** If Cloud Composer is used for orchestration, an Airflow DAG will be created to schedule and execute the `sp_r_ausd_v_ta_barrier_zusgf` BigQuery stored procedure.

This build plan outlines the components required to migrate the `r_ausd_v_ta_barrier_zusgf.ksh` wrapper script to BigQuery, acknowledging the critical dependency on the separate migration of its invoked kernel script.