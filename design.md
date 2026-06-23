# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

## 1. Purpose & Scope

This document outlines the migration design for the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`. This script serves as a wrapper or orchestration component for a contract data reconciliation job specifically for the table `ta_apn_ve`. Its primary functions include environment initialization, command-line parameter validation, setting up logging and error handling, and orchestrating the execution of a core processing script, `k_ausd_v_ta_apn_ve.ksh`. The job's overall purpose is to manage the `medium` stage distribution, as indicated by the `purpose_note`.

The scope of this migration focuses on transforming the existing KornShell script's functionality into a BigQuery-native solution, primarily using BigQuery Stored Procedures, while retaining its orchestration and error handling capabilities.

## 2. Source Inventory

The job consists of a single primary source file, which is a KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`
    *   **Technology:** KornShell
    *   **Category:** shell
    *   **Tier:** Unknown (no data available from `file_complexity`)
    *   **Automation Bucket:** `semi_auto`
    *   **Purpose:** Orchestration, parameter validation, logging, and execution of a core script.

## 3. Target Architecture

The target architecture in BigQuery will consist of:

*   **BigQuery Stored Procedures:**
    *   A main BigQuery Stored Procedure, `project.dataset.vertragsdatenabgleich_wrapper`, will replace the KornShell wrapper script (`r_ausd_v_ta_apn_ve.ksh`). This procedure will handle parameter parsing, job control, logging, and will invoke a separate BigQuery Stored Procedure for the core business logic.
    *   A separate BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_apn_ve`, will be created to encapsulate the logic originally found in the `k_ausd_v_ta_apn_ve.ksh` script. This is where the actual contract data reconciliation for `ta_apn_ve` is expected to reside.
*   **BigQuery Tables for Job Control and Logging:**
    *   `project.dataset.job_control`: A table to manage job entries, statuses, and metadata (equivalent to the shell script's internal job entry number management).
    *   `project.dataset.job_log`: A table for detailed operational logging (replaces file-based logging).
    *   `project.dataset.job_error_log`: A table dedicated to capturing error details.
*   **External Orchestration (Optional but recommended):**
    *   For complex inter-job dependencies or scheduling, Cloud Composer (Apache Airflow) or Dataform could be used to orchestrate the execution of the `vertragsdatenabgleich_wrapper` BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The current script acts as an orchestrator, and its direct data flow within the script is minimal, focusing on control rather than data transformation.

**Current (Legacy) Data Flow:**

1.  **Environment Setup:** The `r_ausd_v_ta_apn_ve.ksh` script sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for environment variables, error handling functions, and date utilities.
2.  **Parameter Processing:** Command-line arguments (`-h`, `-s`, `-l`) are parsed using `getopts`.
3.  **Job Initialization:** Custom shell functions/macros (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`) are used to establish job context, generate a log file name, and record an initial job entry.
4.  **Error Handling:** `trap` commands are set up to catch `INT` (interrupt) and `ERR` (error) signals, invoking `DWMSG_Fehlerbehandlung` for error logging.
5.  **Core Logic Invocation:** The script executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh`, passing `JobKennung` and `DW_EintragsNr` as parameters. All standard output and error from this core script are redirected to the dynamically generated log file.
6.  **Status Update:** Upon successful completion of the core script, a success message is logged, and the job status is updated via `DWMSG_SetzeStatusOK`.

**Target (BigQuery) Data Flow:**

1.  **Orchestration Layer:** (e.g., Cloud Composer DAG, Dataform, or manual execution) invokes the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery Stored Procedure.
2.  **Wrapper Stored Procedure (`vertragsdatenabgleich_wrapper`):**
    *   Receives input parameters (e.g., `p_h`, `p_s`, `p_l`).
    *   Validates parameters and raises SQLSTATE errors for invalid inputs.
    *   Manages job metadata in `project.dataset.job_control` (e.g., increments `DW_EintragsNr`, records `JobKennung`, `script_name`, `stichtag`).
    *   Inserts log messages into `project.dataset.job_log` and error messages into `project.dataset.job_error_log`.
    *   Calls the core logic Stored Procedure: `CALL project.dataset.k_ausd_v_ta_apn_ve(JobKennung, DW_EintragsNr);`.
    *   Handles exceptions using `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, updating job status and logging errors.
    *   Updates the final job status in `project.dataset.job_control` to 'OK' or 'ERROR'.
3.  **Core Logic Stored Procedure (`k_ausd_v_ta_apn_ve`):**
    *   This procedure will perform the actual contract data reconciliation for `ta_apn_ve`. Its specific data reads, writes, and transformations will be detailed in its own migration design document. It is expected to read source data and write processed data, likely into a target `ta_apn_ve` table or related staging tables in BigQuery.

## 5. Transformation Logic

The current script (`r_ausd_v_ta_apn_ve.ksh`) contains primarily orchestration logic rather than data transformation. The transformation logic will be applied as follows:

*   **Parameter Handling:**
    *   Legacy: `getopts` for command-line arguments.
    *   Target: BigQuery Stored Procedure input parameters (`IN p_h STRING`, `IN p_s STRING`, `IN p_l STRING`).
*   **Environment Initialization:**
    *   Legacy: Sourcing `$HOME/.dw_init` and other `BERT_DIR_ROOT` utility scripts.
    *   Target: These environment variables and sourced functions will need to be re-evaluated. Generic path definitions can be hardcoded or managed via BigQuery constants/configuration tables. Specific utility functions (like `DWMSG_*`) will be reimplemented as SQL procedures or incorporated as direct `INSERT` statements into logging tables.
*   **Job Identifiers and Logging:**
    *   Legacy: Shell variables (`DW_EintragsNr`, `JobKennung`, `LogDatei`) and custom shell functions (`DWMSG_*`).
    *   Target: BigQuery `DECLARE` variables for job identifiers. Logging will be replaced by `INSERT` statements into `job_control`, `job_log`, and `job_error_log` tables. Date formatting will use BigQuery's `FORMAT_DATE` and `CURRENT_DATE()` functions.
*   **Error Handling:**
    *   Legacy: `set -eu`, `if [ ! $ErrNr -eq 0 ]`, `trap INT ERR`, `DWMSG_MeldeFehler`, `exit $ErrNr`.
    *   Target: `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks in BigQuery Stored Procedures. `SIGNAL SQLSTATE` will be used to propagate errors. Custom error numbers (`ErrNr=193`, `ErrNr=192`) will be mapped to specific error messages or custom error codes within the BigQuery error logging table.
*   **Core Script Invocation:**
    *   Legacy: `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`.
    *   Target: `CALL project.dataset.k_ausd_v_ta_apn_ve(JobKennung, DW_EintragsNr);`. This assumes `k_ausd_v_ta_apn_ve.ksh` is also migrated to a BigQuery Stored Procedure.

## 6. External Dependencies

The current script itself does not interact with traditional external systems like databases, SFTP, or S3 directly. Its dependencies are primarily on the local filesystem and other shell scripts.

*   **Sourced Utility Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):**
    *   **Legacy:** Local shell scripts and environment configuration files.
    *   **Replacement:**
        *   Environment variables from `.dw_init` should be explicitly defined within the BigQuery Stored Procedure (e.g., as `DECLARE` variables or parameters) or managed via BigQuery configuration tables.
        *   Utility functions like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` need to be reimplemented in BigQuery SQL procedural language or their functionalities absorbed into the `vertragsdatenabgleich_wrapper` procedure and its logging tables. Generic date functions are available natively in BigQuery.
*   **Core Script (`k_ausd_v_ta_apn_ve.ksh`):**
    *   **Legacy:** Another KornShell script invoked by the wrapper.
    *   **Replacement:** Will be migrated to a dedicated BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_apn_ve`. Any external system interactions (e.g., database reads/writes) performed by this core script will be handled within its corresponding BigQuery procedure.

## 7. Unresolved / Risks

*   **Tier & Migration Flags:** The `file_complexity` table returned no rows, meaning the tier and specific migration flags for `r_ausd_v_ta_apn_ve.ksh` are unknown. This might indicate that a deeper manual analysis of complexity is required if the automation bucket `semi_auto` proves challenging.
*   **Content of `k_ausd_v_ta_apn_ve.ksh`:** The actual data transformation logic for `ta_apn_ve` resides in `k_ausd_v_ta_apn_ve.ksh`. The complexity and specific dependencies of this script are critical for a complete migration, and this design document only addresses the wrapper. This is the main dependency for successful migration of the entire job.
*   **Sourced Script Logic:** The exact logic within `$HOME/.dw_init` and the `BERT_DIR_ROOT` utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs to be fully understood to ensure accurate replication in BigQuery. If these scripts contain complex logic beyond simple variable definitions or generic error handling, their translation could introduce additional complexity.
*   **Error Code Mapping:** The numerical error codes (192, 193) used in the KornShell script need clear mapping to BigQuery error handling conventions and the `job_error_log` table.
*   **External Orchestration:** While BigQuery Stored Procedures can be executed manually, a robust solution will require external orchestration (e.g., Cloud Composer, Dataform) for scheduling and dependency management, especially if this job is part of a larger workflow.

## 8. Build Plan

The migration will involve creating BigQuery artifacts.

1.  **Define BigQuery Datasets:** Create the target BigQuery dataset (e.g., `project.dataset`) where all procedures and tables will reside. (Language: DDL)
2.  **Create Job Control and Logging Tables:**
    *   `project.dataset.job_control` (DDL)
    *   `project.dataset.job_log` (DDL)
    *   `project.dataset.job_error_log` (DDL)
3.  **Develop Core Logic Stored Procedure:**
    *   **File:** `bigquery/stored_procedures/k_ausd_v_ta_apn_ve.sql`
    *   **Language:** BigQuery SQL
    *   **Description:** Migrate the business logic from `k_ausd_v_ta_apn_ve.ksh` into this procedure. This will likely involve extensive SQL transformations, joins, and DML operations.
4.  **Develop Wrapper Stored Procedure:**
    *   **File:** `bigquery/stored_procedures/vertragsdatenabgleich_wrapper.sql`
    *   **Language:** BigQuery SQL
    *   **Description:** Implement the wrapper logic as described in the Transformation Logic section, including parameter parsing, job control table updates, logging, error handling, and the call to `k_ausd_v_ta_apn_ve`.
5.  **Implement Utility Functions/Configuration:**
    *   If any generic utility functions from `BERT_DIR_ROOT` scripts are needed as separate procedures/UDFs, create them here.
    *   If configuration tables are preferred over hardcoded values for certain parameters, create and populate them. (Language: BigQuery SQL)
6.  **Develop Orchestration (Optional but recommended):**
    *   **File:** `composer/dags/vertragsdatenabgleich_dag.py`
    *   **Language:** Python (for Apache Airflow on Cloud Composer)
    *   **Description:** Create a DAG to schedule and trigger the `project.dataset.vertragsdatenabgleich_wrapper` stored procedure, handling any external dependencies or scheduling requirements.