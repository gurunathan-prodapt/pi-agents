# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bcp_msisdn.ksh`.
The script's primary purpose is to orchestrate the initial provisioning of selected basic products for BERT. It functions as a wrapper, handling command-line parameter parsing (`getopts`), date determination, error handling, and logging. Its core function is to invoke a "kernel" script, `k_ausd_bp_ta_bcp_msisdn.ksh`, which is expected to contain the main business logic for generating a cutoff-date extraction of contract cache data from DWH for scoring/FOS processing.

The scope of this migration design specifically covers the `r_ausd_bp_ta_bcp_msisdn.ksh` wrapper script, with a placeholder for the invoked kernel script. The target platform is Google Cloud Platform, specifically BigQuery for data processing and Cloud Composer (Airflow) for orchestration.

## 2. Source Inventory
The primary source file for this job is:
*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh**
    *   **Technology**: KornShell (ksh)
    *   **Description**: Orchestration script for initial BERT product provisioning.
    *   **Analysis**: `file_analysis` indicates `category: shell`, `tool: KornShell`, and `primary_bucket: pipeline_orchestrator`. It identifies transformation patterns related to parameter reformatting and orchestration. The `gcp_target_hint` is `Cloud Composer (Airflow)`.
    *   **Complexity Tier**: (Not available from `file_complexity` query)
    *   **Automation Bucket**: (Not available from `automation_rate` query)

This script `sources` several utility KornShell scripts and `invokes` one primary KornShell script:
*   `$HOME/.dw_init` (environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling framework)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling utilities)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)
*   `k_ausd_bp_ta_bcp_msisdn.ksh` (the core business logic script, invoked by this wrapper)

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud Platform services:
*   **BigQuery Stored Procedures**: The orchestration logic, parameter handling, date determination, and error management will be re-implemented as a BigQuery Stored Procedure. This SP will accept parameters corresponding to the original script's command-line arguments.
*   **BigQuery Tables for Logging/Auditing**: Dedicated BigQuery tables will replace the file-based logging and status tracking (`job_control`, `job_log`, `job_error_log`). These tables will store job metadata, execution status, and detailed logs.
*   **BigQuery Date Functions**: Legacy date calculations will be replaced with native BigQuery SQL date and time functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
*   **Cloud Composer (Airflow) / Cloud Workflows**: The overall orchestration of this job, including scheduling and triggering the BigQuery Stored Procedure, will be managed by Cloud Composer (Airflow) or Cloud Workflows. This aligns with the `Cloud Composer (Airflow)` hint from the source analysis.
*   **BigQuery Stored Procedure for Kernel Logic**: The invoked `k_ausd_bp_ta_bcp_msisdn.ksh` script, once analyzed, will also be migrated into a separate BigQuery Stored Procedure, which will be called by the migrated wrapper SP.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bcp_msisdn.ksh` acts as an entry point and orchestrator.
1.  **Environment Setup**: The script first sources environment initialization and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). These provide functions for error handling, parameter parsing, and date manipulation.
2.  **Parameter Input**: Command-line arguments `-s` (Stichtag/cutoff date) and `-l` (restart value) are parsed using `getopts`.
3.  **Date Determination**: The script determines the current system date and, if no Stichtag is provided, defaults the Stichtag to the system date. This involves the `DWDate_Gib_Zeitraum` function.
4.  **Parameter Validation**: Inputs are validated, and if errors are found, an error message is logged using `DWMSG_MeldeFehler`, and the script exits.
5.  **Logging and Error Trapping**: A job entry number is determined, and a log file name is generated. Traps are set using the `trap` command to handle unexpected script termination, invoking `DWMSG_Fehlerbehandlung` for error logging. Job metadata is printed to standard output and the log file.
6.  **Kernel Script Invocation**: The core processing is delegated to `k_ausd_bp_ta_bcp_msisdn.ksh`, which is invoked with parsed parameters (`-j`, `-s`, `-f`, `-l`) and its output redirected to the log file.
7.  **Status Update**: Upon successful completion of the kernel script, a success message is logged, and the job status is updated via `DWMSG_SetzeStatusOK`. Error handling paths also update the status accordingly.

In the target BigQuery architecture:
*   An Airflow DAG will trigger the main BigQuery Stored Procedure (`sp_ausd_bp_ta_bcp_msisdn`).
*   This SP will receive parameters directly.
*   It will manage logging by inserting records into `job_control`, `job_log`, and `job_error_log` tables.
*   It will call the kernel BigQuery Stored Procedure (`sp_ausd_bp_ta_bcp_msisdn_kernel`) with the appropriate parameters.
*   Error handling will use BigQuery's `EXCEPTION WHEN ERROR` block.

## 5. Transformation Logic
The transformation logic of `r_ausd_bp_ta_bcp_msisdn.ksh` is primarily orchestrational and parameter-driven.
*   **Parameter Parsing**: `getopts` is used to read `-s` (Stichtag DDMMYYYY) and `-l` (Wiederanlaufwert) parameters.
    *   **Migration**: These will become direct `IN` parameters to the BigQuery Stored Procedure (`p_stichtag STRING`, `p_wiederanlaufWert INT64`). BigQuery SQL provides procedural constructs to handle defaulting and validation.
*   **Defaulting Logic**: The `p_wiederanlaufWert` defaults to `0` if not set. The `p_stichtag` defaults to `v_sysdate` if not provided.
    *   **Migration**: This will be implemented using `IF ... IS NULL THEN SET ... END IF` blocks within the BigQuery Stored Procedure.
*   **Date Derivation**: `DWDate_Gib_Zeitraum` is used to get the system date.
    *   **Migration**: Replaced by BigQuery functions like `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Parameter Validation**: Basic checks ensure required parameters are set.
    *   **Migration**: `IF ... THEN` blocks will check for `NULL` or empty values, and `SIGNAL SQLSTATE` will be used to raise errors.
*   **Logging & Error Handling**: `DWMSG_*` functions and `trap` commands manage logging, status, and error reporting to a file.
    *   **Migration**: Replaced by `INSERT` and `UPDATE` statements into dedicated BigQuery logging/auditing tables (`job_control`, `job_log`, `job_error_log`). BigQuery's `EXCEPTION WHEN ERROR` block will handle runtime errors.
*   **Kernel Script Invocation**: The script's main action is to execute `k_ausd_bp_ta_bcp_msisdn.ksh`.
    *   **Migration**: This will translate to a `CALL` statement to the migrated BigQuery Stored Procedure for the kernel logic (`sp_ausd_bp_ta_bcp_msisdn_kernel`).

## 6. External Dependencies
The script has several dependencies:
*   **Sourced Utility Scripts**:
    *   `$HOME/.dw_init`: Likely sets environment variables.
    *   `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: Provide functions for error handling, parameter parsing, and date manipulation.
    *   **Migration Strategy**: These utility functions will need to be re-implemented as BigQuery SQL functions or stored procedures (e.g., date formatting, parameter validation helpers) or handled by the orchestration layer (e.g., environment variable settings in Airflow). Logging functions (`DWMSG_*`) will be replaced by direct BigQuery table inserts.
*   **Invoked Kernel Script**:
    *   `k_ausd_bp_ta_bcp_msisdn.ksh`: Contains the actual business transformation logic.
    *   **Migration Strategy**: This script will be migrated as a separate BigQuery Stored Procedure (`sp_ausd_bp_ta_bcp_msisdn_kernel`). Its inputs and outputs will determine its specific BigQuery SQL implementation.
*   **File-based Logging**: The script writes extensively to a log file.
    *   **Migration Strategy**: Replaced by dedicated BigQuery tables for job control, logging, and error tracking, ensuring structured and queryable logs.

## 7. Unresolved / Risks
*   **Missing Complexity/Automation Data**: The `file_complexity` and `automation_rate` queries returned no rows. This means the migration effort and automation bucket are unknown, potentially impacting project planning.
*   **Kernel Script Details**: The actual transformation logic of `k_ausd_bp_ta_bcp_msisdn.ksh` is not part of this analysis. Its complexity, data sources (tables it reads from), and targets (tables it writes to) are critical for a complete migration design. This is the biggest unresolved item.
*   **Shell-specific Features**: Shell `trap` commands for error handling are not directly replicable in BigQuery SQL. While `EXCEPTION WHEN ERROR` covers errors within the SP, external orchestration (Airflow) will need to handle job-level failures.
*   **`$HOME/.dw_init` contents**: The exact environment variables and configurations set by `$HOME/.dw_init` need to be identified and translated into BigQuery runtime variables, Airflow variables, or other GCP configuration mechanisms.
*   **`DWDate_Gib_Zeitraum` exact logic**: While identified as a date helper, its full implementation to determine "MIN(sysdate,maxladedatum)" if stichtag is not set is critical. The current pseudocode simplifies it to `v_sysdate`, but if `maxladedatum` comes from a database table, that interaction needs to be re-established in BigQuery.

## 8. Build Plan
The build plan for this migration involves the following steps:

1.  **Define BigQuery Schema**:
    *   Create `job_control` table: To store overall job status, start/end times, and parameters.
    *   Create `job_log` table: For detailed informational, warning, and error messages.
    *   Create `job_error_log` table: Specifically for capturing detailed error information during parameter validation and runtime exceptions.

2.  **Develop BigQuery Stored Procedure for `r_ausd_bp_ta_bcp_msisdn.ksh`**:
    *   Create a stored procedure `sp_ausd_bp_ta_bcp_msisdn` in BigQuery SQL (e.g., in a `project.dataset` scope).
    *   Implement parameter handling (e.g., `p_stichtag STRING`, `p_wiederanlaufWert INT64`).
    *   Replicate defaulting logic for `p_wiederanlaufWert` and `p_stichtag`.
    *   Translate date derivation using `CURRENT_DATE()` and `FORMAT_DATE()`.
    *   Implement parameter validation using `IF` statements and `SIGNAL SQLSTATE` for error handling.
    *   Replace `DWMSG_*` calls with `INSERT` statements into the `job_log`, `job_control`, and `job_error_log` tables.
    *   Include a `CALL` statement to the future kernel stored procedure: `CALL project.dataset.sp_ausd_bp_ta_bcp_msisdn_kernel(...)`.
    *   Implement robust `EXCEPTION WHEN ERROR` blocks to update `job_control` and `job_log` with error status.

3.  **Migrate `k_ausd_bp_ta_bcp_msisdn.ksh` (Core Logic)**:
    *   This is a separate task. The content of `k_ausd_bp_ta_bcp_msisdn.ksh` needs to be analyzed, and its logic translated into a BigQuery Stored Procedure, `sp_ausd_bp_ta_bcp_msisdn_kernel`, that performs the data transformations. This will likely involve reading from source tables and writing to target tables within BigQuery.

4.  **Develop Orchestration Layer (Cloud Composer / Airflow DAG)**:
    *   Create an Airflow DAG that schedules and triggers the `sp_ausd_bp_ta_bcp_msisdn` BigQuery Stored Procedure.
    *   Configure tasks to pass necessary parameters to the SP.
    *   Implement Airflow sensors or operators to monitor the BigQuery job status and handle retries or notifications.

5.  **Testing**:
    *   Unit tests for the BigQuery Stored Procedures to verify parameter handling, date logic, and logging.
    *   Integration tests to ensure the Airflow DAG correctly triggers the BigQuery SP and captures its output/status.
    *   Data validation tests to compare results produced by the legacy system with the migrated BigQuery solution.

This design provides a clear path for migrating the orchestration wrapper script, with the understanding that the core transformation in `k_ausd_bp_ta_bcp_msisdn.ksh` will require its own detailed migration effort.