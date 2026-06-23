# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_cntrct_crs.ksh`, an orchestration wrapper for a contract data reconciliation process. The script's primary function is to manage job execution, handle parameters, set up logging, and invoke a core data processing script (`k_ausd_v_ta_cntrct_crs.ksh`).

The migration target is **Google BigQuery**. The original KornShell script has been refactored into a BigQuery Stored Procedure (`project.dataset.vertragsdatenabgleich_wrapper`) that encapsulates the orchestration logic. Logging and job control, previously managed by shell scripts and flat files, are now handled by dedicated BigQuery tables (`project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log`). The core data processing logic, originally in `k_ausd_v_ta_cntrct_crs.ksh`, is represented by a placeholder BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_cntrct_crs_stub`) that will be fully implemented in a subsequent phase.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`ddl/job_control_tables.sql`**
    *   **Role**: Defines the schema for the new BigQuery tables used for job orchestration, logging, and error tracking.
    *   `project.dataset.job_control`: Stores metadata for each job execution (ID, status, start/end times, reference date).
    *   `project.dataset.job_log`: Captures detailed informational, warning, and error messages during job execution.
    *   `project.dataset.job_error_log`: Records specific error details when a job fails.

*   **`procedures/k_ausd_v_ta_cntrct_crs_stub.sql`**
    *   **Role**: A placeholder BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_cntrct_crs`) that represents the migrated core processing logic.
    *   Currently, it contains only logging statements to indicate its start and completion. The actual business logic from the original `k_ausd_v_ta_cntrct_crs.ksh` needs to be translated and implemented within this procedure.

*   **`procedures/vertragsdatenabgleich_wrapper.sql`**
    *   **Role**: The main BigQuery Stored Procedure (`project.dataset.vertragsdatenabgleich_wrapper`) that replaces the original `r_ausd_v_ta_cntrct_crs.ksh` wrapper script.
    *   It handles:
        *   Parsing and validating input parameters (replacing `getopts`).
        *   Generating unique job identifiers (`DW_EintragsNr`).
        *   Initializing job metadata in `job_control`.
        *   Logging execution progress and status to `job_log`.
        *   Setting the reference date (`Stichtag`).
        *   Invoking the core processing procedure (`k_ausd_v_ta_cntrct_crs`).
        *   Implementing robust error handling using BigQuery's `EXCEPTION WHEN ERROR THEN` blocks, recording failures in `job_error_log`, and updating `job_control` status.

## 3. Key design decisions

*   **Orchestration to BigQuery Stored Procedure**: The KornShell wrapper's orchestration logic (parameter handling, environment setup, job metadata, error handling) was directly translated into a BigQuery Stored Procedure. This leverages BigQuery's native capabilities for procedural logic and avoids introducing external orchestration for this wrapper layer.
*   **Centralized Logging and Control Tables**: Instead of shell script-managed log files and environment variables, a structured approach using BigQuery tables (`job_control`, `job_log`, `job_error_log`) was adopted. This provides a centralized, queryable, and persistent record of job executions, status, and detailed logs.
*   **BigQuery Exception Handling**: The shell script's `trap` mechanism for error handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This allows for structured error capture, logging, and status updates within the BigQuery environment.
*   **Stub for Core Logic**: The core processing script (`k_ausd_v_ta_cntrct_crs.ksh`) was migrated as a placeholder (stub) procedure. This decision was made because the content and complexity of the core script were not fully analyzed, and its migration might require different approaches (e.g., Python/Cloud Composer if it contains complex non-SQL logic). This allows the wrapper migration to proceed independently.
*   **Parameter Mapping**: Command-line arguments of the original script are mapped to input parameters of the BigQuery Stored Procedure, providing a clear interface for execution.
*   **Replacement of Utility Scripts**: The functionality of sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is either absorbed into the main wrapper procedure (e.g., date functions, parameter handling) or will be implemented as separate BigQuery UDFs/procedures as needed (e.g., advanced error messaging).

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
    ```sql
    CREATE SCHEMA IF NOT EXISTS `project.dataset`;
    ```
2.  **Deploy DDL**: Execute the `ddl/job_control_tables.sql` script to create the `job_control`, `job_log`, and `job_error_log` tables in the target dataset.
3.  **Deploy Stored Procedures**: Execute the `procedures/k_ausd_v_ta_cntrct_crs_stub.sql` and `procedures/vertragsdatenabgleich_wrapper.sql` scripts to create the respective BigQuery Stored Procedures.
4.  **IAM Permissions**:
    *   The service account or user executing the `vertragsdatenabgleich_wrapper` procedure must have `bigquery.dataEditor` permissions on the `project.dataset` to `INSERT`, `UPDATE`, `SELECT` from the `job_control`, `job_log`, and `job_error_log` tables, and `bigquery.routines.call` permission on `project.dataset.k_ausd_v_ta_cntrct_crs`.
    *   Ensure the service account has appropriate permissions for any data sources or sinks that `k_ausd_v_ta_cntrct_crs` will interact with (e.g., `bigquery.dataEditor` on other datasets, `storage.objectViewer`/`storage.objectCreator` for GCS buckets).
5.  **Connection Strings/Configuration**: No explicit connection strings are needed for BigQuery native operations. However, ensure that the `project.dataset` placeholders in the generated code are updated to reflect the actual BigQuery project and dataset IDs.
6.  **Secrets Management**: If the original `k_ausd_v_ta_cntrct_crs.ksh` or any of its dependencies used secrets (e.g., database passwords, API keys), these must be migrated to a secure secrets management solution like Google Secret Manager and integrated into the BigQuery procedures or any external orchestration (e.g., Cloud Composer).
7.  **Scheduling**:
    *   **BigQuery Scheduled Query**: If the job runs on a simple schedule, configure a BigQuery Scheduled Query to `CALL` `project.dataset.vertragsdatenabgleich_wrapper` with the required parameters.
    *   **Cloud Composer (Airflow)**: For more complex scheduling, dependencies, or if the core logic requires Python/external execution, create an Airflow DAG in Cloud Composer to orchestrate the execution of the BigQuery Stored Procedure.
8.  **Complete `k_ausd_v_ta_cntrct_crs` Migration**: The most critical manual step is to fully analyze the original `k_ausd_v_ta_cntrct_crs.ksh` script and translate its business logic into the `project.dataset.k_ausd_v_ta_cntrct_crs` BigQuery Stored Procedure. This may involve writing complex SQL, creating additional UDFs, or even deciding to implement parts in Python if the logic is not SQL-friendly.

## 5. Known gaps & unresolved references

*   **Full Migration of `k_ausd_v_ta_cntrct_crs.ksh`**: The core processing logic is currently a stub. Its complete migration is pending and represents the largest remaining task. This includes:
    *   Translating all SQL statements.
    *   Identifying and migrating any non-SQL shell logic (e.g., file manipulations, external program calls) which might necessitate a Python-based solution orchestrated by Cloud Composer.
*   **Complexity of Sourced Utility Scripts**: The exact functionality of the original KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) was not fully analyzed. While basic error handling and date functions are covered, any advanced features or specific `DWMSG_` functions might need dedicated BigQuery UDFs or procedures.
*   **Unused Parameters (`-s`, `-l`)**: The original `getopts` declaration included `-s` and `-l` options, but the script did not implement any logic for them. The migrated procedure includes placeholders (`p_some_param_s`, `p_some_param_l`) but they remain unused, mirroring the original behavior. Clarification is needed if these parameters were intended for future use or are dead code.
*   **`LogDatei` as Conceptual Reference**: The `LogDatei` variable in the original script referred to a physical file. In the migrated solution, `log_file_name` in `job_control` is a conceptual field. All logging is directed to BigQuery tables, which offer superior queryability and persistence. If a physical file output is still required for external systems, an additional export mechanism would be needed.
*   **Environment Variable Replacement**: The `.dw_init` script likely sets various environment variables. These need to be identified and either replaced by BigQuery procedure parameters, BigQuery session variables, or configuration tables.

## 6. Validation

To validate the migrated `vertragsdatenabgleich_wrapper` procedure, execute it with various scenarios and verify the outcomes in the BigQuery logging tables.

**How to run the tests:**

1.  **Success Scenario**:
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_SUCCESS');
    ```
2.  **Help Scenario**:
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_show_help => TRUE);
    ```
3.  **Parameter Validation Failure (Missing `p_job_kennung`)**:
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => NULL);
    -- Or:
    -- CALL `project.dataset.vertragsdatenabgleich_wrapper`();
    ```
4.  **Simulated Core Logic Failure (requires modification to `k_ausd_v_ta_cntrct_crs_stub.sql`)**:
    Temporarily modify `k_ausd_v_ta_cntrct_crs_stub.sql` to `RAISE_ERROR` under a specific condition (e.g., `IF p_job_kennung = 'TEST_FAIL_CORE' THEN RAISE_ERROR('Simulated core failure'); END IF;`). Then execute:
    ```sql
    CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => 'TEST_FAIL_CORE');
    ```

**What "passing" means:**

*   **`job_control` table**:
    *   A new entry is created for each execution.
    *   `job_id` is unique and incrementing.
    *   `program_name`, `program_version`, `job_kennung`, `start_time` are correctly populated.
    *   For successful runs, `status` is 'OK' and `end_time` is populated.
    *   For failed runs, `status` is 'ERROR' and `end_time` is populated.
    *   `reference_date` (Stichtag) is correctly set to `CURRENT_DATE()`.
*   **`job_log` table**:
    *   Contains a sequence of log messages for each `job_id`.
    *   Messages reflect the execution flow (start, reference date, core script invocation, completion).
    *   `log_level` is appropriate ('INFO', 'ERROR').
*   **`job_error_log` table**:
    *   For failed runs, an entry exists with the corresponding `job_id`.
    *   `error_timestamp`, `error_code`, `error_message`, and `error_details` are accurately captured.
*   **Help Output**: When `p_show_help` is `TRUE`, the `SELECT` statements for usage and options are returned as results, and no further processing occurs.
*   **Core Logic (once implemented)**: The `k_ausd_v_ta_cntrct_crs` procedure, once fully migrated, should produce the expected data transformations and outputs.

## 7. Rollback procedure

In case of issues during or after go-live, the following steps can be taken to roll back the migration:

1.  **Disable New Scheduling**: Immediately disable any BigQuery Scheduled Queries or Cloud Composer DAGs that invoke `project.dataset.vertragsdatenabgleich_wrapper`.
2.  **Re-enable Original Job**: Re-enable the original `r_ausd_v_ta_cntrct_crs.ksh` KornShell script in its previous environment and scheduling system (e.g., UC4).
3.  **Delete BigQuery Procedures**: Delete the migrated BigQuery Stored Procedures:
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.vertragsdatenabgleich_wrapper`;
    DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_v_ta_cntrct_crs`;
    ```
4.  **Delete BigQuery Tables (Optional but Recommended for Clean-up)**: If the logging and control tables are not needed for historical analysis or debugging, they can be dropped. **Caution**: This will delete all historical job execution data.
    ```sql
    DROP TABLE IF EXISTS `project.dataset.job_control`;
    DROP TABLE IF EXISTS `project.dataset.job_log`;
    DROP TABLE IF EXISTS `project.dataset.job_error_log`;
    ```
5.  **Remove IAM Permissions**: Revoke any BigQuery IAM permissions granted specifically for the migrated job's service account or users.

This rollback procedure ensures that the original job can resume operation while the migrated components are removed from the BigQuery environment.