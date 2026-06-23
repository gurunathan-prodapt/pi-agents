# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_bpr_apn.ksh`. The original script served as an orchestration wrapper, handling parameter parsing, environment setup, logging, and invoking a core business logic script (`k_ausd_bp_ta_bpr_apn.ksh`) responsible for provisioning selected base products for BERT (Basisprodukte für BERT) from the Data Warehouse (DWH) to "Forderungsscoring" (FOS).

The job has been migrated from a KornShell environment to **Google BigQuery**. The wrapper logic is now encapsulated within a BigQuery Stored Procedure, leveraging BigQuery's native capabilities for data processing, logging, and error handling.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`sql/ddl/job_control.sql`**
    *   **Role**: This DDL script creates the `job_control` table in BigQuery. This table serves as a centralized logging and metadata repository for job executions, replacing the file-based logging and status tracking of the original KornShell script. It records job start/end times, parameters, status, and error messages.
*   **`sql/stored_procedures/ausd_bp_ta_bpr_apn_wrapper.sql`**
    *   **Role**: This SQL script defines the `ausd_bp_ta_bpr_apn_wrapper` BigQuery Stored Procedure. This procedure is the direct migration of the `r_ausd_bp_ta_bpr_apn.ksh` wrapper script. It handles:
        *   Accepting input parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Applying default values for parameters.
        *   Performing parameter validation.
        *   Logging job execution details (start, parameters, status) to the `job_control` table.
        *   Orchestrating the call to the core business logic, which is expected to be another BigQuery Stored Procedure (e.g., `k_ausd_bp_ta_bpr_apn`).
        *   Handling exceptions and updating the `job_control` table with error information.

## 3. Key design decisions

The following key design decisions guided the migration to BigQuery:

*   **Orchestration via BigQuery Stored Procedures**: The KornShell wrapper's primary role was orchestration. This was directly translated into a BigQuery Stored Procedure (`ausd_bp_ta_bpr_apn_wrapper`) to leverage BigQuery's native SQL capabilities for control flow, parameter handling, and error management, keeping the logic within the data platform.
*   **Centralized `job_control` Table for Logging**: Instead of disparate file-based logs, a dedicated `job_control` BigQuery table was introduced. This provides a structured, queryable, and centralized repository for all job execution metadata, status, and error messages, significantly improving observability and troubleshooting.
*   **Replacement of Shell Utilities with BigQuery Native Functions**: All KornShell utility script functionalities (e.g., `h_alis_date.ksh` for date manipulation, `h_alis_parameter.ksh` for parameter parsing) were replaced by equivalent BigQuery SQL functions (`CURRENT_DATE()`, `PARSE_DATE()`, `FORMAT_DATE()`) and Stored Procedure parameter mechanisms.
*   **Delegation of Core Business Logic**: The original script was a wrapper for `k_ausd_bp_ta_bpr_apn.ksh`. The migrated BigQuery wrapper (`ausd_bp_ta_bpr_apn_wrapper`) maintains this separation of concerns by calling a *separate* BigQuery Stored Procedure (e.g., `k_ausd_bp_ta_bpr_apn`) that will contain the migrated core business logic. This ensures modularity and allows for independent migration and testing of the core functionality.
*   **Robust Error Handling**: The `set -e` and `trap` mechanisms of KornShell were replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides structured error capture, allowing for consistent logging of error messages to the `job_control` table and controlled propagation of exceptions.
*   **Parameter Handling and Defaulting**: Command-line arguments (`-s`, `-l`) are now `IN` parameters to the BigQuery Stored Procedure. Defaulting logic for `p_stichtag` (to `CURRENT_DATE()`) and `p_wiederanlaufWert` (to `0`) is implemented directly within the procedure using `IF` statements.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset` in the generated code, e.g., `your_gcp_project.your_data_warehouse_dataset`) exists. If not, create it.
2.  **Deploy `job_control` Table**: Execute the `sql/ddl/job_control.sql` script in your target BigQuery dataset to create the `job_control` table.
    ```bash
    bq query --use_legacy_sql=false --project_id=<your-gcp-project> < sql/ddl/job_control.sql
    ```
3.  **Deploy `ausd_bp_ta_bpr_apn_wrapper` Stored Procedure**: Execute the `sql/stored_procedures/ausd_bp_ta_bpr_apn_wrapper.sql` script in your target BigQuery dataset to create the wrapper stored procedure.
    ```bash
    bq query --use_legacy_sql=false --project_id=<your-gcp-project> < sql/stored_procedures/ausd_bp_ta_bpr_apn_wrapper.sql
    ```
4.  **Migrate and Deploy Core Business Logic (`k_ausd_bp_ta_bpr_apn`)**:
    *   **Crucial Prerequisite**: The core business logic script `k_ausd_bp_ta_bpr_apn.ksh` **must be migrated to a BigQuery Stored Procedure** (e.g., `project.dataset.k_ausd_bp_ta_bpr_apn`) and deployed to the same BigQuery dataset. The `ausd_bp_ta_bpr_apn_wrapper` procedure will `CALL` this migrated component.
    *   Ensure the signature of the migrated `k_ausd_bp_ta_bpr_apn` SP matches the `CALL` statement in the wrapper: `(v_job_kennung, FORMAT_DATE('%d%m%Y', v_stichtag), v_job_nr, v_wiederanlaufWert)`.
5.  **IAM/Permissions**:
    *   The service account or user executing the BigQuery Stored Procedures must have `BigQuery Data Editor` or `BigQuery Admin` roles on the target dataset (`project.dataset`) to create/update tables and execute stored procedures.
    *   Specifically, permissions to `INSERT` and `UPDATE` into `job_control` and `EXECUTE PROCEDURE` for both `ausd_bp_ta_bpr_apn_wrapper` and `k_ausd_bp_ta_bpr_apn` are required.
6.  **Orchestration/Scheduling**:
    *   Set up an external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, Dataform) to schedule the execution of the `ausd_bp_ta_bpr_apn_wrapper` BigQuery Stored Procedure.
    *   Configure the orchestrator to pass the `p_stichtag` and `p_wiederanlaufWert` parameters as needed. For example, an Airflow `BigQueryExecuteStoredProcedureOperator` can be used.
7.  **Secrets Management**: If the `k_ausd_bp_ta_bpr_apn` core logic requires any sensitive credentials or connection strings, ensure they are securely managed (e.g., via Google Secret Manager) and passed to the BigQuery environment or the core SP.

## 5. Known gaps & unresolved references

*   **Core Business Logic (`k_ausd_bp_ta_bpr_apn.ksh`) Migration**: The most significant gap is that the actual data transformation and provisioning logic, residing in `k_ausd_bp_ta_bpr_apn.ksh`, is *not* part of this migration. The `ausd_bp_ta_bpr_apn_wrapper` procedure contains a placeholder `CALL` to `project.dataset.k_ausd_bp_ta_bpr_apn`. This core script must be migrated and deployed as a BigQuery Stored Procedure for the end-to-end process to function correctly.
*   **Shell Traps**: The original KornShell script utilized `trap` commands for signal handling (INT, STOP, CONT, ERR). BigQuery SQL's `EXCEPTION WHEN ERROR` block covers error handling, but complex signal-based interruption or cleanup logic does not have a direct SQL equivalent and might require external orchestration (e.g., Python in Cloud Composer) if such behavior is critical.
*   **Environment Variables**: The original script relied on sourcing `.dw_init` and using `${BERT_DIR_ROOT}` for environment setup. In BigQuery, these environment-specific configurations need to be translated into BigQuery dataset/project structures, configurable parameters in the orchestration layer, or constants within the BigQuery Stored Procedures.
*   **Proprietary Logging Framework (`DWMSG_*`)**: The original script used a custom `DWMSG_*` logging framework. While the `job_control` table provides structured logging, the exact level of detail and specific message formats from the original framework may not be fully replicated without explicit mapping.

## 6. Validation

To validate the migrated `ausd_bp_ta_bpr_apn_wrapper` BigQuery Stored Procedure:

1.  **Prerequisite**: Ensure the `job_control` table and the *placeholder* `k_ausd_bp_ta_bpr_apn` stored procedure (even if it's just a no-op for initial testing) are deployed.
2.  **Execution**: Call the `ausd_bp_ta_bpr_apn_wrapper` procedure from the BigQuery console or via the `bq` command-line tool.

    *   **Test Case 1: Default `Stichtag` and `Wiederanlaufwert`**
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_apn_wrapper`(NULL, NULL);
        ```
        *   **Expected Pass**: Procedure completes successfully. `job_control` table shows a new entry with `status = 'OK'`, `stichtag` set to `CURRENT_DATE()`, and `restart_value = 0`.
    *   **Test Case 2: Specific `Stichtag` and Default `Wiederanlaufwert`**
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_apn_wrapper`('01012023', NULL);
        ```
        *   **Expected Pass**: Procedure completes successfully. `job_control` table shows a new entry with `status = 'OK'`, `stichtag = '2023-01-01'`, and `restart_value = 0`.
    *   **Test Case 3: Specific `Stichtag` and `Wiederanlaufwert`**
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_apn_wrapper`('15062023', 123);
        ```
        *   **Expected Pass**: Procedure completes successfully. `job_control` table shows a new entry with `status = 'OK'`, `stichtag = '2023-06-15'`, and `restart_value = 123`.
    *   **Test Case 4: Invalid `Stichtag`**
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_apn_wrapper`('invalid_date', NULL);
        ```
        *   **Expected Pass**: Procedure terminates with an error. `job_control` table shows a new entry with `status = 'ERROR'` and `error_message` indicating an invalid date format or parameter.
    *   **Test Case 5: Error in Called Procedure (if `k_ausd_bp_ta_bpr_apn` is implemented to fail)**
        *   If `k_ausd_bp_ta_bpr_apn` is implemented to raise an error, call the wrapper.
        *   **Expected Pass**: The wrapper procedure catches the error from `k_ausd_bp_ta_bpr_apn`, logs `status = 'ERROR'` and the error message in `job_control`, and then re-raises the error.

3.  **"Passing" Criteria**:
    *   The `ausd_bp_ta_bpr_apn_wrapper` stored procedure executes without unhandled errors.
    *   For successful runs, a new record is inserted into `project.dataset.job_control` with `status = 'OK'`, and the `stichtag`, `restart_value`, `created_at`, and `finished_at` fields are correctly populated according to the input parameters and system time.
    *   For error scenarios, a new record is inserted into `project.dataset.job_control` with `status = 'ERROR'` and `error_message` containing relevant details.
    *   The `CALL` to `project.dataset.k_ausd_bp_ta_bpr_apn` is successfully made (its internal logic's success depends on its own migration and validation).

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Disable New Orchestration**: Immediately disable or delete the new orchestration mechanism (e.g., Airflow DAG, Cloud Workflow) that calls the BigQuery `ausd_bp_ta_bpr_apn_wrapper` stored procedure.
2.  **Re-enable Original Job**: Re-enable the original KornShell script `r_ausd_bp_ta_bpr_apn.ksh` in its legacy environment. Ensure its scheduling and dependencies are restored.
3.  **Delete BigQuery Stored Procedures**: Delete the migrated BigQuery Stored Procedures from the target dataset.
    ```bash
    bq rm -f -r `project.dataset.ausd_bp_ta_bpr_apn_wrapper`
    bq rm -f -r `project.dataset.k_ausd_bp_ta_bpr_apn` (if it was deployed)
    ```
4.  **Optional: Archive/Delete `job_control` Table**: Depending on data retention policies, you may choose to archive or delete the `project.dataset.job_control` table.
    ```bash
    bq rm -f `project.dataset.job_control`
    ```
    *Note: Deleting the `job_control` table will remove all historical logging data for the migrated job. Consider archiving it if historical data is needed.*
5.  **Verify Original Job Functionality**: Confirm that the original `r_ausd_bp_ta_bpr_apn.ksh` job is running as expected in the legacy environment.