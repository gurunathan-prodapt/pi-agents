# MIGRATION_NOTES.md

## 1. Summary

This migration involved re-implementing the data preparation workflow previously orchestrated by the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh`. The original script was responsible for parameter parsing, job state management, and invoking a core SQL script (`d_ausd_v_ta_p_discount.sql`) to process data related to `ta_p_discount`.

The entire workflow has been migrated to Google Cloud's BigQuery environment. The orchestration logic, parameter handling, and job state management are now encapsulated within BigQuery Stored Procedures, leveraging BigQuery SQL for data transformation and control flow.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`sql/ddl/job_table.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `dataset.job_table`. This table is crucial for managing the active status and metadata of job executions, replacing the implicit job management and state tracking previously handled by the KornShell script.
*   **`sql/ddl/job_run_log.sql`**
    *   **Role**: Defines the DDL for the `dataset.job_run_log` table. This table serves as a centralized logging mechanism for job execution details, including success/failure status, record counts, and error messages. It replaces the temporary file (`tmpFile`) used for record counting and the `echo` commands for logging in the legacy system.
*   **`sql/ddl/ta_p_discount.sql`**
    *   **Role**: Defines the DDL for the target table `dataset.ta_p_discount`. This table is where the transformed data, originally processed by `d_ausd_v_ta_p_discount.sql`, will reside in BigQuery. The schema is inferred from the expected output of the transformation.
*   **`sql/stored_procedures/d_ausd_v_ta_p_discount.sql`**
    *   **Role**: Contains the BigQuery Stored Procedure `dataset.d_ausd_v_ta_p_discount`. This SP encapsulates the core data transformation logic that was originally present in the `d_ausd_v_ta_p_discount.sql` file. It performs a `TRUNCATE` and `INSERT...SELECT` operation into `dataset.ta_p_discount`, joining data from `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs`.
*   **`sql/stored_procedures/r_ausd_vertrag_control.sql`**
    *   **Role**: Contains the BigQuery Stored Procedure `dataset.r_ausd_vertrag_control`. This is the main control procedure that replaces the `k_ausd_v_ta_p_discount.ksh` KornShell script. It handles:
        *   Parameter validation (`p_JobKennung`, `p_EintragsNr`).
        *   Job state management (deactivating old jobs, activating/deactivating current job) using `dataset.job_table`.
        *   Invocation of the data transformation SP (`dataset.d_ausd_v_ta_p_discount`).
        *   Counting processed records directly from `dataset.ta_p_discount`.
        *   Logging execution details and errors into `dataset.job_run_log`.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Orchestration**: The primary decision was to replace the KornShell script's orchestration logic with BigQuery Stored Procedures. This centralizes the entire workflow within BigQuery, leveraging its native capabilities for data processing, control flow, and error handling. This eliminates the need for external shell environments and `SQL*Plus` interactions.
*   **Separation of Concerns (Control vs. Transformation)**: The workflow is split into two main stored procedures:
    *   `dataset.r_ausd_vertrag_control`: Handles the overarching orchestration, parameter validation, job state, and logging.
    *   `dataset.d_ausd_v_ta_p_discount`: Focuses solely on the data transformation logic.
    This modular approach improves readability, maintainability, and reusability.
*   **Dedicated Control and Logging Tables**: Instead of relying on temporary files or implicit state, explicit BigQuery tables (`dataset.job_table` and `dataset.job_run_log`) were introduced.
    *   `job_table` provides a clear, persistent record of job activity and status, enabling robust job control and preventing concurrent runs for the same `job_kennung`.
    *   `job_run_log` offers structured, queryable logging for all job executions, making monitoring and debugging significantly easier than parsing shell script outputs.
*   **Direct Record Counting**: The legacy method of writing record counts to a temporary file and then reading it back has been replaced by a direct `SELECT COUNT(*)` query on the target table (`dataset.ta_p_discount`) within the control stored procedure. This is more efficient and BigQuery-native.
*   **BigQuery-Native Error Handling**: Shell script error handling (`DWMSG_MeldeFehler`) is replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks and `SIGNAL SQLSTATE` for structured error reporting and logging into `job_run_log`.
*   **Parameter Handling**: Shell `getopts` and environment variables are replaced by direct input parameters to the BigQuery Stored Procedures, simplifying invocation and ensuring type safety.
*   **`TRUNCATE` and `INSERT` for `d_ausd_v_ta_p_discount`**: The `d_ausd_v_ta_p_discount` stored procedure begins with a `TRUNCATE TABLE` statement before inserting new data. This design decision mirrors the likely behavior of the original Oracle SQL script, which often involved clearing and reloading the target table.
*   **Trade-offs**:
    *   **Loss of Shell Script Flexibility**: The ability to use arbitrary shell commands or integrate with other shell-based utilities is lost. However, this is mitigated by BigQuery's rich SQL functionality and potential integration with Cloud Functions or Cloud Composer for more complex external interactions.
    *   **BigQuery-Specific Syntax**: The migration requires adherence to BigQuery SQL syntax and features, which can differ from traditional SQL dialects (e.g., Oracle PL/SQL).
    *   **Increased BigQuery Resource Usage**: Running orchestration logic within BigQuery SPs consumes BigQuery compute resources, which might have been offloaded to a separate shell environment previously.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery `dataset` (e.g., `your_project.your_dataset`) exists. If not, create it.
2.  **Source Table Availability**:
    *   Verify that the source tables `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs` exist within the same BigQuery dataset (or are accessible via appropriate dataset/project references) and contain the necessary data. Their schemas must match the expectations of `d_ausd_v_ta_p_discount.sql`.
3.  **IAM Permissions**:
    *   **Service Account**: Identify or create a dedicated Google Cloud Service Account that will be used to execute the BigQuery Stored Procedures (e.g., via Cloud Composer, BigQuery Scheduled Queries, or direct API calls).
    *   **Required Roles**: Grant the Service Account the following BigQuery roles:
        *   `BigQuery Data Editor` on the `dataset` containing `job_table`, `job_run_log`, `ta_p_discount`, `ta_disc_zusgf`, and `ta_cntrct_crs` (for `INSERT`, `UPDATE`, `TRUNCATE`, `SELECT` operations).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
4.  **Deploy DDL and Stored Procedures**:
    *   Execute the DDL scripts (`sql/ddl/job_table.sql`, `sql/ddl/job_run_log.sql`, `sql/ddl/ta_p_discount.sql`) to create the necessary tables in the target BigQuery dataset.
    *   Execute the Stored Procedure creation scripts (`sql/stored_procedures/d_ausd_v_ta_p_discount.sql`, `sql/stored_procedures/r_ausd_vertrag_control.sql`) to deploy the procedures in the target BigQuery dataset.
5.  **Orchestration Setup**:
    *   **Scheduling**: Configure the external orchestration tool (e.g., Cloud Composer/Apache Airflow, BigQuery Scheduled Queries, Cloud Workflows) to invoke the `dataset.r_ausd_vertrag_control` stored procedure.
    *   **Parameter Passing**: Ensure the orchestration layer correctly passes the `p_JobKennung` and `p_EintragsNr` parameters to the stored procedure.
    *   **Connection Strings/Configuration**: If using Cloud Composer, ensure the BigQuery connection is properly configured and the service account is associated with the DAG runner.

## 5. Known gaps & unresolved references

The migration design and generated code address the explicit logic of the KornShell script. However, some aspects from the original design document remain as known gaps or require further validation:

*   **Exact Logic within `d_ausd_v_ta_p_discount.sql`**: The generated `d_ausd_v_ta_p_discount` stored procedure assumes a standard `TRUNCATE` and `INSERT...SELECT` pattern based on common Oracle ETL practices. If the original `d_ausd_v_ta_p_discount.sql` contained more complex procedural logic (e.g., multiple DML statements, PL/SQL blocks, cursors, specific Oracle functions not directly translatable to BigQuery SQL), these might require further analysis and adaptation. The current implementation omits Oracle-specific elements like `DEFINE`, `COLUMN s_datum new_value v_datum`, `NVL`, `spool`, `parallel hints`, `trace.sql.cfg`, `SET SERVEROUTPUT`, `WHENEVER SQLERROR`, and `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
*   **`starteSQLSkript` Functionality**: The original `starteSQLSkript` (likely from `h_alis_sqlplus.ksh`) might have handled more than just executing SQL, such as specific connection pooling, transaction management, or pre/post-execution hooks. The current BigQuery SP design assumes direct invocation and BigQuery's native transaction model. Any implicit functionality from `starteSQLSkript` that is critical and not replicated in the BigQuery SPs is a potential gap.
*   **`pruefeParameterGesetzt` and `DWMSG_MeldeFehler` Details**: While the core validation and error logging are replicated, the exact behavior and reporting format of these legacy helper functions (e.g., specific error codes, detailed messages, integration with external monitoring systems) might need fine-tuning in the BigQuery `job_run_log` and error handling.
*   **`semi_auto` Migration Bucket (B2)**: The classification as `semi_auto` implies that manual review and potential adjustments were expected. This highlights the need for thorough testing and validation to ensure all edge cases and implicit behaviors of the original script are correctly handled in the BigQuery environment.
*   **Source Table Schemas**: The `d_ausd_v_ta_p_discount` SP implicitly relies on the schemas of `dataset.ta_disc_zusgf` and `dataset.ta_cntrct_crs`. These schemas were inferred during migration. Any discrepancies between the inferred and actual BigQuery schemas of these source tables could lead to runtime errors.

## 6. Validation

To validate the successful migration and functionality of the new BigQuery workflow:

1.  **Deployment Verification**:
    *   Confirm that all DDLs and Stored Procedures are successfully deployed in the target BigQuery dataset.
    *   Verify that the `job_table`, `job_run_log`, and `ta_p_discount` tables exist and have the correct schemas.
2.  **Manual Execution (Initial Test)**:
    *   Manually call the `dataset.r_ausd_vertrag_control` stored procedure in BigQuery, providing sample `p_JobKennung` and `p_EintragsNr` values:
        ```sql
        CALL dataset.r_ausd_vertrag_control('TEST_JOB_1', 'ENTRY_001');
        ```
    *   Verify that the procedure completes without errors.
3.  **Job State Validation**:
    *   Query `dataset.job_table` to ensure the job's `active_flag` was correctly set to `TRUE` at the start and then `FALSE` upon successful completion.
    *   Test with an existing `job_kennung` and a *different* `eintragsnr` to ensure the old active job is deactivated.
4.  **Logging Validation**:
    *   Query `dataset.job_run_log` to confirm that a successful entry was recorded, including the correct `job_kennung`, `eintragsnr`, `tab_name`, and `records_count`.
    *   Introduce an intentional error (e.g., by temporarily dropping a source table or modifying the SP to cause a syntax error) and verify that an error entry is logged in `job_run_log` with an appropriate `error_message`.
5.  **Data Validation**:
    *   Query `dataset.ta_p_discount` after a successful run.
    *   Compare the data in `dataset.ta_p_discount` with the expected output from the legacy system for the same input parameters. This is the most critical step. "Passing" means the data is identical or functionally equivalent.
    *   Verify the `records_count` in `job_run_log` matches the actual `COUNT(*)` from `dataset.ta_p_discount`.
6.  **Orchestration Integration Test**:
    *   If using Cloud Composer or BigQuery Scheduled Queries, trigger the configured DAG/scheduled query.
    *   Monitor the execution logs in Cloud Composer/BigQuery and verify successful completion.
    *   Repeat steps 3-5 to ensure the orchestration layer correctly invokes the BigQuery SP and the data is processed as expected.

**"Passing" means**:
*   The `dataset.r_ausd_vertrag_control` stored procedure executes successfully without raising unhandled exceptions.
*   The `dataset.job_table` accurately reflects the job's lifecycle (activated, then deactivated).
*   The `dataset.job_run_log` contains a success entry for each run, with the correct `records_count`.
*   The data in `dataset.ta_p_discount` is identical to the data produced by the legacy `k_ausd_v_ta_p_discount.ksh` job for the same input parameters.
*   Error conditions are gracefully handled, logged in `job_run_log`, and the job is marked as inactive.

## 7. Rollback procedure

In case of issues or if the migration needs to be reverted, follow these steps:

1.  **Disable New Workflow**:
    *   **Orchestration**: Immediately disable or delete the Cloud Composer DAG or BigQuery Scheduled Query that invokes `dataset.r_ausd_vertrag_control`.
    *   **BigQuery Stored Procedures**: (Optional but recommended for clean rollback) Delete the deployed stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS dataset.r_ausd_vertrag_control;
        DROP PROCEDURE IF EXISTS dataset.d_ausd_v_ta_p_discount;
        ```
2.  **Re-enable Legacy Workflow**:
    *   Re-enable the original `k_ausd_v_ta_p_discount.ksh` KornShell script in its legacy environment.
    *   Ensure all necessary dependencies (e.g., `d_ausd_v_ta_p_discount.sql`, helper scripts, database connections) are functional for the legacy job.
3.  **Data State (if necessary)**:
    *   If the new BigQuery job has already written data to `dataset.ta_p_discount` and this data is deemed incorrect or incomplete, you may need to:
        *   Truncate `dataset.ta_p_discount`.
        *   Reload `dataset.ta_p_discount` with data from a known good state (e.g., a backup or by re-running the legacy job).
    *   The `job_table` and `job_run_log` can typically be left as-is, as they record historical execution.
4.  **Cleanup (Optional)**:
    *   Once the legacy system is stable and fully operational, you may choose to drop the BigQuery tables created for the migration (`dataset.job_table`, `dataset.job_run_log`, `dataset.ta_p_discount`) if they are no longer needed.