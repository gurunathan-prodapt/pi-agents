# MIGRATION_NOTES.md

## 1. Summary

The legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` has been migrated to Google BigQuery. This script, originally responsible for controlling the processing of contract data (`ta_p_vertrag`), including parameter validation, job status management, and orchestration of an external SQL script (`d_ausd_v_ta_p_vertrag.sql`), has been re-implemented using BigQuery stored procedures and tables. The migration aims to leverage BigQuery's native capabilities for data processing, job orchestration, and logging, replacing the shell-based execution environment and Oracle SQL*Plus dependencies.

## 2. Generated artifacts

The migration has produced the following BigQuery artifacts:

*   **`bigquery/ddl/job_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `my_dataset.job_table`. This table is used to manage the overall status of jobs, tracking their activation, completion, and failure, replacing the implicit job table management of the legacy system.
*   **`bigquery/ddl/job_error_log.sql`**
    *   **Role:** Defines the DDL for the `my_dataset.job_error_log` table. This table captures detailed error messages and stack traces when a job fails, replacing the shell script's console error reporting and `DWMSG_MeldeFehler` function.
*   **`bigquery/ddl/job_run_log.sql`**
    *   **Role:** Defines the DDL for the `my_dataset.job_run_log` table. This table logs details of each specific job run, including start/end times, status, and the number of records processed, replacing the temporary file-based record counting and general run logging.
*   **`bigquery/stored_procedures/p_ausd_v_ta_p_vertrag_data_process.sql`**
    *   **Role:** Implements the core data processing logic originally found in `d_ausd_v_ta_p_vertrag.sql`. This BigQuery stored procedure performs data transformations, truncates intermediate tables, and inserts data into the target `sof_ta_p_vertrag` table. It returns the count of processed records.
*   **`bigquery/stored_procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** This is the main control stored procedure, directly replacing the `k_ausd_v_ta_p_vertrag.ksh` script. It handles input parameter validation, updates job status in `my_dataset.job_table`, orchestrates the call to `p_ausd_v_ta_p_vertrag_data_process`, and manages error logging and run logging in `my_dataset.job_error_log` and `my_dataset.job_run_log`.

## 3. Key design decisions

*   **BigQuery Native Orchestration**: The primary control logic of the KornShell script has been migrated directly into a BigQuery stored procedure (`r_ausd_vertrag_control`). This decision centralizes job management, parameter handling, and error reporting within BigQuery's robust scripting environment, reducing reliance on external shell scripts or complex orchestrator logic for core job flow.
*   **Modularization of Data Processing**: The data transformation logic from the external `d_ausd_v_ta_p_vertrag.sql` file was encapsulated into a separate BigQuery stored procedure (`p_ausd_v_ta_p_vertrag_data_process`). This promotes modularity, reusability, and clearer separation of concerns between control flow and data manipulation.
*   **Explicit Job Management Tables**: The implicit job status management and temporary file-based record counting of the legacy system have been replaced with explicit, structured BigQuery tables (`job_table`, `job_error_log`, `job_run_log`). This provides a clear, queryable, and persistent record of job executions, statuses, and errors.
*   **BigQuery SQL Scripting for Control Flow**: Shell script constructs like `if/else`, parameter parsing (`getopts`), and environment variables have been directly translated to BigQuery SQL scripting features (e.g., `IF/THEN/ELSEIF`, `IN` parameters, `DECLARE` variables).
*   **Standardized Error Handling**: The legacy `DWMSG_MeldeFehler` function and shell `exit` calls are replaced by BigQuery's `RAISE` statement within `BEGIN...EXCEPTION WHEN ERROR...END` blocks, coupled with detailed error logging into `job_error_log`. This provides structured error capture and propagation.
*   **Oracle to BigQuery SQL Conversion**: Oracle-specific SQL syntax (e.g., `(+)` for `LEFT JOIN`, `TRUNCATE TABLE ... DROP STORAGE`) has been converted to BigQuery standard SQL. This ensures compatibility and leverages BigQuery's optimized query engine.
*   **Placeholder Dataset Naming**: The dataset name `my_dataset` has been used as a placeholder throughout the generated code. This allows for flexible deployment to different BigQuery projects or environments by simply replacing this placeholder with the actual dataset name.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (e.g., `my_project.my_dataset`) exists. If `my_dataset` is not the desired name, update all generated SQL files to reflect the correct dataset name.
2.  **DDL Execution**: Execute the DDL scripts for the job management and logging tables:
    *   `bigquery/ddl/job_table.sql`
    *   `bigquery/ddl/job_error_log.sql`
    *   `bigquery/ddl/job_run_log.sql`
3.  **Source Data Migration & Schema Verification**:
    *   All source tables referenced in `p_ausd_v_ta_p_vertrag_data_process.sql` (e.g., `my_dataset.dwtk_meldungen`, `my_dataset.sof_ta_vertrag_tmp`, `my_dataset.sof_ta_p_vertrag`, and all other `my_dataset.sof_` tables that are truncated) must be created in BigQuery with schemas compatible with the original Oracle tables.
    *   Ensure that the data from the legacy Oracle database for these tables has been successfully migrated and loaded into their respective BigQuery counterparts.
4.  **Stored Procedure Deployment**: Deploy the two generated stored procedures:
    *   `bigquery/stored_procedures/p_ausd_v_ta_p_vertrag_data_process.sql`
    *   `bigquery/stored_procedures/r_ausd_vertrag_control.sql`
5.  **IAM Permissions**: Configure appropriate Identity and Access Management (IAM) roles and permissions for the service account or user that will execute the `r_ausd_vertrag_control` stored procedure. This includes:
    *   `bigquery.dataEditor` on `my_dataset` (or more granular permissions for specific tables).
    *   `bigquery.routines.call` on the stored procedures.
6.  **Scheduling Configuration**: If using an orchestration tool like Airflow, create or update the DAG to call `my_dataset.r_ausd_vertrag_control` with the required `p_JobKennung` and `p_EintragsNr` parameters. Ensure the scheduler is configured to pass these parameters correctly.

## 5. Known gaps & unresolved references

*   **`dwtk_meldungen` Schema and Content**: The exact schema and content of the `my_dataset.dwtk_meldungen` table, particularly for entries related to `job_kennung = 'BERT_DROP_TEMP_TABLE'`, need to be verified to ensure the `v_datum` variable is correctly derived as per the original script's logic. This is crucial for data consistency.
*   **`sof$` Table Schemas**: The schemas for all `my_dataset.sof_` tables (e.g., `sof_ta_p_vertrag`, `sof_ta_vertrag_tmp`, `sof_ta_disc_zusgf`, etc.) are assumed to be compatible with the data types and structure expected by the migrated SQL. A thorough review and validation of these schemas against the original Oracle definitions are required.
*   **`starteSQLSkript` Hidden Logic**: While the core execution of `d_ausd_v_ta_p_vertrag.sql` has been migrated, any hidden functionalities within the original `starteSQLSkript` wrapper (e.g., additional logging, specific transaction management, or environment setup not explicitly visible) might be a gap. Comprehensive testing is needed to ensure no such behavior is missed.
*   **Data Volume and Performance (B4)**: The performance characteristics of the migrated BigQuery SQL, especially for `p_ausd_v_ta_p_vertrag_data_process`, need to be thoroughly tested with production-like data volumes. Optimization might be required to meet Service Level Objectives (SLOs).
*   **Idempotency Verification (B4)**: The original script's behavior regarding deactivating old jobs and registering new ones implies a certain level of idempotency. This needs to be explicitly verified in the BigQuery implementation to ensure consistent state management even if the job is re-run or fails mid-execution.
*   **Configuration Management**: The original script relied on environment variables like `BERT_DIR_ROOT` and `DW_DIR_UTL`. While these are replaced by BigQuery script variables or direct references, any configuration that was dynamically sourced or changed needs to be managed appropriately in the BigQuery environment or the orchestration layer.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence and correct operation within the BigQuery environment.

**How to run the tests:**

1.  **Deploy Artifacts**: Ensure all DDLs and stored procedures are deployed to the target BigQuery dataset.
2.  **Prepare Test Data**: Load representative test data into the BigQuery source tables (e.g., `dwtk_meldungen`, `sof_ta_vertrag_tmp`) that accurately reflects the state of the legacy Oracle system.
3.  **Execute Control Procedure**: Call the main control stored procedure `my_dataset.r_ausd_vertrag_control` with sample `p_JobKennung` and `p_EintragsNr` parameters.
    *   **Success Scenario**: Execute with valid parameters.
    *   **Failure Scenario (Missing Params)**: Execute with `NULL` or empty `p_JobKennung` or `p_EintragsNr` to test validation.
    *   **Failure Scenario (Data Process Error)**: Introduce an error condition in the test data or `p_ausd_v_ta_p_vertrag_data_process` to simulate a data processing failure.
4.  **Monitor Logs**: Observe the `job_run_log` and `job_error_log` tables for entries related to the executed job runs.
5.  **Inspect Target Data**: Query the `my_dataset.sof_ta_p_vertrag` table to verify the inserted data.
6.  **Inspect Job Status**: Query the `my_dataset.job_table` to check the final status of the job.

**What "passing" means:**

*   **Successful Execution**:
    *   The `my_dataset.r_ausd_vertrag_control` stored procedure completes without BigQuery execution errors.
    *   The `my_dataset.job_table` shows the job's status transitioning from 'ACTIVE' to 'COMPLETED'.
    *   The `my_dataset.job_run_log` contains an entry for the run with `status = 'SUCCESS'`, `end_time` populated, and `records_processed` matching the actual count of records in `my_dataset.sof_ta_p_vertrag`.
    *   The data in `my_dataset.sof_ta_p_vertrag` is identical to the output produced by the original `k_ausd_v_ta_p_vertrag.ksh` script and `d_ausd_v_ta_p_vertrag.sql` when run against the same input data.
    *   All temporary `my_dataset.sof_` tables listed in `p_ausd_v_ta_p_vertrag_data_process` are truncated after successful execution.
*   **Error Handling**:
    *   When an error is deliberately introduced (e.g., missing parameters, data processing failure), the `my_dataset.r_ausd_vertrag_control` procedure raises an error.
    *   The `my_dataset.job_table` shows the job's status as 'FAILED'.
    *   The `my_dataset.job_run_log` contains an entry with `status = 'FAILURE'` and an appropriate `message`.
    *   The `my_dataset.job_error_log` contains a detailed entry for the failed run, including `error_message` and `error_detail`.
*   **Parameter Validation**:
    *   Calling `r_ausd_vertrag_control` with missing or empty required parameters (`p_JobKennung`, `p_EintragsNr`) results in a `RAISE` error and a 'FAILURE' status in the logs, with a clear message indicating the parameter issue.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions**: Immediately halt any scheduled or manual executions of the `my_dataset.r_ausd_vertrag_control` stored procedure in the BigQuery environment (e.g., disable the Airflow DAG).
2.  **Revert Orchestration**: Reconfigure the scheduling system (e.g., Airflow) to point back to the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` script.
3.  **Data State Restoration (if necessary)**:
    *   If the BigQuery job has modified target tables that are also consumed by the legacy system, and these modifications are incompatible or incorrect, a data restoration might be necessary. This would involve restoring the affected BigQuery tables (e.g., `my_dataset.sof_ta_p_vertrag`) to their state prior to the BigQuery job's execution, potentially from backups or by re-running the legacy process.
    *   If the BigQuery target tables are exclusively used by the migrated system and are not read by the legacy system, no data restoration is typically needed for rollback, as the legacy system will continue to operate on its own data sources.
4.  **Monitor Legacy System**: Verify that the original `k_ausd_v_ta_p_vertrag.ksh` script and its associated processes are functioning correctly after the rollback.
5.  **Analyze and Remediate**: Investigate the root cause of the issues that necessitated the rollback, make necessary corrections to the BigQuery migration artifacts, and re-plan deployment.