# MIGRATION_NOTES.md: k_ausd_v_ta_cntrct_valid.ksh

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_cntrct_valid.ksh` and its associated SQL script `d_ausd_v_ta_cntrct_valid.sql`. The migration targets Google BigQuery, transforming the shell-based orchestration and Oracle SQL logic into BigQuery Stored Procedures and DDLs. The primary goal is to leverage BigQuery's scalable data processing capabilities and integrate the job into a cloud-native data platform.

## 2. Generated Artifacts

The migration process generated the following BigQuery assets:

*   **`ddl/bq_job_control_table.sql`**
    *   **Role:** Defines the `job_control` table in BigQuery. This table replaces the implicit job tracking and temporary file usage of the original KornShell script. It stores metadata about each job execution, including status, start/end times, parameters, and processed record counts.
*   **`ddl/bq_log_table.sql`**
    *   **Role:** Defines the `job_log` table in BigQuery. This table centralizes all logging messages (INFO, WARNING, ERROR) generated during job execution, replacing the functionality of `f_alis_msgerr.ksh` and standard shell output.
*   **`procedures/log_error_procedure.sql`**
    *   **Role:** A BigQuery Stored Procedure named `log_message`. This procedure provides a standardized way to insert log entries into the `job_log` table, encapsulating the logging logic and making it reusable across other procedures. It serves as the primary replacement for `f_alis_msgerr.ksh`.
*   **`procedures/d_ausd_v_ta_cntrct_valid_bq.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure named `d_ausd_v_ta_cntrct_valid_bq`. This procedure is intended to contain the core data processing logic originally found in `d_ausd_v_ta_cntrct_valid.sql`. It will read from source tables and write to target tables in BigQuery, returning the number of records processed. **Note: This is currently a placeholder and requires the actual SQL translation.**
*   **`procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** The main control BigQuery Stored Procedure named `r_ausd_vertrag_control`. This procedure is the direct replacement for `k_ausd_v_ta_cntrct_valid.ksh`. It handles:
        *   Parameter validation (`-j`, `-f`).
        *   Job control logic (checking for active jobs, deactivating stale jobs, registering current job status).
        *   Orchestration of the core data processing by calling `d_ausd_v_ta_cntrct_valid_bq`.
        *   Comprehensive error handling and logging.

## 3. Key Design Decisions

*   **Migration to BigQuery Stored Procedures for Orchestration and Logic:**
    *   **Why:** Consolidates job control, parameter handling, and data processing within the BigQuery environment. This reduces external dependencies (e.g., shell environment, separate SQL*Plus calls) and leverages BigQuery's native scalability, performance, and built-in features for data manipulation. It also simplifies deployment and management compared to maintaining shell scripts on VMs.
    *   **Trade-offs:** Requires a complete re-implementation of shell-specific logic (e.g., `getopts`, file operations, environment variables) using BigQuery SQL constructs. The initial development effort for translation can be significant, especially for complex shell logic or Oracle-specific SQL.
*   **Dedicated BigQuery Tables for Job Control and Logging:**
    *   **Why:** Replaces the ad-hoc job status tracking (e.g., checking for active jobs, temporary files for record counts) and file-based logging of the original KornShell script. Using structured BigQuery tables (`job_control`, `job_log`) provides a centralized, queryable, and auditable record of all job executions, statuses, and messages. This significantly improves operational visibility and debugging capabilities.
    *   **Trade-offs:** Introduces new DDLs and DML operations for managing these tables, which need to be maintained.
*   **Modularization of Core Logic:**
    *   **Why:** Separating the core data transformation logic (`d_ausd_v_ta_cntrct_valid_bq`) from the orchestration and control flow (`r_ausd_vertrag_control`) improves readability, maintainability, and reusability. The core SQL can be developed and tested independently.
    *   **Trade-offs:** Introduces an additional layer of procedure calls, which might have a minor overhead, but the benefits of modularity outweigh this for complex jobs.
*   **BigQuery-Native Error Handling:**
    *   **Why:** Utilizes BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks for robust error management within stored procedures. This allows for graceful failure handling, detailed error logging, and status updates in the `job_control` table, replacing the custom error handling of `f_alis_msgerr.ksh`.
    *   **Trade-offs:** Requires careful mapping of original error codes and messages to a BigQuery-compatible logging structure.
*   **Emulation of `process_id` for Active Job Checks:**
    *   **Why:** The original script used `$$` (shell process ID) for unique temporary files and potentially for identifying active jobs. In BigQuery, a unique identifier (like a timestamp-based hash) is used for `process_id` in the `job_control` table to simulate this for active job detection.
    *   **Trade-offs:** This is an emulation and not a true OS process ID. While sufficient for preventing concurrent BigQuery procedure executions, it doesn't offer the same level of system-level process management.

## 4. Manual Steps Before Go-Live

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure `your_project_id` and `your_dataset` exist in your Google Cloud environment. If not, create them.
2.  **IAM Permissions:**
    *   Grant the service account or user executing these procedures the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `your_dataset` (for `job_control`, `job_log`, `SOF$TA_CNTRCT_VALID`, `VIA`).
        *   `BigQuery Job User` (to run jobs/procedures).
        *   `BigQuery Data Viewer` on source tables (`DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`).
3.  **Data Migration:**
    *   **Crucial Step:** Migrate all source tables (`DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`) and target tables (`SOF$TA_CNTRCT_VALID`, `VIA`) from their original database (e.g., Oracle) to BigQuery. This involves:
        *   Defining BigQuery schemas for these tables.
        *   Performing an initial historical data load.
        *   Setting up a continuous data synchronization mechanism (e.g., Data Transfer Service, custom ETL, CDC) if the source systems remain active.
4.  **Populate `d_ausd_v_ta_cntrct_valid_bq.sql`:**
    *   The generated `d_ausd_v_ta_cntrct_valid_bq.sql` is a placeholder. The actual Oracle SQL from `d_ausd_v_ta_cntrct_valid.sql` must be translated into BigQuery SQL and inserted into this procedure. This includes:
        *   Converting Oracle-specific data types and functions (e.g., `NVL` to `IFNULL`, `TO_CHAR` to `FORMAT_DATE`).
        *   Re-implementing any logic from the `DWPA_UTIL_SKRIPT` package using BigQuery UDFs or inline SQL.
5.  **Review and Adjust `job_control` Timeout:**
    *   The `r_ausd_vertrag_control` procedure includes logic to mark jobs as `FAILED_TIMEOUT` if they are `ACTIVE` for more than 1 hour. Review and adjust the `INTERVAL 1 HOUR` clause based on the expected runtime of the job.
6.  **Orchestration Setup:**
    *   **Scheduler Reconfiguration:** The original UC4 job needs to be reconfigured to trigger the new BigQuery process. Options include:
        *   **BigQuery Scheduled Queries:** For simple, time-based scheduling.
        *   **Cloud Composer (Airflow):** Recommended for more complex workflows, dependencies, or external system interactions. A Python DAG would call `r_ausd_vertrag_control`.
        *   **Cloud Workflows:** For event-driven or sequential task orchestration.
        *   **Custom Application/API:** If UC4 needs to directly invoke the BigQuery procedure via an API call.
7.  **Secrets Management (if applicable):**
    *   If the original `ksh` script accessed any sensitive information (e.g., database passwords, API keys), ensure these are securely managed in Google Cloud Secret Manager and accessed appropriately by the new orchestration layer or BigQuery procedures (if BigQuery procedures were to access external services, which is not the case here).

## 5. Known Gaps & Unresolved References

*   **Detailed SQL Logic for `d_ausd_v_ta_cntrct_valid_bq`:** The most significant gap is the actual content of `d_ausd_v_ta_cntrct_valid.sql`. The `d_ausd_v_ta_cntrct_valid_bq.sql` procedure is a placeholder and requires a thorough analysis and translation of the original Oracle SQL into BigQuery SQL. This includes:
    *   Data type mapping.
    *   Function translation (e.g., `NVL`, `TO_CHAR`, `DECODE`).
    *   Handling of Oracle-specific constructs (e.g., `ROWNUM`, `CONNECT BY`).
    *   Replication of `DWPA_UTIL_SKRIPT` package functionality.
*   **Full Utility Script Logic Translation:** While `log_message` replaces `f_alis_msgerr.ksh`, the full functionality of `h_alis_date.ksh`, `h_alis_parameter.ksh` (beyond basic validation), and `h_alis_sqlplus.ksh` has not been explicitly translated into BigQuery procedures. Any remaining logic from these scripts that is critical to the job's execution needs to be identified and implemented.
*   **Original Job Table Schema:** The exact schema and update/insert logic for the "job table" used by the original `ksh` script are unknown. The `job_control` table is a best-effort replacement based on common job control patterns. Discrepancies might exist.
*   **Error Code Mapping:** The original `ErrNr`, `ErrArg`, and `DWMSG_MeldeFehler` mechanism needs to be fully mapped to the `job_log` table's `error_code` and `error_argument` fields for consistent error reporting.
*   **Orchestration Decision:** The specific choice of orchestration tool (BigQuery Scheduled Queries, Cloud Composer, Cloud Workflows) and its integration with the existing UC4 scheduler needs to be finalized and implemented.
*   **`process_id` Emulation:** The current `process_id` in `job_control` is a simple timestamp-based hash. While it serves the purpose of identifying unique job runs for concurrency checks, it is not a true operating system process ID. This is a functional difference but should not impact the job's core logic.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy Artifacts:**
    *   Execute `ddl/bq_job_control_table.sql` and `ddl/bq_log_table.sql` to create the necessary tables.
    *   Execute `procedures/log_error_procedure.sql`, `procedures/d_ausd_v_ta_cntrct_valid_bq.sql` (after populating with actual SQL), and `procedures/r_ausd_vertrag_control.sql` to create the stored procedures.
2.  **Prepare Test Data:**
    *   Load representative sample data into the BigQuery source tables (`DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`) that mirrors the production environment.
    *   Ensure the target tables (`SOF$TA_CNTRCT_VALID`, `VIA`) are empty or in a known state for testing.
3.  **Execute the Job:**
    *   Call the main control procedure:
        ```sql
        CALL `your_project_id.your_dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG', 'TEST_EINTRAGS_NR');
        ```
    *   Test with various valid and invalid parameters (e.g., missing `-j` or `-f` values).
    *   Test concurrent execution by attempting to call the procedure multiple times rapidly.
4.  **Verify Results:**
    *   **`job_control` Table:**
        *   **Passing:** A successful run should have an entry with `status = 'COMPLETED'`, `end_timestamp` populated, and `records_processed` reflecting the expected count.
        *   **Passing (Concurrency):** Concurrent runs should result in an entry with `status = 'IGNORED'` and an appropriate message.
        *   **Passing (Error):** Runs with errors should have `status = 'FAILED'` and a descriptive `message`.
    *   **`job_log` Table:**
        *   **Passing:** Verify that `INFO` messages track the job's progress (start, core processing start/end, completion).
        *   **Passing:** `WARNING` messages should appear for ignored concurrent runs.
        *   **Passing:** `ERROR` messages should be present for failed runs, containing the error details and associated `error_code`/`error_argument` (if mapped).
    *   **Target Data Tables (`SOF$TA_CNTRCT_VALID`, `VIA`):**
        *   **Passing:** Query these tables to ensure the data has been transformed and loaded correctly according to the business logic of the original `d_ausd_v_ta_cntrct_valid.sql`.
        *   **Passing:** Compare record counts and key data points with the expected output from the original KornShell/SQL*Plus execution (if a baseline is available).
    *   **Error Handling:** Intentionally introduce errors in `d_ausd_v_ta_cntrct_valid_bq` (e.g., divide by zero, invalid cast) to ensure the `EXCEPTION WHEN ERROR` block correctly catches, logs, and updates the job status.

## 7. Rollback Procedure

In case of critical issues or if the migrated job does not perform as expected, the following rollback procedure can be executed:

1.  **Revert Scheduler Configuration:**
    *   Immediately reconfigure the UC4 job (or whichever scheduler is in use) to point back to the original `k_ausd_v_ta_cntrct_valid.ksh` script. Ensure the original script is functional and can resume processing.
2.  **Stop BigQuery Scheduled Queries/Cloud Composer DAGs/Workflows:**
    *   Disable or delete any BigQuery Scheduled Queries, Cloud Composer DAGs, or Cloud Workflows that were set up to invoke the migrated BigQuery procedures.
3.  **Data Rollback (if necessary):**
    *   **Option A (Preferred - if target tables were truncated/overwritten):** If the BigQuery job truncates and reloads target tables (`SOF$TA_CNTRCT_VALID`, `VIA`), simply re-run the original `k_ausd_v_ta_cntrct_valid.ksh` script to repopulate them with the correct data.
    *   **Option B (If target tables were incrementally updated):** If the BigQuery job performed incremental updates or inserts, you may need to:
        *   Restore the target tables (`SOF$TA_CNTRCT_VALID`, `VIA`) from a point-in-time backup taken just before the BigQuery job started.
        *   Alternatively, if the original system can handle reprocessing or idempotent operations, allow the original `ksh` script to run and correct any data inconsistencies.
    *   **Note:** Data consistency is paramount. Carefully assess the impact of the BigQuery job on the target data before deciding on the rollback strategy.
4.  **Delete BigQuery Assets:**
    *   Once the original job is confirmed to be running correctly and data consistency is ensured, you can optionally delete the BigQuery tables and procedures created during the migration:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset.d_ausd_v_ta_cntrct_valid_bq`;
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset.log_message`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset.job_control`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset.job_log`;
        ```
    *   Consider retaining the `job_control` and `job_log` tables for auditing purposes if they contain valuable historical information.