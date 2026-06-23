# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell control script `k_ausd_v_ta_p_discount_rr.ksh` and its associated SQL script `d_ausd_v_ta_p_discount_rr.sql`. The original system orchestrated data processing for the `ta_p_discount_rr` table, handling parameter parsing, environment setup, error handling, and job control.

The migration targets Google BigQuery. The KornShell orchestration logic has been refactored into a BigQuery stored procedure (`r_ausd_vertrag`), and the core data processing SQL has been converted into another BigQuery stored procedure (`d_ausd_v_ta_p_discount_rr`). Legacy file-based job control and logging mechanisms have been replaced with dedicated BigQuery control tables (`job_control`, `job_error_log`, `job_audit`).

## 2. Generated artifacts

The migration process generated the following BigQuery SQL scripts and objects:

*   **`your_gcp_project_id.your_bigquery_dataset_id.job_control.sql`**
    *   **Role:** DDL for the `job_control` BigQuery table. This table is used to track the lifecycle and status of job runs, including `STARTING`, `RUNNING`, `COMPLETED`, `FAILED`, and `IGNORED` states. It replaces the implicit job tracking and active job checks from the legacy KornShell script.

*   **`your_gcp_project_id.your_bigquery_dataset_id.job_error_log.sql`**
    *   **Role:** DDL for the `job_error_log` BigQuery table. This table stores detailed information about errors encountered during job execution, including error messages and stack traces. It replaces the error messaging functionality provided by `f_alis_msgerr.ksh` and shell-based error handling.

*   **`your_gcp_project_id.your_bigquery_dataset_id.job_audit.sql`**
    *   **Role:** DDL for the `job_audit` BigQuery table. This table records audit information and metrics for completed job runs, such as start/end times, final status, and the number of processed records. It replaces the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_rr_$$.tmp`) used to capture record counts and provides a structured audit trail.

*   **`your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr.sql`**
    *   **Role:** BigQuery Stored Procedure (`d_ausd_v_ta_p_discount_rr`). This procedure encapsulates the core data transformation logic originally found in `d_ausd_v_ta_p_discount_rr.sql`. It truncates and inserts data into the `sof_ta_p_discount_rr` table based on joins from `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, and `sof_ta_cntrct_templ`. It accepts `p_job_kennung` and `p_eintrags_nr` as input and returns the count of processed records.

*   **`your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag.sql`**
    *   **Role:** BigQuery Stored Procedure (`r_ausd_vertrag`). This is the main orchestration procedure, replacing `k_ausd_v_ta_p_discount_rr.ksh`. It handles:
        *   Parameter validation (`p_job_kennung`, `p_eintrags_nr`).
        *   Job control logic, including checking for and ignoring active job instances using the `job_control` table.
        *   Invoking the `d_ausd_v_ta_p_discount_rr` stored procedure for data processing.
        *   Logging job status and errors to `job_control` and `job_error_log`.
        *   Recording audit information, including processed record counts, to `job_audit`.

## 3. Key design decisions

*   **Orchestration Shift to BigQuery Stored Procedures:** The KornShell script's orchestration logic (parameter handling, job control, error handling, SQL invocation) was fully migrated to a BigQuery stored procedure (`r_ausd_vertrag`). This centralizes the entire job execution within BigQuery, leveraging its native capabilities for control flow and error management, eliminating the need for external shell environments.
*   **Core Logic as BigQuery Stored Procedure:** The data transformation SQL (`d_ausd_v_ta_p_discount_rr.sql`) was also converted into a BigQuery stored procedure (`d_ausd_v_ta_p_discount_rr`). This allows for direct invocation from the orchestration procedure and benefits from BigQuery's query optimization and execution environment.
*   **Structured Job Control and Logging:** Replaced disparate shell utilities, temporary files, and implicit job tracking with dedicated BigQuery tables (`job_control`, `job_error_log`, `job_audit`). This provides a standardized, queryable, and persistent record of job executions, statuses, errors, and audit metrics.
*   **Native BigQuery Features:** Utilized BigQuery's built-in functions for UUID generation (`GENERATE_UUID()`), row count tracking (`@@row_count`), and error handling (`RAISE BQEXCEPTION`, `@@error.message`, `@@error.stack_trace`) to ensure robustness and maintainability.
*   **Deprecation of Oracle-Specific Constructs:** Oracle-specific SQL*Plus commands, `TRUNCATE TABLE` syntax (via `DWPA_UTIL_SKRIPT`), and `PARALLEL` hints were removed or adapted to their BigQuery equivalents. For instance, `TRUNCATE TABLE` is now a direct BigQuery DDL statement.
*   **Parameter Handling:** Command-line parameter parsing (`getopts`) was replaced by direct input parameters to the BigQuery stored procedures, with validation logic implemented using `IF` statements.
*   **Trade-offs:**
    *   **Increased BigQuery Dependency:** The entire job lifecycle is now tightly coupled with BigQuery. While this simplifies deployment and monitoring within GCP, it increases reliance on BigQuery's availability and performance.
    *   **Loss of Shell Flexibility:** The flexibility of shell scripting for complex file system operations or interactions with diverse external systems is replaced by BigQuery's SQL-centric environment. For this specific job, which was primarily database-focused, this is a net positive.
    *   **Manual Conversion of SQL:** The core SQL logic required manual review and potential adjustments for BigQuery SQL dialect differences, though the provided SQL was largely standard.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset_id`) exists within `your_gcp_project_id`. If not, create it.
    *   Command: `bq mk --dataset your_gcp_project_id:your_bigquery_dataset_id`

2.  **Source and Target Table Migration:**
    *   The source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) and the target table (`sof_ta_p_discount_rr`) referenced in `d_ausd_v_ta_p_discount_rr` must be migrated from their original database to `your_gcp_project_id.your_bigquery_dataset_id`. This includes their schema (DDL) and historical data.
    *   Ensure the `sof_ta_p_discount_rr` table exists with the correct schema before deploying `d_ausd_v_ta_p_discount_rr` as it performs a `TRUNCATE` and `INSERT`.

3.  **Deploy BigQuery Control Tables:**
    *   Execute the DDL scripts for the control tables:
        *   `your_gcp_project_id.your_bigquery_dataset_id.job_control.sql`
        *   `your_gcp_project_id.your_bigquery_dataset_id.job_error_log.sql`
        *   `your_gcp_project_id.your_bigquery_dataset_id.job_audit.sql`
    *   Command example: `bq query --use_legacy_sql=false < your_gcp_project_id.your_bigquery_dataset_id.job_control.sql` (repeat for all three DDLs).

4.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL scripts for the stored procedures:
        *   `your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr.sql`
        *   `your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag.sql`
    *   Command example: `bq query --use_legacy_sql=false < your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr.sql` (repeat for `r_ausd_vertrag`).

5.  **IAM Permissions:**
    *   The service account or user that will execute the `r_ausd_vertrag` stored procedure must have appropriate BigQuery permissions:
        *   `bigquery.jobs.create` (to run queries/procedures)
        *   `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.create` (for `job_control`, `job_error_log`, `job_audit` tables)
        *   `bigquery.routines.call` (to call `d_ausd_v_ta_p_discount_rr`)
        *   `bigquery.tables.updateData`, `bigquery.tables.truncate`, `bigquery.tables.insertAll` (for `sof_ta_p_discount_rr` and source tables if they are modified)
        *   `bigquery.tables.getData` (for source tables like `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`)

6.  **Scheduler Configuration:**
    *   Configure a cloud-native scheduler (e.g., Cloud Composer/Airflow, Cloud Workflows, or BigQuery Scheduled Queries) to trigger the `your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag` stored procedure.
    *   The scheduler must pass the required parameters: `p_job_kennung` (STRING) and `p_eintrags_nr` (INT64).
    *   Example for BigQuery Scheduled Query:
        ```sql
        CALL `your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag`('YOUR_JOB_KENNUNG_VALUE', 12345);
        ```
    *   Ensure the scheduler's execution environment has the necessary BigQuery client libraries and authentication configured.

## 5. Known gaps & unresolved references

*   **Missing SQL Script (`d_ausd_v_ta_p_discount_rr.sql`):** The original content of `d_ausd_v_ta_p_discount_rr.sql` was not provided in the initial job components. The generated `d_ausd_v_ta_p_discount_rr` BigQuery stored procedure is based on an assumed or reconstructed version of this SQL. **It is critical to verify that the logic in the generated BigQuery stored procedure accurately reflects the original `d_ausd_v_ta_p_discount_rr.sql` content.**
*   **Detailed Logic of Sourced Utilities:** While the general purpose of the sourced KornShell utilities (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is understood, their precise internal logic (especially for parameter validation and error formatting) was not fully available. The migration implements standard BigQuery equivalents, but a detailed comparison might reveal subtle behavioral differences.
*   **`starteSQLSkript` Functionality:** The full implementation details of the `starteSQLSkript` function (likely from `h_alis_sqlplus.ksh`), particularly regarding its "active job ignoring" mechanism and how it wrote to the temporary file, were inferred. The BigQuery `job_control` table and logic aim to replicate this, but thorough testing is required to ensure functional equivalence.
*   **Original `r_ausd_vertrag.ksh` Context:** The design document mentions the script is a "Kontrollscript zu r_ausd_vertrag.ksh". The name `r_ausd_vertrag` was chosen for the main orchestration procedure. If `r_ausd_vertrag.ksh` is a separate, higher-level orchestrator, its interaction with this migrated component needs to be re-evaluated and potentially updated to call the BigQuery stored procedure.
*   **Complexity Tier:** The `file_complexity` was unknown. This suggests that the automated assessment might have missed nuances, and a manual review of the original KornShell script was necessary to ensure all logic was captured.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data:** Ensure the source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) in BigQuery contain representative test data. The target table (`sof_ta_p_discount_rr`) should be empty or contain data that can be safely truncated.

2.  **Execute the Orchestration Procedure:**
    *   Manually call the main orchestration stored procedure with sample parameters:
        ```sql
        CALL `your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag`('TEST_JOB_KENNUNG', 1);
        ```
    *   Test with valid parameters, invalid/missing parameters, and attempt to run the same job concurrently to test the "ignore active job" logic.

3.  **Verify Job Control and Audit Logs:**
    *   Query the `job_control` table:
        ```sql
        SELECT * FROM `your_gcp_project_id.your_bigquery_dataset_id.job_control` ORDER BY start_time DESC LIMIT 5;
        ```
        *   **Passing Criteria:** Look for a record with `job_name = 'r_ausd_vertrag'`, `job_kennung = 'TEST_JOB_KENNUNG'`, and `status = 'COMPLETED'`. If a concurrent run was attempted, there should be an `IGNORED` entry.
    *   Query the `job_audit` table:
        ```sql
        SELECT * FROM `your_gcp_project_id.your_bigquery_dataset_id.job_audit` ORDER BY start_time DESC LIMIT 5;
        ```
        *   **Passing Criteria:** Verify an entry with `status = 'COMPLETED'` and `processed_records` reflecting the expected number of rows inserted into `sof_ta_p_discount_rr`.
    *   Query the `job_error_log` table:
        ```sql
        SELECT * FROM `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` ORDER BY error_time DESC LIMIT 5;
        ```
        *   **Passing Criteria:** For successful runs, this table should be empty or contain no entries related to the current test run. For tests with invalid parameters, an error entry should be present.

4.  **Verify Data in Target Table:**
    *   Query the target table `sof_ta_p_discount_rr`:
        ```sql
        SELECT COUNT(1) FROM `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_p_discount_rr`;
        ```
        *   **Passing Criteria:** The count should match the `processed_records` value from the `job_audit` table.
        *   Perform data quality checks: Sample data from `sof_ta_p_discount_rr` and compare it against expected results based on the source data and transformation logic.

5.  **Scheduler Integration Test:**
    *   If a scheduler (e.g., Airflow DAG) is configured, trigger it and monitor its execution logs and the BigQuery control tables to ensure it correctly invokes the stored procedure and handles its output/errors.

## 7. Rollback procedure

In case of issues after go-live, the following steps outline the rollback procedure to revert to the original KornShell script:

1.  **Disable New Scheduler:**
    *   Immediately disable or pause the BigQuery Scheduled Query, Cloud Composer DAG, or any other scheduler configured to invoke `your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag`.

2.  **Re-enable Original Scheduler:**
    *   Re-enable the original scheduler that was responsible for triggering `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`.

3.  **Verify Original Job Execution:**
    *   Monitor the execution of the original KornShell script to ensure it runs successfully and processes data as expected. Check its logs and the original job control mechanisms.

4.  **Data Rollback (if necessary):**
    *   If the migrated job introduced incorrect data into `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_p_discount_rr`, and if the original job operates on the same target table, a data rollback might be necessary.
    *   This could involve restoring `sof_ta_p_discount_rr` to a previous state using BigQuery's time travel feature (if within the time window) or by reloading data from a backup. **Note:** The `d_ausd_v_ta_p_discount_rr` procedure performs a `TRUNCATE` before `INSERT`, so if the original job also truncates, data consistency might be maintained by simply re-running the original.

5.  **Cleanup (Optional, post-rollback):**
    *   Once the original system is stable, the migrated BigQuery objects (stored procedures and control tables) can be dropped from `your_gcp_project_id.your_bigquery_dataset_id`.
        *   `DROP PROCEDURE IF EXISTS your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag;`
        *   `DROP PROCEDURE IF EXISTS your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr;`
        *   `DROP TABLE IF EXISTS your_gcp_project_id.your_bigquery_dataset_id.job_control;`
        *   `DROP TABLE IF EXISTS your_gcp_project_id.your_bigquery_dataset_id.job_error_log;`
        *   `DROP TABLE IF EXISTS your_gcp_project_id.your_bigquery_dataset_id.job_audit;`
    *   **Do NOT drop the source or target data tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`, `sof_ta_p_discount_rr`) unless they were specifically created *only* for the migration and are not used by the original system.**