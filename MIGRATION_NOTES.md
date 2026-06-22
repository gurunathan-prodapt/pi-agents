# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the legacy KornShell wrapper script `r_ausd_bp_ta_bpr_beschr.ksh` from its existing execution environment to Google Cloud Platform.

The original script, primarily an orchestration and parameter management wrapper for a core data processing script (`k_ausd_bp_ta_bpr_beschr.ksh`), has been re-platformed to leverage Google Cloud's BigQuery and Cloud Composer services.

**Key Migration Points:**
*   **Source:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh` (KornShell script).
*   **Target Platform:** Google Cloud BigQuery (for data processing and logging) and Cloud Composer (for orchestration).

## 2. Generated Artifacts

The migration process has generated the following artifacts:

*   **`sql/ddl/job_control.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language) script to create the `job_control` table. This table is used to track the status, start/end times, and parameters of job executions, replacing the custom logging framework of the legacy shell script.
*   **`sql/ddl/job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table stores detailed error information for failed job runs, providing a centralized and queryable error log.
*   **`sql/procedures/k_ausd_bp_ta_bpr_beschr.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure. This procedure is intended to house the migrated core data processing logic originally found in `k_ausd_bp_ta_bpr_beschr.ksh`. It is currently an empty shell and represents a critical dependent migration item.
*   **`sql/procedures/ausd_bp_ta_bpr_beschr.sql`**
    *   **Role:** The main BigQuery Stored Procedure that replaces the `r_ausd_bp_ta_bpr_beschr.ksh` wrapper script. It handles parameter parsing, date determination, job status management, and orchestrates the call to the `k_ausd_bp_ta_bpr_beschr` procedure.
*   **`dags/bert_ausd_bp_ta_bpr_beschr_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) for Cloud Composer. This DAG is responsible for scheduling and invoking the `project.dataset.ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure, passing necessary parameters. It replaces the legacy UC4 scheduler.

## 3. Key Design Decisions

*   **Re-platforming Wrapper Logic to BigQuery Stored Procedure:** The orchestration and parameter handling logic of `r_ausd_bp_ta_bpr_beschr.ksh` was re-implemented as a BigQuery Stored Procedure (`ausd_bp_ta_bpr_beschr`). This decision leverages BigQuery's native capabilities for procedural logic, allowing for direct interaction with BigQuery tables and providing a serverless execution environment within the data warehouse.
*   **Replacing Custom Logging with BigQuery Control Tables:** The legacy custom logging and error reporting framework was replaced by dedicated BigQuery tables (`job_control`, `job_error_log`). This provides a structured, centralized, and queryable audit trail for job executions and errors, enhancing observability and debugging capabilities.
*   **Cloud Composer for Orchestration:** The legacy UC4 scheduler was replaced by Cloud Composer (Apache Airflow). This provides a modern, cloud-native orchestration platform with robust scheduling, monitoring, parameterization, and error notification features, aligning with Google Cloud best practices.
*   **Core Logic as a Separate BigQuery Stored Procedure:** The core data transformation logic from `k_ausd_bp_ta_bpr_beschr.ksh` is designated to be migrated into its own BigQuery Stored Procedure (`k_ausd_bp_ta_bpr_beschr`). This modular approach ensures separation of concerns, allowing the wrapper to focus on orchestration while the core procedure handles data processing. The wrapper procedure then calls this core procedure.
*   **Native BigQuery Functions for Helper Logic:** Common shell helper functions (e.g., for date handling, parameter parsing) were replaced by native BigQuery SQL functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`, `IFNULL`) or integrated directly into the stored procedure logic. This reduces external dependencies and simplifies the code.

**Notable Trade-offs:**
*   **Dependency on Core Logic Migration:** The full functionality of the migrated wrapper is dependent on the successful and complete migration of the `k_ausd_bp_ta_bpr_beschr.ksh` script into its corresponding BigQuery Stored Procedure. This introduces a critical dependency and potential for delays if the core migration is complex.
*   **Loss of Direct File System Interaction:** Shell scripts often interact with the file system. This capability is inherently lost when moving to BigQuery Stored Procedures. Any file-based operations would need to be re-architected using cloud storage (e.g., Cloud Storage buckets) and potentially Cloud Functions or Dataflow. For this specific wrapper script, direct file system interaction was minimal and primarily related to logging, which is now handled by BigQuery tables.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it in your Google Cloud Project.
    *   `bq mk --dataset project:dataset` (Replace `project` and `dataset` with actual IDs).

2.  **Deploy BigQuery DDLs:**
    *   Execute `sql/ddl/job_control.sql` and `sql/ddl/job_error_log.sql` in your BigQuery environment to create the necessary control tables.
    *   `bq query --use_legacy_sql=false < sql/ddl/job_control.sql`
    *   `bq query --use_legacy_sql=false < sql/ddl/job_error_log.sql`

3.  **Deploy BigQuery Stored Procedures:**
    *   Execute `sql/procedures/k_ausd_bp_ta_bpr_beschr.sql` and `sql/procedures/ausd_bp_ta_bpr_beschr.sql` in your BigQuery environment.
    *   `bq query --use_legacy_sql=false < sql/procedures/k_ausd_bp_ta_bpr_beschr.sql`
    *   `bq query --use_legacy_sql=false < sql/procedures/ausd_bp_ta_bpr_beschr.sql`

4.  **Implement Core Logic (`k_ausd_bp_ta_bpr_beschr.sql`):**
    *   **CRITICAL:** The `sql/procedures/k_ausd_bp_ta_bpr_beschr.sql` file is currently a placeholder. The actual data processing logic from the original `k_ausd_bp_ta_bpr_beschr.ksh` script *must be fully migrated and implemented* within this BigQuery Stored Procedure before the wrapper can function correctly. This is a separate, dependent migration effort.

5.  **IAM/Permissions Configuration:**
    *   Ensure the Google Cloud service account used by Cloud Composer has the necessary BigQuery permissions (e.g., `BigQuery Data Editor` for the target dataset, `BigQuery Job User` for running queries/procedures).
    *   Verify that the service account used by BigQuery (if different from Composer's) has permissions to read from source tables and write to target tables as required by the `k_ausd_bp_ta_bpr_beschr` procedure.

6.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Update the `project_id` and `dataset_id` in `dags/bert_ausd_bp_ta_bpr_beschr_dag.py` to match your environment.

7.  **Deploy Airflow DAG:**
    *   Upload `dags/bert_ausd_bp_ta_bpr_beschr_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   Verify the DAG appears in the Airflow UI.

8.  **Configure Scheduling:**
    *   Define the desired schedule for the `bert_ausd_bp_ta_bpr_beschr_dag` within the DAG file (e.g., `schedule="@daily"` or a specific cron expression).
    *   Enable the DAG in the Airflow UI.

## 5. Known Gaps & Unresolved References

*   **Core Logic Implementation (B4 Item):** The most significant gap is the actual implementation of the `k_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure. This procedure contains the critical data transformation logic and is currently a placeholder. Its migration is a separate, dependent effort and must be completed for the end-to-end process to function.
*   **Error Notification:** While errors are logged to the `job_error_log` table, direct notification mechanisms (e.g., email, PagerDuty alerts) for job failures are not explicitly part of the generated code. These should be configured at the Cloud Composer level (e.g., Airflow callbacks, custom operators) to ensure operational awareness.
*   **Dynamic Aspects of Shell Scripts:** The migration focused on explicit logic. Any highly dynamic aspects of the original shell script (e.g., complex runtime SQL generation, extensive file system operations beyond logging) would require further manual review and potential re-architecture.
*   **Business Logic in Helper Scripts:** The design assumes that the sourced helper scripts (`h_alis_date.ksh`, `h_alis_parameter.ksh`, `f_alis_msgerr.ksh`) primarily contained utility functions. If any of these contained critical, non-trivial business logic, it needs to be identified and appropriately integrated into the BigQuery Stored Procedures or as BigQuery UDFs.
*   **`Wiederanlaufwert` Logic:** The `p_wiederanlaufWert` parameter is passed to the core procedure. The specific logic for how this value influences data processing (e.g., partial reprocessing, restart from a certain point) needs to be fully implemented and validated within the `k_ausd_bp_ta_bpr_beschr` procedure.

## 6. Validation

To validate the successful migration and functionality of the `bert_ausd_bp_ta_bpr_beschr_dag`:

1.  **Deployment Verification:**
    *   Confirm that all BigQuery DDLs and Stored Procedures are successfully deployed in the target dataset.
    *   Verify the `bert_ausd_bp_ta_bpr_beschr_dag` is visible and unpaused in the Airflow UI.

2.  **Manual DAG Trigger:**
    *   Manually trigger the `bert_ausd_bp_ta_bpr_beschr_dag` from the Airflow UI.
    *   **Test Case 1 (Default Parameters):** Trigger without providing `stichtag` or `wiederanlaufwert` to ensure defaults are applied (current date for `stichtag`, 0 for `wiederanlaufwert`).
    *   **Test Case 2 (Specific Parameters):** Trigger with specific `stichtag` (e.g., "01012023") and `wiederanlaufwert` (e.g., 1).

3.  **Monitor Airflow Logs:**
    *   Observe the task logs in the Airflow UI for `call_ausd_bp_ta_bpr_beschr_sp`. Look for successful execution messages or any errors.

4.  **BigQuery Job Monitoring:**
    *   Check BigQuery's "Job History" for the execution of the `ausd_bp_ta_bpr_beschr` stored procedure.
    *   Verify that the `k_ausd_bp_ta_bpr_beschr` stored procedure was called.

5.  **BigQuery Control Table Inspection:**
    *   Query `project.dataset.job_control`:
        *   `SELECT * FROM project.dataset.job_control WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR' ORDER BY created_at DESC;`
        *   **Passing Criteria:** For successful runs, the latest entry should show `status = 'OK'`, and `finished_at` should be populated.
    *   Query `project.dataset.job_error_log`:
        *   `SELECT * FROM project.dataset.job_error_log WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR' ORDER BY created_at DESC;`
        *   **Passing Criteria:** For successful runs, this table should contain no new entries related to the current job execution.

6.  **Error Scenario Testing:**
    *   (If `k_ausd_bp_ta_bpr_beschr` is implemented to raise errors under certain conditions) Trigger the DAG with parameters designed to cause an error in the core procedure.
    *   **Passing Criteria:** The DAG should fail, `job_control` should show `status = 'ERROR'`, and `job_error_log` should contain a detailed error message.

**What "Passing" Means:**
A successful validation means:
*   The Airflow DAG completes successfully without errors.
*   The `project.dataset.job_control` table accurately reflects the job's execution status as 'OK' for successful runs and 'ERROR' for failed runs.
*   The `project.dataset.job_error_log` table captures detailed error information for failed runs.
*   Parameters (`stichtag`, `wiederanlaufwert`) are correctly passed from the DAG to the BigQuery Stored Procedure and subsequently to the core procedure.
*   (Once `k_ausd_bp_ta_bpr_beschr` is implemented) The data processing logic within `k_ausd_bp_ta_bpr_beschr` executes as expected, producing the correct output in the target tables.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow these steps to roll back to the legacy system:

1.  **Immediate Action - Pause New System:**
    *   **Pause the Airflow DAG:** In the Cloud Composer Airflow UI, locate `bert_ausd_bp_ta_bpr_beschr_dag` and toggle it to "Off" (paused state). This immediately stops any new executions of the migrated job.

2.  **Re-enable Legacy System:**
    *   **Re-enable UC4 Job:** Re-activate the original UC4 job (`DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml`) that invokes `r_ausd_bp_ta_bpr_beschr.ksh`.
    *   **Verify Legacy Execution:** Monitor the legacy system to ensure `r_ausd_bp_ta_bpr_beschr.ksh` is running as expected and processing data correctly.

3.  **Data Integrity Check and Cleanup (if necessary):**
    *   **Assess Data Impact:** Determine if any partial or incorrect data was written by the migrated BigQuery procedures before the rollback.
    *   **Rollback Data:** If data corruption or partial writes occurred, execute appropriate BigQuery SQL statements to revert or clean up affected target tables. This might involve deleting data for the affected `stichtag` or restoring from backups if available.
    *   **Note:** The `p_wiederanlaufWert` logic in the core procedure (once implemented) might assist in reprocessing data from a specific point if a full data rollback is not feasible.

4.  **Investigate and Rectify:**
    *   Analyze the `job_control` and `job_error_log` tables in BigQuery, along with Airflow task logs, to identify the root cause of the issue.
    *   Address the identified problems in the BigQuery Stored Procedures or the Airflow DAG.

5.  **Re-deploy and Re-validate:**
    *   Once the issues are resolved, re-deploy the corrected BigQuery procedures and/or Airflow DAG.
    *   Repeat the full validation procedure (Section 6) before attempting another go-live.