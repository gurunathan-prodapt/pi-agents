# MIGRATION_NOTES.md

## 1. Summary

The legacy KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh`, responsible for orchestrating a temporary address extraction process, has been migrated.

**Original System:**
*   **Job:** `r_ausd_adressen.ksh`
*   **Technology:** KornShell (ksh)
*   **Orchestration:** UC4 scheduler
*   **Purpose:** Parameter parsing, environment setup, logging, error handling, and invocation of the core extraction script `k_ausd_adressen.ksh`.

**Target Platform:** Google Cloud Platform (GCP)
*   **Target Job:** `sp_temp_adressabzug_crs` (BigQuery Stored Procedure)
*   **Technology:** BigQuery SQL
*   **Orchestration:** Airflow DAG (`temp_adressabzug_crs`)
*   **Purpose:** Replicates the wrapper logic in BigQuery, handling parameter input, defaulting, validation, and orchestrating a placeholder core extraction procedure (`sp_ausd_adressen`). Logging and job control are managed via dedicated BigQuery audit tables.

## 2. Generated artifacts

The migration process generated the following files:

*   **`project/dataset/ddl/job_control.sql`**
    *   **Role:** DDL for the `job_control` BigQuery table. This table serves as an audit log for job execution status, start/end times, parameters, and overall outcome (RUNNING, OK, FAILED). It replaces the implicit status tracking and file-based logging of the legacy shell script.
*   **`project/dataset/ddl/job_log.sql`**
    *   **Role:** DDL for the `job_log` BigQuery table. This table stores detailed informational, warning, and error messages generated during job execution, replacing the flat-file log output of the original script.
*   **`project/dataset/ddl/job_error_log.sql`**
    *   **Role:** DDL for the `job_error_log` BigQuery table. This table specifically captures detailed error events, including messages and stack traces, providing a structured repository for troubleshooting.
*   **`project/dataset/sp_ausd_adressen.sql`**
    *   **Role:** Placeholder BigQuery Stored Procedure. This procedure is intended to house the migrated core logic from `k_ausd_adressen.ksh`. For this migration phase, it is a functional placeholder that accepts parameters and can be called, but its internal logic is yet to be implemented.
*   **`project/dataset/sp_temp_adressabzug_crs.sql`**
    *   **Role:** BigQuery Stored Procedure that implements the wrapper logic of `r_ausd_adressen.ksh`. It handles parameter defaulting, validation, logging to audit tables, error handling, and invokes the `sp_ausd_adressen` procedure.
*   **`airflow/dags/dag_temp_adressabzug_crs.py`**
    *   **Role:** Airflow DAG definition. This Python script defines the Airflow workflow responsible for scheduling and invoking the `sp_temp_adressabzug_crs` BigQuery stored procedure, replacing the legacy UC4 scheduler. It allows passing `stichtag` and `wiederanlaufwert` parameters.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Wrapper Logic:** The orchestration and parameter handling logic of `r_ausd_adressen.ksh` was directly translated into a BigQuery Stored Procedure (`sp_temp_adressabzug_crs`). This leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, keeping the entire process within the data warehouse environment.
*   **Audit Tables for Logging and Control:** Instead of file-based logging and implicit status tracking, dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`) were introduced. This provides structured, queryable, and centralized auditing for job executions, significantly improving observability and troubleshooting.
*   **Airflow for Scheduling:** The legacy UC4 scheduler was replaced by an Airflow DAG. Airflow provides robust scheduling, dependency management, and monitoring capabilities, aligning with modern cloud data orchestration practices.
*   **Placeholder for Core Logic (`k_ausd_adressen.ksh`):** Recognizing that `k_ausd_adressen.ksh` contains the primary data transformation logic, a placeholder BigQuery Stored Procedure (`sp_ausd_adressen`) was created. This allows the wrapper migration to proceed independently, while clearly flagging the need for a subsequent, dedicated migration effort for the core logic.
*   **Parameter Defaulting and Validation:** Shell script parameter handling (e.g., `IFNULL`, `TRIM`, `FORMAT_DATE`) was directly translated to BigQuery SQL functions, ensuring consistent behavior for `p_stichtag` and `p_wiederanlaufWert`. Validation checks use `IF ... THEN SIGNAL` for explicit error signaling.
*   **BigQuery `BEGIN...EXCEPTION` for Error Handling:** The shell script's `trap` mechanism was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block. This provides structured error handling, allowing for logging of errors to `job_log` and `job_error_log` tables and graceful termination or re-raising of exceptions.
*   **Replacement of Shell Utilities:** Legacy shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were absorbed into the BigQuery stored procedure's logic using native BigQuery functions or by implementing equivalent logic directly.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure the target GCP project (`project`) and BigQuery dataset (`dataset`) exist. If not, create them.
2.  **IAM Permissions:**
    *   Grant the service account used by Airflow (or any other orchestrator) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `project.dataset` to create/update tables and stored procedures, and insert/update data into audit tables.
        *   `BigQuery Job User` to run BigQuery jobs.
3.  **Deploy DDLs for Audit Tables:**
    *   Execute the DDL scripts for the audit tables in BigQuery:
        *   `project/dataset/ddl/job_control.sql`
        *   `project/dataset/ddl/job_log.sql`
        *   `project/dataset/ddl/job_error_log.sql`
4.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL scripts for the stored procedures in BigQuery:
        *   `project/dataset/sp_ausd_adressen.sql` (the placeholder)
        *   `project/dataset/sp_temp_adressabzug_crs.sql`
5.  **Airflow Configuration:**
    *   **GCP Connection:** Ensure a `google_cloud_default` connection (or a custom one specified in the DAG) is configured in Airflow with appropriate credentials (e.g., service account key file or workload identity federation).
    *   **DAG Deployment:** Deploy `airflow/dags/dag_temp_adressabzug_crs.py` to your Airflow environment.
    *   **DAG Parameters:** Review and configure the `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` variables within the DAG file to match your environment.
    *   **Scheduling:** Set the desired `schedule` for the `temp_adressabzug_crs` DAG in Airflow (currently `None`).
6.  **Core Logic Implementation (B4 Item):**
    *   **Crucially**, the `project.dataset.sp_ausd_adressen` procedure is currently a placeholder. The actual logic from `k_ausd_adressen.ksh` must be migrated and implemented within this procedure before the job can perform its intended data extraction. This is a significant follow-up item.

## 5. Known gaps & unresolved references

The following items have been identified as gaps, risks, or require further follow-up:

*   **Core Logic (`k_ausd_adressen.ksh`) Migration (B4 Item):** The most significant unresolved item. The `sp_ausd_adressen` procedure is a placeholder. The actual data extraction and transformation logic from `k_ausd_adressen.ksh` must be analyzed, designed, and migrated into this BigQuery stored procedure. This will be a separate, substantial effort.
*   **Filesystem Operations / OS Commands:** If `k_ausd_adressen.ksh` performs extensive filesystem operations, invokes external OS commands, or interacts with legacy systems not directly supported by BigQuery, these parts may require redesign (B4) and implementation in Python (e.g., running on Cloud Functions, Cloud Run, or within Airflow tasks) rather than pure BigQuery SQL.
*   **`trap` Mechanism Nuances:** While BigQuery's `BEGIN...EXCEPTION` block handles errors, subtle differences in how shell signals are caught and processed compared to BigQuery's error handling might require careful testing. More granular error handling might be needed within BigQuery procedures or the orchestrating Airflow DAG depending on specific legacy `trap` behaviors.
*   **`usage` Function Replacement:** The shell-specific `usage()` function for displaying help is not directly migrated. This functionality will be replaced by external documentation or comments within the BigQuery procedure.
*   **Source System "CRS" Connectivity:** The original script extracts data from "CRS". The migration of `k_ausd_adressen.ksh` will need to address how this source data is accessed by BigQuery (e.g., via federated queries, Cloud Storage ingestion, or direct BigQuery tables). This is a dependency for the core logic migration.

## 6. Validation

To validate the migrated wrapper job:

1.  **Trigger the Airflow DAG:**
    *   In the Airflow UI, navigate to the `temp_adressabzug_crs` DAG.
    *   Manually trigger a run. You can optionally provide `stichtag` (e.g., `'01012023'`) and `wiederanlaufwert` (e.g., `'1'`) parameters in the trigger UI. If not provided, the stored procedure will use its default logic.
2.  **Monitor Airflow Task Logs:**
    *   Observe the logs for the `call_sp_temp_adressabzug_crs` task. Look for successful execution messages and no errors.
3.  **Query BigQuery Audit Tables:**
    *   **`job_control` table:**
        ```sql
        SELECT *
        FROM `project.dataset.job_control`
        WHERE job_name = 'sp_temp_adressabzug_crs'
        ORDER BY start_time DESC
        LIMIT 1;
        ```
        *   **Passing Criteria:** The `status` column should be `'OK'`. The `start_time`, `end_time`, `stichtag`, and `wiederanlauf_wert` should reflect the expected values.
    *   **`job_log` table:**
        ```sql
        SELECT *
        FROM `project.dataset.job_log`
        WHERE job_run_id = (SELECT job_run_id FROM `project.dataset.job_control` WHERE job_name = 'sp_temp_adressabzug_crs' ORDER BY start_time DESC LIMIT 1)
        ORDER BY log_timestamp ASC;
        ```
        *   **Passing Criteria:** Look for `INFO` messages indicating job start, parameter processing, and successful completion. There should be no `ERROR` level messages.
    *   **`job_error_log` table:**
        ```sql
        SELECT *
        FROM `project.dataset.job_error_log`
        WHERE job_run_id = (SELECT job_run_id FROM `project.dataset.job_control` WHERE job_name = 'sp_temp_adressabzug_crs' ORDER BY start_time DESC LIMIT 1);
        ```
        *   **Passing Criteria:** This table should be empty for a successful run.
4.  **Verify Placeholder Call:**
    *   In the `job_log` table, you should see an `INFO` message from `sp_ausd_adressen` (e.g., "Placeholder for core address extraction logic..."), confirming that the wrapper successfully invoked the core procedure.

**What "passing" means:**
A successful validation means:
*   The Airflow DAG completes successfully without errors.
*   The `job_control` table shows the latest run of `sp_temp_adressabzug_crs` with `status = 'OK'`.
*   The `job_log` table contains expected `INFO` messages, including the invocation of `sp_ausd_adressen`, and no `ERROR` messages.
*   The `job_error_log` table is empty for the corresponding `job_run_id`.
*   Parameter defaulting and validation logic behaves as expected (e.g., if `p_stichtag` is omitted, it defaults to the current date; if an invalid date is passed, an error is logged).

## 7. Rollback procedure

In case of issues with the migrated job, the rollback procedure is as follows:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `temp_adressabzug_crs` DAG to prevent further scheduled executions.
2.  **Re-enable Legacy UC4 Job:**
    *   Re-enable the original UC4 job (`DW.BERT_P_ADRESSEN.xml`) that invokes `r_ausd_adressen.ksh`.
3.  **Monitor Legacy Job:**
    *   Verify that the legacy job is running as expected and producing the correct output.
4.  **Optional: BigQuery Object Cleanup (if necessary):**
    *   If the deployed BigQuery stored procedures or audit tables are causing conflicts or are deemed unnecessary during rollback, they can be dropped. However, for this wrapper migration, simply disabling the Airflow DAG and re-enabling UC4 should be sufficient as the BigQuery objects are isolated.
    *   To drop:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_temp_adressabzug_crs`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_ausd_adressen`; -- Only if no other dependencies
        -- Consider if audit tables should be kept for historical logging or dropped
        -- DROP TABLE IF EXISTS `project.dataset.job_control`;
        -- DROP TABLE IF EXISTS `project.dataset.job_log`;
        -- DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        ```
    *   **Note:** Dropping audit tables will remove historical execution data. It's generally recommended to retain them unless absolutely necessary to drop.

This rollback procedure focuses on reverting the orchestration and wrapper logic. Since the core data extraction logic (`k_ausd_adressen.ksh`) was not migrated in this phase, it remains operational in the legacy environment, simplifying the rollback.