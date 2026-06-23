# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the ETL job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh` from its legacy KornShell environment to Google Cloud Platform.

The original job, primarily a KornShell wrapper script, orchestrated the execution of a core SQL script (`d_ausd_v_ta_vvl_dwh.sql`) for data processing, handled parameter validation, and managed job status logging.

The job has been re-platformed to:
*   **Google BigQuery:** For all data storage, transformation logic, and job orchestration logic (via Stored Procedures).
*   **Cloud Composer (Apache Airflow):** For external scheduling and invocation of the main BigQuery Stored Procedure, replacing the legacy parent orchestrator.

This migration consolidates the shell script's control flow and the SQL script's data manipulation into native BigQuery components, leveraging BigQuery's scalability and managed services.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`your_bq_dataset_id/DWTK_MELDUNGEN.sql`**
    *   **Role:** BigQuery DDL for creating the `DWTK_MELDUNGEN` table. This table serves as a target for data previously residing in the legacy `TABLE:DWTK_MELDUNGEN`.
*   **`your_bq_dataset_id/DWH_TA_F_VVL_EREIGNISSE.sql`**
    *   **Role:** BigQuery DDL for creating the `DWH_TA_F_VVL_EREIGNISSE` table. This table serves as a target for data previously residing in the legacy `TABLE:DWH$TA_F_VVL_EREIGNISSE`.
*   **`your_bq_dataset_id/SOF_TA_VVL_DWH.sql`**
    *   **Role:** BigQuery DDL for creating the `SOF_TA_VVL_DWH` table. This table serves as a target for data previously residing in the legacy `TABLE:SOF$TA_VVL_DWH`.
*   **`your_bq_dataset_id/VIA.sql`**
    *   **Role:** BigQuery DDL for creating the `VIA` table. This table serves as a target for data previously residing in the legacy `TABLE:VIA`.
*   **`your_bq_dataset_id/job_error_log.sql`**
    *   **Role:** BigQuery DDL for creating a dedicated table to log errors encountered during job execution, replacing the legacy shell script's error handling and logging mechanisms.
*   **`your_bq_dataset_id/job_run_log.sql`**
    *   **Role:** BigQuery DDL for creating a dedicated table to track the start, end, status, and processed record counts of job runs, replacing the legacy shell script's job tracking.
*   **`your_bq_dataset_id/job_table.sql`**
    *   **Role:** BigQuery DDL for creating a table to manage active job instances, specifically for deactivating old active jobs, mirroring functionality from the legacy `starteSQLSkript` function.
*   **`your_bq_dataset_id/d_ausd_v_ta_vvl_dwh_proc.sql`**
    *   **Role:** BigQuery Stored Procedure containing the core data transformation logic. This procedure is a direct migration of the SQL statements found in the legacy `d_ausd_v_ta_vvl_dwh.sql` script, including reading from source tables and writing to target tables. It also includes placeholder for re-implemented `DWPA_UTIL_SKRIPT` functionality.
*   **`your_bq_dataset_id/register_job_start.sql`**
    *   **Role:** Helper BigQuery Stored Procedure responsible for deactivating previous job runs and inserting a new entry into `job_run_log`, centralizing the job registration logic.
*   **`your_bq_dataset_id/r_ausd_vertrag_control.sql`**
    *   **Role:** The main BigQuery Stored Procedure that orchestrates the entire job. It replaces the `k_ausd_v_ta_vvl_dwh.ksh` shell script, handling parameter validation, calling `register_job_start`, invoking `d_ausd_v_ta_vvl_dwh_proc`, and managing overall job status and error logging.
*   **`dags/r_ausd_vertrag_control_dag.py`**
    *   **Role:** An Apache Airflow DAG definition. This Python script defines the workflow to schedule and execute the `r_ausd_vertrag_control` BigQuery Stored Procedure, replacing the invocation by the legacy `r_ausd_v_ta_vvl_dwh.ksh` parent script.

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedures:** The primary design decision was to consolidate both the shell script's orchestration logic (`k_ausd_v_ta_vvl_dwh.ksh`) and the core data transformation SQL (`d_ausd_v_ta_vvl_dwh.sql`) into BigQuery Stored Procedures. This eliminates the need for external compute environments (like a shell interpreter) for the ETL logic, leveraging BigQuery's native capabilities for execution, scalability, and transaction management.
*   **Cloud Composer for External Orchestration:** Apache Airflow, via Cloud Composer, was chosen to replace the legacy parent orchestrator (`r_ausd_v_ta_vvl_dwh.ksh`). This provides a robust, cloud-native solution for scheduling, dependency management, parameter passing, and monitoring of the BigQuery job, offering significant improvements over shell-based scheduling.
*   **Dedicated BigQuery Logging and Tracking Tables:** Instead of relying on file-based logs or implicit status updates, structured BigQuery tables (`job_error_log`, `job_run_log`, `job_table`) were introduced. This centralizes logging, makes job status easily queryable, and simplifies monitoring and auditing.
*   **Parameterization:** The job's dynamic inputs (`p_JobKennung`, `p_EintragsNr`) are now handled via BigQuery Stored Procedure parameters, which are passed directly from the Airflow DAG. This maintains flexibility and allows for dynamic job identification.
*   **Transaction Management:** Explicit `BEGIN TRANSACTION`, `COMMIT TRANSACTION`, and `ROLLBACK TRANSACTION` statements are used within the `d_ausd_v_ta_vvl_dwh_proc` to ensure data integrity during the transformation process.
*   **Trade-offs:**
    *   **Manual Schema Definition:** The initial BigQuery table DDLs include `TODO` comments for schema definition. This requires manual analysis of the legacy Oracle tables to accurately define column names, data types, and constraints in BigQuery.
    *   **Re-implementation of Legacy Logic:** Functionality from legacy KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) and the Oracle package `DWPA_UTIL_SKRIPT` needs to be manually re-implemented as BigQuery UDFs or integrated directly into the stored procedures. This requires a deep understanding of their original business logic.
    *   **Initial Data Loading:** Historical data from the legacy Oracle tables must be extracted and loaded into the new BigQuery tables, which is a separate data migration effort.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the BigQuery dataset: `your_gcp_project_id.your_bq_dataset_id`. This dataset will house all the migrated tables and stored procedures.

2.  **BigQuery Table Schema Definition:**
    *   Review the `CREATE TABLE IF NOT EXISTS` statements for `DWTK_MELDUNGEN`, `DWH_TA_F_VVL_EREIGNISSE`, `SOF_TA_VVL_DWH`, and `VIA`.
    *   **Crucially, replace the `TODO: Define actual schema based on source system` comments with the complete and accurate column definitions (names, data types, nullability, partitioning/clustering if applicable) derived from the legacy Oracle tables.**

3.  **Initial Data Loading (Historical Data):**
    *   Extract historical data from the legacy Oracle tables (`DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE`, `SOF$TA_VVL_DWH`, `VIA`).
    *   Load this historical data into the newly created BigQuery tables (`your_gcp_project_id.your_bq_dataset_id.DWTK_MELDUNGEN`, etc.). This can be done using various GCP tools like Dataflow, Dataproc, or BigQuery's native load jobs.

4.  **Re-implementation of Legacy Utility Logic:**
    *   **`DWPA_UTIL_SKRIPT` Package:** Analyze the functions and procedures within the legacy Oracle `DWPA_UTIL_SKRIPT` package. Re-implement their equivalent logic as BigQuery User-Defined Functions (UDFs) or separate BigQuery Stored Procedures. These will then be called from `d_ausd_v_ta_vvl_dwh_proc` as indicated by the placeholder.
    *   **KornShell Utilities:** Review the functionality of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh`. Any essential logic not covered by BigQuery's built-in functions or the generated procedures must be re-implemented as BigQuery UDFs or integrated into the relevant stored procedures.

5.  **IAM Permissions:**
    *   Ensure the Google Cloud service account used by Cloud Composer (Airflow) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` (for writing to tables and logs)
        *   `BigQuery Job User` (for running queries and stored procedures)
        *   `BigQuery Data Viewer` (for reading from tables)
    *   Ensure the service account running the BigQuery stored procedures (if different from the Airflow service account, e.g., if procedures are invoked directly by other services) has similar permissions.

6.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` connection is correctly configured in your Cloud Composer environment, pointing to the correct GCP project.

7.  **Deploy Airflow DAG:**
    *   Upload the `dags/r_ausd_vertrag_control_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Adjust `project_id`, `dataset_id`, and parameter values (`p_JobKennung`, `p_EintragsNr`) in the DAG to match your environment and requirements.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up and require further attention:

*   **Complete BigQuery Table Schemas (`TODO`):** The generated DDLs for `DWTK_MELDUNGEN`, `DWH_TA_F_VVL_EREIGNISSE`, `SOF_TA_VVL_DWH`, and `VIA` currently contain placeholder schemas. A thorough analysis of the legacy Oracle table structures is required to define the exact column names, data types, and any necessary partitioning/clustering keys in BigQuery.
*   **Detailed `d_ausd_v_ta_vvl_dwh.sql` Transformation Logic:** The `d_ausd_v_ta_vvl_dwh_proc.sql` procedure contains placeholders for the core transformation logic. The complete and exact SQL statements, including `WHERE` clauses, `JOIN` conditions, and any complex data manipulations from the original `d_ausd_v_ta_vvl_dwh.sql` script, must be meticulously translated into BigQuery Standard SQL.
*   **`DWPA_UTIL_SKRIPT` Package Re-implementation:** The functionality of the Oracle `DWPA_UTIL_SKRIPT` package is critical and needs to be fully understood and re-implemented in BigQuery as UDFs or separate stored procedures. The current `d_ausd_v_ta_vvl_dwh_proc.sql` only includes a comment placeholder for this.
*   **KornShell Utility Scripts Re-implementation:** The exact functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` need to be analyzed. Any essential logic from these scripts that is not inherently covered by BigQuery or the generated procedures must be explicitly re-implemented.
*   **Parent Orchestrator Context (`r_ausd_v_ta_vvl_dwh.ksh`):** While the invocation of `k_ausd_v_ta_vvl_dwh.ksh` is now handled by the Airflow DAG, the broader context and any other dependencies or steps within the original `r_ausd_v_ta_vvl_dwh.ksh` script should be reviewed. If `r_ausd_v_ta_vvl_dwh.ksh` performed other actions beyond invoking `k_ausd_v_ta_vvl_dwh.ksh`, those actions might also require migration or integration into the Airflow DAG.
*   **Error Handling Granularity:** The current error logging captures the `@@error.message`. Depending on the original `f_alis_msgerr.ksh` functionality, more detailed error codes or contextual information might need to be captured and logged in `job_error_log`.

## 6. Validation

To ensure the migrated job functions correctly, perform the following validation steps:

1.  **Unit Testing BigQuery Procedures:**
    *   **`d_ausd_v_ta_vvl_dwh_proc`:** Create mock data in the source BigQuery tables (`DWTK_MELDUNGEN`, `DWH_TA_F_VVL_EREIGNISSE`). Execute the procedure directly in BigQuery. Verify that data is correctly inserted/updated into `SOF_TA_VVL_DWH` and `VIA`, and that `processed_record_count` is accurate.
    *   **`register_job_start`:** Test with various `p_JobKennung` and `p_EintragsNr` values. Verify that `job_table` and `job_run_log` are updated correctly, including the deactivation of previous active jobs.
    *   **`r_ausd_vertrag_control`:** Test with valid and invalid parameters (e.g., `NULL` `p_JobKennung`). Verify that parameter validation works, `job_error_log` is populated for errors, and the main transformation procedure is called successfully.

2.  **End-to-End Integration Testing (via Airflow):**
    *   Trigger the `r_ausd_vertrag_control_dag` in Cloud Composer.
    *   Monitor the Airflow task logs for successful execution.
    *   **Data Comparison:**
        *   Run the legacy `k_ausd_v_ta_vvl_dwh.ksh` job with a specific set of input data.
        *   Run the migrated Airflow DAG with the *same* input data (ensuring the BigQuery source tables contain the equivalent data).
        *   Compare the output data in the target tables (`SOF$TA_VVL_DWH` vs. `SOF_TA_VVL_DWH`, `VIA` vs. `VIA`) for accuracy and completeness.
        *   Compare the total number of processed records reported by both systems.
    *   **Logging Verification:**
        *   Check `your_gcp_project_id.your_bq_dataset_id.job_run_log` for a successful entry with the correct `status`, `start_timestamp`, `end_timestamp`, and `processed_records`.
        *   Intentionally introduce an error (e.g., invalid data, missing permissions) and verify that `your_gcp_project_id.your_bq_dataset_id.job_error_log` is populated with relevant error messages.

3.  **Performance Testing:**
    *   Compare the execution time of the migrated job in BigQuery/Airflow against the legacy job for similar data volumes.

**"Passing" Criteria:**
*   The Airflow DAG completes successfully without errors.
*   All BigQuery Stored Procedures execute without unhandled exceptions.
*   The data in the target BigQuery tables (`SOF_TA_VVL_DWH`, `VIA`) is identical to the data produced by the legacy job for the same input.
*   The `processed_records` count in `job_run_log` matches the count from the legacy job.
*   `job_run_log` accurately reflects the job's start, end, and status.
*   `job_error_log` contains no unexpected entries for successful runs.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow this rollback procedure:

1.  **Immediate Action:**
    *   **Deactivate the Airflow DAG:** Pause or delete the `r_ausd_vertrag_control_dag` in your Cloud Composer environment to prevent further execution of the migrated job.

2.  **Revert to Legacy System:**
    *   **Re-enable Legacy Job:** Re-activate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh` job and its parent orchestrator (`r_ausd_v_ta_vvl_dwh.ksh`) in the legacy environment. Ensure it can process data from its original sources and write to its original targets.

3.  **Data Rollback (if necessary):**
    *   **Target Tables:** If the migrated job performed destructive operations (e.g., `TRUNCATE` + `INSERT`, `UPDATE` with incorrect logic) on the BigQuery target tables (`SOF_TA_VVL_DWH`, `VIA`), and these tables are critical, consider:
        *   Using BigQuery's [time travel](https://cloud.google.com/bigquery/docs/data-manipulation-language#time_travel) feature to restore the tables to a state before the problematic run.
        *   Restoring the tables from a recent backup if time travel is not sufficient or if the tables were completely dropped.
    *   **Logging Tables:** The `job_run_log` and `job_error_log` tables are append-only and typically do not require rollback, but their entries can be ignored or filtered if needed.

4.  **Communication:**
    *   Inform all relevant stakeholders (data consumers, business users, operations team) about the rollback and the status of the data.

5.  **Root Cause Analysis:**
    *   Investigate the cause of the failure in the migrated system, address the issues, and re-test thoroughly before attempting another go-live.