# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_austausch.ksh`. This script, originally responsible for orchestrating the preparation and extraction of a contract cache snapshot for BERT reports and Forderungsscoring, has been migrated from a Unix/Linux KornShell environment to Google Cloud Platform.

The target platform for this migration is primarily **Google BigQuery** for data processing and storage, leveraging **BigQuery Stored Procedures** to encapsulate the ETL logic. Orchestration, which was previously handled by a UC4 scheduler, is now managed by **Google Cloud Composer (Apache Airflow)**.

## 2. Generated Artifacts

The migration process generated the following key artifacts:

*   **`your_bq_dataset/stored_procedures/k_ausd_austausch.sql`**
    *   **Role:** This BigQuery Stored Procedure (`your_gcp_project.your_bq_dataset.k_ausd_austausch`) contains the core data preparation logic. It translates the data filtering, deletion, and insertion operations originally performed by the `k_ausd_austausch.ksh` shell script into BigQuery SQL. It reads from a hypothesized `contract_cache` table and writes to a `fos_table`, applying `Stichtag` and `Wiederanlaufwert` filters. It also includes transactional control and logging to `job_log`.
*   **`your_bq_dataset/stored_procedures/bert_austausch_ksh.sql`**
    *   **Role:** This BigQuery Stored Procedure (`your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`) acts as the main orchestration layer, replicating the functionality of the original `r_ausd_austausch.ksh` script. It handles parameter parsing, defaulting (`Stichtag`, `Wiederanlaufwert`), logging to `job_log`, and updating `job_status`. Crucially, it invokes the `k_ausd_austausch` stored procedure to perform the actual data processing.
*   **`dags/bert_austausch_ksh_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG for Google Cloud Composer. It replaces the legacy UC4 scheduler. The DAG is responsible for triggering the `BERT_AUSTAUSCH_KSH` BigQuery Stored Procedure, passing any necessary parameters (e.g., `stichtag_in`, `wiederanlaufwert_in`) from Airflow's DAG run configuration.

## 3. Key Design Decisions

The migration strategy involved several key design decisions:

*   **BigQuery Stored Procedures for ETL Logic:** The core data transformation and orchestration logic, originally in KornShell scripts, was re-implemented as BigQuery Stored Procedures. This leverages BigQuery's native SQL capabilities for efficient, scalable, and managed data processing directly within the data warehouse.
*   **Cloud Composer for Orchestration:** The legacy UC4 scheduler was replaced by Google Cloud Composer (Apache Airflow). This provides a robust, cloud-native, and highly scalable platform for scheduling, monitoring, and managing data pipelines, offering better integration with other Google Cloud services.
*   **Re-implementation of Utility Functions:** Common shell utility functions (e.g., date handling, parameter parsing, error handling) were re-implemented using native BigQuery SQL functions and control flow (e.g., `FORMAT_DATE`, `IFNULL`, `BEGIN...EXCEPTION WHEN ERROR THEN`). This eliminates external script dependencies and consolidates logic within BigQuery.
*   **Centralized Logging and Status Tracking:** File-based logging from the original script was replaced by structured logging to dedicated BigQuery tables (`job_log` and `job_status`). This provides a centralized, queryable, and auditable record of job executions, statuses, and errors.
*   **Transactional Data Modifications:** The core data modification logic within `k_ausd_austausch` (DELETE then INSERT) is wrapped in a BigQuery transaction. This ensures atomicity, meaning either all changes are committed, or none are, maintaining data integrity in case of failures.

**Notable Trade-offs:**

*   **Language Shift:** Moving from KornShell to BigQuery SQL and Python (for Airflow) requires a different skillset and debugging approach.
*   **Dependency Management:** While external shell script dependencies are eliminated, new dependencies on BigQuery's SQL dialect and Airflow's Python APIs are introduced.
*   **Schema Rigidity:** BigQuery's schema-on-write approach requires explicit schema definition for tables, which might be more rigid than file-based processing.

## 4. Manual Steps Before Go-Live

Before the migrated solution can go live, the following manual steps must be completed:

1.  **Google Cloud Project and BigQuery Dataset Setup:**
    *   Ensure a Google Cloud Project (`your_gcp_project`) is established.
    *   Create the target BigQuery Dataset (`your_bq_dataset`) within this project.
2.  **IAM Permissions:**
    *   Grant appropriate IAM roles to the service account used by Cloud Composer and/or the user executing the BigQuery procedures. This typically includes:
        *   `BigQuery Data Editor` (for `job_log`, `job_status`, `fos_table`).
        *   `BigQuery Data Viewer` (for `contract_cache`).
        *   `BigQuery Job User` (to run queries and procedures).
        *   `Composer Worker` and `Composer User` roles for the Cloud Composer environment.
3.  **BigQuery Table Creation (DDL):**
    *   **`job_log` Table:** Create the logging table.
        ```sql
        CREATE TABLE `your_gcp_project.your_bq_dataset.job_log` (
            job_id STRING,
            job_name STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status STRING,
            message STRING,
            stichtag_param STRING,
            wiederanlaufwert_param INT64
        );
        ```
    *   **`job_status` Table:** Create the job status tracking table.
        ```sql
        CREATE TABLE `your_gcp_project.your_bq_dataset.job_status` (
            job_name STRING NOT NULL OPTIONS(description="Name of the job"),
            job_id STRING OPTIONS(description="Last run job ID"),
            last_run_status STRING OPTIONS(description="Status of the last run (SUCCESS/FAILED)"),
            last_run_time TIMESTAMP OPTIONS(description="Timestamp of the last run"),
            last_success_time TIMESTAMP OPTIONS(description="Timestamp of the last successful run"),
            last_stichtag STRING OPTIONS(description="Stichtag of the last successful run"),
            PRIMARY KEY (job_name) NOT ENFORCED
        );
        ```
    *   **Source Data Table (`contract_cache`):** Create the source table schema. **Note:** The exact schema for `contract_cache` (and `fos_table`) needs to be determined from the legacy system. The generated code uses placeholders.
        ```sql
        CREATE TABLE `your_gcp_project.your_bq_dataset.contract_cache` (
            dwh_vertrag_id INT64,
            gueltig_von DATE,
            gueltig_bis DATE,
            ladedatum DATE,
            some_data_column_1 STRING, -- Placeholder, adjust as per actual schema
            some_data_column_2 INT64,   -- Placeholder, adjust as per actual schema
            some_data_column_3 FLOAT64  -- Placeholder, adjust as per actual schema
            -- Add all other relevant columns from the source system
        );
        ```
    *   **Target Data Table (`fos_table`):** Create the target table schema.
        ```sql
        CREATE TABLE `your_gcp_project.your_bq_dataset.fos_table` (
            dwh_vertrag_id INT64,
            gueltig_von DATE,
            gueltig_bis DATE,
            ladedatum DATE,
            some_data_column_1 STRING, -- Placeholder, adjust as per actual schema
            some_data_column_2 INT64,   -- Placeholder, adjust as per actual schema
            some_data_column_3 FLOAT64  -- Placeholder, adjust as per actual schema
            -- Add all other relevant columns for the target output
        );
        ```
4.  **Data Ingestion:**
    *   Ensure that the `contract_cache` table (and any other source tables) is populated with the necessary data from the legacy system or other upstream sources. This might involve using `bq load`, Dataflow, Dataproc, or other ingestion tools.
5.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL for `k_ausd_austausch.sql` and `bert_austausch_ksh.sql` in BigQuery to create the stored procedures.
6.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is running.
    *   Verify the `google_cloud_default` connection is configured correctly in Airflow.
7.  **Deploy Airflow DAG:**
    *   Upload `dags/bert_austausch_ksh_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   Configure the `schedule_interval` in the DAG to match the desired execution frequency (e.g., daily, weekly).

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further follow-up:

*   **`k_ausd_austausch.ksh` Full Logic:** The detailed business logic of the original `k_ausd_austausch.ksh` script was not fully available during this migration design. The generated `k_ausd_austausch` BigQuery stored procedure contains placeholder columns (`some_data_column_1`, etc.). A thorough analysis and mapping of all columns and transformations from the original `k_ausd_austausch.ksh` is required to complete the `SELECT` and `INSERT` statements accurately.
*   **Exact Table Schemas:** The precise schemas for `contract_cache`, `fos_table`, and the exact data types for columns like `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`, and `ladedatum` are assumed based on common patterns. These must be verified against the legacy system's actual schema definitions.
*   **`FOSHoleLadedatum` Logic:** The commented-out line `FOSHoleLadedatum "DWH$TA_C_VERTRAG" v_ladedatum` in the original script suggests a potential alternative or historical method for determining `Stichtag` based on a `max_load_date` from a specific table. While the active code uses `v_sysdate`, it's crucial to confirm if this historical logic needs to be considered for data integrity or specific use cases in the `k_ausd_austausch` procedure.
*   **`p_eintragsnr` Parameter:** The purpose and usage of the `p_eintragsnr` parameter in the original script were unclear. It is passed as a placeholder ('0') in the generated BigQuery procedures. Its actual function should be investigated and implemented if necessary.
*   **Error Code Handling:** The specific meaning and required actions for legacy `ErrNr` values (e.g., 192, 193) need to be mapped to appropriate BigQuery error handling, logging categories, or alert mechanisms.
*   **Mail Notification:** The commented-out `mail` command in the original script indicates a potential email notification requirement. If this is still needed, it should be implemented using Cloud Composer's notification features or by integrating with Cloud Functions/Cloud Run.

## 6. Validation

Validation of the migrated solution involves verifying both the orchestration and the data processing logic.

**How to Run Tests:**

1.  **Manual BigQuery Stored Procedure Execution:**
    *   Execute `CALL your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH('DDMMYYYY', 0);` directly in the BigQuery console, providing test `Stichtag` and `Wiederanlaufwert` values.
    *   Monitor the BigQuery job history for completion status.
2.  **Cloud Composer DAG Trigger:**
    *   In the Cloud Composer UI (Airflow UI), manually trigger the `bert_austausch_ksh_dag`.
    *   Provide test parameters for `stichtag_in` and `wiederanlaufwert_in` via the "Trigger DAG w/ config" option.
    *   Monitor the DAG run in the Airflow UI for task success/failure.

**What "Passing" Means:**

*   **Orchestration Success:**
    *   The Cloud Composer DAG completes successfully without errors.
    *   The `BERT_AUSTAUSCH_KSH` BigQuery Stored Procedure completes successfully.
    *   The `k_ausd_austausch` BigQuery Stored Procedure completes successfully.
*   **Logging and Status Updates:**
    *   The `your_gcp_project.your_bq_dataset.job_log` table contains accurate entries for the start, end, and status of both `BERT_AUSTAUSCH_KSH` and `k_ausd_austausch` procedures, including correct parameter values and any error messages.
    *   The `your_gcp_project.your_bq_dataset.job_status` table is updated correctly with the `last_run_status`, `last_run_time`, and `last_success_time` for the `BERT_AUSTAUSCH_KSH` job.
*   **Data Integrity and Correctness:**
    *   **Target Table (`fos_table`):**
        *   Verify that the `fos_table` contains the expected data snapshot based on the provided `Stichtag` and `Wiederanlaufwert`.
        *   Perform row count comparisons between the expected output (based on source data and logic) and the actual data in `fos_table`.
        *   Spot-check specific records to ensure `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`, `ladedatum`, and other data columns are correctly filtered and inserted.
        *   Ensure the `DELETE` operation correctly removed previous data based on `dwh_vertrag_id >= p_wiederanlaufWert` before the new data was inserted.
    *   **Error Handling:** Test with invalid parameters or by simulating upstream data issues to ensure errors are caught, logged, and propagated correctly, leading to a `FAILED` status.

## 7. Rollback Procedure

In the event of critical issues post-go-live, the following rollback procedure can be executed:

1.  **Disable Cloud Composer DAG:**
    *   In the Cloud Composer UI (Airflow UI), toggle off the `bert_austausch_ksh_dag` to prevent further executions.
2.  **Re-enable Legacy UC4 Job:**
    *   Re-activate the original UC4 job `DW.BERT_P_AUSTAUSCH.xml` that invokes `r_ausd_austausch.ksh`.
3.  **Delete/Rename BigQuery Stored Procedures:**
    *   To prevent accidental invocation, delete or rename the migrated BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.k_ausd_austausch`;
        ```
4.  **Data Rollback (if necessary):**
    *   The `k_ausd_austausch` procedure performs a `DELETE` followed by an `INSERT` into `fos_table`. If a rollback is required due to incorrect data in `fos_table`, the following options exist:
        *   **Restore from Snapshot/Backup:** If `fos_table` has BigQuery table snapshots or if a backup was taken before the migration, restore the table to its state prior to the problematic BigQuery job run.
        *   **Re-run Legacy Job:** If the legacy system can regenerate the data for the affected `Stichtag` and `Wiederanlaufwert` correctly, running the original `r_ausd_austausch.ksh` script might overwrite the incorrect data in the target system (assuming the target system is still accessible by the legacy process).
        *   **Manual Correction:** For small-scale issues, manual data correction in `fos_table` might be an option, but this is generally not recommended for production rollbacks.
    *   **Note:** The `job_log` and `job_status` tables are append-only or merged, so their data typically does not require rollback, but rather serves as an audit trail.