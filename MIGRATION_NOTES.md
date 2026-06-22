# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_v_ta_inv_assign.ksh` from its legacy environment to Google Cloud Platform. The original script served as an orchestration wrapper for a database extraction job related to the `ta_inv_assign` table.

The migration involved replatforming the orchestration logic to **Google Cloud Composer (Airflow)** and refactoring the underlying SQL logic to **Google BigQuery**. The `ta_inv_assign` table and associated audit/log tables have been migrated to BigQuery, and the core business logic, originally in `d_ausd_v_ta_inv_assign.sql`, has been encapsulated within a BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bigquery/ddl/ta_inv_assign.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the target `ta_inv_assign` table. This table will store the extracted and transformed data, serving as the BigQuery equivalent of the original `ta_inv_assign` table.
*   **`bigquery/ddl/dwtk_meldungen_bq.sql`**
    *   **Role**: BigQuery DDL script to create the `dwtk_meldungen_bq` table. This table replaces the `isbert_schema.dwtk_meldungen` table used by the original script for logging and metadata, specifically for tracking the last successful run date. It also includes an example `INSERT` statement for initial setup.
*   **`bigquery/stored_procedures/sp_d_ausd_v_ta_inv_assign.sql`**
    *   **Role**: BigQuery Stored Procedure that encapsulates the core business logic originally found in `d_ausd_v_ta_inv_assign.sql`. It handles data extraction, transformation, and insertion into the `ta_inv_assign` table, including logic for determining the processing date based on `dwtk_meldungen_bq`. It accepts `p_job_kennung` and `p_eintrags_nr` parameters.
*   **`airflow/dags/k_ausd_v_ta_inv_assign_dag.py`**
    *   **Role**: Python script defining an Airflow Directed Acyclic Graph (DAG). This DAG orchestrates the execution of the `sp_d_ausd_v_ta_inv_assign` BigQuery Stored Procedure. It replaces the KornShell script's role in environment setup, parameter parsing, and job execution, leveraging Airflow's capabilities for scheduling, monitoring, and error handling. It accepts `job_kennung` and `eintrags_nr` parameters via `dag_run.conf`.

## 3. Key design decisions

*   **Orchestration Replatforming**: The KornShell script's orchestration capabilities were replatformed to Google Cloud Composer (Airflow). This provides a managed, scalable, and robust platform for scheduling, monitoring, and managing data pipelines, replacing custom shell scripting for job control.
*   **Data Processing Refactoring**: The core SQL logic, originally in `d_ausd_v_ta_inv_assign.sql`, was refactored into a BigQuery Stored Procedure (`sp_d_ausd_v_ta_inv_assign`). This centralizes the business logic within the data warehouse, allowing for direct execution within BigQuery and leveraging its performance and scalability.
*   **Replacement of Utility Scripts**: The various KornShell utility scripts (e.g., for error handling, date utilities, parameter parsing, SQL*Plus interaction) were replaced by native Airflow features (e.g., task failure handling, DAG parameters) and BigQuery's built-in SQL functions and error handling mechanisms. This reduces dependency on custom shell utilities and leverages cloud-native capabilities.
*   **Parameter Handling**: The original `getopts` mechanism for command-line parameters was replaced by Airflow DAG parameters (`dag_run.conf`). These parameters are then passed directly to the BigQuery Stored Procedure, maintaining the script's configurability.
*   **Job Control Logic Integration**: Implicit job control logic (e.g., ignoring active jobs, deactivating old ones) from the original script and its SQL component was integrated directly into the BigQuery Stored Procedure. This ensures that the data processing logic remains self-contained and consistent.
*   **Elimination of Temporary Files**: The use of temporary files for passing record counts was replaced by the ability of BigQuery Stored Procedures to return results or by direct logging within the Airflow DAG, simplifying the data flow and reducing I/O overhead.
*   **Trade-offs**:
    *   **Dependency on `d_ausd_v_ta_inv_assign.sql` content**: The migration assumed a specific structure and logic for `d_ausd_v_ta_inv_assign.sql`. Any Oracle-specific SQL constructs or complex PL/SQL logic within the original SQL file would require more extensive refactoring and testing in BigQuery.
    *   **Source Data Schema Assumption**: The DDL for `ta_inv_assign` and the Stored Procedure's `INSERT` statement are based on an assumed schema for the source `cds_ta_inv_assignment` table. Any deviation in the actual source schema would necessitate adjustments.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset for target data: `your-gcp-project.isbert_target_data`.
    *   Create the BigQuery dataset for log/audit data: `your-gcp-project.isbert_log_data`.
    *   Ensure the source dataset exists and is accessible: `your-gcp-project.isbert_source_carmen` (or the actual dataset where `cds_ta_inv_assignment` resides).
2.  **IAM Permissions**:
    *   Grant the Google Cloud Composer service account (typically `service-<PROJECT_NUMBER>@cloudcomposer.gserviceaccount.com`) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `your-gcp-project.isbert_target_data` to create/truncate/insert into `ta_inv_assign`.
        *   `BigQuery Data Editor` on `your-gcp-project.isbert_log_data` to create/insert into `dwtk_meldungen_bq`.
        *   `BigQuery Data Viewer` on `your-gcp-project.isbert_source_carmen` to read from `cds_ta_inv_assignment`.
        *   `BigQuery Job User` on `your-gcp-project` to run BigQuery jobs.
3.  **Airflow Connection**:
    *   Ensure the `google_cloud_default` connection is configured correctly in your Airflow environment. This is typically set up by default in Composer, but verify its validity.
4.  **Source Data Availability**:
    *   Confirm that the source table `your-gcp-project.isbert_source_carmen.cds_ta_inv_assignment` exists and contains the expected data.
5.  **Initial `dwtk_meldungen_bq` Entry**:
    *   Execute the `INSERT` statement provided in `bigquery/ddl/dwtk_meldungen_bq.sql` to ensure the `BERT_DROP_TEMP_TABLE` entry exists. This is crucial for the `sp_d_ausd_v_ta_inv_assign` stored procedure to correctly determine `v_datum` on its first run.
6.  **Deployment of BigQuery Objects**:
    *   Execute the DDL scripts (`ta_inv_assign.sql`, `dwtk_meldungen_bq.sql`) to create the tables.
    *   Execute the Stored Procedure DDL (`sp_d_ausd_v_ta_inv_assign.sql`) to create the stored procedure.
7.  **Airflow DAG Deployment**:
    *   Upload `airflow/dags/k_ausd_v_ta_inv_assign_dag.py` to the DAGs folder of your Cloud Composer environment.
8.  **Scheduling**:
    *   The DAG is currently configured with `schedule_interval=None`, meaning it will not run automatically. If automatic scheduling is required, update this parameter in the DAG file (e.g., to a cron expression) and re-deploy.

## 5. Known gaps & unresolved references

*   **Missing `d_ausd_v_ta_inv_assign.sql` content**: The exact business logic and Oracle-specific constructs within the original `d_ausd_v_ta_inv_assign.sql` file were not fully available during the design phase. The generated BigQuery Stored Procedure is an interpretation based on common patterns. A thorough review and potential adjustments are required once the full content of the original SQL is analyzed.
*   **Implicit Job Control Logic**: While an attempt was made to integrate job control logic (e.g., for ignoring/deactivating jobs) into the BigQuery Stored Procedure, the precise details of this logic from the original `starteSQLSkript` function and `d_ausd_v_ta_inv_assign.sql` are still an area for potential refinement.
*   **`tmpFile` Usage**: The original script used a temporary file to store record counts. The migration replaces this with the expectation that the BigQuery Stored Procedure's DML operations implicitly handle this, or that Airflow can log affected rows. If downstream processes explicitly relied on this temporary file's content, further integration might be needed.
*   **`is_production` Type Casting**: The DDL for `ta_inv_assign` assumes `is_production` is a `BOOL` in BigQuery, while the source `cds_ta_inv_assignment` is assumed to have it as a numeric type (0/1). The `CAST` operation in the Stored Procedure handles this, but confirmation of the source type is recommended.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Trigger the Airflow DAG**:
    *   Navigate to the Airflow UI for your Composer environment.
    *   Find the `k_ausd_v_ta_inv_assign_dag` DAG.
    *   Click the "Trigger DAG" button.
    *   Optionally, provide configuration parameters in the JSON payload:
        ```json
        {
            "job_kennung": "TEST_JOB_123",
            "eintrags_nr": "TEST_ENTRY_456"
        }
        ```
2.  **Monitor DAG Execution**:
    *   Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully (green status).
    *   Check the task logs for `execute_ta_inv_assign_sp` for any BigQuery errors or warnings.
3.  **Verify Data in BigQuery**:
    *   Query the `your-gcp-project.isbert_target_data.ta_inv_assign` table in BigQuery.
    *   **Passing Criteria**:
        *   The `ta_inv_assign` table should be populated with data.
        *   The number of records inserted should match the expected count based on the source data and the logic in `sp_d_ausd_v_ta_inv_assign`.
        *   The data content (e.g., `cntrct_id`, `inv_definition_id`, date fields) should be accurate and consistent with the original script's output.
        *   The `your-gcp-project.isbert_log_data.dwtk_meldungen_bq` table should show an updated `timecreated` for `BERT_DROP_TEMP_TABLE` (if the Stored Procedure updates it, which is not explicitly in the current generated SP but was implied by the original SQL's `MAX(m.timecreated)`).
        *   No errors should be reported in BigQuery job history or Airflow logs.
4.  **Compare with Original Output**:
    *   Run the original `k_ausd_v_ta_inv_assign.ksh` script with identical parameters (if possible) and compare the resulting data in the original `ta_inv_assign` table with the data in the BigQuery `ta_inv_assign` table. This is the ultimate validation for functional equivalence.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, toggle off the `k_ausd_v_ta_inv_assign_dag` to prevent further executions.
    *   Alternatively, remove the DAG file from the Composer DAGs folder.
2.  **Reactivate Original Script**:
    *   Re-enable and re-schedule the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh` script in its legacy environment.
3.  **Data Reversion (if necessary)**:
    *   If the BigQuery `ta_inv_assign` table was populated incorrectly and this impacts downstream processes, consider:
        *   Truncating the `your-gcp-project.isbert_target_data.ta_inv_assign` table.
        *   Restoring the `ta_inv_assign` table from a previous backup (if available and necessary).
    *   For this specific extraction job, data corruption in the target is less critical than for update/insert jobs, as the source remains untouched.
4.  **Investigate and Rectify**:
    *   Analyze the logs from both Airflow and BigQuery to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery Stored Procedure, DDL, or Airflow DAG.
    *   Once fixed, re-attempt the migration and validation.