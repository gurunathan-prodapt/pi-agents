# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_bp_ta_bpr_optionen.ksh` job, originally responsible for provisioning basic product data for the BERT system. The job, which involved KornShell orchestration and Oracle PL/SQL data processing, has been migrated to Google Cloud Platform (GCP).

The target platform leverages:
*   **Google BigQuery** for data storage and transformation logic, replacing Oracle tables and PL/SQL.
*   **Google Cloud Composer (Airflow)** for job orchestration and scheduling, replacing the UC4 scheduler and KornShell scripts.

The migrated solution provides a BigQuery Stored Procedure to encapsulate the core data loading and transformation, with an Airflow DAG to manage its execution, parameter passing, and logging.

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/r_ausd_bp_ta_bpr_optionen.sql`**
    *   **Role:** This BigQuery Stored Procedure is the core of the migrated job. It encapsulates the logic from the original `r_ausd_bp_ta_bpr_optionen.ksh`, `k_ausd_bp_ta_bpr_optionen.ksh`, and `d_ausd_bp_ta_bpr_optionen.sql` scripts. It handles parameter parsing, date determination, truncating the target table, inserting data from the source table with restart logic, and comprehensive logging to `job_audit_log`.
*   **`dags/r_ausd_bp_ta_bpr_optionen_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG. It is responsible for orchestrating the execution of the `r_ausd_bp_ta_bpr_optionen` BigQuery Stored Procedure. It allows for scheduling, parameterization (e.g., `stichtag`, `wiederanlaufWert`), and monitoring of the job within the Cloud Composer environment.
*   **`sql/create_job_audit_log_table.sql`**
    *   **Role:** This SQL script defines the schema and creates the `job_audit_log` table in BigQuery. This table serves as a centralized repository for all job execution metadata, status, parameters, and error messages, replacing the custom logging mechanisms of the original KornShell scripts.
*   **`sql/create_dwtk_meldungen_table.sql`**
    *   **Role:** This SQL script defines the schema and creates the `dwtk_meldungen` table in BigQuery. This table is a placeholder for the migrated `isbert_schema.dwtk_meldungen` Oracle table, used by the stored procedure to derive a date variable.
*   **`sql/create_sof_ta_bpr_optionen_table.sql`**
    *   **Role:** This SQL script defines the schema and creates the `sof_ta_bpr_optionen` table in BigQuery. This is the target table where the processed basic product data is loaded, migrated from the Oracle `sof$ta_bpr_optionen` table.
*   **`sql/create_sof_ta_bpr_instance_table.sql`**
    *   **Role:** This SQL script defines the schema and creates the `sof_ta_bpr_instance` table in BigQuery. This is the source table from which the basic product data is extracted, migrated from the Oracle `sof$ta_bpr_instance` table. It includes the `DWH_VERTRAG_ID` column crucial for the restart logic.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Transformation Logic:** The core data processing and business logic, previously spread across KornShell and Oracle PL/SQL, is consolidated into a single BigQuery Stored Procedure. This centralizes the logic, leverages BigQuery's performance, and simplifies maintenance.
*   **Cloud Composer (Airflow) for Orchestration:** Airflow replaces the UC4 scheduler and KornShell orchestrator scripts. This provides a modern, cloud-native, and highly scalable orchestration platform with robust scheduling, monitoring, and error handling capabilities.
*   **Centralized `job_audit_log`:** A dedicated BigQuery table (`job_audit_log`) is introduced for comprehensive logging. This replaces disparate logging mechanisms in the original KornShell scripts, offering a unified view of job execution status, parameters, and errors.
*   **Direct Translation of Logic to BigQuery SQL:** KornShell parameter parsing, date calculations, and error handling logic are directly translated into BigQuery SQL functions and `EXCEPTION` blocks within the stored procedure, minimizing external dependencies.
*   **Removal of Oracle-Specific Hints:** Oracle-specific query hints (e.g., `/*+ full(bp) parallel(bp,4) */`) are removed. BigQuery's query optimizer automatically handles parallelism and execution plans, making such hints unnecessary and potentially counterproductive.
*   **Implementation of Restart Logic in BigQuery SP:** The `p_wiederanlaufWert` parameter is directly incorporated into the `WHERE` clause of the `INSERT...SELECT` statement (`bp.DWH_VERTRAG_ID > p_wiederanlaufWert`), enabling the job to resume processing from a specific point.
*   **Trade-offs:**
    *   **Loss of Shell Scripting Flexibility:** The direct control and flexibility offered by KornShell for system-level operations are replaced by BigQuery SQL and Airflow operators, which might require different approaches for certain tasks (e.g., file system interactions, though not directly relevant here).
    *   **Reliance on GCP Ecosystem:** The solution is tightly coupled with BigQuery and Cloud Composer, requiring familiarity with GCP services and potentially increasing vendor lock-in.
    *   **Schema Assumptions:** Initial BigQuery table schemas (`dwtk_meldungen`, `sof_ta_bpr_optionen`, `sof_ta_bpr_instance`) are based on observed usage. A full schema definition from the source Oracle system is required for complete accuracy.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project is available.
    *   Create the target BigQuery dataset (e.g., `your_bq_dataset`) within your GCP project.
    *   **Action:** Replace `your_gcp_project` and `your_bq_dataset` placeholders in all generated SQL and Python files with your actual project ID and dataset ID.
2.  **BigQuery Table Creation:**
    *   Execute the following SQL scripts in BigQuery to create the necessary tables:
        *   `sql/create_job_audit_log_table.sql`
        *   `sql/create_dwtk_meldungen_table.sql`
        *   `sql/create_sof_ta_bpr_optionen_table.sql`
        *   `sql/create_sof_ta_bpr_instance_table.sql`
    *   **Important:** Review and adjust the placeholder schemas for `dwtk_meldungen`, `sof_ta_bpr_optionen`, and `sof_ta_bpr_instance` based on the complete and accurate schema definitions from the original Oracle tables. Ensure all columns and data types are correctly mapped.
3.  **Data Migration:**
    *   Migrate historical data from the Oracle `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_optionen`, and `sof$ta_bpr_instance` tables to their corresponding BigQuery tables. This can be done using tools like BigQuery Data Transfer Service, custom ETL jobs, or `bq load` commands.
    *   Populate the `dwtk_meldungen` table with the necessary `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry and its corresponding date `value`.
4.  **IAM Permissions:**
    *   Grant the Cloud Composer service account (or the service account used by your Airflow environment) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on the target dataset (`your_bq_dataset`) to allow table creation, truncation, and data insertion.
        *   `BigQuery Data Viewer` on the target dataset for reading source tables.
        *   `BigQuery Job User` for running BigQuery jobs (stored procedures).
5.  **Airflow Connection:**
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment, pointing to your GCP project.
6.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `sql/r_ausd_bp_ta_bpr_optionen.sql` script in BigQuery to create the stored procedure.
7.  **Airflow DAG Deployment:**
    *   Upload the `dags/r_ausd_bp_ta_bpr_optionen_dag.py` file to your Cloud Composer environment's DAGs folder.
8.  **Scheduling Configuration:**
    *   Configure the `schedule` parameter in the Airflow DAG (`r_ausd_bp_ta_bpr_optionen_dag.py`) to match the original UC4 job's schedule.
9.  **External Dependency Resolution:**
    *   Address the `v_carmen = "@pcrs1"` dependency as identified in the "Known Gaps" section. If it represents a data source, ensure its data is available in BigQuery (either replicated or via federated query).

## 5. Known gaps & unresolved references

The following items were identified as gaps or risks during the migration design and require further investigation or follow-up:

*   **`v_carmen = "@pcrs1"` Resolution:** The exact purpose and usage of this variable in the original system need to be determined. If it points to an external Oracle instance for data, that data source must be identified and either replicated to BigQuery or accessed via BigQuery's federated query capabilities if real-time access is critical. Its current usage in `d_ausd_bp_ta_bpr_optionen.sql` was only as a `DEFINE` variable not used in the provided SQL body, so its migration impact might be minimal if it's truly unused.
*   **Functionality of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** The migration assumes this Oracle utility was a simple DDL execution wrapper (e.g., for `TRUNCATE TABLE`). If it contains complex logic beyond basic DDL, that logic needs to be extracted and migrated to BigQuery.
*   **`DWH_VERTRAG_ID` in Restart Logic Confirmation:** The `WHERE bp.DWH_VERTRAG_ID > p_wiederanlaufWert` filter was added based on the parameter's name and common restart patterns. It needs explicit confirmation that `sof$ta_bpr_instance` contains this column and that this is the correct column and logic for restart functionality. The original SQL did not explicitly include this filter.
*   **Complete Functionality of KornShell Utility Scripts:** While major functionalities like date handling and error logging are translated, a detailed analysis of all `.ksh` utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) is required to ensure no nuanced logic or side effects are missed during translation to BigQuery/Airflow.
*   **Metadata Table `isbert_schema.dwtk_meldungen` Schema and Data:** Confirm that the `dwtk_meldungen` table and its `job_kennung` column are properly migrated to BigQuery with the correct schema and that the necessary `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry exists with the expected date format.
*   **Orchestration of Multiple Jobs:** The original UC4 job might have invoked multiple sub-jobs. This migration focuses specifically on the `r_ausd_bp_ta_bpr_optionen.ksh` workflow. A broader Airflow DAG design might be needed to encompass all sub-jobs invoked by the main UC4 process if this job is part of a larger chain.
*   **Full Schema Definition:** The generated `CREATE TABLE` statements for `sof_ta_bpr_optionen` and `sof_ta_bpr_instance` are placeholders based on observed usage. A complete and accurate schema definition from the source Oracle system is required to ensure all columns, data types, and constraints are correctly migrated.

## 6. Validation

To validate the successful migration and functionality of the `r_ausd_bp_ta_bpr_optionen` job:

1.  **Trigger the Airflow DAG:**
    *   In the Cloud Composer Airflow UI, navigate to the `r_ausd_bp_ta_bpr_optionen` DAG.
    *   Manually trigger the DAG.
    *   Provide parameters if necessary:
        *   `stichtag`: (Optional) A date in `DDMMYYYY` format (e.g., "01012023"). If left `None`, the current system date will be used.
        *   `wiederanlaufWert`: (Optional) An integer for the restart value (e.g., `0` for a full run, or a specific `DWH_VERTRAG_ID` to resume from).
2.  **Monitor Airflow DAG Execution:**
    *   Observe the DAG run in the Airflow UI. All tasks should transition to a "success" state.
    *   Check task logs for any errors or unexpected output.
3.  **Verify BigQuery `job_audit_log`:**
    *   Query the `your_gcp_project.your_bq_dataset.job_audit_log` table for the `run_id` corresponding to the triggered DAG.
    *   **Passing Criteria:** The `status` column for the latest entry of `job_name = 'r_ausd_bp_ta_bpr_optionen'` should be `SUCCESS`. The `message` should indicate successful completion, and `inserted_rows` should reflect the expected count.
4.  **Inspect Target Table Data:**
    *   Query the `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` table.
    *   **Passing Criteria:**
        *   The table should have been truncated and repopulated.
        *   The number of rows should match the expected count based on the source data and the applied `wiederanlaufWert` filter.
        *   Perform data integrity checks: Sample `CNTRCT_ID` and `BPR_ID` values to ensure they are correct and consistent with the source `sof_ta_bpr_instance` table.
        *   If possible, compare row counts and a sample of data with the output of the original Oracle job for the same parameters.
5.  **Test Restart Logic:**
    *   Run the DAG with a non-zero `wiederanlaufWert` (e.g., `1000`).
    *   Verify that only records with `DWH_VERTRAG_ID > 1000` are inserted into `sof_ta_bpr_optionen`.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions:**
    *   In the Cloud Composer Airflow UI, disable the `r_ausd_bp_ta_bpr_optionen` DAG to prevent any further runs.
2.  **Revert to Original System:**
    *   Re-enable and resume the original UC4 job `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN.xml` in the legacy environment.
3.  **Data Restoration (if necessary):**
    *   If data in `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` was corrupted or incorrectly loaded, restore it from the most recent valid backup or by re-running the original Oracle job to repopulate it. Note that the `TRUNCATE` and `INSERT` pattern means the target table is fully overwritten, so corruption of *source* data is less likely from this job itself.
    *   If the `job_audit_log` or `dwtk_meldungen` tables were affected, restore them from BigQuery table snapshots or backups.
4.  **Clean Up Migrated Artifacts (Optional):**
    *   Once the legacy system is confirmed to be fully operational, you may choose to:
        *   Delete the `r_ausd_bp_ta_bpr_optionen` BigQuery Stored Procedure.
        *   Delete the `r_ausd_bp_ta_bpr_optionen` DAG from Cloud Composer.
        *   Delete the BigQuery tables (`job_audit_log`, `dwtk_meldungen`, `sof_ta_bpr_optionen`, `sof_ta_bpr_instance`) if they were created solely for this migration and are not used by other processes.