# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_vertrag_tmp.ksh`. This script, originally responsible for orchestrating data preparation, environment setup, parameter parsing, error handling, and triggering an underlying SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) that operates on the `ta_vertrag_tmp` table, has been re-engineered.

The migration targets Google Cloud Platform, leveraging BigQuery for data storage and processing, and Apache Airflow (via Cloud Composer) for job orchestration. The original KornShell script, being in the B0 migration bucket (retire), has been completely replaced by cloud-native components rather than a direct lift-and-shift.

## 2. Generated Artifacts

The migration process has generated the following files, each serving a specific role in the new BigQuery and Airflow environment:

*   **`project/dataset/ta_vertrag_tmp.bqsql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script for creating the `ta_vertrag_tmp` table. This table will serve as the target for the data processing logic previously handled by the original SQL script.
*   **`project/dataset/error_log.bqsql`**
    *   **Role**: BigQuery DDL script for creating the `error_log` table. This table is designed to capture and store error messages and details, replacing the shell-based error logging mechanisms of the original script.
*   **`project/dataset/job_run_log.bqsql`**
    *   **Role**: BigQuery DDL script for creating the `job_run_log` table. This table will record job execution details, including timestamps, job identifiers, and processed record counts, replacing temporary file outputs and implicit logging.
*   **`project/dataset/d_ausd_v_ta_vertrag_tmp.bqsql`**
    *   **Role**: BigQuery Stored Procedure. This procedure is a placeholder for the core data manipulation logic originally contained within `d_ausd_v_ta_vertrag_tmp.sql`. It will be responsible for performing the actual data transformations and operations on `ta_vertrag_tmp`.
*   **`project/dataset/r_ausd_vertrag.bqsql`**
    *   **Role**: BigQuery Stored Procedure. This procedure encapsulates the control and orchestration logic of the original `k_ausd_v_ta_vertrag_tmp.ksh` script. It handles parameter validation, error logging, and calls the `d_ausd_v_ta_vertrag_tmp` procedure.
*   **`k_ausd_v_ta_vertrag_tmp_dag.py`**
    *   **Role**: Apache Airflow Directed Acyclic Graph (DAG). This Python script defines the orchestration workflow, allowing the `r_ausd_vertrag` BigQuery Stored Procedure to be scheduled and executed within a Cloud Composer environment.

## 3. Key Design Decisions

The migration strategy for `k_ausd_v_ta_vertrag_tmp.ksh` was driven by its B0 (retire) automation bucket classification, necessitating a complete re-engineering rather than a direct conversion. Key design decisions include:

*   **Re-engineering to Cloud-Native Components**: Instead of attempting to emulate KornShell behavior on GCP, the decision was made to leverage BigQuery's native capabilities for both data processing and procedural logic. This aligns with modern cloud data warehousing best practices and reduces operational overhead associated with legacy scripting environments.
*   **BigQuery Stored Procedures for Control Logic**: The orchestration, parameter handling, and error management logic of the original KornShell script are reimplemented as a BigQuery Stored Procedure (`r_ausd_vertrag`). This centralizes the control flow within the data platform, benefiting from BigQuery's scalability, security, and integration with other GCP services.
*   **BigQuery Tables for Logging**: Shell-based temporary files and basic `echo` statements for logging have been replaced by dedicated BigQuery tables (`error_log`, `job_run_log`). This provides structured, queryable, and persistent logging, significantly improving observability and troubleshooting capabilities.
*   **Apache Airflow for Orchestration**: The original script's execution environment (likely cron or manual invocation) is replaced by an Airflow DAG. Airflow offers robust scheduling, dependency management, monitoring, and error handling features, making it a suitable modern orchestrator for complex data pipelines.
*   **Separation of Concerns**: The core data transformation logic (from `d_ausd_v_ta_vertrag_tmp.sql`) is encapsulated in its own BigQuery Stored Procedure (`d_ausd_v_ta_vertrag_tmp`), which is then called by the orchestration procedure (`r_ausd_vertrag`). This promotes modularity and reusability.

**Notable Trade-offs:**

*   **Complete Re-implementation**: While offering long-term benefits, this approach requires a full understanding of the original script's implicit behaviors and helper functions, which can be time-consuming compared to automated translation.
*   **Dependency on Core SQL Analysis**: The effectiveness of this migration heavily relies on the accurate analysis and conversion of the `d_ausd_v_ta_vertrag_tmp.sql` content, which was not part of the initial scope. This introduces a critical dependency and potential for rework if the core SQL logic is complex or uses highly proprietary features.
*   **New Skillset Requirement**: The target platform (BigQuery SQL, Airflow/Python) requires a different skillset than traditional KornShell scripting and Oracle SQL, potentially necessitating training or new hires.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset `project.dataset` in your GCP project. This dataset will house all the migrated tables and stored procedures.
2.  **BigQuery Table and Procedure Deployment**:
    *   Execute the DDL scripts (`ta_vertrag_tmp.bqsql`, `error_log.bqsql`, `job_run_log.bqsql`) to create the necessary tables in the `project.dataset`.
    *   Deploy the BigQuery Stored Procedures (`d_ausd_v_ta_vertrag_tmp.bqsql`, `r_ausd_vertrag.bqsql`) to the `project.dataset`.
3.  **IAM Permissions Configuration**:
    *   Ensure the service account used by Cloud Composer (Airflow) has the necessary BigQuery permissions:
        *   `bigquery.datasets.get`
        *   `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.tables.getData`, `bigquery.tables.list`
        *   `bigquery.routines.create`, `bigquery.routines.update`, `bigquery.routines.call`
    *   Verify that any user or service account interacting directly with the BigQuery procedures has appropriate permissions.
4.  **Data Ingestion for `ta_vertrag_tmp`**:
    *   If the `ta_vertrag_tmp` table's source data originates from an external system (e.g., Oracle), a dedicated data ingestion pipeline must be established to load this data into `project.dataset.ta_vertrag_tmp` in BigQuery. This pipeline is outside the scope of this specific migration but is critical for the job's functionality.
5.  **Airflow Environment Setup**:
    *   Ensure a Cloud Composer environment (or a self-managed Airflow instance) is provisioned and operational.
    *   Verify the `google_cloud_default` connection is correctly configured in Airflow, pointing to the target GCP project.
6.  **Airflow DAG Deployment**:
    *   Upload the `k_ausd_v_ta_vertrag_tmp_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Configure the desired schedule for the DAG in Airflow. The generated DAG currently has `schedule=None`, meaning it will only run manually or via external triggers. Adjust this as needed for production.
7.  **Secrets Management (if applicable)**:
    *   If `p_JobKennung` or `p_EintragsNr` (or any future parameters) contain sensitive information, consider managing them as Airflow Variables or using Google Secret Manager, rather than hardcoding or passing directly in DAG parameters.

## 5. Known Gaps & Unresolved References

The following items have been identified as gaps or require further analysis and resolution:

*   **Core SQL Script Analysis (`d_ausd_v_ta_vertrag_tmp.sql`)**: This is the most significant unresolved item. The actual data manipulation logic resides in this original SQL file, which was not provided for analysis. The generated `d_ausd_v_ta_vertrag_tmp.bqsql` is a placeholder. **This file must be thoroughly analyzed, and its logic accurately converted to BigQuery SQL before the migration can be considered complete.** This is a **B4 (Redesign)** item.
*   **`ta_vertrag_tmp` Table Schema Details**: The precise schema, data types, and indexing requirements of the original `ta_vertrag_tmp` table are unknown. The generated DDL for `project.dataset.ta_vertrag_tmp` includes placeholder columns. The actual DDL must be derived from the source system's schema definition.
*   **Implicit Logic in Sourced Helper Scripts**: The original KornShell script sourced several helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While their general functions have been accounted for, any subtle or implicit business logic embedded within these scripts might not have been fully captured and re-implemented. A review of these original helper scripts is recommended if unexpected behavior is observed.
*   **Original `file_complexity` and `Migration Flags`**: These were not available for the source KornShell script, which might indicate specific challenges or considerations that were not automatically identified during the initial assessment.

## 6. Validation

Validation ensures that the migrated job functions correctly and produces the expected output.

**How to Run Tests:**

1.  **BigQuery Stored Procedure Unit Tests**:
    *   **`d_ausd_v_ta_vertrag_tmp`**: Once the core SQL logic is implemented, execute this procedure directly in BigQuery (e.g., via the BigQuery UI or `bq query` command-line tool) with sample `p_EintragsNr` and `p_JobKennung` values. Verify that `ta_vertrag_tmp` is updated as expected.
    *   **`r_ausd_vertrag`**: Execute this procedure directly in BigQuery with valid and invalid parameters.
        *   `CALL project.dataset.r_ausd_vertrag('TEST_JOB', 'ENTRY_123');`
        *   `CALL project.dataset.r_ausd_vertrag(NULL, 'ENTRY_123');` (Expected to fail validation)
        *   `CALL project.dataset.r_ausd_vertrag('TEST_JOB', NULL);` (Expected to fail validation)
2.  **Airflow DAG Integration Tests**:
    *   Trigger the `k_ausd_v_ta_vertrag_tmp_dag` manually from the Airflow UI, providing sample `p_job_kennung` and `p_eintrags_nr` parameters via the "Trigger DAG with config" option.
    *   Monitor the DAG run in the Airflow UI for successful completion.
    *   Observe the BigQuery job history for the execution of the stored procedures.

**What "Passing" Means:**

*   **Successful Procedure Execution**: The `r_ausd_vertrag` BigQuery Stored Procedure completes without raising a `SIGNAL SQLSTATE` error for valid inputs.
*   **Correct Parameter Validation**: When invalid parameters are provided to `r_ausd_vertrag`, the procedure correctly logs an entry into `project.dataset.error_log` and terminates with a `SIGNAL SQLSTATE` error.
*   **Core Logic Invocation**: The `r_ausd_vertrag` procedure successfully calls `project.dataset.d_ausd_v_ta_vertrag_tmp`.
*   **Accurate Logging**:
    *   `project.dataset.job_run_log` contains a new entry for each successful run, accurately reflecting the `job_kennung`, `eintrags_nr`, and the `records` count (once `d_ausd_v_ta_vertrag_tmp` is fully implemented and updates `ta_vertrag_tmp`).
    *   `project.dataset.error_log` contains appropriate entries for failed validation attempts.
*   **Data Integrity**: The `project.dataset.ta_vertrag_tmp` table is populated or updated correctly according to the business logic defined in the migrated `d_ausd_v_ta_vertrag_tmp` procedure.
*   **Airflow DAG Success**: The `k_ausd_v_ta_vertrag_tmp_dag` runs to completion in Airflow without any task failures.

## 7. Rollback Procedure

Given that this is a B0 (retire) migration, the original `k_ausd_v_ta_vertrag_tmp.ksh` script is intended to be decommissioned. A rollback would effectively mean reverting to the legacy execution environment and process.

To roll back the migration:

1.  **Deactivate New Components**:
    *   In Airflow, pause or delete the `k_ausd_v_ta_vertrag_tmp_dag`.
    *   (Optional) Delete the BigQuery Stored Procedures (`r_ausd_vertrag`, `d_ausd_v_ta_vertrag_tmp`) and the logging tables (`error_log`, `job_run_log`) from `project.dataset`. **Do NOT delete `ta_vertrag_tmp` unless its data can be fully restored or is not critical.**
2.  **Re-enable Legacy Process**:
    *   Re-enable the original `k_ausd_v_ta_vertrag_tmp.ksh` script in its legacy execution environment (e.g., re-add its entry to cron, or resume manual execution).
    *   Ensure all original dependencies (Oracle database, helper scripts, etc.) are operational and accessible.
3.  **Data State Reconciliation**:
    *   If any data was processed by the new BigQuery job before rollback, assess the state of `ta_vertrag_tmp` in BigQuery and the original system. Data reconciliation or a full data reload into the original system might be necessary to ensure consistency.

**Note**: As this is a re-engineering effort, a direct "undo" of code changes is not applicable. The rollback focuses on switching back to the original, retired system.