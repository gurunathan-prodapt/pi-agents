# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_cntrct_crs2.ksh` and its associated SQL logic to Google Cloud Platform, specifically leveraging BigQuery. The original script, responsible for orchestrating a data processing job related to `ta_cntrct_crs2` data, including parameter parsing, error handling, and record counting, has been re-platformed.

The migration target is a BigQuery-native environment where:
*   The orchestration logic of the KornShell script is replaced by a main BigQuery Stored Procedure (`control_k_ausd_v_ta_cntrct_crs2`).
*   The core SQL transformation logic (originally in `d_ausd_v_ta_cntrct_crs2.sql`) is encapsulated within another BigQuery Stored Procedure (`sp_d_ausd_v_ta_cntrct_crs2`).
*   All source and target Oracle tables (`DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_CRS`, `SOF$TA_CNTRCT_CRS2`, `VIA`) are migrated to BigQuery tables.
*   Job status management and execution logging are handled by dedicated BigQuery tables (`job_table`, `job_run_log`) and helper stored procedures (`sp_job_prepare`, `sp_log_error`).

## 2. Generated Artifacts

The migration process generated the following BigQuery DDL and Stored Procedure files:

*   **`bq_ddl/dwtk_meldungen.sql`**:
    *   **Role**: Defines the BigQuery table schema for `bq_dataset.dwtk_meldungen`. This table serves as the migrated destination for the Oracle `DWTK_MELDUNGEN` table, which is a source for the data processing job.
*   **`bq_ddl/sof_ta_cntrct_crs.sql`**:
    *   **Role**: Defines the BigQuery table schema for `bq_dataset.sof_ta_cntrct_crs`. This table serves as the migrated destination for the Oracle `SOF$TA_CNTRCT_CRS` table, another source for the data processing job.
*   **`bq_ddl/sof_ta_cntrct_crs2.sql`**:
    *   **Role**: Defines the BigQuery table schema for `bq_dataset.sof_ta_cntrct_crs2`. This table is the primary target for the processed data, replacing the Oracle `SOF$TA_CNTRCT_CRS2` table.
*   **`bq_ddl/via.sql`**:
    *   **Role**: Defines the BigQuery table schema for `bq_dataset.via`. This table is another target for the processed data, replacing the Oracle `VIA` table. Its exact purpose needs further clarification.
*   **`bq_ddl/job_table.sql`**:
    *   **Role**: Defines the BigQuery table schema for `bq_dataset.job_table`. This table is used for managing the activation status of various jobs, replacing the implicit job management logic in the original KornShell environment.
*   **`bq_ddl/job_run_log.sql`**:
    *   **Role**: Defines the BigQuery table schema for `bq_dataset.job_run_log`. This table captures detailed execution logs for each job run, including start/end times, status, processed record counts, and error messages, replacing the temporary file and shell-based logging of the original script.
*   **`bq_sp/sp_log_error.sql`**:
    *   **Role**: A helper BigQuery Stored Procedure responsible for logging error details into the `bq_dataset.job_run_log` table. It centralizes error reporting, replacing aspects of the `f_alis_msgerr.ksh` utility.
*   **`bq_sp/sp_job_prepare.sql`**:
    *   **Role**: A helper BigQuery Stored Procedure that manages the activation and deactivation of jobs within the `bq_dataset.job_table`. It also allows checking a job's active status, replacing the job management logic implied by comments in the original KornShell script.
*   **`bq_sp/sp_d_ausd_v_ta_cntrct_crs2.sql`**:
    *   **Role**: The core BigQuery Stored Procedure that encapsulates the data transformation logic. This procedure is a translation of the original `d_ausd_v_ta_cntrct_crs2.sql` Oracle script, adapted for BigQuery Standard SQL. It accepts job parameters and returns the count of processed records. (Note: Contains placeholder logic, requires actual Oracle SQL translation).
*   **`bq_sp/control_k_ausd_v_ta_cntrct_crs2.sql`**:
    *   **Role**: The main BigQuery Stored Procedure that orchestrates the entire data processing flow. It replaces the `k_ausd_v_ta_cntrct_crs2.ksh` KornShell script, handling parameter validation, job activation checks, calling the core transformation procedure, and logging execution details and errors to `bq_dataset.job_run_log`.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration and Logic**: The primary decision was to re-platform the entire job into BigQuery Stored Procedures. This leverages BigQuery's native capabilities for ETL, reducing external dependencies and simplifying deployment within the GCP ecosystem.
    *   **Trade-off**: Requires re-writing KornShell logic and Oracle PL/SQL into BigQuery SQL, which can be a significant effort, especially for complex PL/SQL packages. However, it offers better performance, scalability, and maintainability within BigQuery.
*   **BigQuery Tables for All Data**: All source and target tables are migrated to BigQuery. This ensures data locality and optimizes performance for BigQuery-native transformations.
*   **Centralized Logging and Job Management**: Dedicated BigQuery tables (`job_run_log`, `job_table`) and helper stored procedures (`sp_log_error`, `sp_job_prepare`) are introduced. This provides a structured, queryable, and scalable mechanism for monitoring job executions and managing job states, replacing disparate shell-based logging and implicit job control.
*   **Direct Parameter Passing**: Command-line parameters from the KornShell script are directly translated into input parameters for the BigQuery Stored Procedures. This simplifies the interface and removes the need for environment variable management or complex parsing.
*   **Elimination of Temporary Files**: The original script's use of temporary files for passing record counts is replaced by direct return values from stored procedures and logging to `job_run_log`. This is a more robust and BigQuery-native approach.
*   **Re-implementation of Utility Logic**: Generic shell utilities and Oracle PL/SQL packages are either re-implemented as BigQuery UDFs/SPs or their logic is directly integrated into the main procedures. This ensures all logic resides within BigQuery, minimizing external dependencies.
*   **Cloud Composer as Optional External Orchestrator**: While the primary orchestration is within BigQuery, the design acknowledges Cloud Composer (Airflow) as a potential external orchestrator for more complex scheduling, dependency management, or integration with other GCP services. This provides flexibility for future expansion.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `bq_dataset` (or the chosen target dataset name) exists in your GCP project. If not, create it.
    *   `bq mk --dataset your_gcp_project_id:bq_dataset`

2.  **Schema Creation (DDL Execution)**:
    *   Execute all DDL scripts (`bq_ddl/*.sql`) in the target BigQuery dataset to create the necessary tables:
        *   `bq_ddl/dwtk_meldungen.sql`
        *   `bq_ddl/sof_ta_cntrct_crs.sql`
        *   `bq_ddl/sof_ta_cntrct_crs2.sql`
        *   `bq_ddl/via.sql`
        *   `bq_ddl/job_table.sql`
        *   `bq_ddl/job_run_log.sql`
    *   This can be done via the BigQuery UI, `bq` command-line tool, or a deployment script.

3.  **Stored Procedure Deployment**:
    *   Execute all Stored Procedure scripts (`bq_sp/*.sql`) in the target BigQuery dataset to create the procedures:
        *   `bq_sp/sp_log_error.sql`
        *   `bq_sp/sp_job_prepare.sql`
        *   `bq_sp/sp_d_ausd_v_ta_cntrct_crs2.sql` (Ensure placeholder logic is replaced with actual translated SQL)
        *   `bq_sp/control_k_ausd_v_ta_cntrct_crs2.sql`
    *   This can be done via the BigQuery UI, `bq` command-line tool, or a deployment script.

4.  **IAM / Permissions**:
    *   Ensure the service account or user identity that will execute the BigQuery Stored Procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` on `bq_dataset` to write to `sof_ta_cntrct_crs2`, `via`, `job_table`, `job_run_log`.
        *   `BigQuery Data Viewer` on `bq_dataset` to read from `dwtk_meldungen`, `sof_ta_cntrct_crs`, `job_table`.
        *   `BigQuery Job User` to run BigQuery jobs.

5.  **Initial Data Ingestion**:
    *   Ingest historical and/or initial data from the Oracle `DWTK_MELDUNGEN` and `SOF$TA_CNTRCT_CRS` tables into their respective BigQuery counterparts (`bq_dataset.dwtk_meldungen`, `bq_dataset.sof_ta_cntrct_crs`). This typically involves using services like BigQuery Data Transfer Service, Dataflow, or custom ETL scripts.

6.  **Job Table Initialization**:
    *   Insert an initial entry for `p_job_kennung` (e.g., 'TA_CNTRCT_CRS2') into `bq_dataset.job_table` with an appropriate initial status (e.g., 'INACTIVE' or 'ACTIVE' if ready for testing).
    *   Example: `INSERT INTO bq_dataset.job_table (job_kennung, job_description, status, last_update_time, updated_by) VALUES ('TA_CNTRCT_CRS2', 'Contract CRS2 Processing Job', 'INACTIVE', CURRENT_TIMESTAMP(), 'initial_setup');`

7.  **Scheduling (if applicable)**:
    *   If using Cloud Composer (Airflow), deploy the corresponding DAG that triggers `bq_dataset.control_k_ausd_v_ta_cntrct_crs2`.
    *   Alternatively, set up a BigQuery Scheduled Query to call the main stored procedure at the desired frequency.
    *   For manual execution, the procedure can be called directly.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further follow-up:

*   **Exact Schema for Source/Target Tables**: The DDLs for `dwtk_meldungen`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_crs2`, and `via` are placeholders. The actual schemas from the Oracle source system must be obtained and used to create accurate BigQuery DDLs.
*   **Full `d_ausd_v_ta_cntrct_crs2.sql` Content**: The detailed SQL logic of the original `d_ausd_v_ta_cntrct_crs2.sql` was not available. The `bq_sp/sp_d_ausd_v_ta_cntrct_crs2.sql` procedure contains placeholder logic and *must be replaced* with the fully translated and verified BigQuery Standard SQL equivalent of the original Oracle script.
*   **Oracle Package Functionality**: The precise implementation details of Oracle PL/SQL packages `DWPA_UTIL_SKRIPT` and `CR` are critical. Their functionality needs to be reverse-engineered and accurately reimplemented as BigQuery UDFs or integrated directly into `sp_d_ausd_v_ta_cntrct_crs2`.
*   **`TABLE:VIA` Purpose**: The exact purpose and data structure of the `VIA` table, and how `d_ausd_v_ta_cntrct_crs2.sql` interacts with it, need to be clarified to ensure correct migration and data integrity.
*   **Exact Logic of Sourced Shell Scripts**: The full functionality of the original KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and `h_alis_job.ksh`) needs to be thoroughly understood. While some aspects are covered by `sp_log_error` and `sp_job_prepare`, any remaining specific logic (e.g., date formatting, advanced parameter handling) must be translated or accounted for.
*   **Job Table Schema Details**: While a basic `job_table` DDL is provided, a comprehensive understanding of all fields and their usage in the original system's job management is required to ensure the BigQuery `job_table` fully replicates necessary functionality.
*   **Error Handling Granularity**: The current error handling in `control_k_ausd_v_ta_cntrct_crs2` provides a general error message. Depending on the original `f_alis_msgerr.ksh` functionality, more granular error codes or specific error types might need to be implemented.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Prepare Test Data**:
    *   Ensure `bq_dataset.dwtk_meldungen` and `bq_dataset.sof_ta_cntrct_crs` contain representative test data.
    *   Ensure `bq_dataset.job_table` has an entry for `TA_CNTRCT_CRS2` with `status = 'ACTIVE'`.

2.  **Execute the Main Stored Procedure**:
    *   Call the main control procedure with sample parameters:
        ```sql
        CALL `bq_dataset.control_k_ausd_v_ta_cntrct_crs2`('TA_CNTRCT_CRS2', 'TEST_ENTRY_001');
        ```
    *   Observe the output in the BigQuery console or job logs.

3.  **Verify "Passing" Criteria**:
    *   **Successful Execution**: The `CALL` statement completes without raising an unhandled exception.
    *   **Log Entry**: Query `bq_dataset.job_run_log` for the `run_id` generated during the execution.
        *   The `status` column for this `run_id` should be 'SUCCESS'.
        *   The `records_processed` column should reflect the expected number of records processed by `sp_d_ausd_v_ta_cntrct_crs2`.
        *   `start_time` and `end_time` should be populated correctly.
        *   `error_message` should be NULL.
    *   **Target Data Verification**:
        *   Query `bq_dataset.sof_ta_cntrct_crs2` and `bq_dataset.via` to confirm that the expected data has been inserted/updated correctly, matching the logic of the original `d_ausd_v_ta_cntrct_crs2.sql`.
        *   Perform data comparison between the original Oracle target tables (if still available with test data) and the new BigQuery target tables to ensure data integrity and transformation accuracy.
    *   **Job Status**: Query `bq_dataset.job_table` to ensure the job's status remains 'ACTIVE' (or as expected if the job logic modifies it).
    *   **Error Handling (Negative Test)**:
        *   Attempt to call the procedure with invalid parameters (e.g., `NULL` `p_job_kennung`) or when the job is 'INACTIVE' in `job_table`.
        *   Verify that the procedure raises an error and `bq_dataset.job_run_log` records a 'FAILED' status with an appropriate `error_message`.

## 7. Rollback Procedure

In case of critical issues or incorrect behavior after deployment, the following rollback procedure can be followed:

1.  **Stop New BigQuery Executions**:
    *   Immediately disable any scheduled queries or Cloud Composer DAGs that trigger `bq_dataset.control_k_ausd_v_ta_cntrct_crs2`.
    *   If the job is managed via `bq_dataset.job_table`, call `CALL bq_dataset.sp_job_prepare('TA_CNTRCT_CRS2', 'DEACTIVATE', @is_active);` to prevent further runs.

2.  **Revert to Original System**:
    *   Resume execution of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh` script on the legacy platform. Ensure all necessary configurations and dependencies for the original script are in place and functional.

3.  **Data Remediation (if necessary)**:
    *   If the BigQuery job produced incorrect data in `bq_dataset.sof_ta_cntrct_crs2` or `bq_dataset.via`, determine the extent of the incorrect data.
    *   **Option A (Truncate/Delete)**: If the impact is limited and a full refresh is acceptable, truncate or delete the affected partitions/data from `bq_dataset.sof_ta_cntrct_crs2` and `bq_dataset.via`.
    *   **Option B (Point-in-Time Recovery)**: If BigQuery's time travel feature is enabled and the window allows, restore the tables to a state before the erroneous run.
    *   **Option C (Corrective Script)**: Develop and execute a BigQuery SQL script to correct the erroneous data.
    *   **Note**: Data in the original Oracle target tables should ideally be unaffected by the BigQuery migration. If the Oracle tables were also modified (e.g., by a dual-write strategy during migration), their rollback would involve restoring from backups or running corrective scripts on the Oracle side.

4.  **Analyze and Rectify**:
    *   Investigate the root cause of the failure or incorrect behavior in the BigQuery implementation.
    *   Apply necessary fixes to the BigQuery DDLs, Stored Procedures, or data ingestion pipelines.
    *   Re-test thoroughly in a non-production environment before attempting another go-live.