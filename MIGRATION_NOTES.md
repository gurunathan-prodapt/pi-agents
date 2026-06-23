# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_bp_ta_msisdn_his.ksh` from a legacy Unix/KornShell environment to Google Cloud Platform (GCP).

The original script was responsible for:
*   Orchestrating the provisioning of basic product data for the BERT system.
*   Generating a snapshot of the Data Warehouse (DWH) contract cache.
*   Making this data available for "Forderungsscoring" (demand scoring).
*   Handling command-line parameter parsing, environment initialization, logging, and error handling.
*   Invoking a downstream KornShell script, `k_ausd_bp_ta_msisdn_his.ksh`, which contains the core data processing logic.

The job has been migrated to the following target platform:
*   **BigQuery:** For data storage, transformation, and execution of the core business logic via Stored Procedures.
*   **Cloud Composer (Airflow):** For scheduling, orchestration, and parameter management.
*   **Cloud Logging & Monitoring:** For centralized logging and operational oversight.

The migration involved transforming the shell script's orchestration logic into a BigQuery Stored Procedure (`ausd_bp_ta_msisdn_his_wrapper_sp`) and an Airflow DAG (`r_ausd_bp_ta_msisdn_his_dag`). The core data processing logic, originally in `k_ausd_bp_ta_msisdn_his.ksh`, is conceptually migrated to another BigQuery Stored Procedure (`ausd_bp_ta_msisdn_his_core_sp`), though its detailed implementation remains a placeholder due to the unavailability of the source script.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`job_audit_ddl.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_audit` table. This table serves as a centralized log for tracking the execution status, parameters, and messages of the migrated job, replacing the file-based logging of the legacy system.
    *   **Location:** `project.dataset.job_audit`

*   **`job_error_log_ddl.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table captures detailed error information, including messages, timestamps, and stack traces, for any failures encountered during job execution. It replaces the legacy error handling and logging mechanisms.
    *   **Location:** `project.dataset.job_error_log`

*   **`ausd_bp_ta_msisdn_his_core_sp.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the core data extraction, transformation, and loading (ETL) logic. This procedure is intended to replace the functionality of the original `k_ausd_bp_ta_msisdn_his.ksh` script. **Note:** This file currently contains placeholder logic based on assumptions from the design document, as the source `k_ausd_bp_ta_msisdn_his.ksh` was not available for detailed analysis. It requires manual completion.
    *   **Location:** `project.dataset.ausd_bp_ta_msisdn_his_core_sp`

*   **`ausd_bp_ta_msisdn_his_wrapper_sp.sql`**
    *   **Role:** BigQuery Stored Procedure that acts as the primary entry point for the job. It handles parameter parsing, defaulting, job auditing (inserting into `job_audit` and `job_error_log`), and invokes the `ausd_bp_ta_msisdn_his_core_sp`. This procedure replaces the orchestration logic of `r_ausd_bp_ta_msisdn_his.ksh`.
    *   **Location:** `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`

*   **`r_ausd_bp_ta_msisdn_his_dag.py`**
    *   **Role:** An Airflow Directed Acyclic Graph (DAG) written in Python. This DAG is responsible for scheduling the job, passing parameters (like `p_stichtag` and `p_wiederanlaufWert`) to the `ausd_bp_ta_msisdn_his_wrapper_sp`, and monitoring its execution. It replaces the cron-based scheduling and manual invocation of the legacy KornShell script.
    *   **Location:** Cloud Composer environment (e.g., `dags/r_ausd_bp_ta_msisdn_his_dag.py`)

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Business Logic:** The core and wrapper logic are implemented as BigQuery Stored Procedures. This decision leverages BigQuery's native SQL capabilities for data manipulation, its scalability, and performance for DWH operations. It centralizes the business logic within the data platform, simplifying maintenance and improving data governance.
*   **Cloud Composer (Airflow) for Orchestration:** Airflow was chosen for orchestration due to its robust scheduling capabilities, ability to manage complex workflows, native integration with GCP services (like BigQuery), and rich monitoring features. It provides a modern, scalable, and observable platform for job execution, replacing the simpler KornShell wrapper and cron scheduling.
*   **Centralized BigQuery Audit and Error Logging:** Instead of disparate file-based logs, `job_audit` and `job_error_log` tables in BigQuery provide a structured, queryable, and centralized mechanism for tracking job execution and errors. This significantly improves observability, debugging, and reporting.
*   **Parameter Handling in Wrapper SP:** The `ausd_bp_ta_msisdn_his_wrapper_sp` is designed to handle parameter parsing and defaulting (`p_stichtag`, `p_wiederanlaufWert`), mirroring the functionality of the original `r_ausd_bp_ta_msisdn_his.ksh`. This keeps the Airflow DAG cleaner and focuses the parameter logic within the BigQuery environment.
*   **Replacement of Shell Utilities:** Legacy shell utilities (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are replaced by native BigQuery SQL functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`, `SAFE_CAST`), BigQuery's error handling (`EXCEPTION WHEN ERROR THEN`), and Airflow's parameter passing. This eliminates dependencies on the legacy shell environment.
*   **Trade-off: Placeholder Core Logic:** The most significant trade-off is the current placeholder implementation of `ausd_bp_ta_msisdn_his_core_sp`. This was necessary because the source `k_ausd_bp_ta_msisdn_his.ksh` was not available for analysis. This means the migration is incomplete and requires a critical manual step to fully implement the core business logic.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **Analyze `k_ausd_bp_ta_msisdn_his.ksh` and Complete `ausd_bp_ta_msisdn_his_core_sp.sql`:**
    *   **Action:** Obtain the original `k_ausd_bp_ta_msisdn_his.ksh` script. Perform a detailed analysis to understand its exact data extraction, transformation, and loading logic (source tables, join conditions, filters, aggregations, target schema).
    *   **Update:** Replace the placeholder logic within `ausd_bp_ta_msisdn_his_core_sp.sql` with the fully translated BigQuery SQL. This is a **critical prerequisite** for the job to function correctly.

2.  **BigQuery Dataset and Table Creation:**
    *   **Action:** Ensure the target BigQuery dataset (`project.dataset`) exists.
    *   **Action:** Execute `job_audit_ddl.sql` and `job_error_log_ddl.sql` to create the audit and error logging tables.
    *   **Action:** Create the necessary source DWH tables (e.g., `project.dataset.dwh_contract_cache`) and target tables for "Forderungsscoring" (e.g., `project.dataset.fos_target_table`) in BigQuery. The schemas for these tables must align with the requirements identified during the analysis of `k_ausd_bp_ta_msisdn_his.ksh`.

3.  **IAM/Permissions Configuration:**
    *   **Action:** Grant the service account used by Cloud Composer (or the specific Airflow worker) appropriate BigQuery permissions:
        *   `BigQuery Data Editor` on `project.dataset` to allow writing to `job_audit`, `job_error_log`, and the target "Forderungsscoring" tables.
        *   `BigQuery Data Viewer` on source DWH tables (e.g., `project.dataset.dwh_contract_cache`).
        *   `BigQuery Job User` to execute BigQuery jobs and stored procedures.
    *   **Action:** Ensure the service account has permissions to deploy and manage Airflow DAGs in Cloud Composer.

4.  **Cloud Composer Environment Setup:**
    *   **Action:** Deploy the `r_ausd_bp_ta_msisdn_his_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Action:** Verify that the `GCP_PROJECT_ID` and `BQ_DATASET_ID` variables in `r_ausd_bp_ta_msisdn_his_dag.py` are updated with the correct values for your GCP environment.

5.  **Scheduling Configuration:**
    *   **Action:** Review and adjust the `schedule_interval` in `r_ausd_bp_ta_msisdn_his_dag.py` to match the required execution frequency of the original job.

6.  **"Forderungsscoring" Integration:**
    *   **Action:** Define and implement the integration method for the "Forderungsscoring" system to consume data from the BigQuery target tables. This might involve setting up BigQuery views, external tables, data exports to Cloud Storage, or Pub/Sub notifications, depending on the downstream system's requirements.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or unresolved during the migration design and require follow-up:

*   **Core Script (`k_ausd_bp_ta_msisdn_his.ksh`) Logic:** This is the most critical gap. The detailed data extraction, transformation, and loading logic from `k_ausd_bp_ta_msisdn_his.ksh` was not available. The `ausd_bp_ta_msisdn_his_core_sp.sql` is a placeholder and **must be fully implemented** based on a thorough analysis of the original script.
*   **"Forderungsscoring" Integration:** The precise method by which the "Forderungsscoring" system consumes the output data is unclear. This needs to be determined and implemented to ensure seamless integration post-migration.
*   **Historical `MIN(sysdate,maxladedatum)` Logic:** The original wrapper script comments suggested `MIN(sysdate, maxladedatum)` for `Stichtag` determination, but the implemented code always used `sysdate`. It needs to be clarified if the `maxladedatum` logic is truly abandoned or if `k_ausd_bp_ta_msisdn_his.ksh` handles this. If `maxladedatum` logic is critical, it must be incorporated into the BigQuery migration.
*   **Error Code Mapping:** The legacy script uses specific error codes (e.g., 192, 193). While BigQuery's `RAISE` and Airflow's error handling provide robust mechanisms, a specific mapping strategy for these legacy error codes to BigQuery/Airflow error messages should be established if granular error differentiation is required by downstream systems.
*   **`BERT_DIR_ROOT` Variable:** This environment variable was heavily used in the legacy environment. Its value and contents (e.g., paths to common utilities) need to be mapped to appropriate GCP project/dataset paths or configuration values, or their functionalities fully absorbed into the BigQuery/Airflow solution.

## 6. Validation

To validate the successful migration and functionality of the job:

1.  **Trigger the Airflow DAG:**
    *   Navigate to the Airflow UI for your Cloud Composer environment.
    *   Find the `r_ausd_bp_ta_msisdn_his_dag` DAG.
    *   Manually trigger a run, optionally providing specific `p_stichtag_input` and `p_wiederanlaufWert_input` parameters if testing specific scenarios (e.g., a historical date or a restart value).

2.  **Monitor Airflow Task Status:**
    *   Observe the DAG run in the Airflow UI. The `call_ausd_bp_ta_msisdn_his_wrapper_sp` task should transition from `running` to `success`.
    *   Check the task logs for any errors or unexpected output.

3.  **Verify BigQuery Audit Tables:**
    *   Query the `project.dataset.job_audit` table:
        ```sql
        SELECT * FROM `project.dataset.job_audit` WHERE job_name = 'r_ausd_bp_ta_msisdn_his' ORDER BY start_timestamp DESC LIMIT 10;
        ```
    *   **Passing Criteria:** The latest entry for this job should have `status = 'SUCCESS'` and `end_timestamp` populated.
    *   Query the `project.dataset.job_error_log` table:
        ```sql
        SELECT * FROM `project.dataset.job_error_log` WHERE job_name = 'r_ausd_bp_ta_msisdn_his' ORDER BY error_timestamp DESC;
        ```
    *   **Passing Criteria:** There should be **no new entries** in the `job_error_log` table corresponding to the successful DAG run.

4.  **Validate Target Data:**
    *   Query the target "Forderungsscoring" table (e.g., `project.dataset.fos_target_table`) in BigQuery.
    *   **Passing Criteria:**
        *   Verify that new data has been inserted corresponding to the `p_stichtag` used in the run.
        *   Perform data quality checks:
            *   Count of records: Compare with expected counts from the legacy system for the same `Stichtag`.
            *   Data integrity: Check for `NULL` values in critical columns, correct data types.
            *   Business logic: Spot-check a sample of records to ensure transformations (e.g., date filtering, `DWH_VERTRAG_ID` filtering for restart) were applied correctly as per the original `k_ausd_bp_ta_msisdn_his.ksh` logic.
            *   Ensure the `p_wiederanlaufWert` parameter correctly filtered `DWH_VERTRAG_ID` values.

**"Passing" means:**
*   The Airflow DAG completes successfully without any task failures.
*   The `job_audit` table shows a `SUCCESS` status for the corresponding job run.
*   The `job_error_log` table contains no entries for the job run.
*   The target BigQuery tables contain the expected data, correctly transformed and filtered, matching the output of the legacy system for the same input parameters.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Disable Airflow DAG:**
    *   In the Cloud Composer Airflow UI, locate the `r_ausd_bp_ta_msisdn_his_dag`.
    *   Toggle the DAG to "Off" to prevent any further scheduled executions.

2.  **Revert to Legacy Script:**
    *   Re-enable the original `r_ausd_bp_ta_msisdn_his.ksh` script in its legacy environment (e.g., re-activate its cron job).
    *   Verify that the legacy script is running as expected and producing the correct output.

3.  **Data Reversion (Conditional):**
    *   If the migrated job introduced incorrect or corrupted data into the target BigQuery tables, a data rollback might be necessary.
    *   **Action:** Restore the affected BigQuery target tables (e.g., `project.dataset.fos_target_table`) to a previous known good state using BigQuery's time travel feature (`FOR SYSTEM_TIME AS OF TIMESTAMP '...'`) or from a backup if available.
    *   **Note:** This step requires a clear understanding of the impact of the corrupted data and a defined data recovery strategy.

4.  **Remove Migrated Artifacts (Optional, after successful rollback):**
    *   Once the legacy system is confirmed to be stable and operational, the migrated BigQuery Stored Procedures and Airflow DAG can be removed or archived from the GCP environment.
    *   This step should only be performed after a thorough root cause analysis of the failure and a decision to either re-migrate or abandon the migration.