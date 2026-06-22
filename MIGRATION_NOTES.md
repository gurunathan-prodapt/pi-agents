# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell (ksh) script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` and its associated SQL script (`d_ausd_bp_ta_bpr_apn.sql`). The original ksh script served as an orchestration layer, handling environment setup, parameter parsing, date validation, and the execution of a core SQL script, along with rudimentary error handling and job tracking.

The migration targets a BigQuery-native solution, leveraging BigQuery Stored Procedures for both the control flow and the core data transformation logic. Orchestration and scheduling are handled by Google Cloud Composer (Airflow). This transition moves the entire process from a shell-scripted, file-based environment to a fully managed, scalable cloud data warehousing platform.

## 2. Generated artifacts

The migration process has generated the following artifacts:

*   **BigQuery Stored Procedures:**
    *   `project.dataset.r_ausd_bp_ta_bpr_apn`: This is the main control stored procedure, replacing the `k_ausd_bp_ta_bpr_apn.ksh` script. It handles parameter validation, date calculations, error logging, job tracking, and orchestrates the call to the core data processing logic.
    *   `project.dataset.d_ausd_bp_ta_bpr_apn`: This stored procedure encapsulates the data transformation logic originally found in `d_ausd_bp_ta_bpr_apn.sql`, translated into BigQuery Standard SQL.
*   **BigQuery Tables:**
    *   `project.dataset.job_error_log`: A dedicated table for logging execution errors and parameter validation failures, replacing the shell script's `DWMSG_MeldeFehler` function and file-based error logs.
        *   Schema: `job_name STRING`, `error_nr INT64`, `error_arg STRING`, `created_ts TIMESTAMP`
    *   `project.dataset.job_tracking`: A table to record job execution metadata, status, and record counts, replacing the functionality of the commented-out `FOSJobErzeugeEintrag` calls.
        *   Schema: `tab_name STRING`, `status STRING`, `mode STRING`, `stichtag_from DATE`, `stichtag_to DATE`, `job_type STRING`, `restart_flag STRING`, `record_count INT64`, `description STRING`, `created_ts TIMESTAMP`
    *   *(Conditional)* `project.dataset.cibasis_data24_raw`, `project.dataset.cibasis_data96_raw`, `project.dataset.cibasis_fax_raw`: If the commented-out file processing logic was deemed active and migrated, these tables would serve as staging areas for ingested flat files.
    *   *(Conditional)* `project.dataset.cibasis_data24_clean`, `project.dataset.cibasis_data96_clean`, `project.dataset.cibasis_fax_clean`, `project.dataset.cibasis_24_96_tmp`, `project.dataset.cibasisprodukt_csv`: Intermediate and final tables for the migrated file processing logic, if applicable.
*   **Airflow DAG:**
    *   `dags/r_ausd_bp_ta_bpr_apn_dag.py`: An Airflow DAG responsible for scheduling and triggering the `project.dataset.r_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure, passing necessary parameters.

## 3. Key design decisions

The following key design decisions guided this migration:

*   **BigQuery Stored Procedures for Orchestration and Logic:** The core decision was to replace the ksh script's control flow and the dependent SQL script's data transformation with BigQuery Stored Procedures. This centralizes all logic within BigQuery, leveraging its native capabilities for parameter handling, error management, and data processing, eliminating the need for external shell environments.
*   **Cloud Composer (Airflow) for Scheduling and Orchestration:** Airflow was chosen as the external orchestrator to manage the scheduling, parameter passing, and dependency management for the BigQuery Stored Procedures. This provides a robust, scalable, and observable solution for job execution, replacing the legacy scheduler (e.g., cron) and manual script execution.
*   **BigQuery Tables for Logging and Tracking:** Instead of relying on file-based logs or an external job tracking system (like `FOSJobErzeugeEintrag`), dedicated BigQuery tables (`job_error_log`, `job_tracking`) were created. This integrates logging and tracking directly into the BigQuery ecosystem, allowing for easier querying, analysis, and integration with monitoring tools.
*   **Native BigQuery Functions for Utilities:** All utility functions previously handled by external ksh scripts (e.g., `gestern.ksh` for date calculation, `h_alis_date.ksh` for date validation, `h_alis_parameter.ksh` for parsing) are replaced by BigQuery's built-in SQL functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`) and procedural logic within the stored procedures. This reduces external dependencies and simplifies the architecture.
*   **Elimination of Temporary Files:** The reliance on temporary files (e.g., `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp` for record counts, or `cibasis_*.dat` for intermediate data) has been replaced by BigQuery's in-memory processing, temporary tables, or direct querying of target tables. This improves performance, reduces I/O, and simplifies data flow.

**Notable Trade-offs:**

*   **BigQuery-Specific SQL Knowledge:** The migration requires expertise in BigQuery Standard SQL and its procedural language, which differs from traditional SQL dialects (e.g., Oracle PL/SQL).
*   **Migration of Complex PL/SQL:** If `d_ausd_bp_ta_bpr_apn.sql` contained highly complex Oracle PL/SQL, its translation to BigQuery Standard SQL might have required significant refactoring or a different approach (e.g., UDFs, external functions). This was assumed to be straightforward SQL for this design.
*   **Shift from File-based to Table-based Data:** For any migrated file processing, the paradigm shifts from shell commands (`sed`, `sort`, `join`) operating on flat files to SQL operations on BigQuery tables. This requires initial data ingestion into BigQuery.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it in the Google Cloud Console or via `bq mk --dataset project:dataset`.
2.  **IAM Permissions:**
    *   Grant the service account used by Cloud Composer (Airflow) the necessary BigQuery roles:
        *   `BigQuery Data Editor` (for creating/writing to tables and executing stored procedures).
        *   `BigQuery Job User` (for running BigQuery jobs).
        *   `BigQuery Data Viewer` (for reading data, if applicable).
    *   Ensure the service account has permissions to create/manage BigQuery connections if not using the default.
3.  **BigQuery Connection in Airflow:**
    *   Verify that the `google_cloud_default` connection (or a custom BigQuery connection) is correctly configured in Airflow, pointing to the appropriate GCP project.
4.  **Deployment of BigQuery Objects:**
    *   Deploy the `project.dataset.job_error_log` and `project.dataset.job_tracking` tables.
    *   Deploy the `project.dataset.d_ausd_bp_ta_bpr_apn` stored procedure.
    *   Deploy the `project.dataset.r_ausd_bp_ta_bpr_apn` stored procedure.
    *   *(Conditional)* Deploy any BigQuery tables or views related to the migrated file processing (e.g., `cibasis_data24_raw`, `cibasisprodukt_csv`).
5.  **Deployment of Airflow DAG:**
    *   Upload the `dags/r_ausd_bp_ta_bpr_apn_dag.py` file to the Airflow DAGs folder in Cloud Storage.
6.  **Configure DAG Parameters:**
    *   Review and update the placeholder parameters within the `r_ausd_bp_ta_bpr_apn_dag.py` (e.g., `p_JobKennung`, `p_EintragsNr`). These might need to be dynamic based on Airflow context variables or specific configuration.
    *   Set the desired `schedule_interval` for the DAG.
7.  **Initial Data Ingestion (if applicable):**
    *   If the commented-out file processing was migrated, ensure that the initial flat files (e.g., `cibasis_data24.dat`) are ingested into their respective BigQuery raw tables (e.g., `project.dataset.cibasis_data24_raw`) in Cloud Storage.

## 5. Known gaps & unresolved references

The following items were identified as gaps or unresolved references during the design phase and require further attention:

*   **Missing Metadata:** The absence of detailed metadata (technology, complexity, automation bucket) from source analysis tools meant that assumptions were made. A manual review of the original ksh script and its dependencies is recommended to confirm these aspects and ensure no critical details were missed.
*   **`d_ausd_bp_ta_bpr_apn.sql` Content Complexity:** The migration assumed that `d_ausd_bp_ta_bpr_apn.sql` contains standard SQL easily translatable to BigQuery. If it contains complex Oracle PL/SQL, specific database features, or intricate procedural logic, a more detailed redesign and potentially different migration approach (e.g., BigQuery scripting, UDFs, or external Python logic) might be required.
*   **`starteSQLSkript` Functionality:** The exact behavior of the `starteSQLSkript` function within `h_alis_sqlplus.ksh` (e.g., specific error handling, output parsing, connection details, environment variables passed to SQL*Plus) was inferred. Any subtle behaviors not captured in the BigQuery migration could lead to discrepancies.
*   **Commented-Out Logic Decision:** The design noted commented-out sections for file processing (`sed`, `sort`, `join`) and job tracking (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`). It was assumed that if these were commented, they were either inactive or not critical. A definitive decision on whether these functionalities are truly obsolete or need to be migrated (and how) is crucial.
*   **Parameter `p_wiederanlaufWert` Clarification:** The exact purpose and impact of the `p_wiederanlaufWert` (restart value) parameter on the original SQL logic were not fully detailed. Its influence on the migrated `d_ausd_bp_ta_bpr_apn` stored procedure needs to be thoroughly understood and replicated.
*   **Error Code Mapping:** The specific `ErrNr` values (e.g., 192, 193) and their corresponding messages from the original system should be formally documented and mapped to a new, BigQuery-native error logging standard for consistency and clarity.

## 6. Validation

Validation ensures the migrated process functions correctly and produces accurate results.

**How to run tests:**

1.  **Unit Tests (BigQuery Stored Procedures):**
    *   Execute `project.dataset.d_ausd_bp_ta_bpr_apn` directly in BigQuery with various input parameters (including edge cases) to verify its data transformation logic.
    *   Execute `project.dataset.r_ausd_bp_ta_bpr_apn` directly in BigQuery with different parameter combinations (valid, invalid, missing) to test its control flow, validation, error logging, and job tracking.
2.  **Integration Tests (Airflow DAG):**
    *   Trigger the `r_ausd_bp_ta_bpr_apn_dag` in Airflow manually.
    *   Monitor the Airflow UI for task success/failure and logs.
    *   Verify that the BigQuery Stored Procedures are called correctly with the expected parameters.
3.  **Data Validation:**
    *   Run the original `k_ausd_bp_ta_bpr_apn.ksh` script with a specific reference date (`Stichtag`).
    *   Run the migrated Airflow DAG for the *same* reference date.
    *   Compare the output data in the target BigQuery tables with the output generated by the legacy system. This may involve exporting legacy data for comparison or using data comparison tools.
    *   Verify record counts reported by the new system against the old system.

**What "passing" means:**

*   **Successful DAG Execution:** The Airflow DAG completes successfully without any task failures.
*   **No Errors in `job_error_log`:** The `project.dataset.job_error_log` table contains no entries for the test runs, indicating successful parameter validation and execution.
*   **Correct Data in Target Tables:** The data generated in the target BigQuery tables by the migrated process is identical (or functionally equivalent, considering data type changes) to the data produced by the legacy system for the same input.
*   **Accurate Record Counts:** The `record_count` logged in `project.dataset.job_tracking` matches the actual count of records processed and the count reported by the legacy system.
*   **Correct Job Tracking:** Entries in `project.dataset.job_tracking` accurately reflect the job's status, start/end times, and other metadata.
*   **Performance within SLA:** The execution time of the migrated process meets or exceeds the performance requirements of the legacy system.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New Airflow DAG:**
    *   Immediately pause or un-schedule the `r_ausd_bp_ta_bpr_apn_dag` in the Airflow UI to prevent further execution of the migrated process.
2.  **Re-enable Legacy Process:**
    *   Re-enable and re-schedule the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` script in its legacy environment (e.g., cron, existing scheduler).
3.  **Data Rollback (if necessary):**
    *   If the migrated process has written incorrect data to target tables, determine the extent of the impact.
    *   **Option A (Preferred):** If the target tables are new or distinct from the legacy system's outputs, simply revert to using the legacy system's output.
    *   **Option B (If target tables were overwritten/modified):**
        *   Restore the affected BigQuery tables from a previous successful backup or snapshot (if available).
        *   Alternatively, re-run the legacy process to overwrite the incorrect data produced by the migrated job.
4.  **Investigate and Rectify:**
    *   Analyze the logs from Airflow and BigQuery (`job_error_log`) to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery Stored Procedures or Airflow DAG.
5.  **Re-deploy and Re-test:**
    *   Once the issues are resolved, re-deploy the corrected BigQuery objects and Airflow DAG.
    *   Perform thorough re-testing following the validation steps before attempting another go-live.