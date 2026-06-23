# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh`. This script, originally a wrapper for orchestrating the deletion of temporary intermediate tables, has been migrated to a BigQuery-native solution within the Google Cloud Platform (GCP) ecosystem.

The migration involved:
- Replacing the KornShell wrapper logic with a BigQuery Stored Procedure (`k_drop_temp_table_wrapper`).
- Encapsulating the core temporary table dropping logic (originally in `k_drop_temp_table.ksh`) into another BigQuery Stored Procedure (`k_drop_temp_table_core`).
- Replacing file-based logging with dedicated BigQuery audit and status log tables.
- Replacing the UC4 scheduler with a Cloud Composer (Airflow) DAG for orchestration.

## 2. Generated artifacts

The migration process generated the following files:

-   **`sql/ddl/job_audit_log.sql`**
    -   **Role:** Defines the Data Definition Language (DDL) for the `project.dataset.job_audit_log` BigQuery table. This table centralizes all operational logs, replacing the legacy file-based logging.
-   **`sql/ddl/job_status_log.sql`**
    -   **Role:** Defines the DDL for the `project.dataset.job_status_log` BigQuery table. This table tracks the overall status (OK/FAILED) of job executions.
-   **`sql/stored_procedures/k_drop_temp_table_core.sql`**
    -   **Role:** A BigQuery Stored Procedure (`project.dataset.k_drop_temp_table_core`) designed to contain the actual DDL/DML logic for dropping temporary tables. This procedure replaces the functionality of the original `k_drop_temp_table.ksh` script. **Note: This file currently contains a placeholder; its core logic needs to be implemented based on the original `k_drop_temp_table.ksh` content.**
-   **`sql/stored_procedures/k_drop_temp_table_wrapper.sql`**
    -   **Role:** A BigQuery Stored Procedure (`project.dataset.k_drop_temp_table_wrapper`) that replaces the `r_drop_temp_table.ksh` wrapper script. It handles parameter parsing, default value assignment, audit logging, error handling, and orchestrates the call to `k_drop_temp_table_core`.
-   **`dags/bert_drop_temp_table_dag.py`**
    -   **Role:** An Apache Airflow DAG written in Python. This DAG is responsible for scheduling and invoking the `project.dataset.k_drop_temp_table_wrapper` BigQuery Stored Procedure, replacing the legacy UC4 job definition.

## 3. Key design decisions

-   **BigQuery Stored Procedures for Core Logic:** The decision to use BigQuery Stored Procedures (`k_drop_temp_table_wrapper` and `k_drop_temp_table_core`) was driven by the assumption that the underlying cleanup logic in `k_drop_temp_table.ksh` is primarily SQL-centric. This approach leverages BigQuery's native capabilities for DDL/DML operations, ensuring high performance and scalability within the data warehouse environment.
-   **Separation of Wrapper and Core Logic:** Mirroring the original KornShell structure, the migration maintains a clear separation between the wrapper (parameter handling, logging, error management) and the core business logic (actual table dropping). This enhances modularity, reusability, and maintainability.
-   **Cloud Composer (Airflow) for Orchestration:** Cloud Composer was chosen to replace the legacy UC4 scheduler. Airflow provides a robust, cloud-native, and highly configurable platform for orchestrating data pipelines, offering better visibility, monitoring, and integration with other GCP services.
-   **Centralized BigQuery Logging:** File-based logging in the legacy system was replaced by structured logging into dedicated BigQuery tables (`job_audit_log`, `job_status_log`). This provides a centralized, queryable, and scalable solution for auditing job executions, making it easier to monitor, debug, and analyze job performance and failures.
-   **Error Handling with `BEGIN...EXCEPTION`:** BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks were utilized to replicate the `set -e` and `trap ERR` mechanisms of KornShell, ensuring robust error handling and proper logging of failures.

**Notable Trade-offs:**

-   **Unknown Core Logic of `k_drop_temp_table.ksh`:** The most significant trade-off is the current placeholder status of `k_drop_temp_table_core`. The actual DDL/DML from the original `k_drop_temp_table.ksh` was not available during the design phase. This introduces a critical dependency on a manual implementation step and carries the risk of misinterpreting complex non-SQL logic if present in the original script.
-   **Re-implementation of Custom Shell Functions:** Legacy custom shell functions (e.g., `DWMSG_*`, date formatting utilities) were re-implemented using native BigQuery SQL functions. While functionally equivalent, subtle behavioral differences or edge cases might exist and require thorough testing.

## 4. Manual steps before go-live

The following manual steps are required to deploy and operate the migrated job:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP Project is available.
    *   Create the target BigQuery dataset (e.g., `your_bigquery_dataset_id`) where the tables and stored procedures will reside.
2.  **IAM Permissions:**
    *   Grant the service account used by Cloud Composer/Airflow the necessary BigQuery roles (e.g., `BigQuery Data Editor` for the dataset, `BigQuery Job User`) to execute stored procedures and write to log tables.
    *   Ensure the service account has permissions to create tables and stored procedures during deployment.
3.  **Deploy BigQuery DDLs:**
    *   Execute `sql/ddl/job_audit_log.sql` to create the `job_audit_log` table.
    *   Execute `sql/ddl/job_status_log.sql` to create the `job_status_log` table.
4.  **Implement `k_drop_temp_table_core` Logic (CRITICAL):**
    *   **Manually translate the full DDL/DML logic from the original `k_drop_temp_table.ksh` script into the `sql/stored_procedures/k_drop_temp_table_core.sql` file.** This is a **Blocker (B4)** item and must be completed before the job can function correctly.
    *   Ensure the logic correctly handles the `p_stichtag` and `p_wiederanlauf_wert` parameters as per the original script's intent (e.g., `DELETE FROM ... WHERE dwh_vertrag_id >= p_wiederanlauf_wert`).
    *   Include appropriate `INSERT` statements into `job_audit_log` within `k_drop_temp_table_core` to log key operations and progress.
5.  **Deploy BigQuery Stored Procedures:**
    *   Execute the updated `sql/stored_procedures/k_drop_temp_table_core.sql` to create or replace the stored procedure.
    *   Execute `sql/stored_procedures/k_drop_temp_table_wrapper.sql` to create or replace the stored procedure.
6.  **Configure Airflow Connection:**
    *   Ensure the `google_cloud_default` Airflow connection is properly configured in your Cloud Composer environment, pointing to your GCP project.
7.  **Deploy Airflow DAG:**
    *   Upload `dags/bert_drop_temp_table_dag.py` to your Cloud Composer DAGs folder.
    *   **Update `GCP_PROJECT_ID` and `BIGQUERY_DATASET` placeholders** in the DAG file with your actual project and dataset IDs.
    *   Configure the `schedule` parameter in the DAG definition as required (e.g., `@daily`, `0 3 * * *`) or leave as `None` for manual/external triggers.
8.  **Parameter Configuration for DAG:**
    *   Decide how `p_stichtag_in` and `p_wiederanlauf_wert_in` will be passed to the `k_drop_temp_table_wrapper` procedure. This can be done via Airflow's `arguments` parameter in the `BigQueryExecuteStoredProcedureOperator` task, potentially using Jinja templating for dynamic values (e.g., `{{ ds_nodash }}`). If omitted, the stored procedure's internal defaults will apply.

## 5. Known gaps & unresolved references

-   **`k_drop_temp_table.ksh` Core Logic (B4 Item):** The most significant gap is the unknown content of the original `k_drop_temp_table.ksh`. The `k_drop_temp_table_core` stored procedure is currently a placeholder and requires manual implementation based on the legacy script's actual DDL/DML. If `k_drop_temp_table.ksh` contains non-SQL operations (e.g., filesystem manipulations, calls to external systems), a redesign might be necessary, potentially involving Cloud Functions or Cloud Run.
-   **Exact Behavior of Legacy Shell Functions:** The precise behavior and any subtle nuances of custom KornShell functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and `DWMSG_*` have been translated to BigQuery SQL equivalents. While the general intent is captured, thorough testing is needed to ensure exact functional parity.
-   **`file_complexity` Information:** The absence of complexity tier information for the original script means the migration effort was estimated without a formal complexity assessment, potentially underestimating unforeseen challenges.
-   **`p_wiederanlaufWert` Logic:** The specific implementation of how `p_wiederanlaufWert` affects the `DELETE` or `DROP` operations in the core script needs careful translation to BigQuery SQL to ensure data integrity and correct filtering (e.g., `dwh_vertrag_id >= p_wiederanlauf_wert`).
-   **Airflow DAG Placeholders:** The `GCP_PROJECT_ID` and `BIGQUERY_DATASET` variables in `dags/bert_drop_temp_table_dag.py` must be updated with actual values.

## 6. Validation

To validate the successful migration and functionality:

1.  **Unit Test `k_drop_temp_table_core` (Post-Implementation):**
    *   **How to run:** After implementing the core logic, execute `CALL project.dataset.k_drop_temp_table_core('TEST_JOB', '01012023', 123, 0);` in BigQuery, varying `p_stichtag` and `p_wiederanlauf_wert`.
    *   **Passing means:** The procedure executes without errors, performs the expected DDL/DML operations (e.g., drops tables, deletes rows), and logs appropriate `INFO` messages to `job_audit_log`.
2.  **Unit Test `k_drop_temp_table_wrapper`:**
    *   **How to run:** Execute `CALL project.dataset.k_drop_temp_table_wrapper('01012023', 12345);` in BigQuery. Test with `NULL` for both parameters to verify default handling.
    *   **Passing means:** The procedure completes successfully, `job_audit_log` contains `INFO` messages for start, core call, and successful completion, and `job_status_log` shows an 'OK' entry. Test error scenarios by introducing a deliberate error in `k_drop_temp_table_core` and verify `job_audit_log` shows `ERROR` and `job_status_log` shows 'FAILED'.
3.  **Integration Test via Airflow DAG:**
    *   **How to run:** Trigger the `bert_drop_temp_table_dag` in Cloud Composer. Test with different DAG run configurations, passing various `p_stichtag_in` and `p_wiederanlauf_wert_in` values via Airflow's `conf` parameter or by hardcoding them in the DAG for specific test runs.
    *   **Passing means:** The Airflow DAG runs successfully (all tasks turn green). The `k_drop_temp_table_wrapper` procedure is invoked correctly. BigQuery `job_audit_log` and `job_status_log` tables reflect the successful execution and correct parameter passing. The underlying cleanup operations performed by `k_drop_temp_table_core` are verified to be correct.

## 7. Rollback procedure

In case of critical issues, unexpected behavior, or a decision to revert the migration, follow these steps:

1.  **Deactivate Airflow DAG:** Pause or delete the `bert_drop_temp_table_dag` in Cloud Composer to prevent further execution of the migrated job.
2.  **Re-enable Legacy UC4 Job:** Reactivate the original UC4 job definition (`DW.BERT_DROP_TEMP_TABLE.xml`) to resume operations using the legacy `r_drop_temp_table.ksh` script.
3.  **Verify Legacy System Functionality:** Monitor the legacy job to ensure it is running as expected and performing its cleanup tasks correctly.
4.  **Optional: Clean Up Migrated Assets:**
    *   If the rollback is permanent or for a significant redesign, consider dropping the BigQuery stored procedures (`k_drop_temp_table_wrapper`, `k_drop_temp_table_core`) and the log tables (`job_audit_log`, `job_status_log`) from the BigQuery dataset.
    *   Remove the `bert_drop_temp_table_dag.py` file from the Cloud Composer DAGs folder.
5.  **Root Cause Analysis:** Investigate the reasons for the rollback to inform future migration attempts or redesigns. Ensure no data loss or corruption occurred during the failed migration attempt.