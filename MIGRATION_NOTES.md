# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh`. This script was responsible for orchestrating a contract data reconciliation process, including environment initialization, parameter parsing, logging, error handling, and invoking a core processing script (`k_ausd_v_ta_acc_ref.ksh`).

The job has been migrated to Google Cloud Platform, leveraging:
*   **Google BigQuery** for the core wrapper logic, logging, and error handling.
*   **Cloud Composer (Apache Airflow)** for job orchestration and scheduling.

The primary goal was to re-platform the orchestration and logging mechanisms, while acknowledging that the core business logic within `k_ausd_v_ta_acc_ref.ksh` requires a separate, detailed migration effort.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`sql/ddl/job_audit_log.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_audit_log` table. This table centralizes job execution metadata, including start/end times, status, messages, and parameters, replacing the filesystem-based logging of the legacy script.
*   **`sql/ddl/job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table captures detailed error information, such as timestamps, messages, error codes, and stack traces, replacing the legacy filesystem-based error logging.
*   **`bigquery_sp/k_ausd_v_ta_acc_ref.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure named `k_ausd_v_ta_acc_ref`. This procedure is intended to house the migrated core business logic from the original `k_ausd_v_ta_acc_ref.ksh` script. Its current implementation is a stub that simulates success or failure, pending the full migration of the complex business logic.
*   **`bigquery_sp/vertragsdatenabgleich_wrapper.sql`**
    *   **Role:** The main BigQuery Stored Procedure, `vertragsdatenabgleich_wrapper`, which is the direct migration of `r_ausd_v_ta_acc_ref.ksh`. It handles parameter validation, orchestrates the call to `k_ausd_v_ta_acc_ref`, and manages logging and error handling by inserting records into `job_audit_log` and `job_error_log`.
*   **`airflow_dag/r_ausd_v_ta_acc_ref_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) designed for Cloud Composer. This DAG is responsible for scheduling and invoking the `vertragsdatenabgleich_wrapper` BigQuery Stored Procedure, passing necessary parameters. It replaces the legacy scheduler that triggered `r_ausd_v_ta_acc_ref.ksh`.

## 3. Key design decisions

*   **Migration of Wrapper to BigQuery Stored Procedure:** The original KornShell wrapper script (`r_ausd_v_ta_acc_ref.ksh`), which primarily handled orchestration, parameter parsing, and logging, was migrated to a BigQuery Stored Procedure (`vertragsdatenabgleich_wrapper`).
    *   **Why:** This approach centralizes the job's control flow within BigQuery, leveraging its native capabilities for execution, error handling, and variable management. It reduces the need for external compute resources for simple orchestration tasks and aligns with a BigQuery-centric data platform strategy.
    *   **Trade-offs:** Shell-specific features like `trap` for signal handling and sourcing of utility scripts required re-implementation using BigQuery SQL's `BEGIN...EXCEPTION...END` blocks and explicit procedure calls/logic integration.
*   **Centralized Logging to BigQuery Tables:** Filesystem-based logging and error reporting were replaced by dedicated BigQuery tables (`job_audit_log` and `job_error_log`).
    *   **Why:** Provides a scalable, queryable, and centralized logging solution within the BigQuery ecosystem. This simplifies monitoring, debugging, and auditing compared to distributed log files.
*   **Core Logic (`k_ausd_v_ta_acc_ref.ksh`) as a Placeholder SP:** The core business logic script `k_ausd_v_ta_acc_ref.ksh` was migrated as a placeholder BigQuery Stored Procedure.
    *   **Why:** This acknowledges its critical dependency and allows the wrapper migration to proceed, while deferring the complex and potentially multi-faceted migration of the core business logic to a separate, dedicated effort. This modular approach reduces the immediate scope and risk of the wrapper migration.
    *   **Trade-offs:** The end-to-end solution is not fully functional until the `k_ausd_v_ta_acc_ref` placeholder is replaced with its actual implementation.
*   **Orchestration via Cloud Composer (Airflow):** The scheduling and invocation of the BigQuery Stored Procedure are managed by an Airflow DAG deployed on Cloud Composer.
    *   **Why:** Cloud Composer provides a robust, scalable, and managed orchestration service on GCP, offering advanced scheduling, monitoring, and dependency management capabilities, which is a significant upgrade from legacy schedulers.
*   **Parameter Handling and Validation:** Command-line parameters (`-s`, `-l`, `-h`) from the original script are mapped to explicit input parameters of the BigQuery Stored Procedure. Validation logic is implemented using BigQuery SQL `IF/ELSEIF` blocks.
    *   **Why:** Ensures type safety and clear definition of inputs, consistent with BigQuery stored procedure best practices.
*   **Error Handling with `BEGIN...EXCEPTION...END`:** The shell's `trap` mechanism for error handling is replaced by BigQuery SQL's `BEGIN...EXCEPTION...END` blocks.
    *   **Why:** Provides structured error handling within the SQL context, allowing for graceful failure, logging of error details, and controlled exits.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `project.dataset`) exists. If not, create it in your GCP project.
2.  **Deploy BigQuery DDLs:**
    *   Execute `sql/ddl/job_audit_log.sql` to create the `job_audit_log` table.
    *   Execute `sql/ddl/job_error_log.sql` to create the `job_error_log` table.
3.  **Deploy BigQuery Stored Procedures:**
    *   Execute `bigquery_sp/k_ausd_v_ta_acc_ref.sql` to create the placeholder core logic procedure.
    *   Execute `bigquery_sp/vertragsdatenabgleich_wrapper.sql` to create the main wrapper procedure.
4.  **Implement `k_ausd_v_ta_acc_ref` Core Logic:**
    *   **Crucially, the placeholder `bigquery_sp/k_ausd_v_ta_acc_ref.sql` must be fully implemented and thoroughly tested.** This involves migrating the actual business logic, data sources, and targets from the original `k_ausd_v_ta_acc_ref.ksh` script.
5.  **IAM/Permissions Configuration:**
    *   Ensure the service account used by your Cloud Composer environment (or any other orchestrator) has the necessary BigQuery permissions. This typically includes:
        *   `BigQuery Data Editor` role on the `project.dataset` dataset to insert/update log entries and execute stored procedures.
        *   `BigQuery Job User` role to run BigQuery jobs.
6.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
7.  **Deploy Airflow DAG:**
    *   Upload `airflow_dag/r_ausd_v_ta_acc_ref_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   **Update DAG Parameters:** Modify the `airflow_dag/r_ausd_v_ta_acc_ref_dag.py` to replace placeholder values for `p_s_parameter` and `p_l_parameter` with actual, dynamic, or Airflow variable-driven values.
    *   **Configure Schedule:** Set the `schedule` parameter in the DAG to define the desired execution frequency (e.g., `@daily`, `0 5 * * *`).

## 5. Known gaps & unresolved references

The following items are flagged for follow-up or represent areas requiring further attention:

*   **Full Migration of `k_ausd_v_ta_acc_ref.ksh`:** The `bigquery_sp/k_ausd_v_ta_acc_ref.sql` is currently a placeholder. The complete migration of its complex business logic, including data transformations, source/target interactions, and specific reconciliation rules, is the largest outstanding item and requires a dedicated analysis and implementation effort.
*   **Utility Script Logic Integration:** The functionalities of legacy utility scripts (`$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) have been conceptually replaced. While error messaging and parameter handling are integrated into the wrapper SP, specific date handling or environment initialization logic might need further explicit implementation within BigQuery UDFs or directly in the SPs if not fully covered.
*   **Legacy Data Sources:** The `k_ausd_v_ta_acc_ref.ksh` script likely interacts with specific legacy data sources. These sources must be migrated to BigQuery tables or made accessible via BigQuery federated queries (e.g., Cloud SQL, external tables) before the core logic can be fully implemented.
*   **Airflow DAG Parameterization:** The `p_s_parameter` and `p_l_parameter` in the Airflow DAG are currently hardcoded placeholder strings. These should be replaced with dynamic values, Airflow variables, or XComs to provide flexibility and proper configuration for production runs.
*   **`DWMSG_*` Error Code Mapping:** While the general error logging mechanism is in place, a detailed mapping of specific `DWMSG_*` error codes and messages from the legacy system to the new `job_error_log` structure might be required for complete functional parity and easier debugging.
*   **Environment Variable Equivalents:** The reliance on shell environment variables like `$HOME` and `$BERT_DIR_ROOT` in the legacy script has been replaced by explicit parameters or BigQuery-native variables. A comprehensive review is needed to ensure all such dependencies are correctly mapped and configured in the new environment.

## 6. Validation

To ensure the successful migration and functionality of the job, the following validation steps should be performed:

### How to run the tests

1.  **Deploy all generated artifacts** as described in "Manual steps before go-live" (including the fully implemented `k_ausd_v_ta_acc_ref` if available).
2.  **Trigger the Airflow DAG:**
    *   From the Cloud Composer UI, manually trigger the `r_ausd_v_ta_acc_ref_wrapper_dag`.
    *   Alternatively, if a schedule is configured, wait for the next scheduled run.
3.  **Monitor Airflow Task Logs:** Observe the logs of the `call_vertragsdatenabgleich_wrapper` task in the Airflow UI for any errors or unexpected behavior.
4.  **Query BigQuery Log Tables:**
    *   Execute `SELECT * FROM `project.dataset.job_audit_log` ORDER BY insert_timestamp DESC LIMIT 10;`
    *   Execute `SELECT * FROM `project.dataset.job_error_log` ORDER BY insert_timestamp DESC LIMIT 10;`
5.  **Test Error Scenarios:**
    *   Modify the Airflow DAG to pass `NULL` values for `p_s_parameter` or `p_l_parameter` to trigger the parameter validation error.
    *   (If `k_ausd_v_ta_acc_ref` is implemented) Introduce a controlled error within `k_ausd_v_ta_acc_ref` to test its error handling and logging.
    *   Test the `-h` flag by setting `p_h_flag => TRUE` in the DAG's BigQuery call.

### What "passing" means

A successful migration and functional validation are indicated by the following:

*   **Airflow DAG Success:** The `r_ausd_v_ta_acc_ref_wrapper_dag` completes successfully in Cloud Composer (green status).
*   **Audit Log Entries:**
    *   For a successful run, the `job_audit_log` table contains two entries for the same `job_entry_number`: one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
    *   The `end_timestamp` and `message` fields in the `COMPLETED` entry are correctly populated.
    *   The `parameters` JSON field accurately reflects the input parameters.
*   **Error Log Absence (for successful runs):** For successful executions, there are no corresponding entries in the `job_error_log` table for that `job_entry_number`.
*   **Error Handling Validation:**
    *   When an expected error (e.g., missing parameters) is triggered, the `job_audit_log` shows `status = 'FAILED'`, and a corresponding entry exists in `job_error_log` with the correct `error_message` and `error_code`.
    *   The Airflow task should fail gracefully in error scenarios, indicating the issue.
*   **Help Flag Functionality:** When `p_h_flag` is set to `TRUE`, the BigQuery stored procedure should output the usage message and exit without performing any other actions or logging `STARTED`/`COMPLETED` entries (it should return immediately).
*   **Core Logic Output (once implemented):** The `k_ausd_v_ta_acc_ref` stored procedure (once fully implemented) produces the expected data reconciliation results in the target BigQuery tables, matching the output of the legacy system.

## 7. Rollback procedure

In case of issues or unexpected behavior after deployment, the following steps outline the rollback procedure to revert to the legacy system:

1.  **Deactivate New Orchestration:**
    *   In Cloud Composer, deactivate or delete the `r_ausd_v_ta_acc_ref_wrapper_dag` to prevent further execution of the migrated job.
2.  **Re-enable Legacy Job:**
    *   Reactivate the original `r_ausd_v_ta_acc_ref.ksh` script in its legacy scheduler (e.g., cron, Autosys, etc.).
3.  **Remove BigQuery Stored Procedures (Optional):**
    *   If necessary, drop the deployed BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.vertragsdatenabgleich_wrapper`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_v_ta_acc_ref`;
        ```
4.  **Remove BigQuery Log Tables (Optional):**
    *   If these tables were created solely for this migration and are not used by other processes, they can be dropped. **Exercise caution as this will delete all logged history.**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_audit_log`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        ```
5.  **Data Rollback (Critical for Core Logic):**
    *   If the fully implemented `k_ausd_v_ta_acc_ref` (which is currently a placeholder) performed any data modifications to production tables, a specific data rollback strategy must be executed. This could involve:
        *   Restoring affected tables from a point-in-time backup.
        *   Executing compensating transactions or scripts to revert changes.
        *   This step is highly dependent on the actual implementation of `k_ausd_v_ta_acc_ref` and should be planned in detail as part of its separate migration.

It is crucial to thoroughly test the rollback procedure in a non-production environment before go-live.