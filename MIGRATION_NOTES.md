# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh`, which orchestrates the contract data reconciliation for the `ta_vvl_dwh` table, has been migrated. The migration targets Google BigQuery, leveraging BigQuery Stored Procedures for both the orchestration logic and the core data processing. Shell-based environment setup, parameter parsing, logging, and error handling have been translated into BigQuery SQL scripting constructs and dedicated logging/audit tables.

## 2. Generated Artifacts

The migration produced the following BigQuery SQL stored procedures and DDL for logging/audit tables:

*   **`stp/k_ausd_v_ta_vvl_dwh.sql`**
    *   **Role:** This BigQuery stored procedure (`my_project.my_dataset.k_ausd_v_ta_vvl_dwh`) is the direct migration target for the original `k_ausd_v_ta_vvl_dwh.ksh` core processing script. It acts as an intermediary, handling logging and error trapping around the invocation of the actual data reconciliation logic. It calls `my_project.my_dataset.d_ausd_v_ta_vvl_dwh` to perform the core DML operations.
*   **`stp/d_ausd_v_ta_vvl_dwh.sql`**
    *   **Role:** This BigQuery stored procedure (`my_project.my_dataset.d_ausd_v_ta_vvl_dwh`) is a placeholder for the actual data reconciliation logic. It is intended to contain the translated DML operations (INSERT, UPDATE, DELETE) that were originally present in the `d_ausd_v_ta_vvl_dwh.sql` script invoked by `k_ausd_v_ta_vvl_dwh.ksh`. It is responsible for reading from source tables, applying transformations, and updating the `ta_vvl_dwh` table.
*   **`project.dataset.Vertragsdatenabgleich` (Conceptual)**
    *   **Role:** This is the primary wrapper stored procedure, conceptually replacing `r_ausd_v_ta_vvl_dwh.ksh`. It handles parameter validation, environment setup (via parameters/config), job registry updates, and orchestrates the call to `my_project.my_dataset.k_ausd_v_ta_vvl_dwh`. (Note: The actual SQL for this wrapper SP was not provided in the generated code but is a key part of the design).
*   **`my_project.my_dataset.dw_job_registry` (DDL)**
    *   **Role:** BigQuery table to store job execution metadata (start/end times, status, script name, unique entry number). This replaces the implicit log file management and status tracking of the original shell script.
*   **`my_project.my_dataset.dw_job_log` (DDL)**
    *   **Role:** BigQuery table to store detailed log messages generated during job execution. This centralizes logging, replacing direct `print` and `tee` operations.
*   **`my_project.my_dataset.dw_error_log` (DDL)**
    *   **Role:** BigQuery table to store detailed error information, including messages and stack traces. This replaces the `DWMSG_MeldeFehler` calls and shell error trapping.
*   **`my_project.my_dataset.ta_vvl_dwh` (DDL - assumed)**
    *   **Role:** The target table for the data reconciliation process. Its schema must be defined in BigQuery.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration and Logic:** The entire shell-based workflow, including environment setup, parameter parsing, logging, and core logic invocation, has been translated into BigQuery Stored Procedures. This leverages BigQuery's native capabilities for data processing and scripting, eliminating the need for external shell environments.
*   **Separation of Concerns:** The original script's separation into a wrapper (`r_ausd_v_ta_vvl_dwh.ksh`) and a core processing script (`k_ausd_v_ta_vvl_dwh.ksh`) has been maintained. The wrapper (`Vertragsdatenabgleich` SP) handles orchestration, while `k_ausd_v_ta_vvl_dwh` SP acts as an intermediary for the actual DML logic in `d_ausd_v_ta_vvl_dwh` SP.
*   **Centralized Logging and Auditing:** Shell-based logging and error handling (`print`, `tee`, `DWMSG_*` functions) have been replaced by dedicated BigQuery tables (`dw_job_registry`, `dw_job_log`, `dw_error_log`). This provides a structured, queryable, and centralized audit trail for all job executions.
*   **BigQuery SQL Scripting for Utilities:** Common shell utilities for parameter handling, date formatting, and error messaging have been absorbed into BigQuery SQL scripting features (e.g., `DECLARE`, `IF`, `FORMAT_DATE`, `BEGIN...EXCEPTION WHEN ERROR...END`). This reduces external dependencies and keeps the logic within the BigQuery ecosystem.
*   **Placeholder for Core DML:** The `d_ausd_v_ta_vvl_dwh` stored procedure is explicitly designed as a placeholder for the actual data reconciliation DML. This acknowledges that the detailed SQL logic was not available in the initial analysis and requires further dedicated translation.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`my_project.my_dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA `my_project.my_dataset`;
        ```
2.  **BigQuery Table Creation (DDL):**
    *   Create the logging and audit tables:
        ```sql
        -- dw_job_registry
        CREATE TABLE `my_project.my_dataset.dw_job_registry` (
            dw_entry_nr INT64 OPTIONS(description="Unique entry number for job execution"),
            job_name STRING OPTIONS(description="Name of the job/script"),
            start_time TIMESTAMP OPTIONS(description="Job start timestamp"),
            end_time TIMESTAMP OPTIONS(description="Job end timestamp"),
            status STRING OPTIONS(description="Execution status (e.g., 'RUNNING', 'OK', 'FAILED')"),
            log_file_name STRING OPTIONS(description="Generated log file name (for reference)"),
            stichtag_info STRING OPTIONS(description="Stichtag information if applicable")
        );

        -- dw_job_log
        CREATE TABLE `my_project.my_dataset.dw_job_log` (
            dw_entry_nr INT64 OPTIONS(description="Foreign key to dw_job_registry"),
            log_time TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
            message_type STRING OPTIONS(description="Type of message (e.g., 'INFO', 'WARNING', 'ERROR')"),
            message_text STRING OPTIONS(description="Detailed log message")
        );

        -- dw_error_log
        CREATE TABLE `my_project.my_dataset.dw_error_log` (
            dw_entry_nr INT64 OPTIONS(description="Foreign key to dw_job_registry"),
            error_time TIMESTAMP OPTIONS(description="Timestamp of the error"),
            error_code STRING OPTIONS(description="Custom or SQL error code"),
            error_message STRING OPTIONS(description="Detailed error message"),
            stack_trace STRING OPTIONS(description="SQL stack trace for the error")
        );
        ```
    *   Create the target data table `ta_vvl_dwh` and any necessary source/staging tables that `d_ausd_v_ta_vvl_dwh` will interact with. The schema for these tables must match the requirements of the reconciliation logic.
3.  **IAM / Permissions:**
    *   The service account or user executing the BigQuery stored procedures must have the following IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` to create/update/delete data in `dw_job_registry`, `dw_job_log`, `dw_error_log`, and `ta_vvl_dwh`.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
        *   `BigQuery Data Viewer` on any source tables read by `d_ausd_v_ta_vvl_dwh`.
4.  **Connection Strings / Configuration:**
    *   No explicit connection strings are needed for BigQuery native operations. However, ensure that `my_project` and `my_dataset` placeholders in the generated code are replaced with the actual project ID and dataset name.
    *   If `d_ausd_v_ta_vvl_dwh` needs to access external systems (e.g., Cloud Storage, other databases via federated queries), ensure the necessary connections and credentials are configured in BigQuery.
5.  **Secrets Management:**
    *   If the core logic in `d_ausd_v_ta_vvl_dwh` requires any sensitive information (e.g., API keys for external services), these should be managed securely, for example, using Google Secret Manager and accessed within the stored procedure or passed as parameters.
6.  **Scheduling:**
    *   Configure a scheduler to invoke the main wrapper stored procedure (`my_project.my_dataset.Vertragsdatenabgleich`). Options include:
        *   **Cloud Composer (Apache Airflow):** Create a DAG that calls the BigQuery stored procedure. This is recommended for complex workflows or dependency management.
        *   **BigQuery Scheduled Queries:** If the job is simple and doesn't have external dependencies, a scheduled query can be set up to `CALL` the stored procedure.
        *   **Cloud Functions/Cloud Run:** Triggered by Pub/Sub or HTTP, these can execute the stored procedure.

## 5. Known Gaps & Unresolved References

*   **Core DML Logic for `d_ausd_v_ta_vvl_dwh`:** The most significant gap is the actual data reconciliation logic within `d_ausd_v_ta_vvl_dwh.sql`. The provided generated code contains only a placeholder. This requires a detailed analysis of the original `d_ausd_v_ta_vvl_dwh.sql` (or equivalent) to translate its DML operations into BigQuery SQL. This is a **B4 (Redesign/Re-implementation)** item.
*   **Wrapper Stored Procedure (`Vertragsdatenabgleich`):** The SQL for the main wrapper stored procedure, which directly replaces `r_ausd_v_ta_vvl_dwh.ksh` and handles parameter parsing, job registry updates, and the initial call to `k_ausd_v_ta_vvl_dwh`, was not generated. This needs to be developed based on the design document.
*   **Specifics of `ta_vvl_dwh` Reconciliation:** Without the content of `d_ausd_v_ta_vvl_dwh.sql`, the precise business rules, data sources, and target schema for `ta_vvl_dwh` are not fully defined. This impacts the complete validation of the data transformation.
*   **Error Code Mapping:** The original script used specific error codes (e.g., 192, 193). While BigQuery's `ERROR_MESSAGE()` and `ERROR_STACK_TRACE()` provide rich detail, a mapping strategy for these specific legacy error codes to the `dw_error_log.error_code` column might be needed for consistency with existing monitoring.
*   **Idempotency and Restartability:** While the logging framework supports tracking, the actual DML in `d_ausd_v_ta_vvl_dwh` must be designed to be idempotent and restartable, especially for reconciliation tasks. This needs careful consideration during the translation of the core DML.
*   **Performance Tuning:** The placeholder DML in `d_ausd_v_ta_vvl_dwh` will require thorough performance testing and tuning once implemented, especially for large datasets.

## 6. Validation

Validation involves ensuring the migrated stored procedures execute correctly, log appropriately, and produce the expected data outcomes.

*   **How to Run Tests:**
    1.  **Deploy DDL:** Ensure all logging tables (`dw_job_registry`, `dw_job_log`, `dw_error_log`) and the target `ta_vvl_dwh` table (with appropriate test data) are created in BigQuery.
    2.  **Deploy Stored Procedures:** Deploy `my_project.my_dataset.k_ausd_v_ta_vvl_dwh` and `my_project.my_dataset.d_ausd_v_ta_vvl_dwh` (with its placeholder logic, or ideally, with initial translated DML).
    3.  **Invoke Wrapper SP:** Execute the main wrapper stored procedure (`my_project.my_dataset.Vertragsdatenabgleich`) using a BigQuery query, passing test parameters.
        ```sql
        -- Example invocation (assuming the wrapper SP is named Vertragsdatenabgleich)
        CALL `my_project.my_dataset.Vertragsdatenabgleich`(
            p_job_kennung => 'TA_VVL_DWH_TEST',
            p_stichtag => '20231026' -- Example parameter
        );
        ```
    4.  **Test Error Scenarios:** Invoke the wrapper SP with invalid parameters or simulate errors within `d_ausd_v_ta_vvl_dwh` to test error handling.

*   **What "Passing" Means:**
    *   **Successful Execution:** The `CALL` statement for `Vertragsdatenabgleich` completes without BigQuery job errors.
    *   **Job Registry Status:** A new entry is created in `my_project.my_dataset.dw_job_registry` with `status = 'OK'` and accurate `start_time`/`end_time`.
    *   **Detailed Logging:** `my_project.my_dataset.dw_job_log` contains a sequence of `INFO` messages detailing the execution flow, including the start and end of `k_ausd_v_ta_vvl_dwh` and `d_ausd_v_ta_vvl_dwh`.
    *   **No Error Logs (for successful runs):** `my_project.my_dataset.dw_error_log` should have no new entries corresponding to the successful job run. For error test cases, it should contain the expected error details.
    *   **Data Reconciliation Correctness:** The data in `my_project.my_dataset.ta_vvl_dwh` (and any related tables) is updated/reconciled according to the business rules defined in the original `d_ausd_v_ta_vvl_dwh.sql`. This requires a comparison of the BigQuery output with the expected output from the legacy system for a given set of input data.
    *   **Parameter Handling:** The stored procedure correctly parses and utilizes input parameters, and gracefully handles invalid or missing required parameters by logging an error and failing the job.

## 7. Rollback Procedure

In case of issues with the migrated job, the following steps can be taken to roll back to the original system:

1.  **Deactivate New Orchestration:**
    *   If using Cloud Composer, disable or delete the DAG that triggers `my_project.my_dataset.Vertragsdatenabgleich`.
    *   If using BigQuery Scheduled Queries, disable or delete the scheduled query.
    *   If using other schedulers, stop the job that invokes the BigQuery stored procedure.
2.  **Revert to Original Execution:**
    *   Re-enable the scheduling or manual execution of the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh` on the legacy platform.
3.  **Clean Up BigQuery Artifacts (Optional, but recommended):**
    *   **Delete Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.k_ausd_v_ta_vvl_dwh`;
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.d_ausd_v_ta_vvl_dwh`;
        -- DROP PROCEDURE IF EXISTS `my_project.my_dataset.Vertragsdatenabgleich`; -- If it was created
        ```
    *   **Delete Logging/Audit Tables:**
        ```sql
        DROP TABLE IF EXISTS `my_project.my_dataset.dw_job_registry`;
        DROP TABLE IF EXISTS `my_project.my_dataset.dw_job_log`;
        DROP TABLE IF EXISTS `my_project.my_dataset.dw_error_log`;
        ```
    *   **Revert `ta_vvl_dwh` (if modified):** If the `ta_vvl_dwh` table in BigQuery was modified by the migrated job and its state is critical, restore it from a backup or revert to a known good state. If the legacy system continues to write to its own `ta_vvl_dwh` equivalent, this step might not be necessary for the BigQuery version if it's considered a temporary or test table.
    *   **Delete BigQuery Dataset (Optional):** If the entire dataset was created solely for this migration, it can be deleted.
        ```sql
        DROP SCHEMA IF EXISTS `my_project.my_dataset` CASCADE;
        ```