# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh` has been migrated. This script, which served as an orchestration layer for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system, has been re-implemented in Google Cloud's BigQuery ecosystem.

The core orchestration logic, including parameter handling, logging, error management, and invocation of the main processing logic, has been converted into a BigQuery Stored Procedure. The target platform is Google BigQuery, leveraging its procedural language capabilities and native table storage for logging.

## 2. Generated artifacts

The migration process has generated the following BigQuery artifacts:

*   **`src/bigquery/ddl/job_log.sql`**
    *   **Role:** This DDL script creates the `project.dataset.job_log` table. This table serves as the central repository for all job execution logs, status updates, error messages, and key parameters (e`Stichtag`, `Wiederanlaufwert`, `DW_EintragsNr`). It replaces the original shell script's file-based logging and framework-specific logging functions (`DWMSG_*`).

*   **`src/bigquery/procedures/k_ausd_bp_ta_bpr_basis.sql`**
    *   **Role:** This SQL script defines a placeholder BigQuery Stored Procedure named `project.dataset.k_ausd_bp_ta_bpr_basis`. This procedure is intended to house the core data transformation and business logic originally present in the `k_ausd_bp_ta_bpr_basis.ksh` kernel script. It is designed to be called by the wrapper procedure and accepts parameters for job identification, processing date, entry number, and restart value. **Note:** This is currently a placeholder and requires the actual migration of the kernel script's logic.

*   **`src/bigquery/procedures/ausd_bp_ta_bpr_basis_wrapper.sql`**
    *   **Role:** This SQL script defines the main BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_basis_wrapper`. It is the direct replacement for `r_ausd_bp_ta_bpr_basis.ksh`. Its responsibilities include:
        *   Parsing and validating input parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`).
        *   Applying default values for parameters if not provided.
        *   Generating a unique job identifier (`v_job_kennung`) and a sequential job entry number (`v_dw_eintrags_nr`).
        *   Logging job start, progress, and completion/failure events to the `job_log` table.
        *   Calling the `project.dataset.k_ausd_bp_ta_bpr_basis` (kernel) stored procedure with the prepared parameters.
        *   Implementing robust error handling, capturing error messages and stack traces, and logging them.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The decision to use BigQuery Stored Procedures for the orchestration logic directly leverages BigQuery's native capabilities, reducing the need for external compute (e.g., Cloud Functions, Dataflow) for simple procedural flows. This keeps the logic close to the data.
    *   **Trade-off:** While powerful for SQL-centric logic, BigQuery Stored Procedures are less flexible for complex file system interactions, external API calls, or highly dynamic shell-like operations. These would require a different approach (e.g., Cloud Composer). For this specific script, which primarily orchestrates other scripts and manages parameters/logging, BQSP is a good fit.
*   **Dedicated BigQuery Logging Table:** Instead of replicating the original shell script's log file output and framework-specific logging functions, a structured `job_log` table in BigQuery was chosen.
    *   **Trade-off:** This provides structured, queryable logs, which is a significant improvement for monitoring and auditing. However, it means losing byte-for-byte fidelity with the original log file format, which might be a concern if external systems parsed the old log files directly. The `DWMSG_*` framework functions are replaced by direct `INSERT` statements into this table.
*   **Separation of Wrapper and Kernel Logic:** The original design of `r_ausd_bp_ta_bpr_basis.ksh` calling `k_ausd_bp_ta_bpr_basis.ksh` was preserved by creating two distinct BigQuery Stored Procedures: a wrapper and a kernel placeholder.
    *   **Trade-off:** This maintains modularity and aligns with the original architecture. However, it introduces a critical dependency: the `k_ausd_bp_ta_bpr_basis` procedure must be fully migrated and functional for the wrapper to work correctly. The current `k_ausd_bp_ta_bpr_basis.sql` is a placeholder.
*   **`DW_EintragsNr` Implementation:** The `DW_EintragsNr` (job entry number) is derived using `SELECT IFNULL(MAX(kernel_job_entry_nr), 0) + 1 FROM project.dataset.job_log;`.
    *   **Trade-off:** This is a simple and effective way to generate sequential numbers within BigQuery. However, it is susceptible to race conditions if multiple instances of the wrapper procedure are started concurrently, potentially leading to duplicate `DW_EintragsNr` values. A more robust solution would involve a dedicated sequence table with atomic updates or an external sequence generator. This was flagged as a known gap.
*   **Error Handling with `EXCEPTION WHEN ERROR`:** BigQuery's procedural language `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block is used to capture and log errors, replacing the shell script's `trap` commands and `DWMSG_Fehlerbehandlung`.
    *   **Trade-off:** This provides structured error handling within the SQL context. The `SIGNAL SQLSTATE '45000'` mechanism ensures that errors are propagated to the caller, mimicking the shell script's exit code behavior.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation:** Ensure the BigQuery dataset `project.dataset` exists in your Google Cloud project. If not, create it:
    ```bash
    bq mk --project_id=<YOUR_PROJECT_ID> <YOUR_DATASET_ID>
    ```
2.  **IAM Permissions:**
    *   The service account or user executing these DDL/DML statements and procedures must have the necessary IAM roles:
        *   `BigQuery Data Editor` (or equivalent granular permissions) on `project.dataset` to create tables and procedures, and insert into `job_log`.
        *   `BigQuery Job User` to run queries and procedures.
    *   If Cloud Composer is used for orchestration, the Composer service account will need these permissions.
3.  **Deploy `job_log` table:** Execute the `src/bigquery/ddl/job_log.sql` script to create the logging table.
    ```bash
    bq query --use_legacy_sql=false --file=src/bigquery/ddl/job_log.sql
    ```
4.  **Migrate and Deploy Kernel Procedure:** The `src/bigquery/procedures/k_ausd_bp_ta_bpr_basis.sql` is currently a placeholder. The actual business logic from `k_ausd_bp_ta_bpr_basis.ksh` must be fully migrated into this BigQuery Stored Procedure. Once migrated, deploy it:
    ```bash
    bq query --use_legacy_sql=false --file=src/bigquery/procedures/k_ausd_bp_ta_bpr_basis.sql
    ```
5.  **Deploy Wrapper Procedure:** Execute the `src/bigquery/procedures/ausd_bp_ta_bpr_basis_wrapper.sql` script to create the main wrapper procedure.
    ```bash
    bq query --use_legacy_sql=false --file=src/bigquery/procedures/ausd_bp_ta_bpr_basis_wrapper.sql
    ```
6.  **Scheduling:**
    *   **BigQuery Scheduled Query:** If simple scheduling is sufficient, create a BigQuery Scheduled Query to call `project.dataset.ausd_bp_ta_bpr_basis_wrapper` with the desired frequency and parameters.
        *   Example: `CALL project.dataset.ausd_bp_ta_bpr_basis_wrapper(NULL, NULL);` (for default behavior) or `CALL project.dataset.ausd_bp_ta_bpr_basis_wrapper('01012023', 0);`
    *   **Cloud Composer (Airflow):** For more complex workflows, dependencies, or external integrations, create a Cloud Composer DAG that invokes the BigQuery Stored Procedure. This would involve writing a Python DAG file using the `BigQueryOperator`.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and implementation as known gaps, risks, or areas requiring further attention:

*   **Missing Analysis Data:** The initial analysis lacked `file_purpose`, `complexity_signals`, `tier`, and `migration_flags`. While the `semi_auto` (B2) bucket was assigned, this gap means there might be unforeseen complexities not fully captured.
*   **Kernel Script Migration (B4 Item):** The `project.dataset.k_ausd_bp_ta_bpr_basis` procedure is a placeholder. The core business logic from `k_ausd_bp_ta_bpr_basis.ksh` must be separately and fully migrated. This is a critical dependency, and its complexity (e.g., file system operations, external calls) will dictate the final implementation approach for the kernel.
*   **Framework-Specific Functions:** The original script relied on custom framework functions like `DWMSG_*` for logging and `DWDate_Gib_Zeitraum` for date handling.
    *   `DWDate_Gib_Zeitraum` has been replaced by native BigQuery date functions (`FORMAT_DATE`, `CURRENT_DATE`).
    *   `DWMSG_*` functions have been replaced by direct `INSERT` statements into the `job_log` table. The exact semantics (e.g., specific log levels, message formats) of the original framework were inferred and replicated to the best extent possible.
*   **Exact Log File Semantics:** The original script wrote to a dynamically named log file. The BigQuery `job_log` table provides structured logging. If byte-for-byte fidelity of the original log file content or format is strictly required by downstream systems, this migration might introduce a breaking change. Cloud Logging or GCS integration might be considered for raw log storage if needed.
*   **Global Job Numbering (`DW_EintragsNr`) Race Condition:** The current implementation of `v_dw_eintrags_nr` using `SELECT IFNULL(MAX(kernel_job_entry_nr), 0) + 1 FROM project.dataset.job_log;` is prone to race conditions if multiple instances of the wrapper procedure start simultaneously. This could lead to non-unique or non-sequential `DW_EintragsNr` values. A more robust solution, such as a dedicated sequence table with atomic updates or an external sequence generator, should be considered for production environments if strict uniqueness and sequentiality are required.

## 6. Validation

To validate the migrated `ausd_bp_ta_bpr_basis_wrapper` procedure, perform the following steps:

1.  **Prerequisites:** Ensure all manual steps (dataset, IAM, `job_log` table, and both procedures) have been completed.
2.  **Test Cases:**

    *   **Successful Execution (Default Parameters):**
        ```sql
        CALL project.dataset.ausd_bp_ta_bpr_basis_wrapper(NULL, NULL);
        ```
        *   **Expected Outcome:** The procedure should complete without error. A new entry should appear in `project.dataset.job_log` with `status = 'COMPLETED'`, `job_name = 'ausd_bp_ta_bpr_basis_wrapper'`, and `processing_date` set to today's date. The `kernel_job_entry_nr` should be incremented.
        *   **Passing Criteria:**
            *   Procedure returns successfully.
            *   `job_log` contains `STARTED`, `RUNNING` (for kernel call), and `COMPLETED` entries for the `v_job_kennung`.
            *   `processing_date` in `job_log` matches `CURRENT_DATE()`.
            *   `restart_value` in `job_log` is `0`.

    *   **Successful Execution (Explicit Parameters):**
        ```sql
        CALL project.dataset.ausd_bp_ta_bpr_basis_wrapper('01012023', 1);
        ```
        *   **Expected Outcome:** Similar to the default case, but `processing_date` should be '2023-01-01' and `restart_value` should be `1` in the `job_log`.
        *   **Passing Criteria:**
            *   Procedure returns successfully.
            *   `job_log` contains `STARTED`, `RUNNING`, and `COMPLETED` entries for the `v_job_kennung`.
            *   `processing_date` in `job_log` is `2023-01-01`.
            *   `restart_value` in `job_log` is `1`.

    *   **Parameter Validation Failure (Missing Stichtag):**
        ```sql
        CALL project.dataset.ausd_bp_ta_bpr_basis_wrapper('', 0); -- Empty string for Stichtag
        ```
        *   **Expected Outcome:** The procedure should terminate with an error. An entry should appear in `project.dataset.job_log` with `status = 'FAILED_PARAM_VALIDATION'` and an error message indicating a missing or invalid `Stichtag`.
        *   **Passing Criteria:**
            *   Procedure fails with `SQLSTATE '45000'` and `MESSAGE_TEXT` indicating parameter error.
            *   `job_log` contains an `ERROR` entry with `status = 'FAILED_PARAM_VALIDATION'` and `error_details` matching the expected message.

    *   **Kernel Procedure Failure (Simulated):**
        *   **Pre-requisite:** Temporarily uncomment the `SIGNAL SQLSTATE` line within `src/bigquery/procedures/k_ausd_bp_ta_bpr_basis.sql` (e.g., `IF p_stichtag = '01012023' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in kernel for specific date!'; END IF;`) and re-deploy the kernel procedure.
        ```sql
        CALL project.dataset.ausd_bp_ta_bpr_basis_wrapper('01012023', NULL);
        ```
        *   **Expected Outcome:** The wrapper procedure should catch the error from the kernel, log it, and then re-raise it. An entry should appear in `project.dataset.job_log` with `status = 'FAILED'` and `error_details` containing the kernel's error message.
        *   **Passing Criteria:**
            *   Wrapper procedure fails with `SQLSTATE '45000'` and `MESSAGE_TEXT` indicating a job failure.
            *   `job_log` contains `STARTED`, `RUNNING` (for kernel call), and `ERROR` entries for the `v_job_kennung`, with `status = 'FAILED'` and `error_details` reflecting the kernel's simulated error.

3.  **Review `job_log` Table:** After each test, query the `project.dataset.job_log` table to verify the entries:
    ```sql
    SELECT * FROM project.dataset.job_log ORDER BY entry_timestamp DESC LIMIT 10;
    ```

## 7. Rollback procedure

In case of issues or if the migration needs to be reverted, follow these steps:

1.  **Stop Scheduling:** Immediately stop or delete any BigQuery Scheduled Queries or Cloud Composer DAGs that invoke the `project.dataset.ausd_bp_ta_bpr_basis_wrapper` procedure.
2.  **Delete BigQuery Procedures:** Delete the migrated BigQuery Stored Procedures.
    ```sql
    DROP PROCEDURE IF EXISTS project.dataset.ausd_bp_ta_bpr_basis_wrapper;
    DROP PROCEDURE IF EXISTS project.dataset.k_ausd_bp_ta_bpr_basis;
    ```
3.  **Delete Logging Table (Optional):** If the `job_log` table is not used by other processes, it can be deleted. Be cautious as this will remove all historical log data.
    ```sql
    DROP TABLE IF EXISTS project.dataset.job_log;
    ```
4.  **Revert to Original Script:** Ensure the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh` and its dependencies are in place and configured to run as they did prior to the migration.
5.  **Verify Original Script Functionality:** Run the original KornShell script to confirm it executes correctly and produces the expected output and logs.