# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh`. The original script was responsible for preparing and initiating the initial provisioning of selected base products for the BERT system, generating a snapshot extraction of contract cache data from the Data Warehouse (DWH), and making it available to the Forderungsscoring (FOS) system. It handled parameter parsing, date determination, environment setup, error handling, logging, and delegated core data processing to an external "kernel script."

The job has been migrated to Google Cloud Platform, specifically:
*   **BigQuery Stored Procedure:** The orchestration logic, parameter handling, logging, and core data processing (from the kernel script) have been consolidated into a BigQuery Stored Procedure.
*   **BigQuery Tables:** Dedicated tables for job logging, error logging, source contract cache, and the target FOS data.
*   **Cloud Composer (Airflow):** For scheduling and orchestrating the execution of the BigQuery Stored Procedure.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`ddl/job_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `project.dataset.job_log` table in BigQuery. This table is used to record the start, progress, and completion status of job executions, including parameters and general messages. It replaces the file-based logging mechanisms of the original KornShell script.

*   **`ddl/job_error_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_error_log` table in BigQuery. This table captures detailed error information, including error codes, arguments, timestamps, and messages, providing a structured way to track and analyze job failures. It replaces the error handling and reporting functions of the original script.

*   **`ddl/source_contract_cache_placeholder.sql`**
    *   **Role:** Provides a placeholder DDL for the `project.dataset.source_contract_cache` table. This table represents the BigQuery equivalent of the `DWH$TA_C_VERTRAG` source table from the legacy DWH. **Note:** This is a placeholder; its exact schema needs to be finalized based on the detailed analysis of the original kernel script (`k_ausd_bp_ta_bpr_instance.ksh`).

*   **`ddl/target_table_placeholder.sql`**
    *   **Role:** Provides a placeholder DDL for the `project.dataset.target_table`. This table is the final destination for the processed data, corresponding to the FOS-Tabelle in the legacy system. **Note:** This is a placeholder; its exact schema needs to be finalized based on the detailed analysis of the original kernel script.

*   **`stored_procedures/ausd_bp_ta_bpr_instance.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance`. This procedure encapsulates the entire migrated logic, including:
        *   Parameter parsing and validation (`p_stichtag`, `p_wiederanlaufWert`).
        *   Date determination.
        *   Structured logging to `job_log` and `job_error_log`.
        *   Restart logic (conditional `DELETE` based on `p_wiederanlaufWert`).
        *   The core data extraction, transformation, and loading logic (via a `MERGE INTO` statement) which was previously handled by the external "kernel script."

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedure:** The primary orchestration logic of `r_ausd_bp_ta_bpr_instance.ksh` and the core data processing logic (assumed to be from `k_ausd_bp_ta_bpr_instance.ksh`) are combined into a single BigQuery Stored Procedure.
    *   **Why:** This centralizes the entire job execution within BigQuery, leveraging its native processing power for data manipulation. It eliminates the overhead of external script execution and data transfer between different environments, leading to improved performance and simplified management.
    *   **Trade-offs:** Requires all logic, including conditional flows and error handling, to be expressed in BigQuery SQL. Any complex non-SQL logic from the kernel script would need to be re-engineered or potentially handled by external Python components invoked by Airflow.

*   **BigQuery Tables for Logging:** File-based logging (`DWMSG_*` functions) has been replaced with structured logging to BigQuery tables (`job_log`, `job_error_log`).
    *   **Why:** Provides a centralized, queryable, and persistent log repository. This significantly enhances observability, debugging, and auditing capabilities compared to scattered log files.
    *   **Trade-offs:** Requires DDL definition and management for log tables.

*   **Cloud Composer (Airflow) for Orchestration:** The scheduling and triggering mechanism will be managed by Cloud Composer.
    *   **Why:** Standardizes job scheduling on a robust, managed, and scalable orchestration platform. Airflow provides advanced features like dependency management, retries, monitoring, and alerts, which are superior to simple cron-based scheduling.
    *   **Trade-offs:** Introduces a new technology stack (Airflow/Python) for scheduling, requiring DAG development and maintenance.

*   **Direct Parameter Mapping:** The command-line parameters (`-s` for `Stichtag`, `-l` for `Wiederanlaufwert`) are directly mapped to input parameters of the BigQuery Stored Procedure (`p_stichtag`, `p_wiederanlaufWert`).
    *   **Why:** Maintains functional parity with the legacy script's invocation method, making the transition smoother for users and downstream systems.

*   **BigQuery's `MERGE INTO` for Core Data Logic:** The data transformation and loading logic, previously in the kernel script, is implemented using a `MERGE INTO` statement within the stored procedure.
    *   **Why:** `MERGE` is an atomic and efficient way to perform upserts (insert or update) in BigQuery, handling both new records and updates in a single statement. This is ideal for snapshot-based data provisioning.
    *   **Trade-offs:** Requires careful mapping of source to target columns and understanding of the exact transformation rules from the kernel script.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `project.dataset` exists in your Google Cloud project. If not, create it.

2.  **Deploy Log Tables DDL:**
    *   Execute `ddl/job_log.sql` to create the `project.dataset.job_log` table.
    *   Execute `ddl/job_error_log.sql` to create the `project.dataset.job_error_log` table.

3.  **Finalize and Deploy Source/Target Table DDLs:**
    *   **CRITICAL:** Analyze the full content of `k_ausd_bp_ta_bpr_instance.ksh` to determine the exact schema (column names, data types, primary keys) for the source contract cache table (`DWH$TA_C_VERTRAG` equivalent) and the target FOS table.
    *   Update `ddl/source_contract_cache_placeholder.sql` and `ddl/target_table_placeholder.sql` with the *actual* schemas.
    *   Execute the finalized DDLs to create `project.dataset.source_contract_cache` and `project.dataset.target_table`.

4.  **Deploy BigQuery Stored Procedure:**
    *   **CRITICAL:** Based on the kernel script analysis, complete the `UPDATE SET` and `INSERT` clauses within the `MERGE INTO` statement in `stored_procedures/ausd_bp_ta_bpr_instance.sql` with the correct column mappings and transformation logic.
    *   Execute the finalized `stored_procedures/ausd_bp_ta_bpr_instance.sql` to create or replace the stored procedure in BigQuery.

5.  **IAM / Permissions Configuration:**
    *   Ensure the service account used by Cloud Composer (or any other orchestrator) has the necessary BigQuery permissions:
        *   `bigquery.datasets.get`
        *   `bigquery.tables.create`, `bigquery.tables.update`, `bigquery.tables.delete`, `bigquery.tables.getData`, `bigquery.tables.updateData` on `project.dataset.job_log`, `project.dataset.job_error_log`, `project.dataset.target_table`.
        *   `bigquery.tables.getData` on `project.dataset.source_contract_cache`.
        *   `bigquery.routines.call` on `project.dataset.ausd_bp_ta_bpr_instance`.
        *   Consider using a custom role for fine-grained control.

6.  **Cloud Composer / Airflow DAG Deployment:**
    *   Develop and deploy an Airflow DAG that invokes the `project.dataset.ausd_bp_ta_bpr_instance` BigQuery Stored Procedure, passing `p_stichtag` and `p_wiederanlaufWert` as parameters.
    *   Configure the DAG's schedule to match the legacy job's execution frequency.
    *   Ensure the Airflow environment has a BigQuery connection configured.

7.  **Secrets Management (if applicable):**
    *   If the original kernel script or wrapper handled any sensitive information (e.g., database credentials, API keys), ensure these are securely managed in Google Secret Manager and accessed by the Airflow DAG. (Not explicitly identified in the design document, but good practice).

## 5. Known Gaps & Unresolved References

The following items are identified as gaps or require further resolution before full production readiness:

*   **Kernel Script Logic (`k_ausd_bp_ta_bpr_instance.ksh`) Analysis:** This is the most significant unresolved item. The full content and specific data manipulation logic of the kernel script are currently unknown.
    *   **Impact:** Prevents finalization of the `MERGE INTO` statement's `UPDATE SET` and `INSERT` clauses, and the precise DDL for `source_contract_cache` and `target_table`.
    *   **Action:** Obtain and thoroughly analyze `k_ausd_bp_ta_bpr_instance.ksh` to extract all data sources, transformations, and target mappings.

*   **Exact Data Schemas:** The DDLs for `project.dataset.source_contract_cache` and `project.dataset.target_table` are currently placeholders.
    *   **Impact:** The stored procedure cannot be fully functional or tested without accurate schemas.
    *   **Action:** Define and implement the correct schemas based on the kernel script analysis.

*   **Proprietary Utility Replication:** The original script relied on proprietary KornShell utilities (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) and custom functions (`DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_*`).
    *   **Impact:** While core functionalities (date handling, parameter validation, logging) have been replicated, subtle behaviors or specific error codes from these utilities might not be perfectly mirrored.
    *   **Action:** Review the exact behavior of these utilities and ensure complete functional equivalence in the BigQuery Stored Procedure, especially for edge cases and error conditions.

*   **Column Mapping in `MERGE` Statement:** The `UPDATE SET` and `INSERT` clauses within the `MERGE INTO` statement in `stored_procedures/ausd_bp_ta_bpr_instance.sql` contain placeholder columns (`example_column_1`, `example_column_2`, `target_column_1`, `target_column_2`).
    *   **Impact:** The data transformation logic is incomplete.
    *   **Action:** Populate these clauses with the actual source-to-target column mappings and any required transformations identified from the kernel script analysis.

## 6. Validation

Validation ensures the migrated job functions correctly and produces identical results to the legacy system.

**How to Run Tests:**

1.  **Data Preparation:**
    *   Load a representative sample of data into `project.dataset.source_contract_cache` that mirrors the legacy `DWH$TA_C_VERTRAG` for a specific `Stichtag` and `Wiederanlaufwert`.
    *   Optionally, pre-populate `project.dataset.target_table` to simulate existing data for `MERGE` updates.

2.  **BigQuery Stored Procedure Unit Tests:**
    *   Execute the `project.dataset.ausd_bp_ta_bpr_instance` stored procedure directly in BigQuery for various scenarios:
        *   **Full Run:** `CALL project.dataset.ausd_bp_ta_bpr_instance('DDMMYYYY', 0);` (replace DDMMYYYY with a test date).
        *   **Restart Run:** `CALL project.dataset.ausd_bp_ta_bpr_instance('DDMMYYYY', <some_DWH_VERTRAG_ID>);`
        *   **Invalid Parameters:** Test with `NULL` or malformed `p_stichtag` to verify error handling.
        *   **No Data:** Test with an empty source table.
        *   **All Updates/All Inserts:** Test scenarios where all source records are new or all are updates.

3.  **Airflow DAG Integration Tests:**
    *   Deploy the Airflow DAG to a test Cloud Composer environment.
    *   Trigger the DAG manually or allow it to run on its schedule.
    *   Verify that the DAG completes successfully and invokes the BigQuery Stored Procedure with the correct parameters.

4.  **Data Comparison:**
    *   Run the legacy `r_ausd_bp_ta_bpr_instance.ksh` job with the same input parameters (`Stichtag`, `Wiederanlaufwert`) as used in the BigQuery tests.
    *   Extract the resulting data from the legacy FOS target table.
    *   Extract the data from `project.dataset.target_table` after the BigQuery job completes.
    *   Perform a row-by-row and column-by-column comparison of the two datasets.

5.  **Log Verification:**
    *   After each test run, query `project.dataset.job_log` and `project.dataset.job_error_log` to ensure:
        *   Correct entries are recorded for job start, info, and completion.
        *   Status is accurate ('SUCCESS' or 'FAILURE').
        *   Error messages are meaningful and captured correctly for failed runs.

**What "Passing" Means:**

*   **Successful Execution:** The BigQuery Stored Procedure executes without unhandled exceptions, and the Airflow DAG completes successfully.
*   **Log Integrity:** The `project.dataset.job_log` table shows a 'SUCCESS' status for the corresponding job run, and `project.dataset.job_error_log` is empty (for successful runs).
*   **Data Equivalence:** The data in `project.dataset.target_table` is bit-for-bit identical to the data produced by the legacy `r_ausd_bp_ta_bpr_instance.ksh` (and its kernel script) for the same input parameters. This includes all columns, data types, and row counts.
*   **Performance:** The migrated job completes within acceptable performance thresholds, ideally matching or improving upon the legacy execution time.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action (Disable New Job):**
    *   Access the Cloud Composer UI (Airflow).
    *   Locate the DAG corresponding to `r_ausd_bp_ta_bpr_instance.ksh` and immediately **pause/disable** it to prevent further executions.

2.  **Re-enable Legacy Job:**
    *   If the issue impacts production data or downstream systems, immediately re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh` job on the legacy platform.
    *   Verify that the legacy job is running as expected.

3.  **Data Rollback (if necessary):**
    *   If the `project.dataset.target_table` has been corrupted or contains incorrect data due to the migrated job:
        *   **Option A (Snapshot/Backup):** Restore `project.dataset.target_table` from the most recent valid BigQuery snapshot or backup taken before the problematic run.
        *   **Option B (Re-run Legacy):** If the legacy job can safely overwrite the target data, run the legacy job to correct the data in the target system (if it's a shared target or if the legacy job can write to BigQuery).
        *   **Option C (Manual Correction):** For minor issues, perform targeted `DELETE`, `UPDATE`, or `INSERT` statements to correct the data in `project.dataset.target_table`.

4.  **Code Rollback (if necessary):**
    *   If the issue is identified as a bug in the BigQuery Stored Procedure:
        *   Revert the `stored_procedures/ausd_bp_ta_bpr_instance.sql` file in your version control system to a previously known good version.
        *   Redeploy the older version of the stored procedure to BigQuery using `CREATE OR REPLACE PROCEDURE`.

5.  **Root Cause Analysis:**
    *   Investigate the `project.dataset.job_log` and `project.dataset.job_error_log` tables for detailed error messages.
    *   Review Cloud Composer logs for the DAG execution.
    *   Analyze the BigQuery job history for the stored procedure execution.
    *   Identify the root cause of the failure or incorrect behavior.

6.  **Remediation and Re-deployment:**
    *   Implement the necessary fixes in the BigQuery Stored Procedure or Airflow DAG.
    *   Thoroughly re-test the corrected job in a staging environment.
    *   Once validated, redeploy the updated code and re-enable the Airflow DAG.