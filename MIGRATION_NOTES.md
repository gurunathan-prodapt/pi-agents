# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh` and its orchestrated SQL component `d_ausd_bp_ta_bpr_basis.sql`.

The original KornShell script, responsible for orchestrating data preparation, parameter handling, date validation, and executing core SQL logic, has been migrated to **Google BigQuery Stored Procedures**. The core data transformation logic from `d_ausd_bp_ta_bpr_basis.sql` has also been translated into a dedicated BigQuery Stored Procedure. Logging mechanisms have been replaced with BigQuery tables.

The target platform is **Google BigQuery**, leveraging its native SQL scripting capabilities for orchestration and data processing.

## 2. Generated Artifacts

The migration process generated the following BigQuery artifacts:

*   **`ddl/job_error_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_error_log` table in BigQuery. This table serves as a centralized repository for capturing and logging errors encountered during the execution of BigQuery jobs, replacing the `DWMSG_MeldeFehler` mechanism from the legacy system.
*   **`ddl/job_table.sql`**
    *   **Role:** Defines the DDL for the `job_table` in BigQuery. This table is used for comprehensive logging of job execution details, including start/end times, status, record counts, and input parameters. It replaces the functionality of the commented-out `FOSJobErzeugeEintrag` in the original KornShell script.
*   **`stored_procedures/core_d_ausd_bp_ta_bpr_basis_proc.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core data transformation logic originally found in `d_ausd_bp_ta_bpr_basis.sql`. It performs data manipulation (TRUNCATE, INSERT) on BigQuery tables, deriving basis product instances and SIM card information based on a given `stichtag`. This procedure is designed to be called by the main orchestration procedure.
*   **`stored_procedures/r_ausd_bp_ta_bpr_basis.sql`**
    *   **Role:** This is the main orchestration BigQuery Stored Procedure, directly replacing the `k_ausd_bp_ta_bpr_basis.ksh` KornShell script. It handles:
        *   Parsing and validating input parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag_str`, `p_wiederanlauf_wert`).
        *   Validating the date format of `p_stichtag_str`.
        *   Deriving current and yesterday's dates.
        *   Calling the `core_d_ausd_bp_ta_bpr_basis_proc` to execute the main data transformations.
        *   Capturing record counts from the target table.
        *   Logging job status and errors to `job_table` and `job_error_log` respectively.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration:** The KornShell script's control flow, parameter handling, and error checking have been directly translated into a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_basis`). This leverages BigQuery's native scripting capabilities, eliminating the need for external shell environments and providing a more integrated solution within the data warehouse.
*   **Separation of Core Logic:** The core SQL logic from `d_ausd_bp_ta_bpr_basis.sql` was encapsulated into a separate BigQuery Stored Procedure (`core_d_ausd_bp_ta_bpr_basis_proc`). This promotes modularity, reusability, and allows for independent testing and maintenance of the data transformation logic.
*   **BigQuery Tables for Logging:** File-based temporary files and legacy logging mechanisms (`DWMSG_MeldeFehler`, `FOSJobErzeugeEintrag`) have been replaced by dedicated BigQuery tables (`job_error_log`, `job_table`). This centralizes logging within BigQuery, enabling easier querying, monitoring, and integration with other GCP services.
*   **Dynamic Fully Qualified Domain Names (FQDNs):** Within `core_d_ausd_bp_ta_bpr_basis_proc`, table references are constructed dynamically using input parameters for project and dataset IDs. This design allows the same procedure code to be deployed across different environments (development, staging, production) without modification, enhancing flexibility and maintainability.
*   **Native BigQuery Date Functions:** The external `gestern.ksh` script and `DWDate_Datum_Check` function have been replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`), simplifying date handling and validation.
*   **Structured Error Handling:** Error conditions (e.g., missing parameters, invalid date formats) are handled using BigQuery `IF` statements, `RAISE USING MESSAGE`, and `EXCEPTION` blocks, with detailed error messages logged to `job_error_log`. This provides robust error reporting and immediate termination on critical failures.
*   **Trade-offs:**
    *   **Increased BigQuery SQL Complexity:** Translating shell script control flow into BigQuery SQL scripting can sometimes be more verbose than simple shell commands. However, this is offset by the benefits of native execution within BigQuery.
    *   **Explicit Parameterization:** While the original script sourced environment variables, the BigQuery procedures require explicit parameters for project and dataset IDs. This adds verbosity to procedure calls but provides clear dependency management and environment isolation.
    *   **Loss of Arbitrary External Commands:** The direct execution of external utilities (like `sed`, `sort`, `join` if they were active) is no longer possible. Any such functionality would need to be re-implemented using BigQuery SQL or other GCP services (e.g., Dataflow, Cloud Functions).

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in a production environment, the following manual steps are required:

1.  **Replace Placeholders:**
    *   In all generated SQL files, replace `your_gcp_project_id`, `your_logging_dataset`, `your_orchestration_dataset`, `your_source_dataset`, and `your_staging_dataset` with the actual GCP project ID and BigQuery dataset names for your environment.
2.  **Dataset Creation:**
    *   Ensure the following BigQuery datasets exist in your target GCP project:
        *   `your_logging_dataset` (e.g., `dw_logs`)
        *   `your_orchestration_dataset` (e.g., `dw_orchestration`)
        *   `your_source_dataset` (e.g., `dw_source_data`) - This dataset should contain the `rma_ta_sim` and `rma_ta_sim_card_type` tables.
        *   `your_staging_dataset` (e.g., `dw_staging`) - This dataset should contain the `sof_ta_sim`, `sof_ta_bpr_basis`, and `sof_ta_bpr_basis_his` tables.
3.  **Deploy DDL for Logging Tables:**
    *   Execute `ddl/job_error_log.sql` to create the `job_error_log` table.
    *   Execute `ddl/job_table.sql` to create the `job_table`.
4.  **Migrate and Populate Source/Staging Tables:**
    *   **Crucially, the tables referenced in `core_d_ausd_bp_ta_bpr_basis_proc` (`rma_ta_sim`, `rma_ta_sim_card_type`, `sof_ta_sim`, `sof_ta_bpr_basis`, `sof_ta_bpr_basis_his`) must exist and be populated with relevant data in `your_source_dataset` and `your_staging_dataset` respectively.** The DDL for these tables is *not* part of this migration package and must be handled separately.
5.  **Deploy Stored Procedures:**
    *   Execute `stored_procedures/core_d_ausd_bp_ta_bpr_basis_proc.sql` to create the core logic procedure.
    *   Execute `stored_procedures/r_ausd_bp_ta_bpr_basis.sql` to create the main orchestration procedure.
6.  **IAM Permissions:**
    *   The service account or user executing `r_ausd_bp_ta_bpr_basis` must have the following BigQuery permissions:
        *   `bigquery.dataEditor` on `your_logging_dataset` (for `job_error_log` and `job_table`).
        *   `bigquery.dataViewer` on `your_source_dataset` (for reading `rma_ta_sim`, `rma_ta_sim_card_type`).
        *   `bigquery.dataEditor` on `your_staging_dataset` (for `sof_ta_sim`, `sof_ta_bpr_basis`, `sof_ta_bpr_basis_his`).
        *   `bigquery.routines.call` on `your_orchestration_dataset` (to call `core_d_ausd_bp_ta_bpr_basis_proc`).
        *   `bigquery.routines.create` and `bigquery.routines.update` for deploying the stored procedures.
7.  **Scheduling:**
    *   If external scheduling is required (e.g., daily runs), configure a **Cloud Composer DAG** or **Cloud Workflows** to invoke the `r_ausd_bp_ta_bpr_basis` BigQuery Stored Procedure, passing all necessary parameters.

## 5. Known Gaps & Unresolved References

*   **Core SQL Logic (`d_ausd_bp_ta_bpr_basis.sql`) Content (B4 Item):** The provided `core_d_ausd_bp_ta_bpr_basis_proc.sql` is a *placeholder* based on the description in the design document. The actual, complete content of the original `d_ausd_bp_ta_bpr_basis.sql` was not available for direct translation. **This is the most significant B4 item.** A thorough analysis and migration of the full SQL logic is required to ensure functional equivalence.
*   **Source/Staging Table DDL and Data (B4 Item):** The DDL for the source tables (`rma_ta_sim`, `rma_ta_sim_card_type`) and staging tables (`sof_ta_sim`, `sof_ta_bpr_basis`, `sof_ta_bpr_basis_his`) is not included. These tables must be created and populated as part of a broader data migration effort.
*   **Commented-Out Functionality:** The original KornShell script contained commented-out `sed`, `sort`, and `join` operations. It is assumed these are not currently required. If they become active requirements, they would need to be re-evaluated and migrated to BigQuery SQL transformations (e.g., `REPLACE`, `ORDER BY`, `GROUP BY`, `JOIN`, `MERGE`).
*   **Full `starteSQLSkript` Behavior:** The exact nuances of the `starteSQLSkript` function in the original KornShell script (e.g., specific error handling, parameter binding mechanisms beyond simple substitution) might not be fully replicated. The current migration assumes direct BigQuery SQL execution with explicit parameter passing.
*   **Job Framework (`FOSJobDeaktivate`, `h_alis_job.ksh`):** While basic job logging is implemented, a full replication of a complex job control framework (if `FOSJobDeaktivate` and `h_alis_job.ksh` imply one) is not included. If more advanced job control features (e.g., dependency management, restartability beyond simple parameter passing) are needed, this would be a B4 item requiring further design.

## 6. Validation

To validate the successful migration and functionality of the BigQuery procedures:

1.  **Unit Testing of `r_ausd_bp_ta_bpr_basis`:**
    *   **Successful Run:** Call the procedure with valid parameters (e.g., `CALL your_gcp_project_id.your_orchestration_dataset.r_ausd_bp_ta_bpr_basis('JOB123', 'ENTRY456', '20231026', '0', 'your_gcp_project_id', 'your_source_dataset', 'your_gcp_project_id', 'your_staging_dataset', 'your_gcp_project_id', 'your_logging_dataset');`).
        *   **Passing Means:** The procedure completes without error. The `job_table` contains a new entry for `v_job_name` with `status_a = 'SUCCESS'` and `status_i = 'END'`. The `record_count` should reflect the number of rows inserted into `sof_ta_bpr_basis`. The `job_error_log` table should remain empty.
    *   **Invalid `p_stichtag_str`:** Call with an invalid date format (e.g., `'2023-10-26'`, `'ABC'`).
        *   **Passing Means:** The procedure raises an error message indicating invalid date format. The `job_error_log` contains an entry with the specific error. The `job_table` contains an entry with `status_a = 'FAILED'` and `status_i = 'ERROR'`.
    *   **Missing Mandatory Parameters:** Call with `NULL` or empty strings for `p_job_kennung`, `p_eintrags_nr`, or `p_stichtag_str`.
        *   **Passing Means:** The procedure raises an error message for the missing parameter. The `job_error_log` contains an entry with the specific error. The `job_table` contains an entry with `status_a = 'FAILED'` and `status_i = 'ERROR'`.
2.  **Data Validation:**
    *   After a successful run, query the target table `your_gcp_project_id.your_staging_dataset.sof_ta_bpr_basis`.
    *   **Passing Means:** The data in `sof_ta_bpr_basis` for the given `p_stichtag_str` matches the expected output from the legacy `k_ausd_bp_ta_bpr_basis.ksh` script when run with the same `stichtag`. This requires a comparison of row counts and a sample of data content.
3.  **Performance Testing:**
    *   Compare the execution time of the BigQuery stored procedure with the historical execution time of the legacy KornShell script for similar data volumes.
    *   **Passing Means:** The BigQuery procedure executes within acceptable performance thresholds, ideally faster or at least comparable to the legacy system.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following steps can be taken to roll back to the legacy system:

1.  **Halt New Executions:**
    *   If using Cloud Composer or Cloud Workflows, disable or pause the DAG/workflow that invokes the BigQuery stored procedure.
    *   Ensure no manual executions of the BigQuery stored procedure are initiated.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh` script in its original scheduling system.
3.  **Clean Up BigQuery Target Data (Optional but Recommended):**
    *   If the BigQuery procedure partially processed data, consider truncating or deleting data from the target tables (`your_gcp_project_id.your_staging_dataset.sof_ta_sim`, `your_gcp_project_id.your_staging_dataset.sof_ta_bpr_basis`) for the affected `stichtag` to prevent data inconsistencies when the legacy job runs.
4.  **Delete BigQuery Stored Procedures (Optional):**
    *   If the rollback is permanent or for a significant period, consider dropping the deployed BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_orchestration_dataset.r_ausd_bp_ta_bpr_basis`;
        DROP PROCEDURE IF EXISTS `your_gcp_project_id.your_orchestration_dataset.core_d_ausd_bp_ta_bpr_basis_proc`;
        ```
5.  **Retain Logging Tables:**
    *   The `job_table` and `job_error_log` tables can typically be retained as they contain valuable historical execution and error information, even for failed migration attempts.