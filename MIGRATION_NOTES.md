# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh` and its associated core logic (implied from `k_ausd_bp_ta_bpr_evn.ksh`) to Google BigQuery.

The original script served as an orchestration wrapper for the initial provisioning of selected basic products for the BERT system. It handled parameter parsing (for `Stichtag` and `Wiederanlaufwert`), established error handling, and invoked a core script to generate a snapshot of the Data Warehouse (DWH) contract cache for demand scoring (FOS-Tabelle).

The migration targets Google BigQuery, leveraging BigQuery Stored Procedures for both orchestration and core data transformation logic, and BigQuery Tables for data storage and auditing. An optional Apache Airflow DAG is provided as an example for external scheduling and triggering of the BigQuery stored procedures.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`bq_ddl/job_audit_table.sql`**
    *   **Role:** This SQL DDL script defines the schema for the `job_audit` table in BigQuery. This table is central to the new logging and auditing framework, capturing job execution details, parameters, status, and error information, replacing the file-based logging of the original KornShell script.
*   **`bq_sp/sp_k_ausd_bp_ta_bpr_evn.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core data transformation logic. It is designed to replace the functionality of the original `k_ausd_bp_ta_bpr_evn.ksh` script. It handles conditional deletion of records from the target FOS table and inserts processed data from the source contract cache, applying filtering based on `Stichtag`, date ranges (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`), and the `Wiederanlaufwert` for restartability.
*   **`bq_sp/sp_r_ausd_bp_ta_bpr_evn.sql`**
    *   **Role:** This BigQuery Stored Procedure serves as the main orchestration layer, directly replacing the `r_ausd_bp_ta_bpr_evn.ksh` wrapper script. It is responsible for:
        *   Receiving and validating input parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`).
        *   Applying default values for missing parameters.
        *   Logging job initiation, parameters, and status updates to the `job_audit` table.
        *   Invoking the `sp_k_ausd_bp_ta_bpr_evn` stored procedure to perform the core data transformations.
        *   Implementing robust error handling using BigQuery's `EXCEPTION WHEN ERROR` blocks, logging any failures to the `job_audit` table.
*   **`airflow_dags/dag_r_ausd_bp_ta_bpr_evn.py`**
    *   **Role:** This is an example Apache Airflow DAG. It demonstrates how to externally schedule and trigger the `sp_r_ausd_bp_ta_bpr_evn` BigQuery Stored Procedure. It shows how to pass dynamic parameters, such as the execution date for `Stichtag`, to the BigQuery procedure. This file is optional and depends on the chosen external orchestration tool.
*   **`docs/r_ausd_bp_ta_bpr_evn_migration_summary.md`**
    *   **Role:** A summary document detailing the purpose of the original job, the target architecture components, and highlights of the transformation logic.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration and Core Logic:** The decision to migrate both the wrapper (`r_ausd_bp_ta_bpr_evn.ksh`) and the core logic (`k_ausd_bp_ta_bpr_evn.ksh`) into BigQuery Stored Procedures (`sp_r_ausd_bp_ta_bpr_evn` and `sp_k_ausd_bp_ta_bpr_evn` respectively) was made to:
    *   **Consolidate Logic:** Keep all processing logic within the BigQuery environment, leveraging its native capabilities for data manipulation and execution.
    *   **Simplify Deployment:** Reduce the need for external shell environments and dependencies.
    *   **Improve Performance:** Utilize BigQuery's optimized SQL engine for data transformations.
*   **Dedicated `job_audit` Table:** A new `job_audit` table was introduced to centralize all job execution logging. This replaces the disparate file-based logging of the original KSH script, providing a structured, queryable, and persistent record of job runs, parameters, status, and errors. This enhances observability and troubleshooting.
*   **Separation of Concerns (Orchestration vs. Core Logic):** The migration maintains a clear separation between the orchestration logic (parameter handling, validation, logging, error trapping) and the core data transformation logic. This is achieved by creating two distinct stored procedures (`sp_r_ausd_bp_ta_bpr_evn` for orchestration and `sp_k_ausd_bp_ta_bpr_evn` for core logic), promoting modularity and reusability.
*   **Translation of Parameter Handling and Defaults:** The `getopts` and conditional defaulting logic from the KSH script are directly translated into BigQuery Stored Procedure parameters and `IF...THEN...ELSE` blocks, ensuring equivalent behavior for `Stichtag` and `Wiederanlaufwert`.
*   **Robust Error Handling:** The intricate shell `trap` mechanisms for error handling are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR` blocks. This provides structured error capture, allowing for detailed error logging to the `job_audit` table and controlled re-raising of errors for external orchestrators.
*   **Preservation of Restartability:** The `Wiederanlaufwert` logic, crucial for restartable and idempotent job executions, is fully preserved. The `sp_k_ausd_bp_ta_bpr_evn` procedure includes conditional `DELETE` and `INSERT` statements that respect this parameter, ensuring correct data handling upon re-runs.
*   **Use of `project.dataset` Placeholders:** All generated BigQuery SQL uses `project.dataset` as placeholders. This design decision allows for flexibility in deployment across different GCP projects and BigQuery datasets, requiring replacement with actual values during deployment.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it in your GCP project.
2.  **`job_audit` Table Deployment:**
    *   Execute the `bq_ddl/job_audit_table.sql` script in your BigQuery environment to create the `job_audit` table.
3.  **Source and Target Table Availability:**
    *   Verify that the source tables (e.g., `project.dataset.source_contract_cache`) and the target FOS table (`project.dataset.fos_target_table`) are created and accessible in BigQuery. These tables must be populated with data, potentially via separate data ingestion pipelines.
4.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `bq_sp/sp_k_ausd_bp_ta_bpr_evn.sql` and `bq_sp/sp_r_ausd_bp_ta_bpr_evn.sql` scripts in BigQuery to create or replace the stored procedures.
5.  **IAM / Permissions Configuration:**
    *   **BigQuery Data Editor/Viewer:** Ensure the service account or user executing the stored procedures has appropriate BigQuery IAM roles (e.g., `BigQuery Data Editor` for the target dataset, `BigQuery Data Viewer` for source datasets, and `BigQuery Job User` to run jobs).
    *   **Airflow (if used):** If using the provided Airflow DAG, ensure the Airflow service account has permissions to execute BigQuery stored procedures and access the relevant datasets.
6.  **Placeholder Replacement:**
    *   **Crucial:** Replace all instances of `project.dataset` with your actual GCP project ID and BigQuery dataset ID in all generated SQL and Airflow DAG files.
7.  **Scheduling Configuration:**
    *   **Airflow (if used):** Deploy the `airflow_dags/dag_r_ausd_bp_ta_bpr_evn.py` DAG to your Airflow environment. Configure the `schedule_interval` as required and ensure the `gcp_conn_id` is correctly set up.
    *   **Other Schedulers:** If not using Airflow, configure your chosen scheduler (e.g., Cloud Scheduler, Cloud Workflows) to invoke the `sp_r_ausd_bp_ta_bpr_evn` stored procedure, passing the `p_stichtag_in` and `p_wiederanlaufWert_in` parameters as needed.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas where further analysis/decisions are required:

*   **Core Script Logic (`k_ausd_bp_ta_bpr_evn.ksh`) Detail:** The `sp_k_ausd_bp_ta_bpr_evn.sql` procedure is a framework based on the summary of the original core script. The `SELECT *` statement is a placeholder. A detailed analysis of the original `k_ausd_bp_ta_bpr_evn.ksh` is **critical** to:
    *   Identify the exact column list for insertion into `fos_target_table`.
    *   Uncover any complex transformations, aggregations, or joins that might be embedded within the original core script's logic.
    *   Determine the precise schema of `source_contract_cache` and `fos_target_table`.
*   **Target Table Deletion Scope:** The `DELETE` statement in `sp_k_ausd_bp_ta_bpr_evn` currently deletes based solely on `dwh_vertrag_id >= p_wiederanlaufWert`. If `fos_target_table` contains data for multiple `Stichtag` values, this deletion might be too broad and could lead to unintended data loss for other `Stichtag`s. It is recommended to refine the `DELETE` logic to include `Stichtag` in the `WHERE` clause if the target table is partitioned or contains `Stichtag` as a column.
*   **Dynamic `MAX(ladedatum)` (B4 Item):** The original KSH script contained commented-out logic (`FOSHoleLadedatum`) to dynamically determine `Stichtag` from `MAX(ladedatum)` in a source table. If this functionality needs to be reactivated, it requires an additional BigQuery SQL query to fetch this value and integrate it into the `sp_r_ausd_bp_ta_bpr_evn` procedure's parameter defaulting logic. This is currently not implemented.
*   **Missing Metadata:** The `complexity_tier` and `automation_bucket` for the original script were unavailable. This information could be useful for future effort estimations or migration prioritization.

## 6. Validation

To ensure the migrated job functions correctly, comprehensive validation is required:

1.  **Execute BigQuery Stored Procedures:**
    *   Manually call `CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('DDMMYYYY', 0);` with various `Stichtag` values and `Wiederanlaufwert` (0 and >0).
    *   Test edge cases:
        *   `p_stichtag_in` NULL or empty (should default to current date).
        *   `p_stichtag_in` invalid format (should raise error 193).
        *   `p_wiederanlaufWert_in` NULL (should default to 0).
        *   `p_wiederanlaufWert_in` > 0 (verify conditional delete and insert).
2.  **Airflow DAG Execution (if used):**
    *   Trigger the `r_ausd_bp_ta_bpr_evn_dag` in Airflow. Monitor task logs for successful completion.
3.  **`job_audit` Table Verification:**
    *   After each execution, query the `project.dataset.job_audit` table.
    *   Verify that entries are created with correct `job_id`, `job_name`, `stichtag_param`, `restart_value_param`, `stichtag_processed`, `restart_value_processed`.
    *   Confirm `status` is 'SUCCESS' for successful runs and 'FAILED' with appropriate `error_code` and `message` for failed runs.
4.  **Target Data Verification:**
    *   Query `project.dataset.fos_target_table` after successful runs.
    *   Compare the data with the expected output from the legacy system for the same `Stichtag` and `Wiederanlaufwert`.
    *   Specifically check:
        *   Correct filtering based on `gueltig_von`, `gueltig_bis`, `ladedatum`.
        *   Correct application of `dwh_vertrag_id > p_wiederanlaufWert` filter.
        *   Correct conditional deletion behavior when `p_wiederanlaufWert` > 0.
5.  **Error Scenario Testing:**
    *   Introduce artificial errors (e.g., invalid table names in `sp_k_ausd_bp_ta_bpr_evn`) to ensure `EXCEPTION WHEN ERROR` blocks correctly catch, log, and re-raise errors.
    *   Verify `job_audit` records the failure details.

**"Passing" Criteria:**
*   All test cases execute without unhandled errors.
*   The `job_audit` table accurately reflects the status and parameters of each job run.
*   The data in `project.dataset.fos_target_table` precisely matches the expected output from the legacy system for a given `Stichtag` and `Wiederanlaufwert`.
*   All BigQuery job logs and Airflow task logs (if applicable) indicate successful completion for successful runs and provide clear error messages for failed runs.

## 7. Rollback Procedure

In the event of critical issues post-go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions:**
    *   If using Airflow, pause or disable the `r_ausd_bp_ta_bpr_evn_dag`.
    *   If using another scheduler, disable the scheduled trigger for `sp_r_ausd_bp_ta_bpr_evn`.
2.  **Revert to Legacy System:**
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh` script in the legacy environment.
3.  **BigQuery Data Reversion (if necessary):**
    *   If the `fos_target_table` was modified incorrectly by the migrated job, revert its state. BigQuery's time travel feature can be used to restore the table to a point before the problematic execution (e.g., `SELECT * FROM `project.dataset.fos_target_table` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`). Alternatively, restore from a backup if available.
4.  **Cleanup BigQuery Artifacts (Optional):**
    *   If the rollback is permanent, consider dropping the deployed BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_r_ausd_bp_ta_bpr_evn`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_bp_ta_bpr_evn`;
        ```
    *   The `job_audit` table can be retained for historical logging or dropped if no longer needed.
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_audit`;
        ```