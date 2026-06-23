# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`. This script, which served as an orchestration layer for executing a core SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) and managing job lifecycle, has been migrated to Google Cloud's BigQuery platform.

The migration involved:
*   Reimplementing the orchestration logic (parameter handling, job activation/deactivation, error logging, audit) as a BigQuery Stored Procedure named `project.dataset.r_ausd_vertrag_control`.
*   Creating placeholder BigQuery tables (`project.dataset.job_table`, `project.dataset.job_error_log`, `project.dataset.job_run_audit`) to replace the legacy job control and logging mechanisms.
*   Defining a placeholder BigQuery table schema for the target data (`project.dataset.ta_cntrct_crs2`).
*   Creating a placeholder BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_cntrct_crs2_sp`) to house the core data transformation logic, which will be derived from the original `d_ausd_v_ta_cntrct_crs2.sql` script.

The target platform is BigQuery, leveraging its native SQL capabilities for data processing and orchestration.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`project.dataset.ta_cntrct_crs2.sql`**
    *   **Role**: Defines the schema for the target table `ta_cntrct_crs2` in BigQuery. This table will store the processed contract data. The current schema is a placeholder and requires refinement based on the detailed analysis of `d_ausd_v_ta_cntrct_crs2.sql`.
*   **`project.dataset.job_table.sql`**
    *   **Role**: Defines the schema for the `job_table` in BigQuery. This table is used to manage the status (active/inactive) of ETL jobs, replacing the implicit job control mechanisms of the legacy system.
*   **`project.dataset.job_error_log.sql`**
    *   **Role**: Defines the schema for the `job_error_log` table in BigQuery. This table captures detailed error information, replacing the legacy `DWMSG_MeldeFehler` function and associated logging.
*   **`project.dataset.job_run_audit.sql`**
    *   **Role**: Defines the schema for the `job_run_audit` table in BigQuery. This table stores audit information for each job run, including start/end times, status, and the count of processed records, replacing the temporary file-based record counting and implicit auditing of the legacy script.
*   **`project.dataset.d_ausd_v_ta_cntrct_crs2_sp.sql`**
    *   **Role**: This is a placeholder BigQuery Stored Procedure. Its purpose is to encapsulate the core data transformation logic originally found in `d_ausd_v_ta_cntrct_crs2.sql`. The actual DML/DDL statements for processing `ta_cntrct_crs2` data will be implemented here.
*   **`project.dataset.r_ausd_vertrag_control.sql`**
    *   **Role**: This is the main BigQuery Stored Procedure that orchestrates the entire job. It replaces the `k_ausd_v_ta_cntrct_crs2.ksh` script's functionality, including parameter validation, job activation/deactivation, error handling, and calling the `d_ausd_v_ta_cntrct_crs2_sp` for data processing.

## 3. Key design decisions

*   **Orchestration Reimplementation**: The KornShell script's orchestration logic (parameter validation, job management, error handling) was directly translated into a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This centralizes control within BigQuery, eliminating external shell dependencies and leveraging BigQuery's native scripting capabilities.
*   **Dedicated Job Control Tables**: Instead of relying on implicit job state management or filesystem-based logging, dedicated BigQuery tables (`job_table`, `job_error_log`, `job_run_audit`) were introduced. This provides a structured, queryable, and scalable way to manage job status, log errors, and audit execution.
*   **Core Logic Encapsulation**: The data transformation logic from `d_ausd_v_ta_cntrct_crs2.sql` is to be encapsulated in its own BigQuery Stored Procedure (`d_ausd_v_ta_cntrct_crs2_sp`). This promotes modularity, reusability, and easier testing of the core business logic.
*   **Parameter Handling**: Shell script arguments (`JobKennung`, `EintragsNr`) are directly mapped to input parameters of the BigQuery Stored Procedures, simplifying invocation and type safety.
*   **Error Handling**: The legacy `DWMSG_MeldeFehler` function and shell error checks are replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks, a helper `LogError` procedure, and inserts into the `job_error_log` table. This provides robust, structured error reporting.
*   **Record Counting**: The legacy method of writing record counts to a temporary file is replaced by capturing `@@row_count` (or a similar mechanism from the core data processing SP) and storing it in the `job_run_audit` table.

**Notable Trade-offs**:
*   **Increased BigQuery Dependency**: The entire job lifecycle is now tightly coupled with BigQuery's SQL scripting capabilities. This reduces flexibility in using other processing engines but maximizes native integration and performance within BigQuery.
*   **SQL Complexity**: Orchestration logic, previously handled by shell scripting, is now expressed in BigQuery Standard SQL. This might lead to more verbose or complex SQL for control flow compared to a dedicated orchestration tool, but it keeps the entire solution within a single technology stack.
*   **Initial Schema Definition**: The initial schemas for `ta_cntrct_crs2` are placeholders. A thorough analysis of `d_ausd_v_ta_cntrct_crs2.sql` is required to finalize these schemas, which is a manual effort.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the BigQuery dataset specified as `project.dataset` (e.g., `your_gcp_project.your_dw_dataset`) exists in your target GCP project.
2.  **IAM Permissions**: Grant the service account or user that will execute these BigQuery procedures the necessary IAM roles. This typically includes:
    *   `BigQuery Data Editor` on the target dataset for `ta_cntrct_crs2`, `job_table`, `job_error_log`, `job_run_audit`.
    *   `BigQuery Job User` to run BigQuery jobs.
    *   `BigQuery Data Viewer` on any source tables that `d_ausd_v_ta_cntrct_crs2_sp` will read from.
3.  **Migrate `d_ausd_v_ta_cntrct_crs2.sql` Content**:
    *   **Crucial Step**: Obtain the full content of the original `d_ausd_v_ta_cntrct_crs2.sql` script.
    *   Thoroughly analyze its SQL dialect (likely Oracle SQL) and convert all DML/DDL statements, functions, and logic to BigQuery Standard SQL.
    *   Update the placeholder `project.dataset.d_ausd_v_ta_cntrct_crs2_sp.sql` with this converted logic.
    *   Refine the schema of `project.dataset.ta_cntrct_crs2.sql` based on the actual data transformations and target structure defined in the migrated `d_ausd_v_ta_cntrct_crs2_sp`.
4.  **Refine `job_table` Business Rules**: Review the legacy system's exact business rules for `JobKennung` and `EintragsNr` and how job activation/deactivation is managed. Ensure the `job_table` schema and the logic within `r_ausd_vertrag_control` accurately reflect these rules.
5.  **Initial Data Load (Optional)**: If the `job_table` or `job_error_log` needs to be pre-populated with historical data or initial configurations, perform an initial data load.
6.  **Scheduling Integration**: Integrate the `project.dataset.r_ausd_vertrag_control` stored procedure into your chosen BigQuery scheduler (e.g., Cloud Composer/Airflow, Cloud Scheduler, or a custom orchestrator). Configure the scheduler to pass the required `p_job_kennung` and `p_eintrags_nr` parameters.

## 5. Known gaps & unresolved references

*   **`d_ausd_v_ta_cntrct_crs2.sql` Content**: The most significant gap. The actual SQL code for data transformation is *not* included in the provided design and therefore *not* migrated. The `project.dataset.d_ausd_v_ta_cntrct_crs2_sp` is a placeholder and currently performs no actual data processing. This requires a dedicated follow-up analysis and migration effort.
*   **`ta_cntrct_crs2` Schema Detail**: The schema for `project.dataset.ta_cntrct_crs2` is generic (`contract_data JSON`). This needs to be fully defined based on the detailed analysis of `d_ausd_v_ta_cntrct_crs2.sql`.
*   **Legacy Utility Script Logic**: The internal logic of the sourced KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) was not provided. While their high-level functionalities (error handling, parameter parsing, date ops, SQL*Plus interaction) have been re-implemented in BigQuery, any subtle or complex logic within them might require further investigation if issues arise.
*   **`starteSQLSkript` Details**: The exact implementation of `starteSQLSkript` (e.g., how it handles `SQL*Plus` parameters, error codes, and output parsing) is not fully known. The BigQuery equivalent assumes a direct `CALL` to the data processing SP and relies on BigQuery's native error handling and `@@row_count` for record counts.
*   **`d_ausd_v_ta_cntrct_crs2_sp` Return Value for `records_processed`**: The current `r_ausd_vertrag_control` procedure attempts to capture `v_processed_records` by calling `d_ausd_v_ta_cntrct_crs2_sp` and selecting from its result. This is a simplification. Once `d_ausd_v_ta_cntrct_crs2_sp` is fully implemented, the mechanism for returning the processed record count (e.g., an `OUT` parameter, a temporary table, or a final `SELECT` statement) needs to be finalized and the calling `r_ausd_vertrag_control` adjusted accordingly.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy Artifacts**: Ensure all generated BigQuery SQL files (tables and stored procedures) are deployed to the target BigQuery dataset.
2.  **Execute the Orchestration Procedure**:
    *   Call the main orchestration procedure `project.dataset.r_ausd_vertrag_control` with sample parameters.
    *   Example:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('TA_CNTRCT_CRS2', '20231027103000');
        ```
    *   Test with valid and invalid parameters (e.g., `NULL` for `p_job_kennung`) to verify parameter validation and error logging.
3.  **Verify Job Status in `job_table`**:
    *   After execution, query `project.dataset.job_table` to ensure the job entry for the given `p_job_kennung` and `p_eintrags_nr` is present and its `active_flag` is `FALSE` (indicating successful completion).
    *   Verify that any older active jobs for the same `p_job_kennung` were correctly deactivated.
4.  **Verify Audit Log in `job_run_audit`**:
    *   Query `project.dataset.job_run_audit` to confirm a new entry exists for the executed job.
    *   Check that `status` is 'SUCCESS' and `records_processed` reflects the expected count (once `d_ausd_v_ta_cntrct_crs2_sp` is fully implemented).
5.  **Verify Error Logging in `job_error_log`**:
    *   If an error was intentionally triggered (e.g., invalid parameters, or if `d_ausd_v_ta_cntrct_crs2_sp` raises an error), query `project.dataset.job_error_log` to ensure the error details are correctly captured. For successful runs, this table should remain empty for the specific job run.
6.  **Data Validation in `ta_cntrct_crs2` (Post-`d_ausd_v_ta_cntrct_crs2_sp` Migration)**:
    *   Once `d_ausd_v_ta_cntrct_crs2_sp` is fully implemented, query `project.dataset.ta_cntrct_crs2` to verify that the data has been processed and loaded correctly, matching the expected output from the legacy system. This is the ultimate "passing" criteria for the data transformation itself.

**"Passing" means**:
*   The `CALL` to `r_ausd_vertrag_control` completes without raising an unhandled BigQuery error.
*   The `job_table` accurately reflects the activation and deactivation of the job.
*   A 'SUCCESS' entry is recorded in `job_run_audit` with the correct `records_processed` count.
*   No unexpected errors are logged in `job_error_log`.
*   (Once `d_ausd_v_ta_cntrct_crs2_sp` is complete) The `ta_cntrct_crs2` table contains the correct and complete transformed data.

## 7. Rollback procedure

In case of critical issues or if the migration needs to be reverted, follow these steps:

1.  **Stop New Executions**: Immediately halt any new scheduled executions of `project.dataset.r_ausd_vertrag_control` in your scheduler (e.g., pause Airflow DAGs, disable Cloud Scheduler jobs).
2.  **Re-enable Legacy Job**: Reactivate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh` job in the legacy environment.
3.  **Revert BigQuery Objects**:
    *   **Option A (Recommended for clean rollback)**: Drop the newly created BigQuery tables and stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_v_ta_cntrct_crs2_sp`;
        DROP TABLE IF EXISTS `project.dataset.ta_cntrct_crs2`;
        DROP TABLE IF EXISTS `project.dataset.job_table`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        DROP TABLE IF EXISTS `project.dataset.job_run_audit`;
        ```
    *   **Option B (If data in `ta_cntrct_crs2` needs to be preserved or reverted to a specific state)**: If `ta_cntrct_crs2` was populated, either revert it to a previous snapshot (if enabled) or perform a targeted `DELETE` or `TRUNCATE` if the data can be re-generated by the legacy system. For `job_table`, `job_error_log`, `job_run_audit`, a `TRUNCATE` or `DELETE` of recent entries might be sufficient if the tables themselves are to be kept.
4.  **Monitor Legacy System**: Ensure the legacy job is running as expected and processing data correctly.
5.  **Post-Rollback Analysis**: Investigate the root cause of the rollback to address the issues before attempting re-migration.