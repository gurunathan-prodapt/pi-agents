# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_bpr_beschr.ksh` from a legacy Unix/Oracle environment to Google Cloud Platform, specifically leveraging BigQuery. The original script served as an orchestration component, handling parameter validation, date processing, and the execution of an embedded SQL script (`d_ausd_bp_ta_bpr_beschr.sql`).

The migration transforms this orchestration logic into a BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_bpr_beschr`, which in turn calls another BigQuery Stored Procedure, `project.dataset.d_ausd_bp_ta_bpr_beschr`, containing the core data transformation logic. All data storage and processing now occur within BigQuery.

## 2. Generated Artifacts

The migration process has generated the following BigQuery artifacts:

*   **`project.dataset.k_ausd_bp_ta_bpr_beschr` (BigQuery Stored Procedure)**
    *   **Role:** This procedure is the direct replacement for the original `k_ausd_bp_ta_bpr_beschr.ksh` KornShell script. It handles:
        *   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Performing date format checks and deriving `v_datum_heute` and `v_datum_gestern`.
        *   Orchestrating the execution of the data processing logic by calling `project.dataset.d_ausd_bp_ta_bpr_beschr`.
        *   Capturing the record count from the target table.
        *   Logging job execution details (status, record count, dates) into the `project.dataset.job_table` audit table.
*   **`project.dataset.d_ausd_bp_ta_bpr_beschr` (BigQuery Stored Procedure)**
    *   **Role:** This procedure encapsulates the data transformation logic originally found in `d_ausd_bp_ta_bpr_beschr.sql`. It performs:
        *   Truncation of the target table `dw_target.isrpt.sof_ta_bpr_beschr`.
        *   Insertion of processed data from source tables (`dw_source.isrpt.pds_ta_bpr`, `dw_source.isrpt.pds_ta_care_description`) into the target table.
        *   Any `PACKAGE:DWPA_UTIL_SKRIPT` functionality would be reimplemented here as BigQuery SQL or UDFs.
*   **`project.dataset.job_table` (BigQuery Table)**
    *   **Role:** This table serves as an audit log for job executions, replacing any legacy job table entries or temporary file-based logging. It stores details such as `tab_name`, `status_a`, `status_i`, `stichtag_from`, `stichtag_to`, `job_type`, `restart_flag`, `record_count`, `description`, and `insert_datetime`.
*   **`dw_source.isrpt.dwtk_meldungen` (BigQuery Table)**
    *   **Role:** Migrated source table.
*   **`dw_source.isrpt.pds_ta_bpr` (BigQuery Table)**
    *   **Role:** Migrated source table.
*   **`dw_source.isrpt.pds_ta_care_description` (BigQuery Table)**
    *   **Role:** Migrated source table (inferred from the SQL logic).
*   **`dw_target.isrpt.sof_ta_bpr_beschr` (BigQuery Table)**
    *   **Role:** Migrated target table where the processed data is stored.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration and Logic:** The KornShell script's orchestration (`k_ausd_bp_ta_bpr_beschr.ksh`) and the embedded SQL logic (`d_ausd_bp_ta_bpr_beschr.sql`) are both migrated to BigQuery Stored Procedures. This centralizes execution within BigQuery, leveraging its native capabilities for parameter handling, error management, and SQL execution, eliminating the need for external shell environments or `sqlplus` calls.
*   **Parameter Handling via Procedure Arguments:** The `getopts` and manual parameter validation logic from the KornShell script is directly translated to BigQuery Stored Procedure input parameters and `IF`/`RAISE` statements, ensuring robust parameter validation at the BigQuery layer.
*   **Native BigQuery Functions for Date Operations:** Shell-based date derivations (`gestern.ksh`) are replaced by BigQuery's `CURRENT_DATE()`, `DATE_SUB()`, and `PARSE_DATE()` functions, providing efficient and reliable date manipulation.
*   **BigQuery Tables for Logging and Auditing:** Temporary files (`bert_k_ausd_bp_ta_bpr_beschr.tmp`) and potential legacy job table entries (`FOSJobErzeugeEintrag`) are replaced by dedicated BigQuery audit tables (`project.dataset.job_table`). This provides a centralized, queryable, and scalable logging solution.
*   **Direct SQL Execution:** The `starteSQLSkript` function's role of executing SQL is replaced by direct `CALL` statements to other BigQuery Stored Procedures, streamlining the execution flow.
*   **Data Storage in BigQuery:** All source and target tables are migrated to BigQuery, ensuring a unified data platform and leveraging BigQuery's performance and scalability.
*   **Reimplementation of Helper Script Functionality:** Generic helper scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) are replaced by BigQuery's native SQL functions, `ASSERT` statements, or custom BigQuery UDFs/procedures where complex logic is required. This avoids external dependencies and keeps the solution self-contained within BigQuery.
*   **Record Count Capture:** Instead of relying on temporary files, the record count is captured directly from the target table using `COUNT(*)` after the `INSERT` operation, ensuring accuracy and integrating with the BigQuery environment.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the `project.dataset` dataset (e.g., `my_project.my_dataset`) to host the BigQuery Stored Procedures and audit tables.
    *   Create the `dw_source.isrpt` dataset to host the migrated source tables.
    *   Create the `dw_target.isrpt` dataset to host the migrated target tables.
2.  **BigQuery Table Creation:**
    *   Create the schema for `dw_source.isrpt.dwtk_meldungen` based on the original Oracle table definition.
    *   Create the schema for `dw_source.isrpt.pds_ta_bpr` based on the original Oracle table definition.
    *   Create the schema for `dw_source.isrpt.pds_ta_care_description` based on the original Oracle table definition.
    *   Create the schema for `dw_target.isrpt.sof_ta_bpr_beschr` based on the original Oracle table definition.
    *   Create the `project.dataset.job_table` with the following schema (or adapt to existing audit table schema):
        ```sql
        CREATE TABLE `project.dataset.job_table` (
          tab_name STRING,
          status_a STRING,
          status_i STRING,
          stichtag_from DATE,
          stichtag_to DATE,
          job_type STRING,
          restart_flag STRING,
          record_count INT64,
          description STRING,
          insert_datetime TIMESTAMP
        );
        ```
    *   If `project.dataset.job_run_log` is also required for more granular logging, create its schema.
3.  **Historical Data Migration:**
    *   Perform a one-time historical data load from the legacy Oracle tables (`DWTK_MELDUNGEN`, `PDS$TA_BPR`, `SOF$TA_BPR_BESCHR`, `PDS$TA_CARE_DESCRIPTION`) into their respective BigQuery counterparts (`dw_source.isrpt.dwtk_meldungen`, `dw_source.isrpt.pds_ta_bpr`, `dw_source.isrpt.pds_ta_care_description`, `dw_target.isrpt.sof_ta_bpr_beschr`). This can be done using tools like Dataflow, Datastream, or custom ETL scripts.
4.  **Continuous Data Ingestion Setup:**
    *   Establish a continuous data ingestion pipeline (e.g., using Datastream, Striim, or custom Change Data Capture solutions) to keep the `dw_source.isrpt` tables synchronized with the operational Oracle database.
5.  **IAM/Permissions:**
    *   Ensure the service account that will execute the BigQuery Stored Procedures has the necessary BigQuery roles (e.g., `BigQuery Data Editor` on `project.dataset`, `dw_source.isrpt`, and `dw_target.isrpt`) to create, read, write, and execute procedures and tables.
6.  **UDFs (if applicable):**
    *   If any complex logic from `PACKAGE:DWPA_UTIL_SKRIPT` or other helper scripts was translated into BigQuery User-Defined Functions (UDFs), these UDFs must be created in the `project.dataset` (or appropriate dataset) before the stored procedures are deployed.
7.  **Scheduling Configuration:**
    *   Configure an external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, Cloud Scheduler) to invoke the `project.dataset.k_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure with the required parameters at the appropriate schedule. This replaces the original job scheduler.

## 5. Known Gaps & Unresolved References

*   **`r_ausd_bp_ta_bpr_beschr.ksh` Scope:** The full functionality and migration scope of the invoking script `r_ausd_bp_ta_bpr_beschr.ksh` are not fully defined. If it contains additional orchestration or business logic, that needs to be migrated separately, potentially as part of a larger Cloud Composer DAG.
*   **Helper Script Logic Detail:** While the design outlines the approach for helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`), the exact, granular logic within each of these scripts needs to be thoroughly analyzed to ensure a complete and accurate translation into BigQuery equivalents (UDFs, native functions, or procedure logic).
*   **`FOSJobErzeugeEintrag` and `FOSJobDeaktivate`:** These functions were commented out in the original script but hint at a broader job management framework. The current migration provides a basic `job_table` entry. It needs to be confirmed if the full functionality of these legacy job management components is required in the target state, and if so, how `FOSJobDeaktivate` would be implemented (e.g., updating a status in `job_table`).
*   **`file_complexity` Data:** The absence of `file_complexity` data for the source script means that potential hidden complexities or edge cases might not have been fully captured during the initial analysis. Thorough testing is crucial to uncover any such issues.
*   **`bert_k_ausd_bp_ta_bpr_beschr.tmp`:** The temporary file used for record counts is replaced by direct `COUNT(*)` on the target table. This assumes the count is needed *after* the insert. If the original script used this file for inter-process communication or other purposes not immediately apparent, that specific use case needs further investigation.
*   **`PACKAGE:DWPA_UTIL_SKRIPT`:** The design assumes this package's functions will be translated to BigQuery UDFs or native SQL. A detailed analysis of each function within this package is required to ensure correct translation and functionality.
*   **Error Handling Granularity:** The current BigQuery procedures use `RAISE USING MESSAGE` for parameter validation errors. The original `f_alis_msgerr.ksh` might have more sophisticated error logging or notification mechanisms that need to be replicated in the BigQuery environment (e.g., logging to Cloud Logging, sending notifications via Pub/Sub).

## 6. Validation

To validate the successful migration of `k_ausd_bp_ta_bpr_beschr.ksh` to BigQuery:

1.  **Execution:**
    *   Execute the main BigQuery Stored Procedure: `CALL project.dataset.k_ausd_bp_ta_bpr_beschr('JOB001', 'ENTRY001', '25122023', '0');` (Replace parameters with valid test data).
    *   Test with various valid and invalid parameter combinations (e.g., missing parameters, invalid date format) to ensure error handling works as expected.
2.  **Passing Criteria:**
    *   **Successful Completion:** The `k_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure must complete without raising any unhandled exceptions.
    *   **Target Table Population:** The `dw_target.isrpt.sof_ta_bpr_beschr` table must be populated with data.
    *   **Data Accuracy:**
        *   Perform record count comparison: The number of rows in `dw_target.isrpt.sof_ta_bpr_beschr` should match the expected count based on the source data and transformation logic.
        *   Run data quality checks: Verify that the data in `dw_target.isrpt.sof_ta_bpr_beschr` is accurate and consistent with the original job's output. This includes checking specific column values, data types, and referential integrity (if applicable).
    *   **Audit Log Verification:**
        *   Query `project.dataset.job_table` to confirm that a new entry has been created for the job run.
        *   Verify that the `record_count`, `status_a`, `status_i`, `stichtag_from`, `stichtag_to`, `restart_flag`, and `description` fields in the `job_table` entry are correct and reflect the successful execution.
    *   **Error Handling:** When invalid parameters are provided, the procedure should `RAISE` an appropriate error message, and no data should be written to the target table.

## 7. Rollback Procedure

In case of critical issues or failure during the go-live or post-migration validation, the following rollback procedure should be followed:

1.  **Stop New Invocations:** Immediately halt any scheduled or manual invocations of the `project.dataset.k_ausd_bp_ta_bpr_beschr` BigQuery Stored Procedure from the orchestrator (e.g., disable the Cloud Composer DAG, Cloud Scheduler job).
2.  **Revert to Legacy System:** Re-enable and resume the execution of the original `k_ausd_bp_ta_bpr_beschr.ksh` KornShell script in the legacy environment. Ensure the legacy job scheduler is reactivated.
3.  **Data Cleanup (Optional but Recommended):**
    *   If the `dw_target.isrpt.sof_ta_bpr_beschr` table was populated incorrectly or partially, truncate or delete the affected data from this BigQuery table to prevent data inconsistencies.
    *   If the `project.dataset.job_table` contains erroneous entries from the failed BigQuery job runs, these can be deleted or marked as failed for auditing purposes.
4.  **Issue Analysis:** Analyze the root cause of the failure using BigQuery job logs, Cloud Logging, and any other available diagnostic tools.
5.  **Redeploy (After Fix):** Once the issues are resolved, the BigQuery Stored Procedures can be updated and redeployed, and the validation steps repeated before attempting another go-live.