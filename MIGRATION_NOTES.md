# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_cntrct_templ.ksh` job, which orchestrates the data processing for contract template data. The original job, consisting of a KornShell script and an Oracle SQL script, has been migrated to Google BigQuery. The KornShell orchestration logic is re-implemented as a BigQuery Stored Procedure, and the Oracle SQL data processing logic is converted into a separate BigQuery Stored Procedure.

**Original Job:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`
**Target Platform:** Google BigQuery

## 2. Generated Artifacts

The migration produced the following BigQuery Stored Procedures:

*   **`sql/proc_d_ausd_v_ta_cntrct_templ.sql`**
    *   **Role:** This stored procedure encapsulates the core data extraction, transformation, and loading (ETL) logic. It is a direct translation of the original `d_ausd_v_ta_cntrct_templ.sql` Oracle script. It determines a snapshot date, truncates the target table, and then inserts processed contract template data from source tables (`isbert_dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`) into the target table (`sof_ta_cntrct_templ`).

*   **`sql/proc_k_ausd_v_ta_cntrct_templ.sql`**
    *   **Role:** This stored procedure serves as the main orchestration script, replacing the original `k_ausd_v_ta_cntrct_templ.ksh` KornShell script. It handles parameter validation, calls the `proc_d_ausd_v_ta_cntrct_templ` for data processing, captures the number of records processed, and includes placeholders for job control logic (e.g., checking for active jobs, logging status). It also incorporates BigQuery's native error handling.

## 3. Key Design Decisions

*   **Orchestration Layer Conversion:** The KornShell script's orchestration logic, including parameter parsing, validation, and SQL script execution, was migrated to a BigQuery Stored Procedure (`proc_k_ausd_v_ta_cntrct_templ`). This centralizes the job's control flow within BigQuery, leveraging its native scripting capabilities for variables, control structures, and error handling. This approach simplifies deployment and management compared to maintaining external shell scripts.
*   **Data Processing Logic Conversion:** The Oracle SQL script (`d_ausd_v_ta_cntrct_templ.sql`) was directly translated into a separate BigQuery Stored Procedure (`proc_d_ausd_v_ta_cntrct_templ`).
    *   **Oracle-specific Function Translation:** `NVL` was converted to `COALESCE`, and `TO_CHAR`/`TO_DATE` functions were replaced with BigQuery's `PARSE_DATE` and direct date comparisons.
    *   **Schema and DB-link Handling:** Oracle schema prefixes (e.g., `isbert_schema.`, `cds$`) and DB-links (`@pcrs1`) were replaced with fully qualified BigQuery table references (`project.dataset.table_name`).
    *   **`TRUNCATE TABLE`:** The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call for truncating the target table was replaced with a direct BigQuery `TRUNCATE TABLE` statement.
*   **Record Count Mechanism:** The original KornShell script used a temporary file to store and retrieve the record count. In BigQuery, this was replaced by a `DECLARE` variable (`v_records_processed`) and a `SELECT COUNT(*)` query directly on the target table after insertion. This eliminates file system dependencies and streamlines the process.
*   **Job Control Logic (Placeholder):** The complex job control logic from the original KornShell script (ignoring active jobs, deactivating old jobs) was identified as a candidate for external orchestration (e.g., Cloud Composer/Airflow) or a dedicated BigQuery job control table. The generated code includes commented-out placeholders to illustrate where this logic would reside, deferring the full implementation to the target orchestration strategy.
    *   **Trade-off:** This decision simplifies the initial BigQuery migration by focusing on core data logic, but requires a separate effort to fully implement the job control and scheduling aspects, potentially outside of BigQuery's native stored procedures.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the BigQuery environment for the migrated job:

1.  **BigQuery Dataset Creation:**
    *   Create the BigQuery datasets that will host the tables. Based on the generated code, at least one dataset (referred to as `project.dataset`) is needed. If source schemas (`isbert_schema`, `cds`) are to be separate, create corresponding datasets (e.g., `project.isbert_dataset`, `project.cds_dataset`, `project.sof_dataset`).
2.  **Table DDL Creation and Data Ingestion:**
    *   Create the BigQuery DDLs for the following tables based on their Oracle counterparts:
        *   `project.dataset.isbert_dwtk_meldungen` (source)
        *   `project.dataset.cds_ta_cntrct_template` (source)
        *   `project.dataset.cds_ta_care_description` (source)
        *   `project.dataset.sof_ta_cntrct_templ` (target)
    *   Ingest historical and/or initial data into the source tables (`isbert_dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`) from the Oracle source system.
3.  **IAM Permissions:**
    *   Ensure the service account or user executing the BigQuery Stored Procedures has the necessary IAM roles and permissions:
        *   `BigQuery Data Editor` on the target dataset (`project.dataset`) to `TRUNCATE` and `INSERT` into `sof_ta_cntrct_templ`.
        *   `BigQuery Data Viewer` on the source datasets (`project.dataset` or specific source datasets) to `SELECT` from `isbert_dwtk_meldungen`, `cds_ta_cntrct_template`, and `cds_ta_care_description`.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
4.  **Job Control Table (Optional but Recommended):**
    *   If the job control logic (checking for active jobs, logging status) is to be implemented within BigQuery, create the `project.dataset.job_control_table` with appropriate columns (e.g., `job_name`, `entry_number`, `status`, `start_time`, `end_time`, `records_processed`, `error_message`, `error_stack`).
5.  **Scheduling Configuration:**
    *   Configure the scheduling mechanism (e.g., Cloud Composer/Airflow DAG, Cloud Scheduler, or other orchestration tool) to invoke `CALL project.dataset.proc_k_ausd_v_ta_cntrct_templ('YOUR_JOB_KENNUNG', YOUR_ENTRY_NR)`. Ensure parameters `p_jobkennung` and `p_eintragsnr` are passed correctly.

## 5. Known Gaps & Unresolved References

*   **Full `MERGE` Statement:** The original design document indicated a `MERGE` operation involving `TABLE:VIA` in `d_ausd_v_ta_cntrct_templ.sql`, but the full SQL for this was not provided in the source inventory. The generated `proc_d_ausd_v_ta_cntrct_templ.sql` does *not* include any `MERGE` statement or reference to `TABLE:VIA`. This is a critical gap that needs to be addressed. The complete `MERGE` logic must be obtained and translated into BigQuery SQL.
*   **`DWPA_UTIL_SKRIPT` Package Functionality:** While the `runstatement` function was assumed to primarily perform `TRUNCATE TABLE`, it's crucial to confirm if `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` has any other side effects or complex logic beyond a simple truncate. If so, that logic needs to be migrated.
*   **Job Control Logic Implementation:** The generated `proc_k_ausd_v_ta_cntrct_templ.sql` contains commented-out placeholders for the job control logic (checking for active jobs, deactivating old jobs, logging job status). The actual implementation of this logic (e.g., using a dedicated BigQuery job control table, or delegating entirely to an external orchestrator like Airflow) needs to be finalized and coded.
*   **Schema Mapping Consistency:** The generated code uses `project.dataset` as a generic placeholder. The actual BigQuery dataset names for `isbert_schema`, `cds`, and `sof` (e.g., `isbert_raw`, `cds_staging`, `sof_curated`) need to be consistently defined and applied across all generated procedures and DDLs.

## 6. Validation

To ensure the successful migration and correct functionality of the BigQuery job, the following validation steps should be performed:

1.  **Unit Testing `proc_d_ausd_v_ta_cntrct_templ`:**
    *   **Execution:** Call the stored procedure directly: `CALL project.dataset.proc_d_ausd_v_ta_cntrct_templ();`
    *   **Passing Criteria:**
        *   Successful execution without errors.
        *   Verify that `sof_ta_cntrct_templ` is truncated and then populated with data.
        *   Compare record counts: `SELECT COUNT(*) FROM project.dataset.sof_ta_cntrct_templ;` should match the expected count from the source Oracle system for the same snapshot date.
        *   Spot-check data: Select a sample of records from `sof_ta_cntrct_templ` and compare values against the corresponding records in the Oracle source `sof$ta_cntrct_templ` for data integrity and transformation accuracy.

2.  **Integration Testing `proc_k_ausd_v_ta_cntrct_templ`:**
    *   **Execution:** Call the main orchestration procedure with test parameters: `CALL project.dataset.proc_k_ausd_v_ta_cntrct_templ('TEST_JOB', 123);`
    *   **Passing Criteria:**
        *   Successful execution without errors.
        *   The log messages (from `SELECT FORMAT(...)`) should indicate successful completion and the correct number of records processed.
        *   If job control logic is implemented, verify that job status updates in the `job_control_table` are correct (e.g., 'ACTIVE' -> 'COMPLETED').
        *   Verify that `proc_d_ausd_v_ta_cntrct_templ` was successfully invoked and its data processing completed correctly (as per unit test criteria).

3.  **End-to-End Orchestration Testing:**
    *   **Execution:** Trigger the job via the configured scheduler (e.g., Airflow DAG).
    *   **Passing Criteria:**
        *   The scheduler reports a successful run.
        *   All BigQuery procedures execute successfully.
        *   The final data in `project.dataset.sof_ta_cntrct_templ` is accurate and complete, matching the source system's output for the same run.
        *   Performance metrics (execution time, slot usage) are within acceptable limits.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Stop New Executions:** Immediately halt any scheduled or manual executions of the new BigQuery job (`proc_k_ausd_v_ta_cntrct_templ`) in the target orchestration system (e.g., disable the Airflow DAG).
2.  **Revert Scheduling:** Re-enable the original scheduling mechanism for the legacy Oracle job (`k_ausd_v_ta_cntrct_templ.ksh`).
3.  **Data Restoration (if necessary):**
    *   If the `sof_ta_cntrct_templ` table in BigQuery was corrupted or incorrectly populated by the new job, it can be restored from a previous successful run's snapshot (if BigQuery table snapshots are enabled) or by re-running the *last known good* legacy Oracle job to populate the BigQuery table.
    *   Alternatively, if the issue is with the BigQuery job itself and not data corruption, simply stopping the new job and reverting to the old one might be sufficient, as the BigQuery job truncates the target table before inserting.
4.  **Re-enable Legacy Job:** Verify that the original Oracle job is running as expected and producing correct output.
5.  **Investigation:** Analyze the root cause of the failure in the BigQuery environment, rectify the issues, and re-test thoroughly before attempting another go-live.