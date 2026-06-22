# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_bp_ta_rn_einzeln.ksh` from a legacy environment to Google Cloud BigQuery. The original script orchestrated the execution of a core SQL process (`d_ausd_bp_ta_rn_einzeln.sql`), handling parameter parsing, date validation, SQL execution, record counting, and logging.

The migrated solution re-implements this orchestration and data processing logic within Google BigQuery. The core logic is now encapsulated in a BigQuery Stored Procedure, which accepts parameters, performs validations, executes the data transformation, and logs execution metadata and errors to dedicated BigQuery tables.

**Target Platform:** Google Cloud BigQuery

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/d_ausd_bp_ta_rn_einzeln_core.sql`**
    *   **Role:** This file contains the core `INSERT...SELECT` SQL logic extracted and migrated from the original `d_ausd_bp_ta_rn_einzeln.sql`. It serves as the blueprint for the data transformation part embedded within the BigQuery Stored Procedure. It includes a placeholder `_stichtag_yyyymmdd` for the key date, which is dynamically replaced at runtime by the stored procedure.
*   **`ddl/error_log.sql`**
    *   **Role:** This DDL (Data Definition Language) script defines the schema for the `project.dataset.error_log` BigQuery table. This table is used to capture and store detailed error messages and execution context, replacing the functionality of the legacy `f_alis_msgerr.ksh` utility.
*   **`ddl/job_table.sql`**
    *   **Role:** This DDL script defines the schema for the `project.dataset.job_table` BigQuery table. This table serves as the central audit log for job executions, recording status, parameters, record counts, and timestamps. It replaces the functionality of the legacy `FOSJobErzeugeEintrag` and `FOSJobDeaktivate` mechanisms.
*   **`procedures/r_ausd_bp_ta_rn_einzeln.sql`**
    *   **Role:** This is the main BigQuery Stored Procedure. It encapsulates the entire orchestration logic of the original KornShell script. It handles:
        *   Receiving input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Initial logging of job start.
        *   Validation of mandatory parameters and date format (`DDMMYYYY`).
        *   Truncating the target table (`project.dataset.sof_ta_rn_einzeln`).
        *   Executing the core data transformation logic (migrated from `d_ausd_bp_ta_rn_einzeln.sql`).
        *   Counting processed records.
        *   Handling exceptions and logging errors to `error_log`.
        *   Updating the `job_table` with final status and metrics.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Orchestration:** The entire control flow, parameter handling, validation, and logging logic previously managed by the KornShell script is now consolidated into a single BigQuery Stored Procedure (`r_ausd_bp_ta_rn_einzeln`). This centralizes the logic within the data warehouse environment, leveraging BigQuery's native capabilities for procedural SQL.
*   **Native BigQuery SQL for Core Logic:** The data transformation logic from `d_ausd_bp_ta_rn_einzeln.sql` has been directly embedded into the BigQuery Stored Procedure. This avoids external file dependencies and allows for seamless execution within BigQuery, benefiting from its query optimization and performance.
*   **Dedicated BigQuery Tables for Logging and Auditing:** Instead of file-based logs or external job control systems, `project.dataset.error_log` and `project.dataset.job_table` are used. This provides structured, queryable, and centralized logging within BigQuery, making monitoring and troubleshooting more efficient.
*   **BigQuery Date Functions for Validation:** The legacy `h_alis_date.ksh` functionality for date format validation is replaced by BigQuery's `SAFE.PARSE_DATE` and `FORMAT_DATE` functions, ensuring robust date handling directly within SQL.
*   **`TRUNCATE` and `INSERT` Pattern:** The core data processing starts with a `TRUNCATE` of the target table (`sof_ta_rn_einzeln`) followed by an `INSERT` of the transformed data. This implies a full refresh strategy for the target table, consistent with common ETL patterns.
*   **Error Handling with `EXCEPTION WHEN ERROR` and `RAISE`:** BigQuery Stored Procedures provide structured error handling. The `BEGIN...EXCEPTION WHEN ERROR...END` block captures runtime errors during the core SQL execution, logs them to `error_log`, updates the `job_table` with a `FAILED` status, and re-raises the error to signal failure to the caller.
*   **Parameterization:** All dynamic values (Job identifier, Entry number, Key date, Restart value) are passed as explicit parameters to the BigQuery Stored Procedure, enhancing reusability and testability.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `project.dataset` (or the appropriate project/dataset names) exists in your Google Cloud project. If not, create it.
2.  **Target Table Creation (`sof_ta_rn_einzeln`):**
    *   The target table `project.dataset.sof_ta_rn_einzeln` must be created with the appropriate schema to receive the output of the transformation. The schema can be inferred from the `INSERT` statement in `sql/d_ausd_bp_ta_rn_einzeln_core.sql` and `procedures/r_ausd_bp_ta_rn_einzeln.sql`.
3.  **Source Tables Availability (`sof_ta_bpr_basis`, `sof_ta_msisdn`):**
    *   Ensure that the source tables `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` exist in BigQuery and contain the necessary data with compatible schemas as expected by the core SQL logic. These tables must be populated with data before the procedure can run successfully.
4.  **Logging and Auditing Table Creation:**
    *   Execute the DDL scripts:
        *   `ddl/error_log.sql` to create `project.dataset.error_log`.
        *   `ddl/job_table.sql` to create `project.dataset.job_table`.
5.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `procedures/r_ausd_bp_ta_rn_einzeln.sql` script to create or replace the stored procedure in `project.dataset`.
6.  **IAM Permissions:**
    *   The service account or user executing the BigQuery Stored Procedure must have:
        *   `BigQuery Data Editor` role (or equivalent) on `project.dataset` to `INSERT` into `sof_ta_rn_einzeln`, `error_log`, `job_table`, and `TRUNCATE` `sof_ta_rn_einzeln`.
        *   `BigQuery Data Viewer` role (or equivalent) on `project.dataset` to `SELECT` from `sof_ta_bpr_basis` and `sof_ta_msisdn`.
        *   `BigQuery Job User` role to run BigQuery jobs (including stored procedures).
7.  **Scheduling (if applicable):**
    *   If the job requires scheduled execution, configure a Cloud Scheduler job to trigger the BigQuery Stored Procedure, or integrate it into a Cloud Composer (Airflow) DAG or Cloud Workflows definition. This will involve defining the parameters to be passed to the stored procedure.

## 5. Known Gaps & Unresolved References

*   **Detailed Schema for `sof_ta_rn_einzeln`:** While the `INSERT` statement defines the columns, the full DDL for the target table `project.dataset.sof_ta_rn_einzeln` (including data types, partitioning, clustering, and primary keys if applicable) was not provided and needs to be defined and created manually.
*   **Source Table Schemas:** The exact schemas for `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` are assumed to be compatible with the `SELECT` query. These must be verified and created/migrated if they don't exist.
*   **`FOSJobDeaktivate` Functionality:** The original script referenced `FOSJobDeaktivate`. The migration addresses `FOSJobErzeugeEintrag` by logging to `job_table`. If `FOSJobDeaktivate` had a more complex function (e.g., disabling other jobs, interacting with an external scheduler), that specific functionality is not directly replicated in the BigQuery Stored Procedure and would need to be handled by an external orchestrator (e.g., Cloud Composer) or by extending the `job_table` logic.
*   **Oracle-Specific SQL:** The migration assumes the original `d_ausd_bp_ta_rn_einzeln.sql` was largely ANSI SQL compatible or has been fully converted to BigQuery Standard SQL. Any remaining Oracle-specific constructs would need further conversion.
*   **Commented-out `sed`, `sort`, `join`:** The design document noted commented-out shell commands. If these were ever intended to be active or represent a potential future requirement, their BigQuery SQL equivalents would need to be implemented.
*   **`p_wiederanlaufWert` Usage:** The `p_wiederanlaufWert` (restart value) parameter is passed to the stored procedure and used to set the `restart_flag` in `job_table`. However, the current procedure does not implement any specific restart logic (e.g., conditional processing based on this value). If the original script had specific restart behavior, this would need to be added to the BigQuery Stored Procedure.

## 6. Validation

To validate the successful migration and execution of the job:

1.  **Prepare Test Data:**
    *   Populate `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` with representative test data that covers various scenarios (e.g., different `bpr_id` and `callnumber_role_id` combinations, valid/invalid `valid_to` dates, edge cases).
    *   Ensure `project.dataset.sof_ta_rn_einzeln` is empty or contains only test data that can be truncated.

2.  **Execute the BigQuery Stored Procedure:**
    *   Call the stored procedure using the BigQuery UI, `bq` command-line tool, or a client library:
        ```sql
        CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
            'TEST_JOB_KENNUNG',
            'TEST_ENTRY_NR',
            '01012023', -- Example Stichtag in DDMMYYYY format
            NULL        -- No restart value for initial run
        );
        ```
    *   Test with invalid parameters (e.g., missing `p_Stichtag`, invalid date format) to ensure error handling works as expected.

3.  **Verify "Passing" Criteria:**
    *   **Job Table Entry:** Query `project.dataset.job_table` for the `TEST_JOB_KENNUNG` and `TEST_ENTRY_NR`.
        *   A "passing" run will have an entry with `status_a = 'COMPLETED'` and `status_i = 'SUCCESS'`.
        *   The `record_count` should reflect the actual number of records inserted into `sof_ta_rn_einzeln`.
        *   `stichtag_from` and `stichtag_to` should match the parsed `p_Stichtag`.
    *   **Error Log:** Query `project.dataset.error_log`.
        *   A "passing" run should **not** have any new entries related to `v_job_name` ('k_ausd_bp_ta_rn_einzeln') for the execution timestamp.
        *   For runs with intentionally invalid parameters, corresponding error entries should be present.
    *   **Target Data Validation:** Query `project.dataset.sof_ta_rn_einzeln`.
        *   Verify that the number of rows matches the `record_count` in `job_table`.
        *   Perform data quality checks to ensure the transformed data is correct according to business rules and matches expected output based on the source data and the SQL logic. This includes checking `CASE` statement logic, `PARSE_DATE` conversions, and `valid_to` comparisons.

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be followed:

1.  **Deactivate New Job Schedule:**
    *   If the BigQuery Stored Procedure was integrated into a scheduler (e.g., Cloud Scheduler, Cloud Composer), immediately pause or disable the new schedule to prevent further executions.
2.  **Reactivate Legacy Job:**
    *   Re-enable the original `k_ausd_bp_ta_rn_einzeln.ksh` script in the legacy environment. Ensure its schedule and dependencies are restored to their pre-migration state.
3.  **Data Restoration (if necessary):**
    *   If the `project.dataset.sof_ta_rn_einzeln` table was corrupted or incorrectly populated by the new procedure, restore it from the most recent valid backup. BigQuery offers point-in-time recovery for up to 7 days, allowing you to restore a table to a previous state.
    *   Alternatively, if the data in `sof_ta_rn_einzeln` is derived entirely from other source tables, a simple `TRUNCATE` and re-run of the *legacy* job (if it can populate BigQuery) or a manual re-population using the legacy process might suffice.
4.  **Monitor Legacy Job:**
    *   Verify that the legacy job is running correctly and producing the expected output.
5.  **Analyze and Rectify:**
    *   Investigate the root cause of the failure in the migrated BigQuery job using the `error_log` and `job_table` entries, BigQuery job history, and logs. Plan for remediation and re-testing.
6.  **Remove Migrated Artifacts (Optional):**
    *   Once the legacy system is stable and the issue with the migrated job is understood, the BigQuery Stored Procedure and DDLs can be dropped from the BigQuery dataset if they are causing confusion or are deemed faulty. This step is usually performed after a thorough analysis and only if the migrated components are not expected to be fixed and re-deployed soon.