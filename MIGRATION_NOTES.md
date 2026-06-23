# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` to Google Cloud Platform.

The original `k_ausd_v_ta_c_bfc.ksh` script served as an orchestration layer, managing the execution of a core SQL script (`d_ausd_v_ta_c_bfc.sql`), handling parameter validation, job status checks (ignoring active jobs, deactivating old ones), and error reporting.

The job has been migrated to a BigQuery-native solution. The orchestration and data processing logic are now encapsulated within BigQuery Stored Procedures, leveraging BigQuery Standard SQL for all data transformations and job control mechanisms.

**Target Platform:** Google Cloud Platform (GCP)
**Key GCP Services:** BigQuery (for data storage, processing, and orchestration logic)

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`bigquery/ddl/job_table.sql`**
    *   **Role:** Defines the schema for the `project.dataset.job_table`. This table is central to the new job control mechanism, replacing the implicit job management and status tracking previously handled by the KornShell script and its dependencies. It records `run_id`, `job_kennung`, `eintrags_nr`, `start_time`, `end_time`, `status`, `message`, and `processed_records`.
*   **`bigquery/ddl/error_log.sql`**
    *   **Role:** Defines the schema for the `project.dataset.error_log` table. This table replaces the custom error messaging utility (`f_alis_msgerr.ksh`) and `echo` statements, providing a centralized and structured way to log errors encountered during job execution. It captures `timestamp`, `run_id`, `job_kennung`, `eintrags_nr`, `error_code`, and `error_message`.
*   **`bigquery/stored_procedures/bfc_get_bindefrist.sql`**
    *   **Role:** A placeholder BigQuery User-Defined Function (UDF) or Stored Procedure. This artifact is intended to encapsulate the complex business logic originally found in the Oracle PL/SQL function `Cds$vr_Bindefrist.GetBindeFrist`, which is invoked by `d_ausd_v_ta_c_bfc.sql`. Its current implementation is a basic placeholder and requires full translation of the Oracle logic.
*   **`bigquery/stored_procedures/d_ausd_v_ta_c_bfc_core_logic.sql`**
    *   **Role:** A BigQuery Stored Procedure that contains the core data transformation logic. This procedure is a direct migration of the SQL from `d_ausd_v_ta_c_bfc.sql`, translated to BigQuery Standard SQL. It handles the population of `sof_ta_c_bfc_akt`, initial population and merging into `sof_ta_c_bfc`, and updates to `bindefrist` values. It accepts `p_run_id`, `p_job_kennung`, `p_eintrags_nr` and outputs `p_records_processed`.
*   **`bigquery/stored_procedures/k_ausd_v_ta_c_bfc.sql`**
    *   **Role:** The main BigQuery Stored Procedure, serving as the direct replacement for the `k_ausd_v_ta_c_bfc.ksh` KornShell script. This procedure orchestrates the entire job:
        *   Performs parameter validation (`p_jobkennung`, `p_eintragsnr`).
        *   Implements job control logic (checking for active jobs, deactivating old ones) using `project.dataset.job_table`.
        *   Calls `d_ausd_v_ta_c_bfc_core_logic` to execute the main data transformation.
        *   Logs errors to `project.dataset.error_log`.
        *   Updates the `project.dataset.job_table` with the final status and metrics.

## 3. Key Design Decisions

The migration strategy focused on leveraging BigQuery's capabilities for both data processing and job orchestration, aiming for a BigQuery-native solution.

*   **BigQuery Stored Procedures for Orchestration:** Instead of migrating the KornShell script to a different scripting language (e.g., Python) or an external orchestrator (e.g., Cloud Composer), the decision was made to re-implement the orchestration logic directly within a BigQuery Stored Procedure (`k_ausd_v_ta_c_bfc.sql`).
    *   **Rationale:** This approach minimizes external dependencies, keeps the entire workflow within BigQuery, and simplifies deployment and management for jobs that are primarily SQL-driven. It leverages BigQuery's robust execution environment and built-in error handling.
    *   **Trade-offs:** Complex external interactions or multi-system workflows might still require an external orchestrator like Cloud Composer. However, for this specific job, which is self-contained within a database context, a BigQuery Stored Procedure is efficient.
*   **Dedicated BigQuery Tables for Job Control and Logging:** The implicit job tracking and custom error reporting of the legacy system were replaced by explicit BigQuery tables (`job_table` and `error_log`).
    *   **Rationale:** Provides a structured, queryable, and centralized mechanism for monitoring job status, history, and errors. This improves observability, debugging, and auditing compared to parsing shell script logs or temporary files.
    *   **Trade-offs:** Requires DDL creation and management for these control tables.
*   **Direct Migration of SQL Logic to BigQuery Standard SQL:** The core data transformation logic from `d_ausd_v_ta_c_bfc.sql` was directly translated into a BigQuery Stored Procedure (`d_ausd_v_ta_c_bfc_core_logic.sql`).
    *   **Rationale:** Maintains the data processing logic close to the data, leveraging BigQuery's performance for large-scale transformations. `EXECUTE IMMEDIATE` is used to run dynamic SQL, mimicking the original script's ability to execute SQL.
    *   **Trade-offs:** Requires careful translation of Oracle-specific SQL constructs, data types, and functions to BigQuery Standard SQL.
*   **Replacement of Shell Utilities with BigQuery Equivalents:** Custom shell utilities for parameter parsing, date handling, and error messaging were replaced by BigQuery Stored Procedure parameters, built-in functions (e.g., `CURRENT_TIMESTAMP()`), and the `error_log` table.
    *   **Rationale:** Eliminates the need to port or re-implement these utilities in a new scripting language, streamlining the solution and keeping it BigQuery-native.
*   **Handling of `bfc_get_bindefrist` as a UDF/SP:** The Oracle function `Cds$vr_Bindefrist.GetBindeFrist` was identified as a critical piece of business logic and designated for migration into a BigQuery UDF or Stored Procedure (`bfc_get_bindefrist`).
    *   **Rationale:** Encapsulating this complex logic ensures reusability and maintainability within BigQuery, allowing the core data transformation to call it directly.
    *   **Trade-offs:** Requires detailed reverse-engineering and accurate translation of potentially complex PL/SQL logic.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure the target GCP project exists.
    *   Create the BigQuery dataset (`project.dataset`) where all tables and stored procedures will reside. This dataset should be created in the appropriate region.
2.  **Deploy DDL for Control Tables:**
    *   Execute `bigquery/ddl/job_table.sql` to create the `job_table`.
    *   Execute `bigquery/ddl/error_log.sql` to create the `error_log` table.
3.  **Deploy `bfc_get_bindefrist` (Full Implementation Required):**
    *   **Crucially, the placeholder logic in `bigquery/stored_procedures/bfc_get_bindefrist.sql` must be replaced with the actual, fully translated business logic from Oracle's `Cds$vr_Bindefrist.GetBindeFrist`.** This will likely involve detailed analysis of the Oracle source code. Once implemented, deploy this BigQuery UDF/Stored Procedure.
4.  **Deploy Core Logic Stored Procedure:**
    *   Execute `bigquery/stored_procedures/d_ausd_v_ta_c_bfc_core_logic.sql` to create the core data transformation procedure.
5.  **Deploy Main Orchestration Stored Procedure:**
    *   Execute `bigquery/stored_procedures/k_ausd_v_ta_c_bfc.sql` to create the main orchestration procedure.
6.  **Source and Target Table Creation/Migration:**
    *   Ensure all source tables (e.g., `sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) referenced in `d_ausd_v_ta_c_bfc_core_logic.sql` are migrated to BigQuery and populated with data.
    *   Create the target tables (`project.dataset.sof_ta_c_bfc_akt`, `project.dataset.sof_ta_c_bfc`) in BigQuery with schemas matching the expected output of the core logic.
7.  **IAM Permissions:**
    *   The service account or user executing the main BigQuery Stored Procedure (`project.dataset.r_ausd_ta_c_bfc`) must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on the `project.dataset` dataset (to create/update/delete data in `job_table`, `error_log`, `sof_ta_c_bfc_akt`, `sof_ta_c_bfc`).
        *   `BigQuery Data Viewer` on any source datasets/tables.
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
8.  **Scheduling:**
    *   Determine the scheduling mechanism for the `project.dataset.r_ausd_ta_c_bfc` stored procedure. Options include:
        *   **Cloud Scheduler:** To trigger the procedure at defined intervals.
        *   **Cloud Composer (Airflow):** If more complex workflow dependencies or external system integrations are required. A simple DAG would call the BigQuery Stored Procedure.
        *   **BigQuery Scheduled Queries:** If the procedure can be wrapped in a `CALL` statement within a scheduled query.
9.  **Connection Strings/Secrets:**
    *   No explicit connection strings are needed for BigQuery Stored Procedures as they execute natively within BigQuery.
    *   If external orchestration (e.g., Cloud Composer) is used, ensure appropriate service account keys or workload identity is configured for BigQuery access.

## 5. Known Gaps & Unresolved References

The following items have been identified as requiring further attention or are currently placeholders:

*   **`bfc_get_bindefrist` Full Implementation (B4 Item):** The `bigquery/stored_procedures/bfc_get_bindefrist.sql` artifact is currently a placeholder. The complex business logic from Oracle's `Cds$vr_Bindefrist.GetBindeFrist` needs to be thoroughly reverse-engineered and accurately translated into BigQuery Standard SQL. This is a critical B4 (Blocker for Go-Live) item.
*   **`d_ausd_v_ta_c_bfc.sql` Source Table Schemas:** The DDL for the source tables (`sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) and target tables (`sof_ta_c_bfc_akt`, `sof_ta_c_bfc`) used in `d_ausd_v_ta_c_bfc_core_logic.sql` were not provided and need to be accurately defined in BigQuery, including data types and nullability, based on the Oracle source.
*   **Job Deactivation Logic Precision:** While the job control logic for "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert" has been implemented, a thorough review against the exact behavior of the original `starteSQLSkript` function and surrounding shell logic is recommended to ensure 100% functional equivalence.
*   **Error Code Mapping:** The error codes used in the BigQuery Stored Procedure (e.g., `193` for missing parameters, `-1`, `-2`, `-3` for internal errors) are either direct translations or new internal codes. A comprehensive mapping of all legacy error codes from `f_alis_msgerr.ksh` and other shell logic to the new `error_log` table schema should be established.
*   **`v_bfc_procedure` Date Logic:** In `d_ausd_v_ta_c_bfc_core_logic.sql`, `v_bfc_procedure` is set to `CURRENT_DATE()`. The original script likely derived this from a specific deployment or execution date. Confirm if `CURRENT_DATE()` is the correct interpretation or if a fixed deployment date or parameter should be used.
*   **`ROWNUM` to `LIMIT` Translation:** The `ROWNUM <= &v_max_update` in the original Oracle SQL was translated to `LIMIT %d` in BigQuery. While functionally similar for limiting rows, ensure that the exact behavior of `ROWNUM` (e.g., in subqueries or complex `WHERE` clauses) is fully replicated if the original SQL had more intricate usage.
*   **`append` Hint:** The `INSERT /*+ append */` hint in Oracle is for direct-path inserts. BigQuery's `INSERT` behavior is generally optimized, but if the original hint was critical for performance, ensure the BigQuery `INSERT` performance is acceptable.

## 6. Validation

Validation involves running the migrated BigQuery Stored Procedure and verifying its behavior and output against the legacy system.

**Steps to Run Tests:**

1.  **Prerequisites:** Ensure all manual steps (Section 4) are completed, including the full implementation of `bfc_get_bindefrist` and the creation/population of all source and target tables.
2.  **Execute the Main Stored Procedure:**
    *   Open the BigQuery console.
    *   Navigate to the `project.dataset` dataset.
    *   Run the main orchestration procedure using a `CALL` statement:
        ```sql
        CALL `project.dataset.r_ausd_ta_c_bfc`('TEST_JOB', '001');
        ```
    *   Replace `'TEST_JOB'` and `'001'` with appropriate `p_jobkennung` and `p_eintragsnr` values for testing.
3.  **Monitor Execution:**
    *   Observe the BigQuery job execution status in the console.
    *   Check the `job_table` for entries related to the `run_id` generated by the `CALL`.
    *   Check the `error_log` table for any error messages.

**What "Passing" Means:**

A successful migration and test run will exhibit the following characteristics:

*   **Main Procedure Completes Successfully:** The `CALL` statement for `project.dataset.r_ausd_ta_c_bfc` should complete without BigQuery-level errors.
*   **`job_table` Status:**
    *   A new entry should appear in `project.dataset.job_table` with the `job_kennung` and `eintrags_nr` used in the `CALL`.
    *   The `status` column for this entry should be `'SUCCESS'`.
    *   The `start_time` and `end_time` should be populated, and `processed_records` should reflect the actual count of records processed by the core logic.
    *   If a job was intentionally ignored (e.g., by running the same job concurrently), a separate entry with `status = 'IGNORED'` should appear.
    *   If old jobs were active, their status should be updated to `'DEACTIVATED'`.
*   **`error_log` Empty (for successful runs):** For a successful execution, there should be no new entries in `project.dataset.error_log` corresponding to the `run_id` of the successful job.
*   **Data Correctness:**
    *   The data in the target table (`project.dataset.sof_ta_c_bfc`) should be identical to the data produced by the legacy system when run with the same input conditions. This requires a data comparison strategy (e.g., row counts, checksums, or detailed row-by-row comparison).
    *   The `bindefrist` column, which relies on `bfc_get_bindefrist`, should contain the correct calculated dates.
*   **Parameter Validation:** Test with missing or invalid parameters to ensure the procedure correctly logs errors to `error_log` and exits gracefully (e.g., `RAISE` statement).
*   **Concurrency Handling:** Test running the procedure concurrently with the same `job_kennung` and `eintrags_nr` to verify the "ignore active jobs" and "deactivate old jobs" logic functions as expected.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following rollback procedure can be executed to revert to the legacy system:

1.  **Stop New Job Execution:**
    *   Immediately halt any scheduled executions of the BigQuery Stored Procedure (`project.dataset.r_ausd_ta_c_bfc`) in Cloud Scheduler, Cloud Composer, or BigQuery Scheduled Queries.
    *   Cancel any currently running BigQuery jobs related to this migration.
2.  **Verify Legacy System Readiness:**
    *   Ensure the original KornShell script (`k_ausd_v_ta_c_bfc.ksh`) and its dependencies are fully operational and accessible in the legacy environment.
    *   Confirm that the legacy Oracle database is in a consistent state and contains the expected data.
3.  **Revert Scheduling to Legacy:**
    *   Re-enable the original scheduling mechanism for `k_ausd_v_ta_c_bfc.ksh` (e.g., cron jobs, legacy scheduler).
4.  **Data Restoration (if necessary):**
    *   If the BigQuery job made irreversible changes to shared target tables that are also consumed by the legacy system, or if the data in BigQuery is deemed corrupted, a data restoration might be necessary.
    *   **Option A (Point-in-Time Restore):** If BigQuery tables were modified, consider using BigQuery's time travel feature to restore the tables to a state before the problematic BigQuery job ran.
    *   **Option B (Reload from Source):** If the target tables in BigQuery can be fully rebuilt from source data (either legacy or other BigQuery sources), initiate a full reload.
    *   **Note:** The current design implies `sof_ta_c_bfc` is a target table. If this table is critical for other systems and the BigQuery job corrupted it, restoring it to a known good state is paramount.
5.  **Monitor Legacy System:**
    *   Closely monitor the re-enabled legacy job and its output to ensure it is functioning correctly and producing accurate results.
6.  **Post-Rollback Analysis:**
    *   Once the legacy system is stable, conduct a thorough root cause analysis of the issues that necessitated the rollback. This will inform corrective actions before attempting re-migration.

**Important Considerations for Rollback:**

*   **Data Consistency:** The most critical aspect of rollback is ensuring data consistency across systems. Understand the impact of the migrated job on shared data.
*   **Idempotency:** Ideally, the migrated job should be idempotent, meaning running it multiple times produces the same result. This simplifies recovery but doesn't negate the need for careful rollback planning.
*   **Communication:** Clearly communicate the rollback decision and status to all stakeholders.