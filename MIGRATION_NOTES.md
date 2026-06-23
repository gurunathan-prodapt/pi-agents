# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell control script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh` and its associated core SQL logic (`d_ausd_v_ta_inv_def.sql`). The migration targets Google BigQuery, transforming the shell script's orchestration and parameter handling into a BigQuery Stored Procedure, and the core SQL logic into a separate BigQuery Stored Procedure. This transition aims to leverage BigQuery's native capabilities for data processing, job management, and logging, eliminating dependencies on legacy shell utilities and `sqlplus`.

## 2. Generated Artifacts

The migration process has generated the following BigQuery SQL scripts:

*   **`project.dataset.d_ausd_v_ta_inv_def.sql`**
    *   **Role:** This script defines a BigQuery Stored Procedure that encapsulates the core data manipulation logic originally found in `d_ausd_v_ta_inv_def.sql`. It is responsible for truncating the target table `sof$ta_inv_def` and then inserting processed data based on complex joins and conditions from source tables.
*   **`project.dataset.job_table_ddl.sql`**
    *   **Role:** Data Definition Language (DDL) script to create the `job_table`. This table is used for managing job statuses, tracking active runs, and storing metadata for different job instances (`job_kennung`, `eintrags_nr`).
*   **`project.dataset.job_error_log_ddl.sql`**
    *   **Role:** DDL script to create the `job_error_log` table. This table provides structured logging for errors encountered during job execution, replacing the shell-based `DWMSG_MeldeFehler` mechanism.
*   **`project.dataset.job_run_log_ddl.sql`**
    *   **Role:** DDL script to create the `job_run_log` table. This table records details for each job run, including start/end timestamps, processed record counts, and overall status, replacing temporary file-based logging.
*   **`project.dataset.r_ausd_vertrag_control.sql`**
    *   **Role:** This script defines the main BigQuery Stored Procedure, `r_ausd_vertrag_control`. It is the direct migration of `k_ausd_v_ta_inv_def.ksh`. Its responsibilities include:
        *   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`).
        *   Managing job status in `job_table` (deactivating older runs, updating current run).
        *   Calling the `d_ausd_v_ta_inv_def` stored procedure to execute the core data logic.
        *   Capturing and logging the number of processed records.
        *   Handling exceptions and logging errors to `job_error_log`.

## 3. Key Design Decisions

The migration strategy was guided by the following key design decisions:

*   **Migration of Shell Script Control Flow to BigQuery Stored Procedure:** The entire control logic, parameter handling, and job orchestration from the original KornShell script were translated into a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This centralizes the logic within BigQuery, eliminating external shell dependencies and leveraging BigQuery's native execution environment.
*   **Modularization of Core SQL Logic:** The data manipulation logic (`d_ausd_v_ta_inv_def.sql`) was encapsulated in its own BigQuery Stored Procedure (`d_ausd_v_ta_inv_def`). This promotes modularity, reusability, and clearer separation of concerns between orchestration and data transformation.
*   **Dedicated BigQuery Tables for Job Management and Logging:** Instead of relying on temporary files or shell-based logging, dedicated BigQuery tables (`job_table`, `job_error_log`, `job_run_log`) were introduced. This provides structured, queryable, and persistent logging and job status tracking, significantly improving observability and maintainability.
*   **Leveraging Native BigQuery Features:** The migration extensively uses BigQuery's built-in functions and constructs, such as `GENERATE_UUID()`, `CURRENT_TIMESTAMP()`, `MERGE INTO` for atomic upserts, and `EXCEPTION WHEN ERROR` blocks for robust error handling. This ensures efficiency and adherence to BigQuery best practices.

**Notable Trade-offs:**

*   **Increased BigQuery SQL Complexity:** Translating shell script control flow (e.g., `if` conditions, loops, parameter parsing) into BigQuery SQL stored procedure syntax can be more verbose and complex than simple shell scripting.
*   **Loss of Direct Filesystem Interaction:** The ability to interact directly with the filesystem (e.g., creating temporary files) is replaced by table-based logging and data storage, which requires a different paradigm for data exchange and status reporting.
*   **Potential for External Orchestration:** While the core control is within BigQuery, if the original script had complex external system interactions or dependencies beyond simple database operations, an external orchestration tool like Cloud Composer or Dataform might still be required, adding another layer of complexity.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project_id.dataset_id`) exists. If not, create it.
2.  **DDL Execution for Job Management Tables:**
    *   Execute `project.dataset.job_table_ddl.sql` to create the `job_table`.
    *   Execute `project.dataset.job_error_log_ddl.sql` to create the `job_error_log` table.
    *   Execute `project.dataset.job_run_log_ddl.sql` to create the `job_run_log` table.
3.  **Target Data Table Creation:** Ensure the target data table `project_id.dataset_id.sof$ta_inv_def` exists with the correct schema as expected by `d_ausd_v_ta_inv_def.sql`.
4.  **Source Data Table Accessibility:** Verify that all source tables referenced in `d_ausd_v_ta_inv_def.sql` (e.g., `project_id.dataset_id.dwtk_meldungen`, `project_id.dataset_id.cds$ta_inv_definition`, `project_id.dataset_id.cds$ta_inv_cont_config`, `project_id.dataset_id.cds$ta_care_description`) exist in the specified dataset and are accessible to the BigQuery service account.
5.  **IAM Permissions:**
    *   The service account or user executing the BigQuery Stored Procedures must have `BigQuery Data Editor` role on `project_id.dataset_id` to create/update tables and execute procedures.
    *   It also requires `BigQuery Job User` to run BigQuery jobs.
6.  **Connection Strings / Secrets:** Not directly applicable for BigQuery Stored Procedures. If an external orchestrator (e.g., Cloud Composer) is used, ensure the orchestrator's service account has the necessary BigQuery permissions.
7.  **Scheduling Update:** Update the existing job scheduler (e.g., Cron, Airflow, Cloud Scheduler) to invoke the BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` with the required parameters (`p_JobKennung`, `p_EintragsNr`). This typically involves using the `bq query` command-line tool or a BigQuery client library.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas where further investigation might be beneficial:

*   **Detailed Logic of `d_ausd_v_ta_inv_def.sql`:** While a migration of the SQL logic has been provided, any highly specific Oracle functions, PL/SQL constructs, or performance-critical aspects of the original `d_ausd_v_ta_inv_def.sql` might require further review and optimization for BigQuery.
*   **`starteSQLSkript` Implementation Details:** The exact behavior of the original `starteSQLSkript` (e.g., specific `sqlplus` commands, transaction management, error handling within the SQL execution) was not fully known. The migration assumes a direct `CALL` to the nested BigQuery Stored Procedure is sufficient.
*   **"Job Table" Semantics:** The precise business rules and schema for the original "job table" (referenced by the shell script for deactivating older jobs) were inferred. The migrated `job_table` DDL and logic should be validated against the original system's exact requirements.
*   **Temporary File Content:** The temporary file (`bert_k_ausd_v_ta_inv_def_$$.tmp`) was assumed to contain only the record count. If it held other critical information, that data flow needs to be re-evaluated.
*   **Helper Script Specifics:** The functionalities of the sourced shell helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) have been replaced by native BigQuery features. Any complex or unique logic within these original helpers that goes beyond standard error messaging, date handling, or parameter parsing might not be fully replicated.
*   **`file_complexity` Data:** The absence of `file_complexity` data for the original script means the "medium" complexity tier is an estimate. Unforeseen complexities could arise during testing.
*   **B4 Item: Inconsistency in Target Table Naming:** The `d_ausd_v_ta_inv_def.sql` procedure writes to `project_id.dataset_id.sof$ta_inv_def`, but the `r_ausd_vertrag_control.sql` procedure attempts to count records from `project_id.dataset_id.ta_inv_def_result`. This is an inconsistency that needs to be resolved. The `r_ausd_vertrag_control` procedure should be updated to count from `project_id.dataset_id.sof$ta_inv_def` to reflect the actual target table.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Deployment:**
    *   Execute all DDL scripts (`job_table_ddl.sql`, `job_error_log_ddl.sql`, `job_run_log_ddl.sql`) to create the necessary tables.
    *   Execute the Stored Procedure creation scripts (`d_ausd_v_ta_inv_def.sql`, `r_ausd_vertrag_control.sql`) to deploy the procedures.
2.  **Test Execution:**
    *   Call the main control procedure using a BigQuery client (e.g., `bq query --use_legacy_sql=false 'CALL project_id.dataset_id.r_ausd_vertrag_control("TEST_JOB", "ENTRY_1");'`).
    *   Test with different valid `p_JobKennung` and `p_EintragsNr` values.
    *   Test error scenarios, such as calling the procedure with `NULL` parameters.
3.  **Verification of Results:**
    *   **`r_ausd_vertrag_control` Completion:** The procedure should complete without unhandled errors.
    *   **`job_run_log` Table:**
        *   Verify that an entry exists for each run with `status = 'SUCCESS'` for successful runs.
        *   Check `start_timestamp`, `end_timestamp`, `job_kennung`, `eintrags_nr`, and `records_processed` for accuracy.
    *   **`job_table` Table:**
        *   Verify that the entry corresponding to the latest `job_kennung` and `eintrags_nr` has `active_flag = TRUE`.
        *   Verify that previous runs with the same `job_kennung` but different `eintrags_nr` have `active_flag = FALSE`.
    *   **`job_error_log` Table:**
        *   For error test cases (e.g., missing parameters), verify that an entry is logged with the correct `error_message`.
    *   **`sof$ta_inv_def` Table (Data Validation):**
        *   Perform a data comparison between the data generated in `project_id.dataset_id.sof$ta_inv_def` by the BigQuery job and the expected output from the original source system. This is the most critical step for data integrity.
        *   Ensure the `COUNT(*)` from `sof$ta_inv_def` matches the `records_processed` in `job_run_log`.

**"Passing" Criteria:**

*   All BigQuery Stored Procedures execute successfully without runtime errors.
*   The `job_run_log` table accurately reflects the execution status, timestamps, and processed record counts for all test runs.
*   The `job_table` correctly manages the `active_flag` for different job instances.
*   Error conditions are gracefully handled and logged to `job_error_log`.
*   The data loaded into `project_id.dataset_id.sof$ta_inv_def` is functionally equivalent and numerically consistent with the output of the original `k_ausd_v_ta_inv_def.ksh` job.

## 7. Rollback Procedure

In case of issues or a decision to revert to the original system, follow these steps:

1.  **Revert Scheduling:** Immediately update the job scheduler (e.g., Cron, Airflow, Cloud Scheduler) to stop invoking the BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` and instead point back to the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh` script.
2.  **Data Integrity Check:**
    *   If the BigQuery job modified `project_id.dataset_id.sof$ta_inv_def` in a way that is incompatible with the original system, a data restore or reprocessing by the original job might be necessary.
    *   Ensure the original job can safely overwrite or reprocess the data in its target table.
3.  **Optional: BigQuery Object Deletion:** If the rollback is permanent, the created BigQuery tables and stored procedures can be dropped:
    *   `DROP PROCEDURE IF EXISTS project_id.dataset_id.r_ausd_vertrag_control;`
    *   `DROP PROCEDURE IF EXISTS project_id.dataset_id.d_ausd_v_ta_inv_def;`
    *   `DROP TABLE IF EXISTS project_id.dataset_id.job_table;`
    *   `DROP TABLE IF EXISTS project_id.dataset_id.job_error_log;`
    *   `DROP TABLE IF EXISTS project_id.dataset_id.job_run_log;`
    *   **Note:** Do NOT drop `project_id.dataset_id.sof$ta_inv_def` or any source tables unless they were specifically created *only* for this migration and are not used by other processes.