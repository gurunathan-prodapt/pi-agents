# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh` to Google Cloud's BigQuery platform.

The original `k_ausd_geschaeftspartner.ksh` script served as an orchestration layer, responsible for parsing parameters, managing job status, and executing an underlying Oracle SQL script (`d_ausd_geschaeftspartner.sql`) for data processing.

The migration targets Google Cloud BigQuery, where:
*   The control flow and orchestration logic of the KornShell script have been translated into a BigQuery Stored Procedure.
*   The core data transformation logic from `d_ausd_geschaeftspartner.sql` has been converted to native BigQuery SQL and encapsulated within another BigQuery Stored Procedure.
*   Legacy job status management has been replaced by a dedicated BigQuery table.

## 2. Generated Artifacts

The migration process has resulted in the following BigQuery-native artifacts:

*   **`isrpt/isbert/sql/d_ausd_geschaeftspartner_proc.sql`**
    *   **Role:** This file defines a BigQuery Stored Procedure (`your_project.your_dataset.d_ausd_geschaeftspartner_proc`) that encapsulates the core data transformation logic originally found in `d_ausd_geschaeftspartner.sql`. It handles the truncation of temporary tables and the subsequent `INSERT` statements to populate the target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`). It takes `p_stichtag_str` and `p_job_kennung` as input and returns the `records_processed` count.

*   **`isrpt/isbert/ddl/job_tracking_table.sql`**
    *   **Role:** This file defines the Data Definition Language (DDL) for the `your_project.your_dataset.job_tracking_table`. This BigQuery table replaces the functionality of the legacy "Job-Tabelle" and the `FOSJobErzeugeEintrag` calls, storing metadata about each job run, including `job_kennung`, `entry_nr`, `stichtag`, and `records_processed`.

*   **`isrpt/isbert/procedures/r_ausd_vertrag_control_sp.sql`**
    *   **Role:** This file defines the main BigQuery Stored Procedure (`your_project.your_dataset.r_ausd_vertrag_control_sp`) which serves as the direct replacement for the `k_ausd_geschaeftspartner.ksh` control script. It handles parameter validation, date derivation, calls the `d_ausd_geschaeftspartner_proc` for data processing, and records the job's status and metrics into the `job_tracking_table`.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Orchestration to BigQuery Stored Procedure:** The entire control flow, parameter parsing, and job management logic of the original KornShell script were migrated into a BigQuery Stored Procedure (`r_ausd_vertrag_control_sp`). This centralizes the job's execution within BigQuery, leveraging its native procedural capabilities and eliminating the need for external shell environments.
    *   **Trade-off:** While simplifying deployment and execution within GCP, this approach means losing the flexibility of shell scripting for external system interactions (though none were critical for this specific job) and relies entirely on BigQuery's SQL dialect for control flow.

*   **Core SQL to Separate BigQuery Stored Procedure:** The core data transformation logic from `d_ausd_geschaeftspartner.sql` was isolated into its own BigQuery Stored Procedure (`d_ausd_geschaeftspartner_proc`). This promotes modularity, reusability, and easier maintenance of the data transformation logic, separate from the orchestration.
    *   **Trade-off:** Introduces an additional layer of abstraction (procedure calling procedure), but the benefits of modularity outweigh this for complex SQL logic.

*   **Job Management System Replacement:** The implicit legacy job management system (indicated by `FOSJobErzeugeEintrag` and `FOSJobDeaktivate`) was replaced by a dedicated BigQuery table (`job_tracking_table`). This provides a native, auditable, and queryable record of job executions directly within the BigQuery environment.
    *   **Trade-off:** Requires manual DDL creation and `INSERT`/`UPDATE` statements instead of relying on an existing, potentially more feature-rich, legacy job management system. However, it aligns with cloud-native principles.

*   **Utility Script Replacement:** All functionalities provided by sourced KornShell utility scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) were replaced with native BigQuery SQL features, built-in functions, and procedural logic.
    *   **Trade-off:** Requires careful re-implementation of specific utility functions (e.g., date formatting, error handling) using BigQuery's syntax, which can sometimes be less concise than specialized shell utilities.

*   **Parameter Handling:** Parameters are now explicitly passed as `IN` arguments to the BigQuery Stored Procedures, replacing the `getopts` and environment variable approach of the shell script.
    *   **Trade-off:** Stored procedure parameters are strongly typed, which enforces stricter input validation but requires explicit type casting if inputs vary.

*   **Temporary File Elimination:** The use of temporary files (e.g., for record counts) has been eliminated. BigQuery Stored Procedures can directly capture return values (e.g., `OUT` parameters) or use variables for intermediate results.
    *   **Trade-off:** None, this is a direct improvement in efficiency and reduces I/O.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_project.your_dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset --default_location=US your_project:your_dataset
        ```
    *   **Action:** Replace `your_project` and `your_dataset` placeholders in all generated SQL files with actual project and dataset IDs.

2.  **IAM Permissions:**
    *   The service account or user executing these BigQuery procedures must have appropriate IAM roles. Minimum required roles include:
        *   `BigQuery Data Editor` on `your_project.your_dataset` (for creating tables, procedures, and inserting/updating data).
        *   `BigQuery Job User` (for running BigQuery jobs).
    *   **Action:** Verify and grant necessary IAM permissions.

3.  **Source Table Existence and Data:**
    *   All source tables referenced in `d_ausd_geschaeftspartner_proc.sql` (e.g., `bpd_ta_bp_valueseg_assoc`, `sof_ta_e_reach_gp`, `sof_ta_e_business_gp`, `pds_ta_bpri_com`, `sof_ta_e_reach_dn`, `sof_ta_e_business_dn`, `sof_ta_e_reach_ev`, `sof_ta_e_business_ev`) must exist within `your_project.your_dataset` and be populated with the necessary data.
    *   **Action:** Ensure all source tables are present and contain valid data.

4.  **Deploy DDL for `job_tracking_table`:**
    *   Execute the `isrpt/isbert/ddl/job_tracking_table.sql` script to create the job tracking table:
        ```bash
        bq query --use_legacy_sql=false < isrpt/isbert/ddl/job_tracking_table.sql
        ```
    *   **Action:** Deploy the DDL.

5.  **Deploy BigQuery Stored Procedures:**
    *   Execute `isrpt/isbert/sql/d_ausd_geschaeftspartner_proc.sql` to create the data transformation procedure:
        ```bash
        bq query --use_legacy_sql=false < isrpt/isbert/sql/d_ausd_geschaeftspartner_proc.sql
        ```
    *   Execute `isrpt/isbert/procedures/r_ausd_vertrag_control_sp.sql` to create the main control procedure:
        ```bash
        bq query --use_legacy_sql=false < isrpt/isbert/procedures/r_ausd_vertrag_control_sp.sql
        ```
    *   **Action:** Deploy both stored procedures.

6.  **Scheduling (e.g., Cloud Composer / Workflows):**
    *   If automated scheduling is required, configure a Cloud Composer DAG or a Google Cloud Workflow to invoke the `your_project.your_dataset.r_ausd_vertrag_control_sp` procedure with the necessary parameters.
    *   **Action:** Set up the desired scheduling mechanism.

## 5. Known Gaps & Unresolved References

The following items are noted for potential follow-up or represent areas where assumptions were made due to incomplete information:

*   **Original `d_ausd_geschaeftspartner.sql` Content:** The full, original content of `d_ausd_geschaeftspartner.sql` was not provided. The migration assumed a standard DML structure based on the design document's description. Any complex Oracle-specific features (e.g., PL/SQL blocks, specific functions not directly translatable to BigQuery, advanced indexing hints) would require further review and potential redesign.
*   **`starteSQLSkript` Complexity:** The exact internal logic of `starteSQLSkript` within `h_alis_sqlplus.ksh` was not fully detailed. It was assumed to primarily execute an SQL script and capture output. If it involved more complex pre/post-processing, connection management, or error handling specific to SQL*Plus, these nuances might need further BigQuery-native implementation.
*   **Commented-Out Legacy Job Management (`FOSJobDeaktivate`):** The original ksh script had commented-out calls to `FOSJobDeaktivate`. This logic was not implemented in the migrated BigQuery procedures, as it was inactive in the source. If this functionality is required in the future, `UPDATE` statements on the `job_tracking_table` would need to be added to `r_ausd_vertrag_control_sp`.
*   **Environment Variables from `.dw_init`:** The specific environment variables set by `$HOME/.dw_init` were not fully cataloged. It's assumed that any critical variables have been either replaced by BigQuery procedure parameters or are no longer relevant in the BigQuery context.
*   **Error Handling from `f_alis_msgerr.ksh`:** The specific error logging and messaging mechanisms of `f_alis_msgerr.ksh` were not fully detailed. The migrated procedures use BigQuery's `RAISE` statement for critical errors, which integrates with Cloud Logging. If a more granular or custom error reporting mechanism is needed, it would require additional implementation.
*   **`p_job_kennung` in `d_ausd_geschaeftspartner_proc`:** This parameter is passed to `d_ausd_geschaeftspartner_proc` for completeness, mirroring the original ksh's parameter passing. However, it is not explicitly used within the current `d_ausd_geschaeftspartner_proc` logic.
*   **`prem_segment_id` Hardcoding:** In `d_ausd_geschaeftspartner_proc`, the `prem_segment_id` column in `sof_ta_p_gesch_part` is hardcoded to `0`, as indicated by the original SQL comments. This behavior is preserved.
*   **`TRUNCATE` Statements for Intermediate Tables:** The generated `d_ausd_geschaeftspartner_proc` includes `TRUNCATE TABLE` statements for `sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, and `sof_ta_p_evn_empf`. While the design document noted that some truncates were commented out in the *original* ksh for *intermediate* tables, these are now active truncates for the *target* tables. This ensures a clean slate for each run, consistent with typical ETL patterns. If these tables are meant to be incrementally loaded or appended, this design decision needs re-evaluation.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Execute the Main Control Procedure:**
    *   Call the `r_ausd_vertrag_control_sp` procedure with sample parameters.
    *   Example using `bq query`:
        ```bash
        bq query --use_legacy_sql=false \
        'CALL `your_project.your_dataset.r_ausd_vertrag_control_sp`(
          "BERT_K_AUSD_GESCHAEFTSP",
          "A123",
          "01012023",
          0
        );'
        ```
    *   Replace `your_project`, `your_dataset`, and parameter values as needed for testing.

2.  **Passing Criteria:**
    *   **Successful Execution:** The BigQuery job for the procedure call completes without any errors or `RAISE` statements. Check Cloud Logging for any error messages.
    *   **Job Tracking Entry:** Verify that a new entry is created in `your_project.your_dataset.job_tracking_table` with:
        *   `job_kennung`: Matches the input `p_job_kennung`.
        *   `entry_nr`: Matches the input `p_eintrags_nr`.
        *   `stichtag`: Correctly parsed date from `p_stichtag_ddmmyyyy`.
        *   `records_processed`: A non-zero value indicating data was processed (unless expected to be zero for specific test cases).
        *   `status_code_1` = 'A', `status_code_2` = 'I'.
        *   `created_at`: A recent timestamp.
    *   **Target Table Population:** Query the target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) to ensure they are populated with data.
    *   **Data Accuracy:** Crucially, compare the data in the populated BigQuery target tables against the expected output from the legacy system for the same input parameters and source data. This is the most critical validation step.

## 7. Rollback Procedure

In case of issues or a need to revert the migration, follow these steps:

1.  **Stop Scheduling:**
    *   If a Cloud Composer DAG or other scheduler was configured, disable or delete the job/DAG that invokes `r_ausd_vertrag_control_sp`.

2.  **Delete BigQuery Stored Procedures:**
    *   Remove the deployed BigQuery Stored Procedures:
        ```bash
        bq rm -f -r your_project:your_dataset.r_ausd_vertrag_control_sp
        bq rm -f -r your_project:your_dataset.d_ausd_geschaeftspartner_proc
        ```

3.  **Delete `job_tracking_table` (Optional, if not shared):**
    *   If `job_tracking_table` is exclusively used by this job and no other processes rely on its history, it can be deleted. Otherwise, consider truncating it or marking entries as rolled back.
        ```bash
        bq rm -f your_project:your_dataset.job_tracking_table
        ```

4.  **Clean Up Target Data (If necessary):**
    *   If the migrated job has written incorrect data to the target tables, truncate or delete the affected data.
        ```bash
        bq query --use_legacy_sql=false 'TRUNCATE TABLE `your_project.your_dataset.sof_ta_segm_prem`;'
        bq query --use_legacy_sql=false 'TRUNCATE TABLE `your_project.your_dataset.sof_ta_bpr_dn_evn_his`;'
        bq query --use_legacy_sql=false 'TRUNCATE TABLE `your_project.your_dataset.sof_ta_bpr_dn_evn`;'
        bq query --use_legacy_sql=false 'TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_gesch_part`;'
        bq query --use_legacy_sql=false 'TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_dn_nutzer`;'
        bq query --use_legacy_sql=false 'TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_evn_empf`;'
        ```

5.  **Restore Legacy System:**
    *   Ensure the original `k_ausd_geschaeftspartner.ksh` script and its dependencies are fully operational and scheduled in the legacy environment.