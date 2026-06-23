# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh`. This script, primarily an orchestrator, manages the execution of a core SQL script (`d_ausd_geschaeftspartner.sql`) for business partner data preparation, including parameter parsing, date validation, and record count capture.

The job has been migrated to Google Cloud Platform, leveraging **BigQuery** for all data processing and orchestration logic. The original shell script's control flow, parameter handling, and error management have been translated into a BigQuery Stored Procedure. The core data transformation logic from `d_ausd_geschaeftspartner.sql` has also been converted into a separate BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL artifacts:

*   **`project.dataset.job_log.sql`**
    *   **Role:** This DDL script creates a BigQuery table named `job_log`. This table serves as the central repository for logging job execution metadata, status, input parameters, and record counts. It replaces the legacy system's reliance on temporary files for record counts and implicitly referenced job control tables for status management.
*   **`project.dataset.d_ausd_geschaeftspartner_sp.sql`**
    *   **Role:** This script defines a BigQuery Stored Procedure (`d_ausd_geschaeftspartner_sp`) that encapsulates the core data transformation logic originally found in `d_ausd_geschaeftspartner.sql`. It performs several data manipulation steps, including truncating temporary tables and inserting data into various business partner-related tables within BigQuery. It takes a `DATE` parameter for `stichtag` and returns the `record_count` of the main output table.
*   **`project.dataset.r_ausd_vertrag_control.sql`**
    *   **Role:** This script defines the main BigQuery Stored Procedure (`r_ausd_vertrag_control`) that replaces the `k_ausd_geschaeftspartner.ksh` shell script. It handles parameter parsing and validation, date format checks, job status management (logging to `job_log`), and orchestrates the execution of the `d_ausd_geschaeftspartner_sp` procedure. It also incorporates logic to detect and skip already active job runs, mirroring the original script's behavior.

## 3. Key design decisions

*   **Consolidation into BigQuery Stored Procedures:** The primary design decision was to migrate both the orchestration logic (from `k_ausd_geschaeftspartner.ksh`) and the core data transformation logic (from `d_ausd_geschaeftspartner.sql`) into BigQuery Stored Procedures. This approach centralizes the entire workflow within BigQuery, reducing external dependencies and simplifying deployment and management.
*   **Replacement of File-based Operations with BigQuery Tables:**
    *   **Job Logging:** The legacy system used temporary files and implicitly referenced job control tables (`PoolVertrag`) for job status and record counts. This has been replaced by a dedicated BigQuery `job_log` table, providing structured, queryable, and persistent logging.
    *   **Record Count Capture:** The original script read record counts from a temporary file. In BigQuery, the `d_ausd_geschaeftspartner_sp` directly returns the record count as an `OUT` parameter, which is then captured and logged by the orchestrating `r_ausd_vertrag_control` procedure.
*   **Native BigQuery Functionality for Utilities:** Legacy shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) have been replaced by native BigQuery SQL functions and control flow statements (e.g., `PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB()`, `IF` conditions, `ASSERT` statements, `RAISE USING MESSAGE`). This eliminates the need for external shell execution environments.
*   **Error Handling and Propagation:** Errors are now handled within the BigQuery Stored Procedures using `BEGIN...EXCEPTION` blocks. Critical errors are logged to the `job_log` table and re-raised using `SIGNAL SQLSTATE '45000'` to inform the calling environment (e.g., an orchestrator like Cloud Composer) of the failure.
*   **"Ignore Active Jobs" Logic:** The original script's mechanism to ignore active jobs has been replicated by querying the `job_log` table for existing 'STARTED' entries for the given `JobKennung`, `EintragsNr`, and `Stichtag`. If an active job is found, the procedure logs a 'SKIPPED' status and exits gracefully.
*   **Trade-offs:**
    *   **Increased BigQuery SQL Complexity:** Translating shell scripting logic and external utility calls into BigQuery SQL can lead to more complex stored procedures compared to simple SQL queries. However, this is balanced by the benefits of a fully integrated, serverless data processing environment.
    *   **Dependency on BigQuery Schema:** The migration assumes that all source and target tables referenced in `d_ausd_geschaeftspartner_sp.sql` exist in BigQuery with compatible schemas. This requires a separate, prior migration of these underlying data assets.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `project.dataset` (or the appropriate project and dataset IDs) exists in your Google Cloud project.
2.  **Source and Target Table Migration:**
    *   All source tables referenced in `d_ausd_geschaeftspartner_sp.sql` (e.g., `sof_ta_e_reach_gp`, `bpd_ta_bp_valueseg_assoc`, `pds_ta_bpri_com`, `sof_ta_e_business_gp`, `sof_ta_e_reach_dn`, `sof_ta_e_business_dn`, `sof_ta_e_reach_ev`, `sof_ta_e_business_ev`) must be migrated to BigQuery and reside within the `project.dataset` dataset. Their schemas must be compatible with the SQL logic.
    *   All target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) must also be created in BigQuery within `project.dataset` with appropriate schemas. The `d_ausd_geschaeftspartner_sp` procedure expects to `TRUNCATE` and `INSERT` into these.
3.  **IAM Permissions:**
    *   The service account or user identity that will execute the BigQuery Stored Procedures must have the following IAM roles:
        *   `BigQuery Data Editor` (or equivalent permissions) on `project.dataset` to create/update/delete tables and run procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
4.  **Scheduling Configuration:**
    *   If an external orchestrator (e.g., Cloud Composer/Airflow, Cloud Scheduler, Cloud Functions) is used to invoke `project.dataset.r_ausd_vertrag_control`, it must be configured.
    *   The orchestrator needs to pass the required parameters: `p_JobKennung` (STRING), `p_EintragsNr` (STRING), `p_Stichtag` (STRING in `DDMMYYYY` format), and `p_wiederanlaufWert` (INT64).
5.  **Secrets Management (if applicable):**
    *   Review the original `$HOME/.dw_init` file and `d_ausd_geschaeftspartner.sql` for any hardcoded credentials or sensitive configuration. If found, these should be securely managed using Google Cloud Secret Manager and passed as parameters or environment variables to the orchestrator. (No explicit secrets were identified in the provided code, but this is a general best practice).

## 5. Known gaps & unresolved references

*   **Underlying Table Schemas (B4 Item):** The migration of `d_ausd_geschaeftspartner.sql` to `d_ausd_geschaeftspartner_sp` assumes the existence and compatible schemas of numerous source and target tables (e.g., `sof_ta_e_reach_gp`, `bpd_ta_bp_valueseg_assoc`, `pds_ta_bpri_com`, `sof_ta_p_gesch_part`, etc.) in BigQuery. The DDL for these tables is *not* part of this migration and must be handled separately as a prerequisite.
*   **`r_ausd_vertrag.ksh` Context:** The original script's comment "Kontrollscript zu r_ausd_vertrag.ksh" suggests a relationship with another script. The exact nature of this relationship and potential interdependencies are not fully understood. If `r_ausd_vertrag.ksh` is also an orchestrator or a dependent process, its migration or integration with the new BigQuery procedures needs to be considered.
*   **`BERT_DIR_ROOT` and `DW_DIR_UTL` Resolution:** The original script relied on these environment variables for pathing. In the BigQuery environment, these are replaced by explicit `project.dataset` references. Ensure all such references are correctly mapped.
*   **SQL*Plus Specifics:** While the core SQL has been translated, `d_ausd_geschaeftspartner.sql` might have contained specific SQL*Plus commands or syntax that required careful adaptation. The generated `d_ausd_geschaeftspartner_sp` assumes a direct translation to BigQuery SQL. Any remaining Oracle-specific constructs not fully covered might lead to runtime errors.
*   **Commented-out Code:** The original script contained commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` related to a `PoolVertrag` table. These functionalities are not explicitly re-implemented in the BigQuery procedures, as they were commented out in the source. If these functionalities are required in the future, they would need to be added to the `job_log` table and `r_ausd_vertrag_control` procedure.
*   **Missing `file_complexity` data:** The absence of complexity data for the source script means that potential hidden complexities or edge cases might not have been fully identified during the design phase.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Execute the Orchestrator Stored Procedure:**
    *   Manually call `project.dataset.r_ausd_vertrag_control` from the BigQuery console or via a client tool.
    *   Provide sample input parameters:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`(
            'TEST_JOB',
            '12345',
            '01012023', -- Stichtag in DDMMYYYY format
            1 -- Example wiederanlaufWert
        );
        ```
    *   If an orchestrator (e.g., Cloud Composer) is used, trigger the corresponding DAG or Cloud Function with the appropriate parameters.

2.  **Verify `job_log` Entries:**
    *   Query the `project.dataset.job_log` table:
        ```sql
        SELECT * FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_JOB' ORDER BY created_at DESC;
        ```
    *   **Passing Criteria:**
        *   A `STARTED` entry should appear at the beginning of the run.
        *   A `COMPLETED` entry should appear at the end of a successful run.
        *   The `record_count` in the `COMPLETED` entry should match the expected number of records processed by `d_ausd_geschaeftspartner_sp`.
        *   The `message` field should indicate successful completion.
        *   If the job is run again with the same parameters and the "ignore active jobs" logic is triggered, a `SKIPPED` entry should be logged.
        *   In case of an error, a `FAILED` entry with an informative `message` should be present.

3.  **Verify Target Table Data:**
    *   Query the main target tables populated by `d_ausd_geschaeftspartner_sp`, specifically `project.dataset.sof_ta_p_gesch_part`, `project.dataset.sof_ta_p_dn_nutzer`, and `project.dataset.sof_ta_p_evn_empf`.
    *   **Passing Criteria:**
        *   The tables should contain the expected data for the given `Stichtag`.
        *   The row counts should match the `record_count` reported in the `job_log` for `sof_ta_p_gesch_part` (as this is the table from which the count is taken).
        *   Perform data quality checks to ensure the transformed data is accurate and complete compared to the legacy system's output.

4.  **Error Scenario Testing:**
    *   Test with invalid `Stichtag` formats (e.g., `'2023-01-01'`) to ensure the validation logic correctly catches the error and logs a `FAILED` status.
    *   Test with missing required parameters (e.g., `p_JobKennung` as `NULL` or empty string).

## 7. Rollback procedure

In case of issues or a decision to revert to the legacy system, follow these steps:

1.  **Stop New BigQuery Job Executions:**
    *   If scheduled via Cloud Composer or Cloud Scheduler, disable or pause the corresponding DAGs/jobs.
    *   Ensure no manual executions of `project.dataset.r_ausd_vertrag_control` are initiated.
2.  **Revert to Legacy Job Execution:**
    *   Re-enable or restart the original `k_ausd_geschaeftspartner.ksh` job in the legacy environment.
3.  **Clean Up BigQuery Artifacts (Optional but Recommended):**
    *   **Drop Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_geschaeftspartner_sp`;
        ```
    *   **Drop `job_log` Table:**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        ```
    *   **Revert Target Tables:** If the BigQuery target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) were exclusively created for this migration and are not used by other processes, they can be dropped. If they are shared or contain data from other sources, they should be restored to their state prior to the migration using BigQuery's time travel feature or backups, if necessary.
        *   *Note:* The generated procedures use `TRUNCATE TABLE` before `INSERT`, so a simple re-run of the legacy job and dropping the BigQuery SPs might be sufficient if the target tables are not critical for other BigQuery processes.
4.  **Verify Legacy System Functionality:**
    *   Confirm that the original `k_ausd_geschaeftspartner.ksh` job runs successfully and produces the expected output in the legacy environment.