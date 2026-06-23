# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_geschaeftspartner.ksh` and its associated core logic (`k_ausd_geschaeftspartner.ksh` and `d_ausd_geschaeftspartner.sql`) from a legacy Unix/Oracle environment to Google Cloud Platform's BigQuery.

The original script served as an orchestration wrapper for provisioning contract caches for the Forderungsscoring (FOS) system, handling parameter parsing, error logging, and delegating core data generation. The migration re-implements this functionality using BigQuery Stored Procedures for both orchestration and core data transformation, leveraging BigQuery tables for logging and auditing.

**Target Platform:** Google BigQuery

## 2. Generated Artifacts

The migration process has resulted in the following BigQuery artifacts:

*   **`project.dataset.sp_initial_befuellung_vertrags_cache_fos` (BigQuery Stored Procedure)**
    *   **Role:** This stored procedure serves as the primary orchestration wrapper, directly replacing the functionality of `r_ausd_geschaeftspartner.ksh`. It handles parameter parsing (`p_stichtag`, `p_wiederanlaufWert`), sets default values, performs initial validation, manages job logging and registration, and orchestrates the call to the core data transformation logic.
    *   **Source:** Derived from the pseudocode provided in the `MIGRATION_DESIGN_DOCUMENT`.

*   **`project.dataset.sp_ausd_geschaeftspartner` (BigQuery Stored Procedure)**
    *   **Role:** This stored procedure encapsulates the core data extraction and transformation logic, directly replacing the combined functionality of `k_ausd_geschaeftspartner.ksh` and `d_ausd_geschaeftspartner.sql`. It performs data manipulation, temporary table operations (truncation, insertion), and specific business logic to prepare data for the FOS system.
    *   **Source:** Generated from the analysis of `k_ausd_geschaeftspartner.ksh` and `d_ausd_geschaeftspartner.sql` as provided in the `GENERATED MIGRATION CODE` section.

*   **`project.dataset.job_log` (BigQuery Table)**
    *   **Role:** Replaces file-based logging. Stores detailed job execution events, informational messages, warnings, and errors for auditing and troubleshooting.
    *   **Schema:** `job_kennung STRING`, `eintragsnr INT64`, `event_ts TIMESTAMP`, `level STRING`, `errnr INT64`, `errarg STRING`, `message STRING`.

*   **`project.dataset.job_registry` (BigQuery Table)**
    *   **Role:** Replaces job status tracking and metadata storage. Records the start and end times, parameters, and overall status of each job run.
    *   **Schema:** `job_kennung STRING`, `created_ts TIMESTAMP`, `finished_ts TIMESTAMP`, `stichtag STRING`, `sysdate STRING`, `restart_value INT64`, `status STRING`.

*   **Temporary/Target Tables (BigQuery Tables)**
    *   **Role:** These tables are created or utilized by `sp_ausd_geschaeftspartner` for intermediate processing and as final output for the FOS system. They include:
        *   `project.dataset.sof$ta_segm_prem`
        *   `project.dataset.sof$ta_bpr_dn_evn`
        *   `project.dataset.sof$ta_bpr_dn_evn_his`
        *   `project.dataset.sof$ta_p_gesch_part`
        *   `project.dataset.sof$ta_p_dn_nutzer`
        *   `project.dataset.sof$ta_p_evn_empf`
    *   **Source:** Referenced in the generated `sp_ausd_geschaeftspartner` code.

## 3. Key Design Decisions

*   **KornShell to BigQuery Stored Procedure Migration:** The primary orchestration logic, previously handled by `r_ausd_geschaeftspartner.ksh`, has been directly translated into a BigQuery stored procedure (`sp_initial_befuellung_vertrags_cache_fos`). This leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, eliminating the need for an external compute environment for the orchestrator itself.
*   **Separation of Concerns:** The migration maintains the original separation between orchestration (`r_ausd_geschaeftspartner.ksh`) and core business logic (`k_ausd_geschaeftspartner.ksh`). This is reflected in the creation of two distinct BigQuery stored procedures: `sp_initial_befuellung_vertrags_cache_fos` for orchestration and `sp_ausd_geschaeftspartner` for the core data transformation. This modularity improves readability, maintainability, and reusability.
*   **BigQuery Native Logging:** File-based logging and status updates from the original KornShell script have been replaced with inserts into dedicated BigQuery tables (`job_log`, `job_registry`). This centralizes logging, enables easier querying and analysis of job execution history, and integrates seamlessly with BigQuery's ecosystem.
*   **Parameter Handling:** Shell `getopts` parameter parsing is replaced by explicit `IN` parameters in the BigQuery stored procedures. Default values and validation logic are implemented using BigQuery SQL constructs (`IF`, `COALESCE`).
*   **Core Logic Reimplementation:** The complex SQL logic embedded within `k_ausd_geschaeftspartner.ksh` (specifically `d_ausd_geschaeftspartner.sql`) has been directly translated into BigQuery SQL within `sp_ausd_geschaeftspartner`. This includes `INSERT` statements, `JOIN` conditions, `CASE` expressions, and date manipulations, ensuring functional equivalence.
*   **Handling of Shell Utilities:** Generic shell utilities for error messaging (`f_alis_msgerr.ksh`), parameter handling (`h_alis_parameter.ksh`), and date operations (`h_alis_date.ksh`) have been replaced by BigQuery's built-in functions (e.g., `FORMAT_DATE`, `CURRENT_DATE`, `PARSE_DATE`) and custom logic within the stored procedures.
*   **Trade-offs - Signal Handling (`trap`):** The `trap` statements used in the original KornShell script for robust error and interrupt management cannot be directly replicated in BigQuery SQL. Error handling within the stored procedures relies on `RAISE` statements and BigQuery's transaction model. For more sophisticated retry mechanisms or external signal handling, an external orchestrator (e.g., Cloud Composer) would be required, which is considered optional for this migration phase.
*   **Trade-offs - `v_carmen` reference:** The `&v_carmen` reference in `Step03` of the original SQL, likely an Oracle database link or alias, has been removed and replaced with a direct table reference (`project.dataset.bpd$ta_bp_valueseg_assoc`). This assumes the referenced table is now directly available within the BigQuery project/dataset.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset `project.dataset` exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```

2.  **BigQuery Table Creation (DDL):**
    *   **Logging and Registry Tables:**
        ```sql
        CREATE TABLE `project.dataset.job_log` (
          job_kennung STRING,
          eintragsnr INT64,
          event_ts TIMESTAMP,
          level STRING,
          errnr INT64,
          errarg STRING,
          message STRING
        );

        CREATE TABLE `project.dataset.job_registry` (
          job_kennung STRING,
          created_ts TIMESTAMP,
          finished_ts TIMESTAMP,
          stichtag STRING,
          sysdate STRING,
          restart_value INT64,
          status STRING
        );
        ```
    *   **Source and Target Tables for `sp_ausd_geschaeftspartner`:**
        *   All source tables referenced in `sp_ausd_geschaeftspartner` (e.g., `dwtk_meldungen`, `bpd$ta_bp_valueseg_assoc`, `sof$ta_e_reach_gp`, `sof$ta_e_business_gp`, `pds$ta_bpri_com`, `sof$ta_e_reach_dn`, `sof$ta_e_business_dn`, `sof$ta_e_reach_ev`, `sof$ta_e_business_ev`) must be created in `project.dataset` with their respective schemas and populated with data.
        *   All temporary/target tables (e.g., `sof$ta_segm_prem`, `sof$ta_bpr_dn_evn`, `sof$ta_bpr_dn_evn_his`, `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`) must be created in `project.dataset` with their respective schemas.
        *   **Note:** The exact schemas for these tables are not provided in the design document or generated code, but can be inferred from the `INSERT` statements. These DDLs must be generated and applied.

3.  **IAM / Permissions:**
    *   The service account or user executing the stored procedures must have the following BigQuery permissions:
        *   `bigquery.datasets.get`
        *   `bigquery.tables.create`, `bigquery.tables.get`, `bigquery.tables.updateData`, `bigquery.tables.delete` (for temporary tables and `TRUNCATE`)
        *   `bigquery.routines.create`, `bigquery.routines.get`, `bigquery.routines.update` (for deploying the stored procedures)
        *   `bigquery.jobs.create` (to run queries and stored procedures)
        *   Specific `bigquery.tables.getData` on all source tables.
        *   Specific `bigquery.tables.updateData` on all target tables.

4.  **Connection Strings / Secrets:**
    *   No direct connection strings or secrets are required within the BigQuery stored procedures themselves, as they operate natively within BigQuery.
    *   If an external orchestrator (e.g., Cloud Composer, Workflows) is used to trigger the BigQuery stored procedure, ensure the orchestrator's service account has the necessary BigQuery permissions.

5.  **Scheduling:**
    *   Configure a scheduling mechanism to invoke `project.dataset.sp_initial_befuellung_vertrags_cache_fos`.
    *   **Options:**
        *   **Cloud Scheduler:** For simple time-based scheduling, trigger a Cloud Function or Cloud Run service that then calls the BigQuery stored procedure.
        *   **Cloud Composer (Apache Airflow):** For complex workflows, dependencies, retries, and monitoring, create an Airflow DAG that uses the `BigQueryExecuteStoredProcedureOperator`.
        *   **Workflows:** For event-driven or sequential task orchestration.
    *   Ensure the scheduler passes the required parameters (`p_stichtag`, `p_wiederanlaufWert`) to the stored procedure.

## 5. Known Gaps & Unresolved References

While the core logic has been migrated, the following items are noted for follow-up or represent inherent differences in the target platform:

*   **Orchestration Wrapper (`sp_initial_befuellung_vertrags_cache_fos`) Implementation:** The `MIGRATION_DESIGN_DOCUMENT` provided pseudocode for this wrapper. While its functionality is clear, the final, production-ready BigQuery SQL implementation of this procedure needs to be formally generated and reviewed.
*   **Shell `trap` Statements:** The original KornShell script used `trap` for robust signal handling (e.g., `INT`, `STOP`, `ERR`). BigQuery stored procedures do not have an equivalent mechanism. Error handling is managed via `RAISE` and BigQuery's transactional behavior. For advanced error recovery (e.g., retries on transient errors, custom cleanup on specific signals), an external orchestrator (like Cloud Composer) would be necessary to wrap the BigQuery stored procedure call.
*   **`v_carmen` Reference Removal:** In `Step03` of `sp_ausd_geschaeftspartner`, the `&v_carmen` reference (likely an Oracle DB link/alias) was removed and replaced with a direct table reference (`project.dataset.bpd$ta_bp_valueseg_assoc`). This assumes `bpd$ta_bp_valueseg_assoc` is now directly available in the BigQuery dataset. This change should be explicitly confirmed with the business/data owners to ensure data source integrity.
*   **`dwtk_meldungen` Table and `v_datum` Calculation:** The `sp_ausd_geschaeftspartner` procedure relies on a `dwtk_meldungen` table to determine `v_datum`. This table and its `timecreated` and `job_kennung` columns must be correctly migrated and populated in BigQuery to ensure the `v_datum` logic functions as intended. The specific `job_kennung = 'BERT_DROP_TEMP_TABLE'` used for `MAX(m.timecreated)` needs to be understood and validated.
*   **Commented-out `TRUNCATE TABLE` Statements:** The original `d_ausd_geschaeftspartner.sql` contained commented-out `TRUNCATE TABLE` statements for `sof$ta_bpr_dn_evn_his`, `sof$ta_segm_prem`, and `sof$ta_bpr_dn_evn`. The generated `sp_ausd_geschaeftspartner` does not include these truncations, following the commented-out status in the source. This behavior should be explicitly confirmed to ensure it aligns with the desired data retention and cleanup strategy.
*   **`DW_EintragsNr` Management:** The `DW_EintragsNr` is dynamically determined in the orchestration layer. While the BigQuery `job_registry` table provides a mechanism for this, ensuring its uniqueness and proper sequencing across concurrent job runs should be verified, especially if multiple instances of this job (or other jobs using the same registry) could run simultaneously.

## 6. Validation

Validation of the migrated job involves executing the BigQuery stored procedures and verifying their output and behavior.

1.  **Deploy Stored Procedures:**
    *   Ensure both `sp_initial_befuellung_vertrags_cache_fos` and `sp_ausd_geschaeftspartner` are deployed to `project.dataset`.

2.  **Execute the Orchestration Procedure:**
    *   Call the main orchestration procedure, providing test parameters for `p_stichtag` and `p_wiederanlaufWert`.
    *   **Example:**
        ```sql
        CALL `project.dataset.sp_initial_befuellung_vertrags_cache_fos`('01012023', 0);
        -- Or with default values:
        CALL `project.dataset.sp_initial_befuellung_vertrags_cache_fos`(NULL, NULL);
        ```

3.  **Verification Steps:**

    *   **Log Verification:**
        *   Query `project.dataset.job_log` for the `job_kennung` and `eintragsnr` corresponding to the test run.
        *   **Passing Criteria:**
            *   No entries with `level = 'ERROR'` should be present unless an expected error condition was deliberately tested.
            *   All expected `INFO` messages, including start/end messages and step-by-step progress, should be present.
            *   The `message` content should accurately reflect the execution flow and parameter values.

    *   **Job Registry Verification:**
        *   Query `project.dataset.job_registry` for the `job_kennung` and `eintragsnr`.
        *   **Passing Criteria:**
            *   The `status` column should be `OK` (or the equivalent success status).
            *   `created_ts` and `finished_ts` should be populated correctly.
            *   `stichtag` and `restart_value` should match the input parameters.

    *   **Target Data Verification:**
        *   Query the target tables populated by `sp_ausd_geschaeftspartner` (e.g., `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`).
        *   **Passing Criteria:**
            *   **Row Counts:** Compare row counts in the migrated target tables with the row counts from the original system for the same `Stichtag` and `WiederanlaufWert`.
            *   **Data Samples:** Select random samples of data from the target tables and compare them field-by-field with corresponding data generated by the original system. Pay close attention to `CASE` statements, `COALESCE` functions, and date transformations.
            *   **Edge Cases:** Test with various `p_stichtag` values (e.g., current date, historical date, dates with no data) and `p_wiederanlaufWert` values (e.g., 0, a specific ID).
            *   **Error Conditions:** Test with invalid input parameters (e.g., malformed `p_stichtag`) and verify that the error handling (`RAISE` and `job_log` entries) behaves as expected.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Stop New Invocations:**
    *   Immediately disable or remove the BigQuery job from its scheduler (e.g., Cloud Scheduler, Cloud Composer DAG, Workflows). This prevents any further execution of the migrated stored procedures.

2.  **Revert Scheduling:**
    *   Re-enable the original scheduling mechanism for `r_ausd_geschaeftspartner.ksh` in the legacy environment.

3.  **Clean Up BigQuery Target Tables (Optional but Recommended):**
    *   If the migrated job produced incorrect data, truncate or delete data from the target tables in BigQuery that were populated by the failed run.
    *   **Example (for a specific job run identified by `eintragsnr`):**
        ```sql
        DELETE FROM `project.dataset.sof$ta_p_gesch_part` WHERE job_eintragsnr = <failed_eintragsnr>;
        -- Repeat for other target tables
        ```
    *   **Caution:** Ensure you only delete data related to the problematic run. If the tables are fully overwritten on each run, a simple `TRUNCATE TABLE` before the next successful run might suffice.

4.  **Delete BigQuery Stored Procedures:**
    *   Remove the deployed BigQuery stored procedures to prevent accidental invocation.
    *   ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_initial_befuellung_vertrags_cache_fos`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_ausd_geschaeftspartner`;
        ```

5.  **Retain Logging and Registry Tables:**
    *   The `job_log` and `job_registry` tables should generally be retained for post-mortem analysis and auditing, even during a rollback. Do not delete these tables unless specifically instructed.

6.  **Verify Legacy System Operation:**
    *   Confirm that the original `r_ausd_geschaeftspartner.ksh` job is running successfully and producing correct output in the legacy environment.