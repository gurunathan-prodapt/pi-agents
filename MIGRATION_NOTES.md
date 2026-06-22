# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_barrier.ksh` KornShell script and its associated Oracle SQL script `d_ausd_v_ta_barrier.sql`. The original job orchestrated the population of the `sof$ta_barrier` table in an Oracle database, handling parameter parsing, environment setup, error checking, and SQL execution.

The job has been migrated to **Google BigQuery**. The core orchestration and data transformation logic have been refactored into a BigQuery Stored Procedure, leveraging BigQuery's native SQL capabilities for data processing and a dedicated BigQuery table for job control and logging.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`data_warehouse/ddl/sof_ta_barrier.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the target table `sof_ta_barrier` in BigQuery. This table replaces the `sof$ta_barrier` table from the legacy Oracle environment and will store the transformed `ta_barrier` data.
*   **`data_warehouse/ddl/job_control_log.sql`**
    *   **Role:** Defines the DDL for a new BigQuery table `job_control_log`. This table serves as a centralized mechanism for tracking job execution status, parameters, start/end times, and messages, replacing the implicit job tracking and temporary file handling of the legacy KornShell script.
*   **`data_warehouse/procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** This is the main BigQuery Stored Procedure that encapsulates the entire migrated logic. It combines the orchestration aspects of `k_ausd_v_ta_barrier.ksh` (parameter handling, job control, error logging) with the data transformation and loading logic from `d_ausd_v_ta_barrier.sql`. It performs the `v_datum` calculation, truncates the target table, and inserts transformed data from the ingested source tables into `sof_ta_barrier`.

## 3. Key design decisions

*   **Consolidation into BigQuery Stored Procedure**: The orchestration logic from the KornShell script (`k_ausd_v_ta_barrier.ksh`) and the data transformation logic from the Oracle SQL script (`d_ausd_v_ta_barrier.sql`) were combined into a single BigQuery Stored Procedure (`r_ausd_vertrag_control`). This centralizes the job's logic, reduces cross-platform dependencies, and leverages BigQuery's native capabilities for scripting, error handling, and data manipulation.
*   **Explicit Job Control and Logging**: The implicit job tracking, active job checking, and temporary file usage for record counts in the legacy KornShell script were replaced by an explicit `job_control_log` BigQuery table. This provides better visibility, auditability, and a standardized mechanism for job status management within BigQuery.
*   **External Data Ingestion**: It was decided to pre-ingest all required Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`) into a dedicated `oracle_raw` dataset in BigQuery. This decouples the ingestion process from the transformation logic, allowing for flexible ingestion strategies (e.g., CDC, batch loads) and ensuring data availability within BigQuery.
*   **Oracle SQL to BigQuery SQL Translation**:
    *   Oracle-specific functions like `NVL` were translated to BigQuery's `COALESCE` or `IFNULL`.
    *   `DECODE` statements were translated to BigQuery's `CASE` expressions for improved readability and standard compliance.
    *   `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` was translated to `FORMAT_DATE('%Y%m%d', MAX(m.timecreated))`.
    *   `TO_DATE('&v_datum','YYYYMMDD')` was translated to `PARSE_DATE('%Y%m%d', v_datum)`.
    *   `TRUNCATE TABLE` has a direct BigQuery equivalent.
    *   `GREATEST` function remains the same in BigQuery.
*   **Boolean Type for `ist_stillegung`**: The `ist_stillegung` column, which was represented as `1` or `0` in Oracle, was converted to a native `BOOL` type (`TRUE` or `FALSE`) in BigQuery for better data type adherence and clarity.
*   **Transaction Management**: The BigQuery Stored Procedure utilizes `BEGIN TRANSACTION`, `COMMIT TRANSACTION`, and `ROLLBACK TRANSACTION` with an `EXCEPTION WHEN ERROR` block to ensure atomicity and proper error handling, mirroring the robustness expected from the legacy system.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery datasets `my-project.data_warehouse` and `my-project.oracle_raw` exist.
2.  **Source Data Ingestion**:
    *   Set up and verify the continuous ingestion or replication of the following Oracle tables into `my-project.oracle_raw`:
        *   `isbert_schema.dwtk_meldungen`
        *   `cds$ta_barrier`
        *   `cds$ta_barrier_class`
        *   `cds$ta_barrier_kind`
        *   `cds$ta_care_description`
    *   Confirm that the ingested data is up-to-date and matches the source system.
3.  **DDL Deployment**:
    *   Execute the DDL scripts:
        *   `data_warehouse/ddl/sof_ta_barrier.sql` to create the target table.
        *   `data_warehouse/ddl/job_control_log.sql` to create the job control and logging table.
4.  **Stored Procedure Deployment**:
    *   Execute the `data_warehouse/procedures/r_ausd_vertrag_control.sql` script to create or replace the BigQuery Stored Procedure.
5.  **IAM Permissions**:
    *   Grant the necessary IAM roles to the service account that will execute the BigQuery Stored Procedure. This includes:
        *   `BigQuery Data Editor` on `my-project.data_warehouse` (for `sof_ta_barrier` and `job_control_log`).
        *   `BigQuery Data Viewer` on `my-project.oracle_raw` (for source tables).
        *   `BigQuery Job User` for running BigQuery jobs.
6.  **Scheduling Configuration**:
    *   Configure a scheduling mechanism (e.g., Cloud Composer/Airflow DAG, Google Cloud Workflows, Cloud Scheduler calling a Cloud Function) to invoke the `my-project.data_warehouse.r_ausd_vertrag_control` stored procedure with the required `p_job_kennung` and `p_eintrags_nr` parameters at the appropriate frequency.
7.  **Secrets Management (if applicable for ingestion)**:
    *   If the ingestion process for `oracle_raw` tables requires database credentials, ensure these are securely stored and managed (e.g., using Google Secret Manager).

## 5. Known gaps & unresolved references

The following items were identified as potential gaps or areas requiring further investigation/redesign (B4 items):

*   **Legacy Utility Script Logic**: The full functionality of the sourced KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) has not been exhaustively translated. While core aspects like error logging, date calculation, and parameter handling are covered, any complex OS-level interactions or specific features of `starteSQLSkript` (within `h_alis_sqlplus.ksh`) might require further analysis and potential implementation via Cloud Composer/Workflows if they cannot be encapsulated within BigQuery SQL.
*   **`DWPA_UTIL_SKRIPT.runstatement`**: The Oracle PL/SQL call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_barrier')` was translated to a direct `TRUNCATE TABLE` statement in BigQuery. Any additional logging, auditing, or error handling logic embedded within the `DWPA_UTIL_SKRIPT` package in Oracle is not replicated and would require a separate BigQuery implementation if deemed critical.
*   **`trace.sql.cfg`**: The content and purpose of `trace.sql.cfg` referenced in the original SQL script are unknown. If this file contained critical SQL*Plus settings, logging, or other operational logic, it has not been migrated.
*   **`p_EintragsNr` and `p_JobKennung` Usage**: While `p_job_kennung` is used for the simplified active job check and both parameters are logged, their full lifecycle and meaning within the broader legacy job tracking system might have nuances not fully captured. The current active job check is a basic prevention of concurrent runs for the same `job_name` and `job_kennung`; more sophisticated logic might be needed if `p_eintrags_nr` implies distinct job instances that *can* run concurrently.
*   **`v_carmen` DB Link Scope**: The migration assumes that all `cds$` tables accessed via the `v_carmen` DB link originate from the same Oracle source system and are ingested into the `oracle_raw` dataset. If `v_carmen` pointed to a distinct, separate Oracle instance, that instance would require its own dedicated ingestion pipeline.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Manual Execution**:
    *   Execute the stored procedure manually from the BigQuery console:
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_KENNUNG', 'TEST_ENTRY_NR');
        ```
    *   Monitor the job execution in the BigQuery UI.
2.  **Automated Testing (Recommended)**:
    *   Develop automated tests using tools like Dataform, dbt, or custom Python scripts with the BigQuery client library.
    *   These tests should:
        *   Invoke the stored procedure.
        *   Query the `job_control_log` table to verify the job's status, start/end times, and messages.
        *   Query the `sof_ta_barrier` table to verify data correctness.
3.  **"Passing" Criteria**:
    *   **Successful Completion**: The `job_control_log` table shows an entry for the executed job with `status = 'SUCCESS'` and a meaningful `message`.
    *   **Data Integrity**:
        *   **Record Count**: The `records_processed` count in `job_control_log` should match the expected number of records inserted, ideally compared against the source system's output for the same `v_datum`.
        *   **Data Comparison**: Perform spot checks and, if feasible, a row-by-row comparison of a representative sample of data in `my-project.data_warehouse.sof_ta_barrier` against the corresponding data in the legacy `sof$ta_barrier` table (or a known good snapshot).
        *   **Aggregate Checks**: Verify aggregate values (e.g., `COUNT(*)`, `SUM(numeric_columns)`) for key columns in the target table against the source.
    *   **Functional Equivalence**: All transformation logic (e.g., `sperrgrund` mapping, `sperr_beginn`/`sperr_ende` logic, `ist_stillegung` calculation) should produce identical results to the legacy system.
    *   **Performance**: The job should complete within acceptable timeframes, meeting any defined Service Level Agreements (SLAs).

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Stop BigQuery Scheduler**: Immediately disable or pause the scheduler (e.g., Cloud Composer DAG, Cloud Workflow, Cloud Scheduler job) that invokes the `r_ausd_vertrag_control` BigQuery Stored Procedure.
    *   **Revert to Legacy**: Re-enable the original `k_ausd_v_ta_barrier.ksh` job in the legacy environment. Ensure all necessary Oracle resources and dependencies are still available and functional.
2.  **Data Rollback (if necessary)**:
    *   If the `my-project.data_warehouse.sof_ta_barrier` table was populated with incorrect or corrupted data, it can be truncated:
        ```sql
        TRUNCATE TABLE `my-project.data_warehouse.sof_ta_barrier`;
        ```
    *   If a previous good state of `sof_ta_barrier` is required, restore it from a BigQuery table snapshot, a backup, or re-run a known good version of the procedure if the issue is resolved.
    *   The `job_control_log` table can be reviewed to identify problematic runs, or its entries can be deleted if a clean slate is desired for future runs.
3.  **Code Rollback**:
    *   Delete or revert the BigQuery Stored Procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `my-project.data_warehouse.r_ausd_vertrag_control`;
        ```
    *   If the DDLs for `sof_ta_barrier` or `job_control_log` caused issues, they can be dropped (with caution, as this deletes data) and recreated from a previous version if necessary.
4.  **Dependency Check**:
    *   Verify that the Oracle source systems and the legacy job's environment are fully operational and can resume normal processing.

After rollback, analyze the root cause of the failure, address the issues, and re-test thoroughly before attempting another go-live.