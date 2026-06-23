# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_adressen.ksh` KornShell script and its associated Oracle SQL*Plus script `d_ausd_adressen.sql` to Google Cloud's BigQuery platform.

The original job, `k_ausd_adressen.ksh`, served as an orchestrator, handling environment setup, parameter parsing and validation, date validation, and the execution of `d_ausd_adressen.sql`. The `d_ausd_adressen.sql` script was responsible for processing address-related data for various business partner roles, reading from Oracle source tables (`cds$ta_`, `glv$ta_`, `bpd$ta_`), performing transformations, and populating intermediate and final target tables (`sof$ta_`, `sof$ta_e_`).

The migration translates both the KornShell orchestration logic and the Oracle SQL*Plus data processing logic into BigQuery Standard SQL, primarily utilizing BigQuery Stored Procedures for modularity and execution.

## 2. Generated artifacts

The migration process has generated the following BigQuery-compatible artifacts:

*   **`d_ausd_adressen.bq.sql`**
    *   **Role**: This file defines a BigQuery Stored Procedure named `project.sof.d_ausd_adressen_proc`. It encapsulates the core data processing logic originally found in `d_ausd_adressen.sql`. It performs data extraction from source tables (now assumed to be in BigQuery), applies transformations, and loads data into various `sof.ta_` and `sof.ta_e_` tables. It accepts `p_stichtag` as a parameter to filter data based on validity dates.
*   **`k_ausd_adressen_control.bq.sql`**
    *   **Role**: This file defines a BigQuery Stored Procedure named `project.isbert_schema.k_ausd_adressen_control`. It serves as the primary orchestrator, replacing the original `k_ausd_adressen.ksh` script. Its responsibilities include:
        *   Parsing and validating input parameters (`Jobkennung`, `EintragsNr`, `Stichtag`, `Wiederanlaufwert`).
        *   Performing date format validation for `Stichtag`.
        *   Calling the `project.sof.d_ausd_adressen_proc` procedure to execute the core data processing.
        *   Capturing the record count from the processed data.
        *   Logging job status and details (start, completion, failure) into a dedicated `job_table`.
        *   Handling errors and signaling failures.
*   **`job_table_ddl.sql`**
    *   **Role**: This file contains the Data Definition Language (DDL) for creating the `project.isbert_schema.job_table` in BigQuery. This table is used by `k_ausd_adressen_control.bq.sql` to track the execution status, parameters, and outcomes of the job, replacing the original script's (commented-out) job tracking mechanism.

## 3. Key design decisions

1.  **Consolidated Logic into BigQuery Stored Procedures:**
    *   **Decision**: Both the orchestration logic (`k_ausd_adressen.ksh`) and the data processing logic (`d_ausd_adressen.sql`) are migrated into BigQuery Stored Procedures. `k_ausd_adressen_control` orchestrates by calling `d_ausd_adressen_proc`.
    *   **Rationale**: This approach leverages BigQuery's native capabilities for data manipulation and procedural logic. It simplifies deployment and management by keeping the entire job execution within the BigQuery environment, reducing external dependencies compared to, for example, a Python-based Airflow DAG for this specific job's complexity.
    *   **Trade-offs**: While suitable for this job, more complex orchestration requirements involving external systems or non-SQL logic might still benefit from an external orchestrator like Cloud Composer (Airflow). BigQuery SQL for complex control flow can be more verbose than Python.
2.  **Direct Translation of SQL Logic:**
    *   **Decision**: The DML statements and transformations within `d_ausd_adressen.sql` are directly translated into BigQuery Standard SQL.
    *   **Rationale**: The original SQL script primarily uses standard SQL constructs (SELECT, INSERT, JOIN, WHERE clauses, basic functions). Direct translation is efficient and maintains the original business logic fidelity.
    *   **Trade-offs**: Requires careful mapping of Oracle-specific functions and data types to their BigQuery equivalents. Potential for performance differences that may require post-migration tuning.
3.  **Replacement of Shell Utilities with BigQuery Functions:**
    *   **Decision**: All functionalities provided by external KornShell utility scripts (e.g., date validation, parameter parsing, error reporting) are re-implemented using BigQuery SQL functions and procedural constructs.
    *   **Rationale**: Eliminates dependencies on the legacy shell environment, making the BigQuery solution self-contained and portable within GCP.
    *   **Trade-offs**: Requires re-writing existing, proven utility logic in a new language, which introduces a small risk of functional discrepancies if not thoroughly tested.
4.  **Dedicated BigQuery Table for Job Tracking:**
    *   **Decision**: A new BigQuery table (`project.isbert_schema.job_table`) is introduced to log job execution status and details.
    *   **Rationale**: This replaces the commented-out job tracking functionality in the original `k_ausd_adressen.ksh` and provides a centralized, BigQuery-native mechanism for monitoring job runs.
    *   **Trade-offs**: Requires explicit DDL creation and management for this new table.
5.  **Truncate-and-Load Strategy:**
    *   **Decision**: The `d_ausd_adressen_proc` procedure uses `TRUNCATE TABLE` for all intermediate and final target tables before inserting new data.
    *   **Rationale**: This mirrors the implied behavior of the original Oracle script (using `TRUNCATE TABLE ... REUSE STORAGE`) and simplifies the migration by ensuring idempotency for full refreshes.
    *   **Trade-offs**: This strategy is only suitable for full data refreshes. If incremental loading is required in the future, the DML logic would need to be redesigned (e.g., using `MERGE` statements or `INSERT OVERWRITE`).

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Source Data Ingestion**:
    *   The original Oracle source tables (`cds$ta_`, `glv$ta_`, `bpd$ta_`) must be migrated and continuously synchronized into BigQuery. This is a critical prerequisite. Ensure these BigQuery tables are populated and accessible.
2.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery datasets `project.sof` and `project.isbert_schema` exist in your Google Cloud Project. Replace `project` with your actual Google Cloud Project ID.
        ```bash
        bq mk --dataset project:sof
        bq mk --dataset project:isbert_schema
        ```
3.  **IAM Permissions**:
    *   The Google Cloud service account or user identity that will execute these BigQuery Stored Procedures must have the following IAM roles/permissions:
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the `project.sof` dataset (for `d_ausd_adressen_proc` to write to tables).
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the `project.isbert_schema` dataset (for `k_ausd_adressen_control` to write to `job_table`).
        *   `BigQuery User` (roles/bigquery.user) or `BigQuery Job User` (roles/bigquery.jobUser) to run BigQuery jobs.
        *   `BigQuery Routine Admin` (roles/bigquery.routineAdmin) or `BigQuery Data Editor` (which includes `bigquery.routines.create`) on the respective datasets to create/replace the stored procedures.
4.  **Deploy `job_table_ddl.sql`**:
    *   Execute the `job_table_ddl.sql` script in BigQuery to create the job tracking table. Remember to replace `project.` with your actual project ID.
    ```bash
    bq query --use_legacy_sql=false < job_table_ddl.sql
    ```
5.  **Deploy BigQuery Stored Procedures**:
    *   Execute `d_ausd_adressen.bq.sql` and `k_ausd_adressen_control.bq.sql` in BigQuery to create or replace the stored procedures. Remember to replace `project.` with your actual project ID in both files.
    ```bash
    bq query --use_legacy_sql=false < d_ausd_adressen.bq.sql
    bq query --use_legacy_sql=false < k_ausd_adressen_control.bq.sql
    ```
6.  **Scheduling Integration**:
    *   The upstream invoking script (`r_ausd_adressen.ksh`) needs to be migrated or adapted to call the `k_ausd_adressen_control` BigQuery Stored Procedure. This typically involves:
        *   If `r_ausd_adressen.ksh` is also migrated to BigQuery, it would directly call the procedure.
        *   If `r_ausd_adressen.ksh` is migrated to Cloud Composer (Airflow), a Python DAG would be created to invoke the BigQuery procedure.
        *   Alternatively, a Cloud Scheduler job could be configured to trigger the BigQuery procedure directly or via a Cloud Function.
7.  **Configuration Review**:
    *   Review all `project.` placeholders in the generated SQL files and replace them with your actual Google Cloud Project ID.

## 5. Known gaps & unresolved references

1.  **`r_ausd_adressen.ksh` Migration**: The invoking script `r_ausd_adressen.ksh` is outside the scope of this migration and requires its own migration plan. Its integration with the new BigQuery job must be designed and implemented.
2.  **Commented-Out Job Management in Original Script**: The original `k_ausd_adressen.ksh` contained commented-out lines for `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. The current migration assumes these functionalities are not active or required. If they become necessary, the `k_ausd_adressen_control` procedure and `job_table` schema would need to be extended to support this logic.
3.  **Oracle-Specific Features in `d_ausd_adressen.sql`**: While the generated `d_ausd_adressen.bq.sql` assumes standard SQL, any highly specialized Oracle-specific features (e.g., PL/SQL packages, complex hierarchical queries, specific data types not directly mapped) that might have been present in the original `d_ausd_adressen.sql` would require further investigation and potentially custom BigQuery SQL or alternative processing methods (e.g., Dataflow).
4.  **Performance Tuning**: The initial migration focuses on functional equivalence. Post-migration performance tuning in BigQuery may be required, especially for large datasets or complex queries, to meet desired SLAs.
5.  **`project.` Placeholder**: All instances of `project.` in the generated SQL files must be manually replaced with the actual Google Cloud Project ID where the BigQuery resources reside.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Prepare Test Data**:
    *   Ensure the BigQuery source tables (`project.cds.ta_bp_ref`, `project.glv.ta_country`, `project.glv.ta_description`, `project.bpd.ta_reachability`, `project.bpd.ta_business_partner`, `project.cds.ta_inv_definition`) are populated with representative test data that mirrors the legacy Oracle environment.
    *   Ideally, use a snapshot of production data or carefully crafted synthetic data that covers various scenarios (e.g., different `Stichtag` values, edge cases for validity dates).
2.  **Execute the Orchestration Procedure**:
    *   Call the main orchestration procedure `k_ausd_adressen_control` with test parameters.
    *   Example:
        ```sql
        CALL `project.isbert_schema.k_ausd_adressen_control`(
          'TEST_JOB_KENNUNG',
          'TEST_ENTRY_NR',
          '01012023', -- Example Stichtag (DDMMYYYY)
          0           -- Example Wiederanlaufwert
        );
        ```
    *   Test various parameter combinations, including invalid dates or missing required parameters, to verify error handling.
3.  **Inspect `job_table`**:
    *   Query `project.isbert_schema.job_table` to check the status and details logged for each execution.
    ```sql
    SELECT * FROM `project.isbert_schema.job_table` ORDER BY created_at DESC LIMIT 10;
    ```
4.  **Inspect Target Tables**:
    *   Query the final target tables (`project.sof.ta_e_reach_gp`, `project.sof.ta_e_reach_re`, `project.sof.ta_e_reach_ev`, `project.sof.ta_e_reach_dn`, `project.sof.ta_e_business_gp`, `project.sof.ta_e_business_re`, `project.sof.ta_e_business_ev`, `project.sof.ta_e_business_dn`, `project.sof.ta_e_regulierer`) to verify data content.
    *   Compare record counts and a sample of data rows with the expected output from the legacy system for the same `Stichtag`.

**What "Passing" Means:**

*   **Successful Execution**: The `k_ausd_adressen_control` procedure completes without raising an unhandled error.
*   **Correct Job Status**: The `project.isbert_schema.job_table` contains an entry for the execution with `job_status = 'COMPLETED'` and an appropriate `record_count`. For error scenarios, `job_status` should be `FAILED`.
*   **Parameter Validation**: Invalid input parameters (e.g., missing `Stichtag`, incorrect date format) should result in a `SIGNAL SQLSTATE '45000'` error with the expected error message, and the `job_table` should reflect a `FAILED` status.
*   **Data Accuracy**:
    *   The record counts in the final target tables (`project.sof.ta_e_...`) match the record counts produced by the legacy Oracle job for the same input `Stichtag`.
    *   A statistically significant sample of data rows from the BigQuery target tables matches the corresponding data from the legacy Oracle target tables, ensuring transformations are correctly applied.
*   **Idempotency**: Running the job multiple times with the same parameters should produce the same results in the target tables (due to the `TRUNCATE` statements).

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed to revert to the legacy system:

1.  **Stop New Job Invocations**:
    *   Immediately disable or revert any scheduling mechanisms (e.g., Cloud Scheduler, Cloud Composer DAGs, or modified `r_ausd_adressen.ksh` scripts) that invoke the BigQuery `k_ausd_adressen_control` procedure.
2.  **Re-enable Legacy Job**:
    *   Ensure the original `k_ausd_adressen.ksh` and `d_ausd_adressen.sql` scripts are active and functional in the legacy environment.
    *   Re-enable any legacy scheduling for `r_ausd_adressen.ksh` or `k_ausd_adressen.ksh`.
3.  **Revert BigQuery Stored Procedures**:
    *   Drop the newly deployed BigQuery Stored Procedures to prevent accidental execution.
    ```sql
    DROP PROCEDURE IF EXISTS `project.isbert_schema.k_ausd_adressen_control`;
    DROP PROCEDURE IF EXISTS `project.sof.d_ausd_adressen_proc`;
    ```
4.  **Data Restoration (if necessary)**:
    *   If the BigQuery target tables (`project.sof.ta_...`) were overwritten or corrupted by the new job, restore them from the most recent valid backup. If the `TRUNCATE` strategy was used and no backup exists, data would need to be re-ingested from the source systems.
    *   The `project.isbert_schema.job_table` can be retained for audit purposes or cleared if desired.
5.  **Verify Legacy System**:
    *   Confirm that the legacy `k_ausd_adressen.ksh` job is running successfully and producing correct results in the Oracle environment.