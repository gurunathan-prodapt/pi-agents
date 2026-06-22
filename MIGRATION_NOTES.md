# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_bp_ref.ksh` from a legacy Unix/Oracle environment to Google Cloud Platform, specifically leveraging BigQuery. The original script served as an orchestration wrapper, managing job execution, parameter parsing, and invoking a core SQL script (`d_ausd_v_ta_bp_ref.sql`) for data processing.

The migration refactors the KornShell logic into a BigQuery Stored Procedure, `r_ausd_vertrag_control`, which handles job control, parameter validation, and error logging. The core business logic from `d_ausd_v_ta_bp_ref.sql` is also migrated into a separate BigQuery Stored Procedure, `d_ausd_v_ta_bp_ref`. Job status, error messages, and execution results (like record counts) are now persisted in dedicated BigQuery tables.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL scripts, which define the necessary tables and stored procedures in the target environment:

*   **`your_gcp_project.your_bq_dataset.job_table.sql`**
    *   **Role:** Defines the DDL for the `job_table` in BigQuery. This table replaces the implicit job tracking and status management previously handled by the KornShell script and its associated utilities. It stores job metadata, status, and timestamps.
*   **`your_gcp_project.your_bq_dataset.error_log.sql`**
    *   **Role:** Defines the DDL for the `error_log` table in BigQuery. This table centralizes error reporting, replacing the functionality of `f_alis_msgerr.ksh` and the shell script's ad-hoc error handling. It captures error details, timestamps, and the procedure where the error occurred.
*   **`your_gcp_project.your_bq_dataset.job_result.sql`**
    *   **Role:** Defines the DDL for the `job_result` table in BigQuery. This table stores key metrics from job executions, specifically the record counts processed by the core business logic. It replaces the use of temporary files (`$DW_DIR_UTL/bert_k_ausd_v_ta_bp_ref_$$.tmp`) in the legacy system.
*   **`your_gcp_project.your_bq_dataset.d_ausd_v_ta_bp_ref.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure `d_ausd_v_ta_bp_ref`. This procedure encapsulates the core business logic originally found in the Oracle SQL script `d_ausd_v_ta_bp_ref.sql`. It performs data transformations and returns the number of processed records.
*   **`your_gcp_project.your_bq_dataset.r_ausd_vertrag_control.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure `r_ausd_vertrag_control`. This is the main control procedure, replacing the `k_ausd_v_ta_bp_ref.ksh` KornShell script. It handles parameter validation, updates job status in `job_table`, calls `d_ausd_v_ta_bp_ref` for business logic execution, records results in `job_result`, and logs errors in `error_log`.

## 3. Key design decisions

*   **KornShell to BigQuery Stored Procedure Refactoring:** The entire orchestration logic of the original KornShell script, including parameter parsing, conditional execution, and job status management, has been translated into a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This leverages BigQuery's native scripting capabilities, eliminating the need for external shell environments.
*   **Direct BigQuery SQL for Business Logic:** The core data processing logic from `d_ausd_v_ta_bp_ref.sql` (originally Oracle SQL) is migrated into a separate BigQuery Stored Procedure (`d_ausd_v_ta_bp_ref`). This removes the dependency on `sqlplus` and an Oracle database, allowing for native BigQuery execution.
*   **Centralized Job Control and Logging Tables:** Instead of disparate shell variables, temporary files, and implicit job tracking, dedicated BigQuery tables (`job_table`, `error_log`, `job_result`) are introduced. This provides a structured, queryable, and persistent record of job execution, status, errors, and results.
*   **Parameter Handling via Procedure Arguments:** The `getopts` mechanism from KornShell is replaced by direct input parameters to the BigQuery Stored Procedures, simplifying parameter validation and ensuring type safety.
*   **Elimination of Temporary Files:** The use of temporary files for passing record counts between script components is replaced by `OUT` parameters in BigQuery Stored Procedures and subsequent insertion into the `job_result` table, ensuring atomicity and persistence.
*   **Consolidation of Utility Script Functionality:** The logic from sourced KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) is absorbed and re-implemented directly within the BigQuery Stored Procedures using standard SQL constructs (e.g., `IF...THEN`, `INSERT` into `error_log`).
*   **Error Handling with `RAISE` and `error_log`:** Shell-based error codes and `f_alis_msgerr.ksh` are replaced by BigQuery's `RAISE BQ.ABORT_TRANSACTION` for immediate termination and `INSERT` statements into the `error_log` table for persistent error tracking.
*   **Assumption of Oracle to BigQuery Table Migration:** The migration assumes that all source tables referenced in the original `d_ausd_v_ta_bp_ref.sql` (e.g., `dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`) have been or will be migrated to BigQuery with compatible schemas.

## 4. Manual steps before go-live

Before deploying and running the migrated BigQuery procedures, the following manual steps are required:

1.  **Create BigQuery Dataset:**
    *   Ensure the target BigQuery dataset (`your_bq_dataset`) exists within `your_gcp_project`. If not, create it.
    *   `bq mk --dataset your_gcp_project:your_bq_dataset`
2.  **Deploy DDL for Control Tables:**
    *   Execute the DDL scripts for the control tables:
        *   `your_gcp_project.your_bq_dataset.job_table.sql`
        *   `your_gcp_project.your_bq_dataset.error_log.sql`
        *   `your_gcp_project.your_bq_dataset.job_result.sql`
    *   These can be run via the BigQuery UI, `bq query`, or `gcloud bigquery jobs query`.
3.  **Migrate Source Data Tables:**
    *   **Crucial Step:** All tables referenced by the original `d_ausd_v_ta_bp_ref.sql` (e.g., `dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`) must be migrated from Oracle to BigQuery. This includes creating their DDL in BigQuery and loading the historical data. The generated `d_ausd_v_ta_bp_ref.sql` procedure assumes these tables exist in `your_gcp_project.your_bq_dataset` (e.g., `your_gcp_project.your_bq_dataset.dwtk_meldungen`).
    *   Review and adjust data types and column names as necessary during this migration.
4.  **IAM Permissions:**
    *   The service account or user identity that will execute these BigQuery procedures must have appropriate IAM roles.
    *   Minimum required roles: `BigQuery Data Editor` (for `INSERT`, `UPDATE`, `TRUNCATE` on `job_table`, `error_log`, `job_result`, and `sof_ta_bp_ref`) and `BigQuery Data Viewer` (for `SELECT` on `dwtk_meldungen`, `cds_ta_bp_ref`).
    *   For procedure creation, `BigQuery Data Editor` or `BigQuery Admin` is needed.
5.  **Replace Placeholders:**
    *   Before deployment, replace all instances of `your_gcp_project` and `your_bq_dataset` in the generated SQL files with your actual GCP project ID and BigQuery dataset name.
6.  **Scheduling (Optional, but recommended):**
    *   If the job needs to run on a schedule, configure a BigQuery Scheduled Query, a Cloud Composer (Airflow) DAG, or a Cloud Workflows definition to call the `your_gcp_project.your_bq_dataset.r_ausd_vertrag_control` procedure with the required parameters.

## 5. Known gaps & unresolved references

The migration design and generated code address the known components, but the following items were flagged for follow-up or require further analysis:

*   **Content of `d_ausd_v_ta_bp_ref.sql` (Original Oracle Script):** The exact business logic and Oracle-specific syntax within the original `d_ausd_v_ta_bp_ref.sql` were not fully available during the design phase. The generated `d_ausd_v_ta_bp_ref` BigQuery procedure provides a best-effort translation, but a thorough review by a domain expert is required to ensure:
    *   All Oracle-specific functions (e.g., `TO_DATE`, `NVL`) are correctly translated to BigQuery equivalents.
    *   Data type mappings between Oracle and BigQuery are accurate.
    *   All referenced tables (e.g., `dwtk_meldungen`, `cds$ta_bp_ref`, `sof$ta_bp_ref`) are correctly migrated and accessible in BigQuery.
    *   The `&v_carmen` variable in the original SQL, likely an Oracle SQL*Plus substitution variable for a database link or schema, has been removed. The BigQuery procedure assumes direct access to `cds_ta_bp_ref` within the same project/dataset. If `cds_ta_bp_ref` resides in a different project/dataset, the table reference will need to be updated (e.g., `other_project.other_dataset.cds_ta_bp_ref`).
    *   The `isbert_schema` prefix in the original Oracle SQL has been replaced with the target BigQuery dataset.
*   **Original Job Table Schema:** The precise schema and usage patterns of the original "job tracking table" were inferred. The `job_table` DDL is a reasonable approximation, but it should be validated against the legacy system's actual table definition to ensure all relevant columns and behaviors are captured.
*   **`DWMSG_MeldeFehler` Implementation:** The `f_alis_msgerr.ksh` script's `DWMSG_MeldeFehler` function might have involved more complex logging, alerting, or integration with other systems beyond simple table inserts. The current `error_log` table provides persistent storage, but if real-time alerts (e.g., via Pub/Sub, Cloud Monitoring) were part of the original functionality, these would need to be implemented separately.
*   **Transaction Management:** BigQuery DML operations are typically auto-committed. While the generated procedures handle errors with `RAISE BQ.ABORT_TRANSACTION`, complex multi-statement logic within `d_ausd_v_ta_bp_ref` might require explicit transaction blocks (`BEGIN TRANSACTION; ... COMMIT TRANSACTION; ... ROLLBACK TRANSACTION;`) if strict atomicity across multiple DML statements is critical.

## 6. Validation

To validate the successful migration and functionality of the BigQuery procedures:

1.  **Prerequisites:**
    *   All DDLs for `job_table`, `error_log`, `job_result`, and the migrated source tables (e.g., `dwtk_meldungen`, `cds_ta_bp_ref`, `sof_ta_bp_ref`) must be deployed.
    *   The BigQuery Stored Procedures `d_ausd_v_ta_bp_ref` and `r_ausd_vertrag_control` must be created.
    *   Sample data should be loaded into the source tables (`dwtk_meldungen`, `cds_ta_bp_ref`) that mimics production scenarios, including edge cases.

2.  **Execution Steps:**
    *   Execute the main control procedure using a BigQuery query:
        ```sql
        CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control(
            p_JobKennung => 'TEST_JOB_K_AUSD_V_TA_BP_REF',
            p_EintragsNr => '12345'
        );
        ```
    *   Run with different `p_JobKennung` and `p_EintragsNr` values to test job deactivation logic.
    *   Test error conditions by providing invalid or missing parameters (e.g., `p_JobKennung => NULL`).
    *   Test scenarios where the underlying `d_ausd_v_ta_bp_ref` procedure might encounter errors (e.g., by temporarily introducing a syntax error in `d_ausd_v_ta_bp_ref` for testing purposes, then reverting).

3.  **"Passing" Criteria:**
    *   **Successful Execution:** The `CALL` statement completes without BigQuery reporting a general execution error.
    *   **`job_table` Updates:**
        *   For a successful run: The `job_table` should contain an entry for `('TEST_JOB_K_AUSD_V_TA_BP_REF', '12345')` with `active_flag = FALSE`, `status = 'COMPLETED'`, and valid `start_time`/`end_time`.
        *   If a previous job with the same `p_JobKennung` but different `p_EintragsNr` was active, its `active_flag` should be set to `FALSE` and `status` to `'DEACTIVATED'`.
        *   For a failed run: The `job_table` entry should show `active_flag = FALSE`, `status = 'FAILED'`, and `message` containing the error details.
    *   **`job_result` Entry:** For a successful run, the `job_result` table should contain an entry for `('TEST_JOB_K_AUSD_V_TA_BP_REF', '12345')` with an accurate `record_count` reflecting the number of rows inserted by `d_ausd_v_ta_bp_ref`.
    *   **`error_log` Entries:**
        *   For a successful run: The `error_log` table should have no new entries related to this job execution.
        *   For a failed run (e.g., missing parameters, error in `d_ausd_v_ta_bp_ref`): The `error_log` table should contain relevant entries with `error_ts`, `error_nr`, `error_arg`, and `procedure_name` (`r_ausd_vertrag_control` or `d_ausd_v_ta_bp_ref`).
    *   **Data Integrity in `sof_ta_bp_ref`:** After `d_ausd_v_ta_bp_ref` executes, query `your_gcp_project.your_bq_dataset.sof_ta_bp_ref` to verify that the data inserted matches the expected output based on the original `d_ausd_v_ta_bp_ref.sql` logic and the sample input data. Compare record counts and data content.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Stop New Executions:** Immediately halt any scheduled executions (e.g., BigQuery Scheduled Queries, Cloud Composer DAGs) that invoke `your_gcp_project.your_bq_dataset.r_ausd_vertrag_control`.
2.  **Revert to Legacy System:** Re-enable the original KornShell script (`k_ausd_v_ta_bp_ref.ksh`) and its associated scheduling mechanisms in the legacy environment.
3.  **Delete BigQuery Procedures:**
    *   Drop the migrated BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS your_gcp_project.your_bq_dataset.r_ausd_vertrag_control;
        DROP PROCEDURE IF EXISTS your_gcp_project.your_bq_dataset.d_ausd_v_ta_bp_ref;
        ```
4.  **Delete BigQuery Control Tables (Optional, but recommended for clean rollback):**
    *   Drop the newly created control tables. Be aware that this will delete all historical job tracking, error logs, and results from the migrated system.
        ```sql
        DROP TABLE IF EXISTS your_gcp_project.your_bq_dataset.job_table;
        DROP TABLE IF EXISTS your_gcp_project.your_bq_dataset.error_log;
        DROP TABLE IF EXISTS your_gcp_project.your_bq_dataset.job_result;
        ```
5.  **Data Implications:**
    *   If the `d_ausd_v_ta_bp_ref` procedure modified data in `your_gcp_project.your_bq_dataset.sof_ta_bp_ref` (e.g., `TRUNCATE` and `INSERT`), this data will remain in BigQuery. Depending on the rollback strategy, this table might need to be truncated or restored from a backup if the legacy system needs to re-process the data.
    *   Ensure that the legacy system can safely resume operations without conflicts from any data modifications made in BigQuery during the brief migration period.