# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh`, which orchestrates the dropping of temporary tables, has been migrated from its legacy environment to Google Cloud's BigQuery platform.

The original script's functionality, including parameter parsing, validation, date derivation, and the execution of the underlying SQL logic (`d_drop_temp_table.sql`), has been re-platformed into a BigQuery Stored Procedure named `dataset.r_drop_temp_table_control`. Auxiliary functions like error logging and job control have been replaced with dedicated BigQuery tables (`dataset.job_error_log` and `dataset.job_table`).

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`bigquery/ddl/isrpt/isbert/job_error_log.sql`**
    *   **Role:** This DDL script creates the `dataset.job_error_log` table in BigQuery. This table serves as the centralized logging mechanism for errors encountered during the execution of the migrated BigQuery Stored Procedures, replacing the file-based and console logging of the original KornShell script.
*   **`bigquery/ddl/isrpt/isbert/job_table.sql`**
    *   **Role:** This DDL script creates the `dataset.job_table` table in BigQuery. This table is intended to replace the legacy job control table interactions (e.g., `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) if the commented-out job management logic from the original KornShell script is reactivated.
*   **`bigquery/isrpt/isbert/aufbereitung/d_drop_temp_table.sql`**
    *   **Role:** This BigQuery Stored Procedure (`dataset.d_drop_temp_table`) is a placeholder for the core SQL logic originally contained in `d_drop_temp_table.sql`. It is designed to accept parameters from the control procedure and perform the actual `DROP TABLE` or `TRUNCATE TABLE` operations. **Note: This procedure currently contains placeholder logic and requires the actual implementation from the original `d_drop_temp_table.sql` file.**
*   **`bigquery/isrpt/isbert/aufbereitung/r_drop_temp_table_control.sql`**
    *   **Role:** This BigQuery Stored Procedure (`dataset.r_drop_temp_table_control`) is the main entry point for the migrated job. It replaces the `k_drop_temp_table.ksh` script, handling parameter validation, date derivations, error logging, and orchestrating the call to `dataset.d_drop_temp_table` for the actual table dropping operations.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures:** The primary decision was to re-platform the entire shell-orchestrated SQL process into native BigQuery Stored Procedures. This leverages BigQuery's built-in capabilities for SQL execution, parameter handling, and error management, eliminating the need for external shell scripting and `sqlplus` interactions.
*   **Consolidation of Logic:** The orchestration logic (parameter parsing, validation, date derivation) from `k_drop_temp_table.ksh` and the core SQL logic from `d_drop_temp_table.sql` are separated into two BigQuery Stored Procedures (`r_drop_temp_table_control` and `d_drop_temp_table` respectively). This maintains a clear separation of concerns while keeping the entire process within the BigQuery environment.
*   **Native BigQuery Constructs for Utilities:** All external KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `gestern.ksh`) have been replaced with equivalent BigQuery SQL functions and scripting constructs (e.g., `SAFE.PARSE_DATE`, `CURRENT_DATE()`, `INSERT` into logging tables, `SIGNAL SQLSTATE`). This reduces external dependencies and simplifies the architecture.
*   **Dedicated Logging and Job Control Tables:** File-based logging and implicit job control mechanisms were replaced with explicit BigQuery tables (`dataset.job_error_log`, `dataset.job_table`). This provides a structured, queryable, and centralized way to monitor job execution and status.
*   **Parameter Handling:** Command-line arguments from the original script are directly translated into input parameters for the BigQuery Stored Procedure, ensuring a clear interface. Temporary files for intermediate results (like record counts) are replaced by BigQuery `DECLARE` variables or `INOUT` parameters.
*   **Trade-offs:**
    *   **Initial Implementation Effort:** Requires a complete rewrite of the shell logic into BigQuery SQL scripting.
    *   **Dependency on `d_drop_temp_table.sql` content:** The exact logic of the original `d_drop_temp_table.sql` was not available, leading to a placeholder procedure. This requires manual completion and verification.
    *   **Loss of direct file system interaction:** Any logic in the original script that directly manipulated files (other than temporary result files) would need a different approach in BigQuery (e.g., Cloud Storage interaction via external tables or Cloud Functions). For this specific job, this was not identified as a major issue.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`;
        ```
2.  **Deploy DDL for Logging and Job Control Tables:**
    *   Execute `bigquery/ddl/isrpt/isbert/job_error_log.sql` to create the error logging table:
        ```bash
        bq query --use_legacy_sql=false --project_id=<YOUR_PROJECT_ID> < bigquery/ddl/isrpt/isbert/job_error_log.sql
        ```
    *   Execute `bigquery/ddl/isrpt/isbert/job_table.sql` to create the job control table (if job management logic is to be reactivated):
        ```bash
        bq query --use_legacy_sql=false --project_id=<YOUR_PROJECT_ID> < bigquery/ddl/isrpt/isbert/job_table.sql
        ```
3.  **Implement `d_drop_temp_table.sql` Logic:**
    *   **Crucial Step:** Manually translate the actual `DROP TABLE` or `TRUNCATE TABLE` logic from the original `d_drop_temp_table.sql` file into the `bigquery/isrpt/isbert/aufbereitung/d_drop_temp_table.sql` procedure. This placeholder must be filled with the correct BigQuery SQL statements.
    *   Ensure the `INOUT p_records INT64` parameter is correctly updated with the count of tables dropped or records affected, if the original script tracked this.
4.  **Deploy BigQuery Stored Procedures:**
    *   Deploy the completed `d_drop_temp_table` procedure:
        ```bash
        bq query --use_legacy_sql=false --project_id=<YOUR_PROJECT_ID> < bigquery/isrpt/isbert/aufbereitung/d_drop_temp_table.sql
        ```
    *   Deploy the `r_drop_temp_table_control` procedure:
        ```bash
        bq query --use_legacy_sql=false --project_id=<YOUR_PROJECT_ID> < bigquery/isrpt/isbert/aufbereitung/r_drop_temp_table_control.sql
        ```
5.  **IAM Permissions:**
    *   Ensure the service account or user that will execute these BigQuery Stored Procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on the `dataset` for creating/dropping tables and inserting into log/job control tables.
        *   `BigQuery Job User` for running queries and stored procedures.
6.  **Connection Strings/Secrets:**
    *   No direct connection strings or secrets are required for the BigQuery Stored Procedures themselves, as they operate natively within BigQuery. If an external orchestrator (e.g., Cloud Composer) is used, ensure it has the necessary BigQuery connection configured.
7.  **Scheduling:**
    *   If the original `k_drop_temp_table.ksh` was scheduled (e.g., via cron), configure a new scheduler for the BigQuery Stored Procedure. Options include:
        *   **Cloud Scheduler:** To trigger a BigQuery scheduled query or a Cloud Function that calls the stored procedure.
        *   **BigQuery Scheduled Queries:** If the procedure can be called directly without complex orchestration.
        *   **Cloud Composer (Airflow):** For more complex scheduling, dependency management, or integration with other systems.

## 5. Known gaps & unresolved references

*   **`d_drop_temp_table.sql` Implementation:** The most significant gap is the placeholder nature of the `bigquery/isrpt/isbert/aufbereitung/d_drop_temp_table.sql` procedure. The actual `DROP TABLE` or `TRUNCATE TABLE` logic from the original `d_drop_temp_table.sql` file *must* be manually translated and implemented. This includes understanding which tables are targeted for dropping and any conditional logic involved.
*   **`PoolVertrag` Table:** The original script implied interaction with a `PoolVertrag` table. This table (and any other tables referenced in `d_drop_temp_table.sql`) must be migrated to BigQuery (e.g., `project.dataset.PoolVertrag`) before the procedures can function correctly.
*   **Commented Job Management Calls:** The original `k_drop_temp_table.ksh` contained commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. The `dataset.job_table` DDL has been provided, but the `INSERT` logic within `r_drop_temp_table_control.sql` is also commented out. If these functionalities are required, they need to be uncommented and fully implemented.
*   **Original `starteSQLSkript` Function Details:** The precise implementation of the `starteSQLSkript` function in the original ksh was not fully detailed. The migration assumes it was a standard `sqlplus` wrapper. If it contained highly specific logic beyond simple SQL execution, that logic might need further review.
*   **Complex Utility Script Logic:** While basic date and error handling utilities were replaced with native BigQuery functions, if any of the original utility scripts (`h_alis_date.ksh`, `f_alis_msgerr.ksh`, etc.) contained complex, domain-specific logic, those specific patterns might require further analysis and re-implementation as BigQuery UDFs or separate procedures.
*   **Error Code Mapping:** The original script used specific error numbers (e.g., `ErrNr = 1`, `ErrNr = 193`). While these are preserved in the `job_error_log` table, a comprehensive mapping or documentation of these error codes might be beneficial for future debugging.

## 6. Validation

Validation involves testing the deployed BigQuery Stored Procedures to ensure functional parity with the original KornShell script.

1.  **Test Environment Setup:**
    *   Ensure all DDLs are deployed and the `d_drop_temp_table.sql` procedure has been fully implemented with the correct logic.
    *   Populate any necessary BigQuery tables (e.g., `PoolVertrag` or temporary tables that `d_drop_temp_table` is expected to drop) with test data that mimics production scenarios.
2.  **Execution:**
    *   Call the main control procedure `dataset.r_drop_temp_table_control` using `bq query` or the BigQuery UI.
    *   **Example Calls:**
        *   **Successful run:**
            ```sql
            CALL dataset.r_drop_temp_table_control('JOB123', 'ENTRY001', '01012023', 0);
            ```
        *   **Invalid `Stichtag` (date format error):**
            ```sql
            CALL dataset.r_drop_temp_table_control('JOB123', 'ENTRY001', '2023-01-01', 0);
            ```
        *   **Missing `JobKennung` (parameter validation error):**
            ```sql
            CALL dataset.r_drop_temp_table_control(NULL, 'ENTRY001', '01012023', 0);
            ```
3.  **Verification (What "passing" means):**
    *   **Successful Execution:**
        *   The procedure completes without raising an unhandled `SIGNAL SQLSTATE` error.
        *   The output message `---------- ENDE Datenverarbeitung ----------` is displayed.
        *   The `dataset.job_error_log` table contains no new entries for this specific job run.
        *   The `dataset.d_drop_temp_table` procedure successfully drops/truncates the expected temporary tables. Verify this by querying `INFORMATION_SCHEMA.TABLES` or attempting to query the dropped tables.
        *   The `v_records` output from `d_drop_temp_table` (and logged by `r_drop_temp_table_control`) accurately reflects the number of tables dropped or records affected.
        *   If job management is reactivated, `dataset.job_table` should show correct entries/updates.
    *   **Error Handling:**
        *   **Invalid Parameter (e.g., missing `JobKennung`):** The procedure should return early with a `SELECT` message indicating the error (e.g., `FEHLER: 0 E 1 Jobkennung`). A corresponding entry should be found in `dataset.job_error_log` with `error_nr = 1`.
        *   **Invalid Date Format (`Stichtag`):** The procedure should `SIGNAL SQLSTATE '45000'` with the specified error message. A corresponding entry should be found in `dataset.job_error_log` with `error_nr = 193`.
        *   **Errors within `d_drop_temp_table`:** If `d_drop_temp_table` encounters an error (e.g., trying to drop a non-existent table without `IF EXISTS`), ensure the error is propagated correctly and logged in `dataset.job_error_log`.
    *   **Performance:** Compare the execution time of the BigQuery procedures with the original KornShell script to ensure performance parity or improvement.

## 7. Rollback procedure

In case of issues with the migrated BigQuery job, the rollback procedure involves reverting to the original KornShell script.

1.  **Stop New Process:**
    *   Immediately halt any scheduled executions of the BigQuery Stored Procedure (`dataset.r_drop_temp_table_control`) in Cloud Scheduler, BigQuery Scheduled Queries, or Cloud Composer.
2.  **Reactivate Original Process:**
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh` script in its legacy environment. Ensure its original scheduling mechanism (e.g., cron) is active.
3.  **Verify Original Functionality:**
    *   Monitor the execution of the original KornShell script to confirm it is running as expected and performing its intended function of dropping temporary tables. Check its logs and the state of the legacy database.
4.  **Data Cleanup (Optional, if necessary):**
    *   If the BigQuery procedures made any unintended changes to shared data (unlikely for a `DROP TEMP TABLE` job, but possible if `d_drop_temp_table` was misconfigured), perform necessary data restoration or cleanup in BigQuery.
    *   The `dataset.job_error_log` and `dataset.job_table` can be retained for post-mortem analysis or dropped if they were only for the migrated job.
5.  **Analysis and Remediation:**
    *   Analyze the `dataset.job_error_log` and BigQuery job history to identify the root cause of the rollback. Address the issues in the BigQuery procedures before attempting another deployment.