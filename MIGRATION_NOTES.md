# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_v_ta_cntrct_crs.ksh` from its original environment to Google Cloud's BigQuery platform.

The original script served as an orchestration layer, responsible for:
*   Parsing and validating runtime parameters.
*   Implementing job control logic (activating/deactivating job entries in a tracking table).
*   Executing an underlying SQL script (`d_ausd_v_ta_cntrct_crs.sql`) for data processing.
*   Capturing and logging processed record counts.

The migration targets BigQuery, where the entire orchestration and data processing logic will be encapsulated within a BigQuery Stored Procedure. Auxiliary functions like error logging and job tracking will be handled by dedicated BigQuery tables. This approach leverages BigQuery's native capabilities for scripting, data manipulation, and metadata management, eliminating the need for external shell environments or separate SQL execution engines.

## 2. Generated artifacts

The migration process generates the following BigQuery-specific artifacts:

*   **`sp_ausd_v_ta_cntrct_crs.sql`**:
    *   **Role**: This file contains the Data Definition Language (DDL) for the main BigQuery Stored Procedure. This procedure replaces the original `k_ausd_v_ta_cntrct_crs.ksh` script, handling parameter validation, job control, and orchestrating the core data processing logic (which will be derived from `d_ausd_v_ta_cntrct_crs.sql`). It is the central component of the migrated job.
*   **`job_table.sql`**:
    *   **Role**: This file contains the DDL for the `job_table` BigQuery table. This table is designed to track the status and metadata of data processing jobs, mirroring the functionality of the "job table" used by the original KornShell script for job activation/deactivation and record count persistence.
*   **`job_error_log.sql`**:
    *   **Role**: This file contains the DDL for the `job_error_log` BigQuery table. It serves as a centralized logging mechanism for errors encountered during the execution of the stored procedure, replacing the error handling and messaging functions of the original shell environment.
*   **`job_audit_log.sql`**:
    *   **Role**: This file contains the DDL for the `job_audit_log` BigQuery table. It provides an audit trail for job execution, capturing completion messages and key metrics like record counts, similar to the completion messages and job table updates in the original script.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Consolidation into BigQuery Stored Procedure**: The primary decision was to migrate the entire orchestration logic of the KornShell script, along with the data processing SQL, into a single BigQuery Stored Procedure (`sp_ausd_v_ta_cntrct_crs`). This approach leverages BigQuery's native scripting capabilities, allowing for direct execution within the data warehouse environment, reducing external dependencies, and simplifying deployment and scheduling.
*   **Native BigQuery SQL for Utilities**: All shell-based utility functions (e.g., parameter validation, date handling, error reporting) were replaced with native BigQuery SQL scripting constructs (e.g., `DECLARE`, `SET`, `IF`, `ASSERT`, `ERROR`) and dedicated logging tables (`job_error_log`, `job_audit_log`). This eliminates the need for external shell environments or custom helper scripts.
*   **BigQuery Tables for Job Control and Logging**: The existing "job table" concept for tracking job status and concurrency control was directly translated into a BigQuery table (`job_table`). Similarly, error and audit logging were implemented using dedicated BigQuery tables. This provides a persistent, queryable, and integrated mechanism for monitoring job execution within BigQuery.
*   **Direct SQL Translation**: The core data processing logic from `d_ausd_v_ta_cntrct_crs.sql` is intended to be directly embedded or called within the BigQuery Stored Procedure. This minimizes transformation effort for the data logic itself, focusing the migration on the orchestration layer.
*   **Elimination of Temporary Files**: The use of temporary files for inter-process communication (e.g., capturing record counts) was replaced by BigQuery scripting variables (`DECLARE v_records INT64;`) and direct updates to the `job_table`. This simplifies the logic and removes filesystem dependencies.

**Notable Trade-offs:**

*   **Dependency on BigQuery Ecosystem**: The solution is now tightly coupled with BigQuery. While this offers benefits in terms of integration and performance, it means less portability to other database platforms without significant re-engineering.
*   **Complexity within Stored Procedure**: Consolidating all logic into a single stored procedure can lead to a larger, more complex SQL file. However, for this specific job, the orchestration logic is relatively straightforward, making it manageable. For more complex scenarios, breaking down into multiple procedures or functions might be considered.
*   **Placeholder for Core SQL Logic**: The generated stored procedure includes a placeholder for the actual `d_ausd_v_ta_cntrct_crs.sql` content. This requires a separate, manual step to translate and insert that specific SQL, which introduces a potential point of error if not done carefully.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it:
        ```bash
        bq mk --dataset --default_location=US project:dataset
        ```
        (Adjust `US` to your desired region).

2.  **Deploy DDLs for Control Tables**:
    *   Execute the DDLs for the `job_table`, `job_error_log`, and `job_audit_log` to create these tables in your target dataset.
        ```bash
        bq query --use_legacy_sql=false < job_table.sql
        bq query --use_legacy_sql=false < job_error_log.sql
        bq query --use_legacy_sql=false < job_audit_log.sql
        ```

3.  **Translate and Integrate Core SQL Logic**:
    *   **Crucial Step**: The content of the original `d_ausd_v_ta_cntrct_crs.sql` must be thoroughly reviewed, translated into BigQuery SQL syntax, and then inserted into the `sp_ausd_v_ta_cntrct_crs.sql` file, replacing the placeholder comment. This includes:
        *   Rewriting any non-BigQuery compliant SQL functions or syntax.
        *   Ensuring correct table and column references (e.g., `project.dataset.source_table`).
        *   Adapting any DDL/DML operations to BigQuery best practices (e.g., `MERGE` statements for upserts, `INSERT INTO ... SELECT` for data loading).
        *   Verifying that the `v_records = @@row_count;` statement correctly captures the number of affected rows by the core processing logic.

4.  **Deploy BigQuery Stored Procedure**:
    *   After integrating the core SQL logic, deploy the `sp_ausd_v_ta_cntrct_crs.sql` file to BigQuery:
        ```bash
        bq query --use_legacy_sql=false < sp_ausd_v_ta_cntrct_crs.sql
        ```

5.  **IAM/Permissions**:
    *   Ensure the service account or user that will execute the BigQuery Stored Procedure has the necessary IAM roles:
        *   `BigQuery Data Editor` on the target dataset(s) for `INSERT`, `UPDATE`, `DELETE` operations on `job_table`, `job_error_log`, `job_audit_log`, and any target data tables.
        *   `BigQuery Data Viewer` on source dataset(s) for `SELECT` operations.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).

6.  **Connection Strings/Secrets**:
    *   No explicit connection strings or secrets are required for the BigQuery Stored Procedure itself, as it runs natively within BigQuery. Permissions are managed via IAM.

7.  **Scheduling**:
    *   Set up a BigQuery-native scheduler (e.g., Cloud Composer, Cloud Workflows, or BigQuery Scheduled Queries) to invoke the stored procedure. The scheduling frequency and parameters (`p_JobKennung`, `p_EintragsNr`) should match the original job's schedule.
    *   Example for a BigQuery Scheduled Query:
        ```sql
        CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('YOUR_JOB_ID', 'YOUR_ENTRY_NR');
        ```
        The values for `YOUR_JOB_ID` and `YOUR_ENTRY_NR` should be determined based on the original job's parameter generation logic.

## 5. Known gaps & unresolved references

The following items were flagged during the migration design and require further attention or are currently unresolved:

*   **Details of `d_ausd_v_ta_cntrct_crs.sql`**: The specific SQL logic within `d_ausd_v_ta_cntrct_crs.sql` is not explicitly detailed in the current analysis. A thorough review and translation of this SQL script are critical. This may involve complex SQL constructs, specific database functions, or performance considerations that need careful adaptation to BigQuery. This is the most significant outstanding item.
*   **`dw_init` Environment Variables**: The exact variables and configurations loaded by `. $HOME/.dw_init` are unknown. These need to be identified and mapped to BigQuery procedure parameters, BigQuery configuration tables, or hardcoded values if they are static and non-sensitive.
*   **`starteSQLSkript` Implementation Details**: The internal workings of the `h_alis_sqlplus.ksh` script and its `starteSQLSkript` function (e.g., how it connects to the database, handles errors, and precisely returns the record count) need to be fully understood to ensure faithful replication in BigQuery. The current design assumes `@@row_count` is sufficient, but if the original script had more complex record counting or error handling from the SQL execution, it needs to be replicated.
*   **"Aktive Jobs" Logic Precision**: The precise definition and handling of "aktive Jobs" (active jobs) and their deactivation logic must be fully understood to correctly implement this concurrency control in BigQuery. The current implementation assumes `job_kennung` and `eintrags_nr` uniquely identify a job instance, and that `tab_name` defines the logical process. Any additional criteria for "active" status or deactivation should be incorporated.

## 6. Validation

To validate the successful migration and functionality of the BigQuery Stored Procedure, follow these steps:

1.  **Execute the Stored Procedure**:
    *   Manually call the stored procedure with test parameters:
        ```sql
        CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('TEST_JOB_001', 'ENTRY_001');
        ```
    *   Repeat with different parameters, including invalid ones (e.g., `NULL` for `p_JobKennung`) to test error handling.

2.  **Verify `job_table` Entries**:
    *   Query the `job_table` to check for new entries:
        ```sql
        SELECT * FROM `project.dataset.job_table` WHERE job_kennung = 'TEST_JOB_001' ORDER BY created_ts DESC;
        ```
    *   **Passing Criteria**:
        *   A new entry with `job_kennung = 'TEST_JOB_001'`, `eintrags_nr = 'ENTRY_001'`, `tab_name = 'ta_cntrct_crs'`, and `status = 'DONE'` should exist.
        *   The `record_count` should accurately reflect the number of rows processed by the core SQL logic.
        *   `created_ts` and `updated_ts` should be recent timestamps.
        *   If the procedure is called multiple times for the same `tab_name` but different `job_kennung`/`eintrags_nr`, older "active" jobs for `ta_cntrct_crs` should be updated to `INACTIVE` status, demonstrating correct concurrency control.

3.  **Verify `job_error_log` Entries**:
    *   After executing with invalid parameters, query the `job_error_log`:
        ```sql
        SELECT * FROM `project.dataset.job_error_log` WHERE procedure_name = 'sp_ausd_v_ta_cntrct_crs' ORDER BY event_ts DESC;
        ```
    *   **Passing Criteria**:
        *   For invalid parameter calls, an entry should exist with `err_nr = 193` and the appropriate `err_arg` (`Jobkennung` or `EintragsNr`).
        *   No error entries should appear for successful runs.

4.  **Verify `job_audit_log` Entries**:
    *   Query the `job_audit_log` for completion messages:
        ```sql
        SELECT * FROM `project.dataset.job_audit_log` WHERE job_kennung = 'TEST_JOB_001' ORDER BY event_ts DESC;
        ```
    *   **Passing Criteria**:
        *   An entry with `message = 'ENDE Datenverarbeitung'` should exist for successful runs.
        *   The `record_count` in the audit log should match the `record_count` in the `job_table`.

5.  **Verify Target Data (if applicable)**:
    *   If the core SQL logic (`d_ausd_v_ta_cntrct_crs.sql`) modifies or inserts data into specific target tables, query those tables to ensure the data is correct and complete as expected.

## 7. Rollback procedure

In case of issues or a decision to revert the migration, follow these steps to roll back to the original KornShell script:

1.  **Deactivate BigQuery Scheduled Queries/Workflows**:
    *   Immediately disable or delete any BigQuery Scheduled Queries, Cloud Composer DAGs, or Cloud Workflows that invoke the `sp_ausd_v_ta_cntrct_crs` stored procedure. This prevents further execution of the migrated job.

2.  **Reactivate Original KornShell Script**:
    *   Re-enable the original scheduling mechanism (e.g., cron job, enterprise scheduler) for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh`.
    *   Verify that the original script is running as expected and processing data correctly.

3.  **Monitor Original Job**:
    *   Closely monitor the original KornShell script's execution and its output to ensure full functionality is restored.

4.  **Optional: Clean Up BigQuery Artifacts**:
    *   Once the rollback is confirmed successful and stable, you may optionally drop the BigQuery artifacts created during the migration. This step should only be performed after ensuring the original system is fully operational and there is no need to re-evaluate the BigQuery implementation immediately.
        ```bash
        bq rm -f -r `project.dataset.sp_ausd_v_ta_cntrct_crs` # Drop stored procedure
        bq rm -f `project.dataset.job_table`
        bq rm -f `project.dataset.job_error_log`
        bq rm -f `project.dataset.job_audit_log`
        ```
    *   **Caution**: Do not drop any source or target data tables unless they were specifically created *only* for this migration and contain no production data.