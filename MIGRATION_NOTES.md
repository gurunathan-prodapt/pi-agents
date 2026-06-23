# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh`. This script served as an orchestration layer, handling parameter parsing, date validation, and managing the execution of an underlying SQL script (`d_ausd_geschaeftspartner.sql`) for processing "Geschaeftspartner" (business partner) data.

The migration targets **Google BigQuery**. The orchestration logic of the KornShell script has been re-implemented as a BigQuery Stored Procedure, `project.dataset.r_ausd_vertrag_control`. The core data transformation logic, originally in `d_ausd_geschaeftspartner.sql`, is intended to be migrated into a separate BigQuery Stored Procedure, `project.dataset.d_ausd_geschaeftspartner_proc`. Additionally, dedicated BigQuery tables have been created for job control, run logging, and error logging.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL (Data Definition Language) for tables and stored procedures:

*   **`bq/ddl/job_error_log.sql`**
    *   **Role:** Creates the `project.dataset.job_error_log` table. This table is used to capture and store detailed error messages and contextual information whenever an error occurs during the execution of the migrated BigQuery stored procedures. It replaces the error logging functionality previously handled by shell utilities like `f_alis_msgerr.ksh`.

*   **`bq/ddl/job_run_log.sql`**
    *   **Role:** Creates the `project.dataset.job_run_log` table. This table records details of successful job executions, including job identifiers, key dates, and the number of records processed. It provides an audit trail and operational metrics for each successful run, replacing implied or file-based logging from the legacy environment.

*   **`bq/ddl/job_table.sql`**
    *   **Role:** Creates the `project.dataset.job_table` table. This table serves as a central job control mechanism, managing job status, active flags, processing dates, and other metadata. It replaces the intended (but partially commented out) job control functionality within the original KornShell script.

*   **`bq/procs/d_ausd_geschaeftspartner_proc.sql`**
    *   **Role:** Creates the `project.dataset.d_ausd_geschaeftspartner_proc` stored procedure. This procedure is a placeholder for the core data transformation logic that was originally contained within `d_ausd_geschaeftspartner.sql`. It is designed to accept parameters from the orchestration procedure and return the number of records processed. The actual SQL translation from the legacy script needs to be implemented here.

*   **`bq/procs/r_ausd_vertrag_control.sql`**
    *   **Role:** Creates the `project.dataset.r_ausd_vertrag_control` stored procedure. This is the main orchestration procedure, directly replacing the `k_ausd_geschaeftspartner.ksh` script. It handles parameter validation, date calculations, calls the `d_ausd_geschaeftspartner_proc` for data processing, and updates the `job_run_log` and `job_table` with execution status and metrics. It also incorporates BigQuery's native error handling mechanisms.

## 3. Key design decisions

The migration to BigQuery involved several key design decisions to translate the shell script's orchestration logic and integrate with BigQuery's capabilities:

*   **Orchestration via BigQuery Stored Procedures:** The primary decision was to replace the KornShell script's orchestration with a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This approach keeps the control flow close to the data, leveraging BigQuery's native SQL capabilities for parameter handling, validation, and conditional logic, rather than relying on an external shell environment.
*   **Separation of Concerns:** The core data transformation logic (from `d_ausd_geschaeftspartner.sql`) was separated into its own BigQuery Stored Procedure (`d_ausd_geschaeftspartner_proc`). This promotes modularity, reusability, and easier maintenance, allowing the orchestration layer to focus solely on job control.
*   **Native BigQuery Utilities for Shell Functions:** All shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `gestern.ksh`) were replaced by equivalent BigQuery SQL constructs:
    *   **Error Handling:** `RAISE USING MESSAGE` within `BEGIN...EXCEPTION` blocks, logging to `job_error_log`, replaces `f_alis_msgerr.ksh`.
    *   **Date Functions:** `CURRENT_DATE()`, `DATE_SUB()`, and `SAFE.PARSE_DATE()` replace `h_alis_date.ksh` and `gestern.ksh`.
    *   **Parameter Validation:** `IF` conditions and `RAISE` statements replace `h_alis_parameter.ksh`.
*   **Dedicated Job Control and Logging Tables:** Instead of implied or file-based job tracking, explicit BigQuery tables (`job_table`, `job_run_log`, `job_error_log`) were introduced. This provides a structured, queryable, and centralized mechanism for monitoring job status, history, and errors, improving operational visibility.
*   **Direct Parameter Passing:** Command-line arguments (`getopts`) are replaced by direct `IN` and `OUT` parameters in the BigQuery Stored Procedures, simplifying parameter management and type safety.
*   **Elimination of Temporary Files:** The use of temporary files for capturing record counts (e.g., `tmpFile`) is replaced by BigQuery Stored Procedure variables and direct DML updates to log tables, streamlining the data flow and reducing I/O overhead.

**Notable Trade-offs:**

*   **External Orchestration Requirement:** While the logic is self-contained in BigQuery, an external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, or BigQuery Scheduled Queries) is now required to trigger the main BigQuery Stored Procedure, as BigQuery itself does not provide advanced scheduling capabilities for stored procedures.
*   **SQL Complexity for Shell Logic:** Some shell script logic (e.g., string manipulation, conditional file operations) might translate into more verbose or complex SQL statements within BigQuery, requiring careful implementation and testing.
*   **Loss of File System Interaction:** The direct interaction with the file system (e.g., reading/writing temporary files, sourcing environment scripts) is no longer possible within BigQuery Stored Procedures. All data and configuration must be managed within BigQuery or accessible via BigQuery's external data sources.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery `project.dataset` exists. If not, create the dataset within your Google Cloud Project.
    *   Command example: `bq mk --dataset project:dataset`

2.  **Create BigQuery DDL Objects:**
    *   Execute the DDL scripts for the logging and control tables:
        *   `bq/ddl/job_error_log.sql`
        *   `bq/ddl/job_run_log.sql`
        *   `bq/ddl/job_table.sql`
    *   Execute the DDL scripts for the stored procedures:
        *   `bq/procs/d_ausd_geschaeftspartner_proc.sql` (Note: This is a placeholder and needs to be fully implemented first.)
        *   `bq/procs/r_ausd_vertrag_control.sql`
    *   These can be run using the `bq query` command, BigQuery UI, or a CI/CD pipeline.

3.  **Implement `d_ausd_geschaeftspartner_proc`:**
    *   **Crucially**, the placeholder logic in `bq/procs/d_ausd_geschaeftspartner_proc.sql` must be replaced with the actual translated BigQuery SQL from the original `d_ausd_geschaeftspartner.sql` script. This involves:
        *   Analyzing `d_ausd_geschaeftspartner.sql` for Oracle-specific syntax, functions, and data types.
        *   Translating these to their BigQuery equivalents.
        *   Ensuring all source tables referenced in the original SQL are available in BigQuery (see Data Ingestion below).
        *   Testing the logic thoroughly.

4.  **IAM / Permissions:**
    *   The service account or user executing the BigQuery stored procedures must have appropriate IAM roles:
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the `project.dataset` to create/update/delete data in `job_error_log`, `job_run_log`, `job_table`, and any target tables populated by `d_ausd_geschaeftspartner_proc`.
        *   `BigQuery Job User` (roles/bigquery.jobUser) to run BigQuery jobs (including stored procedures).
        *   `BigQuery Data Viewer` (roles/bigquery.dataViewer) on any source tables read by `d_ausd_geschaeftspartner_proc`.

5.  **Data Ingestion:**
    *   If the original `d_ausd_geschaeftspartner.sql` script sourced data from an external database (e.g., Oracle), ensure that this data is continuously ingested and available in BigQuery tables that `d_ausd_geschaeftspartner_proc` can access. This might involve setting up data pipelines using tools like Cloud Data Fusion, Dataflow, or Striim.

6.  **Scheduling / External Orchestration:**
    *   Configure an external orchestrator (e.g., Cloud Composer/Airflow DAG, Cloud Workflows, or BigQuery Scheduled Query) to call the `project.dataset.r_ausd_vertrag_control` stored procedure.
    *   The orchestrator must pass the required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) to the stored procedure.
    *   Ensure the orchestrator has the necessary BigQuery IAM permissions.

## 5. Known gaps & unresolved references

The following items have been identified as gaps or require further attention and follow-up:

*   **Underlying SQL Logic Complexity (`d_ausd_geschaeftspartner.sql`):** The `d_ausd_geschaeftspartner_proc` is currently a placeholder. The full migration of the `d_ausd_geschaeftspartner.sql` script to BigQuery SQL is a significant effort that needs to be completed. This includes translating Oracle-specific SQL, PL/SQL, functions, and procedures, and ensuring data type compatibility and performance in BigQuery.
*   **Commented-out Code in Legacy Script:** The original `k_ausd_geschaeftspartner.ksh` script contained commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. While the migration includes logic to deactivate old active jobs in `job_table`, it's crucial to confirm if the commented-out functionality was intended to be active or if it's dead code. If active, its full business requirement needs to be understood and implemented.
*   **Environment Variables/Paths:** The legacy script relied on shell environment variables like `BERT_DIR_ROOT` and `DW_DIR_UTL`. Their exact values and contents, particularly if they pointed to configuration files or data directories, need to be fully understood. Any configuration values should be either hardcoded as constants in the BigQuery procedures, passed as parameters, or stored in a BigQuery configuration table.
*   **Detailed Error Handling Mapping:** While a general BigQuery `EXCEPTION` block is implemented, a detailed understanding of the legacy error codes (`ErrNr`) and messaging (`DWMSG_MeldeFehler`) from `f_alis_msgerr.ksh` is needed for a faithful and comprehensive migration of error reporting and severity levels.
*   **`starteSQLSkript` Functionality:** The exact functionality of the `starteSQLSkript` wrapper (e.g., how it connects to the database, handles SQL*Plus errors, parses output) needs to be thoroughly analyzed. The current `CALL` to `d_ausd_geschaeftspartner_proc` assumes a direct execution, but any specific behaviors of `starteSQLSkript` (like connection pooling, specific error code handling, or output formatting) might need to be replicated if critical.

## 6. Validation

Validation ensures that the migrated job functions correctly and produces accurate results in the BigQuery environment.

**How to run the tests:**

1.  **Manual Execution (for `d_ausd_geschaeftspartner_proc`):**
    *   Once the `d_ausd_geschaeftspartner_proc` is fully implemented, execute it directly with sample parameters to verify its core data transformation logic.
    *   Example: `CALL project.dataset.d_ausd_geschaeftspartner_proc('123', 'JOB_TEST', CURRENT_DATE(), 0, CURRENT_DATE(), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), @records_processed);`

2.  **Manual Execution (for `r_ausd_vertrag_control`):**
    *   Execute the main orchestration procedure `r_ausd_vertrag_control` with various valid and invalid parameter combinations to test parameter validation, date validation, and the call to the child procedure.
    *   Example: `CALL project.dataset.r_ausd_vertrag_control('JOB_KENNUNG_001', 'ENTRY_001', '01012023', 0);`
    *   Test with invalid `p_Stichtag` (e.g., `'20230101'`) to ensure error logging.

3.  **Orchestrator Execution:**
    *   Once configured, trigger the job via the external orchestrator (e.g., Cloud Composer DAG) to simulate a production run.

**What "passing" means:**

*   **Successful Execution:** The `project.dataset.r_ausd_vertrag_control` stored procedure completes without raising an unhandled exception.
*   **Job Run Log Entry:** A new entry with `status = 'SUCCESS'` is recorded in `project.dataset.job_run_log` for the corresponding job run.
*   **Job Control Table Update:** The `project.dataset.job_table` is updated correctly:
    *   The `active_flag` for the `v_TabName` ('PoolVertrag') is set to 'A'.
    *   The `record_count` reflects the actual number of records processed by `d_ausd_geschaeftspartner_proc`.
    *   `from_date` and `to_date` match the `p_Stichtag`.
*   **No Error Log Entries:** No new entries are found in `project.dataset.job_error_log` for the successful run.
*   **Data Correctness and Integrity:**
    *   The target tables populated by `d_ausd_geschaeftspartner_proc` contain the expected data.
    *   Record counts in the target tables match the `records_processed` value in `job_run_log`.
    *   Data integrity checks (e.g., referential integrity, uniqueness, data type correctness) pass.
    *   Compare a sample of the output data with the output generated by the legacy `k_ausd_geschaeftspartner.ksh` script for the same input parameters.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Deactivate BigQuery Job:**
    *   Immediately disable or delete the external orchestrator job (e.g., Cloud Composer DAG, BigQuery Scheduled Query) that triggers `project.dataset.r_ausd_vertrag_control`. This stops any further execution of the migrated job.

2.  **Re-enable Legacy Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh` script in its legacy scheduling system.

3.  **BigQuery Object Cleanup (Optional, but Recommended):**
    *   **Drop Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_geschaeftspartner_proc`;
        ```
    *   **Drop Tables:**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        DROP TABLE IF EXISTS `project.dataset.job_run_log`;
        DROP TABLE IF EXISTS `project.dataset.job_table`;
        ```
    *   **Data Rollback (if applicable):** If `d_ausd_geschaeftspartner_proc` modified or created data in production tables, a specific data rollback strategy might be required. This could involve:
        *   Restoring target tables from a point-in-time snapshot (if enabled).
        *   Deleting data inserted by the migrated job based on a timestamp or job identifier.
        *   Running a reverse transformation script.
        *   **Note:** The exact data rollback steps depend heavily on the implementation of `d_ausd_geschaeftspartner_proc` and the impact it has on production data. This should be planned in detail during the implementation phase of `d_ausd_geschaeftspartner_proc`.

4.  **Verify Legacy System:**
    *   Confirm that the legacy `k_ausd_geschaeftspartner.ksh` script is running as expected and producing correct output.