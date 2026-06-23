# MIGRATION_NOTES.md

## 1. Summary

The Korn Shell (ksh) script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh` has been migrated. This script previously served as an orchestration and control script, responsible for environment initialization, parameter parsing and validation (including date format), date derivation, and the execution of a core SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`). It also handled record count capture and error reporting.

The migration target is Google Cloud BigQuery. The orchestration and parameter handling logic of the original ksh script has been re-implemented as a BigQuery Stored Procedure (`my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk`). This stored procedure integrates with a separately migrated BigQuery version of the core SQL logic. Logging and error reporting are now handled by dedicated BigQuery tables (`my_project.my_dataset.job_run_log` and `my_project.my_dataset.job_error_log`). The overall job scheduling and invocation will be managed by a Google Cloud orchestration service (e.g., Cloud Composer, Cloud Workflows, or BigQuery Scheduled Queries).

## 2. Generated artifacts

The migration produced the following BigQuery SQL artifacts:

*   **`my_project.my_dataset.job_run_log.sql`**
    *   **Role:** This DDL script creates the `job_run_log` table in BigQuery. This table is used to record details of successful job executions, including input parameters, processed record counts, and timestamps, replacing the ad-hoc logging mechanisms of the original ksh script.
*   **`my_project.my_dataset.job_error_log.sql`**
    *   **Role:** This DDL script creates the `job_error_log` table in BigQuery. This table captures detailed information about any errors encountered during job execution, including the job name, entry number, processing date, and the error message, replacing the `f_alis_msgerr.ksh` utility.
*   **`my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk.sql`**
    *   **Role:** This script defines the main BigQuery Stored Procedure `r_ausd_bp_ta_rn_da_vda_tk`. It encapsulates the core orchestration logic of the original ksh script:
        *   Accepts input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Performs parameter presence and date format validation.
        *   Derives `v_datum_heute` and `v_datum_gestern` using native BigQuery functions.
        *   Calls the migrated core SQL stored procedure (`my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk`).
        *   Captures the record count from the target table.
        *   Logs successful execution to `job_run_log` or errors to `job_error_log`.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Orchestration**: The entire orchestration and control flow of the ksh script was re-implemented as a BigQuery Stored Procedure. This decision leverages BigQuery's native capabilities for data processing, eliminates cross-platform dependencies (e.g., shell environment, SQL*Plus), and allows for direct, efficient invocation of other BigQuery SQL components.
*   **Native BigQuery Functions for Utilities**: All shell script utility functions (e.g., `.dw_init` for environment setup, `h_alis_date.ksh` for date validation, `gestern.ksh` for date derivation, `h_alis_parameter.ksh` for parsing) were replaced by equivalent BigQuery SQL constructs and built-in functions (e.g., `SAFE.PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB()`, `COALESCE`, `IF` statements). This approach streamlines the code, removes external script dependencies, and ensures all logic executes within the BigQuery environment.
*   **Dedicated BigQuery Logging Tables**: Instead of file-based logging or shell-based error messages, structured `job_run_log` and `job_error_log` tables were introduced. This provides a centralized, queryable, and auditable record of job executions and failures directly within BigQuery, simplifying monitoring and troubleshooting.
*   **BigQuery `RAISE` for Error Signaling**: The `RAISE` statement in BigQuery SQL is used to signal errors immediately upon validation failure or during execution. This integrates seamlessly with BigQuery's transaction model and allows external orchestrators to detect and react to job failures effectively, replacing the shell script's `exit` codes and `DWMSG_MeldeFehler` calls.
*   **Direct Target Table Query for Record Count**: The original method of writing a record count to a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_da_vda_tk.tmp`) and then reading it was replaced by a direct `SELECT COUNT(*)` query on the target BigQuery table. This is a more robust, efficient, and BigQuery-native way to obtain the processed record count.
*   **Exclusion of Commented-out/Inactive Logic**: Commented-out `sed`, `sort`, `join` operations and legacy job control calls (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) were explicitly excluded from the migration. This decision focused the migration effort on the currently active and essential logic, reducing complexity. A trade-off is the assumption that this commented-out logic is truly obsolete and does not represent latent requirements.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure the BigQuery project (`my_project`) and dataset (`my_dataset`) exist. If not, create them.
2.  **IAM Permissions Configuration**:
    *   The service account or user deploying these artifacts must have `bigquery.datasets.create`, `bigquery.tables.create`, and `bigquery.routines.create` permissions on `my_project.my_dataset`.
    *   The service account or user executing the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure must have:
        *   `bigquery.routines.call` permission on `my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk` and `my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk`.
        *   `bigquery.tables.insertData` permission on `my_project.my_dataset.job_run_log` and `my_project.my_dataset.job_error_log`.
        *   `bigquery.tables.getData` permission on `my_project.my_dataset.bp_target_table` (for record count).
        *   Appropriate read permissions on source tables (e.g., `source_project.source_dataset.PoolBasisprodukt`) and write permissions on the target table (`my_project.my_dataset.bp_target_table`) for the core SQL logic.
3.  **Dependent Migrations**:
    *   **Core SQL Script**: The `d_ausd_bp_ta_rn_da_vda_tk.sql` script, which contains the core data processing logic, *must* be migrated to a BigQuery Stored Procedure (e.g., `my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk`) and deployed *prior* to the deployment of `r_ausd_bp_ta_rn_da_vda_tk`. The `r_ausd_bp_ta_rn_da_vda_tk` procedure explicitly calls this dependent procedure.
    *   **Source Data**: The `PoolBasisprodukt` table and any other source tables referenced by `d_ausd_bp_ta_rn_da_vda_tk` must be migrated to BigQuery (e.g., `source_project.source_dataset.PoolBasisprodukt`).
4.  **Target Table Creation**:
    *   The target table where the core SQL logic writes its output (e.g., `my_project.my_dataset.bp_target_table`) must be created with the correct schema before the job runs.
5.  **Orchestration Configuration**:
    *   Configure your chosen Google Cloud orchestration service (e.g., Cloud Composer/Apache Airflow, Cloud Workflows, or BigQuery Scheduled Queries) to invoke the `my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk` stored procedure. Ensure all required input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) are correctly passed during invocation.
6.  **Secrets Management (if applicable)**:
    *   If any parameters passed to the stored procedure become sensitive (e.g., API keys, database credentials if connecting to external systems), integrate with Google Cloud Secret Manager to securely store and retrieve these values.

## 5. Known gaps & unresolved references

The following items have been flagged for follow-up or represent known limitations/risks:

*   **Core SQL Script (`d_ausd_bp_ta_rn_da_vda_tk.sql`)**: The detailed transformation logic, performance characteristics, and specific parameter requirements of this core SQL script were not part of this migration analysis. Its successful migration to BigQuery is a critical prerequisite and external dependency.
*   **Commented-out Legacy Job Control (B4 Item)**: The original ksh script contains commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` and related logic involving `PoolBasisprodukt`. While currently inactive and thus not migrated, a thorough review is required to confirm if this functionality is truly obsolete. If it represents a latent requirement for job status management or inter-job communication, a redesign (B4 item) for a BigQuery-native equivalent is needed.
*   **Intermediate Flat Files**: The presence of commented-out `sed`, `sort`, `join` operations in the original script suggests a historical reliance on intermediate flat files. If the core SQL logic (`d_ausd_bp_ta_rn_da_vda_tk.sql`) or any other part of the workflow still relies on generating or consuming intermediate flat files, this pattern needs to be redesigned to use BigQuery tables or Cloud Storage for staging, as BigQuery Stored Procedures do not directly interact with local file systems.
*   **Error Message Localization/Standardization**: The migrated BigQuery Stored Procedure retains the German error messages from the original script. A decision is needed on whether these should be standardized to English or integrated into a broader localization framework if the target audience or operational procedures require it.
*   **`starteSQLSkript` Function Behavior**: The exact implementation details of the original `starteSQLSkript` function (how it invoked SQL*Plus, handled specific error conditions, and precisely extracted the record count to `tmpFile`) were inferred. Any subtle behaviors or edge cases not fully captured during this inference might require minor adjustments to the BigQuery stored procedure.
*   **`bp_target_table` Schema**: The schema of the `my_project.my_dataset.bp_target_table` (where the core SQL logic writes its output) is assumed to be compatible with the data produced by the migrated `d_ausd_bp_ta_rn_da_vda_tk` procedure. This schema must be correctly defined and maintained.

## 6. Validation

Validation of the migrated job involves verifying the successful deployment of BigQuery objects and testing the stored procedure with various input scenarios.

### Deployment Verification

1.  **Confirm Table Existence**:
    *   Verify that `my_project.my_dataset.job_run_log` exists.
    *   Verify that `my_project.my_dataset.job_error_log` exists.
2.  **Confirm Stored Procedure Existence**:
    *   Verify that `my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk` exists.
    *   Verify that `my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk` (the migrated core SQL procedure) exists.

### Test Cases

Execute the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure with the following parameters. After each execution, query the `job_run_log` and `job_error_log` tables to verify the outcome.

1.  **Successful Execution (Valid Parameters)**
    *   **Action**: Call `CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('JOB123', 'ENTRY001', '01012023', '0');`
    *   **Passing Criteria**:
        *   The procedure completes without raising an error.
        *   A new entry is found in `my_project.my_dataset.job_run_log` with:
            *   `tab_name = 'k_ausd_bp_ta_rn_da_vda_tk'`
            *   `job_kennung = 'JOB123'`, `eintrags_nr = 'ENTRY001'`, `stichtag = '01012023'`, `wiederanlauf_wert = '0'`
            *   `records_processed` reflecting the actual count from `bp_target_table` (should be > 0 if data was processed).
        *   No new entry is found in `my_project.my_dataset.job_error_log`.
        *   The `my_project.my_dataset.bp_target_table` contains the expected processed data.

2.  **Missing `p_JobKennung`**
    *   **Action**: Call `CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(NULL, 'ENTRY001', '01012023', '0');`
    *   **Passing Criteria**:
        *   The procedure fails and raises an error.
        *   A new entry is found in `my_project.my_dataset.job_error_log` with:
            *   `job_name = 'k_ausd_bp_ta_rn_da_vda_tk'`
            *   `error_message` containing "JobKennung must be provided."

3.  **Invalid `p_Stichtag` Format**
    *   **Action**: Call `CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('JOB123', 'ENTRY001', '2023-01-01', '0');`
    *   **Passing Criteria**:
        *   The procedure fails and raises an error.
        *   A new entry is found in `my_project.my_dataset.job_error_log` with:
            *   `job_name = 'k_ausd_bp_ta_rn_da_vda_tk'`
            *   `error_message` containing "Stichtag must be in DDMMYYYY format."

4.  **`p_wiederanlaufWert` Defaulting**
    *   **Action**: Call `CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('JOB123', 'ENTRY001', '01012023', NULL);`
    *   **Passing Criteria**:
        *   The procedure completes successfully.
        *   A new entry is found in `my_project.my_dataset.job_run_log` where `wiederanlauf_wert = '0'`.

5.  **Core SQL Logic Failure (Simulated)**
    *   **Pre-requisite**: Temporarily modify `my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk` to intentionally `RAISE` an error (e.g., `RAISE USING MESSAGE 'Simulated core SQL error';`).
    *   **Action**: Call `CALL my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk('JOB123', 'ENTRY001', '01012023', '0');`
    *   **Passing Criteria**:
        *   The `r_ausd_bp_ta_rn_da_vda_tk` procedure fails and raises an error.
        *   A new entry is found in `my_project.my_dataset.job_error_log` with:
            *   `job_name = 'k_ausd_bp_ta_rn_da_vda_tk'`
            *   `error_message` reflecting the error raised by `d_ausd_bp_ta_rn_da_vda_tk`.
    *   **Post-requisite**: Revert the temporary modification to `d_ausd_bp_ta_rn_da_vda_tk`.

## 7. Rollback procedure

In case of issues requiring a rollback, follow these steps to revert the migration:

1.  **Halt New Executions**:
    *   Immediately disable or remove the scheduled invocation of `my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk` from your orchestration service (Cloud Composer, Cloud Workflows, or BigQuery Scheduled Queries).
2.  **Re-enable Original Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh` script and its associated scheduling in the legacy environment.
3.  **Drop BigQuery Objects**:
    *   Execute the following DDL commands in BigQuery to remove the migrated objects:
        ```sql
        -- Drop the main orchestration stored procedure
        DROP PROCEDURE IF EXISTS my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk;

        -- Drop the logging tables
        DROP TABLE IF EXISTS my_project.my_dataset.job_run_log;
        DROP TABLE IF EXISTS my_project.my_dataset.job_error_log;

        -- Optional: If the core SQL procedure was deployed as part of this migration, drop it.
        -- Otherwise, its rollback would be part of its own migration notes.
        -- DROP PROCEDURE IF EXISTS my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk;

        -- Optional: If the target table was created solely for this migration and is not used by other processes, drop it.
        -- DROP TABLE IF EXISTS my_project.my_dataset.bp_target_table;
        ```
4.  **Verify Rollback**:
    *   Confirm that the original ksh job is running as expected in the legacy environment.
    *   Verify that the BigQuery objects listed above have been successfully dropped.