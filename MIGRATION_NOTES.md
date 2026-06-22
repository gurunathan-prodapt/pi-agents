# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh`. The script, primarily a control flow orchestrator for `d_ausd_adressen.sql`, handled parameter validation, date logic, and external SQL execution.

The migration targets **Google Cloud BigQuery**, translating the shell script's control flow, parameter handling, and utility calls into a BigQuery-native solution. The core data processing logic from `d_ausd_adressen.sql` is considered an external dependency that will be separately migrated into BigQuery SQL and integrated into the new control procedure.

## 2. Generated Artifacts

The migration will result in the creation of the following BigQuery and orchestration artifacts:

*   **BigQuery Stored Procedure**:
    *   `project.dataset.r_ausd_adressen_control`: This is the primary artifact, encapsulating the control flow, parameter validation, date logic, error handling, and the execution of the main data processing logic (migrated from `d_ausd_adressen.sql`).
*   **BigQuery Tables**:
    *   `project.dataset.error_log`: A table designed to capture structured error messages and codes, replacing the shell's `DWMSG_MeldeFehler` functionality.
    *   `project.dataset.job_table`: A table to log job execution details, including `job_kennung`, `eintrags_nr`, `stichtag`, `status`, `record_count`, and timestamps, replacing the intended `FOSJobErzeugeEintrag` functionality.
    *   `config.environment_variables` (Optional): A configuration table to store environment-like variables if direct parameter passing or BigQuery native functions are insufficient for all configurations.
*   **BigQuery SQL Logic**:
    *   The core business logic from `d_ausd_adressen.sql` will be translated into BigQuery SQL statements. This logic will either be embedded directly within `project.dataset.r_ausd_adressen_control` or structured as a separate BigQuery stored procedure/set of DML statements called by the control procedure.
*   **Orchestration Component**:
    *   **Cloud Composer DAG (Python)** or **BigQuery Scheduled Query**: An orchestration mechanism to trigger `project.dataset.r_ausd_adressen_control` and pass the necessary input parameters.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Control Flow**: The decision to use a BigQuery Stored Procedure (`project.dataset.r_ausd_adressen_control`) is central. This allows for the direct translation of shell script control constructs (parameter parsing, conditional logic, error handling, variable assignment) into BigQuery's procedural SQL, leveraging its native capabilities for data processing.
*   **Native BigQuery Functions for Utilities**: Shell utilities for date validation (`h_alis_date.ksh`), date derivation (`gestern.ksh`), and parameter validation (`h_alis_parameter.ksh`) are replaced by BigQuery's rich set of built-in functions (e.g., `SAFE.PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB`) and procedural `IF`/`CASE` statements. This avoids external dependencies and keeps the solution within the BigQuery ecosystem.
*   **Structured Error Logging**: Instead of shell `echo` to stderr and custom error functions, a dedicated `project.dataset.error_log` table is introduced. This provides a structured, queryable, and centralized mechanism for error reporting, improving observability and debugging.
*   **Direct SQL Integration**: The external `d_ausd_adressen.sql` is not executed via an external wrapper (like `sqlplus` in the original script) but is directly integrated into the BigQuery environment. This means its logic will be translated into BigQuery SQL and either embedded or called as a separate BigQuery object, eliminating the need for an external SQL client.
*   **Configuration Management**: Environment variables sourced from `.dw_init` and other scripts will be managed either through direct parameters to the stored procedure, values from a BigQuery configuration table, or by hardcoding if they are static and non-sensitive. This trade-off balances flexibility with simplicity.
*   **Orchestration Choice**: Cloud Composer (Airflow) or BigQuery Scheduled Queries offer robust scheduling and parameter passing capabilities, providing a modern replacement for cron jobs or manual shell script execution. Cloud Composer offers more flexibility for complex workflows, while Scheduled Queries are simpler for direct BigQuery procedure calls.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Create the target BigQuery dataset, e.g., `project.dataset`, if it does not already exist. This dataset will house all migrated tables and stored procedures.
2.  **Table Creation**:
    *   Deploy the Data Definition Language (DDL) for `project.dataset.error_log` and `project.dataset.job_table`.
    *   If a configuration table is deemed necessary, deploy the DDL for `config.environment_variables` and populate it with relevant values.
3.  **IAM/Permissions**:
    *   Ensure the Google Cloud service account used by the orchestration component (Cloud Composer or BigQuery Scheduled Queries) has the necessary BigQuery IAM roles. This includes `BigQuery Data Editor` on `project.dataset` (to create/update tables and run procedures) and `BigQuery Job User` (to run queries and jobs).
    *   Verify permissions for any source or target tables that the `d_ausd_adressen.sql` logic will interact with.
4.  **Connection Strings / Secrets**:
    *   For BigQuery-native operations, explicit "connection strings" are not typically used. Access is managed via IAM.
    *   If any sensitive configuration data is stored in `config.environment_variables`, ensure appropriate access controls are in place for that table.
5.  **Scheduling Setup**:
    *   **Cloud Composer**: Develop and deploy the Python DAG that calls `project.dataset.r_ausd_adressen_control` with the required parameters. Configure its schedule.
    *   **BigQuery Scheduled Query**: Create and configure a BigQuery Scheduled Query to execute the stored procedure, passing parameters as needed.
6.  **Source Data Availability**:
    *   Ensure all source tables referenced by the migrated `d_ausd_adressen.sql` logic are available in BigQuery and accessible to the service account.

## 5. Known Gaps & Unresolved References

*   **`d_ausd_adressen.sql` Detailed Migration**: The most significant gap is the detailed migration of the `d_ausd_adressen.sql` script. Its contents were not provided, and its successful translation into optimized BigQuery SQL is critical. This requires a separate, in-depth analysis and migration effort.
*   **Commented-out Job Management**: The original script contained commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. It is currently unclear if this job management functionality is still required or should be activated in the BigQuery migration. Clarification is needed to determine if `project.dataset.job_table` should be actively used.
*   **Error Handling Granularity**: While a general `error_log` table is proposed, the specific error codes (`ErrNr=193`, `ErrNr=192`) and their exact messages from the original script need to be meticulously mapped. The BigQuery stored procedure should handle these specific error conditions, potentially using `RAISE` for critical failures or detailed `INSERT` statements into `error_log`.
*   **Data Type and Implicit Conversions**: Careful attention is required during the migration of `d_ausd_adressen.sql` to ensure all data types are correctly mapped from the source database to BigQuery. Potential implicit conversions in the original SQL need to be explicitly handled in BigQuery to prevent unexpected behavior or performance issues.
*   **`BERT_DIR_ROOT` and other environment variables**: While a configuration table is proposed, the exact values and their usage (e.g., for file paths) need to be reviewed. If these paths were used for file I/O, an alternative BigQuery-native approach (e.g., Cloud Storage, external tables) might be needed.

## 6. Validation

Validation of the migrated job involves both unit and integration testing:

1.  **Unit Testing the Stored Procedure**:
    *   **Parameter Validation**: Invoke `project.dataset.r_ausd_adressen_control` directly with various combinations of valid and invalid input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   **Passing**: The procedure should correctly identify missing/invalid parameters, log an error to `project.dataset.error_log`, and exit gracefully (e.g., `LEAVE` statement) without processing data.
    *   **Date Validation**: Test with valid and invalid `p_Stichtag` values (e.g., '31022023', '01012023').
        *   **Passing**: Invalid dates should result in an error logged to `project.dataset.error_log` and procedure exit. Valid dates should be parsed correctly.
    *   **Core Logic Execution**: Once `d_ausd_adressen.sql` is migrated, run the stored procedure with valid parameters.
        *   **Passing**: The procedure should execute the migrated SQL logic, produce the expected output in target tables, and capture the correct record count.
    *   **Record Count**: Verify that the `v_records` variable (or equivalent) accurately reflects the number of records processed by the core logic.
        *   **Passing**: The `record_count` in `project.dataset.job_table` matches the actual count of processed records.
    *   **Job Logging**: Check `project.dataset.job_table` for correct entries on successful runs, including `job_kennung`, `eintrags_nr`, `stichtag`, `status`, and `record_count`.
        *   **Passing**: A new, accurate entry exists for each successful execution.
2.  **Integration Testing with Orchestration**:
    *   Trigger the Cloud Composer DAG or BigQuery Scheduled Query.
    *   **Passing**: The orchestration should successfully invoke the stored procedure, pass parameters correctly, and the entire workflow should complete without errors, resulting in updated target tables and job/error log entries as expected.
3.  **Data Verification**:
    *   Compare the output data in BigQuery target tables with the expected output from the original system (if possible, using a small, controlled dataset).
    *   **Passing**: Data integrity and accuracy are maintained.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Stop New Orchestration**:
    *   **Cloud Composer**: Pause or disable the Cloud Composer DAG responsible for invoking `project.dataset.r_ausd_adressen_control`.
    *   **BigQuery Scheduled Query**: Disable or delete the BigQuery Scheduled Query.
2.  **Re-enable Original Job**:
    *   Re-activate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh` script in its legacy environment.
    *   Verify that the original job runs successfully and processes data as expected.
3.  **Data State (if applicable)**:
    *   If the migrated job made irreversible changes to shared target tables, a data recovery plan might be necessary (e.g., restoring from a backup or re-running the original job to correct data). This should be assessed based on the impact of the migrated `d_ausd_adressen.sql` logic.
4.  **Cleanup (Optional)**:
    *   Once the rollback is confirmed and the original system is stable, the newly created BigQuery objects (stored procedures, tables, orchestration components) can be deleted or archived to avoid confusion and incurring unnecessary costs. This step is optional and can be deferred for post-mortem analysis.