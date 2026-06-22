# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_bp_ta_rn_da_vda_tk.ksh` from its legacy environment to Google Cloud BigQuery. The original script served as an ETL orchestrator, handling environment setup, parameter validation, date calculations, and the execution of a core SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`), along with basic error handling and logging.

The migration re-implements this orchestration logic as a BigQuery Stored Procedure, leveraging BigQuery's native SQL capabilities for parameter handling, date operations, error management, and logging. The core business logic, originally in `d_ausd_bp_ta_rn_da_vda_tk.sql`, is intended to be embedded directly within this BigQuery Stored Procedure after its own migration to BigQuery SQL.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL files:

*   **`project/dataset/create_error_log_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `error_log` table in BigQuery. This table is used by the migrated stored procedure to record details of any errors encountered during its execution, including parameter validation failures and unexpected exceptions.
*   **`project/dataset/create_process_log_table.sql`**
    *   **Role:** Defines the DDL for the `process_log` table in BigQuery. This table captures informational messages and progress updates throughout the stored procedure's execution, providing an audit trail of its operations.
*   **`project/dataset/create_job_table.sql`**
    *   **Role:** Defines the DDL for the `job_table` in BigQuery. This table serves as a central repository for tracking job status, record counts, and other metadata, replacing the functionality implied by the legacy `FOSJobErzeugeEintrag` calls.
*   **`project/dataset/r_ausd_bp_ta_rn_da_vda_tk.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `r_ausd_bp_ta_rn_da_vda_tk`. This is the primary migrated artifact, encapsulating the orchestration logic of the original KornShell script. It handles parameter validation, date derivations, error handling, logging, and provides a placeholder for the integration of the core business logic from `d_ausd_bp_ta_rn_da_vda_tk.sql`.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Orchestration:** The KornShell script's orchestration role is fully migrated to a BigQuery Stored Procedure. This decision leverages BigQuery's native capabilities, eliminating the need for external shell environments and `sqlplus` calls, and centralizing the logic within the data platform.
*   **Native BigQuery Constructs:**
    *   **Parameter Handling:** Command-line arguments are replaced by explicit Stored Procedure parameters, providing type safety and clear interfaces.
    *   **Date Operations:** Legacy shell scripts (`gestern.ksh`, `h_alis_date.ksh`) are replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`), simplifying date logic and improving performance.
    *   **Logging and Error Handling:** Console outputs and temporary files for logging are replaced by dedicated BigQuery tables (`error_log`, `process_log`, `job_table`). BigQuery's `ASSERT` and `RAISE` statements provide robust error management.
    *   **Temporary Data:** File-based temporary data handling (e.g., for record counts) is replaced by BigQuery variables, Common Table Expressions (CTEs), or direct queries against target tables.
*   **Embedding Core SQL Logic:** The primary business logic from `d_ausd_bp_ta_rn_da_vda_tk.sql` is designed to be directly embedded within the BigQuery Stored Procedure. This creates a self-contained, atomic unit of work, simplifying deployment and execution.
*   **Trade-offs:**
    *   **Dependency on Core SQL Migration:** The effectiveness of this migration heavily depends on the successful and accurate translation of `d_ausd_bp_ta_rn_da_vda_tk.sql` into BigQuery SQL. This is a significant separate effort.
    *   **Loss of Filesystem Interaction:** The direct interaction with the filesystem for temporary files and external script calls is replaced by BigQuery-native constructs. While this simplifies the architecture, it means any complex file-based operations (like the commented-out `sed`, `sort`, `join` commands) would need complete re-engineering in BigQuery SQL.
    *   **Orchestration Layer:** While the SP handles internal orchestration, external scheduling (e.g., daily runs) will require a separate mechanism like Cloud Composer/Airflow or BigQuery Scheduled Queries.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset` as referenced in the generated code) exists in your Google Cloud project. If not, create it.
2.  **Deploy Logging and Job Tables:**
    *   Execute `project/dataset/create_error_log_table.sql` to create the `error_log` table.
    *   Execute `project/dataset/create_process_log_table.sql` to create the `process_log` table.
    *   Execute `project/dataset/create_job_table.sql` to create the `job_table`.
3.  **Migrate Core SQL Logic:**
    *   **Crucially, the SQL logic from `d_ausd_bp_ta_rn_da_vda_tk.sql` must be manually reviewed, rewritten for BigQuery SQL syntax and semantics, and then inserted into the `r_ausd_bp_ta_rn_da_vda_tk.sql` stored procedure at the designated placeholder.** This includes ensuring all source tables are accessible in BigQuery and target tables are correctly defined.
    *   Update the `v_TabName` variable in the stored procedure to reflect the actual primary target table name used by the core SQL logic.
    *   Modify the `v_records` assignment to accurately capture the count of records processed/inserted by the integrated core SQL logic.
4.  **Deploy BigQuery Stored Procedure:** Execute the final `project/dataset/r_ausd_bp_ta_rn_da_vda_tk.sql` script (after integrating the core SQL logic) to create or replace the stored procedure in BigQuery.
5.  **IAM Permissions:** Grant the necessary Identity and Access Management (IAM) permissions to the service account or user that will execute the stored procedure. This typically includes `BigQuery Data Editor` or `BigQuery Data Owner` roles on the target dataset(s) to allow table creation, data insertion, and procedure execution.
6.  **Scheduling Configuration:** If the job requires automated scheduling, configure an appropriate mechanism:
    *   **Cloud Composer (Airflow):** Create a DAG that invokes the BigQuery Stored Procedure with the required parameters.
    *   **BigQuery Scheduled Queries:** Set up a scheduled query that calls the stored procedure.
    *   **Cloud Functions/Run:** Develop a function to trigger the stored procedure.
7.  **Connection Strings/Secrets:** If the integrated core SQL logic requires access to external data sources outside BigQuery, ensure any necessary connection strings or secrets are securely managed (e.g., via Secret Manager) and configured for access by the BigQuery environment or the orchestrating service.

## 5. Known gaps & unresolved references

*   **Core SQL Logic Migration (`d_ausd_bp_ta_rn_da_vda_tk.sql`):** The most significant gap. The provided design and generated code *assume* that the core SQL logic will be separately migrated to BigQuery SQL and then inserted into the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure. This critical step has not been performed and requires dedicated analysis and implementation.
*   **`v_TabName` Placeholder:** The `v_TabName` variable in the stored procedure is currently a placeholder (`'PoolBasisprodukt'`). This needs to be updated to the actual name of the primary table being processed or generated by the integrated core SQL logic.
*   **Job Management Framework (`h_alis_job.ksh`, `FOSJobDeaktivate`):** The original script contained commented-out references to a broader job management framework. While `job_table` provides a basic replacement for `FOSJobErzeugeEintrag`, a full re-implementation of the legacy job control system (e.g., deactivation logic) is not included and would require further design if needed.
*   **Commented-out File Processing:** The original KornShell script had commented-out sections for file reformatting and joining (`sed`, `sort`, `join`). If these operations are active in other variants of the script or are ever required, their logic would need to be translated into BigQuery SQL, potentially involving external tables or more complex SQL transformations.
*   **`p_wiederanlaufWert` Usage:** The `p_wiederanlaufWert` parameter is initialized but its actual usage within the core business logic (once integrated) is not defined in the current design. Its purpose and how it affects the data processing should be clarified during the core SQL migration.

## 6. Validation

Validation ensures the migrated BigQuery Stored Procedure functions correctly and produces accurate results.

**How to run the tests:**

1.  **Unit Tests (Stored Procedure Logic):**
    *   Execute the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure directly from the BigQuery console or a client tool.
    *   **Valid Parameters:** Call with various combinations of valid `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (e.g., `CALL `project.dataset.r_ausd_bp_ta_rn_da_vda_tk`('JOB123', 'ENTRY001', '01012023', 0);`).
    *   **Invalid Parameters:** Call with missing parameters, empty strings, or an invalid `p_Stichtag` format (e.g., `CALL `project.dataset.r_ausd_bp_ta_rn_da_vda_tk`('JOB123', NULL, '01012023', 0);` or `CALL `project.dataset.r_ausd_bp_ta_rn_da_vda_tk`('JOB123', 'ENTRY001', '2023-01-01', 0);`).
2.  **Integration Tests (Data Flow and Core Logic):**
    *   Ensure the core SQL logic from `d_ausd_bp_ta_rn_da_vda_tk.sql` has been correctly integrated into the stored procedure.
    *   Prepare test data in the BigQuery source tables that the core SQL logic reads from.
    *   Execute the stored procedure with specific `p_Stichtag` values corresponding to your test data.
    *   Query the target tables that the stored procedure populates to verify the transformed data.
    *   Query the `error_log`, `process_log`, and `job_table` to check for correct logging and status updates.

**What "passing" means:**

*   **Successful Execution:** The stored procedure completes without raising unhandled exceptions for valid input parameters.
*   **Correct Error Handling:** For invalid input parameters (e.g., missing `p_JobKennung`, malformed `p_Stichtag`), the stored procedure terminates gracefully with a `RAISE` statement, and an entry is correctly recorded in the `error_log` table with the appropriate error message and code.
*   **Accurate Logging:**
    *   The `process_log` table contains expected informational messages, marking the start and end of processing, and any key milestones.
    *   The `job_table` contains a record for each successful execution, with correct `log_timestamp`, `table_name`, `business_date_start`/`end`, and `records_processed` matching the actual number of rows affected by the core SQL logic.
*   **Data Integrity and Correctness:** The data generated or transformed by the integrated core SQL logic in the target BigQuery tables matches the expected output based on the original script's behavior and the source data. This is the most critical aspect and requires thorough comparison with legacy outputs if possible.
*   **Record Count Accuracy:** The `v_records` variable (and subsequently the `records_processed` column in `job_table`) accurately reflects the number of records processed or inserted by the core business logic.

## 7. Rollback procedure

In the event of critical issues or if the migrated job does not meet requirements, the following rollback procedure can be followed:

1.  **Deactivate BigQuery Scheduled Queries/Composer DAGs:** Immediately disable or delete any scheduled invocations of the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure to prevent further execution.
2.  **Delete BigQuery Stored Procedure:** Drop the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure from BigQuery:
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_bp_ta_rn_da_vda_tk`;
    ```
3.  **Delete Logging and Job Tables (Optional but Recommended):** To clean up the BigQuery environment, delete the tables created for logging and job status. This should only be done if the historical log data is not required for post-mortem analysis.
    ```sql
    DROP TABLE IF EXISTS `project.dataset.error_log`;
    DROP TABLE IF EXISTS `project.dataset.process_log`;
    DROP TABLE IF EXISTS `project.dataset.job_table`;
    ```
4.  **Revert Target Data (if applicable):** If the migrated stored procedure modified or created target tables, assess the impact. Depending on the nature of the core SQL logic, this may involve:
    *   Deleting data inserted by the migrated job for the affected business dates.
    *   Restoring target tables from a backup taken before the migration.
    *   Truncating and reloading target tables if they were completely overwritten.
5.  **Reactivate Legacy Job:** Re-enable and restart the original `k_ausd_bp_ta_rn_da_vda_tk.ksh` KornShell script in the legacy environment. Ensure all necessary environment variables, dependencies, and scheduling are restored to their pre-migration state.
6.  **Monitor Legacy Job:** Closely monitor the reactivated legacy job to confirm it is functioning as expected and processing data correctly.