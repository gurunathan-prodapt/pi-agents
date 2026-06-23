# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_disc_zusgf.ksh`. The original script served as an orchestration wrapper, managing job execution, handling parameters, and executing a core SQL script (`d_ausd_v_ta_disc_zusgf.sql`) to update the `ta_disc_zusgf` table.

The migration targets Google Cloud BigQuery. The control and orchestration logic of the KornShell script has been transformed into a BigQuery Stored Procedure. The core SQL logic, currently a placeholder, is intended to be migrated to a separate BigQuery SQL script or Stored Procedure. Logging and error handling are now managed via dedicated BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`your_project_id.your_dataset_id.job_error_log.sql`**
    *   **Role:** This DDL script creates the `job_error_log` table in BigQuery. This table is used to capture and store detailed information about any errors encountered during the execution of the migrated job, replacing the shell script's `DWMSG_MeldeFehler` functionality.
*   **`your_project_id.your_dataset_id.job_run_log.sql`**
    *   **Role:** This DDL script creates the `job_run_log` table in BigQuery. This table tracks the execution status and metrics of the migrated job, such as start/end times, processed records, and overall status, replacing temporary file-based record counting and implicit job status tracking.
*   **`your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf.sql`**
    *   **Role:** This is a **placeholder** for the migrated core SQL logic originally contained in `d_ausd_v_ta_disc_zusgf.sql`. It is designed to be replaced by a BigQuery Standard SQL script or Stored Procedure that performs the actual data manipulation on the `ta_disc_zusgf` table. Its current form is a no-op procedure.
*   **`your_project_id.your_dataset_id.r_ausd_vertrag_control.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the control flow, parameter handling, validation, and orchestration logic of the original `k_ausd_v_ta_disc_zusgf.ksh` script. It accepts input parameters, performs validation, calls the core `d_ausd_v_ta_disc_zusgf` logic, and logs execution details and errors to the respective BigQuery logging tables.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The control flow and orchestration logic of the original KornShell script were migrated directly into a BigQuery Stored Procedure (`r_ausd_vertrag_control`). This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, eliminating the need for external shell scripting and simplifying the overall architecture.
*   **Replacement of Shell Utilities with Native BigQuery Features:** Custom shell utility scripts (e.g., for error handling, date functions, parameter parsing, SQL*Plus wrapping) were replaced by BigQuery's built-in functions, `IF` statements, `SIGNAL SQLSTATE` for error propagation, and dedicated logging tables (`job_error_log`, `job_run_log`). This reduces external dependencies and aligns with a cloud-native approach.
*   **Direct Parameter Passing:** The `getopts` command-line argument parsing in the original script was replaced by direct `IN` parameters to the BigQuery Stored Procedure, providing a cleaner and more explicit interface.
*   **Modular Migration of Core SQL Logic:** The business logic within `d_ausd_v_ta_disc_zusgf.sql` was identified as a separate, critical migration task. This allows for focused effort on converting its (likely Oracle) SQL dialect to BigQuery Standard SQL, promoting modularity and enabling parallel development.
*   **Trade-off: Placeholder for `d_ausd_v_ta_disc_zusgf.sql`:** A placeholder was generated for `d_ausd_v_ta_disc_zusgf.sql`. While this allows the orchestration layer to be built and tested, it defers the detailed and potentially complex migration of the core business logic, creating a significant dependency and known gap that must be addressed before full functionality.
*   **Trade-off: Record Counting Mechanism:** The original script used a temporary file to store record counts. The migrated solution uses `COUNT(*)` on the target table (`ta_disc_zusgf`) based on a specific condition. This might not precisely reflect the "records processed" or "affected rows" if the underlying DML in `d_ausd_v_ta_disc_zusgf` is complex (e.g., MERGE statements with multiple actions). This requires careful validation to ensure accuracy.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`your_dataset_id` within `your_project_id`) exists. If not, create it.
2.  **Target Table `ta_disc_zusgf`:** Verify that the `your_project_id.your_dataset_id.ta_disc_zusgf` table exists in BigQuery with the correct schema and is populated with any necessary initial data. This table is the ultimate target of the core SQL logic.
3.  **IAM Permissions:** Grant appropriate BigQuery Identity and Access Management (IAM) roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to the service account or user that will be executing these stored procedures.
4.  **Deploy Logging Tables:** Execute the DDL scripts `your_project_id.your_dataset_id.job_error_log.sql` and `your_project_id.your_dataset_id.job_run_log.sql` to create the necessary logging tables in BigQuery.
5.  **Migrate and Deploy Core SQL Logic (`d_ausd_v_ta_disc_zusgf.sql`):** This is a **critical** step. The placeholder `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf.sql` must be replaced with the actual, fully migrated BigQuery Standard SQL logic. This will involve converting the original SQL from its source dialect (likely Oracle) to BigQuery Standard SQL and deploying it as a BigQuery Stored Procedure or a standalone script.
6.  **Deploy Orchestration Stored Procedure:** Execute `your_project_id.your_dataset_id.r_ausd_vertrag_control.sql` to create the main orchestration stored procedure in BigQuery.
7.  **Update Caller Script (`r_ausd_vertrag.ksh`):** The original calling script, `r_ausd_vertrag.ksh`, which previously invoked `k_ausd_v_ta_disc_zusgf.ksh`, must be updated. It should now call the new BigQuery Stored Procedure (`your_project_id.your_dataset_id.r_ausd_vertrag_control`) using a BigQuery client library (e.g., Python, Java) or the `bq` command-line tool, passing the required `p_JobKennung` and `p_EintragsNr` parameters.
8.  **Scheduling (if applicable):** If the job requires external scheduling (e.g., daily, hourly), configure a new job in your chosen orchestrator (e.g., Cloud Composer/Apache Airflow, Cloud Workflows) to trigger the `your_project_id.your_dataset_id.r_ausd_vertrag_control` stored procedure with the appropriate parameters.

## 5. Known gaps & unresolved references

The following items have been flagged for follow-up or represent areas requiring further design and implementation:

*   **Content of `d_ausd_v_ta_disc_zusgf.sql`:** The actual data transformation logic within this SQL script has not been migrated. Its complexity, source database dialect (likely Oracle), and specific DML operations will significantly influence its migration effort to BigQuery Standard SQL. This is the **primary unresolved item and risk** and requires a dedicated migration effort.
*   **`starteSQLSkript` Implementation Details:** The exact logic within the original `starteSQLSkript` function, particularly concerning job registration, deactivation, and database interaction, was not fully detailed in the provided shell script. This functionality needs to be reverse-engineered and accurately re-implemented within the BigQuery Stored Procedures.
*   **`r_ausd_vertrag.ksh` Context and Parameter Mapping:** The fact that `k_ausd_v_ta_disc_zusgf.ksh` is called by `r_ausd_vertrag.ksh` implies a dependency. The migration of `r_ausd_vertrag.ksh` is crucial, and the parameters it passes to `k_ausd_v_ta_disc_zusgf.ksh` must be precisely identified and mapped to the `IN` parameters of the new BigQuery Stored Procedure.
*   **`tmpFile` Content and Origin:** The exact process by which the `tmpFile` was populated with record counts in the original script needs to be fully understood. The current BigQuery implementation uses a `COUNT(*)` on the target table, which may not perfectly replicate the original "records processed" metric if the DML logic is more nuanced (e.g., only counting *inserted* or *updated* rows).
*   **Oracle-specific SQL Features:** If `d_ausd_v_ta_disc_zusgf.sql` contains Oracle-specific SQL constructs (e.g., `ROWNUM`, `CONNECT BY`, specific functions), these will require careful translation and testing in BigQuery Standard SQL.

## 6. Validation

To validate the successful migration and functionality of the BigQuery components:

1.  **Deployment Verification:** Confirm that all BigQuery DDL and Stored Procedures (`job_error_log`, `job_run_log`, `d_ausd_v_ta_disc_zusgf` (once implemented), `r_ausd_vertrag_control`) are successfully deployed in `your_project_id.your_dataset_id`.
2.  **Prepare Test Data:** Ensure the `ta_disc_zusgf` table and any other tables that `d_ausd_v_ta_disc_zusgf` might depend on are populated with representative test data.
3.  **Execute Stored Procedure:**
    *   **Successful Scenario:** Execute the main orchestration stored procedure with valid parameters:
        ```sql
        CALL your_project_id.your_dataset_id.r_ausd_vertrag_control('TEST_JOB_001', '12345');
        ```
    *   **Error Scenario (Missing JobKennung):**
        ```sql
        CALL your_project_id.your_dataset_id.r_ausd_vertrag_control(NULL, '12345');
        ```
    *   **Error Scenario (Missing EintragsNr):**
        ```sql
        CALL your_project_id.your_dataset_id.r_ausd_vertrag_control('TEST_JOB_002', NULL);
        ```
4.  **Expected "Passing" Criteria:**
    *   **Successful Execution:** For valid inputs, the `r_ausd_vertrag_control` procedure should complete without unhandled BigQuery errors.
    *   **Error Handling:** For invalid inputs (e.g., `NULL` parameters), the procedure should log an entry to `your_project_id.your_dataset_id.job_error_log` and `SIGNAL SQLSTATE '45000'` with the appropriate error message.
    *   **Data Integrity:** The `your_project_id.your_dataset_id.ta_disc_zusgf` table should be updated correctly according to the logic of the fully migrated `d_ausd_v_ta_disc_zusgf` (once implemented). Verify row counts, column values, and any other expected data changes.
    *   **Logging:**
        *   A successful execution should insert a record into `your_project_id.your_dataset_id.job_run_log` with `status = 'DONE'` and an accurate `records_processed` count.
        *   An execution with invalid parameters should insert a record into `your_project_id.your_dataset_id.job_error_log` with the relevant error details.
    *   **Output Messages:** The `SELECT` statements within the stored procedure (e.g., `SELECT ' ---------- ENDE Datenverarbeitung ----------'`) should appear in the query results pane of the BigQuery console or client.

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Revert Caller Script:** Revert any changes made to the calling script (`r_ausd_vertrag.ksh`) to restore its original invocation of `k_ausd_v_ta_disc_zusgf.ksh`.
2.  **Drop BigQuery Stored Procedures:**
    ```sql
    DROP PROCEDURE IF EXISTS your_project_id.your_dataset_id.r_ausd_vertrag_control;
    DROP PROCEDURE IF EXISTS your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf; -- Only if it was deployed as a SP
    ```
3.  **Drop BigQuery Logging Tables:**
    ```sql
    DROP TABLE IF EXISTS your_project_id.your_dataset_id.job_error_log;
    DROP TABLE IF EXISTS your_project_id.your_dataset_id.job_run_log;
    ```
4.  **Restore `ta_disc_zusgf` (if modified):** If the `your_project_id.your_dataset_id.ta_disc_zusgf` table was modified during testing or partial deployment, restore it from a backup taken prior to the migration attempt.
5.  **Re-deploy Original Script:** Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh` script is in its correct location and is executable.
6.  **Revert Scheduling:** If any new scheduling (e.g., Cloud Composer DAGs) was configured for the BigQuery job, disable or delete it.