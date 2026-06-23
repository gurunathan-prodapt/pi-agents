# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh` has been migrated to Google BigQuery. This job is responsible for provisioning selected base product contract events for the BERT system, extracting a snapshot of contract cache data from the Data Warehouse (DWH) and making it available for demand scoring. The migration involved translating the shell script's orchestration, parameter handling, date determination, restart functionality, and core business logic into BigQuery Stored Procedures and a dedicated logging table.

## 2. Generated artifacts

The migration produced the following BigQuery artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role:** Defines the `job_log` table in BigQuery. This table serves as the central repository for all job execution logs, replacing the original file-based logging. It captures job number, name, status, timestamp, input parameters (`stichtag`, `restart_value`), and detailed messages.
*   **`sql/procedures/sp_log_job_event.sql`**
    *   **Role:** A helper BigQuery Stored Procedure designed to centralize and standardize logging. It inserts records into the `job_log` table, ensuring consistent logging across all migrated procedures.
*   **`sql/procedures/sp_validate_stichtag.sql`**
    *   **Role:** A helper BigQuery Stored Procedure responsible for validating and normalizing the `stichtag` (key date) parameter. It handles defaulting to the current date if no `stichtag` is provided and ensures the input is in `DDMMYYYY` format, raising an error for invalid formats.
*   **`sql/procedures/k_ausd_bp_ta_cntrct_evn.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core business logic of the original KornShell script. It performs the data extraction, filtering, and insertion operations. Specifically, it handles the restart mechanism (deleting records based on `DWH_VERTRAG_ID`) and inserts new/updated contract event data from `contract_cache` into `fos_table`, applying date and restart value filters.
*   **`sql/procedures/ausd_bp_ta_cntrct_evn.sql`**
    *   **Role:** This is the main wrapper BigQuery Stored Procedure, serving as the entry point for the migrated job. It handles parameter parsing, defaulting (`stichtag`, `wiederanlaufWert`), validation, and orchestrates the call to the core logic procedure (`k_ausd_bp_ta_cntrct_evn`). It also manages overall job logging and exception handling.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Logic:** The entire functionality of the KornShell script, including wrapper logic and core business logic, was translated into BigQuery Stored Procedures. This leverages BigQuery's native capabilities for data processing and orchestration within the data warehouse environment.
*   **Separation of Wrapper and Core Logic:** The original script's structure (wrapper invoking a core script) was maintained by creating two distinct BigQuery Stored Procedures: `ausd_bp_ta_cntrct_evn` (wrapper) and `k_ausd_bp_ta_cntrct_evn` (core logic). This promotes modularity, reusability, and clearer separation of concerns.
*   **Centralized BigQuery Logging:** File-based logging was replaced with structured logging into a dedicated BigQuery table (`job_log`). This enables easier querying, monitoring, and integration with other GCP logging services. A helper procedure (`sp_log_job_event`) was created to ensure consistent logging.
*   **Robust Parameter Handling:** Command-line arguments were translated into `IN` parameters for the BigQuery procedures. Defaulting logic (e.g., for `stichtag` and `wiederanlaufWert`) was implemented using `IFNULL` and conditional logic. A dedicated helper procedure (`sp_validate_stichtag`) was introduced for date validation and normalization.
*   **BigQuery Native Error Handling:** Shell `trap` mechanisms were replaced by BigQuery SQL's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, allowing for structured error capture, logging, and re-raising of errors using `SIGNAL SQLSTATE`.
*   **Set-Based Data Operations:** The restart mechanism (deletion) and data provisioning (insertion) were implemented using BigQuery's set-based `DELETE` and `INSERT INTO ... SELECT` statements, which are highly optimized for large-scale data processing.
*   **Helper Procedures for Common Tasks:** Common functionalities like logging and date validation were encapsulated in separate helper procedures (`sp_log_job_event`, `sp_validate_stichtag`) to reduce code duplication and improve maintainability.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it:
    ```sql
    CREATE SCHEMA `project.dataset`;
    ```
2.  **Source and Target Table Existence:**
    *   Verify that the source table `project.dataset.contract_cache` exists and contains the expected schema and data.
    *   Verify that the target table `project.dataset.fos_table` exists and has a compatible schema for the `INSERT` operation. If not, create it.
3.  **Deploy BigQuery Artifacts:**
    *   Execute the `sql/ddl/job_log.sql` script to create the `job_log` table.
    *   Execute the `sql/procedures/sp_log_job_event.sql`, `sql/procedures/sp_validate_stichtag.sql`, `sql/procedures/k_ausd_bp_ta_cntrct_evn.sql`, and `sql/procedures/ausd_bp_ta_cntrct_evn.sql` scripts in BigQuery to create or replace the stored procedures.
4.  **IAM Permissions:**
    *   Grant the service account or user that will execute the BigQuery procedures the necessary IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` (for `INSERT`, `DELETE` on `fos_table` and `job_log`).
        *   `BigQuery Data Viewer` on `project.dataset` (for `SELECT` on `contract_cache`).
        *   `BigQuery Job User` (to run BigQuery jobs/procedures).
5.  **Orchestration Setup:**
    *   Configure an external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, or Cloud Scheduler) to schedule and trigger the main BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_evn`.
    *   The orchestrator must be configured to pass the `p_stichtag` (optional, e.g., `YYYY-MM-DD` or `DDMMYYYY`) and `p_wiederanlaufWert` (optional, `INT64`) parameters to the procedure.
    *   Ensure the orchestrator has the necessary BigQuery connection details and permissions.
6.  **Review and Update Schema Assumptions:** The `k_ausd_bp_ta_cntrct_evn` procedure uses `src.*` for insertion. This assumes the target table `fos_table` has an identical or compatible schema to the source `contract_cache`. If not, the `INSERT` statement must be updated with explicit column mapping.

## 5. Known gaps & unresolved references

*   **Detailed Core Logic (`k_ausd_bp_ta_cntrct_evn.ksh`) Content:** The migration assumed standard SQL operations for the core business logic. If the original `k_ausd_bp_ta_cntrct_evn.ksh` contained highly complex procedural logic, custom UDFs (User-Defined Functions) in BigQuery SQL or external processing (e.g., via Dataproc or Cloud Functions orchestrated by Cloud Composer) might be required. This should be verified during testing.
*   **`usage()` Functionality:** The detailed help text provided by the original KornShell script's `usage()` function is not directly replicated within the BigQuery Stored Procedures. This documentation should be maintained externally (e.g., in a `README.md` file, procedure comments, or the orchestrator's documentation).
*   **Shell Traps:** The advanced signal handling capabilities of KornShell `trap` are not directly available in BigQuery SQL. The `BEGIN...EXCEPTION` blocks provide robust error handling but may not cover all edge cases of signal interruption in the same way.
*   **Dynamic Log File Naming:** The original script's dynamic log file creation is replaced by structured logging into a BigQuery table. While this offers superior querying and management, the exact dynamic naming convention is not preserved.
*   **Absence of Lineage Edges:** The initial lineage analysis did not explicitly capture the invocation relationship between the wrapper and core scripts, nor the specific tables read/written by the core script. This required manual inference during migration. Future lineage analysis tools should aim for more comprehensive coverage.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deployment:** Ensure all BigQuery artifacts (table and procedures) are deployed as per Section 4.
2.  **Test Data:** Prepare representative test data in `project.dataset.contract_cache` and ensure `project.dataset.fos_table` is either empty or contains expected baseline data.
3.  **Execution Scenarios:**
    *   **Scenario 1: Default `stichtag` and no restart.**
        *   Execute the main procedure without `p_stichtag` and `p_wiederanlaufWert`:
            ```sql
            CALL `project.dataset.ausd_bp_ta_cntrct_evn`(NULL, NULL);
            ```
        *   Expected: `stichtag` defaults to `CURRENT_DATE()`, `wiederanlaufWert` defaults to `0`. Data is inserted into `fos_table` based on current date and no restart.
    *   **Scenario 2: Specific `stichtag` and no restart.**
        *   Execute with a valid `stichtag` (e.g., '01012023') and no restart:
            ```sql
            CALL `project.dataset.ausd_bp_ta_cntrct_evn`('01012023', NULL);
            ```
        *   Expected: Data is inserted based on '01012023' and no restart.
    *   **Scenario 3: Specific `stichtag` and restart value.**
        *   Execute with a valid `stichtag` and a `wiederanlaufWert` (e.g., `12345`):
            ```sql
            CALL `project.dataset.ausd_bp_ta_cntrct_evn`('01012023', 12345);
            ```
        *   Expected: Records in `fos_table` with `DWH_VERTRAG_ID >= 12345` are deleted, then new data is inserted with `DWH_VERTRAG_ID > 12345`.
    *   **Scenario 4: Invalid `stichtag`.**
        *   Execute with an invalid `stichtag` (e.g., '20230101'):
            ```sql
            CALL `project.dataset.ausd_bp_ta_cntrct_evn`('20230101', NULL);
            ```
        *   Expected: The procedure should fail with an error message indicating an invalid `stichtag` format.
4.  **Verification of "Passing":**
    *   **Successful Execution:** The BigQuery job for the procedure call completes successfully (green checkmark in BigQuery UI).
    *   **Log Entries:** Query the `project.dataset.job_log` table to verify:
        *   Entries for `START`, `INFO` (if restart value > 0), and `SUCCESS` for both wrapper and core procedures.
        *   Correct `job_name`, `stichtag`, `restart_value`, and `message` are logged.
    *   **Data Integrity:**
        *   Query `project.dataset.fos_table` to confirm that data has been inserted correctly according to the date and restart value filters.
        *   Verify that the `DELETE` operation (if `p_wiederanlaufWert > 0`) correctly removed the expected records.
        *   Compare the count and content of inserted records against expected results from the original script's output or a manual verification.
    *   **Error Handling:** For invalid input scenarios, confirm that the procedure fails gracefully, logs an `ERROR` status in `job_log`, and provides a meaningful error message.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after the migration, the following rollback procedure can be initiated:

1.  **Stop New Executions:** Immediately halt any scheduled executions of the BigQuery job (`project.dataset.ausd_bp_ta_cntrct_evn`) in the orchestrator (e.g., pause the Airflow DAG).
2.  **Revert to Original Script:** Re-enable and resume the execution of the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh` on its legacy platform.
3.  **BigQuery Artifact Cleanup (Optional, but Recommended):**
    *   Drop the BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_cntrct_evn`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_cntrct_evn`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_log_job_event`;
        DROP PROCEDURE IF EXISTS `project.dataset.sp_validate_stichtag`;
        ```
    *   Drop the `job_log` table (if no longer needed or if a clean slate is desired for re-migration):
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        ```
4.  **Data Rollback (Critical):**
    *   If the migrated job introduced incorrect data into `project.dataset.fos_table`, a data rollback strategy must be executed. This could involve:
        *   Restoring `fos_table` from a previous backup (if available and recent enough).
        *   Executing targeted `DELETE` or `UPDATE` statements to correct the erroneous data.
        *   *Note:* This step is highly dependent on the specific impact of the failure and the data retention/backup policies in place. It should be performed with extreme caution and after thorough analysis.
5.  **Post-Rollback Analysis:** Investigate the root cause of the failure in the BigQuery environment, address the identified issues, and plan for a re-migration if necessary.