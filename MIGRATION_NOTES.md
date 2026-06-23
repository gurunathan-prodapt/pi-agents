# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `r_ausd_bp_ta_rn_einzeln.ksh` from its existing environment to Google BigQuery. The original script served as an orchestration layer for the initial provisioning of selected base products for BERT, handling command-line parameter parsing, robust logging, error handling, and the invocation of a core downstream processing script (`k_ausd_bp_ta_rn_einzeln.ksh`).

The migration involved transforming the shell script's logic into a BigQuery Stored Procedure, leveraging BigQuery's native capabilities for parameter handling, date operations, and structured logging.

**Target Platform:** Google BigQuery

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`ddl/job_control.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_control` table in BigQuery. This table is central to tracking the overall execution status, parameters, and timestamps of the migrated job, replacing the file-based job status reporting of the legacy system.
*   **`ddl/job_messages.sql`**
    *   **Role:** Defines the DDL for the `job_messages` table in BigQuery. This table stores informational, warning, and success messages generated during the job's execution, providing a structured and queryable log of events.
*   **`ddl/job_error_log.sql`**
    *   **Role:** Defines the DDL for the `job_error_log` table in BigQuery. This table captures detailed error information, including error numbers, arguments, and messages, replacing the legacy system's error logging mechanisms.
*   **`stored_procedures/ausd_bp_ta_rn_einzeln.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln`. This procedure is the direct migration of the `r_ausd_bp_ta_rn_einzeln.ksh` script. It handles input parameters, applies defaulting logic, performs parameter validation, manages job control entries, logs messages and errors to the audit tables, and orchestrates the call to the core business logic (represented by `project.dataset.k_ausd_bp_ta_rn_einzeln`).

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Orchestration Layer as BigQuery Stored Procedure:** The `r_ausd_bp_ta_rn_einzeln.ksh` script, being primarily an orchestrator, was migrated to a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_rn_einzeln`). This approach allows for native execution within BigQuery, direct parameter passing, and seamless invocation of other BigQuery Stored Procedures (like the core business logic).
    *   **Trade-off:** This shifts the execution context from a shell environment to a SQL-based environment, requiring re-implementation of shell-specific constructs.
*   **Structured Logging to BigQuery Tables:** The legacy file-based logging (`DWMSG_*` functions) and `trap` mechanisms were replaced by `INSERT` statements into dedicated BigQuery audit tables (`job_control`, `job_messages`, `job_error_log`).
    *   **Rationale:** Provides centralized, structured, and queryable logs, enhancing observability, auditing, and troubleshooting capabilities within the BigQuery ecosystem.
    *   **Trade-off:** Loss of direct file system interaction for logging; requires querying BigQuery tables instead of tailing log files.
*   **Parameter Handling via Stored Procedure Parameters:** Command-line arguments (`-s`, `-l`) from the original script were translated into `IN` parameters for the BigQuery Stored Procedure (`p_stichtag STRING`, `p_wiederanlaufWert INT64`). Defaulting logic was implemented using `IFNULL` and `NULLIF(TRIM(...), '')`.
    *   **Rationale:** Aligns with standard BigQuery Stored Procedure interfaces and provides clear input contracts.
*   **Native BigQuery Date Functions:** Custom shell date handling utilities (`DWDate_Gib_Zeitraum`) were replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions.
    *   **Rationale:** Leverages optimized, built-in BigQuery functions for date manipulation, reducing custom code.
*   **BigQuery `BEGIN...EXCEPTION` for Error Handling:** The shell's `trap` commands for error interception were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block. Error details are captured using `@@error.message` and logged to `job_error_log`.
    *   **Rationale:** Provides robust, structured error handling native to BigQuery SQL, ensuring job status and error messages are consistently recorded.
*   **Invocation of Core Logic as BigQuery Stored Procedure:** The invocation of the downstream `k_ausd_bp_ta_rn_einzeln.ksh` script was replaced by a `CALL` statement to a corresponding BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_rn_einzeln`).
    *   **Rationale:** Ensures seamless integration and execution of the entire data pipeline within BigQuery.
    *   **Trade-off:** This migration is dependent on the successful migration and deployment of `k_ausd_bp_ta_rn_einzeln.ksh` itself.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
    ```bash
    bq mk --dataset project:dataset
    ```
2.  **Deploy Audit Tables:** Execute the DDL scripts to create the necessary audit tables in the target dataset.
    *   `ddl/job_control.sql`
    *   `ddl/job_messages.sql`
    *   `ddl/job_error_log.sql`
    ```bash
    bq query --use_legacy_sql=false < ddl/job_control.sql
    bq query --use_legacy_sql=false < ddl/job_messages.sql
    bq query --use_legacy_sql=false < ddl/job_error_log.sql
    ```
3.  **Deploy Orchestration Stored Procedure:** Deploy the `ausd_bp_ta_rn_einzeln` BigQuery Stored Procedure.
    ```bash
    bq query --use_legacy_sql=false < stored_procedures/ausd_bp_ta_rn_einzeln.sql
    ```
4.  **Migrate and Deploy Core Logic:** **Crucially, the core processing script `k_ausd_bp_ta_rn_einzeln.ksh` must be migrated to a BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_rn_einzeln`) and deployed.** This is a prerequisite for the `ausd_bp_ta_rn_einzeln` procedure to function correctly.
5.  **IAM Permissions:** Grant the service account or user executing the BigQuery Stored Procedure the necessary IAM roles:
    *   `BigQuery Data Editor` on `project.dataset` (for `INSERT`/`UPDATE` on audit tables).
    *   `BigQuery Job User` (to run queries and stored procedures).
    *   `BigQuery Data Viewer` (if reading from other BigQuery tables).
    *   `BigQuery Routine User` on `project.dataset.k_ausd_bp_ta_rn_einzeln` (to `CALL` the core logic SP).
6.  **Scheduling:** If the job is to be scheduled, configure an external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, Dataform) to trigger the `project.dataset.ausd_bp_ta_rn_einzeln` stored procedure with the required parameters.

## 5. Known gaps & unresolved references

*   **Core Script (`k_ausd_bp_ta_rn_einzeln.ksh`) Migration (B4 Item):** The `project.dataset.ausd_bp_ta_rn_einzeln` stored procedure includes a `CALL` to `project.dataset.k_ausd_bp_ta_rn_einzeln`. The migration of the actual business logic within `k_ausd_bp_ta_rn_einzeln.ksh` is **not part of this migration document** and is a critical prerequisite. The current `CALL` is a placeholder.
*   **Dynamic Path Resolution:** The legacy script used `BERT_DIR_ROOT` for dynamic path resolution. In the migrated BigQuery SP, this has been replaced by explicit `project.dataset` references. If the original system allowed for dynamic dataset/project selection, this would require further configuration (e.g., using BigQuery connection properties or additional SP parameters).
*   **System Date vs. Max Load Date (B4 Item):** The original script's comments hinted at a potential `MIN(sysdate, maxladedatum)` logic that was not fully implemented. The migration adheres to the currently active logic (defaulting `Stichtag` to `sysdate` if not provided). A review of this historical intent might be warranted as a follow-up (B4 item) to ensure no business logic was inadvertently missed.
*   **External Orchestration Integration:** While the BigQuery Stored Procedure is self-contained, its integration into a broader data pipeline or scheduling system (e.g., Cloud Composer) is a separate task that needs to be designed and implemented.

## 6. Validation

To validate the successful migration and functionality of the `ausd_bp_ta_rn_einzeln` stored procedure:

1.  **Prerequisites:** Ensure all manual steps (dataset, audit tables, SP deployment, and a placeholder/migrated `k_ausd_bp_ta_rn_einzeln` SP) are completed.
2.  **Test Cases:**
    *   **Successful Execution (Default Parameters):**
        ```sql
        CALL `project.dataset.ausd_bp_ta_rn_einzeln`(NULL, NULL);
        ```
        *   **Expected:** `job_control` table shows a new entry with `status = 'OK'`, `stichtag` set to `CURRENT_DATE()` (DDMMYYYY format), `restart_value = 0`. `job_messages` should contain a success message.
    *   **Successful Execution (Specific Parameters):**
        ```sql
        CALL `project.dataset.ausd_bp_ta_rn_einzeln`('01012023', 10);
        ```
        *   **Expected:** `job_control` table shows a new entry with `status = 'OK'`, `stichtag = '01012023'`, `restart_value = 10`.
    *   **Parameter Validation Failure (Missing Stichtag - if `k_ausd_bp_ta_rn_einzeln` is designed to fail without it):**
        *   *Note: The current SP logic defaults `v_effective_stichtag` to `v_sysdate` if `p_stichtag` is NULL or empty. The `IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN` block will only trigger if `v_sysdate` itself is somehow empty, which is unlikely. To test the `RAISE` for missing `Stichtag`, one would need to modify the SP to not default `v_effective_stichtag` or pass an invalid date format that `FORMAT_DATE` cannot handle.*
        *   **If `k_ausd_bp_ta_rn_einzeln` is designed to fail if `stichtag` is invalid/missing:**
            ```sql
            -- Assuming k_ausd_bp_ta_rn_einzeln would fail if stichtag is invalid
            CALL `project.dataset.ausd_bp_ta_rn_einzeln`('INVALID_DATE', NULL);
            ```
            *   **Expected:** `job_control` table shows a new entry with `status = 'ERROR'`. `job_error_log` and `job_messages` should contain the error details. The `CALL` statement should `RAISE` an error.
    *   **Simulated Core Logic Failure:**
        *   Create a temporary `k_ausd_bp_ta_rn_einzeln` SP that intentionally raises an error.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_einzeln`(
          IN p_jobkennung STRING, IN p_stichtag STRING, IN p_dwh_eintragsnr INT64, IN p_wiederanlaufWert INT64
        )
        BEGIN
          RAISE USING MESSAGE = 'Simulated error in k_ausd_bp_ta_rn_einzeln';
        END;
        ```
        *   Then call the main orchestration SP:
        ```sql
        CALL `project.dataset.ausd_bp_ta_rn_einzeln`(NULL, NULL);
        ```
        *   **Expected:** `job_control` table shows a new entry with `status = 'ERROR'`. `job_error_log` and `job_messages` should contain the simulated error message from `k_ausd_bp_ta_rn_einzeln`. The `CALL` statement should `RAISE` an error.

3.  **"Passing" Criteria:**
    *   The `project.dataset.ausd_bp_ta_rn_einzeln` stored procedure executes without syntax errors.
    *   For successful runs, the `job_control` table is updated with `status = 'OK'`, correct `stichtag`, `restart_value`, `created_at`, `finished_at`, and `success_message`.
    *   For error scenarios, the `job_control` table is updated with `status = 'ERROR'`, `finished_at`, and `error_message`.
    *   `job_messages` and `job_error_log` tables contain accurate and relevant entries for both successful and failed executions.
    *   Parameter defaulting (e.g., `p_wiederanlaufWert` to 0, `p_stichtag` to `CURRENT_DATE()`) functions as expected.
    *   The `CALL` to `project.dataset.k_ausd_bp_ta_rn_einzeln` is made with the correct parameters.

## 7. Rollback procedure

In case of issues or a need to revert the migration, follow these steps:

1.  **Stop New Executions:** Halt any scheduled or manual executions of the `project.dataset.ausd_bp_ta_rn_einzeln` BigQuery Stored Procedure.
2.  **Drop BigQuery Stored Procedure:** Delete the migrated stored procedure from BigQuery.
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_rn_einzeln`;
    ```
3.  **Revert Core Logic (if applicable):** If `project.dataset.k_ausd_bp_ta_rn_einzeln` was migrated as part of this effort, drop or revert it to its previous state (e.g., a placeholder or an older version).
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_rn_einzeln`;
    ```
4.  **Optional: Drop Audit Tables:** If the `job_control`, `job_messages`, and `job_error_log` tables were created solely for this migration and contain no other critical data, they can be dropped.
    ```sql
    DROP TABLE IF EXISTS `project.dataset.job_control`;
    DROP TABLE IF EXISTS `project.dataset.job_messages`;
    DROP TABLE IF EXISTS `project.dataset.job_error_log`;
    ```
    *   **Caution:** Ensure no other processes are relying on these tables before dropping them.
5.  **Restore Legacy Execution:** Re-enable or restore the original scheduling and execution of the `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh` script in its legacy environment.