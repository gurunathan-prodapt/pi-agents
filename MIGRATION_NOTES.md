# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `r_ausd_v_ta_cntrct_templ.ksh` to Google Cloud Platform, specifically BigQuery. The original script served as an orchestration wrapper for a contract data reconciliation process, handling environment setup, parameter parsing, and logging before invoking a core processing script (`k_ausd_v_ta_cntrct_templ.ksh`).

The migration transforms this shell-based orchestration into a BigQuery Stored Procedure, `your_project.your_dataset.Vertragsdatenabgleich`. Custom logging and error handling functions are replaced by dedicated BigQuery audit and error log tables. The core processing logic, currently residing in `k_ausd_v_ta_cntrct_templ.ksh`, is identified as a separate migration effort, expected to also become a BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL files:

*   **`your_project/your_dataset/job_audit_log.sql`**
    *   **Role:** This DDL script creates the `job_audit_log` table in BigQuery. This table is designed to capture the execution status, start/end times, messages, and other metadata for each run of the `Vertragsdatenabgleich` procedure, replacing the custom `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK` and general logging functionality of the original KornShell script.
*   **`your_project/your_dataset/job_error_log.sql`**
    *   **Role:** This DDL script creates the `job_error_log` table in BigQuery. It stores detailed error information, including error messages, codes, and stack traces, replacing the `DWMSG_Fehlerbehandlung` functionality of the original script.
*   **`your_project/your_dataset/job_reference_date.sql`**
    *   **Role:** This DDL script creates the `job_reference_date` table in BigQuery. It is used to store reference date information (`Stichtag`) associated with job executions, replacing the `DWMSG_SetzeStichtagInfo` functionality.
*   **`your_project/your_dataset/Vertragsdatenabgleich.sql`**
    *   **Role:** This DDL script creates the `Vertragsdatenabgleich` BigQuery Stored Procedure. This procedure is the direct replacement for `r_ausd_v_ta_cntrct_templ.ksh`. It encapsulates the orchestration logic, including parameter parsing, logging to the newly created audit tables, and invoking the (yet-to-be-migrated) core processing procedure `k_ausd_v_ta_cntrct_templ`. It also includes robust error handling using BigQuery's `EXCEPTION WHEN ERROR THEN` blocks.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The entire orchestration logic of the KornShell wrapper is translated into a BigQuery Stored Procedure (`Vertragsdatenabgleich`). This leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, eliminating the need for an external shell environment.
*   **BigQuery Tables for Logging and Auditing:** All custom `DWMSG_*` logging and error handling functions are replaced by direct `INSERT` and `UPDATE` operations into dedicated BigQuery tables (`job_audit_log`, `job_error_log`, `job_reference_date`). This centralizes logging within BigQuery, making it queryable and auditable.
*   **Elimination of External Shell Script Dependencies:** Dependencies like `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` are eliminated. Their functionalities (environment setup, parameter parsing, date handling, error messaging) are either absorbed directly into the BigQuery Stored Procedure logic or replaced by BigQuery native functions and the new logging tables.
*   **Staged Migration of Core Logic:** The core processing script, `k_ausd_v_ta_cntrct_templ.ksh`, is recognized as a separate, complex migration effort. The `Vertragsdatenabgleich` procedure is designed to call a placeholder BigQuery Stored Procedure (`k_ausd_v_ta_cntrct_templ`), allowing the orchestration layer to be migrated independently. This is a notable trade-off, as the full end-to-end functionality is not available until the core script is also migrated.
*   **BigQuery Native Error Handling:** Shell `trap` commands are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, providing structured error handling within the SQL procedure.
*   **Parameter Mapping:** `getopts` logic from the KornShell script is directly mapped to `IN` parameters of the BigQuery Stored Procedure, ensuring clear input definition.

## 4. Manual steps before go-live

Before the migrated job can be fully operational, the following manual steps are required:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`your_dataset` within `your_project`) exists. If not, create it:
    ```bash
    bq mk --dataset your_project:your_dataset
    ```
2.  **Deploy Logging Tables:** Execute the DDL for the logging and audit tables:
    *   `your_project/your_dataset/job_audit_log.sql`
    *   `your_project/your_dataset/job_error_log.sql`
    *   `your_project/your_dataset/job_reference_date.sql`
    This can be done via the BigQuery UI, `bq query` command, or a deployment pipeline.
3.  **Deploy `Vertragsdatenabgleich` Stored Procedure:** Execute the DDL for `your_project/your_dataset/Vertragsdatenabgleich.sql`.
4.  **IAM Permissions:**
    *   The service account or user executing the `Vertragsdatenabgleich` procedure must have appropriate BigQuery IAM roles, including:
        *   `BigQuery Data Editor` (to insert/update into logging tables and potentially other data tables if the core procedure modifies data).
        *   `BigQuery Job User` (to run queries and stored procedures).
        *   `BigQuery Data Viewer` (to read from source tables).
5.  **Migrate Core Script (`k_ausd_v_ta_cntrct_templ.ksh`):** This is a critical prerequisite. The core script must be migrated to a BigQuery Stored Procedure named `your_project.your_dataset.k_ausd_v_ta_cntrct_templ` and deployed. The `Vertragsdatenabgleich` procedure will call this.
6.  **Orchestration Setup (Optional but Recommended):** If complex scheduling or external triggers are required, set up an orchestration mechanism (e.g., Cloud Composer/Airflow DAG, Cloud Workflows, Cloud Scheduler) to call the `Vertragsdatenabgleich` BigQuery Stored Procedure. This will involve defining the schedule and passing the required parameters (`p_job_kennung`, `p_stichtag`, `p_typ`).
7.  **Configuration (`your_project`, `your_dataset`):** Ensure that all generated SQL files are updated with the correct `your_project` and `your_dataset` values before deployment.

## 5. Known gaps & unresolved references

*   **Core Script (`k_ausd_v_ta_cntrct_templ.ksh`) Content:** The most significant gap is the content and complexity of `k_ausd_v_ta_cntrct_templ.ksh`. This script contains the actual business logic for data reconciliation and requires a separate, detailed analysis and migration design. The current `Vertragsdatenabgleich` procedure calls a placeholder, and the full solution will not be functional until this core script is migrated.
*   **Missing Complexity Data:** The original `file_complexity` analysis for `r_ausd_v_ta_cntrct_templ.ksh` was unavailable. This means there might be hidden complexities or nuances not captured in the design document that could impact the migration.
*   **Detailed `DWMSG_*` Functionality:** While a general approach to replace `DWMSG_*` functions with BigQuery logging tables has been outlined, the exact schema and detail level required for these tables depend on the full functionality of the `DWMSG_*` suite, which is not entirely exposed in this wrapper script. Further investigation might be needed to ensure complete functional parity for logging.
*   **`-s` and `-l` Parameters:** The original KornShell script declares `-s` (start time) and `-l` (log level) parameters via `getopts` but does not explicitly use them in the provided code snippet. Their purpose in the legacy system (e.g., passed to sourced scripts or the core script) needs to be clarified. If they are critical, they should be added as parameters to the `Vertragsdatenabgleich` procedure and handled appropriately.
*   **Hollow Job Fast Path:** The absence of explicit lineage information for this job suggests that automated dependency detection might have been incomplete. A thorough manual review of the original script's environment and dependencies is recommended to ensure no critical components were missed.

## 6. Validation

To validate the migrated `Vertragsdatenabgleich` procedure, follow these steps:

1.  **Deploy all generated artifacts** as per Section 4, including the placeholder or actual migrated `k_ausd_v_ta_cntrct_templ` procedure.
2.  **Execute the `Vertragsdatenabgleich` Stored Procedure:**
    ```sql
    CALL `your_project.your_dataset.Vertragsdatenabgleich`(
        'TEST_JOB_KENNUNG', -- p_job_kennung
        CURRENT_DATE(),     -- p_stichtag (e.g., '2023-10-26')
        'TYPE_A'            -- p_typ
    );
    ```
    Adjust parameters as needed for testing different scenarios.
3.  **Verify Logging Tables:**
    *   **`job_audit_log`**: Query this table to ensure a new entry for `TEST_JOB_KENNUNG` with `eintrags_nr` and `start_zeit` is present. Upon successful completion, `status` should be 'OK' and `ende_zeit` populated.
    *   **`job_reference_date`**: Verify an entry exists for the `p_stichtag` passed.
    *   **`job_error_log`**: If you intentionally introduce an error (e.g., by making the called `k_ausd_v_ta_cntrct_templ` procedure raise an error), verify that an error entry is logged here, and the `job_audit_log` entry for the job shows 'ERROR' status.
4.  **Verify Core Logic Execution (once `k_ausd_v_ta_cntrct_templ` is migrated):**
    *   Check the output or side effects of the `k_ausd_v_ta_cntrct_templ` procedure. This might involve querying target tables that the core logic is expected to populate or modify.
    *   **Data Validation:** Compare the data produced or transformed by the migrated BigQuery solution with the output of the original legacy KornShell script. This is crucial for ensuring functional parity.

**"Passing" means:**
*   The `Vertragsdatenabgleich` procedure executes without unhandled errors.
*   The `job_audit_log` table accurately reflects the job's start, end, and final status ('OK' for success, 'ERROR' for failure).
*   The `job_reference_date` table contains the correct reference date information.
*   In case of errors, the `job_error_log` table contains detailed error messages and stack traces.
*   Crucially, once `k_ausd_v_ta_cntrct_templ` is migrated, the end-to-end data processing results in the same output as the legacy system, confirming functional equivalence.

## 7. Rollback procedure

In case of critical issues or if the migrated solution does not meet requirements, the following rollback procedure can be executed:

1.  **Stop New Executions:** Halt any new scheduled or manual executions of the `your_project.your_dataset.Vertragsdatenabgleich` BigQuery Stored Procedure.
2.  **Delete BigQuery Stored Procedures:**
    ```sql
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.Vertragsdatenabgleich`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.k_ausd_v_ta_cntrct_templ`; -- If migrated
    ```
3.  **Delete BigQuery Logging Tables:**
    ```sql
    DROP TABLE IF EXISTS `your_project.your_dataset.job_audit_log`;
    DROP TABLE IF EXISTS `your_project.your_dataset.job_error_log`;
    DROP TABLE IF EXISTS `your_project.your_dataset.job_reference_date`;
    ```
    *Note: Consider archiving these tables before dropping if historical audit data is required.*
4.  **Revert Orchestration:** If an external orchestrator (e.g., Cloud Composer) was set up, disable or remove the DAG/workflow that calls the BigQuery procedure.
5.  **Reactivate Legacy System:** Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh` script and its associated scheduling mechanisms.
6.  **Verify Legacy System:** Confirm that the original KornShell script is running as expected and producing correct results.