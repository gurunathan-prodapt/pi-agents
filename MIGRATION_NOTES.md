# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh` has been migrated to Google BigQuery. This script, responsible for orchestrating the contract data reconciliation job for the `ta_inv_assign` table, has been re-implemented as a BigQuery stored procedure. Its associated logging and error handling mechanisms have been translated into dedicated BigQuery tables. The core business logic, originally invoked by the shell script, is now represented by a separate placeholder BigQuery stored procedure, awaiting its own migration.

**Target Platform:** Google BigQuery

## 2. Generated artifacts

The migration produced the following BigQuery SQL files:

*   **`ddl/dw_job_entries.sql`**
    *   **Role:** Creates the `dw_job_entries` BigQuery table. This table serves as the central repository for job metadata, tracking the start time, end time, and final status (`STARTED`, `OK`, `ERROR`) of each execution of the `vertragsdatenabgleich_wrapper` procedure. It replaces the status tracking functionality previously managed by `DWMSG_SetzeStatusOK` and similar functions.
*   **`ddl/dw_job_audit.sql`**
    *   **Role:** Creates the `dw_job_audit` BigQuery table. This table stores detailed, chronological log messages generated during the execution of the wrapper and core procedures. It replaces the file-based logging (`LogDatei` content) and `DWMSG_ErzeugeEintrag` functionality, providing a queryable audit trail.
*   **`ddl/dw_error_log.sql`**
    *   **Role:** Creates the `dw_error_log` BigQuery table. This table is dedicated to capturing specific error details (message, code, stack trace) when an exception occurs during job execution. It centralizes error reporting, replacing the output of `DWMSG_MeldeFehler`.
*   **`sp/k_ausd_v_ta_inv_assign.sql`**
    *   **Role:** Creates the `k_ausd_v_ta_inv_assign` BigQuery stored procedure. This is a **placeholder** for the actual business transformation logic that was originally contained within the `k_ausd_v_ta_inv_assign.ksh` core script. Its implementation is critical for the full functionality of the migrated job.
*   **`sp/vertragsdatenabgleich_wrapper.sql`**
    *   **Role:** Creates the `vertragsdatenabgleich_wrapper` BigQuery stored procedure. This is the primary migrated artifact, directly replacing the `r_ausd_v_ta_inv_assign.ksh` shell script. It handles parameter parsing, initializes job metadata, records start/end status, manages detailed logging, implements error handling, and invokes the core `k_ausd_v_ta_inv_assign` stored procedure.

## 3. Key design decisions

*   **Orchestration within BigQuery Stored Procedure:** The entire wrapper logic, including environment initialization, parameter parsing, logging setup, and core script invocation, has been translated into a single BigQuery stored procedure (`vertragsdatenabgleich_wrapper`). This decision leverages BigQuery's procedural language capabilities, keeping the orchestration logic close to the data processing engine and simplifying the overall architecture by reducing external dependencies for basic job control.
*   **Centralized BigQuery Logging Tables:** File-based logging and status tracking from the original KornShell script have been replaced by three dedicated BigQuery tables (`dw_job_entries`, `dw_job_audit`, `dw_error_log`). This provides a structured, queryable, and scalable logging solution, enabling easier monitoring, auditing, and troubleshooting compared to parsing flat files.
*   **Modular Core Logic (Placeholder):** The core business logic, originally in `k_ausd_v_ta_inv_assign.ksh`, is represented as a separate BigQuery stored procedure (`k_ausd_v_ta_inv_assign`). This promotes modularity, allowing the core transformation logic to be developed, tested, and deployed independently from the wrapper, aligning with best practices for complex ETL jobs.
*   **BigQuery-Native Error Handling:** The shell script's `trap` commands and explicit error checks are replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides robust, structured error handling, automatically capturing error messages and stack traces, and allowing for consistent error logging and signaling (`SIGNAL SQLSTATE`).
*   **Parameter Mapping:** Shell `getopts` for command-line argument parsing is directly mapped to BigQuery stored procedure input parameters, ensuring clear input definition and validation.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_gcp_project_id.your_bq_dataset_name`) exists. If not, create it.
    *   **Command Example:** `bq mk --dataset your_gcp_project_id:your_bq_dataset_name`
2.  **Placeholder Replacement:**
    *   In all generated SQL files (`ddl/*.sql`, `sp/*.sql`), replace `your_gcp_project_id.your_bq_dataset_name` with your actual GCP project ID and BigQuery dataset name.
3.  **Deploy DDLs:**
    *   Execute the `ddl/dw_job_entries.sql`, `ddl/dw_job_audit.sql`, and `ddl/dw_error_log.sql` scripts in BigQuery to create the logging tables.
4.  **Implement Core Logic:**
    *   **Crucial Step:** The `sp/k_ausd_v_ta_inv_assign.sql` file currently contains only a placeholder. The actual business transformation logic from the original `k_ausd_v_ta_inv_assign.ksh` script must be fully migrated and implemented within this stored procedure. This includes all data reads, transformations, and writes.
5.  **Deploy Stored Procedures:**
    *   Execute the `sp/k_ausd_v_ta_inv_assign.sql` (after implementation) and `sp/vertragsdatenabgleich_wrapper.sql` scripts in BigQuery to create the stored procedures.
6.  **IAM Permissions:**
    *   Ensure the service account or user that will execute the `vertragsdatenabgleich_wrapper` stored procedure has the necessary BigQuery IAM roles:
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the target dataset to write to the logging tables and execute stored procedures.
        *   `BigQuery Job User` (roles/bigquery.jobUser) to run BigQuery jobs.
7.  **Scheduling:**
    *   Set up a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer/Airflow, Cloud Workflows) to invoke the `vertragsdatenabgleich_wrapper` stored procedure at the required frequency. The invocation command will be similar to:
        `CALL \`your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper\`(p_h => FALSE, p_s => 'value_for_s', p_l => 'value_for_l');`
8.  **Configuration Management:**
    *   Review any environment variables or configuration values sourced from `.dw_init` or other files in the original environment. Determine how these will be managed in GCP (e.g., BigQuery parameter tables, Secret Manager, environment variables in Cloud Run/Composer).

## 5. Known gaps & unresolved references

*   **Core Logic Implementation (`k_ausd_v_ta_inv_assign`):** The most significant gap is the complete migration and implementation of the business logic within the `sp/k_ausd_v_ta_inv_assign.sql` stored procedure. This is a prerequisite for the job to perform its intended function.
*   **`DWMSG_*` Function Nuances:** While general logging and error handling are covered, specific behaviors or advanced features of the original `DWMSG_*` utility functions (e.g., `DWMSG_ErmittleNr`'s exact number generation logic, specific log message formatting, or additional metadata captured) might require further analysis and refinement if the current BigQuery logging tables are not fully equivalent.
*   **Unused Parameters (`-s`, `-l`):** The original script parsed these parameters but did not explicitly use them. Their intended purpose, especially if they were meant to be passed to or used by the core script, needs to be clarified. The migrated wrapper procedure includes them as parameters but does not use them internally.
*   **Legacy Error Numbering:** The original script referenced specific error numbers (e.g., `192`, `193`). These are not directly mapped in the BigQuery migration. A strategy for mapping these to BigQuery-compatible error codes or messages might be needed for consistency with other legacy systems.
*   **Framework Script Functionality:** The utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) provided common functions. Their full functionality needs to be absorbed into the BigQuery procedures or re-implemented as BigQuery UDFs/helper procedures if not already covered by the current migration.
*   **Environment Configuration (`.dw_init`):** The method for managing environment-specific configurations (e.g., `BERT_DIR_ROOT`) that were sourced from `.dw_init` needs to be formally defined within the GCP environment.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deployment:** Ensure all DDLs and stored procedures (including the implemented `k_ausd_v_ta_inv_assign`) are deployed to the target BigQuery dataset.

2.  **Test Cases:**

    *   **Help Message:**
        *   **Execution:** `CALL \`your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper\`(p_h => TRUE, p_s => NULL, p_l => NULL);`
        *   **Passing Criteria:** The BigQuery console or client should output the usage message, and no entries should be made in the logging tables.
    *   **Successful Execution:**
        *   **Execution:** `CALL \`your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper\`(p_h => FALSE, p_s => 'test_s', p_l => 'test_l');` (Adjust parameters as needed for the core logic).
        *   **Passing Criteria:**
            *   The procedure completes without raising an error.
            *   `dw_job_entries` table: A new entry is present with `status = 'OK'` and `finished_at` populated.
            *   `dw_job_audit` table: Contains a sequence of log messages indicating job start, core logic execution (from `k_ausd_v_ta_inv_assign`), and successful completion.
            *   `dw_error_log` table: No new entries related to this execution.
            *   **Core Logic Validation:** The `k_ausd_v_ta_inv_assign` procedure should have performed its intended data transformations or reconciliation correctly (e.g., verify target tables, data counts, etc.).
    *   **Error Handling (Simulated Failure):**
        *   **Preparation:** Temporarily modify `sp/k_ausd_v_ta_inv_assign.sql` to explicitly raise an error (e.g., `RAISE EXCEPTION 'Simulated core logic error';`) to test the wrapper's error handling.
        *   **Execution:** `CALL \`your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper\`(p_h => FALSE, p_s => 'fail_s', p_l => 'fail_l');`
        *   **Passing Criteria:**
            *   The `vertragsdatenabgleich_wrapper` procedure should terminate with an error, and the `SIGNAL SQLSTATE` message should be visible.
            *   `dw_job_entries` table: A new entry is present with `status = 'ERROR'` and `finished_at` populated.
            *   `dw_job_audit` table: Contains log messages indicating job start and an "AppError: Abbruch" message.
            *   `dw_error_log` table: A new entry is present with the captured `error_message` and `error_code` from the simulated failure.

## 7. Rollback procedure

In case of critical issues or if the migrated job does not perform as expected, the following rollback procedure can be executed:

1.  **Disable New Scheduler:** Immediately disable or delete any new scheduling mechanisms (e.g., Cloud Scheduler job, Cloud Composer DAG) that trigger the `vertragsdatenabgleich_wrapper` BigQuery stored procedure.
2.  **Re-enable Legacy Job:** Re-enable the original `r_ausd_v_ta_inv_assign.ksh` script in its legacy environment and ensure its scheduler is active and functional.
3.  **Delete BigQuery Stored Procedures:**
    *   Drop the `vertragsdatenabgleich_wrapper` stored procedure:
        `DROP PROCEDURE IF EXISTS \`your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper\`;`
    *   Drop the `k_ausd_v_ta_inv_assign` stored procedure:
        `DROP PROCEDURE IF EXISTS \`your_gcp_project_id.your_bq_dataset_name.k_ausd_v_ta_inv_assign\`;`
4.  **Delete BigQuery Logging Tables (Optional but Recommended):**
    *   If the logging data from the migration attempt is not needed, drop the tables:
        `DROP TABLE IF EXISTS \`your_gcp_project_id.your_bq_dataset_name.dw_job_entries\`;`
        `DROP TABLE IF EXISTS \`your_gcp_project_id.your_bq_dataset_name.dw_job_audit\`;`
        `DROP TABLE IF EXISTS \`your_gcp_project_id.your_bq_dataset_name.dw_error_log\`;`
5.  **Data Rollback (If Applicable):** If the `k_ausd_v_ta_inv_assign` core logic performed any data modifications that need to be reverted, execute the appropriate data rollback procedures (e.g., restoring from a backup, running inverse transformations). This step is highly dependent on the specific implementation of the core logic and is outside the scope of this wrapper migration.