# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_vvl_dwh.ksh` to Google Cloud Platform. The original script served as an orchestration wrapper for a contract data reconciliation job, handling environment setup, parameter parsing, logging, and invoking a core processing script (`k_ausd_v_ta_vvl_dwh.ksh`).

The migration targets Google Cloud's BigQuery for data processing and storage, and Cloud Composer (Apache Airflow) for external orchestration. The wrapper's functionality has been translated into a BigQuery Stored Procedure, `project.dataset.vertragsdatenabgleich`, which manages job metadata, parameter validation, and logging into dedicated BigQuery control tables. The invocation of the core business logic is represented by a placeholder BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_vvl_dwh`, which requires separate, detailed analysis and migration.

## 2. Generated artifacts

The following files have been generated as part of this migration:

*   **`project.dataset.job_control.sql`**
    *   **Role:** BigQuery DDL script to create the `job_control` table. This table replaces the file-based job control mechanisms of the original script, storing high-level job execution details such as entry number, job identifier, script name, log name, processing date (`stichtag`), status, and timestamps.
*   **`project.dataset.job_messages.sql`**
    *   **Role:** BigQuery DDL script to create the `job_messages` table. This table centralizes detailed informational and error messages generated during job execution, replacing the content written to the legacy log files.
*   **`project.dataset.job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table specifically captures error details, including error numbers and arguments, providing a structured way to track and analyze job failures.
*   **`project.dataset.job_audit.sql`**
    *   **Role:** BigQuery DDL script to create the `job_audit` table. This table stores final audit records for each job execution, summarizing its outcome.
*   **`vertragsdatenabgleich.sql`**
    *   **Role:** BigQuery Stored Procedure. This is the core migrated wrapper logic. It handles input parameter validation (`p_stichtag`, `p_loglevel`), generates unique job identifiers, manages logging into the `job_control`, `job_messages`, and `job_audit` tables, and orchestrates the call to the core processing logic (`project.dataset.k_ausd_v_ta_vvl_dwh`). It also implements BigQuery's `BEGIN...EXCEPTION...END` block for robust error handling.
*   **`k_ausd_v_ta_vvl_dwh.sql`**
    *   **Role:** Placeholder BigQuery Stored Procedure for the core business logic. This procedure represents the future migration target for `k_ausd_v_ta_vvl_dwh.ksh`. It currently contains only a `SELECT` statement to indicate execution and will need to be fully implemented after a dedicated analysis of the original core script.
*   **`vertragsdatenabgleich_dag.py`**
    *   **Role:** Cloud Composer (Apache Airflow) DAG definition. This Python script defines the orchestration workflow for the `vertragsdatenabgleich` BigQuery Stored Procedure. It includes a `BigQueryInsertJobOperator` to trigger the stored procedure, passing dynamic parameters like the execution date (`stichtag`). This replaces the original shell script's scheduling and direct execution.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Wrapper Logic:** The orchestration and parameter handling logic of the original KornShell script was migrated to a BigQuery Stored Procedure (`vertragsdatenabgleich`). This decision was made to:
    *   **Centralize Logic:** Keep the wrapper logic close to the data processing environment (BigQuery), reducing cross-platform dependencies.
    *   **Leverage BigQuery Features:** Utilize BigQuery's native SQL capabilities for parameter validation, variable management, and transaction-like error handling (`BEGIN...EXCEPTION...END`).
    *   **Simplify Deployment:** A single SQL file for the procedure simplifies deployment compared to managing shell scripts and their dependencies in a cloud environment.
*   **Dedicated BigQuery Logging/Control Tables:** Instead of file-based logging, four BigQuery tables (`job_control`, `job_messages`, `job_error_log`, `job_audit`) were introduced. This approach offers:
    *   **Structured Logging:** Enables easier querying, analysis, and reporting of job execution status and messages.
    *   **Scalability:** BigQuery's inherent scalability handles large volumes of log data efficiently.
    *   **Integration:** Facilitates integration with other GCP services like Cloud Monitoring and Looker Studio for operational dashboards.
*   **Cloud Composer for External Orchestration:** Cloud Composer (Airflow) was chosen to schedule and trigger the BigQuery Stored Procedure. This provides:
    *   **Robust Scheduling:** Airflow's powerful scheduling capabilities replace cron-based scheduling.
    *   **Visibility & Monitoring:** Centralized monitoring and logging of DAG runs.
    *   **Extensibility:** Allows for easy integration of pre-processing, post-processing, data quality checks, and notifications within the same workflow.
*   **Placeholder for Core Logic:** The core business logic from `k_ausd_v_ta_vvl_dwh.ksh` was identified as a separate migration effort and represented by a placeholder BigQuery Stored Procedure (`k_ausd_v_ta_vvl_dwh`). This decision allows for:
    *   **Phased Migration:** Decoupling the wrapper migration from the more complex core logic migration, reducing immediate risk.
    *   **Focused Analysis:** Enables a dedicated, in-depth analysis of the core script's business rules and data transformations.
*   **Parameter Mapping:** Shell script command-line arguments (`-s`, `-l`) are directly mapped to input parameters of the BigQuery Stored Procedure (`p_stichtag`, `p_loglevel`). This maintains functional parity and clarity.
*   **Error Handling Translation:** The shell script's `set -eu` and `trap` mechanisms are translated into BigQuery's `BEGIN...EXCEPTION...END` blocks. While not a direct 1:1 mapping in terms of system-level interception, this provides robust error handling within the SQL context, logging errors to dedicated tables and re-raising them for upstream orchestration.

## 4. Manual steps before go-live

Before deploying and running the migrated job, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a Google Cloud Project is active and billing is enabled.
    *   Create the target BigQuery dataset (e.g., `project.dataset`) if it doesn't already exist. This dataset will host the control tables and stored procedures.
    *   (Optional) Create a separate staging dataset (e.g., `project.dataset.staging`) if temporary tables or intermediate data are required for the core logic.
2.  **Deploy BigQuery DDLs:**
    *   Execute the DDL scripts for the logging and control tables:
        *   `project.dataset.job_control.sql`
        *   `project.dataset.job_messages.sql`
        *   `project.dataset.job_error_log.sql`
        *   `project.dataset.job_audit.sql`
    *   This can be done via the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
3.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL scripts for the stored procedures:
        *   `vertragsdatenabgleich.sql`
        *   `k_ausd_v_ta_vvl_dwh.sql` (placeholder, will be updated with actual core logic later)
    *   Ensure these are created in the correct `project.dataset`.
4.  **IAM Permissions:**
    *   **BigQuery Service Account:** The service account used by Cloud Composer (or any other orchestrator) must have:
        *   `BigQuery Data Editor` role on `project.dataset` to `INSERT`, `UPDATE`, `SELECT` on the control tables and `EXECUTE` stored procedures.
        *   `BigQuery Job User` role to run BigQuery jobs.
    *   **User Permissions:** Users needing to deploy or manage these resources will require appropriate `BigQuery Admin` or `BigQuery Data Editor` roles.
5.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Update the `vertragsdatenabgleich_dag.py` file with the correct `PROJECT_ID` and `DATASET_ID`.
    *   Configure the `schedule` parameter in the DAG to match the desired execution frequency.
    *   Upload the `vertragsdatenabgleich_dag.py` file to the DAGs folder of your Cloud Composer environment.
6.  **Secrets Management (if applicable):**
    *   If the core logic (once migrated) requires any sensitive credentials (e.g., for external systems), these should be securely stored in Google Secret Manager and accessed by the Composer environment's service account. (Not directly applicable to the wrapper, but important for the full job).
7.  **Configuration Management (if applicable):**
    *   If the `.dw_init` file contained critical environment variables or configurations, these should be translated into BigQuery configuration tables, stored procedure parameters, or environment variables within the Cloud Composer environment.

## 5. Known gaps & unresolved references

The following items are identified as gaps or require further attention:

*   **Core Logic of `k_ausd_v_ta_vvl_dwh.ksh` (B4 Item):** This is the most critical unresolved item. The current migration only covers the wrapper script. The actual business logic for contract data reconciliation resides in `k_ausd_v_ta_vvl_dwh.ksh`, which has not been analyzed or migrated. A separate, in-depth analysis and migration design for this script are mandatory for the job to be fully functional. The `k_ausd_v_ta_vvl_dwh.sql` stored procedure is currently a placeholder.
*   **`DWMSG_*` Utilities Implementation:** The exact functionalities of the original `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` utilities are not fully known. While the BigQuery stored procedure implements similar logging and error handling, a detailed comparison and replication of all nuances might be necessary if specific behaviors are critical.
*   **`.dw_init` Content Analysis:** The specific environment variables and configurations sourced from `$HOME/.dw_init` are unknown. These might include critical paths, database connection details, or other settings that need to be identified and either hardcoded, passed as parameters, or stored in BigQuery configuration tables.
*   **Parameter Usage (`-s`, `-l`):** While `-s` (stichtag) is explicitly used and validated, the `-l` (loglevel) parameter is accepted but not actively used within the `vertragsdatenabgleich` stored procedure. Its intended use in the original `k_ausd_v_ta_vvl_dwh.ksh` or other parts of the system needs clarification.
*   **Error Handling Granularity:** The shell `trap` mechanism provides a very low-level, system-wide error interception. BigQuery's `EXCEPTION` blocks operate within the SQL context. While robust, any subtle differences in error propagation, resource cleanup, or recovery behavior between the two paradigms should be carefully tested, especially once the core logic is migrated.

## 6. Validation

Validation involves ensuring the migrated wrapper logic functions correctly and interacts as expected with the logging tables and the placeholder core logic.

**How to run the tests:**

1.  **Direct BigQuery Stored Procedure Execution:**
    *   Open the BigQuery UI.
    *   Execute the `vertragsdatenabgleich` stored procedure directly using a `CALL` statement:
        ```sql
        CALL project.dataset.vertragsdatenabgleich(p_stichtag => '20231026', p_loglevel => 'DEBUG');
        ```
    *   Test with valid and invalid `p_stichtag` values to verify parameter validation.
    *   To test the error path, you can temporarily modify the placeholder `k_ausd_v_ta_vvl_dwh` procedure to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error';`
2.  **Cloud Composer DAG Execution:**
    *   Ensure the `vertragsdatenabgleich_dag.py` is deployed to your Cloud Composer environment.
    *   Trigger the `vertragsdatenabgleich_workflow` DAG manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for successful task completion.
    *   Observe the BigQuery job history for the stored procedure execution.

**What "passing" means:**

*   **Successful Execution (Happy Path):**
    *   The `CALL project.dataset.vertragsdatenabgleich` statement (or DAG run) completes without error.
    *   A new entry is present in `project.dataset.job_control` with `status = 'OK'` and `finished_at` populated.
    *   Multiple `INFO` messages are logged in `project.dataset.job_messages` corresponding to job start, core logic execution (from the placeholder), and successful completion.
    *   A final entry is present in `project.dataset.job_audit` with `status = 'OK'`.
    *   No entries are found in `project.dataset.job_error_log`.
*   **Error Handling (Unhappy Path):**
    *   When an invalid `p_stichtag` is provided, the `CALL` statement (or DAG task) fails immediately with a clear error message.
    *   An `ERROR` message is logged in `project.dataset.job_messages` indicating the parameter validation failure.
    *   An entry is present in `project.dataset.job_error_log` detailing the parameter error.
    *   No entry is made in `project.dataset.job_control` or `job_audit` for this specific error path (as the procedure exits early).
    *   When an error is simulated within `k_ausd_v_ta_vvl_dwh` (or when the actual core logic fails), the `CALL` statement (or DAG task) fails.
    *   The `project.dataset.job_control` entry for that run shows `status = 'ERROR'` and `finished_at` populated.
    *   An `ERROR` message is logged in `project.dataset.job_messages` detailing the failure.
    *   An entry is present in `project.dataset.job_error_log` with the error details.
    *   A final entry is present in `project.dataset.job_audit` with `status = 'ERROR'`.

## 7. Rollback procedure

In case of issues or unexpected behavior after deployment, the following rollback procedure can be followed to revert to the original state:

1.  **Disable Cloud Composer DAG:**
    *   In the Airflow UI, locate the `vertragsdatenabgleich_workflow` DAG and toggle it off to prevent further scheduled executions.
2.  **Remove Cloud Composer DAG:**
    *   Delete the `vertragsdatenabgleich_dag.py` file from the DAGs folder in your Cloud Composer environment.
3.  **Drop BigQuery Stored Procedures:**
    *   Execute the following DDL statements in BigQuery to remove the migrated procedures:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.vertragsdatenabgleich;
        DROP PROCEDURE IF EXISTS project.dataset.k_ausd_v_ta_vvl_dwh; -- If it was deployed
        ```
4.  **Drop BigQuery Logging/Control Tables (Optional, but recommended for clean rollback):**
    *   If no critical data has been written to these tables, or if you wish to completely revert, drop them:
        ```sql
        DROP TABLE IF EXISTS project.dataset.job_control;
        DROP TABLE IF EXISTS project.dataset.job_messages;
        DROP TABLE IF EXISTS project.dataset.job_error_log;
        DROP TABLE IF EXISTS project.dataset.job_audit;
        ```
    *   **Caution:** If these tables contain valuable audit or log data from other processes, consider archiving them or only dropping them if they were exclusively created for this migration.
5.  **Re-enable Original Job:**
    *   Ensure the original `r_ausd_v_ta_vvl_dwh.ksh` script and its dependencies are in place and functional.
    *   Re-enable any original scheduling mechanisms (e.g., cron jobs) that triggered the KornShell script.
6.  **Verify Original Job Functionality:**
    *   Run the original `r_ausd_v_ta_vvl_dwh.ksh` script manually to confirm it executes successfully and produces expected outputs and logs.

This procedure ensures a clean reversion to the previous operational state, allowing for further investigation and re-planning of the migration if necessary.