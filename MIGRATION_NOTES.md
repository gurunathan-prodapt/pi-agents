# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the Korn Shell (KSH) wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh`.

The original script served as an orchestrator for updating the `ta_c_bfc` table (Bindefristcache). Its responsibilities included environment setup, command-line parameter parsing, job and error logging, and invoking a core processing script (`k_ausd_v_ta_c_bfc.ksh`).

The script has been migrated to Google BigQuery, primarily as a BigQuery Stored Procedure named `isrpt.BERT_V_TA_C_BFC`. This procedure encapsulates the orchestration logic, parameter handling, and error management, leveraging BigQuery's native capabilities for logging and execution. The external orchestration is handled by an Airflow DAG.

## 2. Generated artifacts

The migration process generated the following files:

*   **`bq_ddl/dw_job_log.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `isrpt.dw_job_log` table. This table serves as the centralized logging and auditing mechanism, replacing the filesystem-based log files of the original KSH script.
*   **`bq_sprocs/dwmsg_ermittle_nr.sql`**
    *   **Role:** BigQuery Stored Procedure `isrpt.dwmsg_ermittle_nr`. This procedure is responsible for generating or fetching a unique entry number for each job execution, mimicking the `DWMSG_ErmittleNr` function from the original KSH utilities.
*   **`bq_sprocs/dwmsg_logdateiname.sql`**
    *   **Role:** BigQuery Stored Procedure `isrpt.dwmsg_logdateiname`. This procedure constructs a conceptual "log file name" string for BigQuery log entries, providing a descriptive identifier for log records, similar to `DWMSG_Logdateiname`.
*   **`bq_sprocs/dwmsg_setze_status_ok.sql`**
    *   **Role:** BigQuery Stored Procedure `isrpt.dwmsg_setze_status_ok`. This procedure updates the `dw_job_log` table to record a successful completion message for a given job entry number, replacing the `DWMSG_SetzeStatusOK` functionality.
*   **`bq_sprocs/k_ausd_v_ta_c_bfc.sql`**
    *   **Role:** BigQuery Stored Procedure `isrpt.k_ausd_v_ta_c_bfc`. This is a placeholder procedure intended to contain the actual data transformation logic for the `ta_c_bfc` table. It represents the migrated core script `k_ausd_v_ta_c_bfc.ksh`. Its full implementation requires a separate, detailed analysis.
*   **`bq_sprocs/bert_v_ta_c_bfc.sql`**
    *   **Role:** The main BigQuery Stored Procedure `isrpt.BERT_V_TA_C_BFC`. This procedure is the direct migration of the `r_ausd_v_ta_c_bfc.ksh` wrapper script. It handles parameter validation, calls the utility logging procedures, orchestrates the execution of `isrpt.k_ausd_v_ta_c_bfc`, and manages error handling.
*   **`orchestration/r_ausd_v_ta_c_bfc_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) definition. This DAG is responsible for scheduling and invoking the `isrpt.BERT_V_TA_C_BFC` BigQuery Stored Procedure, passing any necessary parameters.

## 3. Key design decisions

*   **KSH Wrapper to BigQuery Stored Procedure:** The core decision was to translate the KSH wrapper script directly into a BigQuery Stored Procedure (`isrpt.BERT_V_TA_C_BFC`). This leverages BigQuery's native execution environment, eliminating the need for external compute for orchestration logic.
*   **Centralized BigQuery Logging:** Filesystem-based logging was replaced by inserts into a dedicated BigQuery table (`isrpt.dw_job_log`). This provides a scalable, queryable, and centralized audit trail for all job executions.
*   **Utility Functions as BigQuery Stored Procedures:** Common KSH utility functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_SetzeStatusOK`) were migrated into separate BigQuery Stored Procedures. This promotes reusability and maintains a modular structure within the BigQuery environment.
*   **Parameter Handling:** `getopts` based command-line parameter parsing was replaced by explicit `IN` parameters in the BigQuery Stored Procedure definition. This provides clear input contracts for the procedure.
*   **Error Handling:** The `trap` mechanism in KSH was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block. This provides robust, structured error handling within the SQL context, allowing for logging of errors and controlled procedure termination.
*   **External Orchestration with Airflow:** The scheduling and invocation of the main BigQuery Stored Procedure are managed by an Airflow DAG. This provides a flexible, cloud-native solution for workflow orchestration, including parameter passing and dependency management.
*   **Core Logic Decoupling:** The actual data transformation logic, originally in `k_ausd_v_ta_c_bfc.ksh`, is represented by a separate BigQuery Stored Procedure (`isrpt.k_ausd_v_ta_c_bfc`). This maintains separation of concerns and allows for independent development and testing of the core business logic.

**Notable Trade-offs:**
*   **Increased BigQuery-specific code:** The migration introduces a dependency on BigQuery's procedural SQL dialect, which might require specific BigQuery knowledge for maintenance.
*   **Dependency on `dw_job_log` and utility procedures:** The main orchestration procedure relies on the existence and correct functioning of the `dw_job_log` table and the `dwmsg_` utility procedures.
*   **Placeholder for Core Logic:** The core data transformation logic (`k_ausd_v_ta_c_bfc`) is currently a placeholder, representing a significant follow-up task.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `isrpt` exists in your Google Cloud Project. If not, create it.
        ```bash
        bq mk --dataset --default_location=EU your_gcp_project_id:isrpt
        ```
2.  **Deploy `dw_job_log` Table:**
    *   Execute the DDL script `bq_ddl/dw_job_log.sql` to create the logging table.
        ```bash
        bq query --use_legacy_sql=false < bq_ddl/dw_job_log.sql
        ```
3.  **Deploy Utility Stored Procedures:**
    *   Deploy `bq_sprocs/dwmsg_ermittle_nr.sql`, `bq_sprocs/dwmsg_logdateiname.sql`, and `bq_sprocs/dwmsg_setze_status_ok.sql` to the `isrpt` dataset.
        ```bash
        bq query --use_legacy_sql=false < bq_sprocs/dwmsg_ermittle_nr.sql
        bq query --use_legacy_sql=false < bq_sprocs/dwmsg_logdateiname.sql
        bq query --use_legacy_sql=false < bq_sprocs/dwmsg_setze_status_ok.sql
        ```
4.  **Implement and Deploy Core Processing Stored Procedure:**
    *   **CRITICAL:** The `bq_sprocs/k_ausd_v_ta_c_bfc.sql` file is currently a placeholder. The actual data transformation logic from the original `k_ausd_v_ta_c_bfc.ksh` script must be fully analyzed, migrated, and implemented within this BigQuery Stored Procedure.
    *   Once implemented, deploy it to the `isrpt` dataset.
        ```bash
        bq query --use_legacy_sql=false < bq_sprocs/k_ausd_v_ta_c_bfc.sql
        ```
5.  **Deploy Main Orchestration Stored Procedure:**
    *   Deploy `bq_sprocs/bert_v_ta_c_bfc.sql` to the `isrpt` dataset.
        ```bash
        bq query --use_legacy_sql=false < bq_sprocs/bert_v_ta_c_bfc.sql
        ```
6.  **IAM/Permissions:**
    *   Ensure the Google Cloud service account used by Airflow (or any other scheduler) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` role on the `isrpt` dataset (to create/update tables and execute procedures).
        *   `BigQuery Job User` role (to run BigQuery jobs).
7.  **Airflow Connection Strings:**
    *   Verify that the `google_cloud_default` connection is correctly configured in your Airflow environment, pointing to the target Google Cloud Project.
8.  **Secrets Management (if applicable):**
    *   If the `p_s` or `p_l` parameters (or any other future parameters) contain sensitive information, ensure they are managed securely (e.g., using Airflow Connections, Google Secret Manager, or environment variables) and passed to the DAG.
9.  **Deploy and Configure Airflow DAG:**
    *   Upload the `orchestration/r_ausd_v_ta_c_bfc_dag.py` file to your Airflow DAGs folder.
    *   Configure the `schedule_interval` in the DAG to match the desired execution frequency.
    *   Update the `p_s` and `p_l` parameter values in the `BigQueryExecuteQueryOperator` to reflect their actual production values or retrieve them dynamically.

## 5. Known gaps & unresolved references

*   **Core Script `k_ausd_v_ta_c_bfc.ksh` Migration (B4 Item):** The most significant gap is the full implementation of the `isrpt.k_ausd_v_ta_c_bfc` BigQuery Stored Procedure. This procedure currently contains only placeholder logging. A separate, detailed analysis and migration of the original `k_ausd_v_ta_c_bfc.ksh` script are required to implement the actual data transformation logic for the `ta_c_bfc` table. This is a **B4 (Redesign/Complex)** item.
*   **Detailed `DWMSG_` Implementation:** While placeholder BigQuery Stored Procedures have been created for `dwmsg_ermittle_nr`, `dwmsg_logdateiname`, and `dwmsg_setze_status_ok`, their full logic should be reviewed against the original KSH utility scripts to ensure exact functional parity, especially regarding error codes, message formatting, and any complex logic for determining entry numbers or log file names.
*   **Parameter `-s` and `-l` Usage:** The original KSH wrapper script accepts `-s` and `-l` parameters but does not explicitly use them within the provided `r_ausd_v_ta_c_bfc.ksh` context. Their intended purpose and whether the core script `k_ausd_v_ta_c_bfc.ksh` utilizes them must be clarified during the core script's analysis. The migrated BigQuery procedure includes them as parameters, but their actual impact is currently unknown.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Manual BigQuery Procedure Execution (Help Message):**
    *   Execute the main procedure with the help parameter:
        ```sql
        CALL `isrpt.BERT_V_TA_C_BFC`(p_h => 'h', p_s => NULL, p_l => NULL);
        ```
    *   **Passing Criteria:** The query should return a result set displaying the program name, version, and usage instructions, then terminate without error.
2.  **Manual BigQuery Procedure Execution (Missing Parameters):**
    *   Execute the main procedure with missing required parameters (e.g., `p_s` or `p_l`):
        ```sql
        CALL `isrpt.BERT_V_TA_C_BFC`(p_h => NULL, p_s => NULL, p_l => 'test_l');
        ```
    *   **Passing Criteria:** The procedure should log a parameter error in `isrpt.dw_job_log` (look for `log_level = 'E'` and `log_text` indicating a parameter error) and return a `job_status` of 'ERROR'. The `SELECT 'usage' AS action...` statement should also be visible in the query results.
3.  **Manual BigQuery Procedure Execution (Successful Run):**
    *   Execute the main procedure with valid placeholder parameters:
        ```sql
        CALL `isrpt.BERT_V_TA_C_BFC`(p_h => NULL, p_s => 'test_s_value', p_l => 'test_l_value');
        ```
    *   **Passing Criteria:**
        *   The procedure should complete successfully, returning `job_status = 'OK'`.
        *   The `isrpt.dw_job_log` table should contain a sequence of log entries for the execution, including:
            *   `Jobstart` message.
            *   `SetzeStichtagInfo` message.
            *   `Executing core logic for k_ausd_v_ta_c_bfc (placeholder)` (from the placeholder core script).
            *   `Core logic execution completed successfully (placeholder)`.
            *   `Die Abarbeitung wurde ohne erkennbare Fehler beendet`.
            *   `Job beendet - OK`.
        *   The `eintrags_nr` should be unique for this run.
        *   The `SELECT` statements mimicking shell output should be visible in the query results.
4.  **Airflow DAG Execution:**
    *   Trigger the `bert_v_ta_c_bfc_orchestration` DAG in Airflow.
    *   **Passing Criteria:**
        *   The Airflow task `call_bert_v_ta_c_bfc_sp` should succeed.
        *   The `isrpt.dw_job_log` table should contain the same sequence of successful log entries as in the manual successful run.
        *   No errors should be reported in Airflow task logs or BigQuery job history.

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Deactivate/Delete Airflow DAG:**
    *   In the Airflow UI, unpause or delete the `bert_v_ta_c_bfc_orchestration` DAG to prevent further executions.
2.  **Delete BigQuery Stored Procedures:**
    *   Execute the following commands to drop the migrated procedures from BigQuery:
        ```sql
        DROP PROCEDURE IF EXISTS `isrpt.BERT_V_TA_C_BFC`;
        DROP PROCEDURE IF EXISTS `isrpt.k_ausd_v_ta_c_bfc`;
        DROP PROCEDURE IF EXISTS `isrpt.dwmsg_ermittle_nr`;
        DROP PROCEDURE IF EXISTS `isrpt.dwmsg_logdateiname`;
        DROP PROCEDURE IF EXISTS `isrpt.dwmsg_setze_status_ok`;
        ```
3.  **Delete `dw_job_log` Table (Optional):**
    *   If the log data is not required for post-mortem analysis, the `dw_job_log` table can be dropped.
        ```sql
        DROP TABLE IF EXISTS `isrpt.dw_job_log`;
        ```
    *   **Caution:** Dropping this table will permanently delete all logged job history. Consider archiving or retaining it if historical data is valuable.
4.  **Revert to Original KSH Script:**
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh` script and its dependencies are in place and configured to run as they did prior to the migration.
    *   Re-enable any original scheduling mechanisms (e.g., cron jobs) for the KSH script.