# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh` has been migrated to Google Cloud BigQuery.

This script served as an orchestration wrapper for a data synchronization process related to the `ta_p_discount` table. Its original responsibilities included environment setup, parameter parsing, logging, error handling, and invoking a core processing script (`k_ausd_v_ta_p_discount.ksh`).

The migrated solution transforms this shell-based orchestration into a BigQuery-native solution, primarily a BigQuery Stored Procedure named `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`, complemented by dedicated BigQuery tables for logging and job control.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`bigquery/ddl/job_control.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_control` table. This table is designed to track the status, metadata, and lifecycle of each job execution, replacing the shell script's ad-hoc status tracking and environment variable management.
*   **`bigquery/ddl/job_log.sql`**
    *   **Role**: Defines the DDL for the `job_log` table. This table centralizes detailed log messages generated during the execution of the BigQuery stored procedure, replacing the shell script's `tee` command and file-based logging.
*   **`bigquery/ddl/job_error_log.sql`**
    *   **Role**: Defines the DDL for the `job_error_log` table. This table specifically records error details, providing a structured way to capture and analyze failures, replacing the shell script's error handling and messaging utilities.
*   **`bigquery/stored_procedures/BERT_V_TA_P_DISCOUNT.sql`**
    *   **Role**: Contains the BigQuery SQL code for the stored procedure `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`. This procedure directly replaces the orchestration logic of the original `r_ausd_v_ta_p_discount.ksh` script, handling parameter input, job initialization, logging, error management, and the invocation of the core data processing logic (expected to be another BigQuery stored procedure, `k_ausd_v_ta_p_discount`).

## 3. Key design decisions

*   **BigQuery Native Orchestration**: The primary decision was to translate the shell script's orchestration logic directly into a BigQuery Stored Procedure. This approach leverages BigQuery's native capabilities for control flow, parameter handling, and error management, ensuring better integration, performance, and maintainability within the Google Cloud ecosystem.
*   **Centralized Logging and Control Tables**: Instead of file-based logging and shell environment variables for job status, dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`) were introduced. This provides a structured, queryable, and scalable mechanism for monitoring job executions and debugging.
*   **Replacement of Shell Utilities**: Generic shell utilities (e.g., `date`, `getopts`, `trap`, `tee`, file sourcing) were replaced with their BigQuery SQL equivalents (e.g., `FORMAT_DATE`, stored procedure parameters, `EXCEPTION WHEN ERROR` blocks, `INSERT` statements into logging tables). This eliminates external dependencies and streamlines the execution environment.
*   **Assumption of Core Logic Migration**: The design explicitly assumes that the core data processing script, `k_ausd_v_ta_p_discount.ksh`, will also be migrated to a BigQuery Stored Procedure (`my_project.my_dataset.k_ausd_v_ta_p_discount`). This allows for a seamless, fully BigQuery-native data pipeline.

**Notable Trade-offs**:

*   **Dependency on Core Script Migration**: The full end-to-end functionality of this migrated wrapper is contingent on the successful migration of the `k_ausd_v_ta_p_discount.ksh` script. Without it, the wrapper will call a non-existent or stub procedure.
*   **Semantic Differences in Error Handling**: While `EXCEPTION WHEN ERROR` in BigQuery SQL provides robust error handling, it's not a direct 1:1 replacement for shell `trap` signals (e.g., `INT`, `ERR`). Careful testing is required to ensure equivalent behavior for all error scenarios.
*   **External Scheduling Requirement**: The original script was likely scheduled via cron. The BigQuery stored procedure requires a new scheduling mechanism (e.g., Cloud Scheduler, Cloud Workflows, or Cloud Composer), which adds an additional component to the architecture.

## 4. Manual steps before go-live

Before the migrated solution can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`my_project.my_dataset` as specified in the generated code) exists. If not, create it:
    ```bash
    bq mk --project_id=my_project my_dataset
    ```
2.  **Deploy DDLs**: Execute the DDL scripts to create the logging and control tables:
    ```bash
    bq query --use_legacy_sql=false < bigquery/ddl/job_control.sql
    bq query --use_legacy_sql=false < bigquery/ddl/job_log.sql
    bq query --use_legacy_sql=false < bigquery/ddl/job_error_log.sql
    ```
3.  **Deploy Stored Procedure**: Execute the stored procedure creation script:
    ```bash
    bq query --use_legacy_sql=false < bigquery/stored_procedures/BERT_V_TA_P_DISCOUNT.sql
    ```
4.  **IAM Permissions**: The service account or user executing these DDLs and the stored procedure must have the necessary BigQuery permissions:
    *   `bigquery.datasets.get`
    *   `bigquery.tables.create`, `bigquery.tables.update`, `bigquery.tables.get`, `bigquery.tables.getData`, `bigquery.tables.updateData` (for the logging tables)
    *   `bigquery.routines.create`, `bigquery.routines.update`, `bigquery.routines.get` (for the stored procedure)
    *   `bigquery.jobs.create` (to run queries and procedures)
5.  **Core Script Migration**: The core script `k_ausd_v_ta_p_discount.ksh` *must* be migrated to a BigQuery Stored Procedure (e.g., `my_project.my_dataset.k_ausd_v_ta_p_discount`) and deployed before this wrapper procedure can function correctly end-to-end. A placeholder or stub procedure can be used for initial testing of the wrapper.
6.  **Scheduling**: Set up a new scheduler for the BigQuery stored procedure. Options include:
    *   **Cloud Scheduler + Cloud Functions**: For simple, time-based scheduling.
    *   **Cloud Workflows**: For more complex orchestration and integration with other GCP services.
    *   **Cloud Composer (Apache Airflow)**: For advanced DAG-based scheduling, dependency management, and monitoring.
    The scheduler will need to execute a BigQuery `CALL` statement:
    ```sql
    CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(p_s => 'value_s', p_l => 'value_l', p_h => FALSE);
    ```
    (Replace `value_s` and `value_l` with actual parameter values, or `NULL` if not used).

## 5. Known gaps & unresolved references

*   **Core Script (`k_ausd_v_ta_p_discount.ksh`)**: This migration only covers the wrapper script. The core data processing logic, currently residing in `k_ausd_v_ta_p_discount.ksh`, is a critical dependency and *must* be migrated separately to a BigQuery Stored Procedure (expected name: `my_project.my_dataset.k_ausd_v_ta_p_discount`) for the end-to-end process to function.
*   **Parameter `s` and `l` Usage**: The original `r_ausd_v_ta_p_discount.ksh` script declares parameters `-s` and `-l` but does not explicitly use them in the provided logic. Their specific purpose and any required validation or processing logic are unknown and need to be clarified during the migration of the core `k_ausd_v_ta_p_discount.ksh` script. The current BigQuery procedure accepts them but performs no specific validation.
*   **Shell-specific Environment Variables and Utilities**: While replacements have been made for `trap`, `tee`, and date formatting, the full extent of environment variable usage (e.g., `$HOME/.dw_init`) and other sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs to be thoroughly reviewed during the core script migration to ensure all functionalities are correctly translated or replaced.
*   **`B4` Items**: The migration bucket is `semi_auto`, implying that some manual review and potential redesign (B4 items) were expected. The primary B4 item identified is the complete migration and functional verification of the dependent `k_ausd_v_ta_p_discount.ksh` script.

## 6. Validation

To validate the migrated BigQuery stored procedure:

1.  **Deployment Verification**:
    *   Confirm that the `job_control`, `job_log`, and `job_error_log` tables exist in `my_project.my_dataset`.
    *   Confirm that the stored procedure `my_project.my_dataset.BERT_V_TA_P_DISCOUNT` exists.
2.  **Test Execution**:
    *   **Help Message**: Execute the procedure with the help flag:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(p_s => NULL, p_l => NULL, p_h => TRUE);
        ```
        *Passing means*: The query should return a result set with `Programm`, `Version`, and `Beschreibung` columns, and the procedure should `RETURN` without further execution or errors.
    *   **Successful Run (with stub core procedure)**: If `k_ausd_v_ta_p_discount` is not yet migrated, create a simple stub procedure:
        ```sql
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_p_discount`(IN job_kennung STRING, IN dw_eintrags_nr INT64)
        BEGIN
          -- Simulate success
          SELECT 'Core script executed successfully' AS message;
        END;
        ```
        Then, execute the wrapper:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(p_s => 'test_s', p_l => 'test_l', p_h => FALSE);
        ```
        *Passing means*:
        *   The procedure completes without error.
        *   `job_control` table contains a new entry for `BERT_V_TA_P_DISCOUNT` with `status = 'OK'` and `finished_at` populated.
        *   `job_log` table contains entries, including "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
        *   `job_error_log` table remains empty.
    *   **Error Handling Test (with failing core procedure)**: Create a failing stub procedure:
        ```sql
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_p_discount`(IN job_kennung STRING, IN dw_eintrags_nr INT64)
        BEGIN
          -- Simulate an error
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script error';
        END;
        ```
        Then, execute the wrapper:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(p_s => 'test_s', p_l => 'test_l', p_h => FALSE);
        ```
        *Passing means*:
        *   The procedure execution should terminate with an error message (e.g., "Job aborted due to error").
        *   `job_control` table contains a new entry for `BERT_V_TA_P_DISCOUNT` with `status = 'ERROR'` and `finished_at` populated.
        *   `job_log` table contains entries, including "AppError: Abbruch - Simulated core script error".
        *   `job_error_log` table contains a new entry for the error.

## 7. Rollback procedure

In case of issues with the migrated BigQuery solution, follow these steps to roll back to the original KornShell script:

1.  **Stop New Scheduling**: Immediately disable or delete any new scheduling mechanisms (e.g., Cloud Scheduler job, Cloud Workflow, Cloud Composer DAG) that invoke the `my_project.my_dataset.BERT_V_TA_P_DISCOUNT` BigQuery stored procedure.
2.  **Re-enable Original Script**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh` script in its original scheduling system (e.g., cron). Ensure it has the necessary permissions and environment to run as before.
3.  **Verify Original Script Functionality**: Monitor the execution of the re-enabled original script to confirm it is running successfully and processing data as expected.
4.  **Optional: Clean Up BigQuery Artifacts**: If the rollback is deemed permanent or for a significant period, you may optionally drop the created BigQuery artifacts:
    ```bash
    DROP PROCEDURE IF EXISTS `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`;
    DROP TABLE IF EXISTS `my_project.my_dataset.job_control`;
    DROP TABLE IF EXISTS `my_project.my_dataset.job_log`;
    DROP TABLE IF EXISTS `my_project.my_dataset.job_error_log`;
    -- Also drop the k_ausd_v_ta_p_discount stub/procedure if it was created
    DROP PROCEDURE IF EXISTS `my_project.my_dataset.k_ausd_v_ta_p_discount`;
    ```