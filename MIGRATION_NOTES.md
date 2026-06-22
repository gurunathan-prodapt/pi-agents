# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh` has been migrated to Google BigQuery. This script, originally responsible for orchestrating the data reconciliation process for the `ta_cntrct_crs2` table, including environment setup, parameter parsing, logging, and error handling, has been transformed into a BigQuery Stored Procedure. The target platform is Google BigQuery, leveraging its native capabilities for data processing, logging, and job control.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`project/dataset/ddl/job_control_tables.sql`**
    *   **Role**: This DDL script creates the necessary logging and control tables in BigQuery. These tables (`job_log`, `job_error_log`, `job_control`) replace the file-based logging and job status tracking mechanisms of the original KornShell script. They provide a structured, queryable record of job executions, errors, and status updates.
*   **`project/dataset/k_ausd_v_ta_cntrct_crs2.sql`**
    *   **Role**: This file defines a BigQuery Stored Procedure named `k_ausd_v_ta_cntrct_crs2`. It serves as a placeholder for the core data processing logic originally contained within the `k_ausd_v_ta_cntrct_crs2.ksh` KornShell script. The actual reconciliation logic for the `ta_cntrct_crs2` table needs to be implemented within this procedure. The wrapper procedure `BERT_V_TA_CNTRCT_CRS2` calls this procedure.
*   **`project/dataset/bert_v_ta_cntrct_crs2.sql`**
    *   **Role**: This is the main migrated artifact, a BigQuery Stored Procedure named `BERT_V_TA_CNTRCT_CRS2`. It directly replaces the original `r_ausd_v_ta_cntrct_crs2.ksh` wrapper script. It handles input parameters (`p_h` for help, `p_s` for stichtag, `p_l` for log name), performs parameter validation, manages job entry creation and status updates in `job_control`, logs execution details to `job_log`, and orchestrates the call to the core processing procedure (`k_ausd_v_ta_cntrct_crs2`). It also implements BigQuery's `EXCEPTION WHEN ERROR` blocks for robust error handling, logging errors to `job_error_log`.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Orchestration**: The original KornShell script, being an orchestration layer, was best suited for migration to a BigQuery Stored Procedure (`BERT_V_TA_CNTRCT_CRS2`). This allows for native execution within BigQuery, leveraging its built-in capabilities for parameter handling, control flow, and error management, eliminating the need for external shell environments.
*   **Centralized BigQuery Logging and Error Handling**: File-based logging and shell `trap` mechanisms were replaced by structured logging to dedicated BigQuery tables (`job_log`, `job_error_log`, `job_control`). This provides a more robust, queryable, and integrated logging solution, simplifying monitoring and debugging within the BigQuery ecosystem.
*   **Parameter Management via Stored Procedure Arguments**: The `getopts` mechanism for command-line parameter parsing in KornShell was directly translated into input parameters for the BigQuery Stored Procedure. This aligns with BigQuery's procedural language best practices.
*   **Core Logic Encapsulation in a Separate Stored Procedure**: The core processing logic, originally in `k_ausd_v_ta_cntrct_crs2.ksh`, was designed to be migrated into its own BigQuery Stored Procedure (`k_ausd_v_ta_cntrct_crs2`). This promotes modularity, reusability, and clear separation of concerns between orchestration and data transformation.
*   **Job Control and Metadata Management**: Utility functions for managing job numbers and status (`DWMSG_ErmittleNr`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`) were replaced by direct `INSERT` and `UPDATE` operations on a BigQuery `job_control` table. This centralizes job metadata and status tracking.
*   **Trade-offs**:
    *   **Placeholder Core Logic**: The `k_ausd_v_ta_cntrct_crs2` procedure is currently a placeholder. Its full implementation is a critical dependency and represents a significant portion of the overall migration effort for the data processing logic.
    *   **Loss of Direct File Output**: The `p_l` (log file name) parameter is accepted but does not result in a physical log file on a file system. All logging is directed to BigQuery tables. While this offers structured benefits, it changes the way logs are accessed and consumed.
    *   **Environment Initialization**: The complex environment setup from `. $HOME/.dw_init` needs to be fully understood and replicated either within the BigQuery environment (e.g., via session variables, UDFs) or managed by the external orchestration tool calling the BigQuery SP.

## 4. Manual steps before go-live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the BigQuery dataset `project.dataset` exists in your GCP project. If not, create it.
    *   `bq mk --dataset project:dataset`
2.  **Deploy DDL for Control Tables**: Execute the `project/dataset/ddl/job_control_tables.sql` script to create the `job_log`, `job_error_log`, and `job_control` tables.
    *   `bq query --use_legacy_sql=false < project/dataset/ddl/job_control_tables.sql`
3.  **Migrate and Implement Core Logic**: **Crucially**, the placeholder BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_cntrct_crs2` must be fully implemented. This involves:
    *   Analyzing the original `k_ausd_v_ta_cntrct_crs2.ksh` script to extract its data processing, transformation, and loading logic.
    *   Translating this logic into BigQuery SQL, potentially involving `INSERT`, `UPDATE`, `MERGE` statements, and other BigQuery DDL/DML.
    *   Ensuring the `ta_cntrct_crs2` table (and any other source/target tables it interacts with) is migrated to BigQuery with its correct schema and data.
    *   Deploying the fully implemented `project.dataset.k_ausd_v_ta_cntrct_crs2` procedure.
4.  **Deploy Wrapper Stored Procedure**: Execute the `project/dataset/bert_v_ta_cntrct_crs2.sql` script to create the main wrapper stored procedure.
    *   `bq query --use_legacy_sql=false < project/dataset/bert_v_ta_cntrct_crs2.sql`
5.  **IAM Permissions**: Grant the necessary IAM roles to the service account or user that will execute these BigQuery Stored Procedures. This typically includes:
    *   `BigQuery Data Editor` on `project.dataset` to allow writing to `job_log`, `job_error_log`, `job_control`, and any data tables modified by `k_ausd_v_ta_cntrct_crs2`.
    *   `BigQuery Job User` to run BigQuery jobs.
6.  **Scheduling**: Configure a scheduling mechanism (e.g., Cloud Composer/Apache Airflow, Cloud Scheduler, or a custom orchestrator) to invoke the `project.dataset.BERT_V_TA_CNTRCT_CRS2` stored procedure with the required parameters.
7.  **Environment Variable Replication**: If the original `.dw_init` script contained critical environment variables or configurations, ensure these are replicated in the BigQuery session context (if applicable) or within the orchestration environment calling the BigQuery SP.

## 5. Known gaps & unresolved references

*   **Core Processing Logic (`k_ausd_v_ta_cntrct_crs2.ksh`)**: The generated `project.dataset.k_ausd_v_ta_cntrct_crs2` is a placeholder. The complete migration and implementation of the actual data reconciliation logic from the original KornShell script is a critical, unresolved task. This includes identifying all source/target tables and their schemas.
*   **`ta_cntrct_crs2` Table Migration**: The target table for reconciliation must be migrated to BigQuery and available as `project.dataset.ta_cntrct_crs2` (or similar). This is an implicit dependency.
*   **Detailed Functionality of Sourced Utility Scripts**: The exact logic within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` was not fully analyzed. While BigQuery native functions and structured logging replace their general purpose, any highly specific or complex logic within them might require dedicated BigQuery UDFs or further investigation.
*   **`.dw_init` Contents**: The full scope and impact of `. $HOME/.dw_init` are unknown. Any critical environment variables, paths, or configurations defined there need to be explicitly handled in the BigQuery environment or the calling orchestrator.
*   **`p_l` Parameter Usage**: The `p_l` parameter (log file name) is accepted by the `BERT_V_TA_CNTRCT_CRS2` procedure, but it is only used to populate the `log_name` column in `job_control` and does not result in a physical log file being created. This is a functional change from the original script's behavior.
*   **Missing Complexity/Automation Data**: The original design document noted a lack of complexity tier, migration flags, and automation bucket data. This means the effort estimation for the remaining `k_ausd_v_ta_cntrct_crs2.ksh` migration might require a more detailed manual assessment.

## 6. Validation

To validate the migrated BigQuery Stored Procedure, follow these steps:

1.  **Deployment**: Ensure all DDL and Stored Procedures (`job_control_tables.sql`, `k_ausd_v_ta_cntrct_crs2.sql` (even as a placeholder), and `bert_v_ta_cntrct_crs2.sql`) are successfully deployed to `project.dataset`.
2.  **Test Help Message**:
    *   Execute: `CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => TRUE, p_s => NULL, p_l => NULL);`
    *   **Expected Outcome**: A help message describing the procedure's usage should be returned, and the procedure should `RETURN` without further execution or error.
3.  **Test Parameter Validation (Missing Stichtag)**:
    *   Execute: `CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => NULL, p_l => 'test_log.log');`
    *   **Expected Outcome**: The call should fail with a `SQLSTATE '45000'` error message indicating a missing `-s` parameter. An entry should be present in `project.dataset.job_error_log` detailing the error, and `project.dataset.job_control` should *not* have an entry for this failed run (as the error occurs before the initial insert).
4.  **Test Successful Execution**:
    *   Execute: `CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-10-26', p_l => 'my_test_run.log');` (Replace '2023-10-26' with a valid date).
    *   **Expected Outcome**:
        *   The procedure should complete successfully without error.
        *   `project.dataset.job_control` should contain a new entry with `status = 'OK'`, `script_name = 'BERT_V_TA_CNTRCT_CRS2'`, `stichtag_info = '2023-10-26'`, and `log_name = 'my_test_run.log'`.
        *   `project.dataset.job_log` should contain several `INFO` messages, including "Job ... started.", "Calling core script: k_ausd_v_ta_cntrct_crs2", "k_ausd_v_ta_cntrct_crs2: Core processing started.", "k_ausd_v_ta_cntrct_crs2: Core processing completed successfully.", and "Job ... completed successfully.".
        *   No entries should be in `project.dataset.job_error_log`.
5.  **Test Error Path (Simulated Core Script Failure)**:
    *   *Pre-requisite*: Modify `project.dataset.k_ausd_v_ta_cntrct_crs2` temporarily to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script failure.';`
    *   Execute: `CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => FALSE, p_s => '2023-10-27', p_l => 'failed_run.log');`
    *   **Expected Outcome**:
        *   The `BERT_V_TA_CNTRCT_CRS2` procedure should fail with a `SQLSTATE '45000'` error.
        *   `project.dataset.job_control` should contain a new entry with `status = 'ERROR'`.
        *   `project.dataset.job_error_log` should contain an entry detailing the simulated error from `k_ausd_v_ta_cntrct_crs2`.
        *   `project.dataset.job_log` should contain `INFO` messages up to the point of failure, followed by an `ERROR` message indicating the job failed.
    *   *Post-test*: Revert `project.dataset.k_ausd_v_ta_cntrct_crs2` to its original placeholder or fully implemented state.

**"Passing" means**: All test cases execute as described above, with correct log entries, job control status updates, and error handling behavior. The core `k_ausd_v_ta_cntrct_crs2` procedure, once fully implemented, should also produce the expected data reconciliation results in `ta_cntrct_crs2`.

## 7. Rollback procedure

In case of critical issues or if the migration needs to be reverted, follow these steps:

1.  **Stop New Executions**: Immediately halt any scheduled or manual executions of the `project.dataset.BERT_V_TA_CNTRCT_CRS2` BigQuery Stored Procedure. This involves disabling or removing the corresponding task from your orchestrator (e.g., Cloud Composer DAG, Cloud Scheduler job).
2.  **Re-enable Original Script**: Re-enable the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh` in its previous scheduling system. Ensure it has access to its original environment and dependencies.
3.  **Delete BigQuery Stored Procedures**: Drop the migrated BigQuery Stored Procedures.
    *   `DROP PROCEDURE IF EXISTS project.dataset.BERT_V_TA_CNTRCT_CRS2;`
    *   `DROP PROCEDURE IF EXISTS project.dataset.k_ausd_v_ta_cntrct_crs2;`
4.  **Delete BigQuery Control Tables (Optional but Recommended for Clean Rollback)**: If the `job_log`, `job_error_log`, and `job_control` tables were created solely for this migration and contain no other critical data, they can be dropped.
    *   `DROP TABLE IF EXISTS project.dataset.job_log;`
    *   `DROP TABLE IF EXISTS project.dataset.job_error_log;`
    *   `DROP TABLE IF EXISTS project.dataset.job_control;`
    *   **Caution**: If these tables are shared or contain historical data, consider archiving them or retaining them as-is, rather than dropping.
5.  **Revert Data Changes (if applicable)**: If the fully implemented `k_ausd_v_ta_cntrct_crs2` procedure made any irreversible data changes to `ta_cntrct_crs2` or other tables, a data rollback strategy (e.g., restoring from a snapshot, running an inverse script) might be necessary. This depends heavily on the specific logic within `k_ausd_v_ta_cntrct_crs2`.
6.  **Verify Original System**: Confirm that the original KornShell script is running as expected and processing data correctly.