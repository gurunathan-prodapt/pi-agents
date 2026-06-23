# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh`. This script served as an orchestration wrapper for a contract data reconciliation job, primarily managing environment setup, parameter handling, logging, error trapping, and invoking a core data synchronization script (`k_ausd_v_ta_inv_def.ksh`) for the `ta_inv_def` table.

The script has been migrated to Google Cloud Platform, leveraging **BigQuery** for the core logic and utility functions, and **Cloud Composer (Apache Airflow)** for job orchestration and scheduling. The original shell wrapper logic is now encapsulated within a BigQuery stored procedure, which in turn calls other BigQuery stored procedures for logging, error handling, and the core business logic.

## 2. Generated Artifacts

The migration process generated the following files, which constitute the new solution:

*   **`ddl/job_audit_log.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_audit_log` table. This table serves as the centralized repository for all job execution logs, status updates, and error messages, replacing the legacy file-based logging.
*   **`stored_procedures/sp_dwmsg_log_info.sql`**
    *   **Role:** BigQuery Stored Procedure (SP) to log informational messages into the `job_audit_log` table. It replaces the `DWMSG_log_info` shell function.
*   **`stored_procedures/sp_dwmsg_meldefehler.sql`**
    *   **Role:** BigQuery SP to record error details into the `job_audit_log` table. It replaces the `DWMSG_MeldeFehler` shell function.
*   **`stored_procedures/sp_dwmsg_ermittle_nr.sql`**
    *   **Role:** BigQuery SP responsible for generating a unique entry number (`DW_EintragsNr`) for each job run. This currently uses `UNIX_SECONDS` for simplicity, but may require a more robust sequence generation mechanism for production. It replaces the `DWMSG_ErmittleNr` shell function.
*   **`stored_procedures/sp_dwmsg_logdateiname.sql`**
    *   **Role:** BigQuery SP to simulate the generation of a log file name. In the BigQuery context, actual logs are stored in `job_audit_log`, so this primarily serves to maintain compatibility with the legacy logging structure's metadata. It replaces the `DWMSG_Logdateiname` shell function.
*   **`stored_procedures/sp_dwmsg_erzeuge_eintrag.sql`**
    *   **Role:** BigQuery SP to create an initial job entry in the `job_audit_log` table when a job starts. It replaces the `DWMSG_ErzeugeEintrag` shell function.
*   **`stored_procedures/sp_dwmsg_setze_stichtag_info.sql`**
    *   **Role:** BigQuery SP to log reference date information into the `job_audit_log` table. It replaces the `DWMSG_SetzeStichtagInfo` shell function.
*   **`stored_procedures/sp_dwmsg_fehlerbehandlung.sql`**
    *   **Role:** BigQuery SP for centralized error handling. When an exception occurs within the main job procedure, this SP is called to log the error message. It replaces the implicit error handling and `trap` mechanisms of the shell script.
*   **`stored_procedures/sp_dwmsg_setze_status_ok.sql`**
    *   **Role:** BigQuery SP to log a successful job completion message into the `job_audit_log` table. It replaces the implicit success reporting of the shell script.
*   **`stored_procedures/sp_k_ausd_v_ta_inv_def.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure. This is intended to house the migrated core data synchronization logic originally found in `k_ausd_v_ta_inv_def.ksh`. **This procedure requires full implementation of the business logic.**
*   **`stored_procedures/sp_r_ausd_v_ta_inv_def.sql`**
    *   **Role:** The main BigQuery Stored Procedure that encapsulates the orchestration logic of the original `r_ausd_v_ta_inv_def.ksh` wrapper script. It handles parameter parsing, calls the `sp_dwmsg_*` utility procedures for logging, and invokes the core `sp_k_ausd_v_ta_inv_def` procedure.
*   **`dags/r_ausd_v_ta_inv_def_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is deployed to Cloud Composer and is responsible for scheduling and invoking the `sp_r_ausd_v_ta_inv_def` BigQuery stored procedure, passing necessary parameters.

## 3. Key Design Decisions

*   **Orchestration Shift to BigQuery Stored Procedures and Cloud Composer:**
    *   **Why:** The original KornShell script was primarily an orchestrator. Migrating this logic to a BigQuery Stored Procedure (`sp_r_ausd_v_ta_inv_def`) allows for native BigQuery execution, leveraging its robust SQL capabilities and transaction management. Cloud Composer (Airflow) provides a managed, scalable, and feature-rich platform for scheduling, monitoring, and managing complex workflows, replacing cron jobs or custom schedulers.
    *   **Trade-offs:** Introduces a new technology stack (Python/Airflow) for orchestration, potentially increasing the learning curve. The wrapper logic, while functional in BigQuery SQL, might be more verbose than a simple shell script.
*   **Centralized Logging and Error Handling in BigQuery:**
    *   **Why:** Replaced disparate shell functions and file-based logging with dedicated BigQuery stored procedures (`sp_dwmsg_*`) and a central `job_audit_log` table. This provides a unified, queryable, and structured logging mechanism, improving operational visibility, troubleshooting, and auditing. BigQuery's `BEGIN...EXCEPTION` blocks offer robust error trapping within the SQL context.
    *   **Trade-offs:** Loss of immediate file-based log tailing (replaced by SQL queries). Requires careful design of the `job_audit_log` schema to capture all necessary information.
*   **Modularization of Core Logic:**
    *   **Why:** The core data synchronization logic from `k_ausd_v_ta_inv_def.ksh` is designated to be migrated into a separate BigQuery stored procedure (`sp_k_ausd_v_ta_inv_def`). This promotes modularity, reusability, and separation of concerns, making the wrapper (`sp_r_ausd_v_ta_inv_def`) cleaner and easier to maintain.
    *   **Trade-offs:** Requires careful definition of the interface (parameters) between the wrapper and the core logic.
*   **Parameter Management:**
    *   **Why:** Shell command-line parameters and environment variables are replaced by explicit input parameters for BigQuery stored procedures and Airflow DAG parameters. This provides clear input definitions and type safety within BigQuery SQL.
    *   **Trade-offs:** Requires mapping shell-specific parameter parsing (`getopts`) to BigQuery SQL `IF` statements and Airflow parameter definitions.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in a production environment, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_dataset_id` in the generated code) exists within your GCP project (`your-gcp-project-id`). If not, create it.
    *   `bq mk --dataset project_id:dataset_id`
2.  **IAM Permissions:**
    *   The service account used by Cloud Composer (Airflow Worker) must have sufficient permissions to:
        *   Execute BigQuery jobs (`roles/bigquery.jobUser`).
        *   Read/write data to BigQuery tables (`roles/bigquery.dataEditor` for `job_audit_log` and any tables `sp_k_ausd_v_ta_inv_def` interacts with).
        *   Create/replace BigQuery stored procedures (`roles/bigquery.admin` or more granular `bigquery.routines.create`, `bigquery.routines.update`).
3.  **BigQuery Connection Configuration:**
    *   The Airflow DAG uses `gcp_conn_id="google_cloud_default"`. Ensure this connection is properly configured in your Airflow environment, typically pointing to the service account associated with your Composer environment.
4.  **Deploy BigQuery DDL and Stored Procedures:**
    *   Execute `ddl/job_audit_log.sql` to create the logging table.
    *   Deploy all stored procedures (`stored_procedures/*.sql`) to the target BigQuery dataset. This can be done via `bq query --use_legacy_sql=false <file.sql` or through the BigQuery UI/API.
5.  **Implement Core Logic (`sp_k_ausd_v_ta_inv_def`):**
    *   **CRITICAL:** The `stored_procedures/sp_k_ausd_v_ta_inv_def.sql` file is currently a placeholder. The actual data synchronization logic from the original `k_ausd_v_ta_inv_def.ksh` script **must be fully translated and implemented** within this BigQuery stored procedure. This includes all DML operations, data transformations, and any specific business rules.
6.  **Review `sp_dwmsg_ermittle_nr` for Robustness:**
    *   The current implementation uses `UNIX_SECONDS(CURRENT_TIMESTAMP())` for `DW_EintragsNr`. For high-volume or highly concurrent environments, this might not guarantee absolute uniqueness or sequential numbering. Consider implementing a more robust sequence generator (e.g., a dedicated sequence table, UUIDs, or a BigQuery sequence if available and suitable).
7.  **Configure Airflow DAG Parameters:**
    *   Update `PROJECT_ID` and `DATASET_ID` in `dags/r_ausd_v_ta_inv_def_dag.py` to your actual GCP project and BigQuery dataset IDs.
    *   Review and update the default values for `p_s` and `p_l` parameters in the DAG to reflect their intended usage and any required business logic.
8.  **Set Airflow DAG Schedule:**
    *   Modify `schedule=None` in `dags/r_ausd_v_ta_inv_def_dag.py` to the desired cron schedule (e.g., `"@daily"`, `"0 0 * * *"`) for regular execution.
9.  **Deploy Airflow DAG:**
    *   Upload `dags/r_ausd_v_ta_inv_def_dag.py` to your Cloud Composer environment's DAGs folder.

## 5. Known Gaps & Unresolved References

*   **Core Logic Implementation (B4 Item):** The most significant gap is the placeholder `sp_k_ausd_v_ta_inv_def.sql`. The full translation and implementation of the `k_ausd_v_ta_inv_def.ksh` script's business logic is a critical follow-up item (B4 - Build Phase 4, indicating it's a major development task). Its complexity and dependencies (e.g., specific database interactions, external system calls) are currently unknown and will dictate the effort.
*   **Missing `file_complexity` Data:** The original complexity tier for `r_ausd_v_ta_inv_def.ksh` was not available. This means the overall migration effort, especially for the core logic, might be underestimated.
*   **Parameter Validation Logic:** The validation for `p_s` and `p_l` parameters in `sp_r_ausd_v_ta_inv_def` is currently minimal. Any specific business rules or mandatory checks for these parameters from the original script need to be explicitly translated into BigQuery SQL `IF` statements.
*   **`DW_EintragsNr` Uniqueness:** As noted in manual steps, the `UNIX_SECONDS` approach for `DW_EintragsNr` might not be robust enough for all production scenarios. A more sophisticated sequence generation mechanism might be required.
*   **Full `DWMSG_*` Fidelity:** While core logging functions are migrated, the exact behavior, message formats, and any additional metadata captured by the full suite of `DWMSG_*` shell functions need to be thoroughly verified and replicated in the BigQuery logging procedures.
*   **Shell Environment Variable Mapping:** A comprehensive review is needed to ensure all environment variables sourced from `. $HOME/.dw_init` and other shell scripts are correctly identified and either replaced by BigQuery parameters, configuration tables, or hardcoded values where appropriate.
*   **`trap INT ERR` Equivalence:** BigQuery's `BEGIN...EXCEPTION` blocks handle SQL errors. However, the exact behavior of shell `trap INT ERR` for external signals (e.g., a user terminating the process) is now handled by the orchestrator (Cloud Composer). While Composer provides its own retry and failure mechanisms, the precise mirroring of the legacy `trap` behavior needs to be confirmed.

## 6. Validation

To ensure the migrated job functions correctly, the following validation steps should be performed:

1.  **Unit Test BigQuery Stored Procedures:**
    *   **`sp_dwmsg_*` procedures:** Call each `sp_dwmsg_log_info`, `sp_dwmsg_meldefehler`, `sp_dwmsg_ermittle_nr`, etc., directly from the BigQuery console or a test script. Verify that the corresponding entries are correctly inserted into the `project_id.dataset_id.job_audit_log` table with the expected `log_level`, `message`, `job_id`, and `entry_number`.
    *   **`sp_r_ausd_v_ta_inv_def`:**
        *   Call with `p_h=TRUE` to verify the help message output.
        *   Call with valid parameters (`p_h=FALSE`, `p_s='some_value'`, `p_l='another_value'`).
        *   Verify that the `job_audit_log` table contains entries for job start, info messages, and a success message.
        *   (Once `sp_k_ausd_v_ta_inv_def` is implemented) Call with parameters that would cause an error in the core logic and verify that `sp_dwmsg_fehlerbehandlung` is invoked and an error is logged.
2.  **Integration Test with Airflow DAG:**
    *   Trigger the `r_ausd_v_ta_inv_def_dag` in Cloud Composer (Airflow UI).
    *   Monitor the DAG run in the Airflow UI to ensure it completes successfully without task failures.
    *   Check the BigQuery `job_audit_log` table for the complete sequence of log entries generated by the `sp_r_ausd_v_ta_inv_def` procedure, including job start, informational messages, and the final success message.
3.  **Data Validation (Post-`sp_k_ausd_v_ta_inv_def` Implementation):**
    *   Once `sp_k_ausd_v_ta_inv_def` is fully implemented, execute the entire workflow.
    *   Compare the output data in BigQuery (e.g., `ta_inv_def` table or related tables) with the expected results from the legacy system. This may involve row counts, checksums, or detailed data comparisons.

**What "passing" means:**

*   The `r_ausd_v_ta_inv_def_dag` in Airflow completes with a "success" status.
*   The `sp_r_ausd_v_ta_inv_def` BigQuery stored procedure executes without unhandled exceptions.
*   The `project_id.dataset_id.job_audit_log` table contains a complete and accurate record of the job execution, including:
    *   A unique `entry_number` for the run.
    *   `INFO` messages for job start, parameter details, and the final success message.
    *   No `ERROR` level entries unless specifically testing error handling.
*   (Crucially, once `sp_k_ausd_v_ta_inv_def` is implemented): The data reconciliation process for `ta_inv_def` is performed correctly, resulting in the expected data state in the target tables, matching the behavior of the original `k_ausd_v_ta_inv_def.ksh` script.

## 7. Rollback Procedure

In the event that the migrated job needs to be reverted to the legacy system, follow these steps:

1.  **Pause/Delete Airflow DAG:**
    *   In the Cloud Composer (Airflow) UI, locate the `r_ausd_v_ta_inv_def_dag` and set its status to "Off" (pause) or delete it entirely. This will stop any further scheduled executions of the migrated job.
2.  **Drop BigQuery Stored Procedures:**
    *   Execute `DROP PROCEDURE` commands for all created stored procedures in the target BigQuery dataset:
        ```sql
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_r_ausd_v_ta_inv_def`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_k_ausd_v_ta_inv_def`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_log_info`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_meldefehler`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_ermittle_nr`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_logdateiname`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_erzeuge_eintrag`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_setze_stichtag_info`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_fehlerbehandlung`;
        DROP PROCEDURE IF EXISTS `project_id.dataset_id.sp_dwmsg_setze_status_ok`;
        ```
3.  **Drop BigQuery Audit Log Table (Optional but Recommended):**
    *   If the `job_audit_log` table was created solely for this migration and is not used by other processes, it can be dropped:
        ```sql
        DROP TABLE IF EXISTS `project_id.dataset_id.job_audit_log`;
        ```
    *   **Caution:** If the `job_audit_log` table is intended for general logging across multiple migrated jobs, do not drop it.
4.  **Re-enable Legacy Script:**
    *   Re-deploy or re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh` script in its legacy environment, ensuring its original scheduling mechanism (e.g., cron) is reactivated.
5.  **Data Rollback (if applicable):**
    *   If the implemented `sp_k_ausd_v_ta_inv_def` made any irreversible data changes, a specific data rollback strategy would be required. This could involve restoring tables from backups, executing inverse DML statements, or using BigQuery's point-in-time recovery features. This aspect is highly dependent on the specific logic within `sp_k_ausd_v_ta_inv_def` and the data governance policies.