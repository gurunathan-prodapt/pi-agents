# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`. This script served as an orchestration layer for a core business logic script (`k_ausd_v_ta_period.ksh`) responsible for synchronizing contract data in the `ta_period` table, handling parameter parsing, logging, and job status management.

The migration targets **Google Cloud Platform (GCP)**, specifically:
*   **BigQuery Stored Procedures** for the orchestration logic and the core business logic.
*   **BigQuery Tables** for job control, detailed logging, and error tracking.
*   **Cloud Composer (Apache Airflow)** for optional external scheduling and orchestration.

The primary goal was to translate the shell script's control flow, parameter handling, and logging mechanisms into a BigQuery-native solution, while acknowledging that the detailed migration of the core business logic (`k_ausd_v_ta_period.ksh`) is a separate, subsequent effort.

## 2. Generated artifacts

The migration produced the following files, designed to be deployed within a GCP BigQuery environment:

*   **`project.dataset.job_control.sql`**
    *   **Role:** DDL (Data Definition Language) script to create the `job_control` table in BigQuery. This table serves as the central repository for tracking the status and metadata of each job execution, replacing the shell script's internal job entry management. It records `job_entry_nr`, `job_name`, `status`, `stichtag`, and timestamps.

*   **`project.dataset.job_log.sql`**
    *   **Role:** DDL script to create the `job_log` table in BigQuery. This table stores all detailed log messages generated during job execution, replacing the file-based log output (`>> $LogDatei`) of the original shell script. It captures `job_name`, `job_entry_nr`, `log_message`, and `created_ts`.

*   **`project.dataset.job_error_log.sql`**
    *   **Role:** DDL script to create the `job_error_log` table in BigQuery. This table is dedicated to capturing detailed error information for failed job runs, providing structured error reporting beyond simple log messages. It includes `job_name`, `job_entry_nr`, `error_nr`, `error_arg`, and `error_message`.

*   **`project.dataset.vertragsdatenabgleich_wrapper.sql`**
    *   **Role:** BigQuery SQL stored procedure. This is the direct migration of the `r_ausd_v_ta_period.ksh` wrapper script. It handles parameter parsing and validation, initializes job control entries, manages logging, and orchestrates the call to the core business logic stored procedure (`k_ausd_v_ta_period`). It also implements robust error handling using BigQuery's `EXCEPTION WHEN ERROR THEN` blocks.

*   **`project.dataset.k_ausd_v_ta_period.sql`**
    *   **Role:** BigQuery SQL stored procedure. This is a **placeholder** for the migrated core business logic from `k_ausd_v_ta_period.ksh`. It currently contains only logging statements to indicate its start and end, and a comment block for where the actual data synchronization logic (DML/DDL on the `ta_period` table or its equivalent) should be implemented. Its full implementation requires a separate, detailed design effort.

*   **`airflow_dag_vertragsdatenabgleich.py`**
    *   **Role:** An optional Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG demonstrates how to schedule and trigger the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure using Cloud Composer. It includes an example of passing parameters like `p_stichtag` to the stored procedure. This artifact is for external orchestration and is not strictly part of the BigQuery-native migration but provides a common deployment pattern.

## 3. Key design decisions

The following key design decisions guided the migration process:

*   **Wrapper to BigQuery Stored Procedure**: The entire orchestration logic of the original KornShell script (`r_ausd_v_ta_period.ksh`) was translated into a single BigQuery stored procedure (`project.dataset.vertragsdatenabgleich_wrapper`). This leverages BigQuery's native scripting capabilities for control flow, variable management, and error handling, keeping the orchestration logic close to the data.
*   **Structured Logging and Job Control in BigQuery Tables**: Instead of file-based logging and ad-hoc job status tracking, a structured approach was adopted. Dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`) were created to centralize job metadata, detailed messages, and error information. This provides better auditability, queryability, and integration with BigQuery's ecosystem.
*   **BigQuery `EXCEPTION` for Error Handling**: The shell script's `trap INT ERR` mechanism was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides a robust and idiomatic way to catch and handle errors within the stored procedure, ensuring proper status updates and error logging.
*   **Parameter Handling via Stored Procedure Arguments**: The `getopts` mechanism for command-line parameter parsing was replaced by direct input parameters to the BigQuery stored procedure (`p_stichtag`, `p_log_level`, `p_show_help`). Validation logic was translated into BigQuery SQL conditional statements.
*   **Modular Core Logic (Placeholder)**: The core business logic (`k_ausd_v_ta_period.ksh`) was designed as a separate BigQuery stored procedure (`project.dataset.k_ausd_v_ta_period`). This promotes modularity, allowing the wrapper to focus solely on orchestration and the core procedure to encapsulate the data transformation, even though its full implementation is pending.
*   **Cloud Composer for External Orchestration**: While the BigQuery stored procedure can be called directly, Cloud Composer (managed Airflow) was chosen as the recommended external scheduler. This provides robust scheduling, dependency management, monitoring, and integration with other GCP services, aligning with modern data warehousing practices.

**Notable Trade-offs:**

*   **Core Logic as Placeholder**: The most significant trade-off is the "placeholder" nature of the `k_ausd_v_ta_period` stored procedure. The full complexity and potential non-SQL operations of the original `k_ausd_v_ta_period.ksh` are not yet addressed, meaning the end-to-end functionality is not fully migrated.
*   **Shell `trap` vs. BigQuery `EXCEPTION`**: While BigQuery's `EXCEPTION` blocks are powerful for logical errors, they don't directly replicate the OS-level signal handling (e.g., `INT` for user interruption) of shell scripts. Graceful handling of external interruptions would rely on the orchestration layer (e.g., Airflow's task cancellation).
*   **Configuration Management**: The `.dw_init` file's role in environment initialization is replaced by direct parameters or hardcoded project/dataset IDs. A more sophisticated configuration management strategy (e.g., a BigQuery config table or Secret Manager for sensitive values) might be needed for production environments.

## 4. Manual steps before go-live

Before the migrated solution can be fully operational, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure a GCP project is active and billing is enabled.
    *   Create the target BigQuery dataset (e.g., `my_dataset`) where the tables and stored procedures will reside. Replace `project.dataset` and `my_dataset` placeholders in the generated code with your actual project ID and dataset ID.

2.  **IAM Permissions**:
    *   The service account or user executing the BigQuery stored procedures must have appropriate IAM roles. Minimum required roles include:
        *   `BigQuery Data Editor` (for `project.dataset`) to create/update tables and insert/update data.
        *   `BigQuery Job User` to run queries and stored procedures.
        *   If using Cloud Composer, the Composer service account will need these permissions.

3.  **Deploy BigQuery DDLs**:
    *   Execute the `project.dataset.job_control.sql`, `project.dataset.job_log.sql`, and `project.dataset.job_error_log.sql` scripts in BigQuery to create the necessary control and logging tables.

4.  **Deploy BigQuery Stored Procedures**:
    *   Execute the `project.dataset.vertragsdatenabgleich_wrapper.sql` script in BigQuery to create the main wrapper stored procedure.
    *   Execute the `project.dataset.k_ausd_v_ta_period.sql` script to create the placeholder core logic stored procedure. **Note:** This procedure must be fully implemented and tested before the wrapper can perform its intended function.

5.  **Core Logic Implementation (`k_ausd_v_ta_period`)**:
    *   **Crucially**, the `project.dataset.k_ausd_v_ta_period` stored procedure needs to be fully developed and deployed. This involves translating the actual data synchronization logic from the original `k_ausd_v_ta_period.ksh` script into BigQuery SQL DML/DDL statements. This step is outside the scope of this wrapper migration but is a prerequisite for end-to-end functionality.

6.  **Cloud Composer Environment Setup (if using Airflow)**:
    *   Provision a Cloud Composer environment.
    *   Ensure the `BIGQUERY_CONNECTION_ID` (e.g., `google_cloud_default`) in the Airflow DAG is correctly configured in your Airflow environment.
    *   Upload the `airflow_dag_vertragsdatenabgleich.py` file to the DAGs folder of your Cloud Composer environment.
    *   Replace `my_gcp_project` and `my_dataset` placeholders in the DAG with your actual project and dataset IDs.

7.  **Scheduling**:
    *   If using Cloud Composer, configure the desired schedule for the `vertragsdatenabgleich_wrapper_dag` within the Airflow UI.
    *   Alternatively, the BigQuery stored procedure can be scheduled directly using BigQuery Scheduled Queries or triggered via Cloud Functions/Workflows.

## 5. Known gaps & unresolved references

The following items are identified as known gaps, unresolved references, or areas requiring further follow-up:

*   **Core Business Logic (`k_ausd_v_ta_period.ksh`) Migration (B4 Item)**: The most significant gap is the detailed migration of the `k_ausd_v_ta_period.ksh` script. The current `project.dataset.k_ausd_v_ta_period` stored procedure is a placeholder. Its full implementation requires a dedicated analysis to understand its exact data manipulation logic, potential external system interactions, and any non-SQL operations. This is flagged as a B4 (Redesign) item, as complex procedural logic or external calls might necessitate alternative GCP services (e.g., Dataflow, Dataproc, Cloud Functions) orchestrated by Cloud Composer.
*   **Exact `DWMSG_` Functionality**: The original `DWMSG_` utility functions (e.g., `DWMSG_MeldeFehler`) might have performed actions beyond simple logging (e.g., sending email alerts, triggering other processes). The current migration maps these to BigQuery table inserts. If additional actions were present, they need to be identified and re-implemented using appropriate GCP services (e.g., Cloud Pub/Sub, Cloud Functions for notifications).
*   **Shell `trap INT ERR` Equivalence**: While BigQuery's `EXCEPTION` blocks handle errors robustly, they do not directly replicate the operating system signal handling of `trap INT` (for user-initiated interruptions). The current design assumes the orchestration layer (e.g., Cloud Composer) will manage graceful task termination or retry logic in such scenarios.
*   **Configuration Management from `.dw_init`**: The original script sourced `$HOME/.dw_init` for environment initialization. The migration replaces this with direct parameters or hardcoded values. A more centralized and dynamic configuration management strategy (e.g., a BigQuery configuration table, environment variables in Cloud Composer, or Secret Manager for sensitive data) should be established for production environments.
*   **`ta_period` Table Definition**: The target `ta_period` table (or its BigQuery equivalent) that the core logic interacts with is not defined in this migration. Its schema and existence are implicit dependencies that must be addressed during the `k_ausd_v_ta_period` migration.

## 6. Validation

Validation of the migrated wrapper involves ensuring that it correctly handles parameters, logs job status, invokes the core logic, and reports errors as expected.

**How to run the tests:**

1.  **Deploy all generated BigQuery DDLs and Stored Procedures** to your target BigQuery dataset.
2.  **Manual BigQuery Stored Procedure Calls**:
    *   **Test Help Message**:
        ```sql
        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_stichtag => NULL, p_log_level => 'INFO', p_show_help => TRUE);
        ```
    *   **Test Missing Stichtag**:
        ```sql
        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_stichtag => NULL, p_log_level => 'INFO', p_show_help => FALSE);
        ```
    *   **Test Invalid Stichtag Format**:
        ```sql
        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_stichtag => '2023-01-01', p_log_level => 'INFO', p_show_help => FALSE);
        ```
    *   **Test Successful Run (with placeholder core logic)**:
        ```sql
        CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_stichtag => '01012023', p_log_level => 'INFO', p_show_help => FALSE);
        ```
    *   **Test Core Logic Failure (if `k_ausd_v_ta_period` is modified to simulate failure)**:
        Modify `project.dataset.k_ausd_v_ta_period` to `RAISE BQ.ERROR('Simulated error');` and then call the wrapper with valid parameters.
3.  **Cloud Composer (Airflow) Validation (if deployed)**:
    *   Trigger the `vertragsdatenabgleich_wrapper_dag` manually from the Airflow UI.
    *   Observe the task logs and DAG run status.

**What "passing" means:**

*   **`job_control` table**:
    *   For successful runs: A new entry should appear with `status = 'OK'` and `finished_ts` populated.
    *   For failed runs: A new entry should appear with `status = 'ERROR'` and `finished_ts` populated.
*   **`job_log` table**:
    *   Contains a sequence of log messages corresponding to the job's execution flow (start, calling core, core completion, wrapper completion).
    *   For help calls, it should contain the usage message.
    *   For parameter validation failures, it should contain the relevant error message.
*   **`job_error_log` table**:
    *   For successful runs: This table should remain empty for the specific `job_entry_nr`.
    *   For failed runs (e.g., invalid parameters, core logic failure): A new entry should appear with `error_nr`, `error_arg`, and a detailed `error_message`.
*   **BigQuery Query Results**:
    *   Direct calls to the wrapper SP should either complete successfully or raise a BigQuery error with a descriptive message.
*   **Cloud Composer UI**:
    *   The `vertragsdatenabgleich_wrapper_dag` run should show a "success" status for successful executions and a "failed" status for expected failures, with detailed logs available for debugging.
*   **Data Synchronization (after `k_ausd_v_ta_period` is implemented)**:
    *   The `ta_period` table (or its BigQuery equivalent) should reflect the correct data changes as per the business logic. This is the ultimate validation for the core functionality.

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Stop New Executions**:
    *   If using Cloud Composer, pause or delete the `vertragsdatenabgleich_wrapper_dag` to prevent any further scheduled runs.
    *   Ensure no other automated triggers (e.g., BigQuery Scheduled Queries, Cloud Functions) are invoking the BigQuery stored procedures.

2.  **BigQuery Artifacts Deletion**:
    *   **Drop Stored Procedures**:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.vertragsdatenabgleich_wrapper`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_v_ta_period`;
        ```
    *   **Drop Tables**:
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_control`;
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        ```
    *   **Important Note on Data**: If the `project.dataset.k_ausd_v_ta_period` (once fully implemented) has modified data in the `ta_period` table (or its BigQuery equivalent), a data rollback strategy will be required. This could involve:
        *   Restoring the `ta_period` table from a point-in-time snapshot or backup taken before the migration.
        *   Executing specific DML statements to revert changes.
        *   **This is a critical consideration, especially given the "placeholder" status of the core logic.**

3.  **Cloud Composer DAG Undeployment**:
    *   If the Airflow DAG was deployed, remove the `airflow_dag_vertragsdatenabgleich.py` file from the Cloud Composer DAGs folder.

4.  **Revert to Original Script**:
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` script and its dependencies are available and configured to run in the legacy environment.
    *   Resume any scheduling or triggering mechanisms that were previously used for the original script.