# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh` to Google BigQuery.

The original job, "Bereitstellung Basisprodukte BERT" (Provisioning Base Products BERT), is responsible for orchestrating the initial provisioning of selected base products by preparing a cutoff-date extraction of contract cache data from the Data Warehouse (DWH). It handles input parameters for the cutoff date and a restart value, manages logging, and delegates core data preparation to a downstream kernel script.

The migration translates this orchestration and parameter handling logic, along with the core business logic, into BigQuery Stored Procedures and associated logging/audit tables.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`your_bigquery_dataset/job_control_ddl.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_control` table. This table serves as the central repository for tracking the execution status, parameters, and metadata of each job run, mirroring the job control and logging functionality of the original shell script.
*   **`your_bigquery_dataset/job_error_log_ddl.sql`**
    *   **Role:** Defines the DDL for the `job_error_log` table. This table captures detailed error information, including timestamps, job IDs, error codes, and messages, providing a structured way to log and analyze job failures, replacing the shell script's error handling and logging to files.
*   **`your_bigquery_dataset/job_message_log_ddl.sql`**
    *   **Role:** Defines the DDL for the `job_message_log` table. This table stores general informational messages, warnings, and start/end notifications for each job execution, replacing the standard output and log file messages from the original shell script.
*   **`your_bigquery_dataset/k_ausd_bp_ta_bpr_opt_text.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure `k_ausd_bp_ta_bpr_opt_text`. This procedure is intended to encapsulate the core business logic for data extraction, filtering, and transformation, which was originally contained within the `k_ausd_bp_ta_bpr_opt_text.ksh` kernel script invoked by the wrapper. **Note: This procedure currently contains placeholder logic and requires full implementation of the data transformation.**
*   **`your_bigquery_dataset/ausd_bp_ta_bpr_opt_text_wrapper.sql`**
    *   **Role:** Defines the main BigQuery Stored Procedure `ausd_bp_ta_bpr_opt_text_wrapper`. This procedure acts as the direct replacement for the `r_ausd_bp_ta_bpr_opt_text.ksh` wrapper script. It handles parameter parsing, validation, default value assignment, job control table updates, and orchestrates the call to the `k_ausd_bp_ta_bpr_opt_text` procedure, including robust error handling.

## 3. Key Design Decisions

The migration to BigQuery Stored Procedures involved several key design decisions:

*   **BigQuery Stored Procedures for Orchestration and Logic:** The entire job, including both the wrapper and the core business logic, is translated into BigQuery Stored Procedures. This leverages BigQuery's native capabilities for procedural SQL, allowing for direct execution within the data warehouse environment without external compute resources for the core logic.
*   **Separation of Wrapper and Kernel Logic:** The original architecture's clear separation between the `r_ausd_bp_ta_bpr_opt_text.ksh` wrapper and the `k_ausd_bp_ta_bpr_opt_text.ksh` kernel script is maintained. This translates to two distinct BigQuery Stored Procedures: `ausd_bp_ta_bpr_opt_text_wrapper` for orchestration and `k_ausd_bp_ta_bpr_opt_text` for the core data transformations. This promotes modularity and reususability.
*   **Native BigQuery SQL for Helper Functions:** The functionalities provided by various shell helper scripts (e.g., `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are replaced by native BigQuery SQL functions, control flow statements (`IF`, `CASE`), and direct integration into the stored procedures. This eliminates external script dependencies and simplifies the execution environment.
*   **Structured Logging and Auditing:** Instead of file-based logging, a structured approach using dedicated BigQuery tables (`job_control`, `job_error_log`, `job_message_log`) is adopted. This centralizes job metadata, status, and messages, making it easier to monitor, query, and analyze job executions and errors.
*   **BigQuery Exception Handling:** The shell script's `trap` commands for error handling are replaced by BigQuery's `EXCEPTION WHEN ERROR THEN ... END` blocks within the stored procedures. This provides robust, native error management, allowing for graceful failure, logging of errors, and propagation of issues.
*   **Parameter Handling:** Command-line argument parsing and default value assignment are directly implemented within the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure using `IFNULL` and explicit `IF` conditions, mirroring the original script's behavior.
*   **UUID for Job Identification:** `GENERATE_UUID()` is used to create unique `job_id` values for each execution, providing a robust and scalable method for tracking individual job runs in the `job_control` and logging tables.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a Google Cloud Project is available.
    *   Create the target BigQuery dataset (e.g., `your_gcp_project.your_bigquery_dataset`) where the tables and stored procedures will reside.
2.  **IAM Permissions:**
    *   Grant appropriate IAM roles to the service account or user that will execute these procedures. This typically includes:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) for the target dataset to create tables and procedures, and to insert/update data in `job_control`, `job_error_log`, `job_message_log`, and any target data tables.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   If Cloud Storage is used for logs (as suggested by `gs://your-log-bucket/`), `Storage Object Creator` and `Storage Object Viewer` roles for the relevant bucket.
3.  **Deploy DDLs and Stored Procedures:**
    *   Execute the DDL scripts (`job_control_ddl.sql`, `job_error_log_ddl.sql`, `job_message_log_ddl.sql`) to create the logging tables in the target BigQuery dataset.
    *   Execute the `k_ausd_bp_ta_bpr_opt_text.sql` and `ausd_bp_ta_bpr_opt_text_wrapper.sql` scripts to create the stored procedures.
    *   **Crucially, replace `your_gcp_project.your_bigquery_dataset` with the actual project ID and dataset name in all generated SQL files before deployment.**
4.  **Implement Core Business Logic:**
    *   **The `k_ausd_bp_ta_bpr_opt_text` stored procedure (defined in `k_ausd_bp_ta_bpr_opt_text.sql`) contains placeholder logic (`TODO`). This procedure must be fully implemented with the actual data extraction, transformation, and loading (ETL) logic derived from the original `k_ausd_bp_ta_bpr_opt_text.ksh` script.** This includes identifying source tables (`your_source_table`), target tables (`your_target_table`), and the specific SQL queries for filtering by `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID`.
5.  **Configure Log Storage (Optional but Recommended):**
    *   If external log files are desired (as hinted by `v_log_file_path` in the wrapper), create the specified Cloud Storage bucket (e.g., `gs://your-log-bucket/`) and ensure appropriate permissions. The current implementation only logs to BigQuery tables, but the placeholder for `v_log_file_path` suggests future integration might be considered.
6.  **Scheduling:**
    *   Integrate the execution of the `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure into the appropriate scheduling tool (e.g., Cloud Composer, Cloud Scheduler, Dataform, or a custom orchestrator). Ensure the scheduler can pass the `p_input_stichtag` and `p_input_wiederanlaufwert` parameters as needed.

## 5. Known Gaps & Unresolved References

The following items are identified as gaps or require further attention:

*   **Core Business Logic Implementation (`k_ausd_bp_ta_bpr_opt_text`):** The `k_ausd_bp_ta_bpr_opt_text.sql` stored procedure is currently a placeholder. The full SQL logic for data extraction, filtering, transformation, and loading (including source and target table names like `your_source_table` and `your_target_table`) must be developed and implemented based on a detailed analysis of the original `k_ausd_bp_ta_bpr_opt_text.ksh` script. This is the most significant outstanding item.
*   **Missing Complexity Data:** The original `file_complexity` data was unavailable. This means the complexity of the `k_ausd_bp_ta_bpr_opt_text.ksh` script was not formally assessed, which could impact the effort required for its BigQuery SQL translation.
*   **Custom Shell Function Logic:** While the general purpose of custom shell functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_ErmittleNr` is understood, their precise internal logic (e.g., how `DWDate_Gib_Zeitraum` determines `v_sysdate` or `maxladedatum`) needs to be thoroughly reviewed during the implementation of `k_ausd_bp_ta_bpr_opt_text` to ensure accurate BigQuery SQL equivalents. The current `v_stichtag` default logic in the wrapper is a simplification.
*   **Character Encoding:** The presence of German special characters in the source comments suggests potential character encoding considerations. Ensure that data ingested into BigQuery and processed by the procedures correctly handles the expected character encoding to prevent data corruption.
*   **External Log File Path Placeholder:** The `v_log_file_path` variable in `ausd_bp_ta_bpr_opt_text_wrapper.sql` currently uses a placeholder `gs://your-log-bucket/`. While logging to BigQuery tables is implemented, if external file-based logging is still a requirement, this placeholder needs to be replaced with a concrete Cloud Storage bucket path, and a mechanism to write logs to this bucket would need to be implemented (e.g., via Cloud Functions triggered by BigQuery log entries, or by using an external orchestration tool that captures BigQuery job logs).

## 6. Validation

Validation of the migrated job involves unit testing of individual components and integration testing of the entire workflow.

**How to Run Tests:**

1.  **Prerequisites:** Ensure all DDLs are deployed, and the `k_ausd_bp_ta_bpr_opt_text` procedure has at least its placeholder logic (or ideally, a partial implementation for testing).
2.  **Execute the Wrapper Procedure:** Call the main wrapper stored procedure `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper` from the BigQuery console, a client tool, or via a scheduling mechanism.
    *   **Test Case 1: Default Parameters:**
        ```sql
        CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`(NULL, NULL);
        ```
    *   **Test Case 2: Valid Parameters:**
        ```sql
        CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`('01012023', 1000);
        ```
    *   **Test Case 3: Invalid Stichtag Format:**
        ```sql
        CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`('2023-01-01', NULL);
        ```
    *   **Test Case 4: Simulate Kernel Error:** (Requires modifying `k_ausd_bp_ta_bpr_opt_text` to intentionally raise an error for testing error handling).
3.  **Monitor BigQuery Job History:** Observe the job execution status in the BigQuery UI.
4.  **Query Logging Tables:** After each execution, query the `job_control`, `job_error_log`, and `job_message_log` tables to verify entries.
    ```sql
    SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_control` ORDER BY start_time DESC LIMIT 5;
    SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_message_log` ORDER BY log_timestamp DESC LIMIT 10;
    SELECT * FROM `your_gcp_project.your_bigquery_dataset.job_error_log` ORDER BY log_timestamp DESC LIMIT 5;
    ```
5.  **Data Validation (Once `k_ausd_bp_ta_bpr_opt_text` is implemented):**
    *   Compare the output data in the target BigQuery tables with the expected output from the legacy system for the same input parameters.
    *   Verify data counts, sums, and specific record values.

**What "Passing" Means:**

*   **Successful Execution:** The `ausd_bp_ta_bpr_opt_text_wrapper` procedure completes without unhandled errors.
*   **Correct `job_control` Entry:**
    *   A new entry exists with a unique `job_id`.
    *   `job_status` is 'OK' for successful runs, 'ERROR' for failed runs.
    *   `start_time` and `end_time` are correctly populated.
    *   `parameter_stichtag` and `parameter_wiederanlaufwert` reflect the input or default values.
    *   `message` accurately describes the job outcome.
*   **Accurate `job_message_log` Entries:**
    *   Messages indicating job start, parameter values, and successful completion are present.
    *   Messages from the `k_ausd_bp_ta_bpr_opt_text` procedure are logged.
*   **Correct `job_error_log` Entries (for error scenarios):**
    *   For invalid inputs or simulated errors, an entry exists with the correct `job_id`, `error_code`, and `error_message`.
*   **Data Integrity (Post `k_ausd_bp_ta_bpr_opt_text` implementation):**
    *   The data generated in the target tables by `k_ausd_bp_ta_bpr_opt_text` is identical (or functionally equivalent, considering BigQuery's data types and precision) to the data produced by the original `k_ausd_bp_ta_bpr_opt_text.ksh` script for the same inputs.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be followed:

1.  **Stop New Executions:** Immediately halt any scheduled or manual executions of the BigQuery stored procedure `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`.
2.  **Revert Scheduling:** Reconfigure the scheduler to invoke the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh` script.
3.  **Delete BigQuery Stored Procedures:**
    ```sql
    DROP PROCEDURE IF EXISTS `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`;
    DROP PROCEDURE IF EXISTS `your_gcp_project.your_bigquery_dataset.k_ausd_bp_ta_bpr_opt_text`;
    ```
4.  **Delete BigQuery Tables (Optional, but recommended for a clean rollback):**
    *   **Caution:** This will delete all historical job control and log data. If this data needs to be preserved for auditing or post-mortem analysis, consider renaming or archiving the tables instead of dropping them.
    ```sql
    DROP TABLE IF EXISTS `your_gcp_project.your_bigquery_dataset.job_control`;
    DROP TABLE IF EXISTS `your_gcp_project.your_bigquery_dataset.job_error_log`;
    DROP TABLE IF EXISTS `your_gcp_project.your_bigquery_dataset.job_message_log`;
    ```
    *   If `k_ausd_bp_ta_bpr_opt_text` created or modified any target data tables, those changes would also need to be reverted or the tables dropped/restored from backup, depending on the impact.
5.  **Verify Original System:** Confirm that the original KornShell script is running as expected and producing correct output.