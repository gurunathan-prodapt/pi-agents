# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_acc_ref.ksh` from its legacy environment to Google BigQuery.

The original script served as an orchestration wrapper for a core reconciliation process, handling parameter parsing, environment setup, logging, and error handling. It invoked a core script, `k_ausd_v_ta_acc_ref.ksh`, which contains the primary business logic.

The migration strategy involved translating the wrapper's orchestration logic into a BigQuery Stored Procedure. The target platform is Google BigQuery, utilizing its native SQL capabilities for procedural logic, data definition, and logging.

## 2. Generated artifacts

The following BigQuery artifacts have been generated as part of this migration:

*   **`src/bigquery/ddl/isbert_logs.job_log.sql`**
    *   **Role:** Defines the `job_log` table within the `isbert_logs` dataset. This table serves as the primary record for overall job execution status, start/end times, and final status (SUCCESS/FAILED), replacing the high-level log entries of the original script.
*   **`src/bigquery/ddl/isbert_logs.job_error_log.sql`**
    *   **Role:** Defines the `job_error_log` table within the `isbert_logs` dataset. This table captures detailed error information, including error codes, messages, and stack traces, replacing the error handling and reporting mechanisms of the original script.
*   **`src/bigquery/ddl/isbert_logs.job_log_detail.sql`**
    *   **Role:** Defines the `job_log_detail` table within the `isbert_logs` dataset. This table stores granular, timestamped log messages generated during job execution, effectively replacing the file-based log output (`LogDatei`) of the original script.
*   **`src/bigquery/ddl/isbert_aufbereitung.configuration.sql`**
    *   **Role:** Defines a `configuration` table within the `isbert_aufbereitung` dataset. This table is intended to store key-value pairs for environment variables and configuration settings that were previously sourced from files like `.dw_init` or hardcoded paths.
*   **`src/bigquery/procedures/isbert_aufbereitung.f_alis_msgerr_bq_placeholder.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the functionality of the legacy `f_alis_msgerr.ksh` utility script. It currently logs error details to `isbert_logs.job_error_log`. Further implementation is required to fully replicate the original script's error messaging and reporting.
*   **`src/bigquery/procedures/isbert_aufbereitung.h_alis_parameter_bq_placeholder.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the functionality of the legacy `h_alis_parameter.ksh` utility script. It demonstrates how command-line parameter parsing is replaced by `IN` parameters in BigQuery procedures. Further implementation is required to fully replicate the original script's parameter handling logic.
*   **`src/bigquery/procedures/isbert_aufbereitung.h_alis_date_bq_placeholder.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the functionality of the legacy `h_alis_date.ksh` utility script. It provides basic date formatting. Further implementation is required to fully replicate the original script's date manipulation logic.
*   **`src/bigquery/procedures/isbert_aufbereitung.k_ausd_v_ta_acc_ref.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the core business logic originally contained in `k_ausd_v_ta_acc_ref.ksh`. This is a critical component that requires detailed analysis and full implementation of its ETL logic in BigQuery SQL or Python.
*   **`src/bigquery/procedures/isbert_aufbereitung.vertragsdatenabgleich.sql`**
    *   **Role:** The main BigQuery Stored Procedure that replicates the orchestration logic of `r_ausd_v_ta_acc_ref.ksh`. It handles input parameters, initializes logging, calls the core `k_ausd_v_ta_acc_ref` procedure, and manages job status and error reporting within BigQuery. This is the direct migration of the wrapper script.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Orchestration:** The `r_ausd_v_ta_acc_ref.ksh` script, being an orchestration wrapper, was naturally translated into a BigQuery Stored Procedure (`isbert_aufbereitung.vertragsdatenabgleich`). This allows BigQuery to manage the control flow, parameter handling, and error management natively, leveraging its robust SQL capabilities.
*   **BigQuery Tables for Logging:** File-based logging (e.g., `LogDatei`) has been replaced by dedicated BigQuery tables (`isbert_logs.job_log`, `isbert_logs.job_error_log`, `isbert_logs.job_log_detail`). This centralizes logging, enables easier querying and analysis of job execution history, and integrates seamlessly with Google Cloud's monitoring tools.
*   **Parameter Handling via Procedure Arguments:** Command-line argument parsing (`getopts`) in the original script is replaced by `IN` parameters in the BigQuery Stored Procedure. This provides a clear, type-safe interface for invoking the job.
*   **Structured Error Handling:** The shell `trap` mechanism for error handling is replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides a structured and robust way to catch and log errors, ensuring job status is correctly updated.
*   **Modularization of Utility Functions:** Sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are represented as separate BigQuery Stored Procedure placeholders. This maintains modularity and allows for their independent development and testing.
*   **Configuration Management via BigQuery Table:** Environment variables and configuration paths (e.g., `BERT_DIR_ROOT`, `.dw_init`) are intended to be managed through a BigQuery configuration table (`isbert_aufbereitung.configuration`). This centralizes configuration and makes it auditable and manageable within the BigQuery environment.
*   **Core Logic as a Separate Procedure:** The invocation of `k_ausd_v_ta_acc_ref.ksh` is translated into a `CALL` to a separate BigQuery Stored Procedure (`isbert_aufbereitung.k_ausd_v_ta_acc_ref`). This clearly separates the orchestration logic from the core business logic, facilitating independent development and future maintenance.

**Notable Trade-offs:**

*   **Placeholder Implementations:** Several components (utility functions, core business logic) are currently placeholders. This allows the wrapper to be migrated and tested independently, but defers the full functionality.
*   **`DW_EintragsNr` Generation:** The generation of `v_dw_eintrags_nr` is currently a simple timestamp-based `CAST`. For production, a more robust, sequential, and unique identifier mechanism (e.g., a dedicated sequence table or a more sophisticated UUID generation) might be preferred to ensure uniqueness and order.
*   **`run_id` Generation:** `GENERATE_UUID()` is used for `run_id`. While unique, ensuring this `run_id` is consistently passed and used across all logging calls within a single execution requires careful implementation, especially if sub-procedures are involved.
*   **`p_log_to_stdout_only` Parameter:** The original `-l` flag redirected output to a log file. In BigQuery, logging is primarily to tables. The `p_log_to_stdout_only` parameter is a conceptual placeholder; its actual implementation might involve conditional `RAISE` statements for console output or simply relying on BigQuery's query results for "stdout".

## 4. Manual steps before go-live

Before the `isbert_aufbereitung.vertragsdatenabgleich` procedure can be used in a production environment, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the `isbert_logs` dataset exists in your BigQuery project.
    *   Ensure the `isbert_aufbereitung` dataset exists in your BigQuery project.
    *   If they don't exist, create them using the BigQuery UI, `bq` command-line tool, or Terraform/Cloud Deployment Manager.
        ```bash
        bq mk --dataset --default_table_expiration 36500 --default_partition_expiration 36500 your_project_id:isbert_logs
        bq mk --dataset --default_table_expiration 36500 --default_partition_expiration 36500 your_project_id:isbert_aufbereitung
        ```
        (Adjust `default_table_expiration` and `default_partition_expiration` as per data retention policies).

2.  **Schema/Table Creation:**
    *   Execute the DDL scripts for the logging tables:
        *   `src/bigquery/ddl/isbert_logs.job_log.sql`
        *   `src/bigquery/ddl/isbert_logs.job_error_log.sql`
        *   `src/bigquery/ddl/isbert_logs.job_log_detail.sql`
    *   Execute the DDL script for the configuration table:
        *   `src/bigquery/ddl/isbert_aufbereitung.configuration.sql`

3.  **IAM/Permissions:**
    *   The service account or user executing these procedures must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on the `isbert_logs` dataset (to insert/update log entries).
        *   `BigQuery Data Editor` on any datasets where `k_ausd_v_ta_acc_ref` will read/write data.
        *   `BigQuery Job User` (to run queries and procedures).
        *   `BigQuery Metadata Viewer` (to view table/procedure definitions).
        *   `BigQuery Data Viewer` on `isbert_aufbereitung.configuration` (to read configuration).
        *   `BigQuery Admin` or `BigQuery Data Editor` on the `isbert_aufbereitung` dataset (to create/replace procedures).

4.  **Configuration Table Population:**
    *   Insert necessary configuration values into `isbert_aufbereitung.configuration`. This includes values that were previously sourced from `.dw_init` or other environment variables.
    *   Example:
        ```sql
        INSERT INTO `your_project_id.isbert_aufbereitung.configuration` (config_key, config_value, description) VALUES
        ('BERT_DIR_ROOT_LEGACY', '/vobs/dw_source/isrpt/isbert/SQL/aktuell', 'Legacy root directory for BERT scripts'),
        ('DEFAULT_LOG_BUCKET', 'gs://your-project-logs-bucket/isbert/', 'Cloud Storage bucket for detailed logs if needed');
        ```

5.  **Scheduling:**
    *   Determine the appropriate scheduling mechanism for the `isbert_aufbereitung.vertragsdatenabgleich` procedure. Options include:
        *   **Cloud Scheduler:** For simple time-based scheduling, triggering a Cloud Function or a Pub/Sub topic that then invokes the BigQuery procedure.
        *   **Cloud Composer (Apache Airflow):** For complex DAGs, dependency management, and integration with other Google Cloud services. This is recommended for production ETL workflows.
        *   **Cloud Workflows:** For orchestrating a sequence of Google Cloud services, including BigQuery procedures.

6.  **Core Script (`k_ausd_v_ta_acc_ref`) Implementation:**
    *   **Crucially**, the placeholder `src/bigquery/procedures/isbert_aufbereitung.k_ausd_v_ta_acc_ref.sql` must be fully implemented with the actual business logic from the original `k_ausd_v_ta_acc_ref.ksh` script. This may involve:
        *   Translating SQL queries.
        *   Re-engineering file operations to use Cloud Storage.
        *   Migrating complex procedural logic to BigQuery SQL or Python (e.g., Cloud Functions, Dataflow).

7.  **Utility Function Implementation:**
    *   The placeholder procedures for `f_alis_msgerr`, `h_alis_parameter`, and `h_alis_date` must be fully implemented to replicate their original functionality.

## 5. Known gaps & unresolved references

The following items are identified as gaps or require further attention:

*   **Core Business Logic (`k_ausd_v_ta_acc_ref.ksh`)**: The most significant gap. The `isbert_aufbereitung.k_ausd_v_ta_acc_ref` BigQuery Stored Procedure is currently a placeholder. Its full implementation, including data sources, transformation logic, and output targets, is pending detailed analysis of the original KornShell script. This is a **B4 item** (requires detailed analysis and implementation).
*   **Utility Function Implementation**: The BigQuery Stored Procedures `isbert_aufbereitung.f_alis_msgerr_bq_placeholder`, `isbert_aufbereitung.h_alis_parameter_bq_placeholder`, and `isbert_aufbereitung.h_alis_date_bq_placeholder` are placeholders. Their full functionality, as present in the original KornShell utility scripts, needs to be implemented in BigQuery SQL. This is a **B4 item**.
*   **Robust `DW_EintragsNr` Generation**: The current implementation of `v_dw_eintrags_nr` uses a simple timestamp cast, which might not guarantee uniqueness or sequentiality in a high-concurrency environment. A more robust mechanism (e.g., a dedicated sequence table, a BigQuery `SEQUENCE` object if available, or a more sophisticated UUID generation) should be considered.
*   **`p_log_to_stdout_only` Parameter Handling**: The original `-l` flag redirected output to a log file. The BigQuery migration primarily logs to tables. The `p_log_to_stdout_only` parameter in the BigQuery procedure is a conceptual placeholder. If direct console output is strictly required, it would need to be implemented via `RAISE` statements or by relying on BigQuery's query result output, which differs from traditional stdout.
*   **`SENDS_MAIL` Functionality**: The `lineage_edges` indicated `SENDS_MAIL` for `FUNCTION:DWMSG_ERMITTLENR`. While the script content suggests it's primarily a logging function, if there is any actual email sending functionality embedded, it needs to be identified and re-implemented using Google Cloud services (e.g., Pub/Sub notifications, Cloud Functions with email APIs like SendGrid or Mailgun).
*   **`BERT_DIR_ROOT` and `.dw_init` Configuration**: The `isbert_aufbereitung.configuration` table is designed to replace these. However, the exact content and usage of these legacy configurations need to be fully mapped and populated into the BigQuery configuration table.
*   **Error Line Number Mapping**: BigQuery's `@@error.line` or `@@error.stack_trace` does not directly map to a single line number in the same way a shell script error might. The `p_line_number` parameter in `f_alis_msgerr_bq_placeholder` is currently `NULL`. A strategy for more precise error location reporting within BigQuery procedures might be needed if granular line-level debugging is critical.

## 6. Validation

To validate the migrated `vertragsdatenabgleich` procedure, follow these steps:

1.  **Prerequisites:** Ensure all manual steps from Section 4 (Dataset/Table creation, IAM, Configuration) have been completed. The placeholder procedures for `k_ausd_v_ta_acc_ref` and utility functions must also be deployed.

2.  **Run the Procedure (Success Scenario):**
    *   Execute the main procedure in BigQuery:
        ```sql
        CALL `your_project_id.isbert_aufbereitung.vertragsdatenabgleich`(p_stichtag => '2023-01-01');
        ```
    *   **Expected "Passing" Criteria:**
        *   The `CALL` statement completes without raising an unhandled error.
        *   Query `SELECT * FROM `your_project_id.isbert_logs.job_log` ORDER BY start_timestamp DESC LIMIT 1;` should show:
            *   `status = 'SUCCESS'`
            *   `job_kennung` matching the expected value (e.g., `ISBERT_VTA_20230101`).
            *   `start_timestamp` and `end_timestamp` populated.
        *   Query `SELECT * FROM `your_project_id.isbert_logs.job_log_detail` WHERE job_kennung = 'ISBERT_VTA_20230101' ORDER BY log_timestamp;` should show:
            *   Informational messages about job start, stichtag, and successful completion.
            *   Messages from the `k_ausd_v_ta_acc_ref` placeholder.
        *   Query `SELECT * FROM `your_project_id.isbert_logs.job_error_log` WHERE job_kennung = 'ISBERT_VTA_20230101';` should return **no rows**.

3.  **Run the Procedure (Help Scenario):**
    *   Execute the procedure with the help flag:
        ```sql
        CALL `your_project_id.isbert_aufbereitung.vertragsdatenabgleich`(p_show_help => TRUE);
        ```
    *   **Expected "Passing" Criteria:**
        *   The BigQuery console output should display the usage and options message, similar to the original script's `-h` output.
        *   No entries should be created in `isbert_logs.job_log` or `isbert_logs.job_log_detail` for this execution.

4.  **Run the Procedure (Failure Scenario):**
    *   To test error handling, temporarily modify the `isbert_aufbereitung.k_ausd_v_ta_acc_ref` placeholder procedure to explicitly raise an error:
        ```sql
        CREATE OR REPLACE PROCEDURE isbert_aufbereitung.k_ausd_v_ta_acc_ref(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr INT64
        )
        BEGIN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_v_ta_acc_ref';
        END;
        ```
    *   Execute the main procedure again:
        ```sql
        CALL `your_project_id.isbert_aufbereitung.vertragsdatenabgleich`(p_stichtag => '2023-01-02');
        ```
    *   **Expected "Passing" Criteria:**
        *   The `CALL` statement should terminate with an error message in the BigQuery console.
        *   Query `SELECT * FROM `your_project_id.isbert_logs.job_log` ORDER BY start_timestamp DESC LIMIT 1;` should show:
            *   `status = 'FAILED'`
            *   `message` containing the error details (e.g., "Simulated error...").
        *   Query `SELECT * FROM `your_project_id.isbert_logs.job_error_log` WHERE job_kennung = 'ISBERT_VTA_20230102';` should return:
            *   One or more rows detailing the error, including `error_code`, `error_message` (e.g., "Simulated error..."), and `program_name`.
        *   Query `SELECT * FROM `your_project_id.isbert_logs.job_log_detail` WHERE job_kennung = 'ISBERT_VTA_20230102' ORDER BY log_timestamp;` should show:
            *   Messages indicating job start, and an `ERROR` level message about the job failure.

## 7. Rollback procedure

In case of issues or critical failures after deployment, the following steps outline the rollback procedure to revert to the original KornShell script:

1.  **Stop New Invocations:**
    *   Immediately disable or remove any scheduling mechanisms (e.g., Cloud Scheduler jobs, Cloud Composer DAGs, Cloud Workflows) that are invoking the BigQuery procedure `isbert_aufbereitung.vertragsdatenabgleich`.

2.  **Re-enable Original Script:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh` script in its legacy environment.
    *   Verify that the original script can execute successfully and produce its expected output (log files, data updates).

3.  **Delete BigQuery Procedures (Optional but Recommended for Clean-up):**
    *   Once the original script is confirmed to be running, you can optionally delete the migrated BigQuery procedures to avoid confusion and ensure a clean state.
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.isbert_aufbereitung.vertragsdatenabgleich`;
        DROP PROCEDURE IF EXISTS `your_project_id.isbert_aufbereitung.k_ausd_v_ta_acc_ref`;
        DROP PROCEDURE IF EXISTS `your_project_id.isbert_aufbereitung.f_alis_msgerr_bq_placeholder`;
        DROP PROCEDURE IF EXISTS `your_project_id.isbert_aufbereitung.h_alis_parameter_bq_placeholder`;
        DROP PROCEDURE IF EXISTS `your_project_id.isbert_aufbereitung.h_alis_date_bq_placeholder`;
        ```

4.  **Retain BigQuery Log Tables:**
    *   It is generally recommended to **NOT** delete the `isbert_logs.job_log`, `isbert_logs.job_error_log`, and `isbert_logs.job_log_detail` tables immediately. These tables contain valuable historical execution data for the migrated job, which can be crucial for post-mortem analysis or future re-migration attempts.
    *   The `isbert_aufbereitung.configuration` table can also be retained for reference.

5.  **Monitor Legacy System:**
    *   Closely monitor the re-enabled legacy KornShell script to ensure it operates correctly and stably after the rollback.