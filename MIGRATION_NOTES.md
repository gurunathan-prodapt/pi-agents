# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `r_ausd_v_ta_cntrct_crs3.ksh`, along with its implicitly invoked core data processing script `k_ausd_v_ta_cntrct_crs3.ksh` (which utilizes `d_ausd_v_ta_cntrct_crs3.sql` for its logic), has been migrated.

The migration target platform is Google Cloud Platform, leveraging:
*   **Google BigQuery** for data storage, transformation, and the execution of core business logic via Stored Procedures.
*   **Cloud Composer (Apache Airflow)** for job orchestration, scheduling, and monitoring.

The original script's wrapper functionality (parameter parsing, logging, and orchestration) has been translated into a BigQuery Stored Procedure, and its core data processing logic has been translated into another BigQuery Stored Procedure. File-based logging has been replaced with structured logging in BigQuery tables.

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/ddl/create_tables.sql`**
    *   **Role:** Contains Data Definition Language (DDL) statements to create the necessary BigQuery tables. This includes:
        *   `dw_job_log`: For structured logging of job execution status and metadata.
        *   `dw_error_log`: For detailed logging of errors encountered during job execution.
        *   `sof_ta_cntrct_crs3`: The target table for the contract data.
        *   `sof_ta_cntrct_crs2`: A source table for contract data, mirroring the structure used in the original SQL.
        *   `dwtk_meldungen`: A source table for metadata/messages, mirroring the structure used in the original SQL.

*   **`sql/sp/sp_k_ausd_v_ta_cntrct_crs3.sql`**
    *   **Role:** A BigQuery Stored Procedure that encapsulates the core data processing and transformation logic previously found in `k_ausd_v_ta_cntrct_crs3.ksh` and `d_ausd_v_ta_cntrct_crs3.sql`. This procedure is responsible for truncating the target table and inserting reconciled contract data.

*   **`sql/sp/sp_vertragsdatenabgleich.sql`**
    *   **Role:** A BigQuery Stored Procedure that serves as the main wrapper and orchestration component. It handles:
        *   Job metadata initialization.
        *   Logging job start and end status to `dw_job_log`.
        *   Calling `sp_k_ausd_v_ta_cntrct_crs3` to execute the core logic.
        *   Implementing error handling (`EXCEPTION WHEN ERROR THEN`) and logging errors to `dw_error_log`.
        *   Updating job status based on execution outcome.

*   **`dags/r_ausd_v_ta_cntrct_crs3_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for scheduling and triggering the execution of the `sp_vertragsdatenabgleich` BigQuery Stored Procedure within the Cloud Composer environment.

## 3. Key design decisions

The migration strategy involved several key design decisions to translate the KornShell script's functionality to Google Cloud Platform:

*   **Wrapper Logic to BigQuery Stored Procedure:** The original KornShell script acted primarily as a wrapper, handling environment setup, parameter parsing, logging, and orchestrating the execution of a core script. This functionality was migrated to a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This centralizes job control, leverages BigQuery's native error handling, and keeps the orchestration logic close to the data processing.
*   **Core Data Processing to BigQuery Stored Procedure:** The actual data reconciliation logic, originally in `k_ausd_v_ta_cntrct_crs3.ksh` (which used `d_ausd_v_ta_cntrct_crs3.sql`), was directly translated into a separate BigQuery Stored Procedure (`sp_k_ausd_v_ta_cntrct_crs3`). This decision was based on the assumption that the core logic is primarily SQL-based, allowing for efficient, scalable execution within BigQuery.
*   **Structured Logging in BigQuery:** The legacy file-based logging (`LogDatei`) and custom `DWMSG_*` functions were replaced with dedicated BigQuery tables (`dw_job_log` and `dw_error_log`). This provides structured, queryable logs, enabling easier monitoring, auditing, and debugging compared to parsing text files.
*   **Cloud Composer for Orchestration:** The legacy scheduler for the KornShell script was replaced by Cloud Composer (Apache Airflow). Airflow offers robust scheduling capabilities, dependency management, retry mechanisms, and a rich UI for monitoring job executions, significantly improving operational visibility and reliability.
*   **Parameter Handling Translation:** The `getopts` mechanism used in the KornShell script for command-line argument parsing was translated into `IN` parameters for the BigQuery Stored Procedures, allowing for clear input definition and validation.
*   **Error Handling Modernization:** The shell's `trap` mechanism for error handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides structured exception handling within the SQL context, ensuring errors are caught, logged, and propagated appropriately.

**Notable Trade-offs:**
*   **Increased BigQuery Dependency:** The solution relies heavily on BigQuery for both data processing and orchestration logic. While this leverages BigQuery's strengths, it means less flexibility for operations that might traditionally be handled by shell scripts (e.g., complex file system interactions), though this was not a primary concern for this specific job.
*   **Initial Setup Complexity:** Setting up Cloud Composer and configuring BigQuery resources (datasets, tables, stored procedures, IAM) requires more initial effort compared to deploying a simple shell script.
*   **Error Code Mapping:** The original `ErrNr` values and `DWMSG_*` messages have been replaced by BigQuery's native error messages and a generic error logging mechanism. A direct one-to-one mapping of all legacy error codes was not performed, focusing instead on capturing the BigQuery error details.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Google Cloud Project and Dataset Setup:**
    *   Ensure a Google Cloud Project is active and billing is enabled.
    *   Create the target BigQuery Dataset (e.g., `YOUR_DATASET_ID`) where the tables and stored procedures will reside.
2.  **IAM Permissions Configuration:**
    *   Grant the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to the service account that Cloud Composer will use to execute BigQuery jobs.
    *   Ensure the Cloud Composer environment's service account has permissions to create/manage BigQuery resources if DDLs are to be run by Composer.
3.  **BigQuery Schema and Table Creation:**
    *   Execute the DDL statements in `sql/ddl/create_tables.sql` against your target BigQuery Dataset (`YOUR_PROJECT_ID.YOUR_DATASET_ID`). This will create `dw_job_log`, `dw_error_log`, `sof_ta_cntrct_crs3`, `sof_ta_cntrct_crs2`, and `dwtk_meldungen`.
4.  **BigQuery Stored Procedure Deployment:**
    *   Deploy `sql/sp/sp_k_ausd_v_ta_cntrct_crs3.sql` to your BigQuery Dataset.
    *   Deploy `sql/sp/sp_vertragsdatenabgleich.sql` to your BigQuery Dataset.
5.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
6.  **Airflow DAG Deployment:**
    *   Upload the `dags/r_ausd_v_ta_cntrct_crs3_dag.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Placeholder Replacement:**
    *   In all generated SQL and Python files, replace `YOUR_PROJECT_ID` and `YOUR_DATASET_ID` with your actual Google Cloud Project ID and BigQuery Dataset ID.
    *   In `dags/r_ausd_v_ta_cntrct_crs3_dag.py`, update the `location` parameter in `BigQueryInsertJobOperator` to match your BigQuery dataset's region (e.g., `'us-central1'`, `'eu'`).
8.  **Scheduling Configuration:**
    *   In `dags/r_ausd_v_ta_cntrct_crs3_dag.py`, update the `schedule_interval` parameter from `None` to your desired cron expression (e.g., `'0 5 * * *'` for daily at 5 AM UTC).
9.  **Initial Data Load (if applicable):**
    *   Ensure that the source tables `sof_ta_cntrct_crs2` and `dwtk_meldungen` are populated with any necessary historical or initial data, if they are not populated by other upstream processes.

## 5. Known gaps & unresolved references

The migration addressed the explicit logic found in the provided source. However, certain aspects require further attention or clarification:

*   **Content of `k_ausd_v_ta_cntrct_crs3.ksh`:** The migration of the core logic (`sp_k_ausd_v_ta_cntrct_crs3`) was based on the assumption that `k_ausd_v_ta_cntrct_crs3.ksh` primarily executed SQL statements (specifically `d_ausd_v_ta_cntrct_crs3.sql`). If `k_ausd_v_ta_cntrct_crs3.ksh` contained complex non-SQL operations (e.g., file system manipulation, external API calls, or intricate shell scripting logic), these aspects would require a different migration approach (e.g., Cloud Functions, Dataflow, or custom Python operators in Airflow) and are not covered by the current BigQuery Stored Procedure.
*   **Environment Initialization (`.dw_init`) and Utility Scripts:** The exact variables and configurations set by `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` were translated to BigQuery native functions or explicit declarations where their purpose was clear. Any remaining untranslated environment setup or utility logic that is critical for the job's execution context needs to be identified and replicated in the BigQuery/Cloud Composer environment.
*   **Robust ID Generation for Logs:** The `job_entry_id` and `error_id` in `dw_job_log` and `dw_error_log` are currently generated using `MAX(id) + 1`. This approach is not robust for concurrent executions and could lead to ID collisions. For a production environment, consider using BigQuery's `GENERATE_UUID()`, a sequence table, or a more sophisticated ID generation strategy.
*   **Unused Parameters (`p_s`, `p_l`):** The original KornShell script defined parameters `-s` and `-l` but did not appear to use their values. These have been carried over as `IN` parameters to `sp_vertragsdatenabgleich` but remain unused. Their original intent, if any, should be clarified.
*   **`DWPA_UTIL_SKRIPT.runstatement` Functionality:** The `TRUNCATE TABLE` operation, originally invoked via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs3')`, has been directly translated to `TRUNCATE TABLE`. If `DWPA_UTIL_SKRIPT.runstatement` had additional side effects or complex logic beyond simple statement execution, those aspects are not captured.
*   **`BERT_DROP_TEMP_TABLE` Dependency:** The `sp_k_ausd_v_ta_cntrct_crs3` procedure queries `dwtk_meldungen` filtered by `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This implies an upstream process or job is responsible for populating `dwtk_meldungen` with this specific `job_kennung`. This dependency needs to be understood and ensured for the migrated job to function correctly.

## 6. Validation

Validation of the migrated job should cover unit testing of individual components and end-to-end integration testing.

**Unit Tests:**

1.  **`sp_k_ausd_v_ta_cntrct_crs3` (Core Logic):**
    *   **Procedure:** Manually populate `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs2` and `YOUR_PROJECT_ID.YOUR_DATASET_ID.dwtk_meldungen` with representative test data, including edge cases.
    *   **Execution:** Execute the stored procedure directly in BigQuery:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_k_ausd_v_ta_cntrct_crs3`('TEST_JOB_KENNUNG', 1);
        ```
    *   **Verification:** Query `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs3` to ensure the data is transformed and loaded correctly according to the expected output of the original `d_ausd_v_ta_cntrct_crs3.sql`.
2.  **`sp_vertragsdatenabgleich` (Wrapper Logic):**
    *   **Execution (Success):** Execute the stored procedure directly in BigQuery:
        ```sql
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(NULL, NULL);
        ```
    *   **Verification:**
        *   Check `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log` for a new entry with `status = 'OK'` and appropriate `start_timestamp`/`end_timestamp`.
        *   Verify that `sp_k_ausd_v_ta_cntrct_crs3` was called and its output is correct in `sof_ta_cntrct_crs3`.
    *   **Execution (Failure):** Introduce a deliberate error in `sp_k_ausd_v_ta_cntrct_crs3` (e.g., by referencing a non-existent table) and re-execute `sp_vertragsdatenabgleich`.
    *   **Verification:**
        *   Check `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log` for an entry with `status = 'ERROR'`.
        *   Check `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_error_log` for a detailed error entry corresponding to the failed execution.

**Integration Tests (End-to-End):**

1.  **Airflow DAG Trigger:**
    *   Upload the `dags/r_ausd_v_ta_cntrct_crs3_dag.py` to your Cloud Composer environment.
    *   Manually trigger the DAG from the Airflow UI.
2.  **Monitoring:**
    *   Monitor the DAG run status in the Airflow UI to ensure it completes successfully.
    *   Observe BigQuery job history for the execution of the stored procedures.
3.  **Data Validation:**
    *   After a successful DAG run, query `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs3` in BigQuery.
    *   **Data Comparison:** Compare the data in `sof_ta_cntrct_crs3` with the output generated by the legacy `r_ausd_v_ta_cntrct_crs3.ksh` script for the same input data. This is the most critical step to ensure functional equivalence.
4.  **Logging Validation:**
    *   Query `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log` and `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_error_log` to confirm that job execution and any errors are logged correctly and comprehensively.

**"Passing" Criteria:**

*   The Airflow DAG `r_ausd_v_ta_cntrct_crs3_dag` completes successfully without any task failures.
*   The `dw_job_log` table contains a successful entry for the job execution, indicating `status = 'OK'`.
*   No unexpected errors are recorded in the `dw_error_log` table for successful runs.
*   The data in the target table `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs3` is identical (or functionally equivalent, considering data type changes) to the output produced by the original `r_ausd_v_ta_cntrct_crs3.ksh` for the same input.
*   The execution time and resource consumption are within acceptable performance thresholds.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be followed to revert to the legacy system:

1.  **Pause/Delete Airflow DAG:**
    *   In the Cloud Composer Airflow UI, locate the `r_ausd_v_ta_cntrct_crs3_dag` DAG.
    *   Set the DAG to "Off" (pause) or delete it entirely to prevent further executions of the migrated job.
2.  **Revert BigQuery Stored Procedures (if necessary):**
    *   If previous versions of `sp_vertragsdatenabgleich` or `sp_k_ausd_v_ta_cntrct_crs3` existed and need to be restored, deploy those older versions.
    *   If these are entirely new procedures, they can be dropped using `DROP PROCEDURE IF EXISTS YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich;` and `DROP PROCEDURE IF EXISTS YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_k_ausd_v_ta_cntrct_crs3;`.
3.  **Restore Target Data (if modified):**
    *   If the `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs3` table was modified by the migrated job and needs to be reverted, use BigQuery's time travel feature to restore the table to a state before the problematic execution, or restore from a backup if available.
    *   The logging tables (`dw_job_log`, `dw_error_log`) are append-only and typically do not require rollback; new entries can simply be ignored.
4.  **Re-enable Legacy System:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh` script in its legacy environment.
    *   Ensure its original scheduler is re-activated.
5.  **Verify Legacy Operation:**
    *   Confirm that the legacy job is running as expected and producing correct output.