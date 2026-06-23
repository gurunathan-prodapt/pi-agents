```markdown
# MIGRATION_NOTES: r_ausd_v_ta_barrier.ksh

## 1. Summary

This document details the migration of the `r_ausd_v_ta_barrier.ksh` job, which is responsible for the reconciliation and processing of contract data for the `ta_barrier` table. The original job consisted of a KornShell orchestration layer (`r_ausd_v_ta_barrier.ksh`, `k_ausd_v_ta_barrier.ksh`) and core data transformation logic implemented in Oracle PL/SQL (`d_ausd_v_ta_barrier.sql`).

The entire workflow has been re-implemented and migrated to Google Cloud BigQuery. The KornShell scripts have been converted into BigQuery Stored Procedures for orchestration and control, while the Oracle PL/SQL has been translated into BigQuery SQL within a dedicated BigQuery Stored Procedure for data transformation. The target platform is Google Cloud BigQuery, leveraging its native scripting capabilities, SQL dialect, and columnar storage.

## 2. Generated Artifacts

The migration produced the following BigQuery Stored Procedures:

*   **`isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`** (`isrpt_isbert_data_processing/d_ausd_v_ta_barrier_etl.sql`)
    *   **Role:** This procedure encapsulates the core data transformation logic. It truncates the target `sof_ta_barrier` table and populates it with transformed data from various `cds_ta_barrier` related source tables, applying filtering, `COALESCE`, `CASE` expressions (replacing Oracle `DECODE`), and date-based logic.
*   **`isrpt_isbert_data_processing.starteSQLSkript`** (`isrpt_isbert_data_processing/starteSQLSkript.sql`)
    *   **Role:** An auxiliary procedure that wraps the execution of the `d_ausd_v_ta_barrier_etl` procedure. It handles logging of the SQL script's start and completion, captures errors, and records execution results (e.g., records processed) into dedicated BigQuery logging tables. This replaces a function within the original `k_ausd_v_ta_barrier.ksh`.
*   **`isrpt_isbert_data_processing.k_ausd_v_ta_barrier_control`** (`isrpt_isbert_data_processing/k_ausd_v_ta_barrier_control.sql`)
    *   **Role:** This procedure serves as the control script, replacing `k_ausd_v_ta_barrier.ksh`. It manages job activation/deactivation logic (using a BigQuery `job_table`), performs parameter validation, and orchestrates the call to the `starteSQLSkript` procedure. It also handles error logging and status updates for the overall job run.
*   **`isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`** (`isrpt_isbert_data_processing/r_ausd_v_ta_barrier_wrapper.sql`)
    *   **Role:** This is the top-level wrapper procedure, replacing `r_ausd_v_ta_barrier.ksh`. It handles initial parameter passing, sets up logging, and invokes the `k_ausd_v_ta_barrier_control` procedure to initiate the entire workflow. This procedure is the primary entry point for scheduling the job.

## 3. Key Design Decisions

*   **BigQuery-Native Orchestration**: The KornShell wrapper and control scripts were migrated directly to BigQuery Stored Procedures.
    *   **Why**: This approach leverages BigQuery's built-in scripting capabilities, eliminating the need for external compute resources (like Cloud Functions or Cloud Run) for job orchestration. It keeps the entire workflow within the BigQuery ecosystem, simplifying deployment, monitoring, and security.
    *   **Trade-offs**: BigQuery scripting, while powerful, can be less flexible than a full-fledged programming language (e.g., Python in Cloud Composer) for complex logic or external API interactions. The orchestration logic is expressed in SQL, which might be less intuitive for complex control flows compared to shell scripting or Python.
*   **Direct SQL Translation for Data Transformation**: The Oracle PL/SQL was translated directly into BigQuery SQL within a Stored Procedure.
    *   **Why**: This ensures optimal performance by utilizing BigQuery's columnar storage and distributed query engine. BigQuery's SQL dialect is highly compatible with standard SQL, making the translation straightforward for most constructs (e.g., `COALESCE` for `NVL`, `CASE` for `DECODE`).
    *   **Trade-offs**: Oracle-specific functions or complex procedural logic required careful translation to BigQuery SQL equivalents, sometimes resulting in more verbose `CASE` statements.
*   **BigQuery Tables for Logging and Job Control**: File-based logging and Oracle job control tables were replaced with dedicated BigQuery tables.
    *   **Why**: Provides centralized, scalable, and queryable logging and auditing. This allows for easy analysis of job history, performance, and error trends using standard BigQuery SQL. It also enables robust job activation/deactivation logic directly within BigQuery.
    *   **Trade-offs**: Requires defining and maintaining specific BigQuery table schemas for logging and control, adding to the initial setup overhead.
*   **Parameter Handling via Procedure Arguments**: Command-line arguments from KornShell were mapped to input parameters of the BigQuery Stored Procedures.
    *   **Why**: This is the native way to pass dynamic values to BigQuery procedures, ensuring type safety and clear definition of inputs.
*   **BigQuery `EXCEPTION WHEN ERROR THEN` for Error Handling**: Shell `trap` statements and custom error functions were replaced by BigQuery's error handling blocks.
    *   **Why**: Provides structured error capture and allows for logging errors to BigQuery tables before re-raising them for external orchestrators.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset `isrpt_isbert_data_processing` in your target Google Cloud project. This dataset will house all migrated tables and stored procedures.
2.  **Schema and Table Creation**:
    *   **Source Data Tables**: Ensure the following BigQuery tables exist and are populated with the necessary data, mirroring their Oracle counterparts. The schemas (column names, data types) must match the original Oracle tables.
        *   `isrpt_isbert_data_processing.cds_ta_barrier`
        *   `isrpt_isbert_data_processing.cds_ta_barrier_class`
        *   `isrpt_isbert_data_processing.cds_ta_barrier_kind`
        *   `isrpt_isbert_data_processing.cds_ta_care_description`
        *   `isrpt_isbert_data_processing.dwtk_meldungen`
    *   **Target Data Table**: Create the target table with the specified schema:
        *   `isrpt_isbert_data_processing.sof_ta_barrier` (Schema: `cntrct_id`, `barrier_kind_id`, `sperrart`, `barrier_init_cv`, `barrier_reason_cv`, `sperr_beginn`, `sperr_ende`, `sperrgrund`, `bfc_age`, `ist_stillegung`)
    *   **Logging and Job Control Tables**: Create the following BigQuery tables with appropriate schemas (e.g., `job_id STRING`, `job_name STRING`, `log_message STRING`, `log_timestamp TIMESTAMP`, `log_level STRING` for `job_log`):
        *   `isrpt_isbert_data_processing.job_log`
        *   `isrpt_isbert_data_processing.job_error_log`
        *   `isrpt_isbert_data_processing.job_table` (Schema: `job_kennung STRING`, `status STRING`, `start_timestamp TIMESTAMP`, `end_timestamp TIMESTAMP`, `last_modified TIMESTAMP`)
        *   `isrpt_isbert_data_processing.sql_execution_results` (Schema: `job_id STRING`, `job_name STRING`, `script_name STRING`, `records_processed INT64`, `execution_timestamp TIMESTAMP`, `status STRING`, `error_message STRING`)
3.  **IAM/Permissions**:
    *   The Google Cloud service account or user identity that will execute the `r_ausd_v_ta_barrier_wrapper` procedure must have the following BigQuery roles on the project or, at minimum, on the `isrpt_isbert_data_processing` dataset:
        *   `BigQuery Data Editor` (to `INSERT`, `TRUNCATE`, `UPDATE` data in target and logging tables)
        *   `BigQuery Data Viewer` (to `SELECT` from source and logging tables)
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures)
4.  **Scheduling**:
    *   If the job is to be scheduled, configure a Cloud Scheduler job, a Cloud Composer DAG, or a Cloud Workflow to call the `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper` procedure with the required parameters (`p_job_name`, `p_job_kennung`, `p_aktiv_nr`).
    *   Example `bq` command for manual execution or integration into a script:
        ```bash
        bq query --project_id=<YOUR_PROJECT_ID> --nouse_legacy_sql \
          "CALL `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`('r_ausd_v_ta_barrier', 'BERT_TA_BARRIER_JOB', '1');"
        ```

## 5. Known Gaps & Unresolved References

*   **Missing Complexity Data (B4 Item)**: The original analysis indicated no specific complexity data for the source components. This implies a potential for hidden complexities in the KornShell or Oracle PL/SQL that might not have been fully captured during automated analysis. Manual review of the original scripts is recommended to ensure all nuances are addressed.
*   **Oracle Data Migration Strategy**: The migration assumes that all necessary Oracle source data (e.g., `cds$ta_barrier`, `dwtk_meldungen`) will be fully migrated and continuously available in BigQuery. The specific strategy, tooling, and timeline for this ongoing data ingestion from Oracle to BigQuery are critical and must be established and maintained independently.
*   **Precise Schema for Logging Tables**: While conceptual schemas for `job_log`, `job_error_log`, `job_table`, and `sql_execution_results` are provided, their exact column definitions (e.g., specific string lengths, nullability, partitioning, clustering) need to be finalized and implemented based on operational requirements.
*   **Oracle SQL*Plus Features**: The original `d_ausd_v_ta_barrier.sql` contained SQL*Plus specific commands (`START ../trace.sql.cfg`, `WHENEVER SQLERROR`). While most are not applicable in BigQuery, any subtle side effects or specific error handling logic tied to these commands might require further investigation and BigQuery-native equivalents if not fully covered by the `EXCEPTION WHEN ERROR THEN` blocks.
*   **`starteSQLSkript` Records Processed Count**: The `starteSQLSkript` procedure currently uses `(SELECT COUNT(*) FROM sof_ta_barrier)` as a placeholder for `v_records_processed`. The `d_ausd_v_ta_barrier_etl` procedure does not explicitly return the number of rows inserted. For accurate metrics, the `d_ausd_v_ta_barrier_etl` procedure should be modified to return this count, or the `starteSQLSkript` procedure should capture the `INSERT` statement's `ROW_COUNT()` result.
*   **Job Control Logic in `k_ausd_v_ta_barrier_control`**: The `starteSQLSkript` procedure notes that "The original script had logic for 'ignore active job' and 'deactivate older job' which should be handled by the calling procedure." This logic is now implemented in `k_ausd_v_ta_barrier_control` using the `job_table`. Ensure the `job_table` schema and the logic correctly reflect the original behavior, especially regarding concurrent job runs and handling of stuck jobs.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, perform the following tests:

1.  **Unit Testing of `d_ausd_v_ta_barrier_etl`**:
    *   **How to run**: Populate the source tables (`cds_ta_barrier`, etc.) and `dwtk_meldungen` with a small, controlled dataset. Execute `CALL `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`();`.
    *   **Passing means**: The `sof_ta_barrier` table is populated correctly, matching the expected output based on the source data and transformation logic. Verify row counts, data types, and specific column values (especially `SPERRGRUND` and date calculations).
2.  **Integration Testing of `r_ausd_v_ta_barrier_wrapper` (End-to-End)**:
    *   **How to run**: Execute the top-level wrapper procedure: `CALL `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`('r_ausd_v_ta_barrier', 'BERT_TA_BARRIER_JOB', '1');`.
    *   **Passing means**:
        *   The procedure completes successfully without raising any errors.
        *   The `sof_ta_barrier` table is populated with the expected data, matching the output of the legacy Oracle job for the same input data.
        *   The `job_log` table contains `INFO` entries for job start, SQL script call, and successful completion.
        *   The `job_table` shows the job status transitioning from `ACTIVE` to `SUCCESS`.
        *   The `sql_execution_results` table contains a successful entry with a non-zero `records_processed` count (once the gap for this count is addressed).
3.  **Error Handling Validation**:
    *   **How to run**: Introduce an error condition (e.g., drop a source table, insert invalid data that causes a BigQuery error) and run the `r_ausd_v_ta_barrier_wrapper` procedure.
    *   **Passing means**:
        *   The procedure terminates with an error.
        *   The `job_error_log` table contains a detailed error message.
        *   The `job_log` table contains an `ERROR` entry.
        *   The `job_table` shows the job status as `FAILED`.
4.  **Job Control Logic Validation**:
    *   **How to run**:
        *   Run `r_ausd_v_ta_barrier_wrapper`. While it's running, attempt to run it again.
        *   Manually set a job in `job_table` to `ACTIVE` and then run `r_ausd_v_ta_barrier_wrapper`.
    *   **Passing means**:
        *   The second concurrent run should log a `WARNING` message indicating the job is already active and exit gracefully without processing, as per the original `k_ausd_v_ta_barrier.ksh` logic.
        *   The run with a manually `ACTIVE` job should first `DEACTIVATE` the older entry and then proceed with the new run.
5.  **Performance Testing**:
    *   **How to run**: Execute the `r_ausd_v_ta_barrier_wrapper` with production-like data volumes.
    *   **Passing means**: The execution time and BigQuery slot consumption are within acceptable limits, ideally matching or improving upon the legacy job's performance.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop BigQuery Job Execution**:
    *   Immediately disable or delete any scheduled executions (e.g., Cloud Scheduler job, Cloud Composer DAG, Cloud Workflow) that trigger the `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper` procedure.
2.  **Re-enable Legacy Job**:
    *   Re-activate the original KornShell job (`r_ausd_v_ta_barrier.ksh`) in the legacy environment. Ensure its scheduling is restored to its previous state.
3.  **Data Reversion (if necessary)**:
    *   If the `sof_ta_barrier` table in BigQuery was corrupted or populated incorrectly, use BigQuery's time travel feature to revert the table to a state before the problematic run.
    *   Alternatively, if time travel is not sufficient or if the data was critical, restore the `sof_ta_barrier` table from a recent backup.
4.  **Clean Up Migrated Artifacts (Optional)**:
    *   If the rollback is permanent, consider deleting the migrated BigQuery Stored Procedures and the associated logging/control tables from the `isrpt_isbert_data_processing` dataset to avoid confusion and resource consumption.
    *   `DROP PROCEDURE `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`;`
    *   `DROP PROCEDURE `isrpt_isbert_data_processing.k_ausd_v_ta_barrier_control`;`
    *   `DROP PROCEDURE `isrpt_isbert_data_processing.starteSQLSkript`;`
    *   `DROP PROCEDURE `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`;`
    *   `DROP TABLE `isrpt_isbert_data_processing.sof_ta_barrier`;`
    *   `DROP TABLE `isrpt_isbert_data_processing.job_log`;`
    *   `DROP TABLE `isrpt_isbert_data_processing.job_error_log`;`
    *   `DROP TABLE `isrpt_isbert_data_processing.job_table`;`
    *   `DROP TABLE `isrpt_isbert_data_processing.sql_execution_results`;`
5.  **Monitor Legacy Job**:
    *   Closely monitor the re-enabled legacy job to ensure it is functioning correctly and processing data as expected.

This rollback procedure ensures a quick return to the previous stable state while allowing for investigation of the issues encountered with the migrated job.
```