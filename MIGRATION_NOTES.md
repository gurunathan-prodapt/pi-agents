# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_acc_ref.ksh` KornShell script and its associated Oracle SQL script (`d_ausd_v_ta_acc_ref.sql`). The original process orchestrated the loading of `ta_acc_ref` data from a remote Oracle database (via a database link) into a local `sof$ta_acc_ref` table, including job state management and error handling.

The entire ETL process has been re-platformed from an Oracle/KornShell environment to Google BigQuery. The orchestration logic is now encapsulated within a BigQuery Stored Procedure, and the data transformation logic is implemented using BigQuery SQL. Source data dependencies (from `dwtk_meldungen` and `cds$ta_acc_ref`) are now fulfilled by dedicated data ingestion pipelines into BigQuery staging tables.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL and Stored Procedure:

*   **`isbert_rpt_staging.sof_ta_acc_ref.sql`**:
    *   **Role**: Defines the schema for the target table `sof_ta_acc_ref` in BigQuery. This table replaces the original Oracle `sof$ta_acc_ref` table and will store the processed `ta_acc_ref` data.
*   **`isbert_rpt_staging.dwtk_meldungen.sql`**:
    *   **Role**: Defines the schema for a staging table `dwtk_meldungen` in BigQuery. This table serves as a replica of the Oracle `isbert_schema.dwtk_meldungen` table, providing the necessary `timecreated` information for determining the processing date.
*   **`isbert_rpt_staging.cds_ta_acc_ref.sql`**:
    *   **Role**: Defines the schema for a staging table `cds_ta_acc_ref` in BigQuery. This table serves as a replica of the Oracle `cds$ta_acc_ref` table (originally accessed via a database link), providing the primary source data for the transformation.
*   **`isbert_rpt_staging.job_control.sql`**:
    *   **Role**: Defines a BigQuery table for managing the state of ETL jobs. This replaces the shell-script based job activation/deactivation logic and provides a centralized, persistent record of job statuses.
*   **`isbert_rpt_staging.job_error_log.sql`**:
    *   **Role**: Defines a BigQuery table for logging detailed error messages and stack traces. This replaces the shell-based error reporting and provides structured error logging for monitoring and debugging.
*   **`isbert_rpt_staging.job_run_log.sql`**:
    *   **Role**: Defines a BigQuery table for logging the execution history of jobs, including start/end times, status, and processed record counts. This replaces the shell-based logging and temporary file outputs.
*   **`isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref.sql`**:
    *   **Role**: This BigQuery Stored Procedure is the core migrated artifact. It encapsulates the entire logic of the original `k_ausd_v_ta_acc_ref.ksh` script and `d_ausd_v_ta_acc_ref.sql` script. It handles parameter parsing, job state management, determining the processing date, truncating the target table, performing the `INSERT INTO ... SELECT FROM` transformation, and logging job outcomes and errors.

## 3. Key design decisions

*   **Orchestration Re-platforming**: The KornShell orchestration logic (`k_ausd_v_ta_acc_ref.ksh`) was fully translated into a BigQuery Stored Procedure (`usp_k_ausd_v_ta_acc_ref`). This centralizes the entire ETL process within BigQuery, leveraging its native scripting capabilities for control flow, parameter handling, and error management, eliminating the need for external shell environments.
*   **Data Transformation within BigQuery**: The Oracle SQL transformation logic (`d_ausd_v_ta_acc_ref.sql`) was directly converted to BigQuery SQL and embedded within the `usp_k_ausd_v_ta_acc_ref` Stored Procedure. This keeps the data processing close to the data, optimizing performance and simplifying the overall architecture.
*   **Elimination of Database Links**: The dependency on Oracle database links (`cds$ta_acc_ref@pcrs1`) was removed. Instead, dedicated data ingestion pipelines (e.g., Datastream, Dataflow) are used to replicate the source Oracle tables (`dwtk_meldungen`, `cds$ta_acc_ref`) into BigQuery staging tables. This decouples the transformation from the source system, improving reliability and scalability.
*   **Centralized Job Control and Logging**: The ad-hoc shell-script based job state management and logging (e.g., `DWMSG_MeldeFehler`, temporary files) were replaced by dedicated BigQuery tables (`job_control`, `job_run_log`, `job_error_log`). This provides a structured, queryable, and persistent mechanism for monitoring job execution, status, and errors.
*   **Handling Oracle-Specific Constructs**: Oracle-specific functions (e.g., `NVL`, `TO_CHAR`, `TO_DATE`) were translated to their BigQuery equivalents (e.g., `COALESCE`, `FORMAT_DATE`, `PARSE_DATE`). The `DWPA_UTIL_SKRIPT.runstatement` call for `TRUNCATE` was replaced with a direct BigQuery `TRUNCATE TABLE` statement, simplifying the DDL execution.
*   **Error Handling**: BigQuery's `EXCEPTION WHEN ERROR` blocks were utilized to robustly capture and log errors, providing more detailed insights (e.g., `@@error.message`, `@@error.stack_trace`) compared to the original shell script's error handling.

## 4. Manual steps before go-live

Before the migrated BigQuery solution can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the target BigQuery dataset: `isbert_rpt_staging`.
    *   `bq mk --dataset <GCP_PROJECT_ID>:isbert_rpt_staging`
2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts for all generated tables within the `isbert_rpt_staging` dataset:
        *   `isbert_rpt_staging.sof_ta_acc_ref.sql`
        *   `isbert_rpt_staging.dwtk_meldungen.sql`
        *   `isbert_rpt_staging.cds_ta_acc_ref.sql`
        *   `isbert_rpt_staging.job_control.sql`
        *   `isbert_rpt_staging.job_error_log.sql`
        *   `isbert_rpt_staging.job_run_log.sql`
    *   These can be run via the BigQuery UI, `bq query`, or a deployment script.
3.  **BigQuery Stored Procedure Deployment**:
    *   Execute the DDL script for `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref.sql` to create the stored procedure.
4.  **Data Ingestion Pipeline Setup**:
    *   **Source Oracle Connection**: Ensure network connectivity and appropriate credentials (e.g., service account with necessary Oracle database permissions) are configured for the chosen ingestion service.
    *   **Ingestion Configuration**: Set up and configure a data ingestion service (e.g., Google Cloud Datastream for CDC, or a scheduled Dataflow/Fivetran job) to continuously or periodically replicate data from:
        *   Oracle `isbert_schema.dwtk_meldungen` to BigQuery `isbert_rpt_staging.dwtk_meldungen`.
        *   Oracle `cds$ta_acc_ref` to BigQuery `isbert_rpt_staging.cds_ta_acc_ref`.
    *   **Initial Load**: Perform an initial full load of both source tables into their respective BigQuery staging tables to ensure data availability before the stored procedure runs.
5.  **IAM and Permissions**:
    *   Grant the service account that will execute the BigQuery Stored Procedure (e.g., Cloud Scheduler, Cloud Composer service account) the following BigQuery roles:
        *   `BigQuery Data Editor` on the `isbert_rpt_staging` dataset (for `INSERT`, `UPDATE`, `TRUNCATE` operations on all tables).
        *   `BigQuery Job User` (to run BigQuery jobs).
    *   Ensure the ingestion service's service account has appropriate permissions to read from Oracle and write to BigQuery.
6.  **Scheduling**:
    *   Configure a scheduler (e.g., Google Cloud Scheduler, Cloud Composer, or a custom Cloud Function/Workflow) to invoke the `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref` stored procedure with the required `p_job_kennung` and `p_eintragsnr` parameters at the desired frequency.
    *   Example for Cloud Scheduler:
        *   Target: BigQuery Data Transfer API (or a Cloud Function wrapper).
        *   Payload: JSON to execute the stored procedure.
7.  **Secrets Management**:
    *   If any sensitive information (e.g., Oracle database credentials for ingestion) is required, ensure it is securely stored and accessed via Google Secret Manager.

## 5. Known gaps & unresolved references

The following items were identified as potential gaps or require further follow-up:

*   **Unknown Complexity Tier**: The original system could not determine the complexity tier for the individual source files. This means the migration effort might have been underestimated, and there could be hidden complexities not fully captured in the design.
*   **Oracle-Specific `DWPA_UTIL_SKRIPT` Functionality**: While `TRUNCATE TABLE` was directly translated, the full scope and criticality of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` (beyond simple DDL execution) needs to be confirmed. If it performs more complex dynamic SQL or logging, those aspects might need further BigQuery implementation.
*   **Data Ingestion Pipeline Robustness**: The successful and timely establishment of robust data ingestion pipelines for `cds$ta_acc_ref` and `dwtk_meldungen` from Oracle to BigQuery is a critical prerequisite. Any issues with this pipeline will directly impact the migrated job.
*   **Oracle SQL Dialect Nuances**: While common constructs were translated, subtle differences in Oracle and BigQuery SQL (e.g., implicit type conversions, specific function behaviors) might require further fine-tuning during testing.
*   **Job Control Table Logic Fidelity**: The original "job table" logic for `p_JobKennung` and `p_EintragsNr` for active job management was inferred. The exact behavior (e.g., how `p_EintragsNr` is used for uniqueness or versioning) should be validated against the original system's behavior to ensure the BigQuery `job_control` table accurately reflects it.
*   **Error Handling Parity**: While BigQuery's error handling is robust, ensuring that the error messages and exit behaviors match the original KornShell/Oracle script for any downstream consumers or monitoring systems might require additional customization.

## 6. Validation

To validate the successful migration and functionality of the BigQuery solution, perform the following steps:

1.  **Prerequisites**:
    *   Ensure the BigQuery dataset, tables, and stored procedure are deployed.
    *   Verify that the data ingestion pipelines for `dwtk_meldungen` and `cds_ta_acc_ref` are active and have populated their respective BigQuery staging tables with up-to-date data.
    *   Confirm that the scheduler (e.g., Cloud Scheduler) is configured to trigger the stored procedure.
2.  **Execute the Stored Procedure**:
    *   Manually trigger the `usp_k_ausd_v_ta_acc_ref` stored procedure with representative `p_job_kennung` and `p_eintragsnr` values.
    *   Example:
        ```sql
        CALL `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`('BERT_TA_ACC_REF_JOB', '1');
        ```
3.  **Verify Job Status and Logs**:
    *   Query the `isbert_rpt_staging.job_run_log` table to confirm a successful run entry, including `start_timestamp`, `end_timestamp`, `status = 'SUCCESS'`, and `processed_records`.
    *   Query the `isbert_rpt_staging.job_control` table to ensure the job's status is updated correctly (e.g., `INACTIVE` after completion).
    *   If an error is expected or simulated, check `isbert_rpt_staging.job_error_log` for detailed error messages.
4.  **Validate Target Data**:
    *   Query `isbert_rpt_staging.sof_ta_acc_ref` to inspect the loaded data.
    *   **Record Count Comparison**: Compare the `processed_records` count from `job_run_log` with the actual `COUNT(*)` in `isbert_rpt_staging.sof_ta_acc_ref`.
    *   **Data Sample Comparison**: Select a sample of records from `isbert_rpt_staging.sof_ta_acc_ref` and compare them against the corresponding data in the original Oracle `sof$ta_acc_ref` table (or directly from `cds$ta_acc_ref` after applying the original logic). Pay close attention to date formats, `NULL` handling, and filtering conditions.
    *   **Filtering Logic**: Specifically verify that the date filtering based on `v_datum` and `is_production = 1` is correctly applied.
5.  **Test Edge Cases**:
    *   Run the procedure with a `p_job_kennung` and `p_eintragsnr` that is already marked `ACTIVE` in `job_control` to ensure the "job already active" skip logic functions correctly.
    *   Simulate an error condition (e.g., by temporarily revoking permissions or introducing a syntax error in a test version of the SP) to verify error logging.

**"Passing" Criteria**:
*   The `usp_k_ausd_v_ta_acc_ref` stored procedure executes without unhandled errors.
*   The `isbert_rpt_staging.sof_ta_acc_ref` table is populated with data.
*   The number of records in `isbert_rpt_staging.sof_ta_acc_ref` matches the expected count from the source system for the given processing date.
*   A sample of records from `isbert_rpt_staging.sof_ta_acc_ref` accurately reflects the transformation logic applied to the source data in `isbert_rpt_staging.cds_ta_acc_ref` and `isbert_rpt_staging.dwtk_meldungen`.
*   The `isbert_rpt_staging.job_run_log` table contains a `SUCCESS` entry for the execution.
*   The `isbert_rpt_staging.job_control` table correctly reflects the job's final status.
*   Error handling and "job already active" skip logic function as designed.

## 7. Rollback procedure

In the event of critical issues or failure to meet validation criteria, the following steps outline the procedure to roll back to the original Oracle/KornShell solution:

1.  **Stop BigQuery Execution**:
    *   Immediately disable or pause the scheduler (e.g., Cloud Scheduler, Cloud Composer DAG) that triggers the `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref` stored procedure.
    *   Ensure no further executions of the BigQuery stored procedure occur.
2.  **Re-enable Original System**:
    *   Re-enable the original `k_ausd_v_ta_acc_ref.ksh` KornShell script in its original scheduling environment.
    *   Verify that the original script can connect to the Oracle database and execute successfully.
3.  **Data Verification (Optional but Recommended)**:
    *   If there is any concern about data integrity in the Oracle `sof$ta_acc_ref` table due to the migration attempt (though unlikely given the `TRUNCATE` and `INSERT` pattern), perform a data integrity check.
    *   If necessary, restore `sof$ta_acc_ref` from the most recent backup taken *before* the migration attempt.
4.  **Monitor Original System**:
    *   Monitor the re-enabled original KornShell job to ensure it is running as expected and producing correct output.
5.  **Post-Rollback Analysis**:
    *   Analyze the `job_error_log` and `job_run_log` tables in BigQuery, along with any Cloud Logging entries, to understand the root cause of the rollback. This information will be crucial for re-planning and re-attempting the migration.
6.  **Clean Up (Optional)**:
    *   Once the original system is stable, the BigQuery tables and stored procedure can be retained for debugging or deleted if no longer needed. The data ingestion pipelines can be paused or deleted.