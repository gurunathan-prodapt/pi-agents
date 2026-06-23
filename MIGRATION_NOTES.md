# MIGRATION_NOTES.md: k_ausd_v_ta_notice.ksh

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh`. The original script orchestrated the execution of an underlying SQL script (`d_ausd_v_ta_notice.sql`), handled parameter validation, job tracking, and basic error reporting.

The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **BigQuery** for data storage, processing, and encapsulating the core logic within a Stored Procedure.
*   **Cloud Composer (Apache Airflow)** for scheduling, orchestration, and parameter management.

The migration transforms a shell-based, file-system-dependent process into a cloud-native, scalable, and observable data pipeline.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`sql/ddl/isbert_reporting_tables.sql`**
    *   **Role:** This SQL script defines the Data Definition Language (DDL) for the necessary BigQuery tables. It includes:
        *   `your_project_id.isbert_reporting.ta_notice`: The target table where processed data will be stored.
        *   `your_project_id.isbert_reporting.job_table`: A table for tracking the execution status, parameters, and metrics of each job run.
        *   `your_project_id.isbert_reporting.error_log`: A centralized table for logging detailed error information.
        *   `your_project_id.isbert_reporting.cds_ta_notice`: The assumed source table in BigQuery, corresponding to the original Oracle `cds$ta_notice`.

*   **`sql/stored_procedures/r_ausd_v_ta_notice.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the entire business logic previously spread across `k_ausd_v_ta_notice.ksh` and `d_ausd_v_ta_notice.sql`. It handles:
        *   Parameter validation (`p_JobKennung`, `p_EintragsNr`).
        *   Parsing `p_EintragsNr` into a `DATE` type.
        *   Updating the `job_table` with 'ACTIVE', 'COMPLETED', or 'FAILED' statuses.
        *   Truncating the target `ta_notice` table (`DELETE FROM WHERE TRUE`).
        *   Executing the core data transformation (INSERT SELECT) from `cds_ta_notice` to `ta_notice`.
        *   Calculating and logging the number of processed records.
        *   Robust error handling, logging exceptions to `error_log`.

*   **`dags/k_ausd_v_ta_notice_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG. It is responsible for:
        *   Scheduling the job (currently `None` for manual trigger, but can be configured).
        *   Defining parameters (`job_kennung`, `eintrags_nr`) that can be passed during manual or scheduled runs.
        *   Invoking the `your_project_id.isbert_reporting.r_ausd_v_ta_notice` BigQuery Stored Procedure using the `BigQueryExecuteStoredProcedureOperator`.
        *   Passing the defined parameters from the DAG run to the Stored Procedure.

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedure:** The orchestration logic from the KornShell script and the data processing logic from `d_ausd_v_ta_notice.sql` were combined into a single BigQuery Stored Procedure. This centralizes the logic, leverages BigQuery's native processing capabilities, simplifies error handling within SQL, and eliminates the need for shell scripting.
*   **Cloud Composer (Airflow) for Orchestration:** Airflow was chosen for its robust scheduling, monitoring, and dependency management features, which are standard for GCP data pipelines. It provides a flexible way to trigger the BigQuery Stored Procedure and manage its parameters.
*   **BigQuery for Data Storage and Processing:** BigQuery's serverless, scalable, and cost-effective nature makes it an ideal target for data warehousing and analytical processing. It natively supports Stored Procedures, simplifying the migration of SQL-based logic.
*   **Dedicated Job Tracking and Error Logging Tables:** Instead of temporary files and custom shell logging, dedicated BigQuery tables (`job_table`, `error_log`) were introduced. This provides a structured, queryable, and centralized mechanism for auditing job executions and analyzing errors.
*   **Parameter Handling via Airflow DAG Params:** Command-line arguments (`getopts`) from the original KornShell script are replaced by Airflow DAG parameters. This allows for easy configuration and dynamic input during DAG runs, which are then passed as `IN` parameters to the BigQuery Stored Procedure.
*   **Explicit Date Parsing and Type Casting:** The `p_EintragsNr` parameter, expected as a `YYYYMMDD` string, is explicitly parsed into a `DATE` type (`v_datum_date`) within the BigQuery Stored Procedure. This ensures robust date comparisons and avoids implicit type conversion issues.
*   **Translation of `d_ausd_v_ta_notice.sql` Logic:** The core `INSERT SELECT` logic from `d_ausd_v_ta_notice.sql` was directly translated into BigQuery SQL and embedded within the Stored Procedure. This included:
    *   Replacing `TRUNCATE TABLE sof$ta_notice` with `DELETE FROM your_project_id.isbert_reporting.ta_notice WHERE TRUE;`.
    *   Adapting table names (e.g., `cds$ta_notice` became `cds_ta_notice` for BigQuery compatibility).
    *   Casting `TIMESTAMP` columns from the source (`cds_ta_notice`) to `DATE` for the target (`ta_notice`) as per the target schema.
*   **Standardized Error Handling:** The custom `DWMSG_MeldeFehler` function is replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks, which log detailed error messages to the `error_log` table and re-raise errors to Airflow for proper task failure reporting.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure the target GCP Project (`your_project_id`) exists.
    *   Create the BigQuery Dataset (`isbert_reporting`) if it does not already exist.
        ```bash
        bq mk --location=US your_project_id:isbert_reporting
        ```
        (Adjust location as needed)

2.  **IAM Permissions:**
    *   Ensure the service account used by your Cloud Composer environment (or the user running manual BigQuery commands) has the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on the `isbert_reporting` dataset to create/update tables and run stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `Composer Worker` (if using Composer) for the Airflow worker to interact with GCP services.

3.  **Replace Placeholders:**
    *   In all generated files (`sql/ddl/isbert_reporting_tables.sql`, `sql/stored_procedures/r_ausd_v_ta_notice.sql`, `dags/k_ausd_v_ta_notice_dag.py`), replace `your_project_id` and `your_dataset_id` (or `isbert_reporting`) with your actual GCP project ID and BigQuery dataset ID.

4.  **Deploy BigQuery DDL:**
    *   Execute the `sql/ddl/isbert_reporting_tables.sql` script in BigQuery to create the `ta_notice`, `job_table`, `error_log`, and `cds_ta_notice` tables.
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/isbert_reporting_tables.sql
        ```

5.  **Deploy BigQuery Stored Procedure:**
    *   Execute the `sql/stored_procedures/r_ausd_v_ta_notice.sql` script in BigQuery to create or replace the `r_ausd_v_ta_notice` stored procedure.
        ```bash
        bq query --use_legacy_sql=false < sql/stored_procedures/r_ausd_v_ta_notice.sql
        ```

6.  **Source Data Ingestion:**
    *   Ensure the `your_project_id.isbert_reporting.cds_ta_notice` table is populated with the necessary source data. This table is assumed to be the BigQuery equivalent of the original Oracle `cds$ta_notice`.

7.  **Airflow Connection Setup:**
    *   Verify that the `google_cloud_default` connection is configured correctly in your Airflow environment, pointing to the appropriate GCP project.

8.  **Deploy Airflow DAG:**
    *   Upload the `dags/k_ausd_v_ta_notice_dag.py` file to your Cloud Composer environment's DAGs folder. Airflow will automatically detect and parse the DAG.

9.  **Scheduling Configuration:**
    *   If the DAG needs to run on a schedule, update the `schedule` parameter in `dags/k_ausd_v_ta_notice_dag.py` from `None` to the desired cron expression (e.g., `"@daily"`, `"0 0 * * *"`) and re-upload the DAG.

## 5. Known Gaps & Unresolved References

*   **Comprehensive `d_ausd_v_ta_notice.sql` Analysis:** While the core `INSERT SELECT` logic from `d_ausd_v_ta_notice.sql` has been translated, a deeper analysis of the original Oracle SQL might be required if it contained highly complex, procedural, or Oracle-specific functions not immediately apparent from the provided context. Any such constructs would need further BigQuery-specific re-implementation.
*   **Full Utility Script Functionality:** The original KornShell script sourced several utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While basic error logging and date parsing are covered, any other complex or side-effect-producing logic within these utilities has not been explicitly migrated. If such functionality is critical, it represents a gap.
*   **`job_table` Deactivation of Old Jobs:** The original design document mentioned "deactivating old active jobs." The current BigQuery Stored Procedure only updates the status of the *current* job run. If there's a requirement to periodically clean up or explicitly deactivate `ACTIVE` jobs from previous failed runs that were not properly updated, a separate mechanism (e.g., a scheduled BigQuery query or another Airflow task) would be needed.
*   **`is_production` Field Mapping:** The `is_production` field in `cds_ta_notice` is assumed to be an `INT64` representing a boolean (1 for true, 0 for false). This mapping should be confirmed against the source system's data types and values.
*   **`p_EintragsNr` Interpretation:** The `p_EintragsNr` parameter is interpreted as a `YYYYMMDD` date string. This interpretation should be confirmed with business users to ensure it aligns with the original script's intent.

## 6. Validation

To ensure the migrated job functions correctly, perform the following validation steps:

1.  **Trigger the Airflow DAG:**
    *   Navigate to the Airflow UI for your Cloud Composer environment.
    *   Find the `k_ausd_v_ta_notice_dag` DAG.
    *   Manually trigger a new DAG run. Provide test values for `job_kennung` (e.g., `TEST_RUN_1`) and `eintrags_nr` (e.g., `20231026`).

2.  **Monitor Airflow Task Execution:**
    *   Observe the `call_r_ausd_v_ta_notice_sp` task in the Airflow UI. It should complete successfully (green status).
    *   Check the task logs for any errors or unexpected output.

3.  **Verify BigQuery `job_table` Status:**
    *   Query the `your_project_id.isbert_reporting.job_table` table:
        ```sql
        SELECT * FROM `your_project_id.isbert_reporting.job_table` WHERE job_kennung = 'TEST_RUN_1' ORDER BY created_at DESC LIMIT 1;
        ```
    *   **Passing Criteria:** The `status` column should be `'COMPLETED'`, and `record_count` should reflect the number of rows inserted into `ta_notice`. The `error_message` column should be `NULL`.

4.  **Verify BigQuery `ta_notice` Data:**
    *   Query the `your_project_id.isbert_reporting.ta_notice` table:
        ```sql
        SELECT * FROM `your_project_id.isbert_reporting.ta_notice` WHERE entry_date_of_notice = PARSE_DATE('%Y%m%d', '20231026');
        ```
    *   **Passing Criteria:** The data in `ta_notice` should match the expected output based on the `cds_ta_notice` source data and the logic derived from `d_ausd_v_ta_notice.sql` for the given `eintrags_nr`. A direct comparison with the legacy system's output for the same input date is highly recommended.

5.  **Test Error Scenarios:**
    *   **Invalid `eintrags_nr`:** Trigger the DAG with an invalid date format (e.g., `2023-10-26` or `ABC`).
        *   **Passing Criteria:** The Airflow task should fail. The `job_table` should show `FAILED` status (if the error occurred after the initial `INSERT ACTIVE`). The `error_log` table should contain an entry with `error_number = 2` and `error_message` indicating an invalid date format.
    *   **Missing Parameters:** Attempt to trigger the DAG without providing required parameters (if Airflow's UI allows, or by modifying the DAG to remove defaults).
        *   **Passing Criteria:** The Airflow task should fail. The `error_log` table should contain an entry with `error_number = 1` indicating a missing parameter.

6.  **Data Volume Testing:**
    *   Run the job with a representative volume of source data in `cds_ta_notice` to ensure performance and scalability meet requirements.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Pause/Delete Airflow DAG:**
    *   In the Airflow UI, pause or delete the `k_ausd_v_ta_notice_dag` to prevent further execution of the migrated job.

2.  **Re-enable Legacy Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh` script in the legacy environment.

3.  **Revert BigQuery Stored Procedure (Optional):**
    *   If a previous version of the `r_ausd_v_ta_notice` stored procedure existed and needs to be restored, execute the DDL for that previous version.
    *   Alternatively, if the new SP is deemed problematic, it can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.isbert_reporting.r_ausd_v_ta_notice`;
        ```

4.  **Restore BigQuery `ta_notice` Data (Critical):**
    *   If the `your_project_id.isbert_reporting.ta_notice` table was modified by the migrated job and its data is inconsistent or incorrect, it must be restored.
    *   **Option A (Point-in-Time Restore):** If BigQuery's time travel feature is enabled and within the retention window, restore the table to a state before the problematic run.
        ```sql
        CREATE OR REPLACE TABLE `your_project_id.isbert_reporting.ta_notice` AS
        SELECT * FROM `your_project_id.isbert_reporting.ta_notice` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
        (Adjust interval as needed)
    *   **Option B (Backup/Snapshot Restore):** If backups or snapshots of the `ta_notice` table exist, restore from the most recent valid backup.
    *   **Option C (Re-run Legacy Job):** If the legacy job can regenerate the `ta_notice` data, run the legacy job to populate the table correctly.

5.  **Clean Up `job_table` and `error_log` (Optional):**
    *   Delete records related to the failed migration attempts from `your_project_id.isbert_reporting.job_table` and `your_project_id.isbert_reporting.error_log` if desired for cleanliness, or retain them for post-mortem analysis.

6.  **Verify Legacy System Functionality:**
    *   Confirm that the original job is running as expected and producing correct output after rollback.