# MIGRATION_NOTES.md

## 1. Summary

The `k_ausd_v_ta_period.ksh` KornShell script, which serves as a control script for data preparation related to the `ta_period` table, has been migrated. This script's original function was to orchestrate the execution of a SQL script (`d_ausd_v_ta_period.sql`), handle parameters, manage job status (including ignoring active jobs and deactivating old ones), and ensure proper environment setup and error management.

The migration translates this control logic and SQL execution orchestration into a BigQuery-native solution. The target platform is Google Cloud BigQuery, primarily utilizing BigQuery Stored Procedures for the orchestration and BigQuery Tables for data storage and job status management.

## 2. Generated Artifacts

The migration process generated the following BigQuery artifacts:

*   **`project/dataset/job_table.sql`**
    *   **Role**: This DDL script creates the `job_table` in the specified BigQuery dataset. This table is designed to centralize and track the status, metadata, and execution results of jobs, replicating the job-control mechanism found in the legacy environment. It stores critical information such as `job_kennung`, `eintrags_nr`, `script_name`, `status`, `record_count`, and timestamps for each job run.
*   **`project/dataset/r_ausd_vertrag_control.sql`**
    *   **Role**: This SQL script defines the BigQuery Stored Procedure named `r_ausd_vertrag_control`. It encapsulates the entire control flow and business logic of the original `k_ausd_v_ta_period.ksh` script. This includes:
        *   Parameter validation (`p_JobKennung`, `p_EintragsNr`).
        *   Job status management (deactivating old active jobs and inserting new job records into `job_table`).
        *   Execution of the core data manipulation logic, which has been translated from `d_ausd_v_ta_period.sql` and embedded directly within the procedure.
        *   Capturing the number of affected records (`v_records`) and updating the `job_table` accordingly.
        *   Native BigQuery error handling.

## 3. Key Design Decisions

Several key design decisions were made to translate the legacy KornShell script into an efficient and maintainable BigQuery solution:

*   **BigQuery Stored Procedure for Orchestration**: The entire control flow, parameter handling, and job status management from the original KornShell script are encapsulated within a single BigQuery Stored Procedure (`r_ausd_vertrag_control`). This leverages BigQuery's native procedural capabilities, eliminating the need for external scripting environments and simplifying deployment.
*   **Dedicated `job_table` for Status Management**: A new BigQuery table, `job_table`, was designed and implemented to replace the implicit or external job-control mechanism of the legacy system. This centralizes job status tracking, including active flags, execution status, and record counts, directly within BigQuery, providing a clear audit trail.
*   **Embedding Core SQL Logic**: The data manipulation logic from the external `d_ausd_v_ta_period.sql` file has been directly translated into BigQuery SQL and embedded within the `r_ausd_vertrag_control` stored procedure. This simplifies deployment and execution by keeping all related logic within a single BigQuery routine, reducing dependencies on external files.
*   **Native BigQuery Error Handling**: BigQuery's `RAISE` statement is used for error signaling and propagation, replacing the legacy `f_alis_msgerr.ksh` and shell `exit` codes. This integrates error management directly into the BigQuery environment, allowing for structured error messages and easier debugging.
*   **`TRUNCATE` and `INSERT` for `ta_period`**: The core data manipulation logic involves truncating the `project.dataset.ta_period` table and then inserting fresh data based on the source tables and the `v_datum` filtering logic. This approach mirrors the likely behavior of the original script, which would typically refresh the target table entirely.
*   **`@@row_count` for Record Count Capture**: The `@@row_count` system variable is used immediately after the `INSERT` statement to accurately capture the number of records processed. This replaces the legacy temporary file mechanism (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`) and provides a robust, BigQuery-native way to track metrics.
*   **`dwtk_meldungen` for `v_datum` Logic**: The logic to determine `v_datum` (a critical date parameter for filtering data) has been translated to query `project.dataset.dwtk_meldungen` directly within the stored procedure. The `IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')` BigQuery expression accurately replicates the `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` behavior from the legacy system.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists. If it does not, create it in your Google Cloud Project.
2.  **Source Table Availability**: Verify that all source tables referenced in the core SQL logic (`project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_period`, `project.dataset.cds_ta_time_meas_cv`, `project.dataset.cds_ta_description`) exist in the `project.dataset` BigQuery dataset and contain the necessary data.
3.  **Target Table Creation**: Ensure the target table `project.dataset.ta_period` exists with the correct schema. If it does not exist, create it manually or via a DDL script.
4.  **`job_table` Deployment**: Execute the `project/dataset/job_table.sql` DDL script to create the `job_table` in the `project.dataset` BigQuery dataset.
    ```bash
    bq query --use_legacy_sql=false < project/dataset/job_table.sql
    ```
5.  **Stored Procedure Deployment**: Execute the `project/dataset/r_ausd_vertrag_control.sql` script to create or replace the BigQuery Stored Procedure.
    ```bash
    bq query --use_legacy_sql=false < project/dataset/r_ausd_vertrag_control.sql
    ```
6.  **IAM Permissions**:
    *   The service account or user executing the stored procedure must have the following BigQuery IAM roles/permissions:
        *   `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.tables.getData`, `bigquery.tables.truncate` on `project.dataset.ta_period`.
        *   `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.tables.insertData`, `bigquery.tables.getData` on `project.dataset.job_table`.
        *   `bigquery.tables.getData` on `project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_period`, `project.dataset.cds_ta_time_meas_cv`, `project.dataset.cds_ta_description`.
        *   `bigquery.routines.create` and `bigquery.routines.update` to deploy the stored procedure, and `bigquery.routines.execute` to run it.
7.  **Scheduling Configuration**: If the job is to be scheduled, configure a Cloud Scheduler job or a Cloud Composer (Apache Airflow) DAG to invoke the `project.dataset.r_ausd_vertrag_control` stored procedure with the appropriate `p_JobKennung` and `p_EintragsNr` parameters.

## 5. Known Gaps & Unresolved References

*   **Full `d_ausd_v_ta_period.sql` Translation Validation**: While the generated code includes a translated `INSERT` statement, a comprehensive review of the original `d_ausd_v_ta_period.sql` is crucial. Any complex procedural logic, database-specific functions, or intricate data type conversions not explicitly covered by the current translation might require further analysis, BigQuery UDFs, or external processing.
*   **`starteSQLSkript` Hidden Logic**: The exact implementation details of the `starteSQLSkript` wrapper from `h_alis_sqlplus.ksh` are not fully known. The current migration assumes its primary role was SQL execution and basic job status updates. Any other hidden side effects (e.g., specific logging, pre/post-processing steps, or complex error handling beyond simple exit codes) would need to be identified and replicated in BigQuery.
*   **Error Framework Completeness**: The legacy `f_alis_msgerr.ksh` might have had more sophisticated logging, alerting, or error classification capabilities than a simple `RAISE` statement. If these are critical for operational monitoring, they need to be integrated with Google Cloud Logging, Monitoring, and Alerting services.
*   **Unresolved Lineage Edges**: The `lineage_edges` metadata for the original script showed no direct dependencies. While static analysis helped identify some dependencies, a thorough manual review of the original script's ecosystem is recommended to ensure all upstream data sources and downstream consumers of the `ta_period` table are understood and accounted for.
*   **Historization**: The design document noted no explicit historization flags. If the `ta_period` table or related data requires historization (e.g., tracking changes over time), this needs to be explicitly designed and implemented (e.g., using BigQuery's time-travel, partitioning, clustering, or creating snapshot tables).

## 6. Validation

Validation should cover unit testing of the stored procedure and integration testing with any external scheduling mechanisms.

*   **How to Run Tests**:
    *   **Manual Execution**: Invoke the `r_ausd_vertrag_control` stored procedure directly from the BigQuery console or via the `bq query` command-line tool.
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG', 'TEST_ENTRY_NR');
        ```
    *   **Automated Testing**: For more robust testing, consider using BigQuery's scripting capabilities to set up test data, execute the procedure, and assert results, or integrate with a CI/CD pipeline.
*   **"Passing" Criteria**:
    *   **Successful Execution**: The `r_ausd_vertrag_control` stored procedure executes without unhandled errors.
    *   **Parameter Validation**: Invoking the procedure with `NULL` or empty `p_JobKennung` or `p_EintragsNr` should correctly trigger a `RAISE` statement with the expected error message.
    *   **Job Status Management**:
        *   A successful run should create an entry in `project.dataset.job_table` with `status = 'RUNNING'`, which is then updated to `status = 'DONE'`, `active_flag = FALSE`, and the correct `record_count`.
        *   If the procedure is called with the same `p_JobKennung` while a previous entry for that `JobKennung` is still `active_flag = TRUE`, the previous entry should be correctly updated to `status = 'DEACTIVATED'` and `active_flag = FALSE`.
    *   **Core SQL Logic**:
        *   The `project.dataset.ta_period` table should be correctly truncated and then populated with data that matches the expected output based on the translated `INSERT` logic from `d_ausd_v_ta_period.sql`.
        *   The `record_count` stored in `project.dataset.job_table` for the completed job should accurately match the actual number of rows inserted into `project.dataset.ta_period`.
    *   **Error Handling**: If an error is deliberately introduced within the `BEGIN...EXCEPTION` block of the stored procedure, the `job_table` should be updated with `status = 'FAILED'`, `active_flag = FALSE`, and a relevant `error_message`, and the error should be re-raised.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after deployment, follow these steps to roll back the migration:

1.  **Stop Scheduling**: Immediately disable any Cloud Scheduler jobs or Cloud Composer DAGs that are configured to invoke the `project.dataset.r_ausd_vertrag_control` stored procedure.
2.  **Drop Stored Procedure**: Remove the migrated BigQuery Stored Procedure from the target dataset.
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_vertrag_control`;
    ```
3.  **Drop `job_table`**: Remove the newly created `job_table`.
    **Caution**: This action will permanently delete all historical job execution data stored in this table. If this data is required for auditing or analysis, ensure it is backed up or archived before dropping the table.
    ```sql
    DROP TABLE IF EXISTS `project.dataset.job_table`;
    ```
4.  **Restore `ta_period` (if necessary)**: If the `TRUNCATE` and `INSERT` operation performed by the stored procedure on `project.dataset.ta_period` caused data integrity issues or incorrect data, restore the `ta_period` table to its state before the migration.
    *   This can often be done using BigQuery's time-travel feature (if within the time-travel window, typically 7 days):
        ```sql
        CREATE OR REPLACE TABLE `project.dataset.ta_period` AS
        SELECT * FROM `project.dataset.ta_period` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR); -- Adjust the INTERVAL as needed to a point before the problematic execution.
        ```
    *   Alternatively, restore from a previously created snapshot or backup.
5.  **Re-enable Legacy Job**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh` script in the legacy environment to resume operations.