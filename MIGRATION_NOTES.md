# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of an ETL workflow responsible for the reconciliation and aggregation of contract discount data. The original workflow, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh`, consisted of two KornShell scripts orchestrating an Oracle PL/SQL script.

The entire workflow has been migrated to **Google BigQuery**, leveraging BigQuery Stored Procedures for orchestration and data transformation. The target platform utilizes BigQuery for all data storage, processing, and logging.

## 2. Generated Artifacts

The migration produced the following BigQuery DDL and Stored Procedure scripts:

*   **`bigquery/ddl/sof_ta_disc_zusgf.sql`**
    *   **Role**: Defines the schema for the target table `isbert_ds.sof_ta_disc_zusgf` in BigQuery. This table stores the aggregated contract discount data, replacing the original `sof$ta_disc_zusgf` table in Oracle.
*   **`bigquery/ddl/job_control.sql`**
    *   **Role**: Defines the schema for the `isbert_ds.job_control` table. This table is used for managing job status, registration, and deactivation, replacing the job control logic previously handled by KornShell scripts and potentially Oracle job control tables.
*   **`bigquery/ddl/error_log.sql`**
    *   **Role**: Defines the schema for the `isbert_ds.error_log` table. This table captures detailed error messages and stack traces, replacing shell-based error logging.
*   **`bigquery/ddl/job_message_log.sql`**
    *   **Role**: Defines the schema for the `isbert_ds.job_message_log` table. This table stores general informational and warning messages related to job execution, replacing shell-based `DWMSG_*` logging.
*   **`bigquery/ddl/job_result_log.sql`**
    *   **Role**: Defines the schema for the `isbert_ds.job_result_log` table. This table records key metrics like record counts, replacing temporary file-based record count logging.
*   **`bigquery/stored_procedures/d_ausd_v_ta_disc_zusgf_sql_logic.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data transformation logic. It replaces the Oracle PL/SQL script `d_ausd_v_ta_disc_zusgf.sql`, including the functionality of the Oracle object types and the pipelined table function, using BigQuery SQL constructs like `STRING_AGG` and `TRUNCATE TABLE`.
*   **`bigquery/stored_procedures/k_ausd_v_ta_disc_zusgf_controller.sql`**
    *   **Role**: This BigQuery Stored Procedure acts as the controller for the job. It replaces `k_ausd_v_ta_disc_zusgf.ksh`, handling parameter validation, job control table updates (registration, deactivation), and invoking the core SQL logic (`d_ausd_v_ta_disc_zusgf_sql_logic`). It also manages logging of results and errors.
*   **`bigquery/stored_procedures/r_ausd_v_ta_disc_zusgf_wrapper.sql`**
    *   **Role**: This BigQuery Stored Procedure serves as the top-level wrapper for the entire workflow. It replaces `r_ausd_v_ta_disc_zusgf.ksh`, initiating the job, setting up top-level logging, and calling the controller procedure (`k_ausd_v_ta_disc_zusgf_controller`).

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Orchestration via Nested BigQuery Stored Procedures**: The original KornShell wrapper and controller scripts (`r_ausd_v_ta_disc_zusgf.ksh`, `k_ausd_v_ta_disc_zusgf.ksh`) were translated into a hierarchy of BigQuery Stored Procedures (`r_ausd_v_ta_disc_zusgf_wrapper` calling `k_ausd_v_ta_disc_zusgf_controller`). This approach leverages BigQuery's native scripting capabilities, simplifying deployment and management by keeping the entire workflow within BigQuery. This avoids the immediate need for an external orchestration tool like Airflow for this specific job, though Airflow remains an option for broader DAGs.
*   **PL/SQL Pipelined Table Function Replacement**: The complex Oracle PL/SQL pipelined table function (`sof$sp_discount_functions.concat_discounts`) was replaced by BigQuery's `STRING_AGG` aggregate function within a subquery. This is the idiomatic and performant way to achieve string concatenation for grouped data in BigQuery, as BigQuery does not support direct equivalents of Oracle's pipelined functions or custom object types.
*   **Dedicated BigQuery Logging and Job Control Tables**: The shell-based logging (`DWMSG_*`) and job control mechanisms (`h_alis_job.ksh`, temporary files) were replaced by dedicated BigQuery tables (`job_control`, `error_log`, `job_message_log`, `job_result_log`). This centralizes logging, makes job status and history easily queryable, and provides a scalable solution within the BigQuery ecosystem.
*   **BigQuery Native Error Handling**: Oracle's `WHENEVER SQLERROR` and KornShell's `trap` mechanisms were replaced by BigQuery Scripting's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides robust, structured error handling directly within the BigQuery stored procedures.
*   **Removal of Oracle-Specific Features**: Oracle-specific commands and hints such as `PARALLEL`, `ANALYZE TABLE`, `COMMIT`, `SET SERVEROUTPUT`, and `DEFINE` were removed. BigQuery automatically manages parallelism and statistics, and its DML operations are transactional by default, making these explicit commands unnecessary or non-applicable.
*   **Data Type Mapping**: Oracle `NUMBER` types were mapped to BigQuery `INT64` (or `NUMERIC` if precision was critical), and `VARCHAR2` types were mapped to `STRING`. An explicit `LEFT` function was used for `rabatt_alle` to ensure the 500-character limit is respected, addressing potential `VARCHAR2(500)` constraints.
*   **`TRUNCATE TABLE` for Target Population**: The original `TRUNCATE TABLE` followed by `INSERT ... SELECT` pattern was directly translated to BigQuery, ensuring efficient and atomic replacement of target table data.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the `isbert_ds` BigQuery dataset exists in your Google Cloud Project. If not, create it.
2.  **Target Table Creation**:
    *   Execute the DDL scripts for the target and logging tables:
        *   `bigquery/ddl/sof_ta_disc_zusgf.sql`
        *   `bigquery/ddl/job_control.sql`
        *   `bigquery/ddl/error_log.sql`
        *   `bigquery/ddl/job_message_log.sql`
        *   `bigquery/ddl/job_result_log.sql`
    *   These `CREATE TABLE IF NOT EXISTS` statements can be run directly in the BigQuery console or via `bq query`.
3.  **Source Data Ingestion**:
    *   **Crucial**: The source tables `isbert_ds.dwtk_meldungen` and `isbert_ds.sof_ta_discount` must be populated with data from their respective Oracle counterparts. This typically involves a one-time historical load and then ongoing incremental synchronization.
    *   Recommended tools for ingestion include:
        *   Google Cloud Data Transfer Service
        *   Cloud Data Fusion
        *   Database Migration Service (DMS)
        *   Custom ETL processes (e.g., using Dataflow or Spark)
    *   Verify that the schemas and data types of the ingested tables match the expectations of the BigQuery stored procedures.
4.  **IAM Permissions**:
    *   Ensure the Google Cloud service account or user identity that will execute these BigQuery stored procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` on the `isbert_ds` dataset (to create/update/delete tables and run procedures).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   Potentially `BigQuery Data Viewer` on any other datasets if source tables are located elsewhere.
5.  **Scheduling**:
    *   Decide on the scheduling mechanism:
        *   **BigQuery Scheduled Queries**: If the job is simple and self-contained, a scheduled query can directly call `CALL `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`()`.
        *   **Cloud Composer (Airflow)**: For more complex scheduling, dependencies with other jobs, or external triggers, deploy an Airflow DAG that calls the top-level BigQuery stored procedure.
        *   **Cloud Scheduler + Cloud Functions/Run**: For event-driven or HTTP-triggered execution.
6.  **Parameter Configuration**:
    *   The migrated job currently uses default parameters (`v_job_kennung`, `v_eintrags_nr`). If the original KornShell scripts accepted external parameters that need to be dynamic, these would need to be passed to the top-level wrapper procedure or configured within the scheduling mechanism.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or areas requiring further investigation/follow-up:

*   **Exact `STRING` Length Constraints**: While `LEFT(con.rabatt_alle, 500)` was added to `rabatt_alle`, a comprehensive review of all `VARCHAR2` to `STRING` mappings is recommended to ensure no data truncation occurs for other fields if source data exceeds BigQuery's default `STRING` length or specific business requirements.
*   **Oracle Helper Scripts Functionality**: The full functionality of the original Oracle helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) is not fully known. While core logic was translated, any subtle environment setups, custom error codes, or date manipulations might need to be replicated as BigQuery UDFs, additional stored procedure logic, or configuration parameters.
*   **Comprehensive Job Control Logic**: The `h_alis_job.ksh` script's full logic for "ignore active jobs" and "deactivate old jobs" has been translated based on the provided design. However, a thorough review against the original script's exact behavior is recommended to ensure no edge cases in job state management are missed.
*   **Performance Tuning**: The Oracle script used `PARALLEL` hints. While BigQuery automatically handles parallelism, for very large datasets, performance tuning (e.g., clustering, partitioning, optimizing `STRING_AGG` usage) might be required to match or exceed the legacy system's performance.
*   **`BERT_DROP_TEMP_TABLE` Logic**: The exact business logic behind using `isbert_schema.dwtk_meldungen WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'` to determine `v_datum` needs to be confirmed. If this is a placeholder or has a more complex meaning, the BigQuery translation might need adjustment.
*   **DB-Link (`@pcrs1`) Data Source**: The `DEFINE v_carmen = "@pcrs1"` in the original Oracle script suggests a potential DB-link to a CARMEN database. It is assumed that any data sourced via this link (e.g., for `sof$ta_discount` or `dwtk_meldungen`) has been fully migrated or is being ingested into BigQuery. If this is not the case, a separate migration plan for the CARMEN data source is required.

## 6. Validation

To validate the successful migration and execution of the job:

1.  **Execute the Top-Level Stored Procedure**:
    *   Run the main wrapper procedure in BigQuery:
        ```sql
        CALL `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`();
        ```
    *   This can be done via the BigQuery console, `bq` command-line tool, or an orchestrated scheduler.
2.  **Check Job Status**:
    *   Query the `isbert_ds.job_control` table for the latest run of `R_AUSD_V_TA_DISC_ZUSGF` (or the `v_job_kennung` used).
    *   **Passing Criteria**: The `status` column for the latest `eintrags_nr` should be `'SUCCESS'`.
3.  **Review Logs**:
    *   Check `isbert_ds.job_message_log` for informational messages.
    *   **Passing Criteria**: `isbert_ds.error_log` should be empty for the specific `job_kennung` and `eintrags_nr` of the run.
4.  **Verify Target Table Population**:
    *   Query `isbert_ds.sof_ta_disc_zusgf` to ensure data has been inserted.
    *   **Passing Criteria**: The table should contain data, and the `rabatt_alle` column should show correctly concatenated discount strings.
5.  **Validate Record Counts**:
    *   Query `isbert_ds.job_result_log` for the `RECORD_COUNT` metric for the latest run.
    *   **Passing Criteria**: The record count in BigQuery should match the record count in the original Oracle `sof$ta_disc_zusgf` table after a successful run of the legacy job with the same source data.
6.  **Data Quality Check**:
    *   Perform a sample-based data comparison between the BigQuery `isbert_ds.sof_ta_disc_zusgf` table and the original Oracle `sof$ta_disc_zusgf` table. Focus on key fields like `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty`, and especially the aggregated `rabatt_alle` to ensure the transformation logic is identical.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be followed:

1.  **Stop New Executions**:
    *   Immediately disable or pause any scheduled BigQuery queries or Airflow DAGs that trigger `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`.
2.  **Revert Scheduling**:
    *   Reconfigure the scheduler (e.g., cron job, Oracle scheduler) to point back to the original `r_ausd_v_ta_disc_zusgf.ksh` script in the legacy environment.
3.  **Data Restoration (if necessary)**:
    *   If the BigQuery job was run against production-critical data and caused corruption or incorrect data in `isbert_ds.sof_ta_disc_zusgf`, and if this table is consumed by other systems, restore `isbert_ds.sof_ta_disc_zusgf` from a known good backup or re-run the original Oracle job to repopulate the Oracle `sof$ta_disc_zusgf` table.
    *   **Note**: This specific migration only reads from source tables (`dwtk_meldungen`, `sof$ta_discount`) and writes to a new target table (`sof_ta_disc_zusgf`). It does not modify the original Oracle source tables, minimizing the risk of source data corruption.
4.  **Clean Up BigQuery Artifacts (Optional)**:
    *   If the migration is deemed unsuccessful and a full rollback is required, the created BigQuery stored procedures and tables can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`;
        DROP PROCEDURE IF EXISTS `isbert_ds.k_ausd_v_ta_disc_zusgf_controller`;
        DROP PROCEDURE IF EXISTS `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`;
        DROP TABLE IF EXISTS `isbert_ds.sof_ta_disc_zusgf`;
        DROP TABLE IF EXISTS `isbert_ds.job_control`;
        DROP TABLE IF EXISTS `isbert_ds.error_log`;
        DROP TABLE IF EXISTS `isbert_ds.job_message_log`;
        DROP TABLE IF EXISTS `isbert_ds.job_result_log`;
        ```
    *   This step should only be performed if there is no intention to re-attempt the migration in the short term, as it removes all generated artifacts.