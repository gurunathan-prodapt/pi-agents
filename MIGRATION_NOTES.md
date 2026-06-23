# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy ETL job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`. This job, originally designed to synchronize contract data into an Oracle table `ta_p_vertrag` using a multi-layered KornShell and SQL*Plus script, has been re-platformed to Google Cloud Platform (GCP).

The migration involved:
*   **Source:** A KornShell wrapper (`r_ausd_v_ta_p_vertrag.ksh`), a KornShell orchestration script (`k_ausd_v_ta_p_vertrag.ksh`), and a core Oracle SQL*Plus script (`d_ausd_v_ta_p_vertrag.sql`).
*   **Target Platform:** Google Cloud Platform, utilizing **BigQuery** for data storage and transformations, and **Cloud Composer (Apache Airflow)** for workflow orchestration, scheduling, and error handling.

## 2. Generated Artifacts

The migration process generated the following key artifacts:

*   **`bq_d_ausd_v_ta_p_vertrag.sql`**
    *   **Role:** Contains the core data transformation logic, converted from Oracle SQL*Plus to BigQuery Standard SQL. This script performs the `INSERT INTO sof_dwh.ta_p_vertrag ... SELECT ...` operation. It is designed to be executed by an Airflow BigQuery operator.
*   **`dag_ta_p_vertrag_sync.py`**
    *   **Role:** An Apache Airflow Directed Acyclic Graph (DAG) written in Python. This DAG replaces the original KornShell wrapper and orchestration scripts. It defines the sequence of tasks, including fetching parameters, determining `v_datum`, truncating tables, executing the core BigQuery SQL transformation, and truncating temporary tables. It also handles logging and error management within the Airflow framework.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **BigQuery as the Data Warehouse:** BigQuery was chosen for its serverless architecture, scalability, performance, and cost-effectiveness, making it an ideal replacement for the Oracle database for data storage and transformation.
*   **Cloud Composer (Airflow) for Orchestration:** Apache Airflow, managed by Cloud Composer, was selected to replace the KornShell scripts. This provides a robust, Python-native environment for workflow management, scheduling, dependency handling, logging, and error recovery, which is superior to custom shell scripting for complex ETL workflows.
*   **Direct SQL Conversion:** The core Oracle SQL*Plus logic (`d_ausd_v_ta_p_vertrag.sql`) was directly translated into BigQuery Standard SQL. This minimizes changes to the business logic and leverages BigQuery's powerful query engine.
    *   **Oracle Outer Join Syntax:** The Oracle-specific `WHERE v.twin_vertrag_id = pv.vertrag_id_carmen (+);` was converted to a standard `LEFT JOIN` in BigQuery.
    *   **Oracle `PARALLEL` Hint Removal:** BigQuery automatically handles query parallelism, so the `/*+ parallel(...) */` hints were removed.
    *   **SQL*Plus Directives Removal:** Client-side commands like `DEFINE`, `PROMPT`, `SPOOL`, `WHENEVER SQLERROR`, `SET TIMING`, `SET SERVEROUTPUT` were removed as their functionality is handled by Airflow's orchestration and logging mechanisms.
*   **BigQuery Operators for Database Interaction:** Airflow's `BigQueryExecuteQueryOperator` and `BigQueryInsertJobOperator` were used to interact with BigQuery, ensuring native and efficient execution of SQL commands.
*   **Simplified Temporary Table Management:** Oracle's `TRUNCATE TABLE ... DROP STORAGE`/`REUSE STORAGE` (executed via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) was replaced by direct `TRUNCATE TABLE dataset.table_name;` statements in BigQuery. This simplifies the DDL operations, as BigQuery manages storage automatically.
*   **Python for Environment and Parameter Handling:** The environment setup and parameter parsing logic from the KornShell scripts were re-implemented in Python within the Airflow DAG, utilizing DAG parameters and Airflow's XComs for inter-task communication.
*   **Airflow Native Logging and Error Handling:** Custom shell-based logging (`DWMSG_*` functions) and error trapping were replaced by Airflow's built-in logging, which integrates with Cloud Logging, and its robust error handling and retry mechanisms.

**Notable Trade-offs:**
*   **Re-implementation of Custom Utilities:** Custom shell utilities for logging and error handling had to be re-implemented using Airflow's native features, requiring a shift in operational paradigm.
*   **Loss of Direct Shell Control:** The granular control offered by shell scripting is replaced by Airflow's Pythonic task definitions, which can be less direct for simple command execution but offers greater structure and maintainability for complex workflows.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project ID Update:**
    *   Edit `dag_ta_p_vertrag_sync.py` and replace `"your-gcp-project-id"` with the actual GCP Project ID where BigQuery and Cloud Composer are deployed.
2.  **BigQuery Dataset Creation:**
    *   Ensure the following BigQuery datasets exist in your GCP project:
        *   `isbert_dwh`
        *   `sof_dwh`
3.  **Initial Data Migration:**
    *   Migrate all necessary source Oracle tables to BigQuery. This includes:
        *   `isbert_schema.dwtk_meldungen` -> `isbert_dwh.dwtk_meldungen`
        *   `sof$ta_p_vertrag` -> `sof_dwh.ta_p_vertrag` (target table, will be truncated by the DAG)
        *   `sof$ta_vertrag_tmp` -> `sof_dwh.ta_vertrag_tmp`
        *   All other `sof$ta_*` temporary tables mentioned in the `truncate_temp_tables_group` (e.g., `sof_dwh.ta_disc_zusgf`, `sof_dwh.ta_discount`, etc.).
    *   This initial data load can be performed using GCP Data Migration Service (DMS), BigQuery Data Transfer Service, or custom ETL processes.
4.  **IAM Permissions Configuration:**
    *   Ensure the service account associated with your Cloud Composer environment (Airflow worker service account) has the necessary IAM roles to:
        *   Read data from `isbert_dwh` dataset (e.g., `BigQuery Data Viewer`).
        *   Read, write, and truncate tables in `sof_dwh` dataset (e.g., `BigQuery Data Editor`).
        *   Execute BigQuery jobs (e.g., `BigQuery Job User`).
5.  **Airflow Connection Setup:**
    *   Verify that the `google_cloud_default` BigQuery connection is correctly configured in your Airflow environment. This connection is used by the BigQuery operators.
6.  **Airflow DAG Deployment:**
    *   Upload `dag_ta_p_vertrag_sync.py` and `bq_d_ausd_v_ta_p_vertrag.sql` to the DAGs folder of your Cloud Composer environment. The `bq_d_ausd_v_ta_p_vertrag.sql` file should be placed in a subfolder accessible by the DAG (e.g., `dags/sql/`).
7.  **Scheduling Configuration:**
    *   Update the `schedule` parameter in `dag_ta_p_vertrag_sync.py` from `None` to your desired schedule interval (e.g., `"@daily"`, `"0 0 * * *"`, or a custom cron expression).

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and remain as known gaps or require further follow-up:

*   **Implicit Dependencies:** The original job's layered shell script invocation was identified through code review, not explicit lineage tools. This highlights a potential risk for future automation or impact analysis if not thoroughly documented.
*   **`v_carmen` DB-Link Usage:** The actual usage and criticality of the `v_carmen` database link (pointing to a "Carmen DB") are unclear. If it's an active data source, it represents a significant external dependency that requires a dedicated migration strategy (e.g., migrating the Carmen DB to GCP, setting up federated queries, or a separate ingestion pipeline). Currently, it's assumed to be unused or for non-critical metadata.
*   **`isbert_schema.DWPA_UTIL_SKRIPT` Complexity:** While assumed to primarily execute DDL (`TRUNCATE TABLE`), the `runstatement` procedure within `DWPA_UTIL_SKRIPT` might encapsulate more complex logic or validations. This needs verification to ensure that direct `TRUNCATE TABLE` statements in BigQuery are a sufficient and functionally equivalent replacement.
*   **Error Handling Equivalency:** The custom shell-based error handling and messaging system (`DWMSG_*` functions) has been replaced by Airflow's native mechanisms. While robust, a thorough review is needed to ensure equivalent alerting, notification, and operational response capabilities are in place.
*   **Temporary Table Management Lifecycle:** The exact lifecycle and data retention requirements for `sof$ta_vertrag_tmp` and other `sof$ta_*` tables (which are truncated at the end of the job) need to be fully understood. While BigQuery temporary tables or CTEs can substitute, ensuring the current pattern aligns with BigQuery best practices and performance is important.
*   **Date Formats and Time Zones:** Consistent handling of date formats and time zones between the Oracle source and BigQuery target is crucial, especially for `m.timecreated` and the derived `v_datum`. Thorough testing is required to confirm no discrepancies arise.

## 6. Validation

To validate the successful migration and functionality of the `dag_ta_p_vertrag_sync` job:

1.  **Trigger the Airflow DAG:**
    *   Access the Airflow UI in Cloud Composer.
    *   Locate `dag_ta_p_vertrag_sync` and manually trigger a run.
    *   Optionally, provide specific `JobKennung` and `EintragsNr` parameters if needed for testing specific scenarios.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. All tasks (`get_v_datum_task`, `log_v_datum_task`, `truncate_target_table_task`, `main_insert_task`, `truncate_temp_tables_group` and its sub-tasks) should complete successfully.
    *   Check task logs in the Airflow UI (which link to Cloud Logging) for any errors, warnings, or unexpected output.
3.  **Verify Data in BigQuery:**
    *   **Passing Criteria:**
        *   The `sof_dwh.ta_p_vertrag` table in BigQuery should be populated with data.
        *   The data in `sof_dwh.ta_p_vertrag` should be functionally identical to the data produced by the original Oracle job for the same input conditions. This requires a data comparison between the Oracle source and BigQuery target.
        *   All temporary tables listed in `truncate_temp_tables_group` (e.g., `sof_dwh.ta_disc_zusgf`, `sof_dwh.ta_discount`, etc.) should be empty after the DAG run, confirming their truncation.
        *   The `v_datum` value logged by `log_v_datum_task` should match the expected value derived from `isbert_dwh.dwtk_meldungen`.
4.  **Performance Check:**
    *   Compare the execution time of the Airflow DAG run with the historical execution time of the original Oracle job. The BigQuery/Airflow solution is expected to be comparable or faster.
5.  **Resource Utilization:**
    *   Monitor BigQuery slot usage and Cloud Composer resource consumption to ensure they are within acceptable limits.

## 7. Rollback Procedure

In case of critical issues or failure during go-live or subsequent runs, the following rollback procedure can be initiated:

1.  **Disable Airflow DAG:**
    *   Immediately disable the `dag_ta_p_vertrag_sync` DAG in the Airflow UI to prevent further executions.
2.  **Revert to Original Oracle Job:**
    *   Re-enable and resume the execution of the original Oracle ETL job (`r_ausd_v_ta_p_vertrag.ksh`) in the legacy environment.
3.  **Data Restoration (if necessary):**
    *   If the BigQuery target table (`sof_dwh.ta_p_vertrag`) was corrupted or incorrectly populated, restore it from a previous backup or a known good state. BigQuery's time travel feature can be used to query data from a specific point in time if the window allows.
    *   For other tables, assess the impact and restore from backups if necessary.
4.  **Remove Migrated Artifacts (optional):**
    *   If the rollback is deemed permanent or long-term, remove the `dag_ta_p_vertrag_sync.py` and `bq_d_ausd_v_ta_p_vertrag.sql` files from the Cloud Composer DAGs folder.
    *   Consider dropping the BigQuery tables created for the migration if they are no longer needed.
5.  **Root Cause Analysis:**
    *   Perform a thorough root cause analysis of the failure before attempting re-migration or re-deployment.