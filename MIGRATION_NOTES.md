# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh` and its associated components. The original job, composed of KornShell scripts and Oracle SQL/PLSQL, was responsible for preparing selected basic products for BERT by extracting, processing, and aggregating APN and contract reference data from the Data Warehouse.

The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **BigQuery** for all data storage and SQL-based transformations, replacing the Oracle database.
*   **Cloud Composer (Apache Airflow)** for orchestration, replacing the KornShell scripts and UC4 scheduling.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`ddl/create_isbert_dwtk_meldungen.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `isbert_dwtk_meldungen` table. This table serves as the BigQuery equivalent of the legacy `isbert_schema.dwtk_meldungen` Oracle table, used for determining the processing `snapshot_date`.
*   **`ddl/create_sof_ta_bpr_apn.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_bpr_apn` table. This table is the BigQuery equivalent of the legacy `sof$ta_bpr_apn` Oracle table, serving as the primary source for contract and APN data.
*   **`ddl/create_sof_ta_apn_vertrag.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_apn_vertrag` table. This is the target table in BigQuery, replacing the legacy `sof$ta_apn_vertrag` Oracle table, where the aggregated APN and contract reference data will be stored. It includes partitioning and clustering for optimized performance.
*   **`sql/d_ausd_bp_ta_apn_vertrag.sql`**
    *   **Role:** BigQuery SQL script containing the core data transformation logic. This script replaces the legacy `d_ausd_bp_ta_apn_vertrag.sql` (Oracle SQL/PLSQL). It handles the determination of the snapshot date, truncation (via `DELETE`), and insertion of aggregated APN and contract reference data using BigQuery's `STRING_AGG` function.
*   **`dags/isbert_r_ausd_bp_ta_apn_vertrag_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the entire ETL workflow on GCP. It replaces the legacy KornShell scripts (`r_ausd_bp_ta_apn_vertrag.ksh`, `k_ausd_bp_ta_apn_vertrag.ksh`) and the UC4 scheduler. It defines the sequence of tasks, including the execution of the BigQuery transformation SQL.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Cloud-Native Platform Adoption**: Migrating to GCP (BigQuery, Cloud Composer) provides a scalable, managed, and cost-effective environment, reducing operational overhead associated with on-premise Oracle and KornShell environments.
*   **Set-Based Transformation in BigQuery**: The legacy PL/SQL explicit loop for aggregation was re-engineered into a single, efficient BigQuery SQL query utilizing `STRING_AGG` and `GROUP BY`. This leverages BigQuery's columnar storage and distributed processing capabilities for superior performance and simplified logic compared to row-by-row processing.
*   **Airflow for Orchestration**: Apache Airflow on Cloud Composer was chosen to replace the KornShell scripts and UC4 scheduler. Airflow offers robust scheduling, dependency management, monitoring, and a Python-native environment for parameter handling and custom logic, enhancing maintainability and observability.
*   **Direct BigQuery DML/DDL Execution**: The reliance on `SQL*Plus` and the `isbert_schema.DWPA_UTIL_SKRIPT` utility package for DDL/DML operations was eliminated. Airflow's `BigQueryExecuteQueryOperator` directly executes BigQuery SQL, simplifying the architecture and removing external dependencies.
*   **Integrated Date Determination**: The logic to derive the `snapshot_date` (formerly `v_datum`) from `isbert_dwtk_meldungen` was integrated directly into the main BigQuery transformation SQL using a `DECLARE` statement. This avoids external script calls and ensures atomicity within the BigQuery job.
*   **Enhanced Target Table Design**: The BigQuery target table `sof_ta_apn_vertrag` includes a `snapshot_date` column, enabling historical data tracking. It is also partitioned by `snapshot_date` and clustered by `cntrct_id` to optimize query performance and data management.
*   **Python-based Utility Replacement**: Legacy KornShell utility scripts (`h_alis_parameter.ksh`, `h_alis_date.ksh`, `gestern.ksh`) were either re-implemented as native Python logic within the Airflow DAG or their functionality was absorbed by BigQuery's native functions, streamlining the codebase.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure the target GCP project (`your_gcp_project`) exists.
    *   Create the BigQuery dataset (`your_bigquery_dataset`) within the project, if it doesn't already exist.
2.  **IAM Permissions Configuration**:
    *   Grant the service account used by Cloud Composer (typically `service-<project-number>@cloudcomposer.gserviceaccount.com`) the necessary BigQuery roles:
        *   `BigQuery Data Editor` (to write to `sof_ta_apn_vertrag` and read from source tables).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   Ensure the Composer Worker service account has sufficient permissions to interact with Cloud Logging.
3.  **BigQuery Table Creation**:
    *   Execute the DDL scripts in BigQuery to create the necessary tables:
        *   `ddl/create_isbert_dwtk_meldungen.sql`
        *   `ddl/create_sof_ta_bpr_apn.sql`
        *   `ddl/create_sof_ta_apn_vertrag.sql`
    *   Replace `your_gcp_project` and `your_bigquery_dataset` placeholders with actual values.
4.  **Initial Data Ingestion**:
    *   Perform an initial load of historical data from the legacy Oracle tables (`sof$ta_bpr_apn`, `isbert_schema.dwtk_meldungen`) into their respective BigQuery counterparts (`sof_ta_bpr_apn`, `isbert_dwtk_meldungen`). This can be done using BigQuery Data Transfer Service, `bq load` command, or other ETL tools.
5.  **Airflow Connection Configuration**:
    *   Verify that the `google_cloud_default` connection in Airflow is correctly configured and points to the target GCP project. If not, create or update it.
6.  **Airflow DAG Deployment**:
    *   Upload the `dags/isbert_r_ausd_bp_ta_apn_vertrag_dag.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Airflow DAG Configuration and Activation**:
    *   In the Airflow UI, locate the `isbert_r_ausd_bp_ta_apn_vertrag_dag`.
    *   Review the DAG parameters (e.g., `schedule`, `start_date`).
    *   Enable the DAG.
8.  **Update Placeholders in DAG**:
    *   Before deployment, ensure the `TODO` placeholders in `dags/isbert_r_ausd_bp_ta_apn_vertrag_dag.py` are updated with actual values: `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `GCP_CONN_ID`.

## 5. Known gaps & unresolved references

The following items were identified as known gaps or unresolved references during the migration and require further attention:

*   **Missing `file_complexity` Data**: The complexity tiers for the original KornShell and SQL files were inferred due to the absence of `file_complexity` data. This might lead to underestimation or overestimation of migration effort for similar jobs.
*   **`UNRESOLVED:BERT_LOG.KSH` Reference**: The `lineage_edges` indicated a reference to `UNRESOLVED:BERT_LOG.KSH` from a related XML job. While not directly part of this migration, its purpose and potential impact on the broader BERT ecosystem remain unclear and should be investigated.
*   **Commented-out Post-processing Logic**: The `k_ausd_bp_ta_apn_vertrag.ksh` script contains commented-out `sed`, `sort`, and `join` commands for post-processing `.dat` files. It is unclear if this functionality is dormant, deprecated, or might become active in the future. If these operations are ever required, they would need to be re-implemented using appropriate GCP services (e.g., Dataflow, PySpark on Dataproc, or BigQuery external tables with transformations).
*   **`AL??` Comments**: Several comments prefixed with `AL??` (e.g., `FOSHoleLadedatum`, `FOSJobDeaktivate`) exist in the legacy code, suggesting potential ties to a FOS (Forderungsscoring) job management system. The relevance and necessity of this functionality in the migrated environment are unknown and require clarification from business stakeholders.
*   **`TODO` Placeholders in DAG**: The generated Airflow DAG contains `TODO` comments for `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, and `GCP_CONN_ID`. These must be replaced with actual environment-specific values before deployment.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **Manual Trigger**: In the Airflow UI, manually trigger the `isbert_r_ausd_bp_ta_apn_vertrag_dag`.
2.  **Parameter Input**: If the DAG were to accept parameters (e.g., a specific `snapshot_date`), provide them during the manual trigger. For this DAG, the `snapshot_date` is derived internally.
3.  **Monitor Execution**: Observe the DAG run in the Airflow UI, checking task logs for any errors or warnings.
4.  **BigQuery Data Inspection**: After successful DAG completion, query the `your_gcp_project.your_bigquery_dataset.sof_ta_apn_vertrag` table in BigQuery.
5.  **Legacy System Comparison**: Run the legacy `r_ausd_bp_ta_apn_vertrag.ksh` job for the same `snapshot_date` (or equivalent key date) and compare its output in `sof$ta_apn_vertrag` with the data generated in BigQuery.

**What "passing" means:**

*   **Successful DAG Execution**: The `isbert_r_ausd_bp_ta_apn_vertrag_dag` completes successfully in Airflow without any failed tasks or retries.
*   **Data Accuracy**: The data in the BigQuery target table `your_gcp_project.your_bigquery_dataset.sof_ta_apn_vertrag` for the processed `snapshot_date` is identical to the data produced by the legacy system for the corresponding key date. This includes:
    *   Matching `cntrct_id` values.
    *   Matching `apn_list` values (considering the 100-character truncation and comma-separated format).
    *   Matching `contract_ref_list` values (considering the 100-character truncation and comma-separated format).
    *   The `snapshot_date` column correctly reflects the date derived from `isbert_dwtk_meldungen`.
*   **Performance**: The execution time of the Airflow DAG and the underlying BigQuery job is within acceptable performance thresholds, ideally matching or improving upon the legacy job's runtime.
*   **Logging**: Relevant logs are generated in Cloud Logging for each task, providing sufficient detail for troubleshooting.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure should be followed:

1.  **Immediate Action (Disable New Job)**:
    *   In the Airflow UI, immediately **disable** the `isbert_r_ausd_bp_ta_apn_vertrag_dag` to prevent further execution.
2.  **Revert to Legacy System**:
    *   Re-enable the original UC4 job that triggers `r_ausd_bp_ta_apn_vertrag.ksh` in the legacy environment.
    *   Verify that the legacy job is running as expected and populating the Oracle `sof$ta_apn_vertrag` table correctly.
3.  **Data Remediation (If Necessary)**:
    *   If the BigQuery `sof_ta_apn_vertrag` table was populated with incorrect data, it can be cleared or corrected:
        *   To clear all data for the affected `snapshot_date`:
            ```sql
            DELETE FROM `your_gcp_project.your_bigquery_dataset.sof_ta_apn_vertrag`
            WHERE snapshot_date = '<affected_snapshot_date>';
            ```
        *   Alternatively, if a full restore is needed, the table could be dropped and recreated, followed by a re-ingestion of a known good state (if backups are available).
    *   Ensure the legacy system's target table (`sof$ta_apn_vertrag`) contains the correct and up-to-date information.
4.  **Code Removal (Optional)**:
    *   If the issue is critical and requires significant redesign, remove the `dags/isbert_r_ausd_bp_ta_apn_vertrag_dag.py` file from the Cloud Composer DAGs folder to fully de-provision the migrated job.
5.  **Root Cause Analysis**:
    *   Perform a thorough root cause analysis of the issue that necessitated the rollback. Address the identified problems in the BigQuery SQL, Airflow DAG, or underlying data, and re-test thoroughly before attempting another go-live.