# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job identified by `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`. The original job, a KornShell script orchestrating an Oracle SQL/PLSQL script, was responsible for preparing and consolidating discount data into the `sof$ta_disc_zusgf` table.

The job has been migrated to Google Cloud Platform, leveraging **BigQuery** for data storage and transformation, and **Cloud Composer (Airflow)** for orchestration. The core logic, which involves reading discount information, processing it (including concatenating discount details), and populating the target table, has been translated from Oracle SQL/PLSQL to BigQuery Standard SQL.

## 2. Generated artifacts

The migration process generated the following files:

*   **`bigquery/ddl/raw_sof/sof_ta_disc_zusgf.sql`**
    *   **Role**: This SQL script defines the Data Definition Language (DDL) for the target BigQuery table `sof$ta_disc_zusgf`. It specifies the table schema (column names and data types) and includes a description. This DDL replaces the implicit table creation or existing schema of the Oracle `sof$ta_disc_zusgf` table.
*   **`bigquery/sql/d_ausd_v_ta_disc_zusgf_transformation.sql`**
    *   **Role**: This SQL script contains the core data transformation logic, translated from the original Oracle `d_ausd_v_ta_disc_zusgf.sql` to BigQuery Standard SQL. It includes a `DECLARE` statement for `v_datum` and an `INSERT` statement with Common Table Expressions (CTEs) to aggregate and join discount data before populating `sof$ta_disc_zusgf`. This script is intended to be executed by an Airflow task.
*   **`dags/k_ausd_v_ta_disc_zusgf_dag.py`**
    *   **Role**: This Python file defines an Apache Airflow Directed Acyclic Graph (DAG). It replaces the orchestration logic of the original KornShell script `k_ausd_v_ta_disc_zusgf.ksh`. The DAG consists of tasks to truncate the target table and then execute the main BigQuery transformation SQL. It also handles parameter passing and integrates with Airflow's native logging and error handling.

## 3. Key design decisions

*   **Platform Choice (BigQuery & Airflow)**: The decision to migrate to BigQuery for data processing and storage, and Cloud Composer (Airflow) for orchestration, aligns with the target cloud architecture for ETL workloads. BigQuery offers scalable, serverless data warehousing, while Airflow provides robust workflow management.
*   **SQL Translation Strategy**:
    *   **Pipelined Functions to CTEs/`STRING_AGG`**: The Oracle PL/SQL pipelined function `concat_discounts` was replaced by BigQuery's `STRING_AGG` function within Common Table Expressions (CTEs). This approach leverages BigQuery's native aggregation capabilities and avoids the need for complex user-defined functions (UDFs) or stored procedures, simplifying the BigQuery SQL.
    *   **Direct SQL Equivalents**: Oracle-specific functions like `NVL` and `TO_CHAR` were translated to their BigQuery Standard SQL equivalents (`COALESCE`, `FORMAT_DATE`). Oracle's implicit outer join syntax `(+)` was converted to explicit `LEFT JOIN` for clarity and standard compliance.
*   **Orchestration Reimplementation**: The KornShell script's responsibilities (environment setup, parameter parsing, SQL execution, error handling) were fully reimplemented in the Airflow DAG.
    *   **Parameter Handling**: Command-line parameters (`-j`, `-f`) from the KornShell script are now handled as Airflow DAG parameters, allowing for flexible triggering and configuration.
    *   **SQL Execution**: The `starteSQLSkript` function's role of executing SQL was replaced by `BigQueryExecuteQueryOperator` tasks in Airflow, directly running BigQuery SQL.
    *   **Truncation Handling**: The `DWPA_UTIL_SKRIPT.runstatement('TRUNCATE TABLE sof$ta_disc_zusgf')` call was replaced by a dedicated `BigQueryExecuteQueryOperator` task in Airflow, ensuring the target table is cleared before insertion.
*   **Data Type Mapping**: Oracle `NUMBER` and `VARCHAR2` types were mapped to BigQuery `INT64` and `STRING` respectively, which are common and appropriate mappings for the data involved.
*   **Hardcoded Project ID**: The generated code includes placeholders (`your_gcp_project_id`) for the GCP Project ID. This is a deliberate decision to make the generated code easily adaptable to different environments by requiring a simple find-and-replace operation.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `isbert_schema` exists.
    *   Ensure the BigQuery dataset `raw_sof` exists.
2.  **Source Table Ingestion**:
    *   The Oracle table `isbert_schema.dwtk_meldungen` must be migrated and continuously ingested into BigQuery as `your_gcp_project_id.isbert_schema.dwtk_meldungen`.
    *   The Oracle table `sof$ta_discount` must be migrated and continuously ingested into BigQuery as `your_gcp_project_id.raw_sof.sof$ta_discount`.
    *   Verify data types and content match the Oracle source.
3.  **Target Table DDL Execution**:
    *   Execute the DDL script `bigquery/ddl/raw_sof/sof_ta_disc_zusgf.sql` in BigQuery to create the target table `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`.
4.  **IAM Permissions**:
    *   The service account used by Cloud Composer (Airflow) must have sufficient permissions to:
        *   Read data from `your_gcp_project_id.isbert_schema.dwtk_meldungen`.
        *   Read data from `your_gcp_project_id.raw_sof.sof$ta_discount`.
        *   Write (INSERT, TRUNCATE) data to `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`.
        *   Execute BigQuery jobs.
5.  **Airflow Connection Configuration**:
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment, pointing to the correct GCP project.
6.  **DAG Deployment**:
    *   Deploy the `dags/k_ausd_v_ta_disc_zusgf_dag.py` file to your Cloud Composer environment's DAGs folder.
7.  **Project ID Replacement**:
    *   **Crucially**, replace all instances of `your_gcp_project_id` in `bigquery/ddl/raw_sof/sof_ta_disc_zusgf.sql`, `bigquery/sql/d_ausd_v_ta_disc_zusgf_transformation.sql`, and `dags/k_ausd_v_ta_disc_zusgf_dag.py` with your actual Google Cloud Project ID.

## 5. Known gaps & unresolved references

*   **Shell Script Utility Functions (B2 Item)**: The exact implementation and dependencies of the original KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, etc.) were not fully detailed in the design. While core functionalities like parameter parsing and SQL execution have been reimplemented in Python/Airflow, any subtle side effects, logging conventions, or specific error handling logic from these utilities might require further manual analysis and adaptation. This was flagged as a `semi_auto` (B2) item, indicating a need for manual review and potential Python reimplementation.
*   **Performance Optimization**: The BigQuery SQL conversion provides a functional equivalent of the Oracle logic. However, BigQuery's columnar storage and execution model might offer further optimization opportunities (e.g., partitioning, clustering, optimized query patterns) beyond a direct translation. Performance testing and tuning will be required post-migration to ensure optimal resource utilization and execution times.
*   **Carmen DB (`@pcrs1`) Dependency**: The original SQL script comments mentioned `@pcrs1` (Carmen DB via DB-Link). While the current SQL logic does not directly use this DB-Link for the main `INSERT`, if `sof$ta_discount` or `isbert_schema.dwtk_meldungen` data ultimately originates from Carmen, then a robust data replication/ingestion mechanism from Carmen to BigQuery (e.g., CDC, batch export/import) must be established and verified. This is an external dependency that needs to be fully understood and addressed if not already covered by existing data pipelines.

## 6. Validation

To validate the successful migration and correct functionality of the new BigQuery/Airflow job:

1.  **Execute the Airflow DAG**:
    *   Trigger the `k_ausd_v_ta_disc_zusgf_dag` in Cloud Composer.
    *   Monitor the DAG run in the Airflow UI to ensure all tasks (`start`, `truncate_target_table`, `execute_main_transformation`) complete successfully without errors.
2.  **Verify BigQuery Job History**:
    *   Check the BigQuery job history for the project to confirm that the `TRUNCATE TABLE` and `INSERT` statements were executed successfully.
3.  **Data Validation**:
    *   **Record Count**: After a successful DAG run, query the `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf` table in BigQuery and compare the number of records with the record count from the legacy Oracle `sof$ta_disc_zusgf` table for the same processing period/data.
    *   **Data Content Comparison**: Perform a detailed comparison of a representative sample of records between the BigQuery `sof$ta_disc_zusgf` table and the Oracle `sof$ta_disc_zusgf` table. This can involve:
        *   Selecting a few `cntrct_id` and `cntrct_obj_version` combinations and comparing all column values.
        *   Using checksums or hash functions on key columns for larger-scale comparison.
        *   Running `MINUS` or `EXCEPT` queries (if possible, by federating or exporting data) to identify discrepancies.
    *   **"Passing" Criteria**: A successful validation means:
        *   The Airflow DAG completes without errors.
        *   The BigQuery jobs complete successfully.
        *   The record counts in the target BigQuery table match the legacy Oracle table (allowing for any expected differences due to data freshness or specific migration cut-off).
        *   A statistically significant sample of data (or all data, if feasible) matches between the source and target, confirming the transformation logic is correct.

## 7. Rollback procedure

In case of issues or critical failures after go-live, the following rollback procedure should be followed:

1.  **Stop New Job Execution**:
    *   Immediately pause or disable the `k_ausd_v_ta_disc_zusgf_dag` in the Airflow UI to prevent further execution of the migrated job.
2.  **Revert to Legacy Job**:
    *   Re-enable and trigger the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh` on the legacy Oracle platform.
3.  **Data Rollback (if necessary)**:
    *   If the `sof$ta_disc_zusgf` table in BigQuery was corrupted or incorrectly populated by the migrated job, and the legacy job needs to write to it (unlikely, as legacy writes to Oracle), then:
        *   Restore `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf` from a previous BigQuery snapshot or backup, if available.
        *   Alternatively, if the legacy job can repopulate the Oracle table, ensure the Oracle table is in a consistent state.
    *   **Note**: Since the BigQuery job truncates the target table before inserting, a rollback primarily involves stopping the new job and resuming the old one. Data in the BigQuery target table will simply reflect the last (potentially erroneous) run of the new job, or be empty if the truncate ran but the insert failed. The Oracle source tables remain unaffected by the BigQuery job.
4.  **Investigation**:
    *   Analyze logs from Airflow and BigQuery to identify the root cause of the failure.
    *   Address the identified issues in the migrated code or configuration.
    *   Re-test thoroughly in a staging environment before attempting another go-live.