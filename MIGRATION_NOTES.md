# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `k_ausd_bp_ta_msisdn.ksh` and its associated SQL processing logic. The original script served as an orchestration wrapper, handling parameter parsing, environment setup, date validation, and the execution of a core SQL script (`d_ausd_bp_ta_msisdn.sql`) against an Oracle database.

The job has been migrated from a KornShell/Oracle environment to Google Cloud Platform (GCP), specifically utilizing:
*   **Google Cloud Composer (Apache Airflow)** for workflow orchestration, replacing the KornShell script's control flow and parameter handling.
*   **Google BigQuery** for data warehousing and SQL execution, replacing the Oracle database and SQL*Plus execution.
*   **Python** for re-implementing utility functions previously handled by various `.ksh` helper scripts.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/utils.py`**
    *   **Role**: This Python module contains helper functions that re-implement the core logic of several legacy KornShell utility scripts. It includes:
        *   `validate_parameters_func`: Replaces `getopts` and `pruefeParameterGesetzt` for parsing and validating command-line parameters (now Airflow DAG parameters).
        *   `derive_dates_func`: Replaces `gestern.ksh` for calculating today's and yesterday's dates based on a reference date.
        *   `capture_record_count_func`: Replaces the logic of reading `$tmpFile` by directly querying BigQuery for record counts.
        *   `custom_error_handler`: A placeholder for custom error handling logic, replacing `f_alis_msgerr.ksh` if specific custom notifications are required beyond Airflow's native capabilities.
        *   `is_valid_date_format`, `validate_complex_parameter`: Placeholders for more granular date and parameter validation logic from `h_alis_date.ksh` and `h_alis_parameter.ksh`.
    *   **Dependencies**: Uses `datetime`, `logging`, `airflow.exceptions.AirflowException`, and `google.cloud.bigquery`.

*   **`sql/pool_basisprodukt_ddl.sql`**
    *   **Role**: This SQL script provides the Data Definition Language (DDL) for creating the `PoolBasisprodukt` table in Google BigQuery. This table is the target for the data processing logic.
    *   **Note**: This is a placeholder DDL. The actual schema, data types, partitioning, and clustering keys must be fully defined based on the original Oracle `PoolBasisprodukt` table and optimized for BigQuery.

*   **`sql/d_ausd_bp_ta_msisdn_bq.sql`**
    *   **Role**: This SQL script is a placeholder for the BigQuery Standard SQL version of the original `d_ausd_bp_ta_msisdn.sql`. It will contain the core data transformation and loading logic.
    *   **Note**: The content of the original Oracle SQL script is currently unknown and requires full analysis and conversion to BigQuery Standard SQL. The placeholder includes Jinja templating for Airflow parameters (e.g., `{{ ti.xcom_pull(...) }}`).

*   **`dags/dag_k_ausd_bp_ta_msisdn.py`**
    *   **Role**: This is the main Apache Airflow DAG definition file. It orchestrates the entire workflow, replacing the `k_ausd_bp_ta_msisdn.ksh` script's control flow.
    *   **Tasks**:
        *   `validate_parameters` (PythonOperator): Calls `utils.validate_parameters_func`.
        *   `derive_dates` (PythonOperator): Calls `utils.derive_dates_func`.
        *   `execute_bigquery_sql` (BigQueryOperator): Executes the SQL from `sql/d_ausd_bp_ta_msisdn_bq.sql` against BigQuery.
        *   `capture_record_count` (PythonOperator): Calls `utils.capture_record_count_func`.
    *   **Dependencies**: Imports `airflow`, `airflow.operators.python`, `airflow.providers.google.cloud.operators.bigquery`, `datetime`, `os`, `sys`, and the local `utils` module.

## 3. Key Design Decisions

*   **Orchestration Paradigm Shift**: The imperative, sequential execution model of KornShell is replaced by a declarative, task-based workflow managed by Apache Airflow (Cloud Composer). This provides better visibility, retry mechanisms, scheduling, and error handling.
*   **Data Platform Modernization**: The Oracle database and SQL*Plus execution are replaced by Google BigQuery. This leverages BigQuery's scalability, performance for analytical workloads, and managed service benefits.
*   **Python for Utility Logic**: Instead of directly migrating KornShell helper scripts, their functional logic is re-implemented in Python. This aligns with Airflow's native language and promotes a unified development environment.
*   **Parameter Handling via Airflow**: Command-line parameters (`-j`, `-f`, `-s`, `-l`) are now passed via Airflow's `dag_run.conf` for manual triggers or can be configured via Airflow Variables/Macros for scheduled runs. XComs are used to pass validated parameters between tasks.
*   **Elimination of Temporary Files**: The legacy script's reliance on temporary files (`$tmpFile`) for capturing metrics is removed. Record counts are now directly queried from BigQuery tables, simplifying the data flow and reducing I/O overhead.
*   **BigQuery Standard SQL Conversion**: The core data processing logic, originally in Oracle SQL, is converted to BigQuery Standard SQL. This ensures compatibility and leverages BigQuery's specific features and optimizations.
*   **Airflow Native Error Handling**: Airflow's built-in logging, alerting, and retry mechanisms are adopted for operational robustness, replacing custom KornShell error handling. Custom Python callbacks can be integrated for specific notification requirements.

**Notable Trade-offs**:
*   **SQL Conversion Effort**: The conversion from Oracle SQL to BigQuery Standard SQL can be significant, especially if the original SQL uses complex Oracle-specific functions, PL/SQL, or highly optimized constructs. This requires thorough analysis and testing.
*   **Performance Tuning**: While BigQuery is powerful, direct migration of SQL might not yield optimal performance. Re-architecting queries or table structures (e.g., partitioning, clustering) might be necessary to achieve desired SLAs.
*   **Loss of Direct Shell Access**: The flexibility of direct shell commands is replaced by structured Airflow tasks. While generally beneficial, complex shell-specific operations might require more elaborate Python or BashOperator implementations.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment for the migrated job:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset` in the generated code) exists in your GCP project (`your-gcp-project-id`). If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 3600 --default_partition_expiration 3600 your-gcp-project-id:your_bigquery_dataset
        ```
        (Adjust expiration times as needed).

2.  **BigQuery Table Creation (`PoolBasisprodukt`)**:
    *   Execute the DDL script `sql/pool_basisprodukt_ddl.sql` in BigQuery to create the `PoolBasisprodukt` table.
    *   **Crucially, update the placeholder schema in `sql/pool_basisprodukt_ddl.sql`** with the actual column names, data types, partitioning, and clustering from the source Oracle `PoolBasisprodukt` table. Pay close attention to data type mapping between Oracle and BigQuery.
    *   **Identify and replace `YOUR_DATE_COLUMN`** with the actual date column used for partitioning and filtering.

3.  **Data Ingestion for `PoolBasisprodukt`**:
    *   Establish a data pipeline (e.g., using Dataflow, Dataproc, or BigQuery Data Transfer Service) to ingest the historical and ongoing data from the source Oracle `PoolBasisprodukt` table into the newly created BigQuery `PoolBasisprodukt` table. This is a prerequisite for the Airflow DAG to process data.

4.  **IAM / Permissions**:
    *   Ensure the Google Cloud Composer service account (or the service account used by your Airflow environment) has the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery User` + `BigQuery Data Editor` on specific datasets/tables) to read from and write to BigQuery tables.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `Storage Object Viewer` (if SQL files are stored in GCS).

5.  **Airflow Connection (`google_cloud_default`)**:
    *   Verify that the `google_cloud_default` connection is configured correctly in your Airflow environment. This connection is used by the `BigQueryOperator` to authenticate with GCP services. Typically, in Cloud Composer, this is pre-configured to use the Composer environment's service account.

6.  **Update GCP Project ID and Dataset Placeholders**:
    *   Review `dags/utils.py`, `sql/pool_basisprodukt_ddl.sql`, `sql/d_ausd_bp_ta_msisdn_bq.sql`, and `dags/dag_k_ausd_bp_ta_msisdn.py` and **replace all instances of `your-gcp-project-id` and `your_bigquery_dataset`** with your actual GCP project ID and BigQuery dataset ID.
    *   **Update `your_source_dataset.source_table`** in `sql/d_ausd_bp_ta_msisdn_bq.sql` if the SQL references other source tables.

7.  **Scheduling**:
    *   Decide on the appropriate `schedule_interval` for `dag_k_ausd_bp_ta_msisdn.py`. It is currently set to `None` for manual/external triggers. If it needs to run daily, for example, change it to `'0 0 * * *'` (midnight UTC).

## 5. Known Gaps & Unresolved References

*   **`d_ausd_bp_ta_msisdn.sql` Content**: The most critical gap is the actual content of the original `d_ausd_bp_ta_msisdn.sql`. This script needs to be thoroughly analyzed, converted from Oracle SQL to BigQuery Standard SQL, and potentially optimized for BigQuery's architecture. This is a **B4 (Redesign)** item requiring significant effort.
*   **Detailed Utility Script Logic**: While the purpose of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` is understood, their full internal implementation details are not yet fully captured in `dags/utils.py`. Any complex or specific logic within these scripts (e.g., custom error codes, specific date calculations, advanced parameter validation rules) must be identified and accurately re-implemented in Python.
*   **`PoolBasisprodukt` Table Schema**: The exact schema, data types, and volume of the `PoolBasisprodukt` table in Oracle are unknown. The placeholder DDL in `sql/pool_basisprodukt_ddl.sql` must be fully populated and validated against the source system.
*   **`YOUR_DATE_COLUMN`**: The specific date column used for partitioning, clustering, and filtering in `PoolBasisprodukt` and the count query needs to be identified and consistently applied.
*   **Source Table for `d_ausd_bp_ta_msisdn_bq.sql`**: The placeholder SQL in `sql/d_ausd_bp_ta_msisdn_bq.sql` assumes a `your_source_dataset.source_table`. The actual source table(s) and their schemas must be identified and data ingested into BigQuery.
*   **Performance Benchmarking**: The performance characteristics of the original job are unknown. Post-migration, thorough performance testing will be required to ensure the BigQuery solution meets or exceeds legacy SLAs.
*   **Commented-out `sed`, `sort`, `join` commands**: The original `k_ausd_bp_ta_msisdn.ksh` contained commented-out sections involving `sed`, `sort`, and `join`. These were not migrated as they were inactive. If these functionalities are ever required, they would need to be re-implemented in BigQuery SQL or Python/PySpark as a **B4 (Redesign)** item.
*   **Custom Error Handling**: The `custom_error_handler` in `dags/utils.py` is a placeholder. If `f_alis_msgerr.ksh` had specific external notification mechanisms (e.g., sending to a specific monitoring system), this logic needs to be implemented.

## 6. Validation

Validation of the migrated job involves several stages:

1.  **Unit Testing (Python Utilities)**:
    *   **How to run**: Execute Python unit tests for functions in `dags/utils.py` (e.g., `validate_parameters_func`, `derive_dates_func`).
    *   **Passing means**: All test cases pass, covering valid and invalid inputs for parameter validation and date derivation.

2.  **BigQuery SQL Validation**:
    *   **How to run**: Manually execute the converted SQL in `sql/d_ausd_bp_ta_msisdn_bq.sql` directly in the BigQuery console or via `bq query` command-line tool, providing sample parameter values.
    *   **Passing means**: The SQL executes successfully without syntax errors, produces the expected output schema, and performs the correct data transformations.

3.  **Airflow DAG Integration Testing**:
    *   **How to run**:
        *   Deploy `dags/utils.py`, `sql/pool_basisprodukt_ddl.sql`, `sql/d_ausd_bp_ta_msisdn_bq.sql`, and `dags/dag_k_ausd_bp_ta_msisdn.py` to your Cloud Composer environment's DAGs folder.
        *   Trigger the `dag_k_ausd_bp_ta_msisdn` DAG manually from the Airflow UI, providing test parameters via `dag_run.conf` (e.g., `{"j": "TEST_JOB", "f": "20231026", "s": "TEST_SOURCE", "l": "DEBUG"}`).
        *   Monitor task logs in the Airflow UI.
    *   **Passing means**:
        *   All tasks (`validate_parameters`, `derive_dates`, `execute_bigquery_sql`, `capture_record_count`) complete successfully (green status).
        *   Task logs show correct parameter validation, date derivation, and BigQuery query execution.
        *   The `capture_record_count` task reports a non-zero (or expected) record count.

4.  **Functional Data Validation**:
    *   **How to run**:
        *   Run the migrated Airflow DAG for a specific reference date.
        *   Run the legacy `k_ausd_bp_ta_msisdn.ksh` script for the *same* reference date on the legacy system.
        *   Compare the data in the BigQuery `PoolBasisprodukt` table with the data in the original Oracle `PoolBasisprodukt` table (or its equivalent output).
        *   Compare the record counts reported by the Airflow DAG with the record counts from the legacy script.
    *   **Passing means**:
        *   The data in BigQuery is functionally identical to the data produced by the legacy system for the same input.
        *   Record counts match between the new and old systems.
        *   No unexpected data types, nulls, or truncation issues are observed.

5.  **Performance Testing**:
    *   **How to run**: Execute the Airflow DAG with production-like data volumes and compare its execution time against the historical execution times of the legacy script.
    *   **Passing means**: The BigQuery job completes within acceptable timeframes, ideally matching or improving upon the legacy system's performance.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after deploying the migrated job, the following rollback procedure can be initiated:

1.  **Disable New Airflow DAG**:
    *   In the Airflow UI, locate the `dag_k_ausd_bp_ta_msisdn` DAG and toggle its status to "Off" (unpause). This will prevent any further runs of the new job.

2.  **Re-enable Legacy Job**:
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh` script on the legacy system. Ensure it can resume processing from the last successfully processed date.

3.  **Data Reversion (Conditional)**:
    *   **If the BigQuery job performs idempotent operations (e.g., `MERGE` or `INSERT OVERWRITE` for a specific date partition)**: No explicit data reversion might be needed, as subsequent runs of the legacy job would overwrite or correctly update the data.
    *   **If the BigQuery job performs `INSERT` operations that are not easily reversible or would lead to duplicate data**:
        *   Identify the data inserted or modified by the problematic Airflow DAG run(s) in BigQuery.
        *   Execute BigQuery DML statements (e.g., `DELETE` or `UPDATE`) to revert the `PoolBasisprodukt` table (and any other affected tables) to its state before the problematic run. This might involve using `_PARTITIONTIME` or other date columns to target specific data.
        *   **Caution**: This step requires careful planning and execution to avoid data loss or corruption. It's recommended to take a snapshot or backup of the affected BigQuery tables before attempting a rollback.

4.  **Root Cause Analysis**:
    *   Investigate the cause of the failure in the migrated Airflow DAG using Airflow logs, BigQuery job history, and Python stack traces. Address the identified issues before attempting re-deployment.