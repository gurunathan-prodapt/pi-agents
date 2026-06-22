```markdown
# MIGRATION_NOTES.md: k_ausd_v_ta_inv_acc.ksh

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh`, along with its invoked SQL script `d_ausd_v_ta_inv_acc.sql`, has been migrated. The original script's orchestration logic, parameter handling, and error reporting have been re-platformed to Google Cloud Composer (Apache Airflow). The data processing logic contained within `d_ausd_v_ta_inv_acc.sql` has been translated to Google BigQuery SQL.

The migration targets a serverless, scalable, and managed environment on Google Cloud Platform, leveraging Airflow for workflow orchestration and BigQuery for high-performance data warehousing and processing.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/d_ausd_v_ta_inv_acc.sql`**
    *   **Role**: This file contains the BigQuery SQL code for the core data transformation. It performs an `INSERT` operation into the `sof_ta_inv_acc` table, joining data from `sof_ta_inv_assign`, `sof_ta_inv_def`, and `sof_ta_acc_ref`. This SQL replaces the original Oracle/SQL*Plus script.
*   **`dags/utils/error_handling.py`**
    *   **Role**: A Python module that re-implements the error logging functionality previously provided by `f_alis_msgerr.ksh`. It includes a `log_error` function to log messages with different severity levels (Critical, Error, Warning) to Airflow's logging system.
*   **`dags/utils/parameter_handling.py`**
    *   **Role**: A Python module that re-implements the parameter validation logic from `h_alis_parameter.ksh`. It provides a `validate_parameter` function to check if a given parameter value is set and not empty.
*   **`dags/k_ausd_v_ta_inv_acc_dag.py`**
    *   **Role**: This is the main Airflow DAG definition file. It orchestrates the entire job, replacing the `k_ausd_v_ta_inv_acc.ksh` script. It includes tasks for:
        *   Starting and ending the job.
        *   Parsing and validating input parameters (`p_job_kennung`, `p_eintrags_nr`) from the DAG run configuration.
        *   Truncating the target BigQuery table (`sof_ta_inv_acc`).
        *   Executing the BigQuery SQL for data insertion (`d_ausd_v_ta_inv_acc.sql`).
        *   Logging a placeholder for the record count.

## 3. Key Design Decisions

*   **Orchestration Platform (Cloud Composer/Airflow)**: Chosen for its managed service, Python-native environment, and robust capabilities for scheduling, monitoring, and error handling, directly replacing the KornShell script's control flow.
*   **Data Processing Platform (BigQuery)**: Selected for its scalability, performance, and serverless architecture, replacing the legacy Oracle/SQL*Plus execution environment. This allows for efficient processing of large datasets.
*   **Utility Re-implementation in Python**: Legacy KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) were re-implemented as modular Python functions within `dags/utils/` modules. This promotes reusability within the Airflow ecosystem and aligns with Python-native development practices.
*   **Parameter Handling via Airflow DAG Run Configuration**: Command-line arguments (`-j`, `-f`) from the original script are now passed as parameters in the Airflow DAG run configuration (`p_job_kennung`, `p_eintrags_nr`). This integrates seamlessly with Airflow's UI and API for triggering and managing DAG runs.
*   **TRUNCATE/INSERT Pattern**: The migration assumes a full refresh strategy, where the target table `sof_ta_inv_acc` is truncated before new data is inserted. This is implemented as separate `BigQueryExecuteQueryOperator` tasks for clarity and control.
*   **Record Count Handling**: The original script used a temporary file (`$tmpFile`) to store record counts. In the migrated DAG, a placeholder log message is used for the record count. This is a pragmatic trade-off for initial migration; a more robust solution (e.g., parsing BigQuery job statistics or adding a `COUNT(*)` task) is noted as a future enhancement.
*   **SQL Script Integration**: The BigQuery SQL for data transformation is directly embedded within the `BigQueryExecuteQueryOperator` in the DAG. While this simplifies deployment, it means the SQL is tightly coupled with the DAG. For more complex scenarios, storing SQL in Google Cloud Storage and referencing it might be considered.
*   **Hardcoded Project/Dataset IDs**: `BQ_PROJECT_ID` and `BQ_DATASET_ID` are currently hardcoded as placeholders (`gcp-project-id`, `isrpt_isbert_prod`). This was done for generation simplicity but should be parameterized (e.g., using Airflow Variables or environment variables) in a production environment for flexibility and maintainability.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset and Table Creation**:
    *   Ensure the BigQuery dataset `isrpt_isbert_prod` exists in your GCP project (`gcp-project-id`).
    *   Create the source tables: `sof_ta_inv_assign`, `sof_ta_inv_def`, and `sof_ta_acc_ref` within the `isrpt_isbert_prod` dataset. Their schemas must accurately reflect the original source database tables.
    *   Create the target table: `sof_ta_inv_acc` within the `isrpt_isbert_prod` dataset, with a schema matching the columns inserted by `d_ausd_v_ta_inv_acc.sql`.
    *   **Action**: Replace `gcp-project-id` placeholder with your actual GCP Project ID in the DAG file (`k_ausd_v_ta_inv_acc_dag.py`) and the SQL file (`d_ausd_v_ta_inv_acc.sql`).
2.  **IAM Permissions**:
    *   The Google Cloud Composer service account (typically `service-<project-number>@cloudcomposer.gserviceaccount.com`) must have the necessary IAM roles.
    *   Grant `BigQuery Data Editor` role on the `gcp-project-id.isrpt_isbert_prod` dataset to allow `TRUNCATE` and `INSERT` operations.
    *   Ensure the service account has permissions to write logs to Cloud Logging.
3.  **Airflow Connections**:
    *   The DAG uses `gcp_conn_id="google_cloud_default"`. Verify that this connection is correctly configured in your Airflow environment and that it uses the appropriate service account for BigQuery access.
4.  **Deployment to Cloud Composer**:
    *   Upload the generated Python files (`dags/utils/error_handling.py`, `dags/utils/parameter_handling.py`, `dags/k_ausd_v_ta_inv_acc_dag.py`) to the DAGs folder of your Cloud Composer environment. Ensure the `utils` directory structure is maintained within the DAGs folder.
5.  **Scheduling Configuration**:
    *   The DAG is currently configured with `schedule=None`, meaning it will only run manually or via external triggers. If a recurring schedule is required, update the `schedule` parameter in `k_ausd_v_ta_inv_acc_dag.py` (e.g., `schedule="@daily"`).

## 5. Known Gaps & Unresolved References

*   **Missing Lineage Details**: The original `lineage_edges` query returned no rows, indicating that dependencies were inferred from script content. This carries a risk of overlooking subtle interactions or data flows that were not explicitly captured.
*   **SQL Script Optimization**: The `d_ausd_v_ta_inv_acc.sql` was directly translated to BigQuery SQL. Further analysis and optimization for BigQuery-specific features (e.g., partitioning, clustering, specific functions) could yield performance benefits and should be considered as a follow-up.
*   **Utility Script Coverage**: While `f_alis_msgerr.ksh` and `h_alis_parameter.ksh` were re-implemented, other sourced utilities (`.dw_init`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) were not explicitly re-platformed as separate Python modules. Their functionality is either implicitly handled by Airflow or not directly required by this specific DAG. A comprehensive review of all common utilities for broader reusability is recommended.
*   **Record Count Accuracy (B4 Item)**: The current `_log_record_count` task provides a placeholder message as the `BigQueryExecuteQueryOperator` for `INSERT` does not directly return the number of affected rows to XCom. A robust solution for capturing and logging the exact number of processed records (e.g., parsing BigQuery job statistics or adding a subsequent `COUNT(*)` task) is a B4 item for future development.
*   **Security & Monitoring Gaps**: The original script had noted gaps in authentication, authorization, credential management, input sanitization, SQL injection protection, structured logging, audit logging, and metrics. While Airflow and GCP provide frameworks for these, explicit configuration and implementation are required:
    *   **Input Sanitization**: Ensure robust validation for all DAG parameters to prevent unexpected behavior.
    *   **Structured Logging/Metrics**: Configure Cloud Logging and Cloud Monitoring for comprehensive observability.
*   **Error Code Mapping**: The original script used specific error codes (e.g., 192, 193). While the Python `log_error` function maps these to Airflow log levels and task failures, a direct 1:1 mapping of all original error codes and their specific meanings might require further refinement for complete parity.
*   **`semi_auto` Bucket**: The `semi_auto` classification indicates that manual effort was required during migration, and further manual verification and adjustments are likely needed post-migration.

## 6. Validation

To ensure the migrated job functions correctly, perform the following validation steps:

*   **Unit Tests**:
    *   Run unit tests for `dags/utils/error_handling.py` and `dags/utils/parameter_handling.py` using a Python testing framework (e.g., `pytest` or `unittest`) to verify individual utility functions.
*   **BigQuery SQL Validation**:
    *   Execute the BigQuery SQL from `sql/d_ausd_v_ta_inv_acc.sql` directly in the BigQuery console or via the `bq query` command using representative sample data. Verify that the query runs successfully and produces the expected output.
*   **Airflow DAG Local Test**:
    *   Use the Airflow CLI to test the DAG locally:
        ```bash
        airflow dags test k_ausd_v_ta_inv_acc_dag <execution_date> -c '{"p_job_kennung": "TEST_JOB", "p_eintrags_nr": "123"}'
        ```
    *   Verify that all tasks execute without errors and that parameter validation works as expected.
*   **Airflow DAG Deployment & Execution**:
    *   Deploy the DAG to the Cloud Composer environment.
    *   Trigger the DAG manually via the Airflow UI, providing valid values for `p_job_kennung` and `p_eintrags_nr` in the DAG Run configuration.
    *   Trigger another run with missing or empty parameters to confirm the validation task fails as expected.

**"Passing" Criteria**:

*   All tasks within the `k_ausd_v_ta_inv_acc_dag` complete successfully (indicated by a green status in the Airflow UI).
*   No errors or critical warnings are observed in the Airflow task logs.
*   The target BigQuery table `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` is truncated and populated with the expected data.
*   The data in `sof_ta_inv_acc` matches the output generated by the original legacy job, both in terms of content and record count (requires manual comparison or a separate data validation process).
*   The parameter validation task correctly identifies missing or empty required parameters and causes the DAG to fail with an appropriate error message.

## 7. Rollback Procedure

In case of issues with the migrated job, follow these steps to roll back to the original legacy system:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, deactivate the `k_ausd_v_ta_inv_acc_dag` to prevent further executions.
    *   (Optional) Remove the DAG file (`k_ausd_v_ta_inv_acc_dag.py`) and its associated utility Python files (`error_handling.py`, `parameter_handling.py`) from the Cloud Composer DAGs folder.
2.  **BigQuery Data Restoration**:
    *   If the data in `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` was corrupted or incorrectly processed by the migrated job, restore the table to its state before the problematic run. This can be done using BigQuery's point-in-time recovery features or by restoring from a previous table snapshot/backup, if available.
3.  **Re-enable Original Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh` script in the legacy environment.
    *   Verify that the legacy environment and all its dependencies (e.g., Oracle database, required utility scripts) are fully operational and can resume processing.
```