# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_BCP_MSISDN

## 1. Summary

The `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job, originally orchestrated by UC4 and executed via KornShell scripts with Oracle SQL transformations, has been successfully migrated. Its core function is to prepare "instantiated basic products" (Basisprodukte) by enriching contract and product data with MSISDN (Mobile Station International Subscriber Directory Number) information, storing the result in a staging table for the BERT process.

The job has been re-platformed to Google Cloud Platform (GCP), leveraging **Apache Airflow on Cloud Composer** for orchestration and **Google BigQuery** for all data storage and transformation. This migration eliminates dependencies on legacy UC4, KornShell, and Oracle Database infrastructure.

## 2. Generated Artifacts

The migration process generated the following files, which collectively form the new data pipeline:

*   **`d_ausd_bp_ta_bcp_msisdn_bq.sql`**
    *   **Role**: This file contains the core data transformation logic, directly translated from the original Oracle SQL (`d_ausd_bp_ta_bcp_msisdn.sql`) to BigQuery SQL. It is responsible for truncating the target table (`sof.ta_bcp_msisdn`) and then populating it by joining `sof.ta_bpr_bcp` and `sof.ta_rn_vertrag` tables, enriching the data with MSISDN information. It also includes a BigQuery-compatible declaration for the `v_datum` variable, derived from `isbert_schema.dwtk_meldungen`.

*   **`bert_bcp_msisdn_utils.py`**
    *   **Role**: This Python module encapsulates utility functions that replicate the logic previously handled by the KornShell wrapper (`r_ausd_bp_ta_bcp_msisdn.ksh`) and control (`k_ausd_bp_ta_bcp_msisdn.ksh`) scripts. It includes functions for parameter validation (e.g., date format checks), default value assignment, and logging/status updates. These functions are called by the Airflow DAG to manage job execution flow and context.

*   **`dw_bert_ausd_bp_ta_bcp_msisdn_dag.py`**
    *   **Role**: This is the main Apache Airflow DAG (Directed Acyclic Graph) definition. It orchestrates the entire job on Cloud Composer. It defines the sequence of tasks:
        1.  A `PythonOperator` to initiate the job.
        2.  A `PythonOperator` (`prepare_parameters`) to handle parameter parsing and validation using functions from `bert_bcp_msisdn_utils.py`.
        3.  A `BigQueryExecuteQueryOperator` (`execute_bq_transformation`) to execute the `d_ausd_bp_ta_bcp_msisdn_bq.sql` script in BigQuery.
        4.  A `PythonOperator` (`log_job_status`) to log the job's completion status, also using functions from `bert_bcp_msisdn_utils.py`.
    *   This DAG replaces the UC4 job definition and the overall KornShell orchestration.

## 3. Key Design Decisions

*   **Target Platform Selection (GCP, Airflow, BigQuery)**:
    *   **Decision**: Migrate to a fully managed, scalable, and cloud-native stack on GCP, utilizing Cloud Composer for orchestration and BigQuery for data warehousing and transformation.
    *   **Rationale**: Eliminates technical debt associated with legacy UC4, KornShell, and Oracle. Provides high scalability, reduced operational overhead, and cost-effectiveness inherent to GCP services.

*   **Transformation Language (BigQuery SQL)**:
    *   **Decision**: Translate the core Oracle SQL logic directly into BigQuery SQL.
    *   **Rationale**: BigQuery SQL is highly optimized for large-scale data processing within BigQuery, ensuring performance and leveraging BigQuery's native capabilities. It simplifies the migration path from Oracle SQL compared to introducing an entirely new transformation engine. Oracle-specific hints were removed as BigQuery handles optimization automatically.

*   **Orchestration Logic (Python in Airflow)**:
    *   **Decision**: Re-implement all KornShell script logic (parameter parsing, environment setup, date validation, logging, error handling) using Python within Airflow `PythonOperator` tasks.
    *   **Rationale**: Python is the native language for Airflow DAGs, offering better maintainability, readability, and integration with Airflow's robust scheduling, monitoring, and error-handling features compared to shell scripting.

*   **Parameter Handling and Data Flow between Tasks**:
    *   **Decision**: Utilize Airflow's `PythonOperator` with `provide_context=True` and XComs (cross-communication mechanism) to manage and pass parameters between tasks.
    *   **Rationale**: This allows for dynamic parameter generation, validation, and sharing of execution context (e.g., `stichtag`) across different tasks in the DAG, mimicking the parameter passing in the original KornShell scripts.

*   **Truncate-and-Load Pattern**:
    *   **Decision**: Maintain the original job's pattern of truncating the target table before inserting new data.
    *   **Rationale**: This ensures idempotency and consistency with the legacy process. The BigQuery SQL explicitly includes a `TRUNCATE TABLE` statement, aligning with this pattern.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset and Table Creation**:
    *   Ensure the BigQuery datasets `sof` and `isbert_schema` exist in your GCP project.
    *   Create the target table `sof.ta_bcp_msisdn` in BigQuery with the appropriate schema (matching `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_TEL_MSISDN` data types, e.g., `STRING`, `INTEGER`, `STRING`, `STRING`).
    *   Verify that the source tables `isbert_schema.dwtk_meldungen`, `sof.ta_bpr_bcp`, and `sof.ta_rn_vertrag` have been migrated to BigQuery and are populated with current data.

2.  **IAM Permissions**:
    *   The Google Cloud service account associated with your Cloud Composer environment (Airflow workers) must have the necessary BigQuery permissions:
        *   `BigQuery Data Editor` role on the `sof` dataset (for `sof.ta_bcp_msisdn`).
        *   `BigQuery Data Viewer` role on the `sof` and `isbert_schema` datasets (for source tables).
        *   Permissions to deploy DAGs to the Cloud Composer environment.

3.  **Airflow Connection Configuration**:
    *   Ensure the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment, providing the necessary credentials for BigQuery access.

4.  **Code Deployment**:
    *   Upload the generated files (`d_ausd_bp_ta_bcp_msisdn_bq.sql`, `bert_bcp_msisdn_utils.py`, `dw_bert_ausd_bp_ta_bcp_msisdn_dag.py`) to the DAGs folder of your Cloud Composer environment (typically a GCS bucket). Ensure `bert_bcp_msisdn_utils.py` is accessible by the DAG, either by placing it in the same folder or a designated Python path.

5.  **Scheduling Verification**:
    *   Review the `schedule_interval` in `dw_bert_ausd_bp_ta_bcp_msisdn_dag.py` (`timedelta(days=1)`) and adjust it to precisely match the original UC4 job's schedule.

## 5. Known Gaps & Unresolved References

*   **Comprehensive Utility Script Re-implementation**: The `bert_bcp_msisdn_utils.py` module provides basic equivalents for KornShell utility functions. However, the full content and potential complexities of legacy scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` were not exhaustively analyzed. Any advanced error handling, logging, or specific date calculations from these utilities might require further refinement in the Python code (B2/B3 item).
*   **`wiederanlaufwert` Parameter Usage**: The `wiederanlaufwert` parameter is parsed and passed by the `prepare_parameters_task` but is not directly utilized in the current BigQuery SQL transformation. Its exact purpose and whether it needs to influence the BigQuery logic (e.g., for incremental loads or specific filtering) should be confirmed (B4 item).
*   **`v_carmen` Variable**: The `v_carmen` variable is declared in the BigQuery SQL but is not used. It likely referred to an Oracle connection string or environment variable that is now obsolete in the BigQuery context. It can be removed for cleaner code (B4 item).
*   **`BigQueryExecuteQueryOperator` Configuration**: The `BigQueryExecuteQueryOperator` is configured with `destination_dataset_table='sof.ta_bcp_msisdn'` and `write_disposition='WRITE_TRUNCATE'`. Since the `d_ausd_bp_ta_bcp_msisdn_bq.sql` also explicitly contains `TRUNCATE TABLE`, this results in a redundant truncate operation. While harmless in this specific case, it's not idiomatic. For DML operations, it's generally cleaner to omit `destination_dataset_table` and `write_disposition` from the operator and let the SQL handle the DML entirely (B4 item).
*   **Row Count Logging**: The `log_job_status_task` attempts to log `num_inserted_rows` from the BigQuery task's results. The `BigQueryExecuteQueryOperator` typically returns the BigQuery job ID, not direct row counts for DML. Retrieving precise row counts for `INSERT` statements would require additional logic to query BigQuery job statistics using the BigQuery client library (B4 item).
*   **`v_datum` Derivation Dependency**: The `v_datum` variable's derivation from `isbert_schema.dwtk_meldungen` is critical. The successful migration and continuous population of this metadata table in BigQuery is a prerequisite for correct job execution.

## 6. Validation

To ensure the migrated job functions correctly and produces accurate results:

1.  **Trigger the Airflow DAG**:
    *   Manually trigger the `dw_bert_ausd_bp_ta_bcp_msisdn` DAG from the Airflow UI in Cloud Composer.
    *   Optionally, provide `stichtag` and `wiederanlaufwert` parameters via the "Trigger DAG w/ config" option in the Airflow UI (e.g., `{"stichtag": "20231026"}`).

2.  **Monitor Airflow Logs**:
    *   Verify that all tasks within the DAG complete successfully (turn green) without errors.
    *   Review the logs for each task, especially `prepare_parameters` and `log_job_status`, to ensure parameters are correctly processed and status messages are as expected.
    *   Check the `execute_bq_transformation` task logs for any BigQuery errors or warnings.

3.  **Data Validation in BigQuery**:
    *   **Row Count Comparison**: Compare the number of rows in the target table `sof.ta_bcp_msisdn` in BigQuery with the corresponding table in the legacy Oracle environment after a successful run.
    *   **Data Content Verification**: Sample data from `sof.ta_bcp_msisdn` in BigQuery and compare it against the legacy Oracle output for key columns (`CNTRCT_ID`, `BPR_ID`, `TN_TEL_MSISDN`).
    *   **Schema Validation**: Confirm that the schema of `sof.ta_bcp_msisdn` in BigQuery matches the expected data types and nullability.

4.  **Performance Metrics**:
    *   Review the BigQuery job history for the `execute_bq_transformation` task to check query duration, bytes processed, and slot usage. Ensure performance is comparable to or better than the legacy Oracle execution.

**"Passing" Criteria**:
*   The Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn` completes successfully without any task failures.
*   The data in `sof.ta_bcp_msisdn` in BigQuery is identical in content and row count to the expected output from the legacy Oracle job for the same execution date.
*   The job completes within acceptable performance thresholds.

## 7. Rollback Procedure

In the event of critical issues or data corruption identified after go-live:

1.  **Immediate Action**:
    *   **Disable Airflow DAG**: Pause or delete the `dw_bert_ausd_bp_ta_bcp_msisdn` DAG in the Airflow UI to prevent further execution.
    *   **Re-enable Legacy Job**: Re-activate the original `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job in the UC4 scheduler.

2.  **Data Rollback (if necessary)**:
    *   If the `sof.ta_bcp_msisdn` table in BigQuery was corrupted or incorrectly populated by the migrated job, restore it. This can be done by:
        *   Using BigQuery's time travel feature to restore the table to a previous state before the erroneous run.
        *   Restoring from a BigQuery snapshot or backup if such a strategy is in place.
    *   If the issue originated from source data, address the data quality problem at its source.

3.  **Investigation**:
    *   Analyze Airflow task logs, BigQuery job history, and Cloud Logging for the root cause of the failure or data discrepancy.
    *   Review the `d_ausd_bp_ta_bcp_msisdn_bq.sql` for logical errors, `bert_bcp_msisdn_utils.py` for Python logic issues, and `dw_bert_ausd_bp_ta_bcp_msisdn_dag.py` for orchestration problems.

4.  **Remediation and Re-deployment**:
    *   Once the issue is identified and fixed, update the relevant code (SQL, Python utilities, or DAG definition).
    *   Deploy the corrected code to the Cloud Composer environment.
    *   Perform thorough testing in a non-production environment.
    *   Once validated, re-enable the `dw_bert_ausd_bp_ta_bcp_msisdn` DAG in Airflow and disable the legacy UC4 job.