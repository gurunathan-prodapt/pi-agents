# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestrator script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh` has been replatformed to Google Cloud Platform (GCP). The migration targets an Apache Airflow DAG running on Cloud Composer for orchestration, with the core data reconciliation logic (originally within `k_ausd_v_ta_discount_rr.ksh`) translated into BigQuery SQL. Utility functions previously sourced by the KornShell script have been re-implemented in Python.

## 2. Generated artifacts

The migration produced the following files:

*   **`sql/d_ausd_v_ta_discount_rr.sql`**
    *   **Role**: This SQL script contains the core data reconciliation logic for the `sof_ta_discount_rr` table. It performs a `TRUNCATE` and `INSERT` operation, selecting and joining data from various `cds_` source tables based on a provided processing date. This script directly replaces the functionality of the `k_ausd_v_ta_discount_rr.ksh` script's core logic, which was identified as primarily SQL-based.
*   **`utils/dw_utils.py`**
    *   **Role**: This Python module re-implements essential utility functions previously provided by legacy KornShell scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`. It includes functions for error reporting (`dwmsg_meldefehler`), parameter validation (`pruefe_parameter_gesetzt`), and date retrieval (`get_current_dw_date_str`), adapted for use within an Airflow Python environment.
*   **`dags/r_ausd_v_ta_discount_rr_dag.py`**
    *   **Role**: This is the main Apache Airflow DAG definition. It orchestrates the entire workflow, replacing the `r_ausd_v_ta_discount_rr.ksh` wrapper script. It defines tasks for:
        *   Extracting the processing date (`v_datum_str`) from a BigQuery table.
        *   Initializing job parameters and performing validation using `dw_utils.py`.
        *   Executing the core BigQuery SQL logic (`sql/d_ausd_v_ta_discount_rr.sql`) using the extracted processing date.
    *   It manages task dependencies, logging to Cloud Logging, and error handling via Airflow's native mechanisms.

## 3. Key design decisions

*   **Orchestration Replatforming to Airflow on Cloud Composer**: The original KornShell script's role as an orchestrator (environment setup, parameter parsing, logging, error handling, core script invocation) was a natural fit for Airflow. Cloud Composer provides a managed, scalable, and robust environment for Airflow, integrating seamlessly with other GCP services.
*   **Core Logic Migration to BigQuery SQL**: The analysis of the original `k_ausd_v_ta_discount_rr.ksh` (implied by its path and the generated SQL) indicated its primary function was data manipulation via SQL. Migrating this to BigQuery SQL leverages BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities for analytical workloads.
*   **Re-implementation of Utility Functions in Python**: Legacy shell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were re-implemented in Python (`dw_utils.py`). This decision ensures full integration with the Airflow Python environment, eliminating shell dependencies and allowing for standard Python testing and maintenance.
*   **Parameter Handling via Airflow XComs and BigQuery Query Parameters**: Command-line parameters from the original script are replaced by Airflow's native mechanisms. The processing date (`v_datum_str`) is dynamically extracted via a BigQuery query and passed between tasks using XComs, then templated into the `BigQueryExecuteQueryOperator` as a query parameter. This provides a robust and traceable way to manage dynamic inputs.
*   **Centralized Logging and Error Handling**: Airflow's native logging integrates directly with Cloud Logging, centralizing all job logs. Custom error handling logic from the original script is replaced by Python's exception handling and Airflow's `on_failure_callback` mechanisms, allowing for robust monitoring and alerting.
*   **Dynamic Processing Date Determination**: The `v_datum_str` (processing date) is derived from a BigQuery query against the `dwtk_meldungen` table, replicating the original script's logic for determining the relevant date for data processing. This ensures consistency with the legacy system's operational context.
*   **Target Table Strategy**: The `sql/d_ausd_v_ta_discount_rr.sql` script uses a `TRUNCATE` and `INSERT` pattern for the target table `sof_ta_discount_rr`. This aligns with the common pattern for full refreshes of reconciliation or aggregate tables.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation**:
    *   Ensure the target BigQuery dataset (`target_project.target_dataset`) exists.
    *   Ensure the target table `target_project.target_dataset.sof_ta_discount_rr` is created with the correct schema, matching the `INSERT` statement in `sql/d_ausd_v_ta_discount_rr.sql`.
    *   Ensure the source BigQuery dataset (`source_project.source_dataset`) exists and contains all required source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`, `dwtk_meldungen`) with their respective schemas and data.
2.  **GCP IAM Permissions**:
    *   The service account associated with the Cloud Composer environment's Airflow workers must have the following BigQuery permissions:
        *   `BigQuery Data Editor` on `target_project.target_dataset.sof_ta_discount_rr` (for `TRUNCATE` and `INSERT`).
        *   `BigQuery Data Viewer` on `source_project.source_dataset.*` (for `SELECT` from source tables).
        *   `BigQuery Data Viewer` on `source_project.source_dataset.dwtk_meldungen` (for `SELECT` to determine `v_datum_str`).
    *   Ensure the service account also has `Logging Log Writer` permissions for Cloud Logging.
3.  **Airflow Connection Configuration**:
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured in the Cloud Composer environment. This connection is used by the `BigQueryExecuteQueryOperator`.
4.  **Airflow Code Deployment**:
    *   Upload `dags/r_ausd_v_ta_discount_rr_dag.py` to the DAGs folder of the Cloud Composer environment (e.g., `dags/`).
    *   Upload `sql/d_ausd_v_ta_discount_rr.sql` to a location accessible by the Airflow worker, typically within a subfolder of the DAGs folder (e.g., `dags/sql/`).
    *   Upload `utils/dw_utils.py` to a location accessible by the Airflow worker, such as a `utils` subfolder within the DAGs folder (e.g., `dags/utils/`). Ensure this path is correctly configured in the Airflow environment's `PYTHONPATH` or that the import statement in the DAG is adjusted.
5.  **Airflow Variable Configuration (if applicable)**:
    *   If `source_project`, `target_project`, `source_dataset`, or `target_dataset` are intended to be configurable, create Airflow Variables for these values and update the SQL and DAG accordingly to reference them.
6.  **Scheduling Configuration**:
    *   The DAG is currently configured with `schedule_interval=None`. If scheduled execution is required, update the `schedule_interval` parameter in `dags/r_ausd_v_ta_discount_rr_dag.py` to a valid cron expression (e.g., `'0 0 * * *'` for daily at midnight UTC).

## 5. Known gaps & unresolved references

*   **Placeholder Project/Dataset IDs**: The generated SQL and DAG use `source_project`, `source_dataset`, `target_project`, and `target_dataset` as placeholders. These must be replaced with the actual GCP project and BigQuery dataset IDs before deployment.
*   **`dwtk_meldungen` Table Schema**: The `extract_v_datum_from_bigquery` task queries `source_project.source_dataset.dwtk_meldungen`. The existence and schema (specifically `timecreated` and `job_kennung` columns) of this table are assumed. This table's purpose and content should be verified to ensure the `v_datum_str` extraction logic is correct for the new environment.
*   **Full `k_ausd_v_ta_discount_rr.ksh` Scope**: While the generated SQL addresses the core data transformation, it's assumed that `k_ausd_v_ta_discount_rr.ksh` did not contain significant non-SQL logic (e.g., complex file operations, external API calls) beyond what is now covered by the BigQuery SQL. Any such additional logic would represent a gap.
*   **Custom Alerting**: The `default_args` in the DAG include a commented-out `on_failure_callback`. If specific alerting mechanisms (e.g., PagerDuty, Slack, custom email lists) are required upon DAG failure, this callback needs to be implemented and configured.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **DAG Syntax Check**:
    *   Before deployment, run `airflow dags parse <path_to_dag_file.py>` in your local Airflow environment or Composer's gcloud shell to check for syntax errors.
2.  **Unit Tests for `dw_utils.py`**:
    *   Execute unit tests for the `dw_utils.py` module to ensure re-implemented utility functions behave as expected.
3.  **Local Airflow Test Run**:
    *   Use `airflow tasks test r_ausd_v_ta_discount_rr_dag initialize_job_parameters <execution_date>` and `airflow tasks test r_ausd_v_ta_discount_rr_dag set_v_datum_parameter <execution_date>` to test individual Python tasks locally. Note that `BigQueryExecuteQueryOperator` cannot be fully tested locally without a BigQuery connection.
4.  **Cloud Composer Deployment and Manual Trigger**:
    *   Deploy the DAG and associated files to the Cloud Composer environment.
    *   Manually trigger the `r_ausd_v_ta_discount_rr_dag` from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI and check task logs in Cloud Logging for any errors or warnings.
5.  **Data Validation**:
    *   **Pre-migration Baseline**: Record the row count and a checksum (e.g., `FARM_FINGERPRINT(TO_JSON_STRING(t))`) of the `sof_ta_discount_rr` table in the legacy system for a specific processing date.
    *   **Post-migration Comparison**: After a successful DAG run for the *same* processing date, query the `target_project.target_dataset.sof_ta_discount_rr` table in BigQuery.
    *   **"Passing" Criteria**:
        *   The DAG run completes successfully without any failed tasks.
        *   The row count in `target_project.target_dataset.sof_ta_discount_rr` matches the baseline row count from the legacy system.
        *   A checksum comparison (e.g., comparing `FARM_FINGERPRINT(TO_JSON_STRING(t))` of all rows) between the BigQuery table and the legacy output yields identical results.
        *   Spot-check a sample of records for data accuracy and completeness.
6.  **Logging and Monitoring Verification**:
    *   Confirm that all task logs are correctly ingested into Cloud Logging.
    *   Verify that any configured alerts (e.g., for task failures) are triggered as expected during test failures.

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, toggle off the `r_ausd_v_ta_discount_rr_dag` to prevent further scheduled or manual executions.
    *   Alternatively, delete the DAG file from the Cloud Composer DAGs folder.
2.  **Restore BigQuery Target Table (if necessary)**:
    *   If the `target_project.target_dataset.sof_ta_discount_rr` table was corrupted or incorrectly populated by the migrated job, restore it from a previous backup or a BigQuery table snapshot taken before the migration. BigQuery automatically maintains a 7-day history for tables, allowing point-in-time recovery.
3.  **Re-enable Legacy System**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh` script and its associated scheduling mechanism in the legacy environment.
4.  **Verify Legacy Operation**:
    *   Confirm that the legacy job is running correctly and producing expected results.