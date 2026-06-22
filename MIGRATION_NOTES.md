# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh`. The script, responsible for coordinating the initial provisioning of "Basisprodukte" for the BERT system, has been re-platformed from its legacy KornShell environment to a Google Cloud Composer (Apache Airflow) DAG. The target data processing platform for the underlying data operations is Google BigQuery.

## 2. Generated Artifacts

The migration process generated the following primary artifact:

*   **`r_ausd_bp_ta_bpr_bcp_dag.py`**
    *   **Role:** This Python file defines an Apache Airflow DAG (`r_ausd_bp_ta_bpr_bcp_dag`) that orchestrates the data provisioning process. It replaces the original KornShell script's logic for parameter parsing, environment setup, and invoking the core processing.
    *   **Key Components:**
        *   **`_parse_and_validate_params` PythonOperator:** Handles the parsing and validation of `stichtag` (key date) and `wiederanlaufwert` (restart value) parameters, mirroring the `getopts` and validation logic of the original script. It pushes these parameters to Airflow XComs for downstream tasks.
        *   **`run_core_processing` BigQueryOperator:** This task is a placeholder for the actual data extraction and staging logic, which was originally performed by `k_ausd_bp_ta_bpr_bcp.ksh`. It is assumed that `k_ausd_bp_ta_bpr_bcp.ksh` will be migrated to BigQuery SQL. The operator demonstrates how the parameters from the `parse_validate_params` task would be passed to the BigQuery query.

## 3. Key Design Decisions

The migration involved several key design decisions to transition from a shell-based orchestration to a cloud-native Airflow DAG:

*   **Orchestration Re-platforming (KornShell to Airflow DAG):** The core decision was to replace the KornShell script with an Airflow DAG. This leverages Cloud Composer's managed orchestration capabilities, providing improved scheduling, monitoring, logging (to Cloud Logging), error handling, and scalability compared to a custom shell script.
*   **Parameter Handling (Shell `getopts` to Airflow DAG `params` and Python):** The original script's `getopts` logic for command-line parameters (`-s` for `Stichtag`, `-l` for `Wiederanlaufwert`) was translated into Airflow DAG run configuration parameters. A PythonOperator (`_parse_and_validate_params`) was introduced to parse and validate these parameters, including handling default values and date format conversions, ensuring robust input handling within the Airflow ecosystem.
*   **Environment Initialization (`.dw_init` to Airflow Variables/Connections):** Instead of sourcing local shell scripts for environment setup, the Airflow DAG relies on Airflow Variables for configurable paths (e.g., `BERT_DIR_ROOT_AIRFLOW_VAR_NAME`) and Airflow Connections (e.g., `google_cloud_default`) for secure access to GCP resources like BigQuery. This centralizes configuration and credentials management.
*   **Utility Functions (Shell to Python):** Generic shell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are intended to be refactored into reusable Python functions or modules within the Airflow environment. For this specific DAG, date handling was integrated directly into the parameter parsing logic using Python's `datetime` module.
*   **Logging and Error Handling (Shell `trap`/`tee` to Airflow Native):** Airflow's built-in logging to Cloud Logging replaces custom shell `print` and `tee` commands. Error handling is managed by Airflow's task-level retry mechanisms, `on_failure_callback` (if configured), and Python `try-except` blocks, providing a more structured and observable error management system.
*   **Core Processing Invocation (`k_ausd_bp_ta_bpr_bcp.ksh` to BigQueryOperator):** The invocation of the core processing script `k_ausd_bp_ta_bpr_bcp.ksh` is replaced by an Airflow task, specifically a `BigQueryOperator`. This assumes that the logic of `k_ausd_bp_ta_bpr_bcp.ksh` will be fully migrated to BigQuery SQL. This decision aligns with the target BigQuery data platform and allows for native execution of data transformations within BigQuery.

**Notable Trade-offs:**

*   **Increased Development Complexity:** Migrating from a simple shell script to a Python-based Airflow DAG introduces a higher initial development overhead due to the need for Python programming, Airflow concepts, and GCP integrations.
*   **Dependency on `k_ausd_bp_ta_bpr_bcp.ksh` Migration:** The current DAG includes a placeholder for the core processing logic. The full functionality of this migration is contingent on the successful and complete migration of `k_ausd_bp_ta_bpr_bcp.ksh` to a BigQuery-native solution.
*   **Managed Service Overhead:** While Cloud Composer offers significant benefits, it also introduces the operational overhead of managing an Airflow environment, including version upgrades, resource scaling, and monitoring.

## 4. Manual Steps Before Go-Live

Before the migrated DAG can be fully operational in a production environment, the following manual steps are required:

1.  **BigQuery Dataset/Table Creation:**
    *   Ensure that the target BigQuery dataset(s) and table(s) where the `k_ausd_bp_ta_bpr_bcp.ksh` migrated logic will write data are pre-created with the correct schema.
    *   If `k_ausd_bp_ta_bpr_bcp.ksh` reads from specific BigQuery tables, ensure those are also in place and populated.
2.  **IAM/Permissions Configuration:**
    *   Grant the Google Cloud service account associated with your Cloud Composer environment the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to read from source tables and write to target tables.
    *   Ensure the service account has permissions to access any other GCP resources (e.g., Cloud Storage for temporary files) that the BigQuery jobs might utilize.
3.  **Airflow Connections:**
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment, providing the necessary credentials for BigQuery access.
4.  **Airflow Variables:**
    *   Create an Airflow Variable named `BERT_DIR_ROOT_AIRFLOW_VAR_NAME` (or the chosen name) and set its value to the appropriate root directory path if it's still needed for any configuration or logging within the BigQuery logic.
5.  **Migration of `k_ausd_bp_ta_bpr_bcp.ksh`:**
    *   **Crucially, the core processing logic of `k_ausd_bp_ta_bpr_bcp.ksh` must be fully migrated to BigQuery SQL (or PySpark/Dataproc) and tested independently.** The `run_core_processing` task in the generated DAG is currently a placeholder. The actual BigQuery SQL query or Dataproc job definition needs to be integrated into this task.
6.  **Scheduling Configuration:**
    *   Since the DAG is defined with `schedule=None`, it will not run automatically. Determine the desired scheduling frequency and configure an external trigger (e.g., Cloud Scheduler, another Airflow DAG) or plan for manual triggering via the Airflow UI/CLI.
7.  **Review and Update DAG Parameters:**
    *   Review the `params` section in the DAG definition (`r_ausd_bp_ta_bpr_bcp_dag.py`) and adjust default values or descriptions as needed for clarity in the Airflow UI.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent current limitations:

*   **Detailed Logic of `k_ausd_bp_ta_bpr_bcp.ksh`:** The most significant gap is the lack of detailed information regarding the internal logic of `k_ausd_bp_ta_bpr_bcp.ksh`. The generated DAG includes a placeholder `BigQueryOperator` for this component. The complete migration of `r_ausd_bp_ta_bpr_bcp.ksh` is dependent on the full analysis, design, and implementation of `k_ausd_bp_ta_bpr_bcp.ksh`'s logic in BigQuery SQL or an equivalent GCP data processing service.
*   **Placeholder BigQueryOperator:** The `run_core_processing` task in `r_ausd_bp_ta_bpr_bcp_dag.py` contains a dummy `SELECT` statement. This must be replaced with the actual BigQuery SQL (or other operator, e.g., `DataprocSparkSubmitOperator`) that implements the migrated logic of `k_ausd_bp_ta_bpr_bcp.ksh`.
*   **Environment Variable Resolution (`BERT_DIR_ROOT`):** The dynamic resolution of `${BERT_DIR_ROOT}` in the original script needs to be fully understood. While an Airflow Variable placeholder is suggested, its exact usage and necessity within the *migrated* `k_ausd_bp_ta_bpr_bcp.ksh` logic need to be confirmed.
*   **Character Encoding:** The original script's comments indicate potential character encoding considerations (e.g., German umlauts). Ensure that the BigQuery environment and any data loading processes correctly handle UTF-8 or the appropriate character encoding to prevent data corruption.
*   **Error Handling Details:** While Airflow provides robust error handling, specific `on_failure_callback` functions or custom alerting mechanisms might need to be implemented for critical failures, mirroring any specific error notification logic in the original shell script.

## 6. Validation

Validation of the migrated DAG involves ensuring correct parameter handling, successful execution of the placeholder core processing, and proper logging.

**How to Run Tests:**

1.  **Deploy the DAG:** Upload `r_ausd_bp_ta_bpr_bcp_dag.py` to your Cloud Composer environment's DAGs folder.
2.  **Trigger Manually:**
    *   Navigate to the Airflow UI for your Composer environment.
    *   Find the `r_ausd_bp_ta_bpr_bcp_dag` DAG.
    *   Click the "Trigger DAG" button.
    *   In the "Trigger DAG w/ config" dialog:
        *   **Test 1 (Default Parameters):** Leave the "DAG Run Configuration" empty or provide `{"stichtag": null, "wiederanlaufwert": null}` to test default behavior (current date, restart value 0).
        *   **Test 2 (Specific Stichtag - YYYY-MM-DD):** Provide `{"stichtag": "2023-10-26", "wiederanlaufwert": 100}`.
        *   **Test 3 (Specific Stichtag - DDMMYYYY):** Provide `{"stichtag": "26102023", "wiederanlaufwert": 50}`.
        *   **Test 4 (Invalid Stichtag):** Provide `{"stichtag": "2023/10/26"}` and observe the expected `ValueError`.
3.  **Monitor in Airflow UI:** Observe the DAG run in the Airflow UI, checking the status of each task.
4.  **Check Cloud Logging:** Access Cloud Logging for your Composer environment and filter logs by `resource.type="cloud_composer_environment"` and `logName="projects/<YOUR_PROJECT_ID>/logs/airflow-tasks"`. Look for logs from `r_ausd_bp_ta_bpr_bcp_dag` tasks.

**What "Passing" Means:**

*   **Successful DAG Run:** The DAG run completes with a "success" status in the Airflow UI.
*   **Correct Parameter Parsing:**
    *   The `parse_validate_params` task logs should correctly display the `stichtag` (in DDMMYYYY format), `wiederanlaufwert`, `job_kennung`, and `dw_eintragsnr` based on the triggered configuration.
    *   For default triggers, `stichtag` should reflect the current date, and `wiederanlaufwert` should be 0.
*   **BigQueryOperator Execution:** The `run_core_processing` task should execute successfully.
    *   In Cloud Logging, you should see logs indicating the BigQuery job started and completed without errors.
    *   If the `destination_dataset_table` is uncommented and configured, verify that the target table is created/updated as expected (even with placeholder data).
*   **Error Handling:** For invalid parameter inputs (e.g., Test 4), the `parse_validate_params` task should fail with a `ValueError`, and the DAG run should be marked as failed.

## 7. Rollback Procedure

In case of issues with the migrated Airflow DAG, the following rollback procedure can be executed:

1.  **Disable/Delete Airflow DAG:**
    *   In the Airflow UI, set the `r_ausd_bp_ta_bpr_bcp_dag` to "Off" (pause it).
    *   Alternatively, remove the `r_ausd_bp_ta_bpr_bcp_dag.py` file from the Cloud Composer DAGs folder. This will effectively remove the DAG from Airflow.
2.  **Revert to Original Script:**
    *   Ensure the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh` is available and correctly configured in its legacy environment.
    *   Re-enable or restart any legacy scheduling mechanisms (e.g., cron jobs) that were previously responsible for executing the KornShell script.
3.  **Data State Check (if applicable):**
    *   If the migrated DAG performed any data modifications before rollback, assess the state of the target data in BigQuery. Depending on the nature of the issue, a data rollback or cleanup might be necessary.
4.  **Monitor Legacy Execution:**
    *   Verify that the original KornShell script executes successfully and produces the expected output in the legacy environment.