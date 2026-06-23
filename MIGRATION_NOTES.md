# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh`. This script, originally an orchestration wrapper for the initial provision of "Basisprodukte" for the BERT system, has been migrated from a legacy Unix/KornShell environment to Google Cloud Platform (GCP).

The target platform for orchestration is **Cloud Composer (managed Airflow)**. The script's core orchestration logic, parameter handling, and logging mechanisms have been re-implemented as an Airflow Directed Acyclic Graph (DAG) in Python. The actual data processing logic, originally encapsulated within the invoked `k_ausd_bp_ta_bpr_bcp.ksh` script, is treated as a separate migration unit and is represented by a placeholder task within this DAG.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`airflow_utils.py`**
    *   **Role**: This Python module serves as a re-implementation of key utility functions originally found in the legacy KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). It provides Python equivalents for date calculation (`DWDate_Gib_Zeitraum`), parameter validation (`pruefeParameterGesetzt`), and custom logging/error handling (`DWMSG_*` functions). These utilities are designed to be imported and used by Airflow DAGs to maintain functional parity with the original script's custom framework.
*   **`r_ausd_bp_ta_bpr_bcp_dag.py`**
    *   **Role**: This is the main Airflow DAG definition file. It orchestrates the entire process, replacing the original KornShell wrapper script.
        *   It defines the sequence of tasks: `initialize_job_context`, `validate_parameters`, `invoke_core_logic`, and `log_success`.
        *   It handles parameter parsing (`stichtag`, `wiederanlaufwert`) using Airflow's `params` and XComs for inter-task communication.
        *   It integrates the custom utility functions from `airflow_utils.py` for logging, error handling, and parameter validation.
        *   It includes a placeholder `BashOperator` (`invoke_core_logic_task`) to represent the future execution of the migrated core business logic (from `k_ausd_bp_ta_bpr_bcp.ksh`), which is expected to be migrated to a GCP-native service like BigQuery or Dataproc.

## 3. Key Design Decisions

*   **Migration to Cloud Composer (Airflow) for Orchestration**: Given the original script's role as an orchestrator, Airflow was chosen as the target platform for its robust scheduling, dependency management, monitoring, and error handling capabilities, which are superior to custom shell scripting.
*   **Re-implementation of Custom Shell Utilities in Python**: Instead of attempting to execute legacy shell scripts directly within Airflow (which would negate many benefits of migration), the custom utility functions (`DWMSG_*`, `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`) were re-implemented in Python. This allows for native integration within the Airflow environment and leverages Python's ecosystem.
*   **Leveraging Airflow's Native Features**:
    *   **Parameters**: Airflow's `params` mechanism is used for passing `stichtag` and `wiederanlaufwert`, providing a structured and auditable way to define DAG inputs.
    *   **XComs**: Task-specific variables (like `job_kennung`, `eintrags_nr`, `stichtag`) are pushed to XComs to ensure data flow between PythonOperators.
    *   **Logging**: While custom `DWMSG_*` functions are re-implemented for semantic consistency, they internally use Python's `logging` module, which Airflow integrates with Cloud Logging, providing centralized and searchable logs.
    *   **Error Handling**: Airflow's `on_failure_callback` and task retry mechanisms replace the KornShell `trap` statements and `set -e`, offering more sophisticated and configurable error management.
*   **Modular Design for Core Logic**: The `k_ausd_bp_ta_bpr_bcp.ksh` script, containing the core business logic, is recognized as a separate migration unit. The DAG includes a placeholder task (`invoke_core_logic_task`) to clearly delineate this dependency and allow for its independent migration to a suitable GCP data processing service (e.g., BigQuery, Dataproc). This prevents a monolithic migration and allows for specialized optimization of the data processing component.
*   **Airflow Variables for Environment Settings**: Environment variables like `BERT_DIR_ROOT` and `LOG_BASE_DIR` (originally sourced from `$HOME/.dw_init`) are managed as Airflow Variables. This centralizes configuration and makes it easily manageable within the Airflow UI.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated DAG in a production Cloud Composer environment, the following manual steps are required:

1.  **Cloud Composer Environment Setup**: Ensure a Cloud Composer environment is provisioned and running.
2.  **IAM Permissions**:
    *   The Service Account associated with your Cloud Composer environment must have appropriate permissions.
    *   **Cloud Composer Worker Service Account**: Needs permissions to:
        *   Read/write to Cloud Storage buckets (for DAGs, logs, and potentially data).
        *   Access Airflow Variables.
        *   If the `invoke_core_logic_task` eventually calls BigQuery or Dataproc:
            *   `bigquery.dataEditor` or `bigquery.jobUser` for BigQuery.
            *   `dataproc.editor` or `dataproc.worker` for Dataproc.
            *   `storage.objectViewer` and `storage.objectCreator` for GCS buckets used by Dataproc/BigQuery.
3.  **Airflow Variables Configuration**:
    *   Create the following Airflow Variables in your Composer environment via the Airflow UI or `gcloud` CLI:
        *   `BERT_DIR_ROOT`: Set to the appropriate root directory path for BERT-related components (e.g., `/app/bert`).
        *   `LOG_BASE_DIR`: Set to the base directory where logs should be stored (e.g., `/var/log/airflow/bert`). Ensure this path is accessible and writable by the Airflow worker.
4.  **Core Logic Migration (Dependency)**: The actual data processing logic from `k_ausd_bp_ta_bpr_bcp.ksh` must be migrated and deployed to its target GCP service (e.g., BigQuery SQL script, PySpark job on Dataproc). The `invoke_core_logic_task` in the DAG will need to be updated to call this migrated component.
5.  **Deployment of DAG and Utilities**:
    *   Upload `airflow_utils.py` to the `dags/` folder or a designated `plugins/` folder within your Composer environment's Cloud Storage bucket. If placed in `plugins/`, ensure it's correctly imported.
    *   Upload `r_ausd_bp_ta_bpr_bcp_dag.py` to the `dags/` folder in your Composer environment's Cloud Storage bucket.
6.  **Scheduling**: Determine the desired schedule for the DAG. Currently, `schedule=None` is set, indicating manual or external triggering. If scheduled execution is required, update the `schedule` parameter in the DAG definition.

## 5. Known Gaps & Unresolved References

*   **Custom Shell Framework Re-implementation Fidelity**: While the `airflow_utils.py` module provides Python equivalents for the custom shell functions (`DWMSG_*`, `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`), their exact behavior, especially regarding logging formats, error codes, and potential side effects (e.g., database updates for job status), needs thorough validation. The current Python implementations are functional but may require refinement to perfectly match the legacy system's nuances.
*   **`k_ausd_bp_ta_bpr_bcp.ksh` Migration (B4 Item)**: This is the most significant unresolved dependency. The `invoke_core_logic_task` in the DAG is currently a placeholder. The internal logic of `k_ausd_bp_ta_bpr_bcp.ksh` needs a dedicated migration analysis and implementation. This will determine whether it becomes a BigQuery SQL script, a PySpark job on Dataproc, or another GCP-native solution. The `BashOperator` will need to be replaced with an appropriate Airflow operator (e.g., `BigQueryOperator`, `DataprocSubmitJobOperator`).
*   **`$HOME/.dw_init` Contents**: The full scope and impact of the `$HOME/.dw_init` script are not entirely known. While `BERT_DIR_ROOT` and `LOG_BASE_DIR` are handled via Airflow Variables, other environment settings or configurations defined in `.dw_init` might be missing and could impact the core logic or other parts of the system. A comprehensive review of `.dw_init` is required.
*   **Error Handling Granularity**: While Airflow's `on_failure_callback` and retry mechanisms are robust, replicating the exact `trap` behavior for specific signals (INT, STOP, CONT, ERR) might require more granular error handling within individual PythonOperators if specific signal-driven logic was present in the original script. For typical data pipeline failures, Airflow's default mechanisms are usually sufficient.

## 6. Validation

To validate the migrated Airflow DAG:

1.  **Deploy the DAG**: Upload `airflow_utils.py` and `r_ausd_bp_ta_bpr_bcp_dag.py` to your Cloud Composer environment's DAGs folder.
2.  **Trigger the DAG**:
    *   Navigate to the Airflow UI.
    *   Find `r_ausd_bp_ta_bpr_bcp_dag`.
    *   Click the "Play" button to trigger a new DAG run.
    *   Provide parameters:
        *   `stichtag`: e.g., `"01012023"` (optional, will default to current date if not provided).
        *   `wiederanlaufwert`: e.g., `1` (optional, will default to `0` if not provided).
3.  **Monitor DAG Run**:
    *   Observe the DAG run in the Airflow UI (Graph View, Grid View, Gantt Chart).
    *   Ensure all tasks (`initialize_job_context`, `validate_parameters`, `invoke_core_logic`, `log_success`) execute in the correct sequence and complete successfully.
4.  **Check Logs**:
    *   For each task, view the logs in the Airflow UI.
    *   **Passing Criteria**:
        *   **`initialize_job_context`**: Logs should show the determined `stichtag` and `wiederanlaufwert`, and the `DWMSG_init_job` message.
        *   **`validate_parameters`**: Logs should confirm "Parameter validation successful."
        *   **`invoke_core_logic`**: Logs should show the "Simulating invocation..." messages and "Core logic simulation complete." (This will change once the actual core logic is migrated).
        *   **`log_success`**: Logs should contain the `DWMSG_SetzeStatusOK` message, indicating successful completion.
        *   The overall DAG run status in the Airflow UI should be "success".
        *   No errors or unexpected warnings should appear in the logs.
5.  **Parameter Testing**: Run the DAG with and without `stichtag` and `wiederanlaufwert` parameters to ensure default values are correctly applied. Test with invalid `stichtag` formats (if validation logic is enhanced) to confirm error handling.

## 7. Rollback Procedure

In case of issues with the migrated Airflow DAG, the rollback procedure is as follows:

1.  **Disable the Airflow DAG**: In the Airflow UI, toggle off the `r_ausd_bp_ta_bpr_bcp_dag` to prevent any further scheduled or manual runs.
2.  **Revert to Original Script**: Ensure the original KornShell script (`r_ausd_bp_ta_bpr_bcp.ksh`) and its dependencies are fully functional and re-enable its scheduling or triggering mechanism in the legacy environment.
3.  **Monitor Legacy System**: Verify that the original script executes correctly and produces the expected output in the legacy environment.
4.  **Data Consistency Check (if core logic migrated)**: If the `k_ausd_bp_ta_bpr_bcp.ksh` core logic was already migrated and potentially wrote data to BigQuery or other GCP services, a data consistency check might be necessary to ensure no partial or incorrect data was written by the new system. Depending on the nature of the data, this might involve:
    *   Identifying and reverting any changes made by the new system.
    *   Running a full reload of the affected data using the legacy system.
    *   This step is highly dependent on the specific implementation of the core logic.
5.  **Remove Migrated Artifacts (Optional)**: Once the rollback is confirmed stable, the `r_ausd_bp_ta_bpr_bcp_dag.py` and `airflow_utils.py` files can be removed from the Composer DAGs folder.