# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh`.

The original script served as an orchestration wrapper for the initial provisioning of selected basic products for BERT, handling parameter parsing, environment setup, logging, error management, and invoking a core processing script (`k_ausd_bp_ta_bpr_basis.ksh`).

The script has been migrated to Google Cloud Platform, specifically targeting **Google Cloud Composer (Apache Airflow)** for scheduling and orchestration. The orchestration logic has been rewritten in **Python**, leveraging standard Python libraries and integrating with Google Cloud services like **Cloud Logging**.

## 2. Generated Artifacts

The migration process for `r_ausd_bp_ta_bpr_basis.ksh` results in the following primary artifacts:

*   **`dags/r_ausd_bp_ta_bpr_basis_dag.py`**
    *   **Role:** This is the main Apache Airflow DAG file written in Python. It encapsulates the orchestration logic of the original KornShell script, including argument parsing, parameter defaulting, logging setup, and the invocation of the core processing logic as a distinct Airflow task.
*   **`dags/common/utils.py`**
    *   **Role:** A Python module containing utility functions. This module replaces the functionalities provided by the legacy KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). It provides Python equivalents for logging, error handling, date manipulation, and job entry number generation, designed to integrate with Cloud Logging.
*   **Dependent Artifacts (Migration of `k_ausd_bp_ta_bpr_basis.ksh`)**
    *   **`dags/k_ausd_bp_ta_bpr_basis.py` (Example)**
        *   **Role:** If the core processing script `k_ausd_bp_ta_bpr_basis.ksh` is migrated to a Python script, this file would contain its logic. The `r_ausd_bp_ta_bpr_basis_dag.py` would then invoke this Python script, likely via a `PythonOperator`.
    *   **`sql/k_ausd_bp_ta_bpr_basis.sql` (Example)**
        *   **Role:** If the core processing script `k_ausd_bp_ta_bpr_basis.ksh` is migrated to a BigQuery SQL script or stored procedure, this file would contain the SQL logic. The `r_ausd_bp_ta_bpr_basis_dag.py` would then invoke this SQL, likely via a `BigQueryOperator`.

## 3. Key Design Decisions

*   **Cloud Composer (Airflow) for Orchestration:** Chosen for its robust scheduling capabilities, extensibility, and cloud-native integration, providing a modern and scalable platform for job orchestration.
*   **KornShell to Python Conversion:** The entire orchestration logic was rewritten in Python. This decision was made to leverage Python's rich ecosystem, improved maintainability, better error handling, and seamless integration with Google Cloud services, moving away from legacy shell scripting.
*   **Replacement of Legacy Utilities with Python Modules:** All custom KornShell utility functions (e.g., `DWMSG_*`, `DWDate_Gib_Zeitraum`) were reimplemented as standard Python modules (`common/utils.py`). This centralizes common logic, improves reusability, and aligns with Python best practices.
*   **Integration with Cloud Logging:** Replaced file-based logging with structured logging directed to Google Cloud Logging. This provides centralized log management, advanced filtering, monitoring, and alerting capabilities.
*   **Airflow's Native Error Handling:** Shell `trap` mechanisms were replaced by Airflow's built-in error handling features, such as `on_failure_callback` and `retries`, which offer more sophisticated and configurable error management.
*   **Decoupled Core Logic Invocation:** The orchestrator DAG is designed to invoke the core processing logic (`k_ausd_bp_ta_bpr_basis.ksh`'s migrated equivalent) as a separate, distinct Airflow task. This allows for independent migration strategies for the core business logic (e.g., to BigQuery SQL, Python, or PySpark) and promotes modularity.
*   **Parameter Handling with `argparse` and Airflow Context:** Python's `argparse` is used for robust command-line argument parsing, and Airflow's templating and `op_kwargs` are utilized to pass parameters dynamically, replacing the legacy `getopts` and shell variable handling.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

*   **BigQuery Dataset and Table Creation:**
    *   Ensure the target BigQuery dataset exists.
    *   Create the necessary BigQuery tables for the source data (e.g., `DWH$TA_C_VERTRAG`) and the target output (e.g., "FOS-Tabelle") with their respective schemas.
*   **IAM Permissions Configuration:**
    *   Grant the Cloud Composer service account (or the specific service account used by the Airflow environment) the necessary IAM roles and permissions. This typically includes:
        *   `BigQuery Data Editor` (for writing to target tables)
        *   `BigQuery Data Viewer` (for reading from source tables)
        *   `BigQuery Job User` (for running BigQuery jobs)
        *   `Logs Writer` (for writing to Cloud Logging)
        *   `Storage Object Viewer` and `Storage Object Creator` (for DAG deployment and potentially intermediate data storage)
        *   Any other permissions required by the core processing script's migrated logic (e.g., Dataproc Worker, Cloud Storage).
*   **Airflow Variables Configuration:**
    *   Define Airflow Variables for any environment-specific configurations or paths, such as `BERT_DIR_ROOT`.
*   **Deployment of Utility Module:**
    *   Ensure the `common/utils.py` module is deployed to a location accessible by the Airflow workers (e.g., within the DAGs folder or a designated Python package directory in the Composer environment).
*   **Core Processing Logic Deployment:**
    *   Deploy the migrated core processing script (`k_ausd_bp_ta_bpr_basis.py` or `k_ausd_bp_ta_bpr_basis.sql`) to the appropriate location (e.g., DAGs folder for Python, Cloud Storage for SQL scripts).
*   **Airflow DAG Deployment:**
    *   Upload `r_ausd_bp_ta_bpr_basis_dag.py` to the Cloud Composer environment's DAGs folder.
*   **Scheduling Configuration:**
    *   Configure the `schedule_interval` for the `r_ausd_bp_ta_bpr_basis_dag` within Airflow to match the original job's execution frequency.

## 5. Known Gaps & Unresolved References

*   **Core Logic Migration (`k_ausd_bp_ta_bpr_basis.ksh`):** The most significant unresolved item is the detailed migration strategy and implementation of the core data transformation and loading logic residing in `k_ausd_bp_ta_bpr_basis.ksh`. Its content and migration approach (e.g., to BigQuery SQL, PySpark, or Python) will dictate the specific Airflow operator used in the orchestrator DAG. This is a critical dependency for full end-to-end functionality.
*   **Detailed Functionality of Legacy Sourced Utilities:** While the general purpose of the sourced KornShell utility scripts is understood, the exact implementation details of functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and `DWMSG_*` were inferred. A detailed analysis of their original source code might be required to ensure 100% behavioral parity in the Python `common/utils.py` module.
*   **Data Volume and Performance:** The current design focuses on functional equivalence. Performance considerations for large data volumes, particularly those handled by the invoked core script, will need to be thoroughly evaluated during the migration of `k_ausd_bp_ta_bpr_basis.ksh` and optimized within BigQuery or other data processing engines.
*   **Restart/Recovery Logic (`-l` parameter):** The precise handling of the `Wiederanlaufwert` (restart value) and its implications for data consistency and idempotent processing (e.g., deletion of entries >= `Wiederanlaufwert`) needs careful re-implementation to ensure correct behavior in the BigQuery environment.

## 6. Validation

To validate the migrated `r_ausd_bp_ta_bpr_basis_dag`, follow these steps:

1.  **Unit Testing:**
    *   Run unit tests for the `common/utils.py` module to ensure individual utility functions (e.g., date formatting, log entry generation) behave as expected.
2.  **Airflow DAG Syntax Check:**
    *   Upload the `r_ausd_bp_ta_bpr_basis_dag.py` to the Cloud Composer environment. Airflow will automatically parse it and report any syntax errors.
3.  **Manual Trigger and Parameter Testing:**
    *   Manually trigger the `r_ausd_bp_ta_bpr_basis_dag` from the Airflow UI.
    *   Test with various combinations of parameters:
        *   No parameters (should default `Stichtag` to system date, `Wiederanlaufwert` to 0).
        *   Only `-s STICHTAG` (e.g., `20231231`).
        *   Only `-l WIEDERANLAUFWERT` (e.g., `1000`).
        *   Both `-s` and `-l`.
    *   Verify that the DAG runs successfully and completes all its tasks.
4.  **Cloud Logging Verification:**
    *   Monitor Cloud Logging for the DAG's execution.
    *   **Passing Criteria:**
        *   All log messages from the Python orchestrator are present in Cloud Logging.
        *   Log entries correctly reflect the parsed parameters (`Stichtag`, `Wiederanlaufwert`), `Job-Nr`, and `Logdatei`.
        *   No errors or exceptions are reported in the logs.
        *   The "success" message ("Die Abarbeitung wurde ohne erkennbare Fehler beendet") is logged upon completion.
5.  **Core Script Invocation Verification:**
    *   Confirm that the core processing task (representing `k_ausd_bp_ta_bpr_basis.ksh`) is correctly invoked with the expected parameters.
    *   If the core script has been migrated, verify its logs and output in BigQuery.
6.  **Exit Status:**
    *   The Airflow DAG should complete with a "success" status (green in the Airflow UI).

## 7. Rollback Procedure

In case of issues or unexpected behavior after deploying the migrated `r_ausd_bp_ta_bpr_basis_dag`, follow this rollback procedure:

1.  **Deactivate the New Airflow DAG:**
    *   In the Airflow UI, toggle off the `r_ausd_bp_ta_bpr_basis_dag` to prevent any further scheduled or manual runs.
2.  **Re-enable Original Scheduling:**
    *   If the original `r_ausd_bp_ta_bpr_basis.ksh` script was scheduled via a cron job or another scheduler, re-enable its original scheduling mechanism.
3.  **Restore Data (if necessary):**
    *   If the migrated DAG or its invoked core processing script made any irreversible changes to BigQuery tables or other data stores, perform a data rollback to the state prior to the new DAG's execution. This might involve restoring from backups or executing specific cleanup/reversal scripts. This step is critical and depends heavily on the idempotency and data impact of the core processing logic.
4.  **Remove Migrated Artifacts (Optional):**
    *   If the rollback is intended to be long-term, consider removing the `r_ausd_bp_ta_bpr_basis_dag.py` and `common/utils.py` files from the Cloud Composer DAGs folder to avoid confusion.
5.  **Investigate and Rectify:**
    *   Analyze the logs in Cloud Logging and Airflow task logs to identify the root cause of the failure. Address the issues in the Python code or Airflow DAG definition before attempting re-deployment.