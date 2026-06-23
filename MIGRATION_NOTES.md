# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh` has been migrated to the Google Cloud Platform. This migration primarily focuses on transforming the orchestration, parameter handling, logging, and error management aspects of the original script.

The target platform leverages:
*   **Google Cloud Composer (Airflow)** for job orchestration, scheduling, and execution control.
*   **Python** for implementing the wrapper logic, parameter handling, logging, and utility functions.
*   **Google Cloud Logging** for centralized log management and monitoring.
*   **Google BigQuery** as the target data warehouse for the `ta_cntrct_crs3` table.

The core data processing logic, originally contained within `k_ausd_v_ta_cntrct_crs3.ksh`, is still pending a separate detailed migration and is represented as a placeholder task within the new Airflow DAG.

## 2. Generated artifacts

The migration process generated the following files:

*   **`ddl/ta_cntrct_crs3.sql`**
    *   **Role:** Placeholder Data Definition Language (DDL) script for creating the `ta_cntrct_crs3` table in Google BigQuery. This script needs to be updated with the actual schema derived from the legacy system.
*   **`utils/env_config.py`**
    *   **Role:** Python module that replaces the environment variable sourcing functionality of the legacy `.dw_init` script. It provides a centralized class for accessing environment-specific configurations, which can be loaded from OS environment variables or Airflow variables.
*   **`utils/logging_utils.py`**
    *   **Role:** Python module that re-implements the logging and error handling functions previously provided by `f_alis_msgerr.ksh`. It uses Python's standard `logging` module, designed for integration with Google Cloud Logging, and includes custom exceptions for error reporting.
*   **`utils/date_utils.py`**
    *   **Role:** Python module that replaces the date calculation and validation utilities from `h_alis_date.ksh`. It uses Python's `datetime` module for date operations, replacing legacy `sqlplus` calls.
*   **`utils/parameter_utils.py`**
    *   **Role:** Python module that replaces the parameter handling and validation logic from `h_alis_parameter.ksh`. It includes functions for parameter validation, code conversion, and date range checks, raising custom exceptions for invalid parameters.
*   **`dags/r_ausd_v_ta_cntrct_crs3_dag.py`**
    *   **Role:** The main Airflow DAG (Directed Acyclic Graph) that orchestrates the contract data synchronization. This Python script replaces the `r_ausd_v_ta_cntrct_crs3.ksh` wrapper, handling parameter parsing, environment setup, logging, error handling, and invoking the (yet-to-be-migrated) core data processing logic.
*   **`tests/test_env_config.py`**
    *   **Role:** Unit tests for the `utils/env_config.py` module, verifying correct loading and overriding of environment configurations.
*   **`tests/test_logging_utils.py`**
    *   **Role:** Unit tests for the `utils/logging_utils.py` module, ensuring that logging functions are called correctly and exceptions are raised as expected.
*   **`tests/test_date_utils.py`**
    *   **Role:** Unit tests for the `utils/date_utils.py` module, validating the accuracy of date calculations and format checks.
*   **`tests/test_parameter_utils.py`**
    *   **Role:** Unit tests for the `utils/parameter_utils.py` module, checking the correctness of parameter validation, conversion, and error handling.

## 3. Key design decisions

1.  **Orchestration Layer Migration (KornShell to Airflow DAG):**
    *   **Decision:** The `r_ausd_v_ta_cntrct_crs3.ksh` wrapper script's control flow, parameter handling, and job execution logic were re-implemented as a Python-based Airflow DAG.
    *   **Rationale:** Airflow (via Cloud Composer) provides a cloud-native, managed, and scalable solution for workflow orchestration. It offers robust scheduling, monitoring, retry mechanisms, and a clear separation of concerns, which are superior to the legacy KornShell script's capabilities. Python is the native language for Airflow DAGs, ensuring seamless integration.

2.  **Utility Script Re-implementation (KornShell to Python Modules):**
    *   **Decision:** The functionality of shared KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) was re-implemented as dedicated Python modules (`env_config.py`, `logging_utils.py`, `parameter_utils.py`, `date_utils.py`).
    *   **Rationale:** This ensures consistency with the Airflow DAG's language (Python), improves testability, leverages modern Python libraries (e.g., `datetime`, `logging`), and facilitates better code organization and maintainability.

3.  **Logging and Error Handling (Custom KSH/SQL to Python Logging & Cloud Logging):**
    *   **Decision:** The legacy `DWMSG_*` functions and `sqlplus` interactions for logging were replaced with Python's standard `logging` module, configured to integrate with Google Cloud Logging. `trap` statements were replaced by Python exception handling and Airflow's built-in task failure/retry mechanisms.
    *   **Rationale:** Google Cloud Logging provides centralized, structured logging, enabling easier monitoring, alerting, and debugging across the GCP environment. Python's exception handling is more robust and idiomatic than KornShell's `trap`, and Airflow natively handles task failures and retries.

4.  **Parameter Management (KSH `getopts` to Airflow DAG Params & Python):**
    *   **Decision:** Command-line parameter parsing (`getopts`) was translated into Airflow DAG parameters and Python functions within `parameter_utils.py`.
    *   **Rationale:** Airflow DAG parameters offer a standardized way to define and pass runtime configurations, visible and manageable through the Airflow UI. Python's parameter handling is more type-safe and flexible.

5.  **Core Data Processing Decoupling:**
    *   **Decision:** The migration of the wrapper script (`r_ausd_v_ta_cntrct_crs3.ksh`) was completed independently of the core data processing script (`k_ausd_v_ta_cntrct_crs3.ksh`). A placeholder task in the Airflow DAG (`execute_core_data_processing`) is provided for future integration.
    *   **Rationale:** This allows for an iterative migration approach, addressing the orchestration layer first. The core script's migration can then be designed and implemented using the most appropriate GCP service (e.g., BigQuery SQL, Dataform, Python with BigQuery client) based on its specific logic and complexity.

6.  **Target Data Store (Oracle to BigQuery):**
    *   **Decision:** The `ta_cntrct_crs3` table will be provisioned as a native BigQuery table.
    *   **Rationale:** BigQuery offers a highly scalable, cost-effective, and fully managed data warehouse solution, aligning with GCP's data analytics ecosystem.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Target Table Creation:**
    *   **Action:** Define and create the actual schema for the `ta_cntrct_crs3` table in BigQuery. The provided `ddl/ta_cntrct_crs3.sql` is a placeholder and must be updated with the precise column names, data types, and partitioning/clustering strategies derived from the legacy `ta_cntrct_crs3` table.
    *   **Details:** Replace `<your-gcp-project-id>` and `<your-bigquery-dataset>` in the DDL script with your specific GCP project ID and BigQuery dataset name.

2.  **GCP Project and Dataset Configuration:**
    *   **Action:** Update the placeholder values for `GCP_PROJECT_ID` and `BQ_DATASET` in `utils/env_config.py` (or ensure these are set as environment variables in the Airflow environment).

3.  **Airflow Environment Setup (Cloud Composer):**
    *   **Action:** Deploy the `dags/r_ausd_v_ta_cntrct_crs3_dag.py` and all `utils/*.py` modules to your Cloud Composer environment's DAGs folder and plugins folder (or ensure they are accessible on the Python path).
    *   **Action:** Configure Airflow Variables or Connections for any sensitive parameters or external system connection details (e.g., if `DW_ORAUSER` or similar credentials are still required for source systems, or if the core script needs specific BigQuery connection details).
    *   **Action:** Set environment variables in the Cloud Composer environment for `BERT_DIR_ROOT`, `DW_DIR_ROOT`, `DW_DIR_PROT` if they are not managed through Airflow Variables or hardcoded.

4.  **IAM Permissions:**
    *   **Action:** Ensure the Google Cloud service account associated with your Cloud Composer environment has the necessary IAM roles and permissions.
    *   **Details:** This typically includes `BigQuery Data Editor` (or similar) for writing to `ta_cntrct_crs3`, `Logs Writer` for Cloud Logging, and potentially permissions for any other GCP services that the core data processing script (`k_ausd_v_ta_cntrct_crs3.ksh` once migrated) will interact with (e.g., Cloud Storage, Dataflow, Pub/Sub).

5.  **Scheduling Configuration:**
    *   **Action:** Update the `schedule_interval` parameter in `dags/r_ausd_v_ta_cntrct_crs3_dag.py` to match the desired execution frequency (e.g., a cron expression like `"0 5 * * *"` for daily at 5 AM, or `@daily`).

6.  **Core Data Processing Script Migration (`k_ausd_v_ta_cntrct_crs3.ksh`):**
    *   **Action:** This is the most critical manual step. The `k_ausd_v_ta_cntrct_crs3.ksh` script needs to be thoroughly analyzed, designed, and migrated to a BigQuery-compatible solution (e.g., BigQuery SQL, Python script using BigQuery client libraries, or a Dataform pipeline).
    *   **Action:** Once migrated, the `execute_core_data_processing` task in `dags/r_ausd_v_ta_cntrct_crs3_dag.py` must be updated to invoke this new core logic, passing any required parameters.

## 5. Known gaps & unresolved references

The following items are identified as known gaps or require further follow-up:

1.  **Core Data Processing Logic (`k_ausd_v_ta_cntrct_crs3.ksh`):**
    *   **Gap:** The exact functionality, data sources, transformation logic, and target writes of `k_ausd_v_ta_cntrct_crs3.ksh` are currently unknown.
    *   **Follow-up:** A detailed analysis and separate migration design for this script are required. This will determine the final implementation (e.g., BigQuery SQL, Python, Dataform) and how it integrates with the `execute_core_data_processing` task in the Airflow DAG.

2.  **`ta_cntrct_crs3` BigQuery Schema:**
    *   **Gap:** The provided `ddl/ta_cntrct_crs3.sql` is a placeholder. The precise schema (column names, data types, nullability, partitioning, clustering) for the target BigQuery table needs to be reverse-engineered from the legacy `ta_cntrct_crs3` table.
    *   **Follow-up:** Obtain the DDL or schema definition from the legacy Oracle database and update `ddl/ta_cntrct_crs3.sql` accordingly.

3.  **Relevance of `DW_ORAUSER`:**
    *   **Gap:** The legacy `DW_ORAUSER` environment variable was likely used for Oracle database connections. Its relevance in the BigQuery context is unclear.
    *   **Follow-up:** Determine if this user was for source data extraction or other purposes. If source data is still from Oracle, a new migration path for that source will be needed, potentially using different credentials (e.g., Cloud SQL Proxy, database migration services). If it's no longer relevant, it can be removed.

4.  **Completeness of Utility Script Re-implementation:**
    *   **Gap:** While common functions from `h_alis_parameter.ksh` and `h_alis_date.ksh` have been translated, there might be less frequently used functions or specific edge-case logic in the original KornShell scripts that were not fully captured.
    *   **Follow-up:** A thorough review of the original KornShell utility scripts against their Python counterparts is recommended to ensure 100% functional parity.

5.  **Implicit Dependencies and Side Effects:**
    *   **Gap:** The original `r_ausd_v_ta_cntrct_crs3.ksh` script might have implicit dependencies on the legacy environment (e.g., specific system configurations, external tools, file paths) that are not immediately apparent from the script's code.
    *   **Follow-up:** During the analysis of `k_ausd_v_ta_cntrct_crs3.ksh` and integration testing, watch for any unexpected behavior or missing components that might indicate such implicit dependencies.

6.  **"semi_auto" Automation Bucket:**
    *   **Gap:** The original job was categorized as `semi_auto`, indicating that some manual intervention or re-engineering was expected. This reinforces the need for careful review and validation, especially for the core script.
    *   **Follow-up:** Acknowledge that the migration of the core logic will likely require significant manual effort and detailed design.

## 6. Validation

Validation of the migrated wrapper script and its utilities involves several layers of testing:

1.  **Unit Tests (Python Utilities):**
    *   **How to run:** Navigate to the `tests/` directory and execute the Python unit tests using a test runner (e.g., `python -m unittest discover tests`).
    *   **What "passing" means:** All tests in `test_env_config.py`, `test_logging_utils.py`, `test_date_utils.py`, and `test_parameter_utils.py` must execute successfully without any failures or errors. This verifies the correct functionality of the re-implemented utility modules.

2.  **Airflow DAG Syntax and Structure Validation:**
    *   **How to run:**
        *   Upload the DAG to your Cloud Composer environment. Airflow will automatically parse it.
        *   Alternatively, use the Airflow CLI: `airflow dags list --subdir dags/r_ausd_v_ta_cntrct_crs3_dag.py` to check for parsing errors.
        *   Perform a dry run: `airflow dags test r_ausd_v_ta_cntrct_crs3_orchestration <execution_date>` (e.g., `2023-01-01`).
    *   **What "passing" means:** The DAG appears in the Airflow UI without syntax errors, and the dry run executes successfully, indicating correct task definitions and dependencies.

3.  **Airflow DAG Execution (Orchestration Logic):**
    *   **How to run:** Trigger the `r_ausd_v_ta_cntrct_crs3_orchestration` DAG manually from the Airflow UI or via the CLI. Provide sample `job_kennung` and other parameters if needed.
    *   **What "passing" means:**
        *   The `initialize_job_entry` task completes successfully, pushing `job_kennung`, `dw_eintrags_nr`, and `log_file_name` to XComs.
        *   The `execute_core_data_processing` task (even in its placeholder state) completes successfully, indicating that the invocation mechanism is sound.
        *   The `finalize_job_status` task completes successfully.
        *   Logs generated by the DAG (from `logging_utils.py`) appear correctly in Google Cloud Logging, reflecting the job's lifecycle (creation, info messages, success/failure status).
        *   Test with invalid parameters to ensure `parameter_utils` correctly raises exceptions and the DAG handles them by failing the task and logging an error.

4.  **End-to-End Data Validation (After Core Script Migration):**
    *   **How to run:** Once `k_ausd_v_ta_cntrct_crs3.ksh` is fully migrated and integrated into the `execute_core_data_processing` task, trigger the Airflow DAG with representative test data.
    *   **What "passing" means:**
        *   The `ta_cntrct_crs3` table in BigQuery is populated with data.
        *   The data in BigQuery matches the expected output from the legacy system for the same input. This involves comparing row counts, specific data points, and overall data integrity.
        *   Performance metrics (e.g., execution time, resource consumption) are within acceptable limits compared to the legacy job.

## 7. Rollback procedure

In the event that the migrated job needs to be rolled back, follow these steps:

1.  **Deactivate/Delete Airflow DAG:**
    *   **Action:** In the Cloud Composer UI, pause or delete the `r_ausd_v_ta_cntrct_crs3_orchestration` DAG. This will prevent any further executions of the migrated job.

2.  **Re-enable Legacy Scheduling:**
    *   **Action:** Re-activate the original scheduling mechanism (e.g., cron job, scheduler entry) that triggered the `r_ausd_v_ta_cntrct_crs3.ksh` script in the legacy environment.
    *   **Verification:** Confirm that the legacy script is running as expected and processing data.

3.  **BigQuery Data Restoration (if necessary):**
    *   **Action:** If the migrated job made any destructive changes (e.g., `WRITE_TRUNCATE`) to the `ta_cntrct_crs3` table in BigQuery, and if data integrity is compromised, restore the table to its state before the migrated job's execution.
    *   **Details:** This can be done using BigQuery's time travel feature (if within the time window), restoring from a BigQuery snapshot, or reloading data from a reliable backup. If the BigQuery table was newly created and not yet in production use, it can simply be dropped.

4.  **Verify Legacy System Functionality:**
    *   **Action:** Monitor the legacy `r_ausd_v_ta_cntrct_crs3.ksh` job and its downstream dependencies to ensure full operational recovery.
    *   **Verification:** Check logs, output data, and any relevant dashboards in the legacy environment.

This rollback procedure assumes that the legacy environment and its dependencies remain operational and can be quickly re-enabled.