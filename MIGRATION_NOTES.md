# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_bpr_evn.ksh`. The original script served as an orchestrator, preparing parameters and invoking a core kernel script (`k_ausd_bp_ta_bpr_evn.ksh`) to provision basic product data for the BERT system, ultimately generating a DWH contract cache snapshot for demand scoring.

The migration involved translating the orchestration logic of this wrapper script from KornShell to a Python script (`r_ausd_bp_ta_bpr_evn.py`). The target platform for this migrated component is the Google Cloud Platform (GCP) ecosystem, specifically designed to run within the Horizon Python framework. The actual data processing logic, originally handled by the core kernel script, is considered a separate, downstream migration effort.

## 2. Generated Artifacts

The migration produced the following primary artifact:

*   **`r_ausd_bp_ta_bpr_evn.py`**: This Python script is the direct replacement for the original KornShell script. Its role is to:
    *   Parse command-line arguments for processing date (`Stichtag`) and restart value (`Wiederanlaufwert`).
    *   Determine the processing date, defaulting to the current system date if not provided.
    *   Set up basic logging and job identification.
    *   Invoke the migrated core processing logic (represented by a placeholder `run_kernel_script`) with the resolved parameters.
    *   Handle basic error reporting and exit status.

## 3. Key Design Decisions

*   **Orchestration-Focused Migration**: The primary decision was to migrate only the orchestration logic of the wrapper script, leaving the complex data processing logic (originally in `k_ausd_bp_ta_bpr_evn.ksh`) for a separate, dedicated migration. This modular approach simplifies the wrapper's migration and allows the core data processing to be optimized for GCP services (e.g., BigQuery SQL, PySpark).
*   **Horizon Python Framework**: The choice of Horizon Python aligns with the target GCP ecosystem, providing a standardized environment for job execution, logging, and integration with other GCP services.
*   **Standard Python Libraries**:
    *   **Parameter Parsing**: The KornShell `getopts` mechanism was replaced with Python's `argparse` module for robust, clear, and maintainable command-line argument handling.
    *   **Date Handling**: Custom shell date functions were replaced by Python's `datetime` module, offering native, reliable, and comprehensive date and time manipulation capabilities.
    *   **Error Handling**: Shell `trap` mechanisms were replaced with Python's `try-except` blocks for structured exception handling.
*   **Decoupled Environment Configuration**: The reliance on sourcing `. $HOME/.dw_init` was replaced by the expectation that necessary environment variables (like `BERT_DIR_ROOT`) will be explicitly defined in the target Python execution environment (e.g., via Airflow variables, Kubernetes config maps, or environment variables). This promotes cloud-native deployment practices.
*   **Placeholder for Core Logic**: The `run_kernel_script` function in the generated Python code is a placeholder. This design decision acknowledges that the actual invocation mechanism for the migrated core processing will depend on its own migration path (e.g., triggering a BigQuery job, calling another Python module). This allows for parallel or sequential migration of dependent components.

**Notable Trade-offs**:
*   The generated Python script is not fully functional in isolation; it requires the subsequent migration and integration of the core kernel script (`k_ausd_bp_ta_bpr_evn.ksh`) to perform its complete intended function.
*   The initial logging in the generated Python code uses basic `print` statements. A full integration with Python's `logging` module and potentially GCP Cloud Logging is a follow-up task.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **Environment Variable Configuration**:
    *   Define `BERT_DIR_ROOT` and any other required environment variables (e.g., GCP project ID, region, dataset names) in the target execution environment (e.g., Cloud Composer/Airflow environment variables, Kubernetes secrets/config maps).
2.  **IAM Permissions**:
    *   Ensure the GCP service account used to execute the Python script has the necessary IAM roles and permissions. This typically includes:
        *   `roles/logging.logWriter` for writing logs to Cloud Logging.
        *   Permissions to trigger or execute the migrated core kernel script (e.g., `bigquery.jobs.create` if it's a BigQuery job, `dataflow.jobs.create` for Dataflow, or `cloudfunctions.invoker` for Cloud Functions).
        *   Permissions to access any required configuration files or secrets (e.g., Secret Manager access).
3.  **Core Kernel Script Migration & Integration**:
    *   The core kernel script `k_ausd_bp_ta_bpr_evn.ksh` **must be migrated and deployed** to its target GCP equivalent (e.g., a BigQuery stored procedure, a Dataflow job, or another Python script).
    *   The `run_kernel_script` placeholder in `r_ausd_bp_ta_bpr_evn.py` must be updated to correctly invoke this migrated component, passing all necessary parameters.
4.  **Helper Function Implementation**:
    *   The basic `print` statements for logging and simplified date handling in the generated Python code should be replaced with a robust implementation leveraging Python's `logging` module and integrating with Horizon Python's framework utilities or GCP Cloud Logging. This includes fully implementing the logic for `get_next_job_entry_number`, `get_log_filename`, `create_log_entry`, `set_stichtag_info`, `log_error`, `handle_error`, `set_status_ok`, and `append_to_log`.
5.  **Scheduling**:
    *   Configure a scheduler (e.g., Cloud Composer/Airflow DAG, Cloud Scheduler) to trigger `r_ausd_bp_ta_bpr_evn.py` at the required frequency and with the appropriate command-line arguments (`-s`, `-l`).
6.  **Secrets Management**:
    *   If the core kernel script (or any part of the overall job) requires sensitive information (e.g., database credentials, API keys), ensure these are securely stored (e.g., in Google Secret Manager) and accessed by the job.

## 5. Known Gaps & Unresolved References

*   **Core Kernel Script Integration (B4 Item)**: The most significant gap is the placeholder `run_kernel_script` function. The actual invocation mechanism for the migrated `k_ausd_bp_ta_bpr_evn.ksh` (whether it's a BigQuery job, a Dataflow pipeline, or another Python script) needs to be implemented. This is a critical dependency for the full functionality of the job.
*   **Full Logging and Error Handling Implementation**: While basic `print` statements are in place, a complete implementation of the `DWMSG_` functions and other utility script logic (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is pending. This should involve integrating with Python's `logging` module and potentially GCP Cloud Logging for centralized log management and monitoring.
*   **Environment Variable Resolution**: The exact values and secure provisioning methods for `BERT_DIR_ROOT` and other environment variables in the GCP environment need to be finalized.
*   **Custom Error Code Mapping**: The custom error codes (e.g., `ErrNr=193`, `ErrNr=192`) from the original KornShell script are not explicitly mapped in the Python code. A standardized error reporting and handling mechanism should be established for the target environment.
*   **Implicit Assumptions**: There might be implicit environment settings, file paths, or system dependencies in the legacy environment that are not immediately apparent from the script code. A thorough review with legacy system experts is recommended to uncover and address these.

## 6. Validation

Validation of the migrated `r_ausd_bp_ta_bpr_evn.py` script involves both unit and integration testing:

**How to Run Tests:**

1.  **Unit Tests (Python `pytest` or similar)**:
    *   Create a test file (e.g., `test_r_ausd_bp_ta_bpr_evn.py`) that imports the functions from `r_ausd_bp_ta_bpr_evn.py`.
    *   Use a testing framework like `pytest` to define test cases.
    *   Run tests using `pytest test_r_ausd_bp_ta_bpr_evn.py`.

2.  **Integration Tests (Manual Execution / CI/CD Pipeline)**:
    *   Execute the script directly from the command line or within a CI/CD pipeline.
    *   `python r_ausd_bp_ta_bpr_evn.py -s 01012023 -l 12345`
    *   `python r_ausd_bp_ta_bpr_evn.py` (to test default date)
    *   `python r_ausd_bp_ta_bpr_evn.py -h` (to test help message)
    *   `python r_ausd_bp_ta_bpr_evn.py --unknown-param` (to test error handling for unknown params)

**What "Passing" Means:**

*   **Unit Tests**:
    *   **Parameter Parsing**: `parse_args` correctly parses valid `-s`, `-l`, and `-h` arguments. It raises `ValueError` for unknown arguments or missing required parameters (if implemented).
    *   **Date Determination**: `get_system_date_ddmmyyyy` returns the current date in `DDMMYYYY` format.
    *   **Default Values**: The `main` function correctly assigns default values for `p_stichtag` (current system date) and `p_wiederanlaufwert` (0) when not provided.
    *   **Validation**: `validate_required_parameter` correctly identifies and raises an error for `None` or empty string inputs.
*   **Integration Tests**:
    *   **Successful Execution**: The script exits with a return code of `0` for valid inputs.
    *   **Correct Output/Logging**: The console output (and eventually Cloud Logging) contains the expected messages, including job start, identified parameters, and the success message "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
    *   **Core Script Invocation**: The `run_kernel_script` placeholder (or its actual implementation) is invoked with the correct `job_kennung`, `stichtag`, `eintragsnr`, and `wiederanlaufwert` values.
    *   **Error Handling**: For invalid inputs or simulated failures in `run_kernel_script`, the script exits with a return code of `1` and prints an appropriate error message (e.g., "AppError: Abbruch - ...").
    *   **End-to-End Data Verification (Post-Core Migration)**: Once the core kernel script is fully migrated and integrated, a passing test also means that the expected data snapshot is correctly generated and available in the target BigQuery table, matching the logic of the original `k_ausd_bp_ta_bpr_evn.ksh`.

## 7. Rollback Procedure

In the event of issues after deployment, the following rollback procedure should be followed:

1.  **Immediate Deactivation**:
    *   If critical errors or unexpected behavior are observed immediately after deploying `r_ausd_bp_ta_bpr_evn.py` (e.g., job fails to start, incorrect parameters passed, high error rates):
        *   **Deactivate** the new `r_ausd_bp_ta_bpr_evn.py` job in the target scheduler (e.g., pause the Airflow DAG, disable the Cloud Scheduler job).
        *   **Re-enable** the original `r_ausd_bp_ta_bpr_evn.ksh` job in the legacy scheduler to ensure business continuity.
        *   Monitor the legacy job to confirm it resumes normal operation.
2.  **Data Rollback (if applicable)**:
    *   This wrapper script primarily orchestrates. If the *migrated core kernel script* (invoked by `run_kernel_script`) has already written or modified data in BigQuery, a specific data rollback strategy for that component would be necessary. This might involve:
        *   Restoring BigQuery tables from a previous snapshot.
        *   Using BigQuery's time travel feature to query data before the erroneous run.
        *   Executing a compensating transaction or deletion script if the data changes are reversible.
        *   **Note**: This data rollback is dependent on the core kernel script's migration and is outside the direct scope of this wrapper script's rollback.
3.  **Investigation and Remediation**:
    *   Collect and analyze logs from the failed Python job (from Cloud Logging).
    *   Review the deployed code (`r_ausd_bp_ta_bpr_evn.py`) and its configuration.
    *   Identify the root cause of the failure (e.g., incorrect environment variables, bug in Python logic, issue with core script invocation).
    *   Address the identified issues, perform thorough testing in a non-production environment, and then plan for re-deployment.