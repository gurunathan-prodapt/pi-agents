# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/istools/seu/template/.dw_init`. The original script was responsible for initializing various environment variables, including directory paths, remote system details, and dynamically determining `ORACLE_HOME`.

The migration targets a BigQuery-centric data platform, leveraging Python-based orchestration (e.g., Horizon Python, Cloud Composer/Airflow) for environment management. The core logic of setting environment variables has been translated into a Python script, `dw_init_environment.py`, which configures the process's environment variables using `os.environ`.

## 2. Generated artifacts

*   **`dw_init_environment.py`**:
    *   **Role**: This Python script is the direct translation of the original KornShell script. Its primary function is to set and export a predefined set of environment variables (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_IMP_*`, `GEN_HOME`, `DW_DIR_CUSTOMER`, `DW_HOST_CUSTOMER`, `ORACLE_HOME`) within the Python process's environment. It is intended to be executed as an initial step in any Python-based data pipeline or orchestration task that requires these environment settings.

## 3. Key design decisions

*   **KornShell to Python Translation**: The core logic was translated from KornShell to Python to align with the target cloud-native, Python-based data platform. This allows for better integration with modern orchestration frameworks and leverages Python's standard library for environment management.
*   **`os.environ` for Environment Variables**: Instead of shell `export` commands, Python's `os.environ` dictionary is used to set environment variables. This ensures that the variables are available to the Python process and any child processes it spawns.
*   **`ORACLE_HOME` Handling**: The original `ORACLE_HOME` detection logic, which relied on local filesystem checks, was directly translated using `os.path.isdir`. However, a critical note has been added to re-evaluate its relevance and replace it with cloud-appropriate mechanisms (e.g., configuration from Secret Manager, explicit environment variables) if Oracle connectivity is still required. If Oracle is no longer an upstream source, this logic can be removed.
*   **Handling of Sourced Scripts (`.dw_global`, `.dw_lokal`)**: These scripts were explicitly *not* directly translated due to unknown content. The design decision was to flag them for manual analysis and integration, either by absorbing their configuration into `dw_init_environment.py` or by creating separate Python modules/configuration files.
*   **`umask` Omission**: The `umask` setting, being shell-specific for file permissions, has been omitted from the Python script. The design assumes that file permissions in the cloud environment will be managed by cloud-native mechanisms (e.g., IAM roles, GCS bucket policies) rather than process-level `umask`.
*   **Correction of Typo**: A typo in the original script (`DW_DIR_IMP_MP_ZM` assigned, `DW_DIR_IMP_MP_TS` exported) was identified and corrected in the Python script, assuming the intent was to set and export `DW_DIR_IMP_MP_ZM`.
*   **Default `HOME` for Cloud**: The `HOME` environment variable, if not set, defaults to `/app` in the Python script. This is a common practice for containerized applications in cloud environments.

## 4. Manual steps before go-live

The following manual steps are required to ensure a successful go-live:

1.  **Analyze and Integrate Sourced Scripts**:
    *   Retrieve and thoroughly analyze the content of `$HOME/.dw_global` and `$HOME/.dw_lokal` from the legacy system.
    *   Based on their content, integrate their functionality into the `dw_init_environment.py` script, create separate Python configuration modules, or define them as environment variables in the target orchestration.
2.  **Define `DW_DIR_CUSTOMER`**:
    *   The `DW_DIR_CUSTOMER` variable is currently set to `os.getenv('DW_DIR_CUSTOMER', '<REPLACE_ME_CUSTOMER_LOGIN>')`.
    *   Determine the correct value for this variable and configure it securely in the target environment (e.g., via Google Secret Manager, as an environment variable in Cloud Composer/Airflow, or in a dedicated configuration file).
3.  **Re-evaluate `ORACLE_HOME` Relevance and Configuration**:
    *   Confirm if Oracle connectivity is still required for any downstream processes.
    *   If yes, replace the legacy filesystem-based `ORACLE_HOME` detection with a cloud-native approach. This might involve:
        *   Setting `ORACLE_HOME` as an explicit environment variable in the orchestration.
        *   Storing Oracle connection details (including `ORACLE_HOME` if still relevant for client libraries) in Google Secret Manager.
        *   Ensuring appropriate Oracle client libraries are available in the execution environment.
    *   If Oracle is no longer needed, remove the `ORACLE_HOME` detection and setting logic from `dw_init_environment.py`.
4.  **Integrate into Orchestration**:
    *   Integrate `dw_init_environment.py` into the chosen orchestration framework (e.g., as a PythonOperator in Airflow, a Cloud Function, or an entrypoint for a Cloud Run service).
    *   Ensure this script is executed *before* any other tasks that rely on the environment variables it sets.
5.  **Manage File Permissions**:
    *   Review and configure file permissions using cloud-native mechanisms (e.g., IAM roles for GCS buckets, service account permissions) to replace the functionality previously provided by `umask`.

## 5. Known gaps & unresolved references

*   **Content of `$HOME/.dw_global` and `$HOME/.dw_lokal`**: The exact content and functionality of these sourced scripts are unknown and require manual analysis and migration. This is the most significant unresolved item.
*   **`DW_DIR_CUSTOMER` Placeholder**: The `<REPLACE_ME_CUSTOMER_LOGIN>` placeholder needs to be replaced with the actual value, sourced from a secure configuration.
*   **`ORACLE_HOME` Relevance and Cloud-Native Configuration**: The necessity of `ORACLE_HOME` in a BigQuery-native environment needs confirmation. If required, the current filesystem-based detection is not suitable for cloud and needs to be replaced with a robust, secure, and cloud-native configuration method.
*   **`umask` Replacement**: The `umask` setting has no direct Python equivalent in a cloud context. Its functionality for managing file permissions must be addressed through cloud-native IAM and storage policies.
*   **Missing Metadata**: The original design document noted missing `file_complexity` and `automation_rate` metadata, which led to assumptions during the migration design.

## 6. Validation

To validate the successful migration and functionality of `dw_init_environment.py`:

1.  **Unit Testing**:
    *   Run `dw_init_environment.py` in an isolated Python environment.
    *   After execution, inspect `os.environ` to confirm that all expected environment variables (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_IMP_*`, `GEN_HOME`, `DW_DIR_CUSTOMER`, `DW_HOST_CUSTOMER`, `ORACLE_HOME` if applicable) are set to their correct values.
    *   Verify that the `ORACLE_HOME` logic correctly handles both cases: when `ORACLE_HOME` is already set, and when it needs to be determined (if this logic is retained).
    *   Ensure the script exits successfully (exit code 0) if all variables are set, and with an error (exit code 1) if critical variables like `ORACLE_HOME` cannot be determined (as per original script behavior).
2.  **Integration Testing (End-to-End)**:
    *   Deploy `dw_init_environment.py` within the target orchestration framework (e.g., a test Airflow DAG).
    *   Execute a simple downstream Python script or BigQuery job that *depends* on one or more of the environment variables set by `dw_init_environment.py`.
    *   **Passing Criteria**:
        *   `dw_init_environment.py` executes without errors.
        *   All environment variables are correctly set and accessible by subsequent tasks in the orchestration.
        *   The downstream process successfully retrieves and uses these variables as expected, completing its execution without errors related to missing or incorrect environment settings.
        *   Specifically, verify the corrected `DW_DIR_IMP_MP_ZM` variable is correctly set.
        *   If Oracle connectivity is still required, ensure the `ORACLE_HOME` (or equivalent connection details) allows successful connection to the Oracle database.

## 7. Rollback procedure

In case of issues after deploying the migrated `dw_init_environment.py`, the following rollback procedure should be followed:

1.  **Orchestration Reversion**: Revert the orchestration configuration (e.g., Airflow DAG, Cloud Function code, Cloud Run service definition) to the last known working version that *does not* include the migrated `dw_init_environment.py` or its dependencies. This typically involves deploying the previous version of the orchestration code.
2.  **Remove Deployed Artifacts**: If `dw_init_environment.py` was deployed as a standalone artifact (e.g., a Cloud Function), remove or disable it.
3.  **Restore Legacy Environment**: Ensure that any processes that were switched to use the new Python environment setup are reverted to their original KornShell-based environment initialization or whatever mechanism was in place before the migration.
4.  **Monitor**: Closely monitor the system to ensure that all processes are functioning correctly with the reverted configuration.
5.  **Root Cause Analysis**: Investigate the reason for the rollback, address the identified issues, and re-plan the migration if necessary.