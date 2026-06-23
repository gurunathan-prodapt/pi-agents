# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_bp_ta_bpr_apn.ksh`. The original script was responsible for parsing command-line arguments, setting up the execution environment, and invoking a core processing script (`k_ausd_bp_ta_bpr_apn.ksh`) for the initial provisioning of selected base products for the BERT system.

The script has been migrated to an **Apache Airflow DAG written in Python**. This DAG now handles the orchestration, parameter management, and environment setup, serving as the entry point for the BERT provisioning workflow on the new cloud platform. The core data processing logic, originally within `k_ausd_bp_ta_bpr_apn.ksh`, is currently represented by a placeholder task within this DAG and requires a separate, subsequent migration effort.

**Target Platform:** Google Cloud Platform (GCP) with Apache Airflow for orchestration and Google BigQuery as the primary data processing and storage engine (for the subsequent core logic migration).

## 2. Generated artifacts

The migration produced the following artifact:

*   **`dags/r_ausd_bp_ta_bpr_apn_dag.py`**
    *   **Role:** This Python file defines an Apache Airflow DAG named `r_ausd_bp_ta_bpr_apn_dag`. It replaces the original KornShell script's orchestration functionality.
    *   **Functionality:**
        *   Parses and validates input parameters (`stichtag` and `wiederanlaufwert`) provided via Airflow DAG Run configuration.
        *   Defaults `stichtag` to the current date if not provided, and `wiederanlaufwert` to 0.
        *   Pushes resolved parameters to XCom for use by downstream tasks.
        *   Includes a placeholder `PythonOperator` (`invoke_core_script_task`) that logs the parameters it would pass to the *actual* migrated core processing logic (which is yet to be fully implemented).
        *   Manages logging and error handling using Airflow's native capabilities.

## 3. Key design decisions

*   **Orchestration Layer Migration (KornShell to Airflow/Python):** The primary decision was to move the orchestration logic from KornShell to an Airflow DAG. This leverages Airflow's robust scheduling, monitoring, parameter management, and dependency handling capabilities, aligning with the target cloud architecture.
*   **Parameter Handling via Airflow DAG Params:** Command-line argument parsing (`getopts`) from the original script was replaced by Airflow's native `params` mechanism. This allows for structured input via the Airflow UI or API, including type validation and default values. A `PythonOperator` (`process_parameters_task`) explicitly handles parameter resolution and validation, pushing results to XCom for clear data flow.
*   **Placeholder for Core Processing Logic:** Recognizing that `k_ausd_bp_ta_bpr_apn.ksh` contains the actual data transformation, a `PythonOperator` (`invoke_core_script_task`) was used as a temporary placeholder. This allows the orchestration layer to be migrated and tested independently, deferring the more complex data processing migration to a separate phase. This is a significant trade-off, as the end-to-end functionality is not yet complete.
*   **Replacement of Utility Scripts:** The functionality of sourced KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) and environment initialization (`.dw_init`) has been replaced by native Python functions, Airflow's logging, and Airflow's environment configuration. This avoids porting shell-specific utilities and leverages the Python ecosystem.
*   **Date Handling:** The `Stichtag` (reference date) determination logic, including defaulting to the current system date, is re-implemented using Python's `pendulum` library, providing robust date parsing and formatting.
*   **Error Handling and Logging:** Airflow's built-in logging and task retry mechanisms replace the `DWMSG_*` functions and `trap` statements from the original KornShell script, providing standardized and centralized logging within the Airflow environment.

## 4. Manual steps before go-live

Before the migrated DAG can be fully operational, the following manual steps are required:

1.  **Airflow Environment Setup:**
    *   Ensure the Airflow environment is running and accessible.
    *   Verify that the necessary Python packages (e.g., `pendulum`) are installed in the Airflow environment.
2.  **DAG Deployment:**
    *   Place the `dags/r_ausd_bp_ta_bpr_apn_dag.py` file into the designated Airflow DAGs folder.
    *   Confirm that the DAG appears in the Airflow UI and is unpaused.
3.  **Configuration of `JobKennung` and `DW_EintragsNr`:**
    *   The original script passed `JobKennung` and `DW_EintragsNr` to the core script. These values are currently placeholders in the migrated DAG's `_invoke_core_processing` function.
    *   **Action:** Determine the appropriate source for these values (e.g., Airflow Variables, Airflow Connections, or derived from the Airflow context) and update the `_invoke_core_processing` function accordingly once the core script is migrated.
4.  **BigQuery Permissions (for future core script migration):**
    *   Once `k_ausd_bp_ta_bpr_apn.ksh` is migrated to BigQuery, ensure that the Airflow service account has the necessary IAM roles and permissions to read from source tables and write to target tables in BigQuery.
5.  **Scheduling:**
    *   Define the appropriate schedule for the `r_ausd_bp_ta_bpr_apn_dag` in Airflow, based on the business requirements (e.g., daily, weekly). The current DAG is set to `schedule=None` for manual triggering.

## 5. Known gaps & unresolved references

*   **Core Processing Logic (`k_ausd_bp_ta_bpr_apn.ksh`):** This is the most significant gap. The actual data extraction, transformation, and loading logic from `k_ausd_bp_ta_bpr_apn.ksh` has *not* been migrated. The `invoke_core_script_task` in the DAG is a placeholder. A separate, detailed migration design and implementation for `k_ausd_bp_ta_bpr_apn.ksh` (likely involving BigQuery SQL and/or Python) is required to complete the end-to-end workflow.
*   **Commented-out `maxladedatum` Logic:** The original script contained commented-out logic to determine `p_stichtag` based on `MIN(sysdate,maxladedatum)` from `DWH$TA_C_VERTRAG`. It is unclear if this functionality is still required.
    *   **Action:** Clarify with business stakeholders if this logic is needed. If so, it must be re-implemented in BigQuery SQL and integrated into the `process_parameters_task` or a preceding task.
*   **`JobKennung` and `DW_EintragsNr` Values:** The specific values or derivation logic for these parameters, which were passed to the original core script, are not yet defined in the Airflow DAG.
    *   **Action:** These need to be determined and configured within the Airflow environment (e.g., as Airflow Variables or derived from Airflow context) when the core script is migrated.
*   **Missing Complexity Analysis:** The absence of `file_complexity` data for the original script means there might be hidden complexities or edge cases that were not fully captured during the semi-automated migration.
*   **German Language Interpretation:** While efforts were made to correctly interpret the German comments and variable names, a final review by a German-speaking domain expert is recommended to ensure no business logic was misinterpreted.

## 6. Validation

To validate the migrated `r_ausd_bp_ta_bpr_apn_dag`:

1.  **Trigger the DAG:**
    *   Navigate to the Airflow UI.
    *   Find the `r_ausd_bp_ta_bpr_apn_dag` and unpause it.
    *   Manually trigger a DAG run.
    *   **Test with parameters:**
        *   Trigger a run *without* specifying `stichtag` or `wiederanlaufwert` to verify default behavior (current date for `stichtag`, 0 for `wiederanlaufwert`).
        *   Trigger a run *with* valid `stichtag` (e.g., `2023-10-26`) and `wiederanlaufwert` (e.g., `1`).
        *   Trigger a run *with* invalid `stichtag` (e.g., `invalid-date`) or `wiederanlaufwert` (e.g., `abc`) to observe error handling and defaulting.
2.  **Monitor DAG Run:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks (`process_parameters_task`, `invoke_core_script_task`) complete successfully (green status).
3.  **Review Task Logs:**
    *   Access the logs for `process_parameters_task`:
        *   Verify that `Resolved Stichtag` and `Resolved Wiederanlaufwert` match the expected values (defaults or provided parameters).
        *   Check for any validation warnings or errors if invalid parameters were provided.
    *   Access the logs for `invoke_core_script_task`:
        *   Verify that the logged parameters for the core script (`Stichtag (s)`, `Wiederanlaufwert (l)`) correctly reflect the values resolved by `process_parameters_task`.
        *   Confirm that the placeholder message "Core processing invocation complete (placeholder)." is present.

**"Passing" Criteria:**
A successful validation means:
*   The Airflow DAG runs to completion without any task failures.
*   The `process_parameters_task` correctly resolves and logs the `stichtag` and `wiederanlaufwert` based on input or defaults.
*   The `invoke_core_script_task` correctly receives and logs these parameters, indicating that the orchestration layer is functioning as expected, ready for the core script migration.
*   No unexpected errors or warnings appear in the logs.

## 7. Rollback procedure

In case of issues with the migrated Airflow DAG, the following rollback procedure can be executed:

1.  **Disable the Airflow DAG:**
    *   In the Airflow UI, locate `r_ausd_bp_ta_bpr_apn_dag` and toggle it to "Off" (paused state). This prevents any further runs of the new DAG.
2.  **Remove the DAG file (Optional but Recommended):**
    *   Delete or move `dags/r_ausd_bp_ta_bpr_apn_dag.py` from the Airflow DAGs folder to prevent it from being re-enabled accidentally.
3.  **Re-enable Original KornShell Script:**
    *   Ensure the original KornShell script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh`) is available and configured in its original scheduling system (e.g., cron, enterprise scheduler).
    *   Re-activate its schedule or manually trigger it as per its original operational procedure.
4.  **Verify Original Script Functionality:**
    *   Monitor the execution of the original KornShell script to confirm it is running as expected and producing the correct outputs.
5.  **Data Integrity Check:**
    *   Since this migration only covers the orchestration layer and the core processing is a placeholder, there should be no data changes directly caused by the new DAG. However, if the core script migration had already begun, ensure that any partially migrated data or tables are reverted or cleaned up to prevent inconsistencies.

This rollback procedure ensures a quick return to the previous operational state while further investigation and remediation of the issues with the Airflow DAG can take place.