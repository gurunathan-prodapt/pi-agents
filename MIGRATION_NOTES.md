# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `r_ausd_bp_ta_iccid_vertrag.ksh`, responsible for the initial provisioning of selected base products for BERT and preparing a cutoff-date extraction of the contract cache for "Forderungsscoring", has been migrated.

*   **Source System**: Legacy environment running KornShell scripts.
*   **Target Platform**: Google Cloud Platform (GCP), specifically Cloud Composer (Airflow).
*   **Migration Type**: Replatforming of an orchestration script from KornShell to a Python-based Airflow Directed Acyclic Graph (DAG). The core data processing logic, originally in `k_ausd_bp_ta_iccid_vertrag.ksh`, is a placeholder for future migration to a GCP-native service.

## 2. Generated Artifacts

The migration produced the following artifact:

*   **File**: `dags/r_ausd_bp_ta_iccid_vertrag_dag.py`
    *   **Role**: This Python file defines an Airflow DAG. It serves as the new orchestrator, replacing the original KornShell script. Its responsibilities include:
        *   Parsing and validating input parameters (`stichtag` and `wiederanlaufwert`).
        *   Applying default values for parameters if not provided.
        *   Orchestrating the invocation of the core data processing component (currently a placeholder for the migrated `k_ausd_bp_ta_iccid_vertrag.ksh`).
        *   Leveraging Airflow's native logging and error handling.

## 3. Key Design Decisions

*   **Cloud Composer (Airflow) as Orchestrator**:
    *   **Rationale**: The original script's primary function was orchestration (parameter handling, calling another script, logging). Airflow is a managed, scalable, and robust orchestration service on GCP, making it a direct and powerful replacement for shell-based orchestrators. It provides advanced scheduling, monitoring, dependency management, and integrates well with other GCP services.
    *   **Trade-offs**: Requires re-implementation of shell logic in Python, introducing a new technology stack.
*   **Airflow DAG Parameters for Input**:
    *   **Rationale**: Airflow's `params` mechanism (accessible via `dag_run.conf` for manual triggers or directly from the UI) provides a structured and user-friendly way to pass `Stichtag` and `Wiederanlaufwert`, replacing the `getopts` command-line parsing. This enhances usability and API integration.
    *   **Trade-offs**: Requires careful translation of parameter validation and defaulting logic from KornShell to Python.
*   **Placeholder for Core Processing Logic**:
    *   **Rationale**: The `k_ausd_bp_ta_iccid_vertrag.ksh` script contains the actual data transformation logic and has its own complex migration path (e.g., to BigQuery SQL, Dataflow, or Dataproc). To allow independent development and testing of the orchestrator, a placeholder Python task (`_invoke_core_processing`) was created. This task clearly marks the dependency and outlines the future invocation command.
    *   **Trade-offs**: The DAG is not fully functional for end-to-end data processing until `k_ausd_bp_ta_iccid_vertrag.ksh` is migrated and its invocation task is replaced with a concrete Airflow operator (e.g., `BigQueryOperator`, `DataflowPythonOperator`).
*   **Python Re-implementation of Utility Functions**:
    *   **Rationale**: The original script relied on several KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). To maintain a self-contained and Python-native Airflow DAG, the functionality of these utilities will need to be re-implemented in Python.
    *   **Trade-offs**: Requires manual effort to translate and verify the functional equivalence of these utility functions.
*   **Native Airflow Logging and Error Handling**:
    *   **Rationale**: Leveraging Airflow's built-in logging (integrated with GCP Cloud Logging) and error handling mechanisms (retries, email alerts) provides a standardized, centralized, and robust approach, replacing custom shell-based logging and `trap` statements.
    *   **Trade-offs**: Requires familiarity with Airflow's specific logging and error handling configurations.

## 4. Manual Steps Before Go-Live

Before the migrated DAG can be fully operational in a production environment, the following manual steps are required:

1.  **Cloud Composer Environment Setup**: Ensure a Cloud Composer environment is provisioned, configured, and running in the target GCP project.
2.  **IAM & Permissions**:
    *   Grant the Cloud Composer Service Account (or a dedicated service account if using workload identity) the necessary IAM roles. This includes:
        *   `Composer Worker` or equivalent for basic Airflow operations.
        *   `Logs Writer` for pushing logs to Cloud Logging.
        *   Permissions required by the *future* migrated `k_ausd_bp_ta_iccid_vertrag.ksh` component (e.g., `BigQuery Data Editor`, `Dataflow Admin`, `Cloud Storage Object Admin`, etc.).
3.  **Migrate `k_ausd_bp_ta_iccid_vertrag.ksh`**: The core data processing script `k_ausd_bp_ta_iccid_vertrag.ksh` must be migrated to a GCP-native solution (e.g., BigQuery SQL, Dataflow job, PySpark on Dataproc). This is a critical prerequisite.
4.  **Replace Placeholder Task**: Once `k_ausd_bp_ta_iccid_vertrag.ksh` is migrated, the `_invoke_core_processing` Python task in `r_ausd_bp_ta_iccid_vertrag_dag.py` must be replaced with the appropriate Airflow operator (e.g., `BigQueryOperator`, `DataflowPythonOperator`) that invokes the newly migrated component.
5.  **Re-implement Utility Functions**: Develop and test Python equivalents for the KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). These Python modules should be made available to the Airflow environment (e.g., placed in the `dags` folder or a shared `plugins` folder).
6.  **Configuration Management**:
    *   Determine the actual values for `JobKennung` and `DW_EintragsNr` (currently placeholders in the DAG). Configure these using Airflow Variables, a configuration file, or directly within the DAG code as appropriate.
    *   Identify any critical environment variables or functions sourced from `$HOME/.dw_init` in the original environment. Replicate these as Airflow Variables, environment variables in the Composer environment, or integrate their logic into the DAG's Python code.
7.  **Scheduling**: If the DAG is to be scheduled automatically (not just manually triggered), update the `schedule_interval` parameter in the DAG definition to the desired cron expression or timedelta.

## 5. Known Gaps & Unresolved References

*   **Core Processing Script Migration (`k_ausd_bp_ta_iccid_vertrag.ksh`)**: This is the most significant unresolved item. The current DAG is an orchestrator; the actual data transformation logic is external and requires its own dedicated migration effort and plan. The `_invoke_core_processing` task explicitly serves as a placeholder for this.
*   **KornShell Utility Script Re-implementation**: The full re-implementation and testing of the Python equivalents for `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` are pending. Their functionality must be thoroughly verified.
*   **`JobKennung` and `DW_EintragsNr` Values**: The placeholder values for `job_kennung` and `dw_eintrags_nr` in the `_invoke_core_processing` task need to be replaced with actual, configured values.
*   **`$HOME/.dw_init` Environment Variables**: A detailed analysis of the contents of `$HOME/.dw_init` is required to ensure all necessary environment variables and functions are correctly translated and configured within the Airflow environment or the DAG itself.
*   **Specific Error Messaging**: While Airflow handles general errors, any specific, custom error messages or logging formats provided by `f_alis_msgerr.ksh` might need to be explicitly replicated in the Python code if business requirements dictate.

## 6. Validation

Validation of the migrated DAG involves several stages:

1.  **Unit Testing (Python Code)**:
    *   **Objective**: Verify the correctness of individual Python functions within the DAG.
    *   **Method**: Write unit tests for `_process_parameters` to ensure:
        *   Correct parsing and validation of `stichtag` (DDMMYYYY format).
        *   Correct defaulting of `stichtag` to the system date.
        *   Correct parsing and validation of `wiederanlaufwert` (integer).
        *   Correct defaulting of `wiederanlaufwert` to 0.
        *   Correct pushing of processed parameters to XComs.
        *   Error handling for invalid parameter formats.
    *   **Passing Criteria**: All unit tests pass, demonstrating robust parameter handling.

2.  **Local Airflow Testing**:
    *   **Objective**: Verify the DAG's structure, task dependencies, and basic execution flow.
    *   **Method**:
        *   Use the Airflow CLI (`airflow dags test r_ausd_bp_ta_iccid_vertrag_dag <execution_date>`) to simulate DAG runs.
        *   Test with various `conf` parameters:
            *   `{}` (no parameters, should default)
            *   `{"stichtag": "01012023", "wiederanlaufwert": 10}` (valid parameters)
            *   `{"stichtag": "31122024"}` (only stichtag, wiederanlaufwert should default)
            *   `{"wiederanlaufwert": 5}` (only wiederanlaufwert, stichtag should default)
            *   `{"stichtag": "invalid_date"}` (invalid stichtag format)
            *   `{"wiederanlaufwert": "not_an_int"}` (invalid wiederanlaufwert format)
    *   **Passing Criteria**:
        *   The DAG parses successfully without syntax errors.
        *   Tasks execute in the correct order.
        *   `process_parameters_task` logs show correct parameter processing and defaulting.
        *   `invoke_core_processing_task` logs show the expected "simulated command" with the correct parameters pulled from XComs.
        *   Invalid parameters lead to task failure with informative error messages in the logs.

3.  **Cloud Composer Deployment & Integration Testing**:
    *   **Objective**: Verify the DAG's functionality in a live Cloud Composer environment and, eventually, its integration with the migrated core processing component.
    *   **Method**:
        *   Deploy the DAG to a development Cloud Composer environment.
        *   Trigger the DAG manually via the Airflow UI with the same test cases as local testing.
        *   Monitor task logs in the Airflow UI and Cloud Logging.
        *   **(Post-`k_ausd_bp_ta_iccid_vertrag.ksh` migration)**: Replace the placeholder task with the actual operator for the migrated core processing. Run end-to-end tests, comparing the output data with the original script's output for various `Stichtag` and `Wiederanlaufwert` values.
    *   **Passing Criteria**:
        *   The DAG runs successfully in Cloud Composer.
        *   All tasks complete as expected, with logs confirming correct parameter handling and invocation.
        *   No unexpected errors or warnings in Airflow or Cloud Logging.
        *   **(Post-`k_ausd_bp_ta_iccid_vertrag.ksh` migration)**: The end-to-end execution produces the same data output as the original KornShell script, confirming functional equivalence.

## 7. Rollback Procedure

In case of issues after deployment, follow these steps to revert to the previous state:

1.  **Immediate Rollback (Orchestrator Level)**:
    *   If issues are detected immediately after deploying the Airflow DAG (e.g., DAG parsing errors, immediate task failures unrelated to data processing):
        *   **Disable the Airflow DAG**: In the Airflow UI, set the `r_ausd_bp_ta_iccid_vertrag_dag` to "Off". This prevents any further scheduled or manual runs.
        *   **Re-enable Original Script**: If the original `r_ausd_bp_ta_iccid_vertrag.ksh` script was disabled or decommissioned, re-enable it in the legacy environment.
2.  **Code Rollback**:
    *   Revert the changes made to `dags/r_ausd_bp_ta_iccid_vertrag_dag.py` in the source code repository to the last known stable version.
    *   Redeploy the previous version of the DAG to Cloud Composer.
3.  **Data Integrity Rollback (if core processing was integrated)**:
    *   If the migrated core processing component (replacing `k_ausd_bp_ta_iccid_vertrag.ksh`) has already run and potentially caused data corruption or incorrect data generation in BigQuery or other target systems:
        *   **Identify Affected Data**: Determine the specific BigQuery tables, datasets, or other data stores that were modified by the erroneous run.
        *   **Restore Data**: Restore the affected data from the most recent valid backup or snapshot. If snapshots are not available, a compensating transaction or a data correction script might be necessary.
        *   **Note**: The detailed data rollback procedure is highly dependent on the specific implementation of the migrated `k_ausd_bp_ta_iccid_vertrag.ksh` and the data storage strategy. This should be documented as part of *that* script's migration notes.
4.  **Post-Rollback Analysis**:
    *   Thoroughly investigate the root cause of the failure (e.g., code bug, configuration error, environment issue) before attempting to re-deploy the corrected DAG.