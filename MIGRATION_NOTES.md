# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`, responsible for orchestrating the "Vertragsdatenabgleich" (Contract Data Reconciliation) for the `ta_bp_ref` table, has been migrated.

The migration re-platforms the orchestration logic from a legacy KornShell environment to **Google Cloud Platform (GCP)**, specifically utilizing **Cloud Composer (Apache Airflow)** for workflow management. The core data processing logic, originally in `k_ausd_v_ta_bp_ref.ksh`, is now expected to run on a BigQuery-compatible technology, invoked as a task within the Airflow DAG.

## 2. Generated artifacts

The migration process generated the following file:

*   **`r_ausd_v_ta_bp_ref_dag.py`**
    *   **Role**: This Python file defines an Apache Airflow Directed Acyclic Graph (DAG). It serves as the new orchestrator for the `r_ausd_v_ta_bp_ref` job on Cloud Composer. It handles job initiation, parameter passing, logging, and invokes the core data reconciliation logic (which is expected to be migrated separately).

## 3. Key design decisions

*   **Orchestration Re-platforming**: The original KornShell wrapper's primary role was orchestration (environment setup, parameter parsing, error handling, invoking core logic). This functionality is directly mapped to an **Apache Airflow DAG**. This leverages Airflow's native capabilities for scheduling, task management, logging, monitoring, and robust error handling, aligning with GCP's recommended practices for workflow management.
*   **Logging Centralization**: The legacy `DWMSG_` functions and shell `echo` statements for logging are replaced by Python's standard `logging` module within the Airflow DAG. This automatically integrates with **Cloud Logging**, providing centralized log collection, searchability, and integration with Cloud Monitoring for alerts.
*   **Parameter Management**: Command-line argument parsing (`getopts` for `-s`, `-l`) is replaced by Airflow's DAG `params` and `dag_run.conf`. This allows for flexible parameterization, supporting both default values defined in the DAG and dynamic overrides at runtime.
*   **Environment Configuration**: Sourcing of environment variables (e.g., from `$HOME/.dw_init`, `BERT_DIR_ROOT`) is replaced by **Airflow Variables**. This provides a centralized, manageable, and versionable way to store configuration values within the Airflow environment.
*   **Error Handling & Resilience**: The shell `trap` commands are superseded by Airflow's built-in error handling mechanisms, including task retries (`retries`, `retry_delay`) and `on_failure_callback` (which can be configured for alerting). This provides a more structured and observable failure management system.
*   **Core Logic Invocation (Placeholder)**: The invocation of `k_ausd_v_ta_bp_ref.ksh` is represented by a `BashOperator` in the generated DAG. This is a **placeholder** to allow the wrapper's migration to proceed independently. The final operator (e.g., `BigQueryOperator`, `PythonOperator`, `DataflowTemplateOperator`) will depend on the detailed migration strategy for the core `k_ausd_v_ta_bp_ref.ksh` script.
*   **Unique Job Identifier**: The legacy `DW_EintragsNr` is simulated using a cleaned version of Airflow's `dag_run.run_id`, which is pushed via XCom for use by downstream tasks. This ensures a unique identifier for each execution instance.

**Notable Trade-offs:**
*   **Dependency on Core Logic Migration**: The current DAG is fully functional as an orchestrator, but its ultimate value depends on the successful migration and integration of the `k_ausd_v_ta_bp_ref.ksh` core logic. The `BashOperator` placeholder is a temporary solution.
*   **Loss of Direct Shell Environment**: The direct sourcing of shell scripts (`.dw_init`, utility functions) is replaced by explicit Airflow Variables and Python re-implementations. This requires careful mapping and setup of these configurations in the Airflow environment.
*   **Granularity of Legacy `DWMSG_` Functions**: While general logging is covered, any highly specific or side-effect-rich logic embedded within the original `DWMSG_` functions (e.g., updating a status table in a specific format) would need explicit re-implementation in Python.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **Cloud Composer Environment Setup**:
    *   Ensure a Google Cloud Composer environment (Airflow instance) is provisioned, configured, and running in the target GCP project.
2.  **IAM Permissions**:
    *   The Service Account associated with the Cloud Composer environment must have the necessary IAM roles and permissions. At a minimum, this includes:
        *   `Composer Worker` (for basic Airflow operations).
        *   `Logs Writer` (for writing to Cloud Logging).
        *   Permissions required by the *migrated core logic* (e.g., `BigQuery Data Editor` for `ta_bp_ref` table, `Dataflow Admin`, `Cloud Storage Object Viewer/Creator`, etc., depending on the chosen target for `k_ausd_v_ta_bp_ref.ksh`).
        *   `Airflow Variable Editor` (to manage Airflow Variables).
3.  **Airflow Variables Configuration**:
    *   Create an Airflow Variable named `BERT_DIR_ROOT` in the Airflow UI (Admin -> Variables) or via `gcloud composer` CLI. This variable should hold the base path equivalent to the legacy `BERT_DIR_ROOT` for resolving paths to the core script.
    *   If the core logic requires specific connection strings or secrets, configure them as Airflow Connections (Admin -> Connections) or integrate with Google Secret Manager.
4.  **Core Logic Deployment**:
    *   The migrated core logic (from `k_ausd_v_ta_bp_ref.ksh`) must be deployed to its target GCP service (e.g., a BigQuery stored procedure, a Python script on Cloud Storage/Compute Engine, a Dataflow template).
    *   The `k_ausd_script_path` variable in the DAG (`r_ausd_v_ta_bp_ref_dag.py`) and the `bash_command` within the `execute_core_logic` task must be updated to correctly invoke this deployed core logic.
5.  **DAG Deployment**:
    *   Upload the `r_ausd_v_ta_bp_ref_dag.py` file to the DAGs folder of your Cloud Composer environment's Cloud Storage bucket.
6.  **Scheduling Configuration**:
    *   If the job is to run on a schedule, update the `schedule_interval` parameter in the `r_ausd_v_ta_bp_ref_dag.py` file (e.g., `'0 0 * * *'` for daily at midnight UTC) and re-deploy the DAG. If it's meant for manual or external triggering, `schedule_interval=None` is appropriate.

## 5. Known gaps & unresolved references

*   **Core Script (`k_ausd_v_ta_bp_ref.ksh`) Migration**: This is the most significant unresolved item. The current Airflow DAG only provides a placeholder (`BashOperator`) for invoking the core logic. The detailed migration strategy for `k_ausd_v_ta_bp_ref.ksh` (e.g., to BigQuery SQL, Python with BigQuery client, Dataflow, etc.) is critical and will dictate the final Airflow operator and its parameters.
*   **`ParamList="s:l:"` Usage**: The exact functionality and impact of the `-s` and `-l` parameters within the original `k_ausd_v_ta_bp_ref.ksh` are not fully detailed. While the DAG passes these parameters through, their specific effect on the core reconciliation logic needs to be confirmed to ensure accurate replication.
*   **Full `DWMSG_` Functionality**: The `log_job_start` and `log_job_success` Python functions in the DAG replicate basic logging. However, if the original `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` functions had more complex side effects (e.g., database updates, specific file system interactions beyond logging), these are not replicated and would require further analysis and implementation.
*   **Specific Exit Codes**: The legacy script used specific exit codes (e.g., `193`, `192`, `1`). Airflow tasks primarily succeed or fail. If these specific exit codes were used for distinct downstream actions or alerts, custom logic (e.g., `on_failure_callback` with conditional checks) would be needed to replicate this behavior.
*   **`ta_bp_ref` Table Migration**: The underlying `ta_bp_ref` table, which is the subject of reconciliation, is assumed to be migrated to BigQuery. This is a critical dependency for the core logic.

## 6. Validation

To validate the migrated `r_ausd_v_ta_bp_ref_dag`:

1.  **Deployment**: Deploy the `r_ausd_v_ta_bp_ref_dag.py` file to your Cloud Composer environment's DAGs folder.
2.  **Trigger Execution**:
    *   **Manual Trigger**: Navigate to the Airflow UI, find the `r_ausd_v_ta_bp_ref_dag`, and trigger it manually.
    *   **With Parameters**: If testing `-s` or `-l` parameters, use the "Trigger DAG w/ config" option in the Airflow UI and provide a JSON payload like `{"param_s": "value_s", "param_l": "value_l"}`.
    *   **Scheduled Trigger**: If a `schedule_interval` is set, wait for the next scheduled run.
3.  **Monitor in Airflow UI**: Observe the DAG run in the Airflow UI. All tasks should transition to a "success" state (green).
4.  **Check Cloud Logging**:
    *   Navigate to Cloud Logging in the GCP Console.
    *   Filter logs by `resource.type="cloud_composer_environment"` and `logName="projects/<PROJECT_ID>/logs/airflow-tasks"`.
    *   Verify that logs for `r_ausd_v_ta_bp_ref_dag` appear.
    *   Look for messages indicating:
        *   `Job 'BERT_V_TA_BP_REF' started.`
        *   `DW_EintragsNr for this run: <generated_id>`
        *   `Invoking core logic script: <path_to_k_ausd_script>` (from the `execute_core_logic` task)
        *   `Core logic execution completed (placeholder).`
        *   `Job 'BERT_V_TA_BP_REF' completed successfully.`
5.  **XCom Verification**: (Optional, for advanced debugging) In the Airflow UI, for the `start_job_logging` task, check the XComs tab to ensure `dw_eintrags_nr` was pushed correctly.

**"Passing" means:**
*   The `r_ausd_v_ta_bp_ref_dag` completes successfully in the Airflow UI (all tasks are green).
*   The Cloud Logging output confirms the job started, generated a `DW_EintragsNr`, attempted to invoke the core logic, and completed successfully.
*   (Once `k_ausd_v_ta_bp_ref.ksh` is fully migrated and integrated) The core data reconciliation logic executes without errors and produces the expected data transformations or updates in BigQuery.

## 7. Rollback procedure

In case of issues with the migrated Airflow DAG, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Disable Airflow DAG**: In the Airflow UI, toggle the `r_ausd_v_ta_bp_ref_dag` to "Off" to prevent any further scheduled or manual runs.
2.  **Revert to Legacy System**:
    *   **Re-enable Legacy Script**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh` script in the legacy environment. Ensure its original scheduling mechanism is reactivated.
3.  **Data Rollback (if applicable)**:
    *   If the migrated core logic (`k_ausd_v_ta_bp_ref.ksh`'s GCP equivalent) made any irreversible data changes to `ta_bp_ref` or related tables, a data rollback or recovery procedure might be necessary. This depends heavily on the nature of the core logic and should be part of its specific migration plan.
4.  **Investigation**:
    *   Analyze the Cloud Logging output and Airflow task logs to identify the root cause of the failure in the migrated DAG. Rectify the issues in the `r_ausd_v_ta_bp_ref_dag.py` or its dependencies before attempting re-deployment.