# MIGRATION_NOTES.md

## 1. Summary

The UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG` has been migrated to Google Cloud Platform (GCP). This job, which acts as a master orchestrator for various productive data processing workflows related to "Bert", including monthly/daily exports, master data processing, and administrative tasks, has been re-implemented as an Apache Airflow DAG. The target platform is a BigQuery-centric environment, with Airflow on Cloud Composer serving as the orchestration engine. The migration ensures functional equivalence of the scheduling and orchestration logic.

## 2. Generated Artifacts

The migration process generated the following primary artifact:

*   **`dw_bert_ablaufsteuerung.py`**: This Python file defines the Apache Airflow DAG. It encapsulates the orchestration logic, including triggering child DAGs, implementing time-based waits, and applying calendar-based conditions, replicating the behavior of the original UC4 Job Scheduler.

## 3. Key Design Decisions

Several key design decisions were made to ensure a robust and functionally equivalent migration:

*   **Apache Airflow as Orchestrator**: Airflow on Cloud Composer was chosen as the target orchestration platform due to its native integration with GCP services, Python-based DAG definitions, and advanced scheduling capabilities, providing a modern and scalable replacement for UC4.
*   **`TriggerDagRunOperator` for Child Workflows**: Each child UC4 Job Plan (JOBP) or Event (EVNT) is represented as an independent Airflow DAG. The master `dw_bert_ablaufsteuerung` DAG uses `TriggerDagRunOperator` to invoke these child DAGs, promoting modularity, reusability, and independent migration of sub-workflows.
*   **`max_active_runs=1` and `guard_active_run` for Concurrency Control**: To replicate UC4's `SYNCREF Else="Skip"` behavior, the DAG is configured with `max_active_runs=1`. Additionally, a `PythonOperator` named `guard_active_run` is implemented at the start of the DAG to explicitly check for active runs and skip the current run if another is in progress, preventing concurrent executions.
*   **`TimeSensor` for Earliest Start Times**: UC4's `ErlstStTime` constraints (e.g., "earliest start at 20:00") are translated into Airflow's `TimeSensor` tasks. This ensures that dependent tasks do not commence before their designated times, maintaining the original scheduling precision.
*   **Custom Python Logic for Calendar-Based Gating**: UC4's complex calendar dependencies (`DW.NEW_CALENDAR`, `DW.KALENDER`) are implemented using `ShortCircuitOperator` or `BranchPythonOperator` with custom Python functions. This approach provides the flexibility to accurately replicate specific calendar rules that are not directly supported by Airflow's standard scheduling mechanisms.
*   **`wait_for_completion` Parameter in `TriggerDagRunOperator`**: The `ActFlg` attribute in UC4 tasks (indicating whether to wait for completion or fire-and-forget) is directly mapped to the `wait_for_completion` parameter of the `TriggerDagRunOperator`, ensuring correct synchronous or asynchronous triggering of child DAGs.

## 4. Manual Steps Before Go-Live

Before the `dw_bert_ablaufsteuerung` DAG can be deployed to production, the following manual steps are required:

1.  **Calendar Logic Implementation**: The specific definitions for UC4 calendars `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05) and `DW.KALENDER` (BERT_NICHT) are not provided in the source XML. Manual investigation is required to understand their exact logic, and this logic must be implemented within the Python callable functions for `calendar_check_task_1` and `calendar_check_task_6` in the `dw_bert_ablaufsteuerung.py` file.
2.  **Child DAGs Deployment**: All child DAGs triggered by `dw_bert_ablaufsteuerung` (e.g., `dw_bert_monatlich_jp`, `dw_bert_run_adm_check_jp_evt`, etc.) must be fully migrated, deployed, and validated in the target Airflow environment *prior* to the go-live of this master orchestrator DAG.
3.  **Airflow Environment Setup**:
    *   Ensure the Cloud Composer environment is provisioned, configured, and operational.
    *   Verify that all necessary Python packages (e.g., `apache-airflow-providers-google`) are installed in the Composer environment.
4.  **IAM/Permissions Configuration**:
    *   The Airflow service account (associated with the Cloud Composer environment) must be granted appropriate IAM roles and permissions. This includes, but is not limited to:
        *   `Airflow Triggerer` role on the specific child DAGs to allow `TriggerDagRunOperator` to function.
        *   Permissions to write logs to Cloud Logging.
        *   Any other GCP resource permissions required by the underlying tasks within the child DAGs (e.g., BigQuery Data Editor, Dataproc Worker, GCS Object Admin).
5.  **Connection Strings/Secrets**: While this orchestrator DAG itself does not typically require external connections, ensure that any connections or secrets required by the *child DAGs* are securely configured in Airflow (e.g., BigQuery connections, GCS connections, external database connections).
6.  **Scheduling Configuration**:
    *   Confirm that the `schedule` parameter (`0 0 * * *` for daily at midnight UTC) in the DAG definition aligns with the desired production schedule.
    *   Set the `start_date` parameter in the DAG definition to an appropriate historical or current date for production deployment.
7.  **Placeholder Replacement**: Replace all placeholder values in `dw_bert_ablaufsteuerung.py` such as `{{ placeholder_start_date }}` and any GCP-specific configuration placeholders (e.g., project IDs, regions, bucket names) with actual production values.

## 5. Known Gaps & Unresolved References

The following items have been identified as known gaps or require further attention:

*   **Calendar Definitions (High Risk)**: The precise logic for `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05) and `DW.KALENDER` (BERT_NICHT) is not explicitly defined in the source UC4 XML. This requires manual investigation and implementation in the Airflow DAG's Python logic. This is a critical item for follow-up.
*   **Child Job/Event Migration (External Dependency Risk)**: The successful operation of `dw_bert_ablaufsteuerung` is entirely dependent on the prior and successful migration and deployment of all its child UC4 JOBP/EVNT objects into their respective Airflow DAGs. Any issues or delays in child DAG migrations will directly impact this orchestrator.
*   **"Earliest Start Time" Precision (Medium Risk)**: While `TimeSensor` is used for `ErlstStTime` (e.g., 04:03), the precise adherence to non-standard minute offsets in a distributed Airflow environment requires careful monitoring and testing to ensure reliability.
*   **UC4 Sync `Else=Skip` Implementation Robustness**: The combination of `max_active_runs=1` and the `guard_active_run` PythonOperator addresses the UC4 `Else=Skip` behavior. However, thorough testing is needed to ensure its robustness in all edge cases, such as DAG runs failing after the guard task but before completion.
*   **GCP Configuration Placeholders**: All GCP project IDs, Dataproc regions, cluster names, and GCS bucket names mentioned in the design are placeholders and require manual configuration during deployment.

## 6. Validation

Validation of the migrated `dw_bert_ablaufsteuerung` DAG involves several stages:

### How to Run Tests:

1.  **Local Airflow Environment**:
    *   Deploy `dw_bert_ablaufsteuerung.py` to a local Airflow instance (e.g., using `airflow standalone` or Docker Compose).
    *   Perform basic syntax checks and verify task dependencies using `airflow dags list` and `airflow dags test`.
2.  **Cloud Composer Staging Environment**:
    *   Deploy the DAG to a non-production Cloud Composer environment.
    *   **Manual Triggering**: Manually trigger the DAG multiple times to test the `guard_active_run` task's behavior (it should skip subsequent runs).
    *   **Calendar Logic Testing**: Manually trigger the DAG with different `execution_date` values (e.g., dates corresponding to DAY_OF_MONTH_25, DAY_OF_MONTH_05, or BERT_NICHT calendar days) to verify that `calendar_check_task_1` and `calendar_check_task_6` correctly branch or skip tasks.
    *   **Time Sensor Verification**: Observe the `TimeSensor` tasks (`time_sensor_task_1`, `time_sensor_task_3`, `time_sensor_task_5`) to ensure they correctly pause execution until the specified target times.
    *   **Child DAG Triggering**: Confirm that `TriggerDagRunOperator` tasks successfully trigger their respective child DAGs. This requires the child DAGs to also be deployed and functional in the staging environment.
    *   **`wait_for_completion` Check**: Verify that tasks with `wait_for_completion=True` wait for the child DAGs to complete, while tasks with `wait_for_completion=False` proceed immediately.
    *   **Log Monitoring**: Continuously monitor Airflow task logs and Cloud Logging for any errors, warnings, or unexpected behavior during execution.

### What "Passing" Means:

A successful validation indicates the following:

*   The `dw_bert_ablaufsteuerung` DAG completes successfully without any failed tasks.
*   The `guard_active_run` task correctly prevents concurrent DAG runs by skipping subsequent triggers when an active run is detected.
*   `TimeSensor` tasks accurately enforce earliest start times, pausing execution until the specified time.
*   `calendar_check_task_1` and `calendar_check_task_6` correctly evaluate the custom calendar logic and branch/skip downstream tasks as per the original UC4 behavior for various dates.
*   All `TriggerDagRunOperator` tasks successfully initiate their corresponding child DAGs.
*   The `wait_for_completion` parameter is correctly honored: tasks configured to wait do so, and fire-and-forget tasks proceed without waiting.
*   The overall task execution flow and dependencies within the Airflow DAG precisely match the logical sequence and conditions defined in the original UC4 Job Scheduler.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Pause of Airflow DAG**:
    *   Access the Airflow UI for the production Cloud Composer environment.
    *   Locate the `dw_bert_ablaufsteuerung` DAG and immediately toggle its status to "Paused". This will prevent any new runs from being scheduled.
2.  **Delete Airflow DAG**:
    *   Remove the `dw_bert_ablaufsteuerung.py` file from the Cloud Composer DAGs folder (typically a GCS bucket). This will remove the DAG from the Airflow UI and scheduler.
3.  **Re-enable Original UC4 Job**:
    *   Access the UC4/Automic Automation Engine.
    *   Locate the original `DW.BERT_ABLAUFSTEUERUNG` job scheduler.
    *   Re-activate the UC4 job to resume its normal operation.
4.  **Verify UC4 Operation**:
    *   Monitor the UC4 environment to confirm that `DW.BERT_ABLAUFSTEUERUNG` is running as expected and correctly orchestrating its child processes.
5.  **Child DAGs Consideration**:
    *   If any child DAGs triggered by `dw_bert_ablaufsteuerung` were also migrated and are running in Airflow, assess their status. Depending on the nature of the issue and the dependencies, these child DAGs may also need to be paused or rolled back to their UC4 counterparts.
6.  **Data Consistency Check**:
    *   Review any data processed or state changes made by partially completed Airflow runs of `dw_bert_ablaufsteuerung` or its child DAGs. Ensure that data consistency is maintained and that the UC4 system can safely reprocess or continue from a consistent state.
7.  **Communication**:
    *   Inform all relevant stakeholders (e.g., data consumers, operations teams) about the rollback and the return to the legacy UC4 system.

This procedure ensures a swift return to the stable, legacy system while allowing time for investigation and resolution of the issues encountered in the Airflow environment.