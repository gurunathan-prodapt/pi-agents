# MIGRATION_NOTES.md — DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG`, responsible for orchestrating various 'Bert' related productive processes (including monthly job plans, administrative checks, housekeeping, daily/monthly APT exports, and master data processing), has been migrated from the legacy UC4/Automic platform to Google Cloud Platform. The target platform for orchestration is Apache Airflow running on Cloud Composer, with underlying data processing expected to leverage Google BigQuery and Python.

## 2. Generated Artifacts

The migration process generated the following file:

*   **`dags/bert_ablaufsteuerung_dag.py`**
    *   **Role**: This Python file defines the Apache Airflow Directed Acyclic Graph (DAG) that replaces the `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler. It encapsulates the overall workflow, defines placeholder tasks for each child Job Plan (JOBP) and Event (EVNT) previously managed by UC4, and establishes their initial dependencies. This DAG will be deployed to a Cloud Composer environment to manage the scheduling and execution of the 'Bert' processes.

## 3. Key Design Decisions

*   **Single DAG for Centralized Orchestration**: The `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler, which acts as a central orchestrator for multiple 'Bert' related processes, has been translated into a single Airflow DAG (`bert_ablaufsteuerung_dag.py`).
    *   **Rationale**: This approach maintains the centralized control and overview of the 'Bert' ecosystem, mirroring the original UC4 design. Airflow's robust scheduling and dependency management capabilities make it a suitable replacement for UC4's job scheduling functionality. Leveraging Cloud Composer aligns with cloud-native best practices, offering scalability, reliability, and reduced operational overhead.
*   **Phased Implementation with Placeholders**: The initial generated DAG utilizes `DummyOperator` tasks for each referenced UC4 JOBP/EVNT.
    *   **Rationale**: This allows for the rapid establishment of the overall workflow structure and dependencies in Airflow (Phase 1 of the Build Plan) while deferring the detailed migration and implementation of the individual child processes (e.g., `DW.BERT_MONATLICH_JP`'s actual logic) to subsequent phases (P2/P3). This modular approach manages complexity and allows for parallel development streams.
    *   **Trade-off**: The generated DAG is not yet fully functional as the actual business logic for the child tasks is pending. Significant follow-up work is required to replace `DummyOperator`s with concrete Airflow operators (e.g., `BigQueryOperator`, `PythonOperator`, `BashOperator`) that execute the migrated BigQuery SQL or Python scripts.
*   **Parallel Task Execution (Initial Assumption)**: The generated DAG initially sets up the child tasks to run in parallel from a `start_task`.
    *   **Rationale**: Based on the provided UC4 XML, explicit sequential dependencies *between* the child JOBPs/EVNTs within the `DW.BERT_ABLAUFSTEUERUNG` scheduler were not immediately apparent, suggesting they are primarily scheduled independently by the parent.
    *   **Trade-off**: If hidden or implicit sequential dependencies exist between these child processes that were not captured in the UC4 XML, they will need to be identified during the detailed analysis of the child JOBPs/EVNTs and explicitly added to the Airflow DAG.
*   **Custom Logic for UC4 Calendars and Synchronization**: The migration design anticipates the need for custom Python logic within the Airflow DAG to replicate UC4's specific calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`) and synchronization object (`DW.BERT_ABLAUFSTEUERUNG_SYNC`).
    *   **Rationale**: Airflow's native scheduling (`schedule_interval`) might not directly support the nuanced conditions of UC4 calendars (e.g., "DAY_OF_MONTH_25", "DAY_OF_MONTH_05", or exclusion days). Similarly, UC4 synchronization objects often imply resource locking or cross-process coordination that requires specific Airflow mechanisms like Pools, `ExternalTaskSensor`, or custom Python-based locking.
    *   **Trade-off**: Implementing custom logic adds complexity to the DAG definition and requires thorough testing to ensure accurate replication of the legacy behavior.

## 4. Manual Steps Before Go-Live

Before the `bert_ablaufsteuerung_dag` can be fully operational in a production environment, the following manual steps and configurations are required:

1.  **Cloud Composer Environment Setup**:
    *   Ensure a Google Cloud Composer environment is provisioned and properly configured.
    *   Verify network connectivity and resource availability for the Airflow workers.
2.  **IAM Permissions**:
    *   The service account associated with the Cloud Composer environment must have the necessary IAM roles and permissions to:
        *   Deploy DAGs to the Composer's DAGs GCS bucket.
        *   Execute BigQuery jobs (e.g., `BigQuery Data Editor`, `BigQuery Job User`).
        *   Access Google Cloud Storage buckets (e.g., `Storage Object Viewer`, `Storage Object Creator`) if intermediate files are used.
        *   Interact with any other GCP services or external systems that the child tasks will utilize.
3.  **BigQuery Datasets and Tables**:
    *   Create all necessary BigQuery datasets and tables that the migrated child processes (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`) will read from or write to.
    *   Ensure that table schemas are correctly defined and match the expected data structures.
4.  **Airflow Connections and Secrets**:
    *   If any child tasks require connections to external databases, APIs, or other systems, configure these as Airflow Connections.
    *   Store sensitive credentials (passwords, API keys) securely using Google Secret Manager and integrate them with Airflow Connections.
5.  **Airflow Variables**:
    *   If any UC4 variables or prompt sets are identified during the detailed analysis of the child JOBPs/EVNTs, these must be configured as Airflow Variables in the Composer environment.
6.  **Refine Scheduling and Calendar Logic**:
    *   The `schedule_interval` in the generated DAG is currently `@daily`. This needs to be refined based on the detailed analysis of UC4's `Period` and the specific calendar conditions (`DW.NEW_CALENDAR`, `DW.KALENDER`).
    *   Implement the custom Python logic (e.g., using `PythonSensor` or `BranchPythonOperator`) to accurately replicate the UC4 calendar-based triggering for tasks like `bert_monthly_jp_task` and `dwh_run_apt_export_monthly_evt_task`.
7.  **Implement Synchronization Logic**:
    *   Based on the detailed function of `DW.BERT_ABLAUFSTEUERUNG_SYNC`, implement the corresponding Airflow mechanism (e.g., Airflow Pools for resource contention, `ExternalTaskSensor` for cross-DAG dependencies, or custom Python locking) to ensure correct synchronization behavior.
8.  **Child Task Implementation**:
    *   **Crucially**, the `DummyOperator` tasks in `bert_ablaufsteuerung_dag.py` must be replaced with actual Airflow operators that execute the migrated business logic for each UC4 JOBP/EVNT. This involves:
        *   Migrating the underlying logic of each `JOBP`/`EVNT` to BigQuery SQL, Python scripts, or other GCP services.
        *   Integrating these migrated components into the Airflow tasks (e.g., `BigQueryOperator`, `PythonOperator` calling a Python script, `BashOperator` for shell commands).

## 5. Known Gaps & Unresolved References

The following items have been identified as gaps, risks, or require further follow-up and detailed design (B4 items):

*   **Child Job Plan (JOBP) and Event (EVNT) Details (B4)**: The most significant gap is the lack of detailed information regarding the internal logic and dependencies *within* the child UC4 objects (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`). The generated DAG uses `DummyOperator`s as placeholders. Each of these child components requires its own detailed migration design and implementation.
*   **UC4 Variables/PromptSets**: While the provided XML for the scheduler did not show populated variables, if the child JOBPs/EVNTs utilize UC4 variables or prompt sets, these will need to be identified and translated into Airflow Variables or Jinja templating within the Airflow tasks.
*   **UC4 Calendar System Implementation**: The specific logic for `DW.NEW_CALENDAR` (e.g., `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`) and `DW.KALENDER` (e.g., excluding `BERT_NICHT` days) is not yet implemented in the generated DAG. This requires custom Python logic (e.g., `PythonSensor` or `BranchPythonOperator`) to be developed and integrated.
*   **UC4 Synchronization Object (`DW.BERT_ABLAUFSTEUERUNG_SYNC`)**: The exact functional behavior of this synchronization object needs to be fully understood to determine the appropriate Airflow mechanism (e.g., Airflow Pools, `ExternalTaskSensor`, or custom Python locking) for its replication.
*   **Error Handling and Restartability**: UC4's specific error handling and restart mechanisms (e.g., `RElseHalt`, `RElseIgn`) need to be thoroughly analyzed and replicated in Airflow using features like `retries`, `retry_delay`, `on_failure_callback` functions, and potentially custom error handling logic within tasks.
*   **Performance Tuning**: The `Ert` (Estimated Run Time) of the original UC4 scheduler is noted as `21317` seconds. Performance will be a critical factor, and the migrated BigQuery queries and Airflow tasks will require tuning to ensure similar or improved execution times and meet SLAs.
*   **Missing Metadata**: The absence of `file_complexity` and `automation_rate` metadata for the source UC4 file made precise effort estimation and risk assessment more challenging.

## 6. Validation

Validation of the migrated `DW.BERT_ABLAUFSTEUERUNG` workflow will involve several stages:

1.  **DAG Syntax and Parsing**:
    *   Upload `dags/bert_ablaufsteuerung_dag.py` to the Cloud Composer DAGs folder. Airflow's scheduler will attempt to parse it. Any syntax errors will be reported in the Airflow UI or logs.
2.  **Unit Testing (for custom logic)**:
    *   Any custom Python functions developed for calendar logic, synchronization, or specific task operations should have dedicated unit tests to ensure their correctness.
3.  **Integration Testing (Staging Environment)**:
    *   **Triggering**: Manually trigger the `bert_ablaufsteuerung_dag` in a dedicated staging Cloud Composer environment.
    *   **Task Execution**: Verify that all tasks within the DAG execute successfully without errors. Monitor task logs for any warnings, unexpected behavior, or resource contention.
    *   **Scheduling Logic**: Confirm that the `schedule_interval` and any implemented custom calendar logic correctly trigger or skip tasks according to the original UC4 schedule (e.g., monthly tasks run on the correct days, daily tasks run daily).
    *   **Synchronization Logic**: If `DW.BERT_ABLAUFSTEUERUNG_SYNC` has been implemented, verify its behavior (e.g., resource locking, cross-DAG dependencies) functions as expected.
    *   **"Passing" means**:
        *   The DAG completes successfully without any failed tasks.
        *   All tasks are triggered and executed according to their defined schedules and dependencies.
        *   The output of the child tasks (e.g., data written to BigQuery tables, files in GCS) matches the expected results from the legacy UC4 processes for the same input data and execution context. This requires a thorough data comparison.
        *   Performance metrics (task duration, overall DAG run time) are within acceptable limits, ideally matching or improving upon legacy performance.
4.  **Functional and Data Validation**:
    *   Perform a comprehensive comparison of the data outputs generated by the migrated Airflow DAG with those produced by the legacy UC4 processes for a representative period. This is the ultimate measure of functional correctness and data integrity.
5.  **Performance Validation**:
    *   Monitor the execution times of individual tasks and the overall DAG run time in Airflow. Compare these against the `Ert` of the legacy UC4 scheduler and established SLAs to ensure performance requirements are met.

## 7. Rollback Procedure

In the event of critical issues or unforeseen problems after deployment, the following rollback procedure should be followed:

1.  **Immediate Rollback (Airflow)**:
    *   If critical issues (e.g., DAG parsing failures, consistent task failures, incorrect data generation) are detected immediately after deployment or during initial testing in production:
        *   **Disable the Airflow DAG**: In the Airflow UI, disable the `bert_ablaufsteuerung_dag` to prevent further executions.
        *   **Re-enable UC4**: Immediately re-enable the original `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler in the legacy environment to resume normal operations.
2.  **Data Rollback (if applicable)**:
    *   If the migrated processes have modified data in BigQuery or other systems, and these modifications are incorrect or corrupted:
        *   Utilize BigQuery's point-in-time recovery or table snapshot capabilities to restore affected tables to their state before the problematic Airflow DAG run.
        *   Restore any affected files in GCS from backups if necessary.
3.  **Full Rollback**:
    *   If, after extended testing and attempts to resolve issues, the migration of `DW.BERT_ABLAUFSTEUERUNG` proves unfeasible or too problematic:
        *   The migration effort for this specific component will be halted.
        *   The legacy `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler will remain the system of record and continue to operate indefinitely.
        *   The deployed Airflow DAG and any associated GCP resources can be decommissioned or retained for future re-evaluation and redesign.