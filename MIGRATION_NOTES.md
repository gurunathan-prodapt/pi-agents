# MIGRATION_NOTES.md: DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The `DW.BERT_ABLAUFSTEUERUNG` job, originally a UC4 Job Scheduler (JSCH) responsible for orchestrating various 'Bert' related productive processes (including monthly job plans, administrative checks, housekeeping, daily/monthly APT exports, and master data processing), has been migrated.

The migration target is a **Google Cloud Platform (GCP) native architecture**, specifically utilizing **Cloud Composer (managed Apache Airflow)** for workflow orchestration. The UC4 JSCH's sequential and calendar-driven logic has been translated into an Airflow Directed Acyclic Graph (DAG), with each child UC4 Job Plan (JOBP) and Event (EVNT) represented as an Airflow task or a placeholder for future sub-DAGs.

## 2. Generated Artifacts

The migration process generated the following primary artifact:

*   **`dags/dw_bert_ablaufsteuerung_dag.py`**
    *   **Role**: This Python file defines the main Airflow DAG for `dw_bert_ablaufsteuerung`. It orchestrates the sequence of 'Bert' related processes, mirroring the dependencies and conditional logic (e.g., calendar-based execution) found in the original UC4 JSCH. It includes placeholder `BashOperator` tasks for each child UC4 JOBP/EVNT, which will be replaced with actual business logic (e.g., `BigQueryOperator`, `PythonOperator`, `TriggerDagRunOperator` for sub-DAGs) in subsequent migration phases. It also contains custom Python functions to handle specific calendar-based skipping logic.

## 3. Key Design Decisions

*   **Target Platform: Cloud Composer (Airflow)**:
    *   **Why**: Cloud Composer provides a fully managed Airflow environment, offering scalability, high availability, and seamless integration with other GCP services (e.g., BigQuery, Cloud Storage, Cloud Monitoring). Airflow's programmatic DAG definition allows for robust version control, testing, and complex workflow orchestration, aligning with modern data engineering practices.
    *   **Trade-offs**: Airflow's scheduling and calendar logic, while powerful, differs from UC4's. Translating intricate UC4 calendar rules (especially exclusions) requires careful implementation using Python functions and conditional task skipping, which adds complexity compared to direct UC4 configuration.

*   **UC4 JSCH to Airflow DAG Mapping**:
    *   **Why**: A direct mapping of the UC4 Job Scheduler to a single Airflow DAG preserves the top-level orchestration logic and dependencies. This approach provides a clear, centralized view of the 'Bert' workflow in Airflow, similar to its UC4 counterpart.
    *   **Trade-offs**: The generated DAG currently uses placeholder `BashOperator` tasks for child JOBPs/EVNTs. This means the actual business logic migration for these child components is a subsequent, significant effort. The current DAG focuses solely on the orchestration layer.

*   **Handling UC4 Calendars and `ErlstStTime`**:
    *   **Why**: Custom Python functions (`_check_monthly_run_day`, `_check_bert_nicht_exclusion`) were implemented within the DAG to handle specific calendar-based conditional execution (e.g., running on the 5th/25th of the month, or skipping based on `BERT_NICHT` exclusion). This allows for flexible and precise control over task execution based on date logic. `ErlstStTime` values are primarily managed through task dependencies and the overall daily `schedule_interval`, with the understanding that precise absolute time triggering might require further refinement (e.g., `TimeSensor` tasks) if strict adherence is critical.
    *   **Trade-offs**: Relying on Python functions for calendar logic requires thorough testing to ensure functional equivalence with the original UC4 rules. The `BERT_NICHT` exclusion logic is currently a placeholder and needs to be fully implemented based on the actual `DW.KALENDER` definition.

*   **Modular Task Design**:
    *   **Why**: Each UC4 JOBP/EVNT is represented as a distinct Airflow task. This modularity facilitates future migration of the actual business logic, allowing each placeholder task to be replaced with a more specific operator (e.g., `BigQueryOperator`, `PythonOperator`) or a `TriggerDagRunOperator` to invoke a sub-DAG for more complex child workflows.

## 4. Manual Steps Before Go-Live

Before deploying and running the `dw_bert_ablaufsteuerung` DAG in a production Cloud Composer environment, the following manual steps are required:

1.  **Cloud Composer Environment Setup**:
    *   Ensure a production-ready Cloud Composer environment is provisioned, configured, and running in the target GCP project.

2.  **IAM Permissions**:
    *   Verify that the Cloud Composer service account has the necessary IAM roles and permissions to:
        *   Deploy DAGs to the Composer environment.
        *   Interact with BigQuery (e.g., `BigQuery Data Editor`, `BigQuery Job User`) for future child job migrations.
        *   Access Cloud Storage (e.g., `Storage Object Admin`) for staging data and APT exports.
        *   Access other GCP services (e.g., Cloud Functions, Dataflow, Secret Manager) as required by the migrated child jobs.

3.  **Connection Strings and Secrets**:
    *   If child jobs require connections to external databases, APIs, or other systems, configure Airflow Connections within the Composer environment.
    *   Store any sensitive credentials (e.g., API keys, database passwords) securely in Google Secret Manager and configure Airflow to retrieve them.

4.  **BigQuery Datasets and Tables**:
    *   Create all necessary BigQuery datasets and tables that will be used or generated by the child jobs (once their logic is migrated). Ensure schemas are correctly defined.

5.  **Cloud Storage Buckets**:
    *   Create any required Cloud Storage buckets for staging data, intermediate files, or APT export destinations.

6.  **Full Child Job Migration (B4 Item)**:
    *   **Crucially, the actual business logic for each child UC4 JOBP and EVNT (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`) must be migrated from its original UC4 implementation to GCP-native solutions (e.g., BigQuery SQL, Python scripts, Cloud Functions, Dataflow).**
    *   The placeholder `BashOperator` tasks in `dw_bert_ablaufsteuerung_dag.py` must be replaced with the appropriate Airflow operators (e.g., `BigQueryOperator`, `PythonOperator`, `TriggerDagRunOperator` for sub-DAGs) that execute this migrated logic.

7.  **Refine Calendar Logic**:
    *   Thoroughly review and implement the complete `DW.KALENDER` logic, especially the `BERT_NICHT` exclusion rules, within the `_check_bert_nicht_exclusion` function to ensure exact functional equivalence with the UC4 scheduler. This might involve querying a calendar table or an external service.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up, including redesign (B4) considerations:

*   **Child Job Logic Migration (B4)**: The most significant gap. The `dw_bert_ablaufsteuerung_dag.py` currently orchestrates placeholder tasks. The actual business logic and data transformations contained within each referenced UC4 `JOBP` and `EVNT` (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_STAMMDATEN_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) must be fully migrated to BigQuery SQL, Python, or other GCP services and integrated into the DAG.
*   **Complex UC4 Calendars**: While basic monthly checks are implemented, the full complexity of `DW.NEW_CALENDAR` and `DW.KALENDER` (especially `BERT_NICHT` and any other intricate inclusion/exclusion rules or custom dates) needs to be thoroughly analyzed and accurately translated into Airflow's scheduling or custom Python logic. The `_check_bert_nicht_exclusion` function is a placeholder and requires concrete implementation.
*   **UC4 Variables/Prompt Sets**: The original UC4 XML contains `<Variables>` and `<PromptSets>` nodes. If any child jobs utilize these, a mechanism for managing and passing these parameters in Airflow (e.g., Airflow Variables, XComs, or external configuration management) needs to be designed and implemented.
*   **Error Handling and Alerting**: The Airflow DAG requires robust error handling, retry mechanisms, and integration with GCP monitoring and alerting services (e.g., Cloud Monitoring, Cloud Logging, PagerDuty, Slack/email notifications) to replicate or improve upon UC4's built-in capabilities.
*   **`ErlstStTime` Precision**: The `ErlstStTime` (Earliest Start Time) values (e.g., 01:00, 04:03, 07:00, 20:00) are currently handled by task dependencies within a daily schedule. If strict, absolute time adherence for these specific start times is critical, `TimeSensor` tasks might be required to pause execution until the specified time.
*   **External System Integration for APT Exports**: The "APT exports" imply interaction with external systems. The current tasks are placeholders. The actual integration (e.g., SFTP, API calls, secure file transfer) needs to be implemented using appropriate Airflow operators (e.g., `SFTPToGCSOperator`, `HttpSensor`) or GCP services (e.g., Cloud Storage, Cloud Functions).
*   **UC4 `EVNT` to Airflow Sensor Mapping**: `DW.BERT_RUN_ADM_CHECK_JP_EVT` is currently a `BashOperator`. If this event truly waits for an external condition (e.g., file arrival, database state change), it should be re-implemented as an appropriate Airflow Sensor (e.g., `GCSObjectSensor`, `SqlSensor`, `ExternalTaskSensor`).

## 6. Validation

Validation of the migrated `dw_bert_ablaufsteuerung` DAG involves several stages:

1.  **Code Review and Static Analysis**:
    *   Review the `dw_bert_ablaufsteuerung_dag.py` for adherence to Airflow best practices, readability, and correctness.
    *   Use `airflow dags parse dags/dw_bert_ablaufsteuerung_dag.py` to check for syntax errors.

2.  **Unit Testing**:
    *   Develop unit tests for the custom Python functions (`_check_monthly_run_day`, `_check_bert_nicht_exclusion`) to ensure they correctly identify run days and exclusion criteria.

3.  **Local Airflow Testing**:
    *   Run the DAG locally using the Airflow CLI (`airflow tasks test <dag_id> <task_id> <ds>`) to test individual tasks and their logic in isolation.
    *   Simulate different execution dates (`ds`) to verify calendar-based skipping.

4.  **Cloud Composer Development Environment Deployment**:
    *   Deploy the DAG to a dedicated development Cloud Composer environment.
    *   **Manual Triggering**: Manually trigger DAG runs for various dates, including:
        *   Regular daily runs.
        *   Days that are the 5th or 25th of the month (to verify `DW.BERT_MONATLICH_JP` execution).
        *   Days that should be excluded by `BERT_NICHT` (once implemented) to verify skipping of `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`.
    *   **Airflow UI Monitoring**: Observe task execution, dependencies, and logs in the Airflow UI. Verify that tasks are skipped or executed as expected.

5.  **Functional Validation (Post-Child Job Migration)**:
    *   Once the child job logic is fully migrated and integrated, perform end-to-end functional tests.
    *   **Data Comparison**: Compare the output data (e.g., BigQuery tables, Cloud Storage files) generated by the Airflow DAG with the output from the legacy UC4 job for identical input data and execution dates.
    *   **Performance**: Monitor task and DAG run durations to ensure performance meets SLAs.
    *   **Resource Utilization**: Observe Cloud Composer resource usage (CPU, memory, disk) to ensure it's within acceptable limits.

**"Passing" Criteria**:

A successful migration and validation means:
*   The `dw_bert_ablaufsteuerung` DAG completes all scheduled runs without errors.
*   All tasks within the DAG execute in the correct sequence, respecting dependencies and conditional logic.
*   Tasks dependent on calendar rules (e.g., `DW.BERT_MONATLICH_JP`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) are correctly skipped or executed according to the original UC4 calendar definitions.
*   The migrated child job logic (once implemented) produces functionally identical and correct output data compared to the legacy UC4 system.
*   All downstream systems or data consumers receive the expected data in the correct format and timeframe.
*   Monitoring and alerting mechanisms are functional and correctly notify on failures or anomalies.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Disable Airflow DAG**: Immediately disable the `dw_bert_ablaufsteuerung` DAG in the Cloud Composer Airflow UI to prevent further runs.
    *   **Re-enable UC4 Job**: Re-enable the original `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler in the legacy UC4 environment.

2.  **Assessment and Recovery**:
    *   **Identify Missed Runs**: Determine if any critical runs were missed or partially completed during the migration attempt.
    *   **Manual Trigger/Catch-up**: Manually trigger any necessary catch-up runs in the UC4 environment to ensure business continuity and data consistency.
    *   **Data Reconciliation**: If the Airflow DAG processed or altered any data before the rollback, a data reconciliation plan may be necessary depending on the nature of the child jobs. This should be pre-defined for each critical child job.

3.  **Post-Rollback Analysis**:
    *   Analyze the root cause of the failure in the Airflow DAG (e.g., code bug, configuration error, environmental issue).
    *   Address the identified issues, re-test thoroughly in development/staging environments, and plan for a re-migration attempt.

**Important Considerations**:
*   The original UC4 `DW.BERT_ABLAUFSTEUERUNG` job should remain fully operational and ready for immediate re-activation until the migrated Airflow DAG is proven stable and reliable in production over a sufficient period.
*   Clear communication with stakeholders is essential during any rollback scenario.