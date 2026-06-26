# MIGRATION_NOTES.md: DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The `DW.BERT_ABLAUFSTEUERUNG` job, a legacy UC4 Job Scheduler (JSCH) responsible for orchestrating various "Bert" related productive processes, has been migrated. Its scheduling and orchestration logic has been re-platformed from UC4 Automic to Google Cloud Composer (Apache Airflow). This migration is part of a broader initiative to move data platform operations to Google Cloud Platform, leveraging BigQuery for data processing.

The new Airflow DAG, `dw_bert_ablaufsteuerung`, now serves as the central orchestrator, triggering other Airflow DAGs (representing migrated UC4 Job Plans) and executing specific event-based logic based on time and calendar conditions.

## 2. Generated Artifacts

The migration produced the following primary artifact:

*   **`dags/dw_bert_ablaufsteuerung_dag.py`**
    *   **Role**: This Python file defines the Apache Airflow Directed Acyclic Graph (DAG) that replaces the UC4 `DW.BERT_ABLAUFSTEUERUNG` JSCH. It contains tasks that:
        *   Utilize `TimeSensor` to wait for specific execution times.
        *   Employ `ShortCircuitOperator` and custom Python functions to implement complex calendar-based conditional logic (e.g., monthly runs on specific days, exclusion days).
        *   Use `TriggerDagRunOperator` to initiate other Airflow DAGs, which represent the migrated UC4 Job Plans (`JOBP`) that `DW.BERT_ABLAUFSTEUERUNG` previously invoked.
        *   Include `BashOperator` placeholders for UC4 Events (`EVNT`) where the specific logic needs to be fully defined.
    *   **Location**: This file should be deployed to the `dags/` folder within your Google Cloud Composer environment's GCS bucket.

## 3. Key Design Decisions

The following key design decisions were made during the migration of `DW.BERT_ABLAUFSTEUERUNG`:

*   **Target Platform: Google Cloud Composer (Apache Airflow)**
    *   **Rationale**: Cloud Composer provides a managed Airflow environment, aligning with the broader strategy of migrating to GCP. Airflow is a robust, industry-standard orchestration tool well-suited for complex scheduling and dependency management.
*   **Orchestration Pattern: Main DAG triggering Sub-DAGs**
    *   **Rationale**: The legacy `DW.BERT_ABLAUFSTEUERUNG` was a scheduler of other UC4 `JOBP`s. Replicating this in Airflow by having a main `dw_bert_ablaufsteuerung_dag` trigger separate DAGs for each `JOBP` (e.g., `dw_bert_monatlich_jp`) promotes modularity, independent testing, and clearer separation of concerns. This allows sub-DAGs to have their own schedules and be triggered independently if needed.
    *   **Trade-offs**: This approach introduces a dependency on the successful migration and deployment of all sub-DAGs. If a sub-DAG is not yet migrated or fails, the `TriggerDagRunOperator` will wait or fail, potentially blocking the main orchestrator.
*   **Time-based Scheduling: `TimeSensor`**
    *   **Rationale**: UC4 tasks often have `ErlstStTime` (earliest start time) or fixed `StartTime` attributes. Airflow's `TimeSensor` is the most direct and idiomatic way to replicate this behavior, ensuring tasks only proceed after a specific time of day.
*   **Complex Calendar Logic: `ShortCircuitOperator` with Python Callables**
    *   **Rationale**: UC4's calendar system (e.g., `DAY_OF_MONTH_25`, `BERT_NICHT`) is highly configurable. Airflow's `schedule_interval` is powerful for recurring schedules but less flexible for complex, conditional calendar logic. Using `ShortCircuitOperator` with custom Python functions allows for precise replication of these conditions, enabling tasks to run only on specific days of the month or excluding certain days.
    *   **Trade-offs**: Requires careful and accurate translation of UC4 calendar definitions into Python code. Any misinterpretation could lead to incorrect scheduling. The placeholder functions in the generated DAG highlight this need for manual verification.
*   **UC4 Events (`EVNT`): Placeholder `BashOperator`**
    *   **Rationale**: UC4 Events often represent triggers or simple actions. For initial migration, a `BashOperator` with an `echo` command serves as a placeholder. This allows the overall DAG structure and scheduling to be validated while the specific logic for each event is determined and implemented (e.g., as a `PythonOperator` calling a specific function, or another `TriggerDagRunOperator`).
    *   **Trade-offs**: The actual functionality of these events is not yet implemented and requires further development.

## 4. Manual Steps Before Go-Live

Before `dw_bert_ablaufsteuerung_dag.py` can be fully operational in a production environment, the following manual steps are required:

1.  **Composer Environment Setup**: Ensure a Google Cloud Composer environment is provisioned and operational.
2.  **IAM Permissions**:
    *   Verify that the Composer environment's service account has the necessary permissions to:
        *   Trigger other DAGs (`TriggerDagRunOperator`).
        *   Access any resources that the sub-DAGs or event logic might interact with (e.g., BigQuery, Cloud Storage, Cloud Functions).
3.  **Deploy Sub-DAGs**:
    *   **Crucial**: All sub-DAGs referenced by `TriggerDagRunOperator` (e.g., `dw_bert_monatlich_jp`, `dw_bert_stammdaten_jp`, `dw_dwh_apt_export_taeglich_jp`, `dw_bert_adm_housekeeping_jp`) must be migrated, deployed, and validated in the Composer environment *before* `dw_bert_ablaufsteuerung_dag` is enabled.
4.  **Define Calendar Logic**:
    *   **Action**: Accurately translate the definitions of UC4 calendars (`DW.NEW_CALENDAR`, `DW.KALENDER`) and their associated keys (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) into the placeholder Python functions (`_check_if_monthly_run_day_callable`, `_check_if_bert_nicht_day_callable`, `_check_monthly_apt_export_day_callable`) within `dw_bert_ablaufsteuerung_dag.py`. This may involve querying UC4 metadata or reviewing UC4 documentation.
    *   **Output**: Updated `dw_bert_ablaufsteuerung_dag.py` with correct calendar logic.
5.  **Implement Event Logic**:
    *   **Action**: Replace the `BashOperator` placeholders for `DW.BERT_RUN_ADM_CHECK_JP_EVT` and `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` with the actual logic. This might involve:
        *   Calling a Python function (`PythonOperator`).
        *   Triggering another specific DAG (`TriggerDagRunOperator`).
        *   Executing a BigQuery query (`BigQueryOperator`).
        *   Invoking a Cloud Function or other GCP service.
    *   **Output**: Updated `dw_bert_ablaufsteuerung_dag.py` with functional event logic.
6.  **Connection Strings/Secrets**: If any of the implemented event logic or sub-DAGs require external connections or secrets, ensure these are securely configured in Airflow (e.g., Airflow Connections, Google Secret Manager).

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas requiring further definition:

*   **Precise Calendar Translation (B4 Item)**: The Python functions for calendar logic (`_check_if_monthly_run_day_callable`, `_check_if_bert_nicht_day_callable`, `_check_monthly_apt_export_day_callable`) are currently placeholders. Their exact implementation requires a detailed analysis of the legacy UC4 calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`, `BERT_NICHT`). This is a critical B4 item that must be resolved for correct scheduling.
*   **UC4 Event Logic Implementation (B4 Item)**: The `BashOperator` tasks for `DW.BERT_RUN_ADM_CHECK_JP_EVT` and `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` are placeholders. The actual business logic or actions these events perform in UC4 need to be fully understood and implemented in Airflow.
*   **Sub-DAG Availability**: This DAG relies on the existence and correct functioning of several other Airflow DAGs (e.g., `dw_bert_monatlich_jp`, `dw_bert_stammdaten_jp`). The migration status and readiness of these dependent DAGs are external to this document but critical for the overall success.
*   **UC4 Synchronization Logic (`SYNCREF`)**: The `SYNCREF` block in the original UC4 XML implies synchronization points (`SETZE_FREI`, `SETZE_LAEUFT`). While Airflow task dependencies handle basic sequencing, complex synchronization mechanisms might require `ExternalTaskSensor` or custom sensor logic if tasks are meant to wait for external conditions or other DAGs to reach a specific state beyond simple completion. This aspect needs further review.
*   **Error Handling and Alerting**: While `email_on_failure` is set, a comprehensive alerting strategy (e.g., PagerDuty, Slack notifications) for production failures should be considered.

## 6. Validation

Validation of the `dw_bert_ablaufsteuerung_dag` involves deploying it to a test Composer environment and verifying its behavior against the expected UC4 execution patterns.

**How to Run Tests:**

1.  **Deploy to Test Composer**: Upload `dags/dw_bert_ablaufsteuerung_dag.py` (along with all its dependent sub-DAGs) to the DAGs folder of a non-production Google Cloud Composer environment.
2.  **Enable the DAG**: Ensure the `dw_bert_ablaufsteuerung` DAG is unpaused in the Airflow UI.
3.  **Trigger Manually (Optional, for immediate feedback)**: For initial checks, you can manually trigger the DAG from the Airflow UI. This will start a DAG run immediately, allowing you to observe task execution.
4.  **Observe Scheduled Runs**: Allow the DAG to run naturally according to its `@daily` schedule. Monitor its behavior over several days, including days that should trigger monthly tasks (e.g., 5th, 25th of the month) and days that should be excluded by `BERT_NICHT` logic.
5.  **Review Airflow Logs**: Examine the logs for each task within the Airflow UI to ensure:
    *   `TimeSensor` tasks correctly wait and then succeed.
    *   `ShortCircuitOperator` tasks correctly evaluate calendar conditions and either proceed or skip downstream tasks as expected.
    *   `TriggerDagRunOperator` tasks successfully initiate runs of their respective sub-DAGs.
    *   Placeholder `BashOperator` tasks execute their `echo` commands.
    *   No unexpected errors or failures occur.
6.  **Cross-reference with Legacy UC4**: Compare the execution times and outcomes of the Airflow DAG runs with historical execution logs from the legacy UC4 `DW.BERT_ABLAUFSTEUERUNG` job.

**What "Passing" Means:**

A "passing" validation means that:

*   The `dw_bert_ablaufsteuerung` DAG completes successfully on its scheduled interval.
*   All `TimeSensor` tasks correctly pause until their target time and then succeed.
*   `ShortCircuitOperator` tasks correctly apply the calendar logic:
    *   `check_monthly_run_day_bert_monatlich` only allows `trigger_bert_monatlich_jp` to run on the 5th and 25th of the month.
    *   `check_monthly_apt_export_day` correctly identifies the monthly export day and skips execution on `BERT_NICHT` days.
*   All `TriggerDagRunOperator` tasks successfully trigger their corresponding sub-DAGs, and those sub-DAGs also complete successfully.
*   The placeholder `BashOperator` tasks execute without errors.
*   The overall sequence and timing of triggered sub-jobs/events in Airflow precisely match the behavior and dependencies observed in the legacy UC4 system.
*   No unexpected task failures or retries occur.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior with the migrated `dw_bert_ablaufsteuerung_dag` that cannot be quickly resolved, the following rollback procedure should be followed:

1.  **Disable New Airflow DAG**:
    *   In the Airflow UI, locate the `dw_bert_ablaufsteuerung` DAG and toggle its status to "Off" (paused). This will prevent any further scheduled runs.
2.  **Re-enable Legacy UC4 Job**:
    *   In the legacy UC4 Automic environment, re-enable the `DW.BERT_ABLAUFSTEUERUNG` JSCH. Ensure its schedule is active and it can resume orchestrating its dependent `JOBP`s and `EVNT`s.
3.  **Verify Legacy Operation**:
    *   Monitor the UC4 system to confirm that `DW.BERT_ABLAUFSTEUERUNG` has successfully resumed its operations and that all dependent jobs are running as expected.
4.  **Investigate and Rectify**:
    *   Analyze the root cause of the issues encountered with the Airflow DAG. Make necessary corrections to the `dw_bert_ablaufsteuerung_dag.py` code, dependent sub-DAGs, or environment configuration.
5.  **Re-test and Re-deploy**:
    *   Once corrections are made, re-deploy the updated Airflow DAGs to a test environment and repeat the validation steps. Only proceed with another go-live attempt after thorough testing.