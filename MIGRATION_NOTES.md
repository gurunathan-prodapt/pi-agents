# MIGRATION_NOTES.md: DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The UC4 Job Scheduler (JSCH) `DW.BERT_ABLAUFSTEUERUNG` has been migrated to an Airflow Directed Acyclic Graph (DAG) for orchestration on Google Cloud Platform. This migration replaces the legacy UC4 job responsible for orchestrating various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. The new Airflow DAG, `dw_bert_ablaufsteuerung`, now manages the sequence of child jobs and events, incorporating time-based and calendar-based dependencies.

## 2. Generated Artifacts

The migration produced the following artifact:

*   **File:** `dags/dw_bert_ablaufsteuerung.py`
    *   **Role:** This Python file defines the Airflow DAG `dw_bert_ablaufsteuerung`. It serves as the primary orchestrator, replacing the UC4 JSCH. It contains tasks for concurrency control, time-based waiting, calendar-based branching, and triggering of child Airflow DAGs (which represent the migrated UC4 JOBP and EVNT objects).

## 3. Key Design Decisions

Several key design decisions were made during the migration:

*   **UC4 JSCH to Airflow DAG:** The entire UC4 Job Scheduler structure was translated into a single Airflow DAG (`dw_bert_ablaufsteuerung`). This is the standard approach for migrating UC4 orchestrators, leveraging Airflow's native scheduling and orchestration capabilities.
*   **Concurrency Control (`SYNCREF` with `Else=Skip`):** The UC4 `SYNCREF` mechanism with `Else=Skip` (preventing concurrent runs) was implemented using a `PythonOperator` (`_guard_active_run`) at the start of the DAG. This task checks for active DAG runs and raises an `AirflowSkipException` if another instance is already running, ensuring that only one `dw_bert_ablaufsteuerung` DAG runs at any given time.
*   **Child Job Orchestration (`JOBP`, `EVNT` to `TriggerDagRunOperator`):** Each invoked UC4 job plan (`JOBP`) or event (`EVNT`) was mapped to an Airflow `TriggerDagRunOperator`. This design decouples the main orchestration DAG from the specific logic of the child jobs, allowing them to be migrated as independent Airflow DAGs.
    *   **`ActFlg` to `wait_for_completion`:** The UC4 `ActFlg` attribute directly determined the `wait_for_completion` parameter of the `TriggerDagRunOperator`. `ActFlg=1` (parent waits) translates to `wait_for_completion=True`, while `ActFlg=0` (fire-and-forget) translates to `wait_for_completion=False`.
*   **Time-Based Triggers (`ErlstStTime` to `TimeSensor`):** UC4's `ErlstStTime` (earliest start time) constraints were directly translated to Airflow `TimeSensor` tasks. These sensors ensure that subsequent tasks do not execute before the specified time of day.
*   **Calendar-Based Triggers (`calendars` to `BranchPythonOperator`):** UC4 calendar dependencies were implemented using `BranchPythonOperator` tasks (`check_monthly_bert_calendar`, `check_dwh_export_monthly_calendar`). These operators contain custom Python logic to evaluate the calendar conditions based on the DAG's execution date and dynamically route the DAG's execution path, either triggering the dependent tasks or skipping them. `EmptyOperator` tasks are used as anchors to manage the flow convergence after conditional branches.
*   **Default Retries:** Given no explicit retry configurations were found in the UC4 JSCH, the Airflow DAG defaults to `retries: 0`. Any specific retry logic for individual child jobs should be configured within their respective child DAGs.

## 4. Manual Steps Before Go-Live

Before the `dw_bert_ablaufsteuerung` DAG can be fully operational in a production environment, the following manual steps are required:

1.  **Complete Calendar Logic Implementation:**
    *   The placeholder functions `_check_new_calendar_branch` and `_check_bert_nicht_calendar_branch` in `dags/dw_bert_ablaufsteuerung.py` *must be manually completed*. This involves accurately translating the exact logic of the original UC4 calendars (`DW.NEW_CALENDAR` with `DAY_OF_MONTH_25` and `DAY_OF_MONTH_05` constraints, and `DW.KALENDER` with the `BERT_NICHT` constraint) into Python code. This is a critical step to ensure correct scheduling behavior.
2.  **Migrate and Deploy Child DAGs:**
    *   The `TriggerDagRunOperator` tasks in `dw_bert_ablaufsteuerung` refer to child DAGs (e.g., `dw_bert_monatlich_jp`, `dw_bert_run_adm_check_jp_evt`). These child DAGs, representing the migrated UC4 `JOBP` and `EVNT` objects, *must be migrated, developed, and deployed* to the Airflow environment before `dw_bert_ablaufsteuerung` can function correctly.
3.  **Set Production `start_date`:**
    *   Replace `start_date=days_ago(1)` with a fixed, historical `datetime` object (e.g., `start_date=datetime(2023, 1, 1)`) in the `dw_bert_ablaufsteuerung.py` file. This ensures consistent historical DAG run behavior in production.
4.  **Airflow Environment Setup:**
    *   Deploy the `dags/dw_bert_ablaufsteuerung.py` file to the designated DAGs folder in your Airflow environment.
    *   Ensure the Airflow environment (scheduler, webserver, workers) is properly configured and running.
    *   Verify that the Airflow service account has the necessary IAM permissions to trigger other DAGs within the same Airflow instance.
5.  **GCP Environment Configuration (if applicable to child DAGs):**
    *   If child DAGs utilize GCP services (e.g., Dataproc, BigQuery), ensure the Airflow service account has appropriate IAM roles and permissions to interact with those services.
    *   Any placeholder configuration parameters (e.g., `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`) within the `conf` dictionary of `TriggerDagRunOperator` (if uncommented and used by child DAGs) must be replaced with actual production values.

## 5. Known Gaps & Unresolved References

*   **Calendar Definitions (B4 Item):** The most significant gap is the *incomplete implementation of the UC4 calendar logic*. The `_check_new_calendar_branch` and `_check_bert_nicht_calendar_branch` functions contain placeholder logic and require manual investigation of the original UC4 calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER` with `BERT_NICHT`) to be fully and accurately implemented. This is flagged as a B4 item requiring redesign/completion.
*   **Child DAG Existence:** The `dw_bert_ablaufsteuerung` DAG relies on the existence and correct functioning of several child DAGs (e.g., `dw_bert_monatlich_jp`, `dw_bert_adm_housekeeping_jp`). These child DAGs are not part of this migration and must be migrated and deployed separately. The `trigger_dag_id` values are placeholders for the names of these future child DAGs.
*   **Error Handling and Retries:** The DAG is configured with `retries=0`. If specific retry strategies were defined in the original UC4 tasks, they need to be applied either to the `TriggerDagRunOperator` tasks in this DAG or, more appropriately, within the respective child DAGs.
*   **Dynamic Variables/Prompt Sets:** No explicit UC4 variables or Prompt Sets were identified in the source JSCH XML. If child jobs rely on such dynamic inputs, their migration designs must address how these will be passed (e.g., via Airflow `conf` parameters or XComs).

## 6. Validation

To validate the `dw_bert_ablaufsteuerung` DAG:

1.  **Unit Testing (Python Functions):**
    *   **`_guard_active_run`:** Write unit tests to verify that this function correctly raises `AirflowSkipException` when other active DAG runs exist and proceeds normally when no other runs are active.
    *   **Calendar Functions (`_check_new_calendar_branch`, `_check_bert_nicht_calendar_branch`):** After completing their implementation, write unit tests to verify that these functions return the correct `task_id` (for branching) based on various `logical_date` inputs (e.g., 5th, 25th, 15th, and other days of the month).
2.  **Local Airflow Deployment and Testing:**
    *   Deploy `dags/dw_bert_ablaufsteuerung.py` to a local Airflow environment.
    *   **Concurrency Test:** Manually trigger the DAG multiple times in quick succession to ensure the `guard_active_run` task correctly skips subsequent runs.
    *   **Calendar Logic Test:** Trigger the DAG for different `logical_date` values (e.g., using `airflow dags trigger -e YYYY-MM-DD`) to simulate various days of the month and verify that the calendar-based branches (`check_monthly_bert_calendar`, `check_dwh_export_monthly_calendar`) correctly route the execution path.
    *   **Time Sensor Test:** Trigger the DAG and observe the `TimeSensor` tasks. They should remain in a `scheduled` state until their target time is reached, then proceed.
    *   **Child DAG Triggering:** Verify that the `TriggerDagRunOperator` tasks attempt to trigger the specified child DAGs. For initial testing, these child DAGs can be simple stub DAGs that immediately succeed.
    *   **`wait_for_completion` Test:** Observe the behavior of `TriggerDagRunOperator` tasks with `wait_for_completion=True` (parent DAG waits) versus `wait_for_completion=False` (parent DAG proceeds without waiting).

**"Passing" Criteria:**

A successful validation means:

*   The `dw_bert_ablaufsteuerung` DAG completes successfully without any Airflow errors.
*   The `guard_active_run` task correctly enforces single-instance execution.
*   The calendar-based branching tasks (`check_monthly_bert_calendar`, `check_dwh_export_monthly_calendar`) accurately evaluate the calendar conditions and route the DAG's execution as expected for various dates.
*   All `TimeSensor` tasks correctly pause execution until their specified `target_time` is reached.
*   All `TriggerDagRunOperator` tasks successfully initiate runs of their respective child DAGs (even if they are stubs for testing).
*   The `wait_for_completion` parameter for `TriggerDagRunOperator` tasks behaves as expected, ensuring correct sequential or parallel execution flow.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior with the migrated `dw_bert_ablaufsteuerung` DAG, the following rollback procedure should be followed:

1.  **Pause Airflow DAG:** Immediately pause the `dw_bert_ablaufsteuerung` DAG in the Airflow UI to prevent any further runs.
2.  **Remove Airflow DAG:** Remove the `dags/dw_bert_ablaufsteuerung.py` file from the Airflow DAGs folder. This will unregister the DAG from Airflow.
3.  **Re-enable UC4 Job:** Re-enable and/or restart the original `DW.BERT_ABLAUFSTEUERUNG` Job Scheduler in the UC4/Automic system.
4.  **Monitor UC4:** Closely monitor the UC4 job to ensure it resumes normal operation and processes data as expected.
5.  **Root Cause Analysis:** Investigate the reason for the rollback, identify the issues in the Airflow DAG or its dependencies, and plan corrective actions before attempting re-migration.