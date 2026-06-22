# MIGRATION_NOTES.md: DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The UC4 Job Scheduler (JSCH) `DW.BERT_ABLAUFSTEUERUNG`, responsible for orchestrating various productive processes related to "Bert" (including monthly job plans, administrative checks, housekeeping, daily/monthly APT exports, and master data processing), has been migrated.

**Source System:** UC4/Automic Job Scheduler
**Target Platform:** Google Cloud Composer (Airflow)

The migration involved translating the orchestration logic, including task sequencing, time-based triggers, and calendar dependencies, into a native Airflow Directed Acyclic Graph (DAG). Data processing, which was previously handled by various UC4 child job plans (JOBP) and events (EVNT), is intended to leverage Google Cloud Platform services, primarily BigQuery, with orchestration managed by Airflow.

## 2. Generated Artifacts

The migration process generated the following Airflow DAG Python file:

*   **`dw_bert_ablaufsteuerung.py`**
    *   **Role:** This is the main Airflow DAG that replaces the `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler. It orchestrates the execution of various child processes by triggering other Airflow DAGs (corresponding to the original UC4 JOBP objects) and managing time-based and calendar-based dependencies. It includes placeholder tasks for UC4 Event (EVNT) objects, pending their detailed migration.

## 3. Key Design Decisions

The following key design decisions were made during the migration of `DW.BERT_ABLAUFSTEUERUNG`:

*   **Airflow as Orchestration Engine:** Google Cloud Composer (managed Airflow) was chosen as the target orchestration platform to leverage cloud-native capabilities and provide a robust, scalable, and observable workflow management system.
*   **UC4 JSCH to Single Airflow DAG:** The entire `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler was translated into a single Airflow DAG (`dw_bert_ablaufsteuerung.py`) to maintain the top-level orchestration logic and dependencies.
*   **UC4 JOBP to Separate Airflow DAGs:** Each referenced UC4 Job Plan (JOBP) (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`) is designed to be migrated into its own dedicated Airflow DAG. These child DAGs are then triggered by the main `dw_bert_ablaufsteuerung` DAG using the `TriggerDagRunOperator`. This promotes modularity, reusability, and independent development/deployment of sub-processes.
*   **UC4 EVNT to Placeholder Tasks:** UC4 Event (EVNT) objects (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`) are initially represented by `TriggerDagRunOperator` tasks pointing to placeholder DAGs. Their final implementation will depend on the detailed analysis of the original event definitions, potentially involving specific Airflow sensors or event-driven architectures (e.g., Pub/Sub).
*   **Time-Based Dependencies (`ErlstStTime`):** UC4's "Earliest Start Time" (`ErlstStTime`) conditions are implemented using Airflow's `TimeSensor` operator, ensuring tasks wait until a specific time of day before proceeding.
*   **Calendar Dependencies:** UC4 calendar rules (e.g., `DW.NEW_CALENDAR`) are implemented using Airflow's `ShortCircuitOperator` with custom Python functions. This allows for dynamic evaluation of calendar logic, skipping downstream tasks if the conditions are not met. The actual calendar logic needs to be manually implemented within these Python functions.
*   **`ActFlg=0` Handling for Parallel Execution:** For UC4 tasks with `ActFlg=0` (meaning "do not wait for completion"), the corresponding `TriggerDagRunOperator` in Airflow is configured with `wait_for_completion=False`. This allows the orchestrating DAG to proceed immediately after triggering the child DAG, mimicking parallel execution.
*   **Single Active Run (`Else=Skip`):** The UC4 `Else=Skip` behavior, which ensures only one instance of the scheduler runs at a time, is primarily handled by setting `max_active_runs=1` at the DAG level in Airflow. A `PythonOperator` (`guard_active_run`) is included as an explicit check, which can be extended for more granular control if needed.
*   **Default Retry Policy:** A default retry policy of `retries=0` and `retry_delay=0` minutes is applied to all tasks, as per the design document. This can be overridden for specific tasks if their underlying processes require different retry behavior.
*   **Failure Callbacks:** A placeholder `on_failure_callback` function is included to allow for custom error handling, alerting, or logging in case of task failures.

## 4. Manual Steps Before Go-Live

Before the `dw_bert_ablaufsteuerung` DAG can be moved to a production environment and go live, the following manual steps are required:

1.  **GCP Project and Resource Configuration:**
    *   Ensure the placeholder variables (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) in `dw_bert_ablaufsteuerung.py` are replaced with actual production values. These should ideally be managed via Airflow Variables, Environment Variables, or a secrets manager.
2.  **Child DAG Migration and Deployment:**
    *   All referenced child DAGs (`dw_bert_monatlich_jp`, `dw_bert_adm_housekeeping_jp`, `dw_dwh_apt_export_taeglich_jp`, `dw_bert_stammdaten_jp`, `dw_bert_run_adm_check_jp_evt`, `dw_dwh_run_apt_export_monatlich_jp_evt`) must be fully migrated, developed, tested, and deployed to the same Airflow environment.
    *   The placeholder DAGs for `EVNT` objects (`dw_bert_run_adm_check_jp_evt`, `dw_dwh_run_apt_export_monatlich_jp_evt`) need to be replaced with their actual implementations.
3.  **Calendar Logic Implementation:**
    *   The `calendar_check_dw_new_calendar_func` Python function within `dw_bert_ablaufsteuerung.py` contains a `TODO`. The actual logic for `DW.NEW_CALENDAR` (and any other relevant calendars like `DW.KALENDER`) must be manually implemented based on its original UC4 definition. This might involve specific date checks, holiday rules, or business day calculations.
4.  **IAM/Permissions:**
    *   Verify that the Airflow service account (used by the Composer environment) has the necessary IAM permissions to:
        *   Trigger other DAGs (`TriggerDagRunOperator`).
        *   Access any GCP resources (e.g., BigQuery datasets/tables, GCS buckets, Dataproc clusters) that the child DAGs will interact with.
5.  **Connection Strings and Secrets:**
    *   Any database connection strings, API keys, or other secrets required by the child DAGs (if they were part of the original UC4 job's environment) must be securely configured in Airflow Connections or a secrets manager (e.g., Google Secret Manager).
6.  **Scheduling Configuration:**
    *   Review and confirm the `schedule_interval` (`0 0 * * *` for daily at midnight) and `start_date` for `dw_bert_ablaufsteuerung` DAG to ensure it aligns with production requirements. Adjust `start_date` from `days_ago(1)` to a specific `datetime` if backfilling is not desired or if a specific historical start point is needed.
7.  **Failure Callback Implementation:**
    *   Customize the `on_failure_callback` function to include specific alerting mechanisms (e.g., email, Slack, PagerDuty) relevant to the production environment.

## 5. Known Gaps & Unresolved References

The following items are known gaps or unresolved references that require further attention:

*   **Missing Child Job Plan/Event Definitions:** The internal logic, transformations, and external dependencies of the referenced UC4 objects (`DW.BERT_MONATLICH_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_STAMMDATEN_JP`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) are not part of this migration design. Each of these requires its own separate migration design, development, and testing.
*   **Calendar Logic Implementation:** The exact definitions for UC4 calendars (`DW.NEW_CALENDAR`, `DW.KALENDER`) are not provided. The `calendar_check_dw_new_calendar_func` in the generated DAG is a placeholder and requires manual implementation based on the original calendar rules.
*   **UC4 Event Objects (`EVNT`):** The Airflow implementation for `DW.BERT_RUN_ADM_CHECK_JP_EVT` and `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` is currently a placeholder `TriggerDagRunOperator`. Their final implementation (e.g., dedicated event-driven DAGs, specific Airflow sensors, or Cloud Functions) depends on a detailed analysis of their original UC4 definitions.
*   **Retry and Error Handling (Child Jobs):** While a default retry policy is set for the main DAG, the specific retry and error handling logic for the individual child job plans/events is unknown. This will need to be defined during their respective migrations.
*   **`r_ai_start` Commands:** If any of the nested UC4 JOBPs contain `r_ai_start` commands (indicating Ab Initio graph executions), their specific graph names, job keys, and types were not extracted in this migration. These will require further analysis during the migration of the child JOBPs.
*   **GCP Placeholder Variables:** The variables `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME` are placeholders in the generated code and must be replaced with actual values.

## 6. Validation

To validate the successful migration and functionality of the `dw_bert_ablaufsteuerung` DAG:

1.  **DAG Parsing and UI Check:**
    *   Upload `dw_bert_ablaufsteuerung.py` to the Airflow DAGs folder in Google Cloud Composer.
    *   Verify that the DAG appears in the Airflow UI without parsing errors.
    *   Inspect the Graph View to ensure task dependencies are correctly represented as per the design document (Section 4).
2.  **Manual Trigger and Execution:**
    *   Manually trigger the `dw_bert_ablaufsteuerung` DAG from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI.
    *   **Passing Criteria:**
        *   The `guard_active_run` task completes successfully.
        *   `TimeSensor` tasks (`wait_until_...`) correctly wait until their target times and then succeed.
        *   `ShortCircuitOperator` tasks (`calendar_check_dw_new_calendar_1`, `calendar_check_dw_new_calendar_2`) execute their Python callable. For testing, ensure the `calendar_check_dw_new_calendar_func` is temporarily set to always return `True` or `False` to test both paths, or test on a day that satisfies the calendar condition.
        *   `TriggerDagRunOperator` tasks (`trigger_dw_bert_monatlich_jp`, etc.) successfully trigger their respective child DAGs.
        *   `wait_for_completion=True` tasks (`trigger_dw_bert_monatlich_jp`, `trigger_dw_bert_adm_housekeeping_jp`, `trigger_dw_bert_stammdaten_jp`, `trigger_dw_dwh_run_apt_export_monatlich_jp_evt`) correctly wait for the triggered child DAGs to complete before succeeding.
        *   `wait_for_completion=False` tasks (`trigger_dw_dwh_apt_export_taeglich_jp`, `trigger_dw_bert_run_adm_check_jp_evt`) succeed immediately after triggering the child DAG, without waiting.
        *   All tasks in the `dw_bert_ablaufsteuerung` DAG complete with a "success" status.
3.  **Log Review:**
    *   Examine the logs for each task instance for any errors, warnings, or unexpected behavior.
    *   Verify that the output from placeholder functions (e.g., `guard_active_run_func`, `calendar_check_dw_new_calendar_func`) is as expected.
4.  **Child DAG Verification:**
    *   Confirm that the triggered child DAGs (e.g., `dw_bert_monatlich_jp`) start and execute as expected in the Airflow UI. (Note: The full validation of child DAGs depends on their individual migration and testing.)

## 7. Rollback Procedure

In the event that the migrated `dw_bert_ablaufsteuerung` DAG encounters critical issues or fails to meet production requirements, the following rollback procedure should be followed:

1.  **Pause Airflow DAG:** Immediately pause the `dw_bert_ablaufsteuerung` DAG in the Airflow UI to prevent any further execution or triggering of child processes.
2.  **Deactivate Airflow DAG:** (Optional, but recommended for clean rollback) Remove or deactivate the `dw_bert_ablaufsteuerung.py` file from the Airflow DAGs folder.
3.  **Reactivate UC4 Job Scheduler:** Reactivate the original `DW.BERT_ABLAUFSTEUERUNG` Job Scheduler in the UC4/Automic environment.
4.  **Monitor UC4 Execution:** Closely monitor the UC4 environment to ensure the `DW.BERT_ABLAUFSTEUERUNG` JSCH resumes its normal operation and orchestrates its child processes correctly.
5.  **Data Consistency Check:** Perform a data consistency check to identify and address any potential discrepancies or partial data processing that might have occurred during the brief period the Airflow DAG was active.
6.  **Root Cause Analysis:** Investigate the issues encountered with the Airflow DAG, address the identified problems, and re-plan the migration if necessary.