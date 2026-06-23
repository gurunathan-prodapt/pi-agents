As a senior data-migration QA engineer, I've analyzed the migration design for `DW.BERT_ABLAUFSTEUERUNG` from UC4 to Airflow. The core of this migration is re-platforming an orchestration job. Therefore, the validation tests will primarily focus on ensuring the Airflow DAG correctly mimics the UC4 scheduler's control flow, timing, and dependency logic. Data transformation, external system interactions, and detailed data quality checks are largely delegated to the *child DAGs* that this main DAG triggers, and would require separate, detailed test plans for each child DAG.

The following tests validate the `dw_bert_ablaufsteuerung.py` Airflow DAG.

---

## Migration Validation Tests for `DW.BERT_ABLAUFSTEUERUNG`

### Test Case 1: Synchronization Guard (`sync_guard`) Functionality

*   **Purpose:** Verify that the `sync_guard` task correctly prevents concurrent runs of the `dw_bert_ablaufsteuerung` DAG, mimicking the UC4 `SYNCREF` object's `Else=Skip` behavior and the `max_active_runs=1` DAG property.
*   **Setup:**
    1.  Ensure the `dw_bert_ablaufsteuerung` DAG is deployed to an Airflow environment.
    2.  Prepare a test environment where Airflow can be triggered rapidly.
*   **Action:**
    1.  Manually trigger the `dw_bert_ablaufsteuerung` DAG.
    2.  Immediately (within seconds) manually trigger the same DAG again.
    3.  Observe the Airflow UI (Graph View, Task Logs, DAG Runs).
*   **Pass/Fail Criterion:**
    *   **Pass:** The first triggered DAG run proceeds normally. The second triggered DAG run's `sync_guard` task fails with an `AirflowSkipException`, and the entire DAG run is marked as `skipped` or `failed` (depending on Airflow version/configuration for skipped tasks). No downstream tasks in the second run are executed.
    *   **Fail:** Both DAG runs proceed concurrently, or the `sync_guard` task in the second run does not skip.

### Test Case 2: Daily Schedule Adherence

*   **Purpose:** Verify that the DAG is scheduled to run daily at midnight UTC, matching the UC4 `Period=1` and `StartTime=00:00`.
*   **Setup:**
    1.  Ensure the `dw_bert_ablaufsteuerung` DAG is deployed and unpaused in an Airflow environment.
    2.  Allow the DAG to run for at least 2-3 days.
*   **Action:**
    1.  Observe the DAG Runs history in the Airflow UI.
    2.  Check the `Logical Date` and `Run ID` for recent runs.
*   **Pass/Fail Criterion:**
    *   **Pass:** A new DAG run is initiated every day, with its logical date corresponding to the day it runs, and the actual start time is consistently around 00:00 UTC.
    *   **Fail:** The DAG does not run daily, or its start time deviates significantly from 00:00 UTC.

### Test Case 3: `DW.BERT_MONATLICH_JP` Triggering with Time and Calendar

*   **Purpose:** Verify that `trigger_dw_bert_monatlich_jp` is executed only after 20:00 UTC and when the `DW.NEW_CALENDAR` condition (5th or 25th day of the month) is met, and that it waits for the child DAG to complete (`wait_for_completion=True`).
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Create a placeholder child DAG named `dw_bert_monatlich_jp` that simply succeeds after a short delay (e.g., 30 seconds) to simulate completion.
    3.  Prepare to manually trigger the main DAG with specific execution dates.
*   **Action:**
    1.  **Scenario A (Calendar & Time Met):** Manually trigger the main DAG for an execution date that is the 5th or 25th of the month, and allow it to run past 20:00 UTC.
    2.  **Scenario B (Calendar Not Met):** Manually trigger the main DAG for an execution date that is *not* the 5th or 25th of the month, and allow it to run past 20:00 UTC.
    3.  **Scenario C (Time Not Met):** Manually trigger the main DAG for an execution date that is the 5th or 25th of the month, but before 20:00 UTC.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   **Scenario A:** `wait_until_20_00_for_monatlich` succeeds. `dw_new_calendar_check` succeeds. `trigger_dw_bert_monatlich_jp` is triggered and its status remains `running` until the child `dw_bert_monatlich_jp` DAG completes, then it succeeds.
        *   **Scenario B:** `wait_until_20_00_for_monatlich` succeeds. `dw_new_calendar_check` is skipped, and consequently, `trigger_dw_bert_monatlich_jp` is also skipped.
        *   **Scenario C:** `wait_until_20_00_for_monatlich` remains `running` until 20:00 UTC, then proceeds. If the DAG is triggered before 20:00 and the `timeout` of the `DateTimeSensor` is reached before 20:00, the sensor should fail.
    *   **Fail:** Any deviation from the above, e.g., `trigger_dw_bert_monatlich_jp` runs when it shouldn't, or doesn't wait for completion.

### Test Case 4: `DW.BERT_RUN_ADM_CHECK_JP_EVT` Triggering (Fire-and-Forget)

*   **Purpose:** Verify that `trigger_dw_bert_run_adm_check_jp_evt` is executed only after 07:00 UTC and does *not* wait for the child DAG to complete (`wait_for_completion=False`).
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Create a placeholder child DAG named `dw_bert_run_adm_check_jp_evt` that runs for a noticeable duration (e.g., 5 minutes) to clearly demonstrate `wait_for_completion=False`.
*   **Action:**
    1.  Manually trigger the main DAG for an execution date, allowing it to run past 07:00 UTC.
    2.  Observe the status of `trigger_dw_bert_run_adm_check_jp_evt` and the child DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Pass:** `wait_until_07_00_for_adm_check` succeeds. `trigger_dw_bert_run_adm_check_jp_evt` immediately transitions to `success` after triggering the child DAG, without waiting for the child DAG to finish. The child `dw_bert_run_adm_check_jp_evt` DAG run is initiated and continues to run independently.
    *   **Fail:** `trigger_dw_bert_run_adm_check_jp_evt` waits for the child DAG to complete, or does not trigger at all after 07:00 UTC.

### Test Case 5: `DW.BERT_ADM_HOUSEKEEPING_JP` Triggering with Time

*   **Purpose:** Verify that `trigger_dw_bert_adm_housekeeping_jp` is executed only after 04:03 UTC and waits for the child DAG to complete (`wait_for_completion=True`).
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Create a placeholder child DAG named `dw_bert_adm_housekeeping_jp` that succeeds after a short delay.
*   **Action:**
    1.  Manually trigger the main DAG for an execution date, allowing it to run past 04:03 UTC.
    2.  Observe the status of `trigger_dw_bert_adm_housekeeping_jp` and the child DAG run.
*   **Pass/Fail Criterion:**
    *   **Pass:** `wait_until_04_03_for_housekeeping` succeeds. `trigger_dw_bert_adm_housekeeping_jp` is triggered and its status remains `running` until the child `dw_bert_adm_housekeeping_jp` DAG completes, then it succeeds.
    *   **Fail:** `trigger_dw_bert_adm_housekeeping_jp` runs before 04:03 UTC, or does not wait for the child DAG to complete.

### Test Case 6: `DW.DWH_APT_EXPORT_TAEGLICH_JP` Triggering (Fire-and-Forget)

*   **Purpose:** Verify that `trigger_dw_dwh_apt_export_taeglich_jp` is executed only after 01:30 UTC and does *not* wait for the child DAG to complete (`wait_for_completion=False`).
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Create a placeholder child DAG named `dw_dwh_apt_export_taeglich_jp` that runs for a noticeable duration.
*   **Action:**
    1.  Manually trigger the main DAG for an execution date, allowing it to run past 01:30 UTC.
    2.  Observe the status of `trigger_dw_dwh_apt_export_taeglich_jp` and the child DAG run.
*   **Pass/Fail Criterion:**
    *   **Pass:** `wait_until_01_30_for_taeglich_export` succeeds. `trigger_dw_dwh_apt_export_taeglich_jp` immediately transitions to `success` after triggering the child DAG, without waiting for the child DAG to finish. The child `dw_dwh_apt_export_taeglich_jp` DAG run is initiated and continues to run independently.
    *   **Fail:** `trigger_dw_dwh_apt_export_taeglich_jp` waits for the child DAG to complete, or does not trigger at all after 01:30 UTC.

### Test Case 7: `DW.BERT_STAMMDATEN_JP` Triggering with Time

*   **Purpose:** Verify that `trigger_dw_bert_stammdaten_jp` is executed only after 01:00 UTC and waits for the child DAG to complete (`wait_for_completion=True`).
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Create a placeholder child DAG named `dw_bert_stammdaten_jp` that succeeds after a short delay.
*   **Action:**
    1.  Manually trigger the main DAG for an execution date, allowing it to run past 01:00 UTC.
    2.  Observe the status of `trigger_dw_bert_stammdaten_jp` and the child DAG run.
*   **Pass/Fail Criterion:**
    *   **Pass:** `wait_until_01_00_for_stammdaten` succeeds. `trigger_dw_bert_stammdaten_jp` is triggered and its status remains `running` until the child `dw_bert_stammdaten_jp` DAG completes, then it succeeds.
    *   **Fail:** `trigger_dw_bert_stammdaten_jp` runs before 01:00 UTC, or does not wait for the child DAG to complete.

### Test Case 8: `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` Triggering with Time and Calendar

*   **Purpose:** Verify that `trigger_dw_dwh_run_apt_export_monatlich_jp_evt` is executed only after 01:00 UTC and when the `DW.KALENDER` condition is met (currently a placeholder always returning True), and that it waits for the child DAG to complete (`wait_for_completion=True`).
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Create a placeholder child DAG named `dw_dwh_run_apt_export_monatlich_jp_evt` that succeeds after a short delay.
    3.  **Important:** For a complete test, the `_check_dw_kalender` function needs to be fully implemented according to the `BERT_NICHT` calendar logic. For this test, we assume the placeholder `_check_dw_kalender` (always True) is sufficient or we're testing the *structure* of the calendar check.
*   **Action:**
    1.  Manually trigger the main DAG for an execution date, allowing it to run past 01:00 UTC.
    2.  Observe the status of `trigger_dw_dwh_run_apt_export_monatlich_jp_ev` and the child DAG run.
*   **Pass/Fail Criterion:**
    *   **Pass:** `wait_until_01_00_for_monatlich_export` succeeds. `dw_kalender_check` succeeds (given its current placeholder implementation). `trigger_dw_dwh_run_apt_export_monatlich_jp_ev` is triggered and its status remains `running` until the child `dw_dwh_run_apt_export_monatlich_jp_ev` DAG completes, then it succeeds.
    *   **Fail:** `trigger_dw_dwh_run_apt_export_monatlich_jp_ev` runs before 01:00 UTC, or does not wait for the child DAG to complete, or the calendar check behaves unexpectedly (once implemented).

### Test Case 9: Overall Task Dependencies and Parallel Execution

*   **Purpose:** Verify that the task dependencies are correctly defined, ensuring `sync_guard` runs first, followed by all `DateTimeSensor` tasks in parallel, and then their respective downstream tasks.
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Ensure all child DAGs referenced are deployed as placeholders (as in previous tests).
*   **Action:**
    1.  Manually trigger the main DAG.
    2.  Observe the Airflow UI's Graph View and Gantt Chart for the running DAG.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   `sync_guard` executes and completes successfully as the first task.
        *   All `DateTimeSensor` tasks (`wait_until_20_00_for_monatlich`, `wait_until_07_00_for_adm_check`, etc.) start executing in parallel immediately after `sync_guard` completes.
        *   Downstream tasks (calendar checks, `TriggerDagRunOperator`s) only start after their respective `DateTimeSensor` (and any preceding calendar check) has completed.
    *   **Fail:** Tasks execute out of order, or tasks that should run in parallel are serialized, or tasks that should be serialized run in parallel.

### Test Case 10: Child DAG Existence and Basic Triggerability (Orchestration Data Quality)

*   **Purpose:** Ensure that all `trigger_dag_id` values specified in the `TriggerDagRunOperator` tasks correspond to actual, existing, and parseable DAGs in the Airflow environment. This is an orchestration-level "data quality" check, ensuring the main DAG can successfully initiate its intended child workflows.
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  For each `trigger_dag_id` referenced (e.g., `dw_bert_monatlich_jp`, `dw_bert_run_adm_check_jp_evt`), deploy a minimal placeholder DAG with that `dag_id` that simply succeeds.
*   **Action:**
    1.  Manually trigger the `dw_bert_ablaufsteuerung` DAG.
    2.  Observe the logs of the `TriggerDagRunOperator` tasks and the DAG Runs view for the child DAGs.
*   **Pass/Fail Criterion:**
    *   **Pass:** All `TriggerDagRunOperator` tasks successfully trigger their respective child DAGs, and no errors related to `dag_id` not found or child DAG parsing failures are observed. The child DAG runs appear in the Airflow UI.
    *   **Fail:** Any `TriggerDagRunOperator` task fails because the `trigger_dag_id` does not exist or the child DAG cannot be parsed.

### Test Case 11: Error Handling and Retries

*   **Purpose:** Verify that tasks in the main DAG do not retry on failure, adhering to the `retries=0` specified in `default_args`, matching the UC4 design's lack of explicit retry logic for child tasks.
*   **Setup:**
    1.  Deploy the `dw_bert_ablaufsteuerung` DAG.
    2.  Temporarily modify one of the `PythonOperator` tasks (e.g., `_check_dw_new_calendar`) to intentionally raise an exception (e.g., `raise ValueError("Simulated failure")`).
*   **Action:**
    1.  Manually trigger the `dw_bert_ablaufsteuerung` DAG.
    2.  Observe the task logs and status in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Pass:** The modified task fails immediately on its first attempt, without any retries. The DAG run status reflects the failure.
    *   **Fail:** The task attempts to retry after its initial failure.

---

### Considerations for Child DAGs (Beyond the Scope of this Document)

While these tests cover the orchestration logic of `DW.BERT_ABLAUFSTEUERUNG`, it's crucial to remember that the actual data processing, external system interactions, and detailed data quality checks will occur within the child DAGs (e.g., `dw_bert_monatlich_jp`, `dw_dwh_apt_export_taeglich_jp`). Each of these child DAGs will require its own comprehensive migration validation plan, covering:

*   **Output Parity:** Comparing BigQuery table contents, file outputs (S3/GCS), or external system updates against legacy system outputs for identical inputs.
*   **Transformation Correctness:** Detailed SQL query validation, Python/PySpark logic review, NULL handling, type conversions, and edge case testing.
*   **External-system replacements:**
    *   **Oracle Reads:** Verify data ingestion from Oracle to BigQuery (e.g., CDC, batch extracts) is complete and accurate.
    *   **SFTP/S3 Drops:** Confirm files are correctly generated, named, and dropped to the specified Cloud Storage buckets (or other external systems) with correct permissions and content.
    *   **Shell Script Replacements:** Validate that Python/PySpark/BashOperator replacements for KSH scripts produce identical results or perform equivalent actions.
*   **Data-quality / row-count / schema assertions:**
    *   **Row Counts:** Compare row counts in target BigQuery tables with legacy source/target tables.
    *   **Schema:** Verify BigQuery table schemas match expected structures and data types.
    *   **Data Validation:** Implement checks for data integrity, uniqueness, referential integrity, and business rule adherence using SQL assertions or data quality frameworks (e.g., Great Expectations).
    *   **Performance:** Compare execution times and resource consumption with legacy jobs.

These tests for the main orchestration DAG provide confidence that the control flow is correctly migrated, laying the groundwork for the detailed validation of the underlying data processing.