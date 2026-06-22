# MIGRATION_NOTES.md for DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG`, responsible for orchestrating various productive 'Bert' workflows, has been migrated. The original XML definition (`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`) has been re-platformed to a BigQuery-native environment, specifically using Apache Airflow (Cloud Composer) for orchestration.

The migration translates the UC4 scheduling logic, task dependencies, earliest start times, and calendar-based execution constraints into a single Airflow Directed Acyclic Graph (DAG) named `dw_bert_ablaufsteuerung`. This DAG acts as a master scheduler, triggering other downstream Airflow DAGs that correspond to the original UC4 child Job Plans (JOBP) and Events (EVNT).

## 2. Generated artifacts

The migration process generated the following primary artifact:

*   **`dags/dw_bert_ablaufsteuerung.py`**:
    *   **Role**: This Python file defines the main Airflow DAG responsible for orchestrating the Bert workflows. It includes tasks for enforcing earliest start times (`TimeSensor`), implementing UC4's `Else=Skip` concurrency behavior (`PythonOperator`), and triggering child DAGs (`TriggerDagRunOperator`). It also contains placeholder `PythonOperator` tasks for the UC4 calendar logic that requires manual implementation.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Airflow as the Orchestration Engine**: Apache Airflow, deployed on Google Cloud Composer, was chosen as the target orchestration platform due to its flexibility, scalability, and native integration with GCP services.
*   **Master DAG for Orchestration**: Instead of a monolithic DAG, a master DAG (`dw_bert_ablaufsteuerung`) was created to mirror the UC4 Job Scheduler's role. This master DAG's primary function is to trigger other, more granular DAGs (corresponding to the original UC4 JOBP/EVNT objects), promoting modularity and reusability.
*   **`TriggerDagRunOperator` for Child Jobs**: Each UC4 child Job Plan (JOBP) or Event (EVNT) is represented as a separate Airflow DAG. The master DAG uses `TriggerDagRunOperator` to initiate these child DAGs, ensuring proper dependency management and allowing for independent development and deployment of sub-workflows. `wait_for_completion=True` is set for all triggered DAGs to replicate the sequential execution and dependency of the original UC4 scheduler.
*   **`TimeSensor` for Earliest Start Times**: UC4's "Earliest Start Time" constraint is translated into Airflow's `TimeSensor` tasks. These sensors pause the DAG execution until the specified time of day is reached, accurately replicating the original timing requirements.
*   **`Else=Skip` Concurrency Handling**: The UC4 `Else=Skip` behavior, which prevents concurrent runs of the same job, is implemented using a custom `PythonOperator` (`_guard_active_run`). This task checks for active DAG runs and raises an `AirflowSkipException` if another instance is already running, ensuring only one instance of the `dw_bert_ablaufsteuerung` DAG runs at a time.
*   **Placeholder for UC4 Calendar Logic**: UC4's complex calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`) are represented by placeholder `PythonOperator` tasks. This decision acknowledges the need for manual analysis and implementation of these specific calendar rules, as their exact logic was not fully derivable from the source XML.
*   **Sequential Execution**: The original sequential flow of tasks within the UC4 Job Scheduler is strictly maintained in the Airflow DAG's task dependencies, ensuring the same order of operations.

## 4. Manual steps before go-live

Before the `dw_bert_ablaufsteuerung` DAG can be deployed and go live, the following manual steps are required:

1.  **Update Configuration Placeholders**:
    *   Edit `dags/dw_bert_ablaufsteuerung.py` and replace the placeholder values for:
        *   `GCP_PROJECT_ID`
        *   `DATAPROC_REGION`
        *   `DATAPROC_CLUSTER_NAME` (if child DAGs use Dataproc)
        *   `GCS_BUCKET_NAME` (if child DAGs use GCS)
        *   `PLACEHOLDER_START_DATE`: Set this to the desired historical or current start date for the DAG.
2.  **Implement UC4 Calendar Logic**:
    *   **Critical Step**: Manually analyze the definitions of UC4 calendars `DW.NEW_CALENDAR` (keys `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`) and `DW.KALENDER` (key `BERT_NICHT`) from the legacy UC4 system.
    *   Update the `_calendar_check_dw_bert_monatlich_jp` and `_calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt` Python functions in `dags/dw_bert_ablaufsteuerung.py` with the actual logic. These functions should raise an `AirflowSkipException` if the current date does not meet the calendar criteria.
3.  **Migrate and Deploy Child DAGs**:
    *   Ensure that all downstream DAGs triggered by `dw_bert_ablaufsteuerung` (e.g., `dw_bert_monatlich_jp`, `dw_bert_run_adm_check_jp_evt`, `dw_bert_adm_housekeeping_jp`, `dw_dwh_apt_export_taeglich_jp`, `dw_bert_stammdaten_jp`, `dw_dwh_run_apt_export_monatlich_jp_evt`) have been successfully migrated, developed, and deployed to the Airflow environment. Their `dag_id`s must exactly match those specified in the `TriggerDagRunOperator` tasks.
4.  **Define DAG Schedule**:
    *   The `schedule` parameter for the `dw_bert_ablaufsteuerung` DAG is currently set to `None`. Determine the appropriate schedule (e.g., a cron expression like `'0 0 * * *'` for daily at midnight, or `None` if it's meant to be triggered externally) and update the `schedule` argument in the DAG definition.
5.  **IAM/Permissions**:
    *   Verify that the Airflow service account (used by Cloud Composer) has the necessary IAM permissions to:
        *   Trigger other Airflow DAGs.
        *   Access any GCP resources (e.g., BigQuery datasets, Dataproc clusters, GCS buckets) that the child DAGs might interact with.
6.  **Connection Strings/Secrets**:
    *   While this specific orchestration DAG does not directly use external connections, ensure that any child DAGs have their required Airflow Connections and/or secrets configured (e.g., in Secret Manager or Airflow Connections UI).
7.  **Deployment to Cloud Composer**:
    *   Upload the `dags/dw_bert_ablaufsteuerung.py` file to the DAGs folder of your Cloud Composer environment.

## 5. Known gaps & unresolved references

*   **UC4 Calendar Logic (B4 Item)**: The most significant gap is the placeholder implementation for the UC4 calendars `DW.NEW_CALENDAR` and `DW.KALENDER`. The exact logic for `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, and `BERT_NICHT` needs to be manually extracted from the legacy UC4 system and implemented in the corresponding Python functions. This is flagged as a "B4" item, indicating a required redesign/manual implementation.
*   **Downstream DAG Content**: The migration of the child `JOBP` and `EVNT` objects (e.g., `DW.BERT_MONATLICH_JP`) into their respective Airflow DAGs is assumed but not covered by this migration document. Their content and functionality need to be fully migrated and tested independently.
*   **`schedule` Parameter**: The `schedule` parameter for the `dw_bert_ablaufsteuerung` DAG is currently `None`. Its final value needs to be determined and set based on the operational requirements.
*   **Complexity Tier**: The source job was categorized as `complex` and in a `manual` automation bucket, reinforcing that the calendar logic and overall integration require careful manual attention.
*   **No `JOBS_UNIX` Objects**: The absence of `JOBS_UNIX` objects in the source XML means the actual data processing logic within the child `JOBP`s is not directly visible from this scheduler's definition. This implies that the content of the child DAGs will need thorough analysis during their individual migration.

## 6. Validation

To validate the successful migration and functionality of the `dw_bert_ablaufsteuerung` DAG, perform the following steps:

1.  **DAG Parsing Check**:
    *   Ensure the DAG file `dags/dw_bert_ablaufsteuerung.py` is successfully parsed by Airflow without syntax errors. Check the Airflow UI for the DAG to appear and be unpaused.
2.  **Unit Testing (Local)**:
    *   Test the `_guard_active_run` function by simulating concurrent DAG runs to ensure it correctly skips subsequent runs.
    *   Test the implemented calendar logic functions (`_calendar_check_dw_bert_monatlich_jp`, `_calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt`) with various dates (e.g., calendar days, non-calendar days) to verify correct execution and skipping behavior.
3.  **Manual Trigger and Monitoring (Cloud Composer)**:
    *   Manually trigger the `dw_bert_ablaufsteuerung` DAG from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI's Graph View and Logs.
    *   **Verify `guard_active_run`**: Trigger the DAG a second time while the first run is still active. The second run should be skipped by the `guard_active_run` task.
    *   **Verify `TimeSensor` tasks**: Observe that the `TimeSensor` tasks (`wait_until_...`) correctly pause execution until their target time is reached.
    *   **Verify Calendar Checks**: For calendar-dependent tasks, ensure they either execute or skip based on the implemented calendar logic and the `logical_date` of the DAG run.
    *   **Verify `TriggerDagRunOperator`**: Confirm that each `TriggerDagRunOperator` successfully triggers its corresponding child DAG. Check the logs for messages indicating successful triggering.
    *   **Verify Child DAG Completion**: Ensure that the master DAG waits for the triggered child DAGs to complete before proceeding to the next task. Monitor the child DAGs' runs to ensure they execute successfully.
    *   **Verify Sequential Execution**: Confirm that tasks execute in the defined sequential order.
4.  **Scheduled Run (Cloud Composer)**:
    *   Once manual triggers are successful, enable the DAG's schedule (if defined) and monitor its automatic execution over several cycles.
    *   Check logs for any errors or unexpected behavior.

**"Passing" means**:
*   The `dw_bert_ablaufsteuerung` DAG completes successfully without any failed tasks.
*   All `TimeSensor` tasks correctly enforce their earliest start times.
*   The `_guard_active_run` task correctly prevents concurrent DAG runs.
*   The implemented calendar logic correctly skips or executes tasks based on the defined calendar rules.
*   All `TriggerDagRunOperator` tasks successfully trigger their respective child DAGs.
*   All triggered child DAGs complete successfully.
*   The overall execution flow and timing match the expected behavior of the original UC4 Job Scheduler.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after deploying the `dw_bert_ablaufsteuerung` Airflow DAG, follow this rollback procedure:

1.  **Disable New Airflow DAG**:
    *   In the Airflow UI, pause the `dw_bert_ablaufsteuerung` DAG to prevent any further scheduled or manual runs.
    *   Consider deleting the DAG file from the Composer environment's DAGs folder if the issue is severe and requires a complete removal.
2.  **Re-enable Original UC4 Job Scheduler**:
    *   Immediately re-enable the original `DW.BERT_ABLAUFSTEUERUNG` Job Scheduler in the UC4/Automic system.
    *   Ensure that any jobs that were partially run or missed during the Airflow migration attempt are either re-run or manually reconciled in UC4.
3.  **Monitor UC4 System**:
    *   Closely monitor the UC4 system to ensure that the re-enabled scheduler and its child jobs resume normal operation without any adverse effects.
4.  **Analyze and Rectify**:
    *   Investigate the root cause of the failure in the Airflow DAG using logs, metrics, and code review. Address the identified issues (e.g., incorrect calendar logic, misconfigured dependencies, permission issues).
    *   Once the issues are resolved, the migration process can be re-attempted.