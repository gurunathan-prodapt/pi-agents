# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope

This migration design document outlines the conversion of the UC4 Job Scheduler (JSCH) `DW.BERT_ABLAUFSTEUERUNG` to an Airflow Directed Acyclic Graph (DAG) for execution on Google Cloud Platform. The original UC4 job orchestrates several productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies. The target platform is Google BigQuery for data processing, orchestrated by Airflow.

## 2. Source Inventory

The job `DW.BERT_ABLAUFSTEUERUNG` is defined by a single UC4 XML file:

*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
    *   **Technology:** UC4/Automic (XML configuration for a Job Scheduler - JSCH)
    *   **Tier:** Not determined (no `file_complexity` entry)
    *   **Automation Bucket:** Not determined (no `automation_rate` entry)
    *   **Summary:** This UC4 Job Scheduler orchestrates various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies.

## 3. Target Architecture

The UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG` will be migrated to an Airflow DAG. The invoked UC4 `JOBP` and `EVNT` objects will likely be converted to separate Airflow DAGs (or specific tasks within other DAGs) and triggered by this orchestrating DAG. Data processing logic within these child jobs is assumed to be migrated to BigQuery SQL, PySpark on Dataproc, or other suitable Google Cloud services.

**Airflow DAG Properties:**

*   **dag_id:** `dw_bert_ablaufsteuerung`
*   **schedule:** `0 0 * * *` (Daily at midnight, reflecting UC4's `StartTime=00:00` and `Period=1`)
*   **start_date:** `{{ placeholder_start_date }}`
*   **catchup:** `False`
*   **max_active_runs:** `1`
*   **is_paused_upon_creation:** `False`
*   **default_args.owner:** `data-engineering`
*   **default_args.retries:** `0` (unless specified in child job migration designs)
*   **default_args.retry_delay:** `timedelta(minutes=0)` (unless specified in child job migration designs)

## 4. Data Flow & Lineage

The original UC4 JSCH `DW.BERT_ABLAUFSTEUERUNG` orchestrates the following child objects in a sequential manner, with time-based and calendar-based dependencies:

1.  `JOBP:DW.BERT_MONATLICH_JP`: This monthly job plan is subject to calendar constraints (`DW.NEW_CALENDAR` on `DAY_OF_MONTH_25` and `DAY_OF_MONTH_05`) and an earliest start time of `20:00`. It acts with `ActFlg=1`, implying the parent waits for its completion.
2.  `EVNT:DW.BERT_RUN_ADM_CHECK_JP_EVT`: An event triggered after `07:00`. It acts with `ActFlg=0`, indicating a fire-and-forget mechanism (parent does not wait for completion).
3.  `JOBP:DW.BERT_ADM_HOUSEKEEPING_JP`: An administrative housekeeping job plan triggered after `04:03`. It acts with `ActFlg=1`.
4.  `JOBP:DW.DWH_APT_EXPORT_TAEGLICH_JP`: A daily APT export job plan triggered after `01:30`. It acts with `ActFlg=0`.
5.  `JOBP:DW.BERT_STAMMDATEN_JP`: A master data job plan triggered after `01:00`. It acts with `ActFlg=1`.
6.  `EVNT:DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`: A monthly DWH export event, gated by calendar `DW.KALENDER` with a `BERT_NICHT` constraint. It acts with `ActFlg=1`.

The overall execution order in Airflow will be a linear sequence, incorporating sensors for time-based triggers and conditional logic for calendar-based triggers. A guard task will be implemented at the start of the DAG to handle the `Else=Skip` logic of the UC4 outer sync.

**Plain-English Flow:**
- The DAG will first check for other active runs and skip if one exists.
- It will then evaluate the calendar for the monthly Bert workflow.
- If the calendar condition is met, the monthly Bert workflow will be triggered after 20:00, and the current DAG will wait for its completion.
- After the monthly Bert workflow (or immediately if skipped/not applicable), the admin check event will be triggered after 07:00, without waiting for its completion.
- Next, the housekeeping workflow will be triggered after 04:03, and its completion will be awaited.
- The daily export workflow will follow, triggered after 01:30, without waiting for completion.
- The master data workflow will be triggered after 01:00, and its completion will be awaited.
- Finally, the monthly DWH export event will be triggered after its calendar conditions are met, and its completion will be awaited.

## 5. Transformation Logic

The core transformation is from UC4 scheduling constructs to Airflow operators.

*   **UC4 JSCH `DW.BERT_ABLAUFSTEUERUNG` → Airflow DAG `dw_bert_ablaufsteuerung`**
    *   The overall scheduler structure will be mapped to a Python-based Airflow DAG definition.
    *   The `StartTime=00:00` and `Period=1` will translate to a daily cron schedule (`0 0 * * *`).
*   **UC4 `SYNCREF` with `Else=Skip` → Airflow `PythonOperator` guard task**
    *   A `PythonOperator` task named `guard_active_run` will be implemented at the start of the DAG. This task will check for existing active DAG runs using `DagRun.find()` and raise an `AirflowSkipException` if another run is active, mimicking the `Else=Skip` behavior.
*   **UC4 `task` objects (`JOBP`, `EVNT`) → Airflow `TriggerDagRunOperator`**
    *   Each invoked UC4 job plan or event (`DW.BERT_MONATLICH_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`, etc.) will be represented by an `TriggerDagRunOperator` task in the main `dw_bert_ablaufsteuerung` DAG.
    *   The `trigger_dag_id` will be a sanitized version of the UC4 object name (e.g., `dw_bert_monatlich_jp`).
    *   The `ActFlg` attribute in UC4 tasks will determine `wait_for_completion`:
        *   `ActFlg=1` (e.g., `DW.BERT_MONATLICH_JP`) will set `wait_for_completion=True`.
        *   `ActFlg=0` (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`) will set `wait_for_completion=False`.
*   **UC4 `ErlstStTime` (`after` attribute) → Airflow `TimeSensor`**
    *   Earliest start times like `20:00`, `07:00`, `04:03`, `01:30`, `01:00` will be implemented using `TimeSensor` tasks that precede their respective `TriggerDagRunOperator` tasks.
    *   Non-round offsets like `04:03` will be preserved accurately.
*   **UC4 `calendars` → Airflow `ShortCircuitOperator` or `BranchPythonOperator`**
    *   Calendar dependencies (e.g., `DW.NEW_CALENDAR` with `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`; `DW.KALENDER` with `BERT_NICHT`) will require `ShortCircuitOperator` or `BranchPythonOperator` tasks. These tasks will contain custom Python logic to check the current execution date against the specified calendar rules.
    *   **Note:** The definitions of `DW.NEW_CALENDAR` and `DW.KALENDER` were not available in the source XML; manual implementation and validation of these calendar logics will be required during the build phase.
*   **Variables:** No explicit UC4 variables were found in the `dynvalues` sections for global or task-specific variables. If child jobs require variables, these will be managed within their respective DAGs or passed via Airflow's `conf` parameter.

## 6. External Dependencies

Based on the provided UC4 XML, there are no direct external system dependencies (e.g., Oracle, SFTP, S3) defined within this scheduler itself. All invoked objects are other UC4 `JOBP` or `EVNT` objects.

Any external system interactions will be managed by the downstream job plans (`JOBP`) and events (`EVNT`) that this scheduler invokes. The migration of those child objects will need to address their specific external dependencies.

## 7. Unresolved / Risks

*   **Calendar Definitions:** The specific definitions for `DW.NEW_CALENDAR` and `DW.KALENDER` are not present in the provided XML. These need to be manually recreated or extracted from other UC4 metadata to ensure correct scheduling logic in Airflow. This is a manual review and implementation step.
*   **Child Job Migration:** This design focuses solely on the orchestration layer. The actual migration of the invoked `JOBP` and `EVNT` objects (`DW.BERT_MONATLICH_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`, etc.) to BigQuery-compatible jobs (e.g., Dataproc PySpark, BigQuery SQL, Cloud Functions) is a separate, downstream effort.
*   **Error Handling/Retries:** No explicit retry counts or wait times were found in the UC4 JSCH tasks. The Airflow DAG will default to no retries. If specific retry behavior is required, it must be determined from the child job definitions or business requirements.
*   **Dynamic Variables/Prompt Sets:** The `dynvalues` section in the XML was empty for this JSCH. If child jobs utilize UC4 PromptSets or dynamic variables, their mapping to Airflow parameters or XComs will need to be handled in their respective migration designs.

## 8. Build Plan

The migration involves generating a Python Airflow DAG file.

1.  **Generate `dw_bert_ablaufsteuerung.py` (Airflow DAG):**
    *   **Language:** Python
    *   **Purpose:** Orchestrates the child data processing workflows.
    *   **Components:**
        *   Standard Airflow DAG definition (`DAG` object).
        *   `PythonOperator` for `guard_active_run` task (concurrency control).
        *   `TimeSensor` tasks for `ErlstStTime` constraints.
        *   `ShortCircuitOperator` or `BranchPythonOperator` tasks for calendar-based conditions.
        *   `TriggerDagRunOperator` tasks for each child UC4 `JOBP` and `EVNT`, configured with `wait_for_completion=True/False` based on UC4 `ActFlg`.
        *   Dependencies defined using Airflow's bitshift operators (`>>`).
    *   **Placeholders:**
        *   `{{ placeholder_start_date }}` for `start_date`.
        *   `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME` for GCP environment settings if Dataproc is used by child jobs.
        *   Manual logic to be implemented for `DW.NEW_CALENDAR` and `DW.KALENDER` within the calendar check tasks.

This Airflow DAG will serve as the entry point for the migrated Bert data processing workflows on Google Cloud.