# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
This document outlines the migration design for the `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler (JSCH) to a Google Cloud Platform (GCP) native Airflow Directed Acyclic Graph (DAG) for orchestration. The original UC4 job orchestrates several productive processes related to "Bert," including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. The migration aims to translate this orchestration logic into an equivalent Airflow DAG, leveraging BigQuery for data processing.

## 2. Source Inventory
The primary source component for this job is a single UC4 XML file defining the Job Scheduler.

| File Path | Technology | Complexity Tier | Automation Bucket | Summary |
|:---|:---|:---|:---|:---|
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml` | UC4/Automic | complex | manual | This UC4 Job Scheduler (JSCH) orchestrates various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies. |

**UC4 Object Inventory:**

| Object Name | Object Type | Active Flag | Description |
|:---|:---|:---:|:---|
| `DW.BERT_ABLAUFSTEUERUNG` | JSCH | 1 | Main scheduler for Bert production workflows |
| `DW.BERT_MONATLICH_JP` | JOBP | N/A | Monthly job plan referenced by task 1 |
| `DW.BERT_RUN_ADM_CHECK_JP_EVT` | EVNT | N/A | Event object referenced by task 2 |
| `DW.BERT_ADM_HOUSEKEEPING_JP` | JOBP | N/A | Admin housekeeping job plan referenced by task 3 |
| `DW.DWH_APT_EXPORT_TAEGLICH_JP` | JOBP | N/A | Daily export job plan referenced by task 4 |
| `DW.BERT_STAMMDATEN_JP` | JOBP | N/A | Master data job plan referenced by task 5 |
| `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` | EVNT | N/A | Monthly export event referenced by task 6 |

*Note: Only the JSCH file is present. The referenced JOBP/EVNT objects are not included, so their internal structure, retries, and downstream tasks cannot be fully analyzed here.*

## 3. Target Architecture
The target architecture will utilize Airflow on Google Cloud Composer for job orchestration. Each invoked UC4 JOBP will correspond to a separate Airflow DAG, triggered by `TriggerDagRunOperator`. UC4 EVNT objects will be represented by placeholder tasks until their definitions are available. Data processing will occur in BigQuery.

**Airflow DAG Properties:**

| Property | Value |
|:---|:---|
| dag_id | `dw_bert_ablaufsteuerung` |
| schedule | `0 0 * * *` (Daily at midnight) |
| start_date | `{{ PLACEHOLDER_START_DATE }}` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `data-platform` |
| default_args.retries | `0` (unless overridden per task) |
| default_args.retry_delay | `timedelta(minutes=0)` (unless overridden per task) |

## 4. Data Flow & Lineage
The `DW.BERT_ABLAUFSTEUERUNG` UC4 JSCH orchestrates a series of job plans (JOBP) and event-triggered jobs (EVNT) in a sequential manner, with specific time and calendar-based dependencies.

**Execution Order (Airflow Task Dependencies):**
1.  `guard_active_run`: Ensures only one active run of this DAG exists (due to `Else=Skip` on UC4 sync).
2.  `wait_until_20_00_for_monthly_jp`: A `TimeSensor` waiting for 20:00.
3.  `calendar_check_dw_new_calendar`: A `ShortCircuitOperator` checking for calendar `DW.NEW_CALENDAR` rules (manual logic required).
4.  `trigger_dw_bert_monatlich_jp`: Triggers the `dw_bert_monatlich_jp` DAG, waiting for its completion.
5.  `wait_until_04_03_for_housekeeping_jp`: A `TimeSensor` waiting for 04:03.
6.  `calendar_check_dw_new_calendar`: Another `ShortCircuitOperator` checking the same calendar rules.
7.  `trigger_dw_bert_adm_housekeeping_jp`: Triggers the `dw_bert_adm_housekeeping_jp` DAG, waiting for its completion.
8.  `wait_until_01_30_for_daily_export_jp`: A `TimeSensor` waiting for 01:30.
9.  `trigger_dw_dwh_apt_export_taeglich_jp`: Triggers the `dw_dwh_apt_export_taeglich_jp` DAG, **not** waiting for its completion (`wait_for_completion=False` due to source `ActFlg=0`).
10. `wait_until_01_00_for_stammdaten_jp`: A `TimeSensor` waiting for 01:00.
11. `trigger_dw_bert_stammdaten_jp`: Triggers the `dw_bert_stammdaten_jp` DAG, waiting for its completion.
12. `wait_until_07_00_for_adm_check_evt`: A `TimeSensor` waiting for 07:00.
13. `trigger_dw_bert_run_adm_check_jp_evt`: Triggers a placeholder task for `dw_bert_run_adm_check_jp_evt` event, **not** waiting for completion.
14. `wait_until_01_00_for_monthly_export_evt`: A `TimeSensor` waiting for 01:00.
15. `trigger_dw_dwh_run_apt_export_monatlich_jp_evt`: Triggers a placeholder task for `dw_dwh_run_apt_export_monatlich_jp_evt` event, waiting for completion.

## 5. Transformation Logic
This design document focuses on the orchestration logic of the main UC4 JSCH. The transformation logic for the actual data processing (e.g., SQL queries, PySpark jobs) will reside within the individual DAGs corresponding to `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, and `DW.BERT_STAMMDATEN_JP`. These nested job plans will need their own migration designs to translate their internal logic (e.g., Ab Initio graphs, shell scripts, SQL) into BigQuery SQL or PySpark.

The current `DW.BERT_ABLAUFSTEUERUNG` JSCH primarily defines task sequencing, dependencies, and timing.

## 6. External Dependencies
The provided UC4 XML for `DW.BERT_ABLAUFSTEUERUNG` does not explicitly detail external system interactions (e.g., Oracle, SFTP, S3) beyond the orchestration of child job plans/events. Any such external dependencies would reside within the invoked `JOBP` or `EVNT` objects.

**Dependency Replacement Strategy:**
*   **UC4 Job Plans (JOBP):** Will be replaced by dedicated Airflow DAGs.
*   **UC4 Events (EVNT):** Will be replaced by placeholder tasks or specific Airflow operators (e.g., S3/GCS sensors, Pub/Sub sensors) once their definitions are available.
*   **Calendars (`DW.NEW_CALENDAR`, `DW.KALENDER`):** Require manual implementation in Airflow using Python logic within a `ShortCircuitOperator`, as their definitions are not provided.
*   **GCP Placeholders:**
    *   `YOUR_GCP_PROJECT_ID`: Dataproc project ID for PySpark tasks.
    *   `YOUR_DATAPROC_REGION`: Dataproc region.
    *   `YOUR_DATAPROC_CLUSTER_NAME`: Dataproc cluster name.
    *   `YOUR_BUCKET_NAME`: GCS bucket for PySpark scripts.

## 7. Unresolved / Risks
*   **Missing Child Job Plan/Event Definitions:** The internal logic, transformations, and external dependencies of `DW.BERT_MONATLICH_JP`, `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_STAMMDATEN_JP`, and `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` are unknown. Each of these will require its own separate migration design and implementation.
*   **Calendar Logic:** The definitions for `DW.NEW_CALENDAR` and `DW.KALENDER` are not provided. Their exact logic for determining execution days needs to be manually reconstructed and implemented within the `ShortCircuitOperator` tasks.
*   **Retry and Error Handling:** No detailed retry or post-condition logic was found in the JSCH XML for the referenced objects. Default Airflow retry policies will be applied, but custom error handling might be needed upon further analysis of the child jobs.
*   **UC4 Event Objects:** Without definitions for `EVNT` objects, their Airflow implementation remains a placeholder, likely a `TriggerDagRunOperator` for an event-driven DAG or a simple `PythonOperator` if no direct Airflow event sensor matches.
*   **`r_ai_start` commands:** If any nested JOBPs contain `r_ai_start` commands (Ab Initio), their graph names, job keys, and types are not extracted and will require further analysis.

## 8. Build Plan
The build plan focuses on generating a single Airflow DAG Python file for `dw_bert_ablaufsteuerung`.

1.  **Generate Airflow DAG for `dw_bert_ablaufsteuerung` (Python):**
    *   Create a Python file named `dw_bert_ablaufsteuerung.py`.
    *   Implement the DAG definition with `dag_id='dw_bert_ablaufsteuerung'`, `schedule='0 0 * * *'`, `catchup=False`, `max_active_runs=1`, `is_paused_upon_creation=False`.
    *   Define a `PythonOperator` named `guard_active_run` to implement the `Else=Skip` logic from the UC4 sync. This task will check for other running instances of the DAG.
    *   For each task with an `ErlstStTime`, create a `TimeSensor` task (e.g., `wait_until_20_00_for_monthly_jp`).
    *   For tasks with calendar constraints, create `ShortCircuitOperator` tasks (e.g., `calendar_check_dw_new_calendar`, `calendar_check_dw_kalender`). These will require manual logic for the calendar definitions.
    *   For each `JOBP` invocation, create a `TriggerDagRunOperator` (e.g., `trigger_dw_bert_monatlich_jp`). Set `wait_for_completion=True` by default, except for `trigger_dw_dwh_apt_export_taeglich_jp` which will be `False` due to `ActFlg=0`.
    *   For each `EVNT` invocation, create a placeholder `TriggerDagRunOperator` or `PythonOperator` (e.g., `trigger_dw_bert_run_adm_check_jp_evt`). Set `wait_for_completion` as per the source (False for `DW.BERT_RUN_ADM_CHECK_JP_EVT`, True for `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`).
    *   Define task dependencies as described in Section 4.
    *   Include placeholder variables for GCP project ID, region, cluster name, and GCS bucket.
    *   Add placeholder functions for `on_failure_callback` if more detailed error handling is required later.
2.  **Migrate Child Job Plans/Events:** (Separate design documents/builds required)
    *   For each `JOBP` (e.g., `DW.BERT_MONATLICH_JP`), create a new design document to migrate its internal logic to BigQuery SQL, PySpark, or other suitable GCP services, and orchestrate it with a dedicated Airflow DAG.
    *   For each `EVNT` (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`), create a new design document to implement its event-driven logic using GCP Pub/Sub, Cloud Functions, or dedicated Airflow sensors/operators.