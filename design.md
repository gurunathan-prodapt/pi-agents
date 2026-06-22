# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
This document outlines the migration plan for the UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG` to a Google Cloud Platform (GCP) BigQuery-centric environment, leveraging Airflow for orchestration. The original UC4 job acts as a master scheduler for various productive data processing workflows related to "Bert". It orchestrates several child Job Plans (JOBP) and Event (EVNT) objects, including monthly and daily export-related processes, master data processing, and administrative housekeeping tasks. The workflow is scheduled with a daily trigger window starting at midnight, incorporating task-level earliest-start time constraints and calendar-based gating.

The scope of this migration is to translate the scheduling and orchestration logic of `DW.BERT_ABLAUFSTEUERUNG` from UC4 XML to an Airflow DAG, ensuring functional equivalence in the target BigQuery ecosystem. The content of the child JOBPs and EVNTs is outside the scope of this document but will be triggered by this Airflow DAG.

## 2. Source Inventory
The primary source artifact for this job is a UC4 Job Scheduler (JSCH) XML file.

*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
    *   **Technology:** UC4/Automic Job Scheduler (XML)
    *   **Complexity Tier:** complex
    *   **Automation Bucket:** manual
    *   **Summary:** This UC4 Job Scheduler orchestrates various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies.

## 3. Target Architecture
The migrated workflow will run on Google Cloud Platform (GCP), orchestrated by Apache Airflow. Each child UC4 Job Plan (JOBP) or Event (EVNT) will be represented as a separate, independently migratable Airflow DAG, triggered by this main `dw_bert_ablaufsteuerung` DAG.

*   **Orchestrator:** Apache Airflow on Cloud Composer
*   **Target Components:**
    *   Airflow DAG: `dw_bert_ablaufsteuerung.py`
    *   Downstream Airflow DAGs (triggered by `TriggerDagRunOperator`):
        *   `dw_bert_monatlich_jp`
        *   `dw_bert_run_adm_check_jp_evt`
        *   `dw_bert_adm_housekeeping_jp`
        *   `dw_dwh_apt_export_taeglich_jp`
        *   `dw_bert_stammdaten_jp`
        *   `dw_dwh_run_apt_export_monatlich_jp_evt`
*   **Scheduling:** Daily at midnight UTC (`0 0 * * *`)
*   **Data Processing:** The child DAGs (corresponding to UC4 JOBPs) are expected to contain the actual data processing logic, likely implemented using BigQuery SQL or PySpark on Dataproc.

## 4. Data Flow & Lineage
The original UC4 JSCH defines a sequence of tasks, where each task invokes a child JOBP or EVNT. The migrated Airflow DAG will replicate this orchestration flow.

**Execution Order (Logical Sequence):**

1.  **`start`**: Airflow DAG initialization.
2.  **`guard_active_run`**: PythonOperator to check for existing active DAG runs, ensuring `max_active_runs=1` and handling the `Else=Skip` UC4 sync behavior. If an active run exists, the current run will be skipped.
3.  **`calendar_check_task_1`**: ShortCircuitOperator or BranchPythonOperator to evaluate the `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05) for `DW.BERT_MONATLICH_JP`.
4.  **`time_sensor_task_1`**: TimeSensor to wait until 20:00 before proceeding to `DW.BERT_MONATLICH_JP`.
5.  **`task_1_dw_bert_monatlich_jp`**: TriggerDagRunOperator to trigger the `dw_bert_monatlich_jp` DAG, waiting for its completion.
6.  **`task_2_dw_bert_run_adm_check_jp_evt`**: TriggerDagRunOperator to trigger the `dw_bert_run_adm_check_jp_evt` DAG as a fire-and-forget task (does not wait for completion).
7.  **`time_sensor_task_3`**: TimeSensor to wait until 04:03 before proceeding to `DW.BERT_ADM_HOUSEKEEPING_JP`.
8.  **`task_3_dw_bert_adm_housekeeping_jp`**: TriggerDagRunOperator to trigger the `dw_bert_adm_housekeeping_jp` DAG, waiting for its completion.
9.  **`task_4_dw_dwh_apt_export_taeglich_jp`**: TriggerDagRunOperator to trigger the `dw_dwh_apt_export_taeglich_jp` DAG as a fire-and-forget task.
10. **`time_sensor_task_5`**: TimeSensor to wait until 01:00 before proceeding to `DW.BERT_STAMMDATEN_JP`.
11. **`task_5_dw_bert_stammdaten_jp`**: TriggerDagRunOperator to trigger the `dw_bert_stammdaten_jp` DAG, waiting for its completion.
12. **`calendar_check_task_6`**: ShortCircuitOperator or BranchPythonOperator to evaluate the `DW.KALENDER` (BERT_NICHT) for `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`.
13. **`task_6_dw_dwh_run_apt_export_monatlich_jp_evt`**: TriggerDagRunOperator to trigger the `dw_dwh_run_apt_export_monatlich_jp_evt` DAG, waiting for its completion.
14. **`end`**: DAG completion.

## 5. Transformation Logic
This migration primarily involves transforming the UC4 XML job scheduling definition into an Airflow Python DAG. There is no direct data transformation within this specific UC4 object, as it is purely an orchestrator. Each UC4 task in the XML will be mapped to a corresponding Airflow operator, primarily `TriggerDagRunOperator` for invoking child DAGs. `TimeSensor` and custom Python operators (`ShortCircuitOperator` or `BranchPythonOperator`) will be used to handle earliest start times and calendar-based conditions, respectively.

**UC4 Element to Airflow Mapping:**

*   **`JSCH` (Job Scheduler)**: Airflow DAG (`dw_bert_ablaufsteuerung`)
*   **`<XHEADER><Title>`**: Becomes the DAG description.
*   **`<SYNCREF>` (`Else="Skip"`)**: Translated to `max_active_runs=1` for the DAG and a `guard_active_run` PythonOperator using `DagRun.find()` at the start of the DAG.
*   **`<ATTR_JSCH><StartTime>` (`00:00`)**: Maps to `schedule="0 0 * * *"` (daily at midnight UTC).
*   **`<ATTR_JSCH><Active>` (`1`)**: Sets `is_paused_upon_creation=False` for the DAG.
*   **`<task OType="JOBP" Object="UC4_JOB_NAME">`**: Maps to `TriggerDagRunOperator` with `trigger_dag_id='uc4_job_name_airflow_dag_id'`.
*   **`<task OType="EVNT" Object="UC4_EVENT_NAME">`**: Maps to `TriggerDagRunOperator` with `trigger_dag_id='uc4_event_name_airflow_dag_id'`.
*   **`<task><after ActFlg="1" ErlstStDays="0" ErlstStTime="HH:MM">`**: `ActFlg="1"` means "wait for completion" (default for `TriggerDagRunOperator`). `ErlstStTime` translates to a `TimeSensor` preceding the `TriggerDagRunOperator`.
*   **`<task><after ActFlg="0">`**: `ActFlg="0"` means "fire and forget". Translates to `wait_for_completion=False` in `TriggerDagRunOperator`.
*   **`<calendars CCTypeOne="1">`**: Requires custom Python logic (e.g., `ShortCircuitOperator` or `BranchPythonOperator`) to implement the calendar-based gating. The specific logic for `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05) and `DW.KALENDER` (BERT_NICHT) will need to be developed as the calendar definitions are not provided in the source XML.

## 6. External Dependencies
The source UC4 job references several child UC4 objects, which themselves might have external dependencies. For this specific orchestration layer, the primary external dependencies are the triggered UC4 JOBPs and EVNTs, which will be migrated to individual Airflow DAGs. There are no direct external system integrations (like Oracle, SFTP, S3) identified within this UC4 Job Scheduler XML itself.

*   **UC4 Child JOBPs/EVNTs:** These are considered internal dependencies within the UC4 ecosystem. In GCP, they will become independent Airflow DAGs. The migration of these child jobs will involve their own design documents and may reveal external system dependencies at that level.

## 7. Unresolved / Risks
*   **Calendar Definitions (High Risk):** The UC4 XML specifies calendar dependencies (`DW.NEW_CALENDAR` with `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05` and `DW.KALENDER` with `BERT_NICHT`). The actual definitions of these calendars are not present in the provided XML. Manual investigation and re-creation of this calendar logic in Airflow (e.g., using `PythonOperator` with specific date-time checks) will be required. This is a critical manual step.
*   **Child Job/Event Migration (External Dependency Risk):** This DAG triggers several other UC4 JOBP/EVNT objects. The successful functioning of this `dw_bert_ablaufsteuerung` DAG in Airflow is dependent on the prior or concurrent migration of these child objects into their respective Airflow DAGs. Any issues in the migration or functionality of child DAGs will impact this orchestrator.
*   **"Earliest Start Time" Precision (Medium Risk):** Task 3 has an earliest-start time of `04:03`. While `TimeSensor` can handle this, it's important to ensure that the Airflow environment's clock synchronization and task scheduling mechanisms can precisely adhere to such non-standard minute offsets.
*   **UC4 Sync `Else=Skip` Implementation:** While `max_active_runs=1` and a `guard_active_run` PythonOperator address this, careful testing is needed to ensure correct behavior in all edge cases (e.g., a DAG run failing after the guard task but before completion, potentially blocking subsequent runs).
*   **Placeholder Replacement:** GCP project IDs, Dataproc regions, cluster names, and GCS bucket names are placeholders and require manual configuration during deployment.

## 8. Build Plan

The build plan focuses on generating a Python Airflow DAG from the design.

**File to Generate:** `dw_bert_ablaufsteuerung.py`

**Language:** Python (for Airflow DAG)

**Steps:**

1.  **Initialize Airflow DAG:** Create the main DAG definition with `dag_id='dw_bert_ablaufsteuerung'`, `schedule='0 0 * * *'`, `start_date` (placeholder), `catchup=False`, `max_active_runs=1`, and `is_paused_upon_creation=False`.
2.  **Define `default_args`:** Set `owner='data-platform'`, `depends_on_past=False`, `retries=0`, and `retry_delay=timedelta(minutes=0)`.
3.  **Implement `guard_active_run` Task:**
    *   Create a `PythonOperator` named `guard_active_run`.
    *   The Python callable should use `DagRun.find()` to check for other active runs of `dw_bert_ablaufsteuerung`.
    *   If an active run is found, raise `AirflowSkipException` to skip the current DAG run.
4.  **Implement `TimeSensor` Tasks:**
    *   Create `TimeSensor` tasks for `time_sensor_task_1` (target 20:00), `time_sensor_task_3` (target 04:03), and `time_sensor_task_5` (target 01:00).
5.  **Implement Calendar Check Tasks:**
    *   Create `ShortCircuitOperator` or `BranchPythonOperator` for `calendar_check_task_1` and `calendar_check_task_6`.
    *   **ACTION REQUIRED:** Develop Python logic within these tasks to replicate the UC4 calendar conditions `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05) and `DW.KALENDER` (BERT_NICHT).
6.  **Implement Child Trigger Tasks (`TriggerDagRunOperator`):**
    *   **`task_1_dw_bert_monatlich_jp`**: Trigger `dw_bert_monatlich_jp`, `wait_for_completion=True`.
    *   **`task_2_dw_bert_run_adm_check_jp_evt`**: Trigger `dw_bert_run_adm_check_jp_evt`, `wait_for_completion=False`.
    *   **`task_3_dw_bert_adm_housekeeping_jp`**: Trigger `dw_bert_adm_housekeeping_jp`, `wait_for_completion=True`.
    *   **`task_4_dw_dwh_apt_export_taeglich_jp`**: Trigger `dw_dwh_apt_export_taeglich_jp`, `wait_for_completion=False`.
    *   **`task_5_dw_bert_stammdaten_jp`**: Trigger `dw_bert_stammdaten_jp`, `wait_for_completion=True`.
    *   **`task_6_dw_dwh_run_apt_export_monatlich_jp_evt`**: Trigger `dw_dwh_run_apt_export_monatlich_jp_evt`, `wait_for_completion=True`.
7.  **Define Task Dependencies:** Establish the linear flow of tasks as outlined in Section 4 using Airflow's `>>` operator.
    *   `guard_active_run >> calendar_check_task_1 >> time_sensor_task_1 >> task_1_dw_bert_monatlich_jp`
    *   `task_1_dw_bert_monatlich_jp >> task_2_dw_bert_run_adm_check_jp_evt`
    *   `task_2_dw_bert_run_adm_check_jp_evt >> time_sensor_task_3 >> task_3_dw_bert_adm_housekeeping_jp`
    *   `task_3_dw_bert_adm_housekeeping_jp >> task_4_dw_dwh_apt_export_taeglich_jp`
    *   `task_4_dw_dwh_apt_export_taeglich_jp >> time_sensor_task_5 >> task_5_dw_bert_stammdaten_jp`
    *   `task_5_dw_bert_stammdaten_jp >> calendar_check_task_6 >> task_6_dw_dwh_run_apt_export_monatlich_jp_evt`
8.  **Replace Placeholders:** Substitute `{{ placeholder_start_date }}` and GCP configuration placeholders (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) with actual values.