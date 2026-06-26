# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope

The `DW.BERT_ABLAUFSTEUERUNG` job serves as a central scheduler within the legacy UC4 Automic environment. Its primary purpose is to orchestrate and initiate various productive processes related to "Bert," likely a specific business domain or data product. This job is an XML-based UC4 Job Scheduler (JSCH) that triggers several other UC4 Job Plans (JOBP) and Events (EVNT) based on predefined schedules and calendar conditions. The scope of this migration is to re-platform this UC4 scheduling logic to Google Cloud Composer (Apache Airflow) as part of a broader data platform migration to BigQuery.

## 2. Source Inventory

This job is composed of a single UC4 XML file:

*   **File Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
*   **Technology**: UC4 (Automic) Job Scheduler (XML)
*   **File Purpose**: Scheduler / Orchestration
*   **Complexity Tier**: `complex`
*   **Migration Bucket**: `manual` (B3) - indicating significant manual effort and potential redesign.
*   **Summary**: The XML defines a UC4 `JSCH` object named `DW.BERT_ABLAUFSTEUERUNG` which contains multiple task definitions. These tasks invoke other UC4 objects (`JOBP` for Job Plans and `EVNT` for Events) with specific start times and calendar dependencies.

## 3. Target Architecture

The `DW.BERT_ABLAUFSTEUERUNG` job will be migrated to Google Cloud Composer, utilizing Apache Airflow as the scheduling and orchestration engine.

*   **Scheduler**: Google Cloud Composer (Airflow)
*   **Orchestration**: A single Python Airflow DAG will be created to represent `DW.BERT_ABLAUFSTEUERUNG`.
*   **Sub-Jobs/Events**: Each `JOBP` or `EVNT` object currently invoked by `DW.BERT_ABLAUFSTEUERUNG` will either be migrated to its own Airflow DAG (if it represents an independent workflow) and triggered by the main `DW.BERT_ABLAUFSTEUERUNG` DAG using `TriggerDagRunOperator`, or will be re-implemented as individual tasks within the `DW.BERT_ABLAUFSTEUERUNG` DAG if they are tightly coupled and do not warrant separate DAGs.
*   **Data Processing**: Any data processing logic currently residing within the `JOBP`s will be re-platformed to BigQuery SQL, Python scripts, or PySpark jobs, executed via appropriate Airflow operators (e.g., `BigQueryOperator`, `PythonOperator`, `BashOperator`).

## 4. Data Flow & Lineage

The legacy UC4 `DW.BERT_ABLAUFSTEUERUNG` job orchestrates a sequence of internal UC4 objects. The data flow primarily represents control flow and triggering mechanisms.

**Legacy Flow (UC4):**

`DW.BERT_ABLAUFSTEUERUNG` (JSCH)
    triggers `DW.BERT_MONATLICH_JP` (JOBP) - monthly, 20:00 (on 5th and 25th of month)
    triggers `DW.BERT_RUN_ADM_CHECK_JP_EVT` (EVNT) - daily, 07:00
    triggers `DW.BERT_ADM_HOUSEKEEPING_JP` (JOBP) - daily, 04:03
    triggers `DW.DWH_APT_EXPORT_TAEGLICH_JP` (JOBP) - daily, 01:30
    triggers `DW.BERT_STAMMDATEN_JP` (JOBP) - daily, 01:00
    triggers `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` (EVNT) - monthly, 01:00 (excluding `BERT_NICHT` days)

**Target Flow (Airflow):**

A main Airflow DAG, tentatively named `bert_ablaufsteuerung_dag`, will be created. This DAG will contain tasks that correspond to the invocation of the sub-jobs/events.

*   **Scheduling**: The main DAG will define a schedule that encompasses the most frequent triggers (e.g., daily). Within the DAG, conditional branching or specific sensor operators will handle monthly and time-of-day specific execution.
*   **Tasks**:
    *   `trigger_bert_monatlich_jp`: A task to trigger the `bert_monatlich_jp` DAG (or its equivalent tasks). This will incorporate the monthly calendar logic (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`) and the `20:00` start time.
    *   `trigger_bert_run_adm_check_jp_evt`: A task for the `bert_run_adm_check_jp_evt` event, scheduled for `07:00`.
    *   `trigger_bert_adm_housekeeping_jp`: A task for the `bert_adm_housekeeping_jp` DAG (or its equivalent tasks), scheduled for `04:03`.
    *   `trigger_dwh_apt_export_taeglich_jp`: A task for the `dwh_apt_export_taeglich_jp` DAG (or its equivalent tasks), scheduled for `01:30`.
    *   `trigger_bert_stammdaten_jp`: A task for the `bert_stammdaten_jp` DAG (or its equivalent tasks), scheduled for `01:00`.
    *   `trigger_dwh_run_apt_export_monatlich_jp_evt`: A task for the `dwh_run_apt_export_monatlich_jp_evt` event, scheduled for `01:00` with exclusion logic based on `BERT_NICHT` calendar.
*   **Dependencies**: Airflow task dependencies (`>>`, `<<`) will replicate the implicit sequential or parallel execution defined by UC4. The `after ActFlg` conditions in the UC4 XML, often indicating earliest start times, will be translated to Airflow `start_date` and `schedule_interval` parameters or custom `DateTimeSensor` like logic.

## 5. Transformation Logic

The core transformation involves converting the UC4 XML structure into a Python-based Airflow DAG.

1.  **UC4 JSCH to Airflow DAG**: The `JSCH` object `DW.BERT_ABLAUFSTEUERUNG` will become a single Airflow DAG with the `dag_id` `dw_bert_ablaufsteuerung`.
2.  **UC4 Tasks to Airflow Tasks/DAGs**:
    *   Each `task` element in the UC4 XML (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_STAMMDATEN_JP`) that references a `JOBP` should be considered for migration to its own Airflow DAG (`bert_monatlich_jp_dag`, etc.). The corresponding task in `dw_bert_ablaufsteuerung_dag` will then use `TriggerDagRunOperator`.
    *   Tasks referencing `EVNT` objects (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) represent triggers or conditions. These will likely translate to specific Airflow operators (e.g., `TimeSensor`, `ExternalTaskSensor` if monitoring another DAG, or custom Python operators).
3.  **Scheduling & Calendars**:
    *   The `StartTime` and `ErlstStTime` attributes will be used to define the `schedule_interval` of the main DAG and potentially `start_date` or `execution_delta` for individual tasks.
    *   UC4 `calendars` like `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT` will be translated using Airflow's cron-style `schedule_interval` or custom Python logic within `PythonOperator` for more complex calendar exclusions/inclusions. The `DW.NEW_CALENDAR` and `DW.KALENDER` objects would need to be analyzed to understand their definitions and replicate them in Airflow.
4.  **Error Handling & Monitoring**: UC4's `Abend` (abend normal/abend on error) and `SYNCREF` (synchronization) logic will be mapped to Airflow's task dependencies, `retries`, `email_on_failure`, and potentially custom alerting or monitoring solutions.

Example Snippets (Illustrative, not final code):

```python
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time import TimeSensor
from airflow.utils.dates import days_ago
import pendulum

# For DW.BERT_MONATLICH_JP - monthly, 20:00 on 5th and 25th
# This would be more complex with custom logic for specific days of month
# For simplicity, here, the main DAG runs daily and specific tasks check date
with DAG(
    dag_id='dw_bert_ablaufsteuerung',
    start_date=days_ago(1),
    schedule_interval='@daily', # Run daily, internal tasks handle specifics
    catchup=False,
    tags=['bert', 'uc4', 'scheduler'],
) as dag:
    # DW.BERT_MONATLICH_JP - triggered on 5th and 25th at 20:00
    check_monthly_run_date = PythonOperator(
        task_id='check_monthly_run_date',
        python_callable=check_if_monthly_run_day, # Custom function to check 5th/25th
    )

    trigger_bert_monatlich = TriggerDagRunOperator(
        task_id='trigger_bert_monatlich_jp',
        trigger_dag_id='dw_bert_monatlich_jp',
        wait_for_completion=True,
        poke_interval=5,
        deferrable=False,
        start_date=pendulum.time(20, 0, 0), # This might need to be a TimeSensor instead
    )

    # DW.BERT_RUN_ADM_CHECK_JP_EVT - daily at 07:00
    wait_for_07am = TimeSensor(
        task_id='wait_for_07am',
        target_time=pendulum.time(7, 0, 0),
    )
    run_adm_check = BashOperator(
        task_id='run_adm_check_jp_evt',
        bash_command='echo "Executing DW.BERT_RUN_ADM_CHECK_JP_EVT logic"',
    )
    # Further tasks for other JOBP and EVNT objects following similar patterns
```

## 6. External Dependencies

The `lineage_assembled_jobs` analysis indicated no direct external system dependencies for `DW.BERT_ABLAUFSTEUERUNG` itself. It acts as an orchestrator of other internal UC4 objects.

*   **Legacy**: The primary "external" dependencies are the other UC4 Job Plans (`JOBP`) and Events (`EVNT`) that this scheduler invokes.
*   **Target**: In the BigQuery/Airflow environment, these will translate to other Airflow DAGs or specific BigQuery/Python/PySpark operations. The dependencies will become inter-DAG dependencies managed by `TriggerDagRunOperator` or direct task calls within the same DAG.
*   **Replacement Strategy**: No specific external system integrations (e.g., SFTP, Oracle) are directly handled by this top-level scheduler. Any such dependencies would be managed within the individual sub-jobs (`JOBP`s and `EVNT`s) that `DW.BERT_ABLAUFSTEUERUNG` orchestrates.

## 7. Unresolved / Risks

*   **Migration Complexity (B3)**: The `manual` migration bucket and `complex` tier confirm that this is not a straightforward mechanical conversion. The nuances of UC4's scheduling logic, especially calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`) and their application to specific tasks, require careful manual translation to Airflow's scheduling mechanisms.
*   **Sub-Job Analysis**: The detailed logic and external dependencies of the invoked `JOBP` and `EVNT` objects (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`) are not contained within `DW.BERT_ABLAUFSTEUERUNG.xml`. Each of these sub-jobs will require its own analysis and migration design document to fully understand and re-platform their functionality. This document assumes those sub-jobs will also be migrated to Airflow DAGs.
*   **Calendar Translation**: The exact definitions of `DW.NEW_CALENDAR` and `DW.KALENDER` and how `CaleKeyName`s like `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, and `BERT_NICHT` are applied within UC4 need to be fully understood to accurately replicate the scheduling in Airflow. This may require querying additional UC4 metadata or manual inspection of the calendar objects.
*   **Synchronization Logic**: The `SYNCREF` block in the UC4 XML implies synchronization points (`SETZE_FREI`, `SETZE_LAEUFT`). This logic needs to be carefully mapped to Airflow's sensor mechanisms or task dependencies to ensure correct execution order and state management.

## 8. Build Plan

The build plan focuses on generating the Airflow DAG for `DW.BERT_ABLAUFSTEUERUNG`.

1.  **Analyze Calendar Definitions**:
    *   **Action**: Investigate the exact definitions of `DW.NEW_CALENDAR` and `DW.KALENDER` and their associated keys (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) within the legacy UC4 system.
    *   **Output**: Documentation of calendar logic and rules.
2.  **Design `dw_bert_ablaufsteuerung_dag.py` Structure**:
    *   **Action**: Define the overall DAG structure, default arguments, and `schedule_interval`.
    *   **Output**: Initial Python file for the DAG.
3.  **Translate Each UC4 Task to Airflow Task(s)**:
    *   **Action**: For each `task` element in `DW.BERT_ABLAUFSTEUERUNG.xml`, create corresponding Airflow tasks.
        *   Map `JOBP` invocations to `TriggerDagRunOperator` if they are separate DAGs, or directly integrate their tasks if they are to be part of this DAG.
        *   Map `EVNT` invocations to `TimeSensor`, custom Python operators, or other relevant sensors.
        *   Incorporate `ErlstStTime` and `calendars` logic into task scheduling or conditional execution.
    *   **Output**: Python code for each task, integrated into `dw_bert_ablaufsteuerung_dag.py`.
4.  **Define Task Dependencies**:
    *   **Action**: Replicate the execution flow and `after` conditions from UC4 XML as Airflow task dependencies (`>>`, `<<`).
    *   **Output**: Updated `dw_bert_ablaufsteuerung_dag.py` with correct task ordering.
5.  **Review and Test**:
    *   **Action**: Code review the generated Airflow DAG. Deploy to a test Composer environment and perform functional testing to ensure all schedules and invocations behave as expected, comparing against legacy UC4 execution logs.
    *   **Output**: Tested and validated `dw_bert_ablaufsteuerung_dag.py`.

**Language**: Python
**Output File**: `dags/dw_bert_ablaufsteuerung_dag.py` (relative to Airflow DAGs folder)