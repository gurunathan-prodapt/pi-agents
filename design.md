# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
This document outlines the migration plan for the `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler. Its primary purpose in the legacy environment is to orchestrate various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies. The scope of this migration is to re-implement this scheduling logic and its direct invocations onto the Google Cloud Platform, specifically utilizing BigQuery for data processing and Apache Airflow for workflow orchestration.

## 2. Source Inventory
The job `DW.BERT_ABLAUFSTEUERUNG` consists of a single UC4 Job Scheduler definition file.

| File Path                                                                     | Technology | Category | Tool      | Tier            | Automation Bucket |
|-------------------------------------------------------------------------------|------------|----------|-----------|-----------------|-------------------|
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml` | UC4 XML    | uc4      | UC4/Automic | **UNKNOWN**     | **UNKNOWN**       |

**Note:** The `file_complexity` and `automation_rate` metadata for this file could not be retrieved, hence the "UNKNOWN" status for Tier and Automation Bucket.

## 3. Target Architecture
The scheduling and orchestration logic currently defined in UC4 will be migrated to Apache Airflow. Each UC4 Job Plan (JOBP) and Event (EVNT) referenced by this scheduler will be translated into Airflow tasks. The underlying data processing logic performed by these JOBPs will be refactored and implemented using BigQuery SQL, possibly orchestrated by Python scripts.

*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Processing:** Google BigQuery.
*   **Storage:** Google Cloud Storage (for intermediate files, if any, and staging data).

The `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler will be converted into a single Apache Airflow DAG.

## 4. Data Flow & Lineage
The `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler orchestrates the execution of several child Job Plans (JOBP) and Events (EVNT) based on specific schedules and dependencies.

**Legacy Flow (as inferred from `references_out` and source XML):**

`DW.BERT_ABLAUFSTEUERUNG` (UC4 JSCH)
  |
  +--- Synchronizes with `DW.BERT_ABLAUFSTEUERUNG_SYNC` (Synchronization Object)
  |
  +--- Invokes / Schedules:
  |    +-- `DW.BERT_MONATLICH_JP` (Job Plan, scheduled monthly on 25th or 5th based on `DW.NEW_CALENDAR`, earliest start 20:00)
  |    +-- `DW.BERT_RUN_ADM_CHECK_JP_EVT` (Event, earliest start 07:00)
  |    +-- `DW.BERT_ADM_HOUSEKEEPING_JP` (Job Plan, earliest start 04:03)
  |    +-- `DW.DWH_APT_EXPORT_TAEGLICH_JP` (Job Plan, earliest start 01:30)
  |    +-- `DW.BERT_STAMMDATEN_JP` (Job Plan, earliest start 01:00)
  |    +-- `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` (Event, scheduled based on `DW.KALENDER` (excluding 'BERT_NICHT'), earliest start 01:00)

**Target Flow (Apache Airflow DAG):**

A single Airflow DAG, tentatively named `bert_ablaufsteuerung_dag.py`, will encapsulate the orchestration logic.

*   **Trigger:** The DAG will be scheduled using Airflow's native scheduling capabilities, replicating the calendar and time dependencies from UC4.
*   **Tasks:** Each `JOBP` and `EVNT` will correspond to an Airflow task.
    *   `DW.BERT_MONATLICH_JP` -> `bert_monthly_jp_task`
    *   `DW.BERT_RUN_ADM_CHECK_JP_EVT` -> `bert_adm_check_evt_task`
    *   `DW.BERT_ADM_HOUSEKEEPING_JP` -> `bert_adm_housekeeping_jp_task`
    *   `DW.DWH_APT_EXPORT_TAEGLICH_JP` -> `dwh_apt_export_daily_jp_task`
    *   `DW.BERT_STAMMDATEN_JP` -> `bert_master_data_jp_task`
    *   `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` -> `dwh_run_apt_export_monthly_evt_task`
*   **Dependencies:** The `after` conditions and synchronization objects (`DW.BERT_ABLAUFSTEUERUNG_SYNC`) in UC4 will be translated into Airflow task dependencies and possibly Airflow Sensors or XComs for inter-task communication/waiting. The `ErlstStTime` values will be used to set `start_date` and `schedule_interval` or `depends_on_past` configurations, and potentially `TimeSensor` operators in Airflow.
*   **Calendars:** UC4 Calendars like `DW.NEW_CALENDAR` and `DW.KALENDER` will need to be translated into custom Airflow sensors or `schedule_interval` logic, potentially utilizing a shared calendar definition in Airflow or by implementing specific Python callables to check date conditions.

## 5. Transformation Logic
The transformation will involve converting the XML structure and its scheduling semantics into a Python-based Airflow DAG.

**UC4 JSCH to Airflow DAG:**
*   The overall `<JSCH name="DW.BERT_ABLAUFSTEUERUNG">` object maps directly to an Airflow DAG.
*   `<XHEADER><Title>` "Scheduler für alle produktiven Abläufe von Bert" will become the DAG's `description`.
*   `<SYNCREF>`: The `DW.BERT_ABLAUFSTEUERUNG_SYNC` synchronization object implies a critical section or resource that needs to be managed. This can be implemented in Airflow using `ExternalTaskSensor` for cross-DAG dependencies, or Airflow Pools for resource contention management if multiple DAGs access it. If it's internal to this DAG, it can be managed by standard task dependencies. The `Abend="SETZE_FREI"` and `Start="SETZE_LAEUFT"` suggest locking/unlocking behavior.
*   `<ATTR_JSCH>`:
    *   `Queue`: `CLIENT_QUEUE` could map to an Airflow `queue` attribute for tasks.
    *   `StartTime`: `00:00` would be part of the DAG's `start_date`.
    *   `Period`: `1` suggests daily execution, but this needs to be cross-referenced with child task calendars.
*   `<JSCH><JschStruct><task>` elements: Each task represents an operator in Airflow.
    *   `OType="JOBP"` or `OType="EVNT"`: These will likely map to `BashOperator` (if underlying jobs are scripts) or `PythonOperator` (if re-implemented in Python, e.g., for BigQuery operations).
    *   `Object`: The name of the `JOBP` or `EVNT` will be the `task_id` in Airflow.
    *   `<after ActFlg="1" ErlstStDays="0" ErlstStTime="20:00"/>`: These are critical for setting task dependencies and scheduling.
        *   `ErlstStTime`: Earliest Start Time can be incorporated into the task's `start_date` or as a `TimeSensor` if it's a hard wait.
        *   `ActFlg`: Activation Flag needs to be considered for active/inactive tasks.
    *   `<calendars>`: UC4 calendars like `DW.NEW_CALENDAR` (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`) and `DW.KALENDER` (`BERT_NICHT`) require custom Python logic within the DAG definition to implement the specific date-based triggering. For example, a `PythonSensor` could check if the current execution date matches the calendar conditions before allowing a task to run.

**Example Task Mapping:**
`DW.BERT_MONATLICH_JP` (UC4 JOBP) with `DW.NEW_CALENDAR` (DAY_OF_MONTH_25, DAY_OF_MONTH_05) and `ErlstStTime="20:00"` could translate to an Airflow `PythonOperator` with a custom `PythonSensor` that checks the day of the month and then triggers the actual task at 20:00. The core logic of `DW.BERT_MONATLICH_JP` itself (which is not provided in this XML) would be implemented as a separate BigQuery SQL job or Python script.

## 6. External Dependencies
The source analysis `external_systems` and `unresolved_targets` were empty, suggesting no direct external systems are managed by *this specific UC4 Job Scheduler itself*. However, the child Job Plans (`JOBP`) and Events (`EVNT`) that it invokes may have their own external dependencies. For the purpose of this design for `DW.BERT_ABLAUFSTEUERUNG`, we assume its direct external dependencies are limited to its internal UC4 scheduling components.

*   **UC4 Calendar System (`DW.NEW_CALENDAR`, `DW.KALENDER`):**
    *   **Replacement:** Will be replaced by Airflow's native scheduling (`schedule_interval`) and custom Python logic within the DAG definition, potentially using `BranchPythonOperator` or `PythonSensor` to replicate the specific calendar conditions (e.g., "DAY_OF_MONTH_25", "DAY_OF_MONTH_05", or excluding "BERT_NICHT" days).
*   **UC4 Synchronization Object (`DW.BERT_ABLAUFSTEUERUNG_SYNC`):**
    *   **Replacement:** This likely indicates a shared resource or a dependency across different processes. In Airflow, this can be managed by:
        *   Airflow Pools: If it controls access to a limited resource.
        *   ExternalTaskSensor: If it's a dependency on another Airflow DAG completing.
        *   Custom Python locking mechanism: If it's a fine-grained lock within the same DAG.
        *   The exact implementation will depend on the detailed function of this synchronization object.

## 7. Unresolved / Risks
*   **Missing `file_complexity` and `automation_rate`:** Without this metadata, assessing the migration effort and automation potential is difficult. This implies a higher risk for accurate effort estimation.
*   **Child Job Plan (JOBP) and Event (EVNT) Details:** This design only covers the `DW.BERT_ABLAUFSTEUERUNG` scheduler. The actual content and dependencies *within* `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, etc., are unknown from the provided data. Each of these child objects will require its own migration design.
*   **UC4 Variables/PromptSets:** The `<dynvalues>` section with `Variables` and `PromptSets` is empty in the provided XML for the child tasks. If these were populated in the actual JOBPs/EVNTs, they would need to be translated to Airflow Variables, Jinja templating, or Airflow connections.
*   **Error Handling and Restartability:** UC4's error handling and restart mechanisms (e.g., `RElseHalt`, `RElseIgn`) need to be thoroughly analyzed and replicated in Airflow, typically through `retries`, `retry_delay`, and custom `on_failure_callback` functions.
*   **Performance Tuning:** The `Ert` (Estimated Run Time) in UC4 (`21317` seconds for the scheduler itself) suggests a long-running process. Performance tuning will be crucial in BigQuery and Airflow to ensure similar or improved execution times.

## 8. Build Plan
The migration will proceed in a phased approach:

1.  **Analyze Child Job Plans/Events (P0):** Before fully implementing the Airflow DAG, each referenced `JOBP` and `EVNT` (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`) needs to be analyzed and designed for migration to BigQuery/Python. This is a prerequisite for defining the Airflow tasks accurately.
2.  **Design Airflow DAG Structure (P1):**
    *   Create `bert_ablaufsteuerung_dag.py` in Python.
    *   Define the DAG properties: `dag_id`, `schedule_interval` (derived from `Period` and calendar logic), `start_date`, `description`.
    *   Define placeholder tasks for each `JOBP`/`EVNT` using `DummyOperator` initially.
    *   Implement basic task dependencies based on the UC4 `after` clauses.
3.  **Implement Calendar Logic (P2):**
    *   Develop custom Python functions or `PythonSensor` operators to replicate `DW.NEW_CALENDAR` and `DW.KALENDER` conditions.
    *   Integrate these into the DAG to control task execution.
4.  **Implement Synchronization Logic (P2):**
    *   Translate `DW.BERT_ABLAUFSTEUERUNG_SYNC` into appropriate Airflow mechanisms (e.g., Pools, Sensors) based on its detailed functionality.
5.  **Refactor Tasks (P3):**
    *   Replace `DummyOperator` tasks with actual `BashOperator` (for shell commands), `PythonOperator` (for Python scripts interacting with BigQuery), or `BigQueryOperator` instances, once the child JOBPs/EVNTs have their own migration designs completed and implemented.
    *   Ensure each task executes the migrated logic for the corresponding UC4 component.
6.  **Testing (P4):**
    *   Unit testing of individual Airflow tasks.
    *   Integration testing of the entire DAG in a staging environment.
    *   Functional testing to verify business logic correctness against legacy outputs.
    *   Performance testing to ensure SLA compliance.
7.  **Deployment (P5):**
    *   Deploy `bert_ablaufsteuerung_dag.py` to Cloud Composer.
    *   Monitor and validate post-deployment.
