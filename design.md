# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
The original UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG` (source file: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`) orchestrates various productive processes related to 'Bert'. These include monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies.

The scope of this migration is to re-platform this UC4 Job Scheduler to a BigQuery-native environment, specifically using Airflow for orchestration. The migration will translate the scheduling logic, task dependencies, and calendar-based execution constraints into an Airflow Directed Acyclic Graph (DAG) that triggers other downstream DAGs or BigQuery jobs.

## 2. Source Inventory
The migration job `5af228f1` consists of one primary source file:

*   **File**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
    *   **Technology**: UC4/Automic (UC4 Job Scheduler - JSCH)
    *   **Purpose**: Orchestrates Bert's productive workflows.
    *   **Complexity Tier**: `complex`
    *   **Automation Bucket**: `manual`
    *   **Summary**: This UC4 Job Scheduler defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies. It's the main scheduler for Bert productive workflows.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform (GCP) services, primarily Airflow (Cloud Composer) for orchestration.

*   **Orchestration**: Apache Airflow DAG (`dw_bert_ablaufsteuerung`) running on Cloud Composer.
*   **Triggered Workflows**: The main Airflow DAG will trigger other downstream Airflow DAGs (corresponding to the original UC4 JOBP/EVNT objects) using `TriggerDagRunOperator`.
*   **Data Processing**: The actual data processing logic within the child DAGs (e.g., `dw_bert_monatlich_jp`, `dw_dwh_apt_export_taeglich_jp`) is assumed to be migrated to BigQuery SQL, PySpark on Dataproc, or other appropriate BigQuery-native services. This design document focuses on the scheduler's migration.

## 4. Data Flow & Lineage
The original UC4 Job Scheduler orchestrates the execution of several child Job Plans (JOBP) and Events (EVNT). The migration will map this orchestration to an Airflow DAG that triggers other DAGs.

**Original UC4 Flow (extracted from XML):**
The `DW.BERT_ABLAUFSTEUERUNG` JSCH invokes the following objects in a sequential manner, with specific earliest start times and calendar dependencies:

1.  `DW.BERT_MONATLICH_JP` (JOBP): Monthly workflow. Earliest start time: 20:00. Calendar dependent (DW.NEW_CALENDAR).
2.  `DW.BERT_RUN_ADM_CHECK_JP_EVT` (EVNT): Event task for admin check workflow. Earliest start time: 07:00.
3.  `DW.BERT_ADM_HOUSEKEEPING_JP` (JOBP): Admin housekeeping workflow. Earliest start time: 04:03.
4.  `DW.DWH_APT_EXPORT_TAEGLICH_JP` (JOBP): Daily APT export workflow. Earliest start time: 01:30.
5.  `DW.BERT_STAMMDATEN_JP` (JOBP): Master data workflow. Earliest start time: 01:00.
6.  `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` (EVNT): Event task for monthly APT export workflow. Earliest start time: 01:00. Calendar dependent (DW.KALENDER).

**Target Airflow Data Flow (`dw_bert_ablaufsteuerung` DAG):**
The Airflow DAG will maintain the sequential execution and time-based constraints.

*   **`guard_active_run` (PythonOperator)**: Checks for active DAG runs to implement the `Else=Skip` UC4 sync behavior.
*   **`wait_until_<time>_for_<job>` (TimeSensor)**: Multiple `TimeSensor` tasks will be introduced before each triggered DAG to enforce the earliest start times (e.g., `wait_until_20_00_for_dw_bert_monatlich_jp`).
*   **`calendar_check_<job>` (PythonOperator/Stub)**: For calendar-dependent jobs, a placeholder task will be inserted to represent the UC4 calendar logic.
*   **`dw_bert_monatlich_jp` (TriggerDagRunOperator)**: Triggers the downstream DAG `dw_bert_monatlich_jp`.
*   **`dw_bert_run_adm_check_jp_evt` (TriggerDagRunOperator)**: Triggers the downstream DAG `dw_bert_run_adm_check_jp_evt`.
*   **`dw_bert_adm_housekeeping_jp` (TriggerDagRunOperator)**: Triggers the downstream DAG `dw_bert_adm_housekeeping_jp`.
*   **`dw_dwh_apt_export_taeglich_jp` (TriggerDagRunOperator)**: Triggers the downstream DAG `dw_dwh_apt_export_taeglich_jp`.
*   **`dw_bert_stammdaten_jp` (TriggerDagRunOperator)**: Triggers the downstream DAG `dw_bert_stammdaten_jp`.
*   **`dw_dwh_run_apt_export_monatlich_jp_evt` (TriggerDagRunOperator)**: Triggers the downstream DAG `dw_dwh_run_apt_export_monatlich_jp_evt`.

**Execution Order:**
`guard_active_run >> wait_until_20_00_for_dw_bert_monatlich_jp >> calendar_check_dw_bert_monatlich_jp >> dw_bert_monatlich_jp >> wait_until_07_00_for_dw_bert_run_adm_check_jp_evt >> dw_bert_run_adm_check_jp_evt >> wait_until_04_03_for_dw_bert_adm_housekeeping_jp >> dw_bert_adm_housekeeping_jp >> wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp >> dw_dwh_apt_export_taeglich_jp >> wait_until_01_00_for_dw_bert_stammdaten_jp >> dw_bert_stammdaten_jp >> wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt >> calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt >> dw_dwh_run_apt_export_monatlich_jp_evt`

## 5. Transformation Logic
The `DW.BERT_ABLAUFSTEUERUNG` is a pure scheduler and does not contain any direct data transformation logic. Its transformation is from a UC4 XML scheduler definition to an Airflow Python DAG.

*   **UC4 JSCH (Scheduler)**: Defines job execution order, timing, and dependencies.
*   **Airflow DAG**: Replicates the scheduling and dependency management using `DAG` definition, `TimeSensor` for time-based triggers, and `TriggerDagRunOperator` to invoke child DAGs. The `Else=Skip` synchronization behavior is translated into a PythonOperator guard task.
*   **Calendars**: UC4 calendars `DW.NEW_CALENDAR` and `DW.KALENDER` are currently represented as stub calendar-check tasks, requiring manual implementation to translate the specific calendar logic (e.g., `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) into Airflow-compatible date/time checks.

## 6. External Dependencies
Based on the analysis of the `lineage_assembled_jobs` record, no external systems were explicitly listed for this job.
The UC4 XML itself does not indicate direct external system integrations for the *scheduler* component. Any external system interactions (e.g., SFTP, Oracle) would typically be managed within the individual `JOBP` or `EVNT` objects that this scheduler invokes. Therefore, the migration of this scheduler focuses solely on its orchestration logic.

## 7. Unresolved / Risks
*   **UC4 Calendar Logic**: The exact definitions for `DW.NEW_CALENDAR` (keys `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`) and `DW.KALENDER` (key `BERT_NICHT`) were not provided in the source XML. This is a significant unresolved item, requiring manual analysis of the UC4 calendar definitions to correctly implement the corresponding logic within Airflow. These are currently placeholders.
*   **Downstream DAGs**: This design assumes that the referenced `JOBP` and `EVNT` objects (e.g., `DW.BERT_MONATLICH_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`) will be migrated into separate Airflow DAGs with corresponding IDs (e.g., `dw_bert_monatlich_jp`). The content and migration of these downstream DAGs are outside the scope of this document.
*   **No JOBS_UNIX objects**: The absence of `JOBS_UNIX` objects means there are no direct mappings to Dataproc jobs or PySpark scripts from the scheduler itself. The actual workload executed by the child `JOBP`s is unknown without further analysis of those objects.
*   **Complexity Tier**: The source file is categorized as `complex` and in a `manual` migration bucket, indicating that manual effort is expected, especially for the calendar logic and potential fine-tuning of `TimeSensor` values and DAG relationships.

## 8. Build Plan
The build plan focuses on generating the Airflow DAG for `DW.BERT_ABLAUFSTEUERUNG`.

1.  **Generate Airflow DAG Python File**:
    *   **Source**: UC4 XML for `DW.BERT_ABLAUFSTEUERUNG`.
    *   **Tool**: Manual development or a specialized `uc4_to_airflow_dag_design` tool (which has already provided the pseudocode).
    *   **Language**: Python.
    *   **Output File**: `dags/dw_bert_ablaufsteuerung.py`

2.  **Implement `guard_active_run` Python Operator**:
    *   **Logic**: Check for concurrently running DAGs using `DagRun.find()` and raise `AirflowSkipException` if another active run is detected, ensuring `Else=Skip` behavior.

3.  **Implement Time Sensors**:
    *   **Logic**: Create `TimeSensor` tasks for each earliest-start time constraint (e.g., `wait_until_20_00_for_dw_bert_monatlich_jp`, `wait_until_07_00_for_dw_bert_run_adm_check_jp_evt`, etc.).

4.  **Implement Calendar Check Logic (Manual)**:
    *   **Logic**: For `DW.BERT_MONATLICH_JP` and `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`, manually implement the UC4 calendar logic (e.g., `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) within dedicated Python operators. This will involve retrieving the exact calendar definitions from the legacy UC4 system.

5.  **Configure TriggerDagRunOperators**:
    *   **Logic**: Configure `TriggerDagRunOperator` for each child `JOBP`/`EVNT` to trigger their respective Airflow DAGs (e.g., `dw_bert_monatlich_jp`, `dw_dwh_run_apt_export_monatlich_jp_evt`). Set `wait_for_completion` based on the original UC4 `ActFlg` and `runtime` settings if applicable (defaults to `True` unless explicitly `False` in UC4).

6.  **Define Task Dependencies**:
    *   **Logic**: Establish the explicit sequential dependencies between the `guard_active_run`, `TimeSensor` tasks, calendar check tasks, and `TriggerDagRunOperator` tasks as outlined in Section 4.

7.  **Placeholder for GCP Project/Dataproc/Bucket Configuration**:
    *   **Action**: Replace `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`, and `PLACEHOLDER_START_DATE` with actual project-specific values.