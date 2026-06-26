# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
The `DW.BERT_ABLAUFSTEUERUNG` job is a UC4 Job Scheduler (JSCH) responsible for orchestrating various productive processes related to 'Bert'. Its primary purpose is to manage and sequence monthly job plans, administrative checks, housekeeping tasks, daily and monthly APT exports, and master data processing. It defines a workflow of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies, ensuring the timely and ordered execution of these critical business operations.

## 2. Source Inventory
The job is defined by a single source file:
*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
*   **Technology:** UC4 Job Scheduler (XML)
*   **Purpose:** Orchestration of 'Bert' related ETL and administrative processes.
*   **Summary:** This UC4 Job Scheduler (JSCH) orchestrates various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies.
*   **Category:** `uc4`
*   **Tool:** `UC4/Automic`

## 3. Target Architecture
The `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler will be migrated to a Google Cloud Platform (GCP) native architecture, primarily utilizing **Cloud Composer (managed Airflow)** for workflow orchestration. Each UC4 Job Plan (JOBP) and Event (EVNT) referenced by this scheduler will be translated into Airflow tasks or sub-DAGs, allowing for granular control and monitoring.
*   **Orchestration:** Apache Airflow DAG hosted on Cloud Composer.
*   **Data Processing:** BigQuery for SQL-based transformations and data storage. Python-based tasks (e.g., `PythonOperator`) will be used for any complex logic not suitable for direct SQL, or for interacting with other GCP services.
*   **External Data Handling:** Cloud Storage for staging incoming/outgoing files, and potentially Cloud Functions or Dataflow for specific data ingestion/egress patterns.

## 4. Data Flow & Lineage
The UC4 Job Scheduler `DW.BERT_ABLAUFSTEUERUNG` acts as a central orchestrator for a sequence of child job plans and events. The migration will preserve this orchestration logic by mapping it to an Airflow DAG.

The observed execution sequence from the UC4 XML is as follows:
1.  **`DW.BERT_MONATLICH_JP` (JOBP):** A monthly job plan, scheduled to run on specific days of the month (25th and 5th) as defined by the `DW.NEW_CALENDAR` with `DAY_OF_MONTH_25` and `DAY_OF_MONTH_05` calendar keys. This task has an earliest start time of 20:00.
2.  **`DW.BERT_RUN_ADM_CHECK_JP_EVT` (EVNT):** An event-driven administrative check, with an earliest start time of 07:00.
3.  **`DW.BERT_ADM_HOUSEKEEPING_JP` (JOBP):** An administrative housekeeping job plan, with an earliest start time of 04:03.
4.  **`DW.DWH_APT_EXPORT_TAEGLICH_JP` (JOBP):** A daily APT export job plan, with an earliest start time of 01:30.
5.  **`DW.BERT_STAMMDATEN_JP` (JOBP):** A master data processing job plan, with an earliest start time of 01:00.
6.  **`DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` (EVNT):** A monthly APT export event, with an earliest start time of 01:00. This event is explicitly excluded by the `BERT_NICHT` key in the `DW.KALENDER`.

In Airflow, this sequence will be translated into a series of tasks with defined `task_dependencies`. The `ErlstStTime` will be handled either by the overall DAG `start_date` and `schedule_interval`, or by specific `TimeSensor` tasks if absolute time triggering is critical.

## 5. Transformation Logic
The UC4 XML structure will be transformed into a Python-based Airflow DAG (`.py` file).

*   **UC4 `JSCH` (`DW.BERT_ABLAUFSTEUERUNG`) -> Airflow DAG:** The main `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler will be converted into a primary Airflow DAG. This DAG will define the overall workflow and its schedule.
*   **UC4 `JOBP` tasks -> Airflow Tasks/Sub-DAGs:** Each child `JOBP` (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`, `DW.BERT_STAMMDATEN_JP`) within the UC4 `JSCH` will be represented as an Airflow task.
    *   If a `JOBP` is a simple sequence of commands, it can be a single `PythonOperator` or `BashOperator` (for shell scripts) or a `BigQueryOperator` (for SQL).
    *   If a `JOBP` itself represents a complex workflow, it might be better represented as a separate Airflow Sub-DAG, invoked by a `TriggerDagRunOperator` from the main `DW.BERT_ABLAUFSTEUERUNG` DAG.
*   **UC4 `EVNT` tasks -> Airflow Sensors/Tasks:** The UC4 `EVNT` tasks (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`) will be translated into Airflow Sensors (e.g., `ExternalTaskSensor`, `S3KeySensor`, `SqlSensor`) if they wait for external conditions. If they represent a triggered action, they will be a standard Airflow task.
*   **Dependencies (`<after>` tag) -> Airflow Task Dependencies:** The sequential nature indicated by the implicit order and explicit `<after>` tags (e.g., `after ErlstStTime="20:00"`) will be mapped directly to Airflow's task dependency operators (`>>`, `<<`). Specific `ErlstStTime` values might require `TimeSensor` tasks for precise timing requirements.
*   **Calendars (`<calendars>` tag) -> Airflow Scheduling:** The UC4 calendar definitions (`DW.NEW_CALENDAR`, `DW.KALENDER`, with keys like `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) will be translated into Airflow's `schedule_interval` for the DAG. Complex calendar logic, especially exclusions, may require custom Python functions to determine the run date or conditional task execution.

## 6. External Dependencies
Based on the analysis of `lineage_assembled_jobs`, no explicit external systems are referenced directly by the `DW.BERT_ABLAUFSTEUERUNG` scheduler. However, the summary mentions "APT exports" which typically imply interaction with external systems for file transfer.
*   **APT Exports:** If these exports involve SFTP, FTP, or other file transfer protocols, these will be re-implemented using GCP services:
    *   **Cloud Storage:** For landing and staging export files.
    *   **Cloud Functions/Cloud Dataflow:** For serverless processing or transformation of files before or after transfer.
    *   **Cloud Interconnect/VPN:** If direct secure connectivity to external systems is required.

## 7. Unresolved / Risks
*   **Child Job Logic:** The `DW.BERT_ABLAUFSTEUERUNG` scheduler only defines the orchestration. The actual business logic and data transformations reside within the referenced `JOBP` and `EVNT` objects. A detailed analysis and migration plan for each of these child components is required. This is the primary unresolved item.
*   **Complex UC4 Calendars:** While simple daily/monthly schedules are straightforward, complex UC4 calendar definitions (especially those with intricate inclusion/exclusion rules and custom dates) might require careful translation to Airflow's `schedule_interval` or custom Python scheduling logic to ensure exact functional equivalence.
*   **UC4 Variables/Prompt Sets:** The XML contains `<Variables>` and `<PromptSets>` nodes, although empty in this specific instance. If child jobs utilize these, their translation to Airflow variables or configuration will be necessary.
*   **Error Handling and Alerting:** UC4 has built-in error handling and alerting mechanisms. These need to be re-designed using Airflow's retry mechanisms, SLA monitoring, and integration with GCP monitoring and alerting services (e.g., Cloud Monitoring, Cloud Logging, PagerDuty).

## 8. Build Plan
1.  **Airflow DAG Development for `DW.BERT_ABLAUFSTEUERUNG`:**
    *   Create a new Python file (e.g., `dags/dw_bert_ablaufsteuerung_dag.py`) for the main Airflow DAG.
    *   Define the DAG's schedule interval based on the UC4 calendar logic.
    *   For each child UC4 `JOBP` and `EVNT` in the XML, define a corresponding Airflow task or sub-DAG trigger.
    *   Establish task dependencies in the Airflow DAG to reflect the sequence and `ErlstStTime` logic from the UC4 XML.
    *   Implement custom calendar logic if needed within the DAG definition or as a separate utility.
2.  **Child Job Plan (`JOBP`) Migration:**
    *   Analyze each child `JOBP` (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`) to understand its internal logic.
    *   Migrate the data transformation logic within each `JOBP` to BigQuery SQL scripts or Python scripts.
    *   Create separate Airflow DAGs for complex `JOBP`s or simple `PythonOperator`/`BigQueryOperator` tasks for straightforward ones.
3.  **Child Event (`EVNT`) Migration:**
    *   Analyze each child `EVNT` (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`) to determine the event it monitors or triggers.
    *   Implement corresponding Airflow Sensors (if monitoring an external event) or standard tasks (if triggering an action).
4.  **External System Integration:**
    *   Design and implement any necessary components for APT exports or other external interactions using GCP services (e.g., Cloud Storage, Cloud Functions).
5.  **Testing and Deployment:**
    *   Thoroughly test the migrated Airflow DAGs and tasks.
    *   Deploy the DAGs to Cloud Composer.