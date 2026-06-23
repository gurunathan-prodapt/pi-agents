# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
The `DW.BERT_ABLAUFSTEUERUNG` is a UC4 Job Scheduler (JSCH) responsible for orchestrating various productive data processing workflows related to "Bert". Its primary purpose is to define and manage the execution sequence of several child Job Plans (JOBP) and Events (EVNT), including monthly job plans, administrative checks, housekeeping routines, daily and monthly APT exports, and master data processing. The job also incorporates calendar-based dependencies and an internal synchronization mechanism to prevent overlapping runs. The scope of this migration is to re-platform this UC4 scheduler to an Airflow DAG running on Google Cloud Platform, leveraging BigQuery as the target data warehouse.

## 2. Source Inventory

| File Path | Technology | Complexity Tier | Automation Bucket | Summary |
| :-------------------------------------------------------------------------------------- | :---------------- | :--------------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml` | UC4/Automic | `complex` | `manual` | This UC4 Job Scheduler (JSCH) orchestrates various productive processes related to 'Bert', including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies. |

**Notes:** The migration bucket is `manual` due to the need for a detailed design to translate UC4 scheduling logic and its nested components into an Airflow DAG and subsequent BigQuery-compatible transformations.

## 3. Target Architecture
The target architecture for `DW.BERT_ABLAUFSTEUERUNG` will be an Airflow DAG deployed on Google Cloud's Cloud Composer.
Each invoked UC4 JOBP and EVNT will be represented as a `TriggerDagRunOperator` task within the main `dw_bert_ablaufsteuerung` Airflow DAG.
The underlying ETL logic executed by these child UC4 jobs (which appear to be shell scripts and SQL based on lineage) will need to be re-implemented in BigQuery SQL, Python with BigQuery API, or PySpark running on Dataproc, depending on their complexity and data volumes.

**Core Components:**
*   **Airflow DAG (`dw_bert_ablaufsteuerung.py`):** Orchestrates the overall workflow.
*   **Trigger DAGs:** Each UC4 JOBP/EVNT will have a corresponding Airflow DAG (e.g., `dw_bert_monatlich_jp.py`, `dw_bert_adm_housekeeping_jp.py`).
*   **BigQuery:** Target data warehouse for all data processed by the migrated ETL jobs.
*   **Cloud Dataproc (optional):** For PySpark or other Spark-based transformations if identified in child jobs.
*   **Cloud Storage:** For intermediate data storage and staging.

## 4. Data Flow & Lineage
The `DW.BERT_ABLAUFSTEUERUNG` UC4 Job Scheduler defines a primary control flow that invokes several nested Job Plans (JOBP) and Events (EVNT). The data flow within this orchestration job is primarily control flow, not direct data transformation.

**Execution Order (from `uc4_to_airflow_dag_design`):**
1.  **`sync_guard`:** A custom Python task to mimic the UC4 `SYNCREF` object's `Else=Skip` behavior, preventing concurrent DAG runs.
2.  **`trigger_dw_bert_monatlich_jp`:** Invokes the monthly job plan (`DW.BERT_MONATLICH_JP`). This task will be preceded by a `TimeSensor` (earliest start `20:00`) and a calendar check for `DW.NEW_CALENDAR` (likely `DAY_OF_MONTH_25` and `DAY_OF_MONTH_05`).
3.  **`trigger_dw_bert_run_adm_check_jp_evt`:** Invokes the admin check event (`DW.BERT_RUN_ADM_CHECK_JP_EVT`) as a fire-and-forget task (earliest start `07:00`).
4.  **`trigger_dw_bert_adm_housekeeping_jp`:** Invokes the admin housekeeping job plan (`DW.BERT_ADM_HOUSEKEEPING_JP`) (earliest start `04:03`).
5.  **`trigger_dw_dwh_apt_export_taeglich_jp`:** Invokes the daily APT export job plan (`DW.DWH_APT_EXPORT_TAEGLICH_JP`) as a fire-and-forget task (earliest start `01:30`).
6.  **`trigger_dw_bert_stammdaten_jp`:** Invokes the master data job plan (`DW.BERT_STAMMDATEN_JP`) (earliest start `01:00`).
7.  **`trigger_dw_dwh_run_apt_export_monatlich_jp_evt`:** Invokes the monthly export event (`DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`). This task will be preceded by a `TimeSensor` (earliest start `01:00`) and a calendar check for `DW.KALENDER`.

**Data Flow within Child Jobs (from `lineage_edges`):**
*   Several SQL files like `d_exis_apt_bestandsdaten.sql`, `d_exis_apt_nna_daten.sql`, `d_exis_apt_nna_voice.sql`, `d_exis_apt_rabattdaten.sql` read from source tables such as `TABLE:RPT$TA_S_D1_VERTRAG`, `TABLE:SOF$TA_BPR_OPTIONEN`, `TABLE:SOF$VI_L_OPTIONZUORDNUNG`, `TABLE:DWH$VI_C_VERTRAG`, `TABLE:DWH$TA_F_NNV_GPRS`, `TABLE:DWH$VI_L_MAP_FA_TARIF`, `TABLE:BL_D_TARIF`, `TABLE:DWH$VI_F_NNV_TVD_12_MONATE`, `TABLE:DWH$VI_L_TVD_LEISTUNGSKLASSE`, `TABLE:RPT$TA_S_D1_DISCOUNT_RR`. These will need to be migrated to BigQuery.
*   Shell scripts (`R_AURD_RECHSTAN.KSH`, `R_DROP_TEMP_TABLE.KSH`, `R_AUSD_ADRESSEN.KSH`, `R_AUSD_AUSTAUSCH.KSH`, `R_AUSD_BP_TA_APN_CARMEN.KSH`, `R_AUSD_BP_TA_APN_VERTRAG.KSH`, `R_AUSD_BP_TA_BCP_ICCID.KSH`, `R_AUSD_BP_TA_BCP_MSISDN.KSH`, `R_AUSD_BP_TA_BPR_APN.KSH`) are invoked, indicating potential file system interactions, external system calls, or data manipulation outside of SQL. These scripts will require individual analysis and conversion to Python/BashOperators or PySpark.

## 5. Transformation Logic
The transformation logic described here focuses on the orchestration (main `DW.BERT_ABLAUFSTEUERUNG.xml` file). The detailed transformation logic for the child jobs (`JOBP`, `EVNT`) and their contained scripts (SQL, KSH) would be part of their respective migration design documents.

**UC4 Orchestrator (`DW.BERT_ABLAUFSTEUERUNG.xml`) to Airflow DAG (`dw_bert_ablaufsteuerung.py`):**
*   **Job Scheduling:** The `Period=1` and `StartTime=00:00` attributes of the UC4 JSCH will be translated to a daily cron schedule (`0 0 * * *`) in Airflow.
*   **Synchronization:** The `SYNCREF` object `DW.BERT_ABLAUFSTEUERUNG_SYNC` with `Else=Skip` will be implemented using a `PythonOperator` at the start of the DAG. This operator will check for active DAG runs and raise an `AirflowSkipException` if another instance is already running.
*   **Task Invocation:** Each UC4 `<task>` element (JOBP or EVNT) will be mapped to an Airflow `TriggerDagRunOperator`. The `Object` attribute (e.g., `DW.BERT_MONATLICH_JP`) will become the `trigger_dag_id` (e.g., `dw_bert_monatlich_jp`).
*   **Earliest Start Times (`<after ErlstStTime="...">`):** These will be implemented using `TimeSensor` tasks in Airflow, ensuring downstream tasks do not start before the specified time.
*   **Calendar Dependencies (`<calendars>`):** The `CaleOn=1` attributes with `CaleName` (e.g., `DW.NEW_CALENDAR`, `DW.KALENDER`) will require custom logic using `ShortCircuitOperator` or `BranchPythonOperator` to evaluate the calendar conditions before triggering the associated tasks. The definitions of these calendars (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) need to be recreated or referenced in Airflow.
*   **Fire-and-Forget vs. Wait-for-Completion:** Tasks with `ActFlg=0` (e.g., `DW.BERT_RUN_ADM_CHECK_JP_EVT`, `DW.DWH_APT_EXPORT_TAEGLICH_JP`) will use `wait_for_completion=False` in `TriggerDagRunOperator`. Tasks with `ActFlg=1` will use `wait_for_completion=True`.
*   **Error Handling:** Since no explicit retry or post-condition logic was found in the UC4 XML for child tasks, Airflow tasks will default to `retries=0`.

## 6. External Dependencies
The following external dependencies were identified or implied:

*   **Databases:**
    *   **Source:** Oracle (implied by `USES_DB_LINK` in `r_exis_v2` and `TABLE:` prefixes in `lineage_edges`).
    *   **Replacement:** BigQuery for data storage. Data ingestion from Oracle will require a separate migration strategy (e.g., Change Data Capture (CDC) tools, batch extracts to Cloud Storage and then loading to BigQuery).
*   **Filesystem / Shell Scripts:**
    *   **Source:** `SCRIPT:` and `SHELL_SCRIPT:` invocations of `.KSH` files (e.g., `R_AURD_RECHSTAN.KSH`, `R_DROP_TEMP_TABLE.KSH`) indicate interactions with the legacy filesystem or execution of shell commands.
    *   **Replacement:** These scripts need to be rewritten.
        *   Simple file operations can be converted to Python scripts interacting with Cloud Storage.
        *   Database interactions within scripts will be converted to BigQuery SQL or Python using BigQuery client libraries.
        *   Complex shell logic might be converted to Python operators or potentially PySpark jobs if they involve large-scale data processing.
*   **External Host/Login:**
    *   **Source:** `HOST:DWHDWH2P` (`CALLS_HTTP`) and `LOGIN:DW.UNIX.ISBERT` (`USES_PACKAGE`). These indicate remote execution or access.
    *   **Replacement:** These interactions will need to be analyzed to understand their purpose. It could involve API calls, secure file transfers (e.g., SFTP to Cloud Storage), or SSH commands. Appropriate Airflow operators (e.g., `SimpleHttpOperator`, custom `PythonOperator` for SSH/SFTP) or cloud services (e.g., Cloud Functions, Managed SFTP on Storage) will be used.

## 7. Unresolved / Risks

*   **Unresolved `BERT_LOG.KSH`:** The `lineage_edges` showed an `UNRESOLVED:BERT_LOG.KSH` invoked by `DW.BERT_MONATLICH_JP/DW.BERT_LOG.xml`. This script's purpose and content are unknown and require manual investigation to determine its migration path.
*   **Calendar Definitions:** The exact logic for `DW.NEW_CALENDAR` (using `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`) and `DW.KALENDER` (using `BERT_NICHT`) is not available. These will need to be manually defined in Airflow as custom Python functions or external configuration.
*   **Nested Job Plan Details:** The provided UC4 XML is only the main scheduler. The detailed logic, data sources, and targets of the child `JOBP`s and `EVNT`s (`DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, etc.) are not available in this file. Each of these will require its own analysis and migration design.
*   **`manual` Migration Bucket:** The `manual` migration bucket for this file indicates that significant manual effort is required beyond automated conversion, specifically in understanding the implicit logic of child jobs and external integrations.
*   **Complexity of Shell Scripts:** The KSH scripts invoked by child jobs can contain complex logic, environment variable dependencies, and calls to legacy binaries. These need thorough analysis for re-implementation in a BigQuery-native context.
*   **Dataproc Cluster Management:** If PySpark jobs are introduced for shell script conversions, careful consideration of Dataproc cluster lifecycle (ephemeral vs. persistent) is needed.

## 8. Build Plan

The build plan will proceed in an iterative fashion, starting with the orchestration layer and then moving to the individual child jobs.

1.  **Orchestration Layer (Airflow DAG for `DW.BERT_ABLAUFSTEUERUNG`):**
    *   **Generate `dw_bert_ablaufsteuerung.py` (Python Airflow DAG):**
        *   Implement `sync_guard` `PythonOperator` for `Else=Skip` logic.
        *   Implement `TimeSensor` tasks for `ErlstStTime` constraints.
        *   Implement placeholder `PythonOperator` or `ShortCircuitOperator` tasks for calendar logic (`DW.NEW_CALENDAR`, `DW.KALENDER`), with a clear TODO for actual calendar implementation.
        *   Implement `TriggerDagRunOperator` tasks for each child UC4 `JOBP`/`EVNT` (e.g., `trigger_dw_bert_monatlich_jp`, `trigger_dw_bert_run_adm_check_jp_evt`). Set `wait_for_completion` based on `ActFlg`.
        *   Define task dependencies based on the UC4 execution flow.
        *   Configure DAG properties (`dag_id`, `schedule`, `start_date`, `max_active_runs`, `default_args`).
        *   Language: Python

2.  **Child Job Migration (Iterative for each `JOBP`/`EVNT`):**
    *   For each child `JOBP`/`EVNT` identified (e.g., `DW.BERT_MONATLICH_JP`, `DW.BERT_ADM_HOUSEKEEPING_JP`, etc.):
        *   **Analyze Child Job:** Retrieve its UC4 definition (if available), or analyze the scripts it invokes (SQL, KSH, etc.).
        *   **Design Child DAG/Job:** Create a separate migration design document for each child job, detailing its source, target, and transformation.
        *   **Implement Child ETL:** Convert the ETL logic to BigQuery SQL, Python scripts for BigQuery, or PySpark scripts.
        *   **Create Child Airflow DAGs:** If the child job is also an orchestrator or has significant internal complexity, create a corresponding Airflow DAG (e.g., `dw_bert_monatlich_jp.py`).
        *   **Address External Dependencies:** Migrate data sources from Oracle to BigQuery, rewrite shell scripts for Cloud Storage/BigQuery/Dataproc interaction, and handle external system calls (e.g., SFTP, HTTP APIs).
        *   Language: BigQuery SQL, Python, PySpark (as needed).

3.  **Calendar Logic Implementation:**
    *   Develop and implement the specific logic for `DW.NEW_CALENDAR` and `DW.KALENDER` within the Airflow environment. This might involve querying a control table in BigQuery or using fixed logic in Python.
    *   Language: Python

4.  **Unresolved `BERT_LOG.KSH`:**
    *   Investigate `BERT_LOG.KSH` to understand its function.
    *   Design and implement its migration, likely to a Python script or BigQuery logging mechanism.
    *   Language: Python (likely).