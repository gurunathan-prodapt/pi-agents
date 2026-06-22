# Migration Design — DW.BERT_ABLAUFSTEUERUNG

## 1. Purpose & Scope
This job, `DW.BERT_ABLAUFSTEUERUNG`, is a UC4 Job Scheduler (JSCH) that orchestrates various productive ETL processes related to "Bert". Its primary function is to manage and schedule child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies. These processes include monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. The migration aims to re-implement this orchestration logic in Google Cloud's BigQuery ecosystem, utilizing Cloud Composer (Airflow) for scheduling and Python/SQL for the underlying data transformations.

## 2. Source Inventory
The core component of this job is a single UC4 XML file.

*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
    *   **Technology:** UC4/Automic Job Scheduler (XML configuration)
    *   **Tier:** Complex
    *   **Automation Bucket:** Manual
    *   **Summary:** Orchestrates various "Bert" related processes, including monthly job plans, administrative checks, housekeeping, daily and monthly APT exports, and master data processing. It defines a sequence of child job plans (JOBP) and events (EVNT) with specific start times and calendar dependencies.
    *   **File Purpose:** Scheduler/Orchestrator

Dependent components identified via lineage (these are likely sub-jobs or scripts invoked by the main scheduler or its child job plans):
*   `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql` (SQL script reading from `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`)
*   `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_nna_daten.sql` (SQL script reading from `DWH$VI_C_VERTRAG`, `DWH$TA_F_NNV_GPRS`, `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`)
*   `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_nna_voice.sql` (SQL script reading from `DWH$VI_C_VERTRAG`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`, `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`)
*   `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_rabattdaten.sql` (SQL script reading from `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`)
*   `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2` (Shell script, uses `EXT:DATABASE`)
*   Child UC4 Job Plans invoked by `DW.BERT_ABLAUFSTEUERUNG.xml`:
    *   `JOBP:DW.BERT_MONATLICH_JP`
    *   `EVNT:DW.BERT_RUN_ADM_CHECK_JP_EVT`
    *   `JOBP:DW.BERT_ADM_HOUSEKEEPING_JP`
    *   `JOBP:DW.DWH_APT_EXPORT_TAEGLICH_JP`
    *   `JOBP:DW.BERT_STAMMDATEN_JP`
    *   `EVNT:DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`

## 3. Target Architecture
The `DW.BERT_ABLAUFSTEUERUNG` job, being an orchestrator, will be migrated to Google Cloud Composer (Airflow) as a Directed Acyclic Graph (DAG).
*   **Orchestration:** Apache Airflow DAG (deployed on Cloud Composer).
*   **Data Storage:** Google BigQuery for all relational data. Source tables like `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `DWH$VI_C_VERTRAG`, etc., will have corresponding BigQuery datasets and tables.
*   **Data Transformation:** SQL scripts will be converted to BigQuery SQL, executed via `BigQueryOperator` in Airflow.
*   **Export/External Interactions:** Cloud Storage for file exports (e.g., APT exports). Integration with external systems (if `EXT:DATABASE` represents an external data source) will utilize appropriate Google Cloud services (e.g., Cloud SQL for managed databases, BigQuery federated queries, or Cloud Functions for API interactions).
*   **Logging:** Cloud Logging and Cloud Monitoring will replace UC4's internal logging mechanisms.

## 4. Data Flow & Lineage
The original UC4 job defines a sequence of invocations. This will be translated into a series of Airflow tasks.

**Overall Flow:**
`DW.BERT_ABLAUFSTEUERUNG.xml` (Airflow DAG: `bert_ablaufsteuerung_dag.py`)
  *   Invokes `DW.BERT_MONATLICH_JP` (Airflow SubDAG/TaskGroup: `bert_monatlich_jp`)
      *   Invokes `DW.BERT_RECHNUNGSDATEN` (BigQuery SQL task for monthly billing data)
      *   Invokes `DW.BERT_LOG` (Python/Bash task for logging)
  *   Invokes `DW.BERT_RUN_ADM_CHECK_JP_EVT` (Airflow Event/Sensor: `bert_run_adm_check_jp_evt`)
  *   Invokes `DW.BERT_ADM_HOUSEKEEPING_JP` (Airflow SubDAG/TaskGroup: `bert_adm_housekeeping_jp`)
  *   Invokes `DW.DWH_APT_EXPORT_TAEGLICH_JP` (Airflow SubDAG/TaskGroup: `dwh_apt_export_taeglich_jp`)
      *   Internally executes SQLs like `d_exis_apt_bestandsdaten.sql`, `d_exis_apt_nna_daten.sql`, `d_exis_apt_nna_voice.sql`, `d_exis_apt_rabattdaten.sql`
          *   These SQLs `READS_TABLE` from various source tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `DWH$VI_C_VERTRAG`, `DWH$TA_F_NNV_GPRS`, `BL_D_TARIF`, `RPT$TA_S_D1_DISCOUNT_RR`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`, `DWH$VI_L_MAP_FA_TARIF`). These will be migrated to BigQuery tables.
      *   Likely uses `r_exis_v2` for export (to Cloud Storage or external system).
  *   Invokes `DW.BERT_STAMMDATEN_JP` (Airflow SubDAG/TaskGroup: `bert_stammdaten_jp`)
      *   Includes jobs like `DW.BERT_DROP_TEMP_TABLE`, `DW.BERT_P_ADRESSEN`, `DW.BERT_P_AUSTAUSCH`, `DW.BERT_P_BASISPRODUKT_JP` (which itself contains `DW.BERT_AUSD_BP_TA_APN_CARMEN`, `DW.BERT_AUSD_BP_TA_APN_VERTRAG`, `DW.BERT_AUSD_BP_TA_BCP_ICCID`).
  *   Invokes `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` (Airflow Event/Sensor: `dwh_run_apt_export_monatlich_jp_evt`)

The `SYNCREF` in the UC4 XML indicates synchronization objects (`DW.BERT_ABLAUFSTEUERUNG_SYNC`) with `Abend="SETZE_FREI"`, `End="SETZE_FREI"`, `Start="SETZE_LAEUFT"`. This synchronization logic will need to be explicitly modeled in Airflow using `ExternalTaskSensor` or other Airflow synchronization primitives.

## 5. Transformation Logic
The main `DW.BERT_ABLAUFSTEUERUNG` UC4 job is primarily a scheduler. Its transformation logic is embedded in the scheduling parameters and task definitions.
*   **Scheduling:** The `<ATTR_JSCH>` section defines `Queue`, `Period` (e.g., `1` for daily), `StartTime` (e.g., `00:00`), `UC4Priority`. The `<task>` elements within `<JschStruct>` define `TimePeriod` and `after` conditions (`ErlstStTime` for earliest start time). These will be translated to Airflow `schedule_interval` and `start_date` parameters, along with task dependencies (e.g., `task_a >> task_b`) and possibly `BranchPythonOperator` for conditional execution. Calendars (`<calendars>`) like `DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT` will be translated into Airflow sensors or custom Python logic within the DAG to control task execution based on specific dates or conditions.
*   **Child Job Plans (JOBP):** These will become separate Airflow DAGs or TaskGroups within the main `bert_ablaufsteuerung_dag.py`.
*   **Events (EVNT):** These will likely become `ExternalTaskSensor` or custom `PythonOperator` tasks that wait for specific conditions or external triggers.
*   **Underlying SQL/Scripts:**
    *   The various `.sql` files identified in the lineage will be rewritten as BigQuery SQL queries.
    *   The `r_exis_v2` shell script, and any other shell scripts (`BERT_LOG.KSH`, `SQL.KSH` from `UNRESOLVED` nodes), will need to be analyzed for their functionality. If they perform file operations, these will be replaced with Google Cloud Storage operations. If they invoke other processes or databases, they will be replaced with appropriate Airflow operators (e.g., `BigQueryOperator`, `DataprocOperator`, `PythonOperator`).

## 6. External Dependencies
*   **Database access for SQL scripts:** The original SQL scripts read from various tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `DWH$VI_C_VERTRAG`). These source databases are external to the new BigQuery environment. Data will need to be ingested into BigQuery first, likely via a separate data ingestion pipeline (e.g., Datastream, Fivetran, custom Dataflow jobs).
*   **`EXT:DATABASE` (used by `r_exis_v2`):** This signifies a connection to an external database. The specifics of this database are unknown but require resolution. It will either be migrated to BigQuery, or a BigQuery federated query / Cloud SQL instance will be established.
*   **`HOST:DWHDWH2P`:** This host is called by several child jobs. Its purpose (e.g., file transfer, API call, database connection) needs to be determined. It will likely be replaced by Cloud Storage for file transfers (via `GCSToSftpOperator`, `SftpToGCSOperator` etc.) or API calls via `PythonOperator`/Cloud Functions.
*   **`LOGIN:DW.UNIX.ISBERT`:** This is a UC4 login object. Credentials will be managed securely using Google Secret Manager and injected into Airflow tasks.

## 7. Unresolved / Risks
*   **`UNRESOLVED:BERT_LOG.KSH` and `UNRESOLVED:SQL.KSH`:** The exact functionality of these shell scripts is unknown. They need to be analyzed to determine their migration path (e.g., Python script, Airflow BashOperator, or re-implementation using Google Cloud services). Without the content of these scripts, a precise migration plan is not possible.
*   **Calendar Translation (`DW.NEW_CALENDAR`, `DW.KALENDER`):** The complex calendar logic in UC4 needs careful translation to Airflow's scheduling mechanisms. This might require custom `PythonOperator` tasks for sophisticated calendar rules.
*   **`EXT:DATABASE` details:** The specific type and access method for `EXT:DATABASE` are unknown, making it hard to plan its replacement. This needs further investigation.
*   **`DWHDWH2P` host functionality:** The exact interaction with `DWHDWH2P` needs to be clarified to choose the appropriate Google Cloud service.
*   **`migration_bucket: manual`:** The job is marked as "manual" migration, indicating a high level of custom development and manual effort will be required.

## 8. Build Plan
The migration will involve creating the following artifacts:

1.  **`dags/bert_ablaufsteuerung_dag.py` (Python, Airflow DAG):**
    *   Defines the overall orchestration of the Bert processes.
    *   Translates the scheduling logic (start times, periods, calendar dependencies from `DW.BERT_ABLAUFSTEUERUNG.xml`) into Airflow `schedule_interval` and task dependencies.
    *   Includes `BigQueryOperator` tasks for executing the transformed SQL.
    *   Includes `PythonOperator` or `BashOperator` tasks for handling custom logic, external calls, and file exports.
    *   Implements synchronization logic (`DW.BERT_ABLAUFSTEUERUNG_SYNC`) using Airflow sensors or other mechanisms.

2.  **`dags/sub_dags/bert_monatlich_jp.py` (Python, Airflow TaskGroup/SubDAG):**
    *   Orchestrates the monthly Bert processes, including `DW.BERT_RECHNUNGSDATEN` and `DW.BERT_LOG`.

3.  **`dags/sub_dags/bert_adm_housekeeping_jp.py` (Python, Airflow TaskGroup/SubDAG):**
    *   Contains tasks for administrative and housekeeping functions.

4.  **`dags/sub_dags/dwh_apt_export_taeglich_jp.py` (Python, Airflow TaskGroup/SubDAG):**
    *   Orchestrates the daily APT exports.
    *   Includes `BigQueryOperator` tasks for `d_exis_apt_bestandsdaten.sql`, `d_exis_apt_nna_daten.sql`, `d_exis_apt_nna_voice.sql`, `d_exis_apt_rabattdaten.sql`.
    *   Includes tasks for exporting data, potentially using `GCSFileTransferOperator` or custom `PythonOperator`.

5.  **`dags/sub_dags/bert_stammdaten_jp.py` (Python, Airflow TaskGroup/SubDAG):**
    *   Orchestrates the Bert master data processes.
    *   Includes tasks for `DW.BERT_DROP_TEMP_TABLE`, `DW.BERT_P_ADRESSEN`, `DW.BERT_P_AUSTAUSCH`, `DW.BERT_P_BASISPRODUKT_JP` and its sub-components.

6.  **`sql/bert_bestandsdaten.sql` (BigQuery SQL):**
    *   Rewritten version of `d_exis_apt_bestandsdaten.sql`.

7.  **`sql/bert_nna_daten.sql` (BigQuery SQL):**
    *   Rewritten version of `d_exis_apt_nna_daten.sql`.

8.  **`sql/bert_nna_voice.sql` (BigQuery SQL):**
    *   Rewritten version of `d_exis_apt_nna_voice.sql`.

9.  **`sql/bert_rabattdaten.sql` (BigQuery SQL):**
    *   Rewritten version of `d_exis_apt_rabattdaten.sql`.

10. **`scripts/r_exis_v2.py` (Python):**
    *   Re-implementation of `r_exis_v2` shell script, using GCS for file operations and appropriate Google Cloud services for external database interactions.

11. **`scripts/bert_log.py` (Python):**
    *   Re-implementation of `BERT_LOG.KSH` for logging within the Cloud environment.

12. **`scripts/sql_runner.py` (Python):**
    *   Re-implementation of `SQL.KSH` for running SQL queries if it had specific logic beyond simple execution.

13. **BigQuery Table Definitions (DDL):**
    *   For all source tables (`RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, etc.) and any intermediate/target tables.

