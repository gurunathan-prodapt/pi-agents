# Migration Design — DW.DWH_APT_EXPORT_MONATLICH_JP

## 1. Purpose & Scope

This migration job, `DW.DWH_APT_EXPORT_MONATLICH_JP`, is a monthly export process responsible for extracting telephone system master data. The data is exported into compressed CSV files and distributed to a target system. The job is orchestrated by an Automic (UC4) Job Plan, triggered by a time-based event, and involves two parallel UNIX jobs for the actual data extraction.

The scope of this migration is to re-platform the existing UC4-managed monthly data export workflow to Google Cloud Platform (GCP). This includes:
*   Migrating the UC4 Job Plan and Event scheduling to Airflow DAGs.
*   Re-implementing the UNIX shell script-based data extraction and export logic using GCP-native services, likely PySpark on Dataproc or Dataflow, targeting BigQuery for data warehousing and Cloud Storage for exported files.

## 2. Source Inventory

The job consists of four primary UC4 components:

| File Name | Category | Tool | Summary | Complexity Tier | Automation Bucket |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------- | :----------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------- | :---------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_APT_EXPORT_MONATLICH_JP.xml` | `uc4`    | `UC4/Automic` | This UC4 Job Plan orchestrates the execution of other jobs to export data into CSV files. It includes synchronization objects and conditional post-processing. | `medium`        | `semi_auto`       |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml` | `uc4`    | `UC4/Automic` | This UC4 job defines a UNIX job that exports telephone system master data into a compressed CSV file and distributes it to a target system. | `medium`        | `semi_auto`       |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_VOIC.xml` | `uc4`    | `UC4/Automic` | This UC4 JOBS_UNIX object defines a job for exporting telephone system master data. It executes a shell script to export data into a compressed CSV file and distributes it to a target system. | `medium`        | `semi_auto`       |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT.xml` | `uc4`    | `UC4/Automic` | This UC4 Event (Time) job defines a schedule and conditional logic to activate a monthly export job plan, DW.DWH_APT_EXPORT_MONATLICH_JP, based on the successful completion of two prerequisite job plans. | `medium`        | `semi_auto`       |

## 3. Target Architecture

The target architecture on GCP will leverage the following components:
*   **Orchestration:** Cloud Composer (managed Apache Airflow) will manage the scheduling and dependencies of the migration job.
*   **Data Processing:** PySpark jobs running on Dataproc (managed Spark) or Dataflow will handle the data extraction, transformation, and loading (ETL) logic, replacing the existing UNIX shell scripts.
*   **Data Storage:** BigQuery will serve as the target data warehouse for any processed data that needs to be stored. Cloud Storage (GCS) will be used for intermediate staging of data, as well as the final destination for exported CSV files.
*   **Source Data:** The current external database accessed via `EXT:DATABASE` will be integrated with Dataproc/Dataflow, likely through appropriate connectors (e.g., JDBC for relational databases, or specific APIs/clients for other systems).

## 4. Data Flow & Lineage

The data flow in the migrated system will largely mirror the logical flow of the current UC4 job, but with GCP-native components:

1.  **Event Trigger (`dw_dwh_run_apt_export_monatlich_jp_evt` DAG):**
    *   An Airflow DAG, `dw_dwh_run_apt_export_monatlich_jp_evt`, will be scheduled (e.g., monthly, daily polling) to mimic the UC4 `EVNT_TIME` object.
    *   This DAG will contain a Python task (`guard_prerequisite_check`) that checks the status of external prerequisite job plans: `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`. This check will likely involve querying metadata of corresponding Airflow DAGs or external systems if they are not yet migrated.
    *   If prerequisites are met, another Python task (`activate_target_jobplan`) will trigger the main data export DAG, `dw_dwh_apt_export_monatlich_jp`.
    *   A guard task for `Else=Skip` synchronization will ensure only one active run.

2.  **Main Data Export Orchestration (`dw_dwh_apt_export_monatlich_jp` DAG):**
    *   This Airflow DAG, `dw_dwh_apt_export_monatlich_jp`, will orchestrate the data export process.
    *   It will consist of a `start` task followed by two parallel data processing tasks: `dw_dwh_exis_sd_apt_nna_data` and `dw_dwh_exis_sd_apt_nna_voic`.
    *   A `DataprocSubmitJobOperator` (or similar for Dataflow) will be used for these tasks.
    *   The DAG will wait for both data export tasks to complete successfully before proceeding to an `end` task.

3.  **Data Export Jobs (`dw_dwh_exis_sd_apt_nna_data` and `dw_dwh_exis_sd_apt_nna_voic`):**
    *   These will be re-implemented as PySpark jobs (or Dataflow pipelines) run on Dataproc/Dataflow.
    *   Each job will connect to the `EXT:DATABASE` (e.g., using Spark's JDBC connector or Dataflow's capabilities).
    *   They will extract the relevant "telephone system master data" based on logic currently embedded in `r_exis_v2` and its configuration files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`).
    *   The extracted data will be transformed as needed and then written to compressed CSV files in a specified GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/exports/DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`).
    *   The distribution to the target system will involve making these GCS files accessible or triggering downstream processes that consume from GCS.

## 5. Transformation Logic

**5.1. Orchestration and Scheduling (UC4 Job Plan & Event to Airflow DAGs)**

*   **DW.DWH_APT_EXPORT_MONATLICH_JP (Job Plan) -> `dw_dwh_apt_export_monatlich_jp` Airflow DAG:**
    *   **Purpose:** Orchestrate parallel data export jobs.
    *   **Mapping:** A main Airflow DAG with `dag_id='dw_dwh_apt_export_monatlich_jp'`.
    *   **Tasks:**
        *   `start`: `EmptyOperator`
        *   `dw_dwh_exis_sd_apt_nna_data`: `DataprocSubmitJobOperator` (placeholder for re-implemented data export)
        *   `dw_dwh_exis_sd_apt_nna_voic`: `DataprocSubmitJobOperator` (placeholder for re-implemented data export)
        *   `end`: `EmptyOperator`
    *   **Dependencies:** `start >> [dw_dwh_exis_sd_apt_nna_data, dw_dwh_exis_sd_apt_nna_voic] >> end`.
    *   **Synchronization:** `SYNCREF Else=Wait` will be handled by `max_active_runs=1` in the DAG definition.

*   **DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT (Event) -> `dw_dwh_run_apt_export_monatlich_jp_evt` Airflow DAG:**
    *   **Purpose:** Trigger the main export Job Plan based on time and prerequisite job statuses.
    *   **Mapping:** An Airflow DAG with `dag_id='dw_dwh_run_apt_export_monatlich_jp_evt'`.
    *   **Schedule:** Approximated to `0 7 * * *` (daily at 07:00 AM) based on `TimePeriodTT=0720` in UC4. This needs manual validation for monthly intent.
    *   **Tasks:**
        *   `start`: `EmptyOperator`
        *   `guard_prerequisite_check`: `PythonOperator` to implement the logic for checking `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` status (UC4 status "1900" for success). This will use Airflow's `DagRun.find` or external system checks. This task will also implement the `SYNCREF Else=Skip` logic from the event object.
        *   `activate_target_jobplan`: `PythonOperator` that uses `TriggerDagRunOperator` or a custom Airflow API call to trigger `dw_dwh_apt_export_monatlich_jp`.
        *   `cancel_event`: `PythonOperator` for logging/cleanup (UC4 `CANCEL_UC_OBJECT`).
        *   `end`: `EmptyOperator`
    *   **Dependencies:** `start >> guard_prerequisite_check >> activate_target_jobplan >> cancel_event >> end`.

**5.2. Data Export Logic (UC4 JOBS_UNIX to PySpark/Dataflow)**

*   **DW.DWH_EXIS_SD_APT_NNA_DATA.xml (JOBS_UNIX) & DW.DWH_EXIS_SD_APT_NNA_VOIC.xml (JOBS_UNIX):**
    *   **Current Logic:** Both jobs execute a custom `r_exis_v2` executable with different configuration files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`) and a monthly parameter (`&MONAT_ID`). The executable interacts with `EXT:DATABASE` (Oracle) to extract data and exports it to compressed CSV files.
    *   **Target Logic:** Each of these will be re-implemented as a separate PySpark application (or Dataflow pipeline) deployed to GCS.
        *   **Data Source Connection:** The PySpark/Dataflow job will establish a connection to the source `EXT:DATABASE` (Oracle). This requires appropriate JDBC drivers and connection string management (e.g., using Secret Manager).
        *   **Query/Extraction:** The current `r_exis_v2` logic, including the SQL queries it executes (implicitly from `.var` files), needs to be reverse-engineered and translated into PySpark/SQL or Dataflow transformations. The UC4 `SCRIPT` sections for these UNIX jobs refer to SQL files like `d_exis_apt_nna_daten.sql` and `d_exis_apt_nna_voice.sql` via `lineage_edges` indicating the source of data for these exports.
        *   **Transformation:** Any in-script transformations or logic within `r_exis_v2` will be converted to PySpark DataFrames or Dataflow transforms.
        *   **Output:** The transformed data will be written as compressed CSV files to a designated Cloud Storage bucket. The filename convention (`DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`) will be maintained.
        *   **Parameters:** The `MONAT_ID` parameter will be passed as an Airflow XCom or as a DAG run configuration to the PySpark/Dataflow job.
        *   **Utility scripts:** The `:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG` calls need to be analyzed. If they are generic utility functions, they can be re-implemented as Python helper functions or logging utilities in the GCP environment.

## 6. External Dependencies

The following external dependencies have been identified:

*   **`EXT:DATABASE` (Oracle):** This is the source database for the master data.
    *   **Replacement Strategy:** Dataproc/Dataflow jobs will connect directly to this Oracle database using appropriate JDBC drivers and service accounts with necessary permissions. The database hostname/IP and credentials should be securely managed (e.g., in Secret Manager or environment variables).
*   **`HOST:DWHDWH1P`:** This is the UNIX host where the `JOBS_UNIX` scripts execute and likely where the exported files are initially written or distributed from.
    *   **Replacement Strategy:** This host will be replaced by Dataproc clusters or Dataflow workers for job execution, and Cloud Storage will replace local filesystem storage and distribution.
*   **`JOBP:DW.BERT_STAMMDATEN_JP`:** A prerequisite UC4 Job Plan checked by the Event object.
    *   **Replacement Strategy:** If `DW.BERT_STAMMDATEN_JP` is also migrated to Airflow, its success will be checked via Airflow's `ExternalTaskSensor` or by querying Airflow metadata. If it remains an external system, a custom sensor or API call to its scheduling system will be required.
*   **`JOBP:DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`:** Another prerequisite UC4 Job Plan checked by the Event object.
    *   **Replacement Strategy:** Similar to `DW.BERT_STAMMDATEN_JP`, its success will be checked via Airflow or a custom sensor/API call.
*   **`DW.HOLE_PFAD` and `DW.LESE_LOG`:** These are UC4 include objects referenced by the UNIX scripts.
    *   **Replacement Strategy:** These are likely common utility scripts for path management or logging. They should be re-implemented as Python helper functions or modules within the PySpark/Dataflow code or integrated into a centralized logging solution like Cloud Logging. The content of these includes needs to be analyzed for precise migration.

## 7. Unresolved / Risks

*   **`r_exis_v2` Executable Logic:** The exact SQL queries, transformations, and business logic within the `r_exis_v2` binary and its `.var` configuration files are unknown. This is the biggest risk and requires manual reverse engineering or documentation to correctly re-implement in PySpark/Dataflow.
*   **`.dw_init` script:** The UNIX jobs execute `. $HOME/.dw_init`. The content of this initialization script is unknown and might contain critical environment setups, variable definitions, or function imports.
*   **`EXT:DATABASE` Details:** While identified as Oracle, specific schema, table names, and connection details for `EXT:DATABASE` are not fully detailed. Further investigation is needed to configure database connectors.
*   **Monthly Schedule:** The UC4 `EVNT_TIME` object's `TimePeriodTT=0720` combined with the "monthly" description needs clarification for the precise Airflow cron schedule.
*   **`Else=Skip` Implementation:** While a guard task is proposed for `Else=Skip` on the Event DAG, careful testing is required to ensure it perfectly matches the UC4 behavior, especially concerning concurrency and what constitutes an "active run."
*   **UC4 External Object Activation/Cancellation:** The `ACTIVATE_UC_OBJECT` and `CANCEL_UC_OBJECT` calls have no direct Airflow equivalents. Their migration depends on the target state of the referenced objects. If `DW.DWH_APT_EXPORT_MONATLICH_JP` is fully migrated to Airflow, `TriggerDagRunOperator` is suitable. Cancellation implies stopping a running DAG run, which may require Airflow API interaction or a simpler "do nothing" if the new Airflow logic handles concurrency.
*   **Postcondition Actions:** The postcondition actions in the main Job Plan (`EXECUTE OBJECT DW.CALL_STANDARD`, `BLOCK`) need analysis. `DW.CALL_STANDARD` could be an alert or a generic handler, while `BLOCK` implies terminal failure. These should be mapped to Airflow callbacks or alert mechanisms.

## 8. Build Plan

The migration will involve building the following components:

1.  **Airflow DAG: `dw_dwh_run_apt_export_monatlich_jp_evt.py`**
    *   **Language:** Python
    *   **Purpose:** Schedule and trigger the main data export DAG, including prerequisite checks.
    *   **Tasks:**
        *   `start` (EmptyOperator)
        *   `guard_concurrency` (PythonOperator to enforce `Else=Skip`)
        *   `check_prerequisites` (PythonOperator to check `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` status)
        *   `trigger_main_export_dag` (TriggerDagRunOperator for `dw_dwh_apt_export_monatlich_jp`)
        *   `cleanup_event_state` (PythonOperator for logging/cleanup)
        *   `end` (EmptyOperator)

2.  **Airflow DAG: `dw_dwh_apt_export_monatlich_jp.py`**
    *   **Language:** Python
    *   **Purpose:** Orchestrate the parallel data export jobs.
    *   **Tasks:**
        *   `start` (EmptyOperator)
        *   `export_nna_data` (DataprocSubmitJobOperator, references `nna_data_exporter.py`)
        *   `export_nna_voice` (DataprocSubmitJobOperator, references `nna_voice_exporter.py`)
        *   `end` (EmptyOperator)

3.  **PySpark Application: `nna_data_exporter.py`**
    *   **Language:** Python (PySpark)
    *   **Purpose:** Extract, transform, and export telephone system master data (DATA).
    *   **Deployment:** GCS bucket for PySpark scripts.
    *   **Dependencies:** Spark JDBC driver for Oracle.

4.  **PySpark Application: `nna_voice_exporter.py`**
    *   **Language:** Python (PySpark)
    *   **Purpose:** Extract, transform, and export telephone system master data (VOIC).
    *   **Deployment:** GCS bucket for PySpark scripts.
    *   **Dependencies:** Spark JDBC driver for Oracle.

5.  **Configuration Files for PySpark Jobs:**
    *   Translate `h_exis_apt_nna_daten.var` and `h_exis_apt_nna_voice.var` into a suitable format (e.g., YAML, JSON, or Python dictionaries) to be consumed by the PySpark applications. This may also involve extracting embedded SQL.

This plan addresses all identified components and dependencies, proposing concrete target technologies and outlining the re-engineering required.