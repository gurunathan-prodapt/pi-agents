# MIGRATION NOTES: DW.DWH_ABPZ_KKM_AIL_AGENT

This document provides comprehensive migration notes for transitioning the legacy job `DW.DWH_ABPZ_KKM_AIL_AGENT` from its on-premise UC4, KornShell, and Ab Initio environment to Google Cloud Platform (GCP).

---

## 1. SUMMARY

The `DW.DWH_ABPZ_KKM_AIL_AGENT` batch job has been migrated from an on-premise Unix/Oracle Data Warehouse environment to a modern, cloud-native architecture on Google Cloud Platform (GCP).

### 1.1. Migration Scope
*   **Orchestration:** Converted from UC4/Automic Job Scheduler to **Google Cloud Composer (Airflow)**.
*   **Wrapper Scripts & Utilities:** Legacy KornShell scripts (`r_alis_objekt`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`) have been replaced by native Python operators, standard Airflow macros, and standard Python libraries (`datetime`, `timedelta`).
*   **Data Processing:** The Ab Initio GDE Graph `BHB_CCM_PROC_WriteAgentADSLookup` (which queries the Oracle View `DWH$VI_S_SDM_AGENT_ADS` and generates a flat-file lookup) has been refactored into a **Dataproc Serverless (PySpark)** pipeline.
*   **Data Storage & Querying:** The source Oracle view is mapped to a **BigQuery** view, and the output is written both as a pipe-delimited flat-file in **Google Cloud Storage (GCS)** and mirrored to a **BigQuery** lookup table.

### 1.2. Target Architecture Mapping
```
[ Legacy Source ]              ───►  [ GCP Target ]
UC4 Job Plan                         Cloud Composer (Airflow DAG)
KornShell Wrappers                   Python Operators / Airflow Tasks
Ab Initio Graph                      Dataproc Serverless (PySpark Job)
Oracle View                          BigQuery View (dwh_views.vi_s_sdm_agent_ads)
Flat-file Target                     GCS Object (gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt)
Oracle Lookup Table                  BigQuery Table (dw_lookups.agent_ads_lookup)
```

---

## 2. GENERATED ARTIFACTS

The migration process generated the following files, which must be deployed to the Cloud Composer environment:

### 2.1. Orchestration DAG
*   **File Path:** `dags/kkm_daily_imports_dag.py`
*   **Role:** Defines the Airflow DAG `dw_dwh_abpz_kkm_ail_agent`. It handles execution sequencing, legacy-compatible start/end logging, date calculations (84-day lookback window), and submits the PySpark job to Dataproc.

### 2.2. Data Processing Pipeline
*   **File Path:** `dags/scripts/dataproc/write_agent_ads_lookup.py`
*   **Role:** The translated PySpark application. It reads from the BigQuery source view, filters records based on the calculated date window, writes a single pipe-delimited CSV to GCS, and overwrites the BigQuery lookup table.

---

## 3. KEY DESIGN DECISIONS

### 3.1. PySpark for Ab Initio Translation
*   **Decision:** Refactor the Ab Initio graph into a PySpark script running on Dataproc Serverless rather than using SQL-only transformations.
*   **Reasoning:** The legacy graph outputs a physical flat-file (`AgentADSLookup.txt`) used as an in-memory lookup by downstream Ab Initio graphs. PySpark allows us to easily write a single, coalesced, pipe-delimited file to GCS while simultaneously updating a BigQuery table for SQL-based downstream processes.

### 3.2. Date Calculation via Airflow PythonOperator
*   **Decision:** Replaced the legacy `h_alis_date.ksh` utility with a native Airflow `PythonOperator` that pushes calculated dates (`FirstDay` and `LastDayPlus1`) to XCom.
*   **Reasoning:** Keeps the PySpark code clean and decoupled from Airflow-specific execution variables, allowing the PySpark script to run independently if needed (e.g., during manual backfills).

### 3.3. Single-File Coalesce (`coalesce(1)`)
*   **Decision:** Used `df.coalesce(1)` before writing to GCS.
*   **Reasoning:** Downstream legacy systems expect a single flat-file lookup asset. While `coalesce(1)` can limit write performance on massive datasets, the filtered agent dimension dataset is small enough that the simplicity of a single output file outweighs the performance trade-off.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

The following infrastructure, schema, and security configurations must be established before executing the migrated job in production:

### 4.1. BigQuery Schema & Dataset Creation
Ensure the target datasets exist in your BigQuery project:
```sql
-- Create the views dataset if not present
CREATE SCHEMA IF NOT EXISTS `your-gcp-project.dwh_views`;

-- Create the lookups dataset
CREATE SCHEMA IF NOT EXISTS `your-gcp-project.dw_lookups`;
```
*   Verify that the source view `dwh_views.vi_s_sdm_agent_ads` is fully migrated, populated, and contains the `stichtag` column.

### 4.2. IAM & Permissions
The Cloud Composer worker service account (e.g., `composer-worker-sa@your-project.iam.gserviceaccount.com`) requires the following roles:
*   **Dataproc Worker** (`roles/dataproc.worker`)
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on `dw_lookups`
*   **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on `dwh_views`
*   **Storage Object Admin** (`roles/storage.objectAdmin`) on the target GCS bucket.

### 4.3. Airflow Variables Configuration
The following variables must be configured in the Cloud Composer Airflow UI (**Admin -> Variables**):

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-123` | The target Google Cloud Project ID |
| `GCS_BUCKET` | `prod-dwh-composer-bucket` | The GCS bucket where DAGs and scripts reside |
| `DATAPROC_REGION` | `europe-west3` | The GCP region for Dataproc execution |
| `DATAPROC_CLUSTER` | `dwh-dataproc-cluster` | The name of the active Dataproc cluster |

### 4.4. Directory Structure in GCS
Upload the generated files to the Composer environment's GCS bucket:
```bash
gsutil cp dags/kkm_daily_imports_dag.py gs://{GCS_BUCKET}/dags/
gsutil cp dags/scripts/dataproc/write_agent_ads_lookup.py gs://{GCS_BUCKET}/dags/scripts/dataproc/
```

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

### 5.1. Error Log Viewer (`showlog.ksh`)
*   **Gap:** The legacy job calls `showlog -uc4` upon failure. This utility is not present in the GCP environment.
*   **Mitigation:** Standard Airflow task failure handlers and Cloud Logging have replaced this. If a task fails, Airflow automatically captures stdout/stderr. Cloud Monitoring alerts should be configured to notify operations teams of DAG failures.

### 5.2. Upstream Job Dependencies
*   **Gap:** In UC4, this job is triggered after `DW.DWH_KKM_AI_LOOKUPS_TAEGLICH_GV_JP` completes.
*   **Mitigation:** In the target environment, this DAG must either be scheduled to run after the upstream DAG completes, or integrated into a master DAG using `ExternalTaskSensor` operators.

---

## 6. VALIDATION

To validate the migration, perform the following test execution:

### 6.1. How to Run the Test
1.  Trigger the DAG manually from the Airflow UI: `dw_dwh_abpz_kkm_ail_agent`.
2.  Monitor the task execution logs in the Airflow UI.
3.  Verify that the `calculate_processing_window` task successfully pushes the correct `first_day` (84 days ago) and `last_day_plus_1` (today) to XCom.

### 6.2. Definition of "Passing" (Success Criteria)
*   **Airflow Status:** All tasks (`dw_dwh_adm_job_monitor_start` $\rightarrow$ `calculate_processing_window` $\rightarrow$ `run_write_agent_ads_lookup` $\rightarrow$ `dw_dwh_adm_job_monitor_end`) complete with a `success` status.
*   **GCS Output:** A pipe-delimited file is successfully generated at `gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt/part-*.csv` (and is non-empty).
*   **BigQuery Output:** The table `dw_lookups.agent_ads_lookup` is populated with records matching the filtered date range. You can verify this by running:
    ```sql
    SELECT COUNT(*), MIN(stichtag), MAX(stichtag) 
    FROM `your-gcp-project.dw_lookups.agent_ads_lookup`;
    ```

---

## 7. ROLLBACK PROCEDURE

In the event of an operational failure or data corruption during deployment, execute the following rollback steps:

### 7.1. Pause the Airflow DAG
Immediately pause the DAG in the Airflow UI to prevent subsequent daily executions:
```bash
gcloud composer environments run {COMPOSER_ENV_NAME} \
    --location {DATAPROC_REGION} \
    dags pause -- dw_dwh_abpz_kkm_ail_agent
```

### 7.2. Revert Data Assets
If the BigQuery lookup table was corrupted, restore it to its previous state using BigQuery's time travel feature or delete the corrupted table:
```sql
-- Drop the corrupted table
DROP TABLE IF EXISTS `your-gcp-project.dw_lookups.agent_ads_lookup`;
```

### 7.3. Re-enable Legacy UC4 Job
If a fallback to the on-premise environment is required:
1.  Ensure the legacy Oracle database and UC4 agents are active.
2.  Re-activate the job `DW.DWH_ABPZ_KKM_AIL_AGENT` in the UC4 scheduler.
3.  Verify that the legacy Ab Initio graph executes and generates the local `AgentADSLookup.txt` file.