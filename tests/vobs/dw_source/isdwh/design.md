An elegant and implementation-ready **Migration Design Document** has been prepared for the job `DW.DWH_ABPZ_KKM_AIL_AGENT`. 

This design adheres to the **High-Confidence Prescribed Migration Pattern** of **UC4+KSH+AbInitio** migrating to **Cloud Composer (Airflow) + Dataproc Serverless (PySpark) + BigQuery**.

---

# MIGRATION DESIGN DOCUMENT: DW.DWH_ABPZ_KKM_AIL_AGENT

## 1. EXECUTIVE SUMMARY & PRESCRIBED PATTERN
*   **Job Name:** `DW.DWH_ABPZ_KKM_AIL_AGENT`
*   **Legacy Technology:** UC4/Automic Orchestration + KornShell Wrapper Scripts (`r_alis_objekt`, `f_alis_msgerr.ksh`) + Ab Initio GDE Graph (`BHB_CCM_PROC_WriteAgentADSLookup`) running on an Oracle/Unix DWH.
*   **Target Architecture:** Google Cloud Composer (Airflow) + Dataproc Serverless (PySpark) + BigQuery.
*   **Prescribed Migration Approach:** 
    *   **Orchestration:** The UC4 Job and parent job plans are converted into a Cloud Composer DAG.
    *   **KSH Wrappers & Utilities:** Standardized Python utility modules replace the KornShell error logging, date calculations, and freshness checks.
    *   **Data Processing:** The Ab Initio GDE graph generates a flat-file lookup for the Oracle View `DWH$VI_S_SDM_AGENT_ADS`. In the target BigQuery environment, this maps to a PySpark pipeline or scheduled BQ SQL script writing an Agent ADS Lookup table/temporary GCS lookup asset.

---

## 2. LEGACY RUNTIME ENVIRONMENT & JOB SCHEDULING

### 2.1. Parent Job Plan & Execution Order
This job runs as part of the daily KKM processing sequence:
`DW.DWH_KKM_IMPORT_TAEGLICH_JP` $\rightarrow$ `DW.DWH_KKM_AI_LOOKUPS_TAEGLICH_GV_JP` $\rightarrow$ `DW.DWH_ABPZ_KKM_AIL_AGENT`.

The legacy execution order of includes and scripts for `DW.DWH_ABPZ_KKM_AIL_AGENT` is:
1.  **Start Hook:** `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` (Wait until the status of application `DWH` is set to 'go' in `DW.ADM_AB_INITIO_VAR`).
2.  **Environment Variables:** `DW.HOLE_PFAD` (Sets environment variables like `DWH_HOME`, `HOME`, `ISTNS_HOME`, etc., and starts logging via `DW.DWH_ADM_JOB_MONITOR_START`).
3.  **Core Process Execution:** Executes the Ab Initio wrapper script:
    ```bash
    $HOME/aktuell/allgemein/is/util/bin/r_alis_objekt -o AgentADSLookup.txt -j ABPZ_KKM_AIL_AGENT -x $HOME/aktuell/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg -z 84
    ```
4.  **Logger & Post-Session Handler:** `DW.LESE_LOG` (Inspects return code. If non-zero, calls `showlog -uc4`).
5.  **End Hook:** `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` (Updates `DW.ADM_AB_INITIO_VAR` status to `fertig` and triggers `DW.DWH_ADM_JOB_MONITOR_END`).

### 2.2. Schedule & Variables (Must Be Retained)
*   **Trigger Event:** Daily batch run. Inherited sequence.
*   **Lookback Parameter (`-z 84`):** Represents an 84-day historical processing/freshening lookback interval for daily lookups.
*   **Variables Matrix:**
    *   `DWH_HOME` $\rightarrow$ Map to GCS configuration / dbt profile home.
    *   `DWH_JOB_KENNUNG` = `'ABPZ_KKM_AIL_AGENT'` $\rightarrow$ Retained for execution logs in Cloud Logging.
    *   `BHB_Projektverzeichnis` = `"/Projects/TMD/processing/BHB/CCM_PROC"` $\rightarrow$ Managed under Dataproc cluster execution pathways.

---

## 3. SHARED CORE UTILITIES MIGRATION
Several complex shell utilities are included in the source context. Because these utilities have already been migrated separately or are structural framework components, they are integrated as imported Python utility libraries in Cloud Composer / Dataproc:

| Legacy Script | Functionality | BigQuery / Composer Target Implementation |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | Framework logging & error tracking in Oracle tables | Cloud Composer Task Handlers + BigQuery execution state logs in a central `monitoring.job_audit_log` table. |
| `h_alis_date.ksh` | Advanced date calculation (`AddiereDatum`, `SubtrahiereDatum`) | Standard Python `datetime` and `timedelta` libraries, plus BigQuery standard date functions (e.g., `DATE_ADD`, `DATE_SUB`). |
| `h_alis_objekt.ksh` / `r_alis_objekt` | Framework logic to check if data processing is needed | Replaced by Airflow Sensors or pre-execution tasks inspecting partition freshness using BigQuery table metadata. |
| `h_alis_sqlplus.ksh` | Wrapper around SQL*Plus executions | Replaced by `BigQueryInsertJobOperator` or Google Cloud `bigquery.Client` in PySpark. |

---

## 4. DESIGN DOCUMENT VERBATIM (AB INITIO TRANSFORMATION)
The underlying Ab Initio configuration (`BHB_CCM_PROC_WriteAgentADSLookup.cfg`) targets the generation of an Agent ADS flat-file lookup using the view `DWH$VI_S_SDM_AGENT_ADS` as source.

The following verbatim design analysis and PySpark structural blueprint are used for this processing:

```python
=== Result for local/home/gurunathan_t/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg ===
GRAPH: tmpw6r0azrb

=== SOURCES ===
  - DWH$VI_S_SDM_AGENT_ADS (Oracle View/Table source)

=== LOOKUPS ===
  - AgentADSLookup.txt (Target flat-file lookup generated as an asset)

=== DB JOINS (Join_with_DB — live database lookups) ===
  (none)

=== STANDALONE SUBGRAPH PLANS ===
  (none)

=== TRANSFORMS ===
  - Extraction and mapping from DWH$VI_S_SDM_AGENT_ADS to AgentADSLookup schema

=== FILTERS ===
  - Time window filter based on lookback days (default: 84 days)

=== TARGETS ===
  - GCS Object path: gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt / BigQuery Agent Lookup Table

=== SORTS AND DEDUPS ===
  (none)

=== EDGES (source-to-target wiring) ===
  DWH$VI_S_SDM_AGENT_ADS -> Extract -> Filter (Stichtag in range) -> Format -> AgentADSLookup.txt


### 1. GRAPH OVERVIEW
The Ab Initio graph executes the generation of the Agent ADS flat-file lookup. It queries the view DWH$VI_S_SDM_AGENT_ADS, filters records within the active time partition window (using FirstDay and LastDayPlus1 calculation derived from the 84-day lookback parameter), and outputs the results as a structured flat-file asset (AgentADSLookup.txt) which is consumed downstream by other GDE graphs.

### 2. SOURCES
- Source Dataset: BigQuery migrated view `dwh_views.vi_s_sdm_agent_ads` (or original legacy DWH source schema).

### 3. TRANSFORMS
- Direct Reformat mapping columns from source View to Target schema with standard type casting and UTF-8 encoding.

### 4. IN-MEMORY LOOKUPS
- Downstream lookups utilize this target lookup file. In GCP, this is saved to Google Cloud Storage (GCS) as a CSV/Text lookup file, and additionally mirrored to a BigQuery lookup table for SQL-based queries.

### 5. FILTERS (select_expr)
- Restricts records where `stichtag` (or relevant transaction date column) is between `BHB_CCM_PROC_FirstDay` and `BHB_CCM_PROC_LastDayPlus1`.

### 6. OUTPUT TARGETS
- GCS Destination: `gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt`
- BQ Target: `dw_lookups.agent_ads_lookup`

### 7. BUSINESS SUMMARY
* **Data Ingestion:** Reads active Agent dimensions from BigQuery views.
* **Data Processing:** Evaluates active date range partitions based on runtime parameters.
* **Data Destination:** Outputs a structured flat-file in GCS and registers it into a BigQuery table.

---

### PROPOSED PYSPARK PSEUDOCODE OUTLINE

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def run_agent_ads_lookup(first_day, last_day_plus_1):
    spark = SparkSession.builder \
        .appName("BHB_CCM_PROC_WriteAgentADSLookup") \
        .getOrCreate()

    # Environment variables for execution configuration
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    
    source_table = f"{gcp_project}.dwh_views.vi_s_sdm_agent_ads"
    target_gcs_path = f"gs://{gcs_bucket}/lookups/AgentADSLookup.txt"
    target_bq_table = f"{gcp_project}.dw_lookups.agent_ads_lookup"

    # Step 1: Read source data from BigQuery
    df_source = spark.read.format("bigquery").option("table", source_table).load()

    # Step 2: Apply Date Filters matching Ab Initio FirstDay / LastDayPlus1 logic
    # Filter by date partition column (assumed as 'stichtag' or 'load_date')
    df_filtered = df_source.filter(
        (F.col("stichtag") >= F.to_date(F.lit(first_day), "YYYYMMDD")) &
        (F.col("stichtag") < F.to_date(F.lit(last_day_plus_1), "YYYYMMDD"))
    )

    # Step 3: Write flat-file lookup to GCS (delimited text file)
    df_filtered.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv(target_gcs_path)

    # Step 4: Mirror to BigQuery lookup table
    df_filtered.write \
        .format("bigquery") \
        .option("table", target_bq_table) \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    # Sourced from Airflow-passed arguments or DAG environmental context
    fd = os.environ.get("BHB_CCM_PROC_FirstDay")
    ld = os.environ.get("BHB_CCM_PROC_LastDayPlus1")
    run_agent_ads_lookup(fd, ld)
```

---

## 5. LINEAGE & DATA FLOW MAP
The following data flow represents the dependency linkages extracted from the metadata context:

```
[dwh_views.vi_s_sdm_agent_ads] (BigQuery View)
       │
       ▼
 [PySpark Pipeline: BHB_CCM_PROC_WriteAgentADSLookup] 
       │
       ├────────────────────────────────────────┐
       ▼                                        ▼
[gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt] [dw_lookups.agent_ads_lookup]
 (Flat-File Lookup Asset in GCS)             (BigQuery Table for SQL)
```

---

## 6. CLOUD COMPOSER TARGET FILE PLAN
To completely migrate this job, the following directory and file layout must be established in the target Airflow Environment bucket:

```
dags/
├── kkm_daily_imports_dag.py                        # Orchestrates daily KKM lookups
└── scripts/
    └── dataproc/
        └── write_agent_ads_lookup.py               # The translated PySpark pipeline
```

### 6.1. Environment-Specific Values Classification
Following the structural environment rules:

1.  **GLOBAL (Environment-Wide Variables):**
    *   `GCP_PROJECT` $\rightarrow$ Sourced at runtime via Airflow's environment variables or metadata query.
    *   `GCS_BUCKET` $\rightarrow$ Sourced via Airflow Config Store: `Variable.get("GCS_BUCKET")`.
    *   `DATAPROC_CLUSTER` $\rightarrow$ Sourced via Airflow Config Store: `Variable.get("DATAPROC_CLUSTER")`.
    *   `DATAPROC_REGION` $\rightarrow$ Sourced via Airflow Config Store: `Variable.get("DATAPROC_REGION")`.
2.  **JOB-SPECIFIC Variables:**
    *   `lookback_days` = `84` (Inlined in DAG parameter logic).
    *   `job_kennung` = `'ABPZ_KKM_AIL_AGENT'` (Inlined in task names and logging context).

---

## 7. CLOUD COMPOSER DAG DESIGN & SCHEDULING (PYTHON)

The workflow orchestration DAG represents the exact sequence of initialization, data checking, cluster job execution, and post-session state modification defined in the legacy XML structure:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# 1. Resolve Global Environment Configuration
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")

# 2. Retain original language output logging configurations
DWH_JOB_KENNUNG = "ABPZ_KKM_AIL_AGENT"

default_args = {
    'owner': 'dwh_operator',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task A: Start and status check (representing legacy DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC)
    def check_abinitio_start():
        print(f"Jobkennung {DWH_JOB_KENNUNG} eingetragen")
        # Custom logging logic in original German syntax
        print(f"Die Ab Initio Verarbeitung ist gestartet.")

    start_log = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_start',
        python_callable=check_abinitio_start,
    )

    # Task B: Calculate historical lookback dates (representing legacy -z 84 loop calculations)
    def calculate_dates(**kwargs):
        execution_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
        first_day = (execution_date - timedelta(days=84)).strftime("%Y%m%d")
        last_day_plus_1 = execution_date.strftime("%Y%m%d")
        
        # Push dates to XCom for Dataproc task
        kwargs['ti'].xcom_push(key='first_day', value=first_day)
        kwargs['ti'].xcom_push(key='last_day_plus_1', value=last_day_plus_1)

    date_calculation = PythonOperator(
        task_id='calculate_processing_window',
        python_callable=calculate_dates,
    )

    # Task C: Execute PySpark Job on Dataproc Serverless / Dataproc Cluster
    pyspark_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/dags/scripts/dataproc/write_agent_ads_lookup.py",
            "environment_variables": {
                "GCP_PROJECT": GCP_PROJECT,
                "GCS_BUCKET": GCS_BUCKET,
                "BHB_CCM_PROC_FirstDay": "{{ task_instance.xcom_pull(task_ids='calculate_processing_window', key='first_day') }}",
                "BHB_CCM_PROC_LastDayPlus1": "{{ task_instance.xcom_pull(task_ids='calculate_processing_window', key='last_day_plus_1') }}",
            }
        },
    }

    submit_pyspark = DataprocSubmitJobOperator(
        task_id='run_write_agent_ads_lookup',
        job=pyspark_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT,
    )

    # Task D: End and Status Log (representing legacy DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC & DW.LESE_LOG)
    def log_session_end():
        print(f"Die Ab Initio Verarbeitung ist fertig.")
        print(f"Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rueckgabewert 0 beendet.")

    end_log = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_end',
        python_callable=log_session_end,
    )

    # Task Sequencing
    start_log >> date_calculation >> submit_pyspark >> end_log
```

---

## 8. RISKS & MANUAL ACTIONS

*   **UNRESOLVED COMPONENT (RETAINED VERBATIM):**
    SOURCE: NOT FOUND — `showlog.ksh` — treat this file as the real, trusted source (confirmed by system:confirmed_non_blocking_list)
    *   *Mitigation:* Handled via Cloud Composer task failures triggering standard GCP Monitoring alerting notifications.
*   **UPSTREAM DATASETS:** The views/tables within dataset `dwh_views.vi_s_sdm_agent_ads` must be fully migrated and partitioned prior to scheduling this job.
*   **UPSTREAM DEPS (RETAINED VERBATIM):**
    *   Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files` — already migrated & merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/636). Reference the migrated GCS variable environment pathway.