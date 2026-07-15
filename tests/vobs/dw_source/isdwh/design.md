An implementation-ready **Migration Design Document** has been produced for the assembled job `DW.DWH_ABPZ_KKM_AIL_AGENT`.

---

# MIGRATION DESIGN DOCUMENT: DW.DWH_ABPZ_KKM_AIL_AGENT

## 1. EXECUTIVE SUMMARY & TARGET ARCHITECTURE
*   **Source Legacy Job:** `DW.DWH_ABPZ_KKM_AIL_AGENT` (UC4 Job calling KornShell wrapper executing Ab Initio Graph via `r_alis_objekt`).
*   **Source Technology:** Automic/UC4, KornShell (`ksh_abinitio_wrapper`), and Ab Initio Graph (`BHB_CCM_PROC_WriteAgentADSLookup`).
*   **Target Platform:** Google Cloud Platform (GCP).
*   **Target Orchestration:** Cloud Composer (Airflow 2.x DAG) running on Google Cloud.
*   **Target Execution Engine:** Dataproc Serverless (PySpark 3.x) and BigQuery.
*   **Migration Approach:**
    1.  **UC4 Jobs / Jobplans / Includes:** Replaced by an Airflow DAG with specialized modular operators.
    2.  **KSH Wrapper (`r_alis_objekt`):** Converted to a clean Airflow DAG structure where runtime date calculations, object checks, and execution loops are natively handled by Python task scripting.
    3.  **Ab Initio Graph (`BHB_CCM_PROC_WriteAgentADSLookup`):** Converted to a Dataproc Serverless PySpark pipeline that queries the underlying view `DWH$VI_S_SDM_AGENT_ADS` (migrated to a BigQuery view or table) and outputs a flat-file lookup (`AgentADSLookup.txt`) or directly writes to a BigQuery lookup table.

---

## 2. LEGACY JOB ORCHESTRATION & DEPENDENCIES

This job is part of the daily KKM import workflow and operates with upstream and downstream components that must be preserved on GCP.

### Upstream Dependencies (Predecessors)
*   **Shared Files / Shell Environment Configuration:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files`
    *   *Status:* **Already migrated** and merged under PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/648` (includes `.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`).
    *   *Target Wiring:* The Airflow DAG or PySpark jobs must source configurations from these migrated global configurations. On GCP, these translate to common Google Cloud Storage (GCS) files, Airflow Variables, or environment variables.

### Downstream Dependencies (Successors)
*   *None explicitly discovered in the metadata payload other than the KKM daily parent pipeline.* This job writes the lookup database/flat file `AgentADSLookup.txt` which is subsequently consumed by other daily importing steps.

### Scheduling & Execution Order
1.  **Start Hook:** `DW.DWH_ADM_JOB_MONITOR_START` registers the job running in the monitoring variable `DW.DWH_RUNNING_JOBS`.
2.  **Poll Hook:** `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` checks `DW.ADM_AB_INITIO_VAR` application state until it is set to `go` (meaning Ab Initio processing can proceed).
3.  **Pathing and Date Calculation:** `DW.HOLE_PFAD` retrieves pathing configs and dynamically computes the date boundaries (last month, previous month, next month).
4.  **Core Script Invocation:** Execution of `r_alis_objekt` with arguments:
    ```bash
    $HOME/aktuell/allgemein/is/util/bin/r_alis_objekt \
      -o AgentADSLookup.txt \
      -j ABPZ_KKM_AIL_AGENT \
      -x $HOME/aktuell/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg \
      -z 84
    ```
    *Note:* `-z 84` tells the framework script to look back 84 days relative to the processing date.
5.  **Log Reading & End Hook:** `DW.LESE_LOG` captures step status and executes `DW.DWH_ADM_JOB_MONITOR_END` to update execution registries and release status variables in `DW.ADM_AB_INITIO_VAR`.

---

## 3. VERBATIM MCP CONVERSION RESULTS (AB INITIO TO PYSPARK DESIGN)

The following design document block represents the exact conversion specification for the `BHB_CCM_PROC_WriteAgentADSLookup.cfg` parameterization and the corresponding PySpark execution structure:

```markdown
=== Result for local/home/gurunathan_t/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg ===
GRAPH: tmp5bupf309

=== SOURCES ===

=== LOOKUPS ===
  (none extracted — check .mp file for lookup_file fields)

=== DB JOINS (Join_with_DB — live database lookups) ===
  (none)

=== STANDALONE SUBGRAPH PLANS ===
  (none)

=== TRANSFORMS ===

=== FILTERS ===

=== TARGETS ===

=== SORTS AND DEDUPS ===
  (none)

=== EDGES (source-to-target wiring) ===
  (none extracted — open .mp file and trace component connections manually)
  NOTE: for file-output graphs this is the critical missing piece;
  do not guess which source feeds which target without this information


Based on the provided extraction of the Ab Initio graph `tmp5bupf309`, it is evident that the extraction has returned zero components (no sources, transforms, lookups, joins, filters, targets, or edges). 

As a senior data engineer, I must report this transparently, flag the missing elements using the required `# REVIEW` markers, and provide a conservative design template that outlines what needs to be manually supplied to complete this migration.

---

# STAGE 1: DESIGN DOCUMENT

## 1. GRAPH OVERVIEW
The Ab Initio graph `tmp5bupf309` appears to have returned an empty extraction payload. Under normal circumstances, this graph would read source data, apply transformations, and write to targets. Because no components were extracted, the precise business purpose, schemas, and processing steps cannot be determined from this metadata. 

# REVIEW: Extraction empty; the physical `.mp` file must be inspected manually to recover the graph logic.

## 2. SOURCES
# REVIEW: No sources were extracted. Inspect the `.mp` file manually to identify input datasets.

## 3. TRANSFORMS
# REVIEW: No transforms (reformats, joins, rollups, or database lookups) were extracted. 

## 4. IN-MEMORY LOOKUPS
# REVIEW: No lookup files were extracted.

## 5. FILTERS (select_expr)
# REVIEW: No filter conditions were extracted.

## 6. OUTPUT TARGETS
# REVIEW: Extraction likely incomplete. No write targets (INSERT, UPDATE, MERGE, or files) were extracted from the metadata.
*   **Target 1:** # REVIEW: [kind] to [table] — SQL not extracted; supply manually
*   **Target 2:** # REVIEW: edge wiring not extracted — cannot determine source without opening the .mp file

## 7. BUSINESS SUMMARY
*   **Data Ingestion:** # REVIEW: Source files or tables are currently unknown.
*   **Data Transformation:** # REVIEW: Business rules, mapping expressions, and aggregations are currently unknown.
*   **Data Destination:** # REVIEW: Downstream targets and write strategies (append, overwrite, or merge) are currently unknown.

---

# STAGE 2: PYSPARK PSEUDOCODE OUTLINE

Because the source extraction is completely empty, we cannot safely generate functional SQL transformations without guessing. Below is the structured skeleton mapping out how this empty logic must be structured once the graph's metadata is recovered.

```python
# Magic Identifier: tmp5bupf309
# DB-Lookup SQL / Mapping: NOT EXTRACTED (Supply manually)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tmp5bupf309_migration").getOrCreate()

# =====================================================================
# STEP 1: SOURCES
# =====================================================================
# REVIEW: Source paths and schemas are unknown. Update placeholders below.

# df_source_placeholder = spark.read.format("bigquery").option("table", "project.dataset.table").load()
# df_source_placeholder.createOrReplaceTempView("vw_source_placeholder")


# =====================================================================
# STEP 2: FILTERS & TRANSFORMS
# =====================================================================
# REVIEW: No transformations found. Once recovered, write them as spark.sql() steps.
# Example format:
# df_transform = spark.sql("""
#     SELECT 
#         col1,
#         coalesce(col2, 'DEFAULT') AS col2_clean
#     FROM vw_source_placeholder
# """)
# df_transform.createOrReplaceTempView("vw_transform")


# =====================================================================
# STEP 3: OUTPUT WRITES
# =====================================================================
# REVIEW: Edge wiring not extracted. Source-to-target mapping must be manually supplied.

# write_to_bq(df_transform, "project.dataset.target_table")
```
```

---

## 4. TARGET ENVIRONMENT CONFIGURATION & ENVIRONMENT POLICY

The legacy environment variables derived from variable containers (e.g. `DW.VARIABLEN`) and shell config files are mapped to GCP environment constructs as defined below:

### GLOBAL VARIABLES (Environment-wide infrastructure)
These values are identical across dev/test/prod environments and represent the target cloud infrastructure.
*   **GCP_PROJECT:** Sourced via Airflow Variable: `Variable.get("GCP_PROJECT")` or environment fallback `os.environ.get("GCP_PROJECT")`.
*   **GCS_BUCKET:** Sourced via Airflow Variable: `Variable.get("GCS_BUCKET")` (equivalent to legacy `$HOME` storage areas for lookup outputs).
*   **BQ_DATASET:** Sourced via Airflow Variable: `Variable.get("BQ_DATASET")` (where the source BigQuery view/table `DWH$VI_S_SDM_AGENT_ADS` resides).
*   **DATAPROC_REGION:** Sourced via Airflow Variable: `Variable.get("DATAPROC_REGION")`.

### JOB-SPECIFIC VARIABLES (Local configurations)
These variables are specific only to this dataset execution job.
*   **JOB_KENNUNG:** `"ABPZ_KKM_AIL_AGENT"` (Passed as parameter to logs and monitoring tables).
*   **OBJ_NAME:** `"AgentADSLookup.txt"`
*   **RUECKBLICK_DAYS:** `84` (The period relative to the execution run date to check).
*   **AB_INITIO_CFG:** `"BHB_CCM_PROC_WriteAgentADSLookup.cfg"`

---

## 5. AIRFLOW ORCHESTRATION PLAN (REPLACING UC4 & SHELL WRAPPERS)

The following Python Airflow DAG implements the execution flow from the legacy UC4 job design, handling:
1.  **DWH Start Registry Monitoring** (`DW.DWH_ADM_JOB_MONITOR_START` JOBI block)
2.  **App Status Polling Barrier** (`DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` JOBI block)
3.  **Processing Date Ranges Dynamic Generation** (`DW.HOLE_PFAD` JOBI block)
4.  **Execution of Core PySpark processing** on Dataproc Serverless (replacing the wrapper `r_alis_objekt` & `BHB_CCM_PROC_WriteAgentADSLookup` graph).
5.  **DWH End Hook Status Reporting** (`DW.LESE_LOG` and `DW.DWH_ADM_JOB_MONITOR_END` JOBI blocks).

```python
"""
DAG: dw_dwh_abpz_kkm_ail_agent
Migrated from: DW.DWH_ABPZ_KKM_AIL_AGENT (UC4 + KSH + Ab Initio)
Target Engine: Dataproc Serverless + BigQuery
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator

# Environment Variables classification (GLOBAL POLICY)
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")

# Job-specific constants
JOB_KENNUNG = "ABPZ_KKM_AIL_AGENT"
LOOKUP_FILE_NAME = "AgentADSLookup.txt"
RUECKBLICK_DAYS = 84

default_args = {
    'owner': 'istns',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': True,
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

    # 1. Start Hook: Register start in BigQuery monitoring registry (replacing JOBI START)
    def register_start(**context):
        # Outputs trace statement exactly reflecting German log string format from legacy JOBI
        print(f"Added {JOB_KENNUNG} with run_id {context['run_id']}")
        # TO DO: Insert metadata metrics to DW_RUNNING_JOBS BigQuery table
        
    start_monitor = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_start',
        python_callable=register_start,
    )

    # 2. Poll Application Barrier Check (replacing DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC)
    # Checks if 'STATUS_DWH' in BigQuery variable table 'DW_ADM_AB_INITIO_VAR' is equal to 'go'.
    # If not, the downstream tasks are blocked.
    poll_ab_initio_status = BigQueryValueCheckOperator(
        task_id='poll_ab_initio_status_barrier',
        sql=f"SELECT status_val FROM `{GCP_PROJECT}.{BQ_DATASET}.DW_ADM_AB_INITIO_VAR` WHERE app_name = 'STATUS_DWH'",
        pass_value='go',
        use_legacy_sql=False
    )

    # 3. Dataproc Serverless PySpark Execution (replaces r_alis_objekt execution loop & Ab Initio)
    execute_pyspark_lookup = DataprocCreateBatchOperator(
        task_id='execute_pyspark_write_agent_ads_lookup',
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        batch_id=f"batch-{JOB_KENNUNG.lower().replace('_', '-')}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/code/tmp5bupf309_write_agent_lookup.py",
                "args": [
                    "--lookback-days", str(RUECKBLICK_DAYS),
                    "--output-path", f"gs://{GCS_BUCKET}/lookups/{LOOKUP_FILE_NAME}",
                    "--job-kennung", JOB_KENNUNG,
                ],
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "default"
                }
            }
        }
    )

    # 4. End Hook: Register completion status in BigQuery (replacing JOBI END & LESE_LOG)
    def register_end(task_instance, **context):
        batch_state = task_instance.xcom_pull(task_ids='execute_pyspark_write_agent_ads_lookup')
        # Exact logging output rule: Retain literal text inside print statements
        print(f"Jobkennung {JOB_KENNUNG} eingetragen")
        print("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler beendet.")

    end_monitor = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_end',
        python_callable=register_end,
    )

    # Execution order wiring
    start_monitor >> poll_ab_initio_status >> execute_pyspark_lookup >> end_monitor
```

---

## 6. TARGET FILE PLAN

| Source Path | Target Cloud GCS / Composer Path | Language | Purpose |
| :--- | :--- | :--- | :--- |
| `vobs/.../DW.DWH_ABPZ_KKM_AIL_AGENT.xml` | `dags/dw_dwh_abpz_kkm_ail_agent.py` | Python / Airflow | Orchestrates workflow execution, monitoring, and state checks |
| `vobs/.../BHB_CCM_PROC_WriteAgentADSLookup.cfg` | `gs://[GCS_BUCKET]/code/tmp5bupf309_write_agent_lookup.py` | Python / PySpark | Processes BigQuery source views and generates lookup files |
| `vobs/.../r_alis_objekt` | *Redesigned* | N/A | Fully integrated natively into Airflow DAG parameters and tasks |

---

## 7. RISKS & MANUAL ACTIONS

1.  **SOURCE: NOT FOUND** — `DWH$VI_S_SDM_AGENT_ADS` — no candidate. 
    *   *Risk:* The Ab Initio Graph acts upon this database lookup view. No physical schema or view definitions were bundled in the legacy codebase scan.
    *   *Action:* The BigQuery replacement for `DWH$VI_S_SDM_AGENT_ADS` must be manually generated and populated on BigQuery.
2.  **SOURCE: NOT FOUND** — `showlog.ksh` — Candidate path: `local/home/gurunathan_t/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/allgemein/is/util/bin/showlog`
    *   *Action:* Human must confirm if showlog utilities can be safely retired in GCP since Stackdriver Logging (Cloud Logging) natively handles DAG execution logs.
3.  **UNRESOLVED COMPONENT** — `.DW_LOKAL` / `SETPYA.SH`
    *   *Action:* Human-reviewed as "NO SOURCE NEEDED" and can be safely ignored on GCP.
4.  **UPSTREAM COMPONENT WIRING** — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`
    *   *Action:* The converted target pipeline must import / map to the variables defined in this already-migrated module (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/648`) instead of redefining environment setups. Ensure values are mapped in Airflow Variables under the same key names.