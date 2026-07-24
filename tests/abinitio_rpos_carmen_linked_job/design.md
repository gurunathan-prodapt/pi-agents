# Migration Design Document: DW.RPOS_CARM_IMPORT

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Migrated to an Apache Airflow DAG that orchestrates the execution of the converted PySpark job. |

---

## Verbatim MCP Tool Output

```markdown
# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
This migration design document covers the transition of the UC4 `JOBS_UNIX` object `DW.RPOS_CARM_IMPORT` to an Apache Airflow DAG. The original UC4 job's primary responsibility is to execute an Ab Initio graph (`map_rpos_carmen_import`) via the `r_ai_start` launcher utility. This process imports Carmen retail/point-of-sale (RPOS) data into the Data Warehouse. 

Because this object was supplied as an isolated Unix job without its enclosing JobPlan (`JOBP`) or calendar trigger definitions, this design establishes a standalone single-task DAG representing the job execution. This DAG is designed to be triggered externally or integrated into a master pipeline at a later stage.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.RPOS_CARM_IMPORT` | `JOBS_UNIX` | Active (`<Active>1</Active>`) | Job startet AbInitio Graph map_rpos_carmen_import |

---

## 3. Scheduling
* **Calendar/Trigger Analysis:** No `EVNT_TIME` or scheduling definitions were present in this extraction bundle. Additionally, no parent `JOBP` or triggering `SCRI` script was provided to define an execution schedule.
* **Trigger Strategy:** This DAG is classified as **externally triggered (source unknown from this extraction alone)**.
* **DAG Schedule:** `schedule=None` (no cron schedule will be generated to avoid inventing logic).

---

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_rpos_carm_import` |
| **schedule** | `None` |
| **start_date** | `datetime(2026, 4, 21)` (Derived from UC4 export metadata year/month/day) |
| **catchup** | `False` |
| **max_active_runs** | `1` (Enforced default to prevent concurrent write collisions on the imported data) |
| **is_paused_upon_creation** | `False` (Derived from active flag `1`) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `map_rpos_carmen_import` | `DW.RPOS_CARM_IMPORT` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/map_rpos_carmen_import.py` | `project_id`, `region`, `cluster_name` | 1 | 5 mins | N/A | N/A | False | N/A | `#REVIEW-STRUCT:` This task executes a PySpark translation of the original Ab Initio graph launched by `r_ai_start`. |

---

## 6. Task Dependency Map
Since this bundle contains only a single Unix job mapped to a single-task DAG:

```python
map_rpos_carmen_import
```

---

## 7. Sync / Concurrency Analysis
* **UC4 Sync Rows:** No sync entries (`<Syncs/>`) are defined in the source XML.
* **Airflow Concurrency Mapping:** 
  * Standard `max_active_runs=1` is applied to the DAG to prevent concurrent pipeline runs from writing to the same targets simultaneously.

---

## 8. Error Handling and Retry Strategy
* **Task Retries:** Built-in task retry is configured to 1 attempt with a 5-minute delay (as per the default args policy).
* **Execution Failure:** No native custom postcondition action scripts or standard block actions were found in the XML. Thus, standard Airflow failure behavior applies (task state set to `failed` and propagation downstream stops).
* **Execution Timing Constraints:**
  * No `earliest_start_time` is configured.
  * No `calendar_on` rule is configured.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'RPOS_CARM_IMPORT'` | Managed internally within the execution environment or passed as a job argument if required by the target script. |
| N/A | Target GCS Bucket | `gcp_bucket_name` (Airflow Variable) |
| N/A | Target GCP Project | `gcp_project_id` (Airflow Variable) |
| N/A | Target Region | `gcp_region` (Airflow Variable) |
| N/A | Target Dataproc Cluster | `dataproc_cluster_name` (Airflow Variable) |

---

## 10. Developer Notes
* **#REVIEW-STRUCT: Unresolved Parent/Triggering Container:** The `DW.RPOS_CARM_IMPORT` Unix job was supplied as an independent object. It has been wrapped in its own DAG `dw_rpos_carm_import`. If this job is later identified as a task within a larger JobPlan (`JOBP`), this DAG should be converted into a `TriggerDagRunOperator` or task block within that parent pipeline's DAG.
* **#REVIEW-STRUCT: Ab Initio Graph Conversion:** The original system executed an Ab Initio graph using `/abinitio/bin/r_ai_start -j RPOS_CARM_IMPORT -k .../map_rpos_carmen_import.cfg`. This design maps the workload to a Google Cloud Dataproc PySpark execution. The development team must ensure that the Ab Initio graph logic is fully translated into Python/PySpark and uploaded to the GCS path: `gs://{gcp_bucket_name}/pyspark_scripts/map_rpos_carmen_import.py`.
* **GCP Credentials/Configuration:** The target Dataproc cluster must have access to the source files previously processed by the UC4 Unix host.

---

# Pseudocode

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# Fetching environment variables configured in Airflow
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="YOUR_PROJECT_ID")
GCP_REGION = Variable.get("gcp_region", default_var="YOUR_REGION")
GCP_BUCKET_NAME = Variable.get("gcp_bucket_name", default_var="YOUR_BUCKET_NAME")
DATAPROC_CLUSTER_NAME = Variable.get("dataproc_cluster_name", default_var="YOUR_CLUSTER_NAME")

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=DEFAULT_ARGS,
    description='Standalone run of RPOS Carmen Import, migrated from UC4 JOBS_UNIX DW.RPOS_CARM_IMPORT',
    schedule_interval=None,  # Externally triggered/No schedule in extraction
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=['dwh', 'abinitio_migration', 'rpos'],
) as dag:

    # ── Task: map_rpos_carmen_import ─────────────────────────
    # #REVIEW-STRUCT: Ab Initio graph map_rpos_carmen_import migrated to PySpark execution on Dataproc
    pyspark_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCP_BUCKET_NAME}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT"
            ]
        },
    }

    map_rpos_carmen_import = DataprocSubmitJobOperator(
        task_id='map_rpos_carmen_import',
        job=pyspark_job,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone task, no upstream or downstream dependencies defined within this extraction.
    map_rpos_carmen_import
```
```

---

## Target File Plan
- **Target File Path:** `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` (mirrors source relative path according to Folder Integrity Rule)
- **Language:** `Python (Apache Airflow DAG)`
- **Derived from Source File:** `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`

---

## Context and Additional Details

### 1. Job Dependencies & Cross-File Lineage
* **Upstream Dependencies:**
  * **Shared Module:** `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` — already migrated to BigQuery/Cloud Composer-compatible Python standard and merged (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/764`). The DAG execution utilizes standard Airflow operators and does not directly execute legacy shell wrappers.
* **Human-Confirmed Resolutions:**
  The following legacy scripts and includes have been reviewed by a human expert and confirmed as **not needed** on the target platform (no replacement is required):
  * `.CCR_INIT`
  * `.DW_INIT`
  * `AB_CATALOG_FUNCTIONS.KSH`
  * `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`
  * `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`
  * `DW.HOLE_PFAD`
  * `DW.LESE_LOG`
  * `H_ALIS_DATE.KSH`
  * `H_ALIS_DATENOBJEKT.KSH`
  * `H_ALIS_MELDUNGEN.KSH`
  * `H_ALIS_PARAMETER.KSH`

### 2. Execution Order Preservation
The legacy system executes dependencies in the following sequence. This order must be preserved and mapped to the target as specified:
1. `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
   * **Target Mapping:** Orchestrated by the newly created Airflow DAG: `dw_rpos_carm_import`.
2. `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg`
   * **Target Mapping:** Environment variables and static configurations are compiled into Cloud Composer/Airflow DAG `default_args` or `params`.
3. `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`
   * **Target Mapping:** Superseded by direct execution in Airflow via the `DataprocSubmitJobOperator`.
4. `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
   * **Target Mapping:** Refactored into a Dataproc PySpark pipeline (`map_rpos_carmen_import.py`) handled in a separate design pass.

### 3. Scheduling & Orchestration
* **Legacy Triggering:** Executed as part of the `CLIENT_QUEUE` under UC4 login profile `DW.UNIX.ISTNS`.
* **Target Scheduling:** Airflow schedule is configured to `None` (triggered externally or integrated into a parent orchestrator DAG).

---

## Environment-Specific Values (Classification)

### 1. GLOBAL (Environment-Wide Infrastructure Constants)
The following parameters identify target cloud infrastructure resources and must be fetched at runtime via standard Airflow Variables (`Variable.get`) or environment variables, **never** hardcoded as strings:
* **`GCP_PROJECT`** — Google Cloud Platform Project ID.
* **`GCP_REGION`** — GCP Region (e.g., `us-central1`).
* **`GCS_BUCKET`** — Cloud Storage Bucket where PySpark artifacts and schema definitions reside.
* **`DATAPROC_CLUSTER`** — Dataproc cluster name running serverless/standard PySpark.

### 2. JOB-SPECIFIC (Parameters Unique to This Run)
The following constants are strictly specific to this job's context and logic, and should be populated inline or via local configs:
* **`DWH_JOB_KENNUNG`** — Literal value `'RPOS_CARM_IMPORT'`.
* **`CFG_FILE`** — `'map_rpos_carmen_import.cfg'`.

---

## Risks and Manual Steps
* **Verification of PySpark Script:** The target DAG depends on the migrated PySpark code (`map_rpos_carmen_import.py`) being loaded into Google Cloud Storage at the path specified in `main_python_file_uri`. This must be verified prior to testing the Airflow DAG.
* **Folder Integrity Adherence:** The generated DAG script MUST be committed to `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` in the target code repository to match the source folder structure. Do not fold this task into any sibling DAG located in a different source subdirectory.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT (Ab Initio Graph)

This migration design document covers the conversion of the Ab Initio graph `map_rpos_carmen_import.mp` into a PySpark pipeline designed to run on Dataproc Serverless, orchestrated via Cloud Composer (Airflow).

---

## 1. DESIGN SCOPE & FILE DISPOSITION TABLE

In accordance with the architectural boundaries defined for this design pass, **only** the source files explicitly assigned to this group are covered under the File Disposition Table and Target File Plan.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Migrating the Ab Initio graph mapping and processing logic to PySpark on Dataproc Serverless. |

---

## 2. CROSS-FILE / CROSS-JOB DEPENDENCIES & LINEAGE

This job processes billing and invoicing transactions, joining them against contract histories. The execution, sequencing, and shared module relationships must be maintained as follows on Google Cloud Platform:

### 2.1. Shared Files & Common Libraries
* **Shared Upstream Library:** `abinitio_pyspark_linked_job/isccr/abinitio/bin` has already been migrated and merged (PR #764). It contains the initialization sequence script `r_ai_start`.
* **Target Mapping:** The converted PySpark script should import or reference the corresponding initialization utilities or setup functions from this migrated utility block at runtime.

### 2.2. Execution Sequencing (Orchestration)
The legacy scheduling dependencies run in a 4-step execution order. To preserve this sequence in the Cloud Composer (Airflow) DAG, the tasks must be scheduled as follows:
1. **Trigger / Initialization:** Cloud Composer parses external inputs, mapping configuration variables from the legacy XML/CFG states.
2. **Configuration Sourcing:** Parameters from the configuration block (`map_rpos_carmen_import.cfg`) are fed as dynamic runtime variables to the PySpark operator.
3. **Execution Task:** The Dataproc Serverless Spark Submit Operator executes the compiled target file `map_rpos_carmen_import.py`.

### 2.3. Lineage Edges
* **Upstream Producers:** Input flat files arrive via an external folder pattern (represented in legacy as `$DW_DIR_IMP_SAP/crs/work/CARMEN_B_*_pos.fix`).
* **Downstream Consumers:** Target database loads affect multiple core DWH target tables on BigQuery, including:
  - `DWH$TA_F_RPOS_CARM`
  - `DWH$TA_F_RPOS_FACT_CARM`
  - `DWH$TA_F_RPOS_RESELLING_CARM`
  - `DWH$TA_F_GPOS_FACT_CARM`
  - `DWH$TA_T_RPOS_CARM`
  - Operational Logging logs to `DWH$TA_K_MELDUNGEN` and `DWH$TA_K_RECH_ABSGRP`.

---

## 3. ENVIRONMENT-SPECIFIC VALUES & POLICIES

To prevent hardcoded environmental references, all environment variables must be classified and sourced dynamically at runtime based on their respective roles.

### 3.1. Global Environment-Wide Values
These values identify target platform infrastructure and remain consistent across all jobs in a given environment (dev/test/prod).

| Source Legacy Ref | Canonical GCP Parameter | PySpark / Composer Retrieval Method |
| :--- | :--- | :--- |
| `DB_TNS_NAME_DWH` | `GCP_PROJECT` / `BQ_DATASET` | `os.environ.get("GCP_PROJECT")` / `Variable.get("BQ_DATASET")` |
| `DW_DIR_IMP_SAP` | `GCS_BUCKET` | `os.environ.get("GCS_BUCKET")` (e.g. `gs://[environment]-dwh-import-sap`) |
| `HOME` | `HOME` | `os.environ.get("HOME")` |

### 3.2. Job-Specific Values
These parameters are unique to this specific processing module and are extracted directly from the legacy parameter set:

| Parameter Key | Extracted Legacy Value | Implementation Sourcing Method |
| :--- | :--- | :--- |
| `BHB_Projektverzeichnis` | `/Projects/TMD/processing/BHB/BD_PROC` | Job-specific config block parameter |
| `BHB_Version` | `RLS_BHB_nach_64_rabatt_sap` | Job-specific config block parameter |
| `BHB_Graph` | `map_rpos_carmen_import` | Job-specific config block parameter |
| `BHB_Prozesstyp` | `D` | Job-specific config block parameter |
| `BHB_Dateimaske` | `CARMEN_B_*_pos.fix` | Job-specific config block parameter |
| `BHB_Kopfdatensatzkennung` | `H` | Job-specific config block parameter |
| `BHB_Nutzdatensatzkennung` | `P` | Job-specific config block parameter |
| `BHB_Endedatensatzkennung` | `X` | Job-specific config block parameter |

---

## 4. VERBATIM AB INITIO GRAPH CONVERSION DESIGN

Below is the complete, unmodified structural extraction and target PySpark architecture generated for `map_rpos_carmen_import.mp`.

=== VERBATIM MCP TOOL OUTPUT START ===
GRAPH: tmp47qvv8ul

=== SOURCES ===
[DWH$TA_F_RPOS_CARM] kind=select
  select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM
[DWH$TA_F_RPOS_CARM-2] kind=select
  select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM
[DWH$TA_F_RPOS_FACT_CARM] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM
[DWH$TA_F_RPOS_FACT_CARM - 2] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM
[DWH$TA_F_RPOS_RESELLING_CARM] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM
[DWH$TA_F_RPOS_RESELLING_CARM-1] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
[dwh$ta_c_vertrag] kind=select
  select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('20050401', 'YYYYMMDD') 
and ABLOCAL(dwh$ta_c_vertrag)

=== LOOKUPS ===
  (none extracted — check .mp file for lookup_file fields)

=== TRANSFORMS ===
[Reformat rechnung_datum to datetime for Delete] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Validate Records] type=reformat
  out::reformat(in) =
begin
  out.monats_id :: if(!is_valid(in.monats_id))
force_error("Invalid data format in monats_id")
else
in.monats_id;
  out.rechnung_datum :: if(!is_valid(in.rechnung_datum))
force_error("Invalid data format in rechnung_datum")
else
in.rechnung_datum;
  out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id))
force_error("Invalid data format in standardvertrags_id")
else
in.standardvertrags_id;
  out.vertrags_id :: if(!is_valid(in.vertrags_id))
force_error("Invalid data format in vertrags_id")
else
in.vertrags_id;
  out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur))
force_error("Invalid data format in rechpos_brutto_eur")
else
in.rechpos_brutto_eur;
  out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur))
force_error("Invalid data format in rechpos_netto_eur")
else
in.rechpos_netto_eur;
  out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur))
force_error("Invalid data format in rechpos_mwst_eur")
else
in.rechpos_mwst_eur;
  out.* :: in.*;
end;
[replace ',' by '.'] type=reformat
  out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
end;
[Reformat Referencerecord] type=reformat
  out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: in.datensatz_rest;
end;
[Reformat for delete] type=reformat
  out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Reformat for delete] type=reformat
  out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Reformat] type=reformat
  out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Filter out where rpos_geschaeftsform_kenn != 'S'] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Filter out where rankindex != 1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Proof Join - criterias gueltig_von and gueltig_bis] type=reformat
  out::reformat(in) =
begin
  let date("YYYYMMDD") month_last_day =(date('YYYYMMDD'))datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) 
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.zv_id :: if(valid_flag == 0)
in.zv_id;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
end;
[Reformat for insert "fact data"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;
[Reformat for insert "temporary data"] type=reformat
  out::reformat(in) =
begin
  let datetime("YYYYMMDDHH24MISS") mindate =(datetime('YYYYMMDDHH24MISS'))(string(14))'19000101000000';

  out.* :: in.*;
  out.bearbeitung_datum :: mindate;
end;
[Proof Join-criteriase gueltig_von and gueltig_bis] type=reformat
  out::reformat(in) =
begin
  let date("YYYYMMDD") month_last_day =(date('YYYYMMDD')) datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von)
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
end;
[Filter out where rankindex != 1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Reformat for insert "Factoring Gutschriften"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rahmenvertrag :: in.rahmenvertrag_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Reformat for insert "Factoring Rechnungen"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;
[Reformat for insert "Reselling"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;
[Reformat Enderecord for Processing] type=reformat
  out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.bemerkung :: in.bemerkung;
  out.stichtag :: in.stichtag;
  out.anzahl :: in.anzahl;
  out.inhalt :: in.inhalt;
  out.erstellt_am :: (string_index(in.erstellt_am, ";") == 0) ? in.erstellt_am : string_substring(in.erstellt_am, 1, string_length(in.erstellt_am)-1);
end;
[Reformat for DB and Filter out where Kompl_Kennzeichen != L] type=reformat
  out::reformat(in) =
begin
  out.monats_id :: (string(6))(date("YYYYMM"))date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1);
  out.abs_grp :: string_substring(in.bemerkung,10,5) ;
  out.dateiname :: in.bemerkung;
  out.rechnung_datum :: (date("YYYYMMDD")) in.stichtag;
  out.rechnungsteil :: (string(1))"P";
  out.ladedatum :: now();
end;
[Reformat fï¿½r testzwecke] type=reformat
  out::reformat(in) =
begin
  out.vertrags_id :: in.vertrags_id;
  out.monats_id :: in.monats_id;
end;

=== FILTERS ===
[Filter by Expression]
  rech_leistung_id_carm == "RABATT"
[Split Data]
  kennzeichen == "$\{BHB_Nutzdatensatzkennung\}"
[Filter by Expression]
  delete_flag == 1
[Select "Positionen auf Debitorenebene" (temporary Data)]
  typ == 'T'
[Select "Factoring Gutschriften"]
  rpos_geschaftsform_kenn == 'G'
[Select "Factoring Rechnungen"]
  rpos_geschaftsform_kenn == 'F'
[Select "Reselling"]
  rpos_geschaftsform_kenn == 'R'
[Split Metadata]
  kennzeichen == "$\{BHB_Endedatensatzkennung\}"

=== TARGETS ===
[DWH$TA_F_RPOS_FACT_CARM] kind=insert table_or_path=dwh_ta_f_rpos_fact_carm
[DWH$TA_T_RPOS_CARM] kind=insert table_or_path=dwh_ta_t_rpos_carm
[DWH$TA_F_RPOS_CARM] kind=insert table_or_path=dwh_ta_f_rpos_carm
[DWH$TA_F_GPOS_FACT_CARM] kind=insert table_or_path=dwh_ta_f_gpos_fact_carm
[DWH$TA_F_RPOS_RESELLING_CARM] kind=insert table_or_path=dwh_ta_f_rpos_reselling_carm
[Update DWH$TA_K_MELDUNGEN] kind=update table_or_path=dwh$ta_k_meldungen
  update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
[Update / Insert DWH$TA_K_RECH_ABSGRP] kind=update table_or_path=DWH$TA_K_RECH_ABSGRP
  UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil

=== EDGES (source-to-target wiring) ===
  node_1576 --> Scan - Mark valid historized datasets
  Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending; --> Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc
  Reformat Data --> Split Data
  node_374 --> Sort within Groups - Sort over rech_leistung_id_carm
  node_928 --> Determine rows to be deleted
  node_1675 --> node_1620
  Replicate --> Scan - Mark valid historized datasets
  node_542 --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Proof Join - criterias gueltig_von and gueltig_bis --> node_1576
  node_1740 --> Replicate
  node_382 --> node_512
  Read File --> Reformat Data
  node_25 --> node_1740
  Sort by rechnung_id; rechnung_datum; debitor_id-2 --> Determine rows to be deleted (incl. dedup of port 1)
  node_240 --> node_338
  Reformat for insert "Reselling" --> node_1459
  Sort by rechnung_id; rechnung_datum; debitor_id-1 --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1
  node_686 --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Sort by rechnung_id; rechnung_datum; debitor_id-3 --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-2
  node_207 --> Replicate
  Select "Positionen auf Debitorenebene" (temporary Data) --> Reformat for insert "temporary data"
  Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Redefine csv-file format --> Reformat for DB
  Sort by rechnung_id; rechnung_datum; debitor_id --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id
  Determine rows to be deleted --> Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Join with dwh$ta_c_vertrag-1 --> Sort by vertrag_id_carmen
  Sort --> Dedup Sorted
  Select "Factoring Gutschriften" --> Select "Reselling"
  Replicate --> Reformat rechnung_datum to datetime for Delete
  Split Data --> Reformat Referencerecord
  node_426 --> Delete rows from DWH$TA_F_RPOS_CARM-1
  Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending; --> node_1426
  Merge --> Join with dwh$ta_c_vertrag-1
  Scan - Mark valid historized datasets --> Filter out invalid data
  Replicate --> node_928
  node_1584 --> node_1324
  Reformat fï¿½r testzwecke --> node_1750
  node_512 --> node_456
  Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Determine rows to be deleted
  Reformat for insert "Factoring Rechnungen" --> node_1053
  node_1426 --> Decode rpos_geschaeftsform_kenn
  node_1746 --> node_25
  node_232 --> Sort within Groups - Sort over rech_leistung_id_carm
  Join with DB, Determine rows to be deleted --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending; --> Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur
  node_338 --> node_284
  Select "Factoring Rechnungen" --> Select "Factoring Gutschriften"
  node_732 --> Sort within Groups - Sort over rech_leistung_id_carm
  node_1097 --> Sort by vertrag_id_carmen
  Reformat --> Delete rows from DWH$TA_F_RPOS_RESELLING_CARM
  Sort by vertrag_id_carmen --> Merge
  Reformat Enderecord for Update --> Update DWH$TA_K_MELDUNGEN
  Decode rpos_geschaeftsform_kenn --> node_1438
  node_794 --> node_732
  Reformat rechnung_datum to datetime for Delete --> node_200
  node_1675 --> node_1746
  node_550 --> node_650
  node_650 --> node_588
  node_1620 --> Reformat for DB and Filter out where Kompl_Kennzeichen != L
  Filter out where rankindex != 1 --> Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending;
  Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1 --> Determine rows to be deleted (incl. dedup of port 1)
  node_284 --> Sort within Groups - Sort over rech_leistung_id_carm
  Select "Positionen auf Debitorenebene" (temporary Data) --> Reformat for insert "fact data"
  Filter out where rpos_geschaeftsform_kenn != 'S' --> node_1584
  Filter out where rankindex != 1 --> Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending;
  Read Filename --> Read File
  Replicate --> Filter out where rpos_geschaeftsform_kenn != 'S'
  Join with DB, Determine rows to be deleted --> Sort by rechnung_id; rechnung_datum; debitor_id-3
  Dedup Sorted --> node_1566
  Sort within Groups - Sort over rech_leistung_id_carm --> Determine rows to be deleted
  Proof Join-criteriase gueltig_von and gueltig_bis --> Replicate
  Reformat --> Delete rows from DWH$TA_T_RPOS_CARM
  Scan - Ranking over gueltig_von, dwh_vertrag_id desc --> Filter out where rankindex != 1
  Split Data --> Split Metadata
  Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending; --> Scan - Ranking over gueltig_von, dwh_vertrag_id desc
  Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur --> Select "Positionen auf Debitorenebene" (temporary Data)
  Reformat for insert "temporary data" --> node_1086
  node_298 --> Determine rows to be deleted
  Sort within Groups - Sort over rech_leistung_id_carm --> node_298
  Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc --> Filter out where rankindex != 1
  Select "Factoring Rechnungen" --> Reformat for insert "Factoring Rechnungen"
  Reformat for insert "fact data" --> node_1162
  node_842 --> Replicate
  Sort by vertrag_id_carmen --> Replicate
  Replicate --> node_357
  Reformat for DB --> Validate Records
  Select "Reselling" --> Reformat for insert "Reselling"
  Reformat for insert "Factoring Gutschriften" --> node_1449
  Format Enderecord --> Replicate Enderecord
  replace ',' by '.' --> Redefine csv-file format
  node_192 --> Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1
  Dedup Sorted --> Sort-1
  node_694 --> node_794
  Join with DB --> Filter by Expression
  Filter by Expression --> node_192
  Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Delete rows from DWH$TA_F_RPOS_CARM
  Reformat for DB and Filter out where Kompl_Kennzeichen != L --> Update / Insert DWH$TA_K_RECH_ABSGRP
  Determine rows to be deleted --> Sort by rechnung_id; rechnung_datum; debitor_id
  node_456 --> Sort within Groups - Sort over rech_leistung_id_carm
  Filter by Expression --> node_426
  Reformat for delete --> Delete rows from DWH$TA_F_RPOS_FACT_CARM
  Join CSV-File with dwh$TA_C_VERTRAG --> Process Enderecord
  Replicate Enderecord --> Reformat Enderecord for Processing
  node_834 --> Sort by rechnung_id; rechnung_datum; debitor_id-1
  node_588 --> Sort within Groups - Sort over rech_leistung_id_carm
  Reformat for delete --> Delete rows from DWH$TA_F_GPOS_FACT_CARM
  node_1573 --> node_1149
  Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1 --> Gather
  Select "Factoring Gutschriften" --> Reformat for insert "Factoring Gutschriften"


# DETAILED DESIGN DOCUMENT

## 1. GRAPH OVERVIEW
The graph `tmp47qvv8ul` processes incoming billing and invoice transaction data from raw CSV files. It parses and validates the payload records, cleanses data fields (such as decimal separator conversions), and joins the records against contract historical records loaded from `dwh$ta_c_vertrag`. Validated matches are filtered using ranking to ensure only active, relevant contract records are utilized, after which they are partitioned into various target outputs (temporary tables, reselling, factoring invoices, and factoring credit notes) using a Delete-and-Insert transactional reload pattern. Finally, metadata trailer records are compiled to update processing status and monitoring logs.

---

## 2. SOURCES

### DWH$TA_F_RPOS_CARM [table / select]
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM
```

### DWH$TA_F_RPOS_CARM-2 [table / select]
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM
```

### DWH$TA_F_RPOS_FACT_CARM [table / select]
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM
```

### DWH$TA_F_RPOS_FACT_CARM - 2 [table / select]
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM
```

### DWH$TA_F_RPOS_RESELLING_CARM [table / select]
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM
```

### DWH$TA_F_RPOS_RESELLING_CARM-1 [table / select]
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
```

### dwh$ta_c_vertrag [table / select]
```sql
select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('20050401', 'YYYYMMDD') 
and ABLOCAL(dwh$ta_c_vertrag)
```

### Read File [file]
- Reads the raw transaction CSV file containing payload lines (Nutzdaten) and processing trailer blocks.

---

## 3. TRANSFORMS

### [Validate Records] (type=reformat)
```dml
  out.monats_id :: if(!is_valid(in.monats_id))
force_error("Invalid data format in monats_id")
else
in.monats_id;
  out.rechnung_datum :: if(!is_valid(in.rechnung_datum))
force_error("Invalid data format in rechnung_datum")
else
in.rechnung_datum;
  out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id))
force_error("Invalid data format in standardvertrags_id")
else
in.standardvertrags_id;
  out.vertrags_id :: if(!is_valid(in.vertrags_id))
force_error("Invalid data format in vertrags_id")
else
in.vertrags_id;
  out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur))
force_error("Invalid data format in rechpos_brutto_eur")
else
in.rechpos_brutto_eur;
  out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur))
force_error("Invalid data format in rechpos_netto_eur")
else
in.rechpos_netto_eur;
  out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur))
force_error("Invalid data format in rechpos_mwst_eur")
else
in.rechpos_mwst_eur;
  out.* :: in.*;
```
*Ensures that essential fields contain valid values, raising a terminal run exception if validation fails.*

### [replace ',' by '.'] (type=reformat)
```dml
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
```
*Normalizes decimal data in raw strings by converting standard European comma separators into periods.*

### [Reformat Referencerecord] (type=reformat)
```dml
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: in.datensatz_rest;
```
*Passes raw CSV split payload lines to structural redefinition blocks.*

### [Reformat for delete] (type=reformat)
```dml
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
```
*Extracts key logical fields from active streams to target records for key-based deletions.*

### [Proof Join - criterias gueltig_von and gueltig_bis] (type=reformat)
```dml
  let date("YYYYMMDD") month_last_day =(date('YYYYMMDD'))datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) 
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.zv_id :: if(valid_flag == 0)
in.zv_id;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
```
*Validates the mapping of master contract rows by ensuring their validity boundaries correspond with the processed month ending; resets matched attributes to null if validation fails.*

### [Proof Join-criteriase gueltig_von and gueltig_bis] (type=reformat)
```dml
  let date("YYYYMMDD") month_last_day =(date('YYYYMMDD')) datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von)
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
```
*Performs temporal boundaries verification for matched contracts (alternative interface without the `zv_id` mapping).*

### [Reformat for insert "fact data"] (type=reformat)
```dml
  out.* :: in.*;
  out.rahmenvertrag :: in.rahmenvertrag_id;
```
*Maps verified contract framework attributes for base targets insert processing.*

### [Reformat for insert "temporary data"] (type=reformat)
```dml
  let datetime("YYYYMMDDHH24MISS") mindate =(datetime('YYYYMMDDHH24MISS'))(string(14))'19000101000000';

  out.* :: in.*;
  out.bearbeitung_datum :: mindate;
```
*Appends the baseline low-value timestamp to temporary business transactions.*

### [Reformat for insert "Factoring Gutschriften"] (type=reformat)
```dml
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rahmenvertrag :: in.rahmenvertrag_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
```
*Trims performance service key identifiers to 9 characters and populates factoring credit master contract variables.*

### [Reformat for insert "Factoring Rechnungen"] (type=reformat)
```dml
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
```
*Maps invoice accounting details for Factoring target tables.*

### [Reformat for insert "Reselling"] (type=reformat)
```dml
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
```
*Structures processed variables to populate down-market reselling databases.*

### [Reformat Enderecord for Processing] (type=reformat)
```dml
  out.kennzeichen :: in.kennzeichen;
  out.bemerkung :: in.bemerkung;
  out.stichtag :: in.stichtag;
  out.anzahl :: in.anzahl;
  out.inhalt :: in.inhalt;
  out.erstellt_am :: (string_index(in.erstellt_am, ";") == 0) ? in.erstellt_am : string_substring(in.erstellt_am, 1, string_length(in.erstellt_am)-1);
```
*Cleans up control metadata in trailer blocks by stripping out optional trailing separator delimiters.*

### [Reformat for DB and Filter out where Kompl_Kennzeichen != L] (type=reformat)
```dml
  out.monats_id :: (string(6))(date("YYYYMM"))date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1);
  out.abs_grp :: string_substring(in.bemerkung,10,5) ;
  out.dateiname :: in.bemerkung;
  out.rechnung_datum :: (date("YYYYMMDD")) in.stichtag;
  out.rechnungsteil :: (string(1))"P";
  out.ladedatum :: now();
```
*Prepares final job run control record states (calculates past accounting periods, structures file paths, and sets load datestamps).*

---

## 4. IN-MEMORY LOOKUPS
*(No static in-memory lookup files were extracted from the structural model graph definition.)*

---

## 5. FILTERS (select_expr)

### Filter by Expression
```dml
rech_leistung_id_carm == "RABATT"
```
*Processes only transaction rows flagged specifically as rebate adjustments.*

### Split Data
```dml
kennzeichen == "${BHB_Nutzdatensatzkennung}"
```
*Isolates raw active data payloads from job execution boundaries.*

### Filter by Expression
```dml
delete_flag == 1
```
*Identifies rows targeted for removal from target tables.*

### Select "Positionen auf Debitorenebene" (temporary Data)
```dml
typ == 'T'
```
*Separates transient customer-level records from central invoice streams.*

### Select "Factoring Gutschriften"
```dml
rpos_geschaeftsform_kenn == 'G'
```
*Routes data rows containing code 'G' to the Factoring Credit Memo pipeline.*

### Select "Factoring Rechnungen"
```dml
rpos_geschaeftsform_kenn == 'F'
```
*Routes data rows containing code 'F' to the Factoring Invoice pipeline.*

### Select "Reselling"
```dml
rpos_geschaeftsform_kenn == 'R'
```
*Routes data rows containing code 'R' to the Reselling pipeline.*

### Split Metadata
```dml
kennzeichen == "${BHB_Endedatensatzkennung}"
```
*Isolates file summary and validation trailers from the invoice streams.*

---

## 6. OUTPUT TARGETS

This graph uses a **DELETE-before-INSERT** pattern. In order to avoid incremental duplicate appends, current stream business keys determine the records to be purged from each respective database target prior to inserting the newly computed dataset.

### PAIRED TARGET 1: [DWH$TA_F_RPOS_CARM]
- **Kind:** Delete + Insert
- **Table:** `dwh_ta_f_rpos_carm`
- **Execution Order:** DELETE must execute before INSERT
- **Purge/Identify Key Sets:** Match on `rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`, `rech_leistung_id_carm`.

### PAIRED TARGET 2: [DWH$TA_F_RPOS_FACT_CARM]
- **Kind:** Delete + Insert
- **Table:** `dwh_ta_f_rpos_fact_carm`
- **Execution Order:** DELETE must execute before INSERT
- **Purge/Identify Key Sets:** Match on `rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`, `rech_leistung_id_carm`.

### PAIRED TARGET 3: [DWH$TA_F_RPOS_RESELLING_CARM]
- **Kind:** Delete + Insert
- **Table:** `dwh_ta_f_rpos_reselling_carm`
- **Execution Order:** DELETE must execute before INSERT
- **Purge/Identify Key Sets:** Match on `rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`, `rech_leistung_id_carm`.

### PAIRED TARGET 4: [DWH$TA_T_RPOS_CARM]
- **Kind:** Delete + Insert
- **Table:** `dwh_ta_t_rpos_carm`
- **Execution Order:** DELETE must execute before INSERT
- **Purge/Identify Key Sets:** Match on keys mapped via `Reformat für testzwecke` and `Reformat for delete`.

### TARGET 5: [DWH$TA_F_GPOS_FACT_CARM]
- **Kind:** Delete + Insert
- **Table:** `dwh_ta_f_gpos_fact_carm`
- **Execution Order:** DELETE must execute before INSERT
- **Purge/Identify Key Sets:** Match on keys derived via factoring credit note properties.

### TARGET 6: [Update DWH$TA_K_MELDUNGEN]
- **Kind:** update
- **Table:** `dwh$ta_k_meldungen`
- **Query:**
```sql
update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
```

### TARGET 7: [Update / Insert DWH$TA_K_RECH_ABSGRP]
- **Kind:** update
- **Table:** `DWH$TA_K_RECH_ABSGRP`
- **Query:**
```sql
UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil
```

---

## 7. DB JOINS
*(No live DB Lookup / "Join with DB" parameterized SQL blocks were extracted from the structural model graph definition.)*

---

## 8. BUSINESS SUMMARY
* **Ingestion & Classification:** The graph parses incoming files, isolating payload records (`${BHB_Nutzdatensatzkennung}`) from job execution and control verification footer lines (`${BHB_Endedatensatzkennung}`).
* **Data Cleansing:** The payload data is standardized by normalizing numeric formats (replacing comma decimals with dot notations) and validated to prevent null parameters in primary invoice keys.
* **Master Mapping:** Active payloads are joined with contract structures (`dwh$ta_c_vertrag`). Matches undergo date-boundary validation: if a matched contract's active range does not align with the transaction month's end date, the contract mapping is nullified.
* **Priority Matching:** The matches are sorted by effective start dates and unique IDs to select the most relevant matched record (where `rankindex = 1`).
* **Multi-Channel Distribution:** Verified records are divided into distinct tables based on business type (e.g., temporary storage, Reselling, Factoring Invoices, or Factoring Credit notes).
* **Purge and Reload Execution:** To prevent duplicate entries, matching keys are first deleted from the target tables before inserting the fresh batch of processed invoice records.

---

# PYSPARK PYSPARK SQL PSEUDOCODE OUTLINE

```python
# tmp47qvv8ul - PySpark Ingestion and Processing Pipeline

# Read primary CSV stream source
df_raw_file = spark.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", ";") \
    .load("BIGQUERY_SOURCE_DS.raw_invoice_input_file")
df_raw_file.createOrReplaceTempView("vw_raw_file")

# Step 1: Split active data rows (Nutzdaten)
df_nutzdaten = spark.sql("""
    SELECT 
        _c0 AS kennzeichen,
        _c1 AS monats_id,
        _c2 AS rechnung_datum,
        _c3 AS standardvertrags_id,
        _c4 AS vertrags_id,
        _c5 AS rech_leistung_id_carm,
        _c6 AS rechpos_brutto_eur,
        _c7 AS rechpos_netto_eur,
        _c8 AS rechpos_mwst_eur,
        _c9 AS rpos_geschaeftsform_kenn,
        _c10 AS typ,
        _c11 AS datensatz_rest
    FROM vw_raw_file
    WHERE _c0 = '${BHB_Nutzdatensatzkennung}'
""")
df_nutzdaten.createOrReplaceTempView("vw_nutzdaten")

# Step 2: Split control footer records (Metadaten)
df_metadata = spark.sql("""
    SELECT 
        _c0 AS kennzeichen,
        _c1 AS bemerkung,
        _c2 AS stichtag,
        _c3 AS anzahl,
        _c4 AS inhalt,
        _c5 AS erstellt_am,
        _c6 AS entrynr
    FROM vw_raw_file
    WHERE _c0 = '${BHB_Endedatensatzkennung}'
""")
df_metadata.createOrReplaceTempView("vw_metadata")

# Step 3: Numeric normalization (decimal comma replace)
df_normalized = spark.sql("""
    SELECT 
        kennzeichen,
        monats_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        rpos_geschaeftsform_kenn,
        typ,
        regexp_replace(datensatz_rest, ',', '.') AS datensatz_rest
    FROM vw_nutzdaten
""")
df_normalized.createOrReplaceTempView("vw_normalized")

# Step 4: Strict validation checks
# If any required field is invalid, we flag the run (mimics force_error)
df_validated = spark.sql("""
    SELECT 
        CASE WHEN monats_id IS NULL THEN raise_error("Invalid data format in monats_id") ELSE monats_id END AS monats_id,
        CASE WHEN rechnung_datum IS NULL THEN raise_error("Invalid data format in rechnung_datum") ELSE rechnung_datum END AS rechnung_datum,
        CASE WHEN standardvertrags_id IS NULL THEN raise_error("Invalid data format in standardvertrags_id") ELSE standardvertrags_id END AS standardvertrags_id,
        CASE WHEN vertrags_id IS NULL THEN raise_error("Invalid data format in vertrags_id") ELSE vertrags_id END AS vertrags_id,
        CASE WHEN rechpos_brutto_eur IS NULL THEN raise_error("Invalid data format in rechpos_brutto_eur") ELSE rechpos_brutto_eur END AS rechpos_brutto_eur,
        CASE WHEN rechpos_netto_eur IS NULL THEN raise_error("Invalid data format in rechpos_netto_eur") ELSE rechpos_netto_eur END AS rechpos_netto_eur,
        CASE WHEN rechpos_mwst_eur IS NULL THEN raise_error("Invalid data format in rechpos_mwst_eur") ELSE rechpos_mwst_eur END AS rechpos_mwst_eur,
        rech_leistung_id_carm,
        rpos_geschaeftsform_kenn,
        typ,
        datensatz_rest
    FROM vw_normalized
""")
df_validated.createOrReplaceTempView("vw_validated")

# Step 5: Read master contract database history (dwh$ta_c_vertrag)
df_vertrag = spark.read.format("bigquery").option("table", "BIGQUERY_SOURCE_DS.dwh_ta_c_vertrag").load()
df_vertrag.createOrReplaceTempView("vw_vertrag_src")

df_vertrag_filtered = spark.sql("""
    SELECT 
        rahmenvertrag_id,
        vertrag_id_carmen,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von, 
        gueltig_bis
    FROM vw_vertrag_src
    WHERE gueltig_bis >= CAST('2005-04-01' AS DATE)
""")
df_vertrag_filtered.createOrReplaceTempView("vw_vertrag_filtered")

# Step 6: Map contracts (Left Join)
df_mapped_vertrag = spark.sql("""
    SELECT 
        i.*,
        v.rahmenvertrag_id,
        v.dwh_vertrag_id,
        v.dwh_gp_id,
        v.dwh_konto_id,
        v.dwh_tarifgr_id,
        v.vo_kenn,
        v.zv_id,
        v.gueltig_von,
        v.gueltig_bis
    FROM vw_validated i
    LEFT JOIN vw_vertrag_filtered v ON i.vertrags_id = v.vertrag_id_carmen
""")
df_mapped_vertrag.createOrReplaceTempView("vw_mapped_vertrag")

# Step 7: Proof Join temporal ranges verification
df_proofed = spark.sql("""
    WITH prep_proof AS (
        SELECT *,
            last_day(to_date(concat(monats_id, '01'), 'yyyyMMdd')) AS month_last_day
        FROM vw_mapped_vertrag
    ),
    flagged_proof AS (
        SELECT *,
            CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von)
                      AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
                 THEN 0 ELSE 1 END AS valid_flag
        FROM prep_proof
    )
    SELECT 
        monats_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        rpos_geschaeftsform_kenn,
        typ,
        datensatz_rest,
        CASE WHEN valid_flag = 0 THEN rahmenvertrag_id ELSE NULL END AS rahmenvertrag_id,
        CASE WHEN valid_flag = 0 THEN dwh_vertrag_id ELSE NULL END AS dwh_vertrag_id,
        CASE WHEN valid_flag = 0 THEN dwh_gp_id ELSE NULL END AS dwh_gp_id,
        CASE WHEN valid_flag = 0 THEN dwh_konto_id ELSE NULL END AS dwh_konto_id,
        CASE WHEN valid_flag = 0 THEN dwh_tarifgr_id ELSE NULL END AS dwh_tarifgr_id,
        CASE WHEN valid_flag = 0 THEN vo_kenn ELSE NULL END AS vo_kenn,
        CASE WHEN valid_flag = 0 THEN zv_id ELSE NULL END AS zv_id,
        CASE WHEN valid_flag = 0 THEN gueltig_von ELSE NULL END AS gueltig_von,
        CASE WHEN valid_flag = 0 THEN gueltig_bis ELSE NULL END AS gueltig_bis
    FROM flagged_proof
""")
df_proofed.createOrReplaceTempView("vw_proofed")

# Step 8: Priority Deduplication (Ranking contract matches)
df_ranked = spark.sql("""
    SELECT *,
        row_number() OVER (
            PARTITION BY vertrags_id, monats_id, rechnung_datum, standardvertrags_id
            ORDER BY gueltig_von DESC, dwh_vertrag_id DESC
        ) AS rankindex
    FROM vw_proofed
""")
df_ranked.createOrReplaceTempView("vw_ranked")

df_clean_active_records = spark.sql("""
    SELECT * FROM vw_ranked WHERE rankindex = 1
""")
df_clean_active_records.createOrReplaceTempView("vw_clean_active_records")


# --- ROUTING TARGET 1: Temporary Storage Debitor Accounts ('T') ---

df_temp_records = spark.sql("""
    SELECT *,
        CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS bearbeitung_datum
    FROM vw_clean_active_records
    WHERE typ = 'T'
""")
df_temp_records.createOrReplaceTempView("vw_temp_records")


# --- ROUTING TARGET 2: Factoring Credit Notes ('G') ---

df_factoring_gutschriften = spark.sql("""
    SELECT *,
        substring(rech_leistung_id_carm, 1, 9) AS trimmed_leistung_id,
        rahmenvertrag_id AS rahmenvertrag
    FROM vw_clean_active_records
    WHERE rpos_geschaeftsform_kenn = 'G'
""")
df_factoring_gutschriften.createOrReplaceTempView("vw_factoring_gutschriften")


# --- ROUTING TARGET 3: Factoring Invoices ('F') ---

df_factoring_rechnungen = spark.sql("""
    SELECT *,
        substring(rech_leistung_id_carm, 1, 9) AS trimmed_leistung_id,
        rahmenvertrag_id AS rahmenvertrag
    FROM vw_clean_active_records
    WHERE rpos_geschaeftsform_kenn = 'F'
""")
df_factoring_rechnungen.createOrReplaceTempView("vw_factoring_rechnungen")


# --- ROUTING TARGET 4: Downmarket Reselling Contracts ('R') ---

df_reselling = spark.sql("""
    SELECT *,
        substring(rech_leistung_id_carm, 1, 9) AS trimmed_leistung_id,
        rahmenvertrag_id AS rahmenvertrag
    FROM vw_clean_active_records
    WHERE rpos_geschaeftsform_kenn = 'R'
""")
df_reselling.createOrReplaceTempView("vw_reselling")


# --- ROUTING TARGET 5: Base Invoice Transactions (Remaining Standard Records) ---

df_base_invoice = spark.sql("""
    SELECT *,
        rahmenvertrag_id AS rahmenvertrag
    FROM vw_clean_active_records
    WHERE typ != 'T' AND rpos_geschaeftsform_kenn NOT IN ('G', 'F', 'R')
""")
df_base_invoice.createOrReplaceTempView("vw_base_invoice")


# ==========================================
# TRANSACTIONAL RELOAD: DELETE BEFORE INSERT
# ==========================================

# -- TRANSACTION 1: Target dwh_ta_t_rpos_carm --
df_existing_temp = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_t_rpos_carm").load()
df_existing_temp.createOrReplaceTempView("vw_existing_temp")

df_target_temp_post_delete = spark.sql("""
    SELECT e.* 
    FROM vw_existing_temp e
    LEFT ANTI JOIN vw_temp_records n ON 
        e.vertrags_id = n.vertrags_id AND 
        e.monats_id = n.monats_id
""")
df_target_temp_post_delete.createOrReplaceTempView("vw_target_temp_post_delete")

df_final_temp_write = spark.sql("""
    SELECT * FROM vw_target_temp_post_delete
    UNION ALL
    SELECT 
        vertrags_id,
        monats_id,
        bearbeitung_datum
    FROM vw_temp_records
""")
write_to_bq(df_final_temp_write, "BIGQUERY_TARGET_DS.dwh_ta_t_rpos_carm")


# -- TRANSACTION 2: Target dwh_ta_f_gpos_fact_carm --
df_existing_gpos = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_f_gpos_fact_carm").load()
df_existing_gpos.createOrReplaceTempView("vw_existing_gpos")

df_target_gpos_post_delete = spark.sql("""
    SELECT e.* 
    FROM vw_existing_gpos e
    LEFT ANTI JOIN vw_factoring_gutschriften n ON 
        e.rechnung_id = n.rechnung_id AND
        e.rechnung_datum = n.rechnung_datum AND
        e.standardvertrags_id = n.standardvertrags_id AND
        e.vertrags_id = n.vertrags_id AND
        e.rech_leistung_id_carm = n.rech_leistung_id_carm
""")
df_target_gpos_post_delete.createOrReplaceTempView("vw_target_gpos_post_delete")

df_final_gpos_write = spark.sql("""
    SELECT * FROM vw_target_gpos_post_delete
    UNION ALL
    SELECT 
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        trimmed_leistung_id AS rech_leistung_id_carm,
        rahmenvertrag
    FROM vw_factoring_gutschriften
""")
write_to_bq(df_final_gpos_write, "BIGQUERY_TARGET_DS.dwh_ta_f_gpos_fact_carm")


# -- TRANSACTION 3: Target dwh_ta_f_rpos_fact_carm --
df_existing_rpos_fact = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_f_rpos_fact_carm").load()
df_existing_rpos_fact.createOrReplaceTempView("vw_existing_rpos_fact")

df_target_rpos_fact_post_delete = spark.sql("""
    SELECT e.* 
    FROM vw_existing_rpos_fact e
    LEFT ANTI JOIN vw_factoring_rechnungen n ON 
        e.rechnung_id = n.rechnung_id AND
        e.rechnung_datum = n.rechnung_datum AND
        e.standardvertrags_id = n.standardvertrags_id AND
        e.vertrags_id = n.vertrags_id AND
        e.rech_leistung_id_carm = n.rech_leistung_id_carm
""")
df_target_rpos_fact_post_delete.createOrReplaceTempView("vw_target_rpos_fact_post_delete")

df_final_rpos_fact_write = spark.sql("""
    SELECT * FROM vw_target_rpos_fact_post_delete
    UNION ALL
    SELECT 
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        trimmed_leistung_id AS rech_leistung_id_carm,
        rahmenvertrag
    FROM vw_factoring_rechnungen
""")
write_to_bq(df_final_rpos_fact_write, "BIGQUERY_TARGET_DS.dwh_ta_f_rpos_fact_carm")


# -- TRANSACTION 4: Target dwh_ta_f_rpos_reselling_carm --
df_existing_reselling = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_f_rpos_reselling_carm").load()
df_existing_reselling.createOrReplaceTempView("vw_existing_reselling")

df_target_reselling_post_delete = spark.sql("""
    SELECT e.* 
    FROM vw_existing_reselling e
    LEFT ANTI JOIN vw_reselling n ON 
        e.rechnung_id = n.rechnung_id AND
        e.rechnung_datum = n.rechnung_datum AND
        e.standardvertrags_id = n.standardvertrags_id AND
        e.vertrags_id = n.vertrags_id AND
        e.rech_leistung_id_carm = n.rech_leistung_id_carm
""")
df_target_reselling_post_delete.createOrReplaceTempView("vw_target_reselling_post_delete")

df_final_reselling_write = spark.sql("""
    SELECT * FROM vw_target_reselling_post_delete
    UNION ALL
    SELECT 
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        trimmed_leistung_id AS rech_leistung_id_carm,
        rahmenvertrag
    FROM vw_reselling
""")
write_to_bq(df_final_reselling_write, "BIGQUERY_TARGET_DS.dwh_ta_f_rpos_reselling_carm")


# -- TRANSACTION 5: Target dwh_ta_f_rpos_carm --
df_existing_rpos_carm = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_f_rpos_carm").load()
df_existing_rpos_carm.createOrReplaceTempView("vw_existing_rpos_carm")

df_target_rpos_carm_post_delete = spark.sql("""
    SELECT e.* 
    FROM vw_existing_rpos_carm e
    LEFT ANTI JOIN vw_base_invoice n ON 
        e.rechnung_id = n.rechnung_id AND
        e.rechnung_datum = n.rechnung_datum AND
        e.standardvertrags_id = n.standardvertrags_id AND
        e.vertrags_id = n.vertrags_id AND
        e.rech_leistung_id_carm = n.rech_leistung_id_carm
""")
df_target_rpos_carm_post_delete.createOrReplaceTempView("vw_target_rpos_carm_post_delete")

df_final_rpos_carm_write = spark.sql("""
    SELECT * FROM vw_target_rpos_carm_post_delete
    UNION ALL
    SELECT 
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rahmenvertrag
    FROM vw_base_invoice
""")
write_to_bq(df_final_rpos_carm_write, "BIGQUERY_TARGET_DS.dwh_ta_f_rpos_carm")


# ==========================================
# METADATA & OPERATIONAL LOGGING UPDATES
# ==========================================

# Clean metadata ends (stripping semicolons)
df_metadata_clean = spark.sql("""
    SELECT 
        kennzeichen,
        bemerkung,
        stichtag,
        anzahl,
        inhalt,
        CASE WHEN instr(erstellt_am, ';') = 0 THEN erstellt_am
             ELSE substring(erstellt_am, 1, length(erstellt_am) - 1)
        END AS erstellt_am,
        entrynr
    FROM vw_metadata
""")
df_metadata_clean.createOrReplaceTempView("vw_metadata_clean")

# Reformat trailer row parameters for accounting logging update
df_meta_calculated = spark.sql("""
    SELECT 
        -- monats_id: Subtract 1 month from stichtag
        CAST(add_months(to_date(substring(stichtag, 1, 6), 'yyyyMM'), -1) AS STRING) AS monats_id,
        substring(bemerkung, 10, 5) AS abs_grp,
        bemerkung AS dateiname,
        to_date(stichtag, 'yyyyMMdd') AS rechnung_datum,
        'P' AS rechnungsteil,
        current_timestamp() AS ladedatum,
        anzahl,
        inhalt,
        bemerkung,
        entrynr
    FROM vw_metadata_clean
""")
df_meta_calculated.createOrReplaceTempView("vw_meta_calculated")

# Update 1: Update log table dwh$ta_k_meldungen
df_existing_meldungen = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_k_meldungen").load()
df_existing_meldungen.createOrReplaceTempView("vw_existing_meldungen")

df_updated_meldungen = spark.sql("""
    SELECT 
        e.entrynr,
        coalesce(m.anzahl, e.anzahl_ds_eof) AS anzahl_ds_eof,
        coalesce(m.dateiname, e.dateiname) AS dateiname,
        coalesce(m.inhalt, e.enderecord_text) AS enderecord_text,
        coalesce(m.bemerkung, e.zusatzinfo) AS zusatzinfo
    FROM vw_existing_meldungen e
    LEFT JOIN vw_meta_calculated m ON e.entrynr = m.entrynr
""")
write_to_bq(df_updated_meldungen, "BIGQUERY_TARGET_DS.dwh_ta_k_meldungen")

# Update 2: Update/Insert DWH$TA_K_RECH_ABSGRP run markers
df_existing_rech_absgrp = spark.read.format("bigquery").option("table", "BIGQUERY_TARGET_DS.dwh_ta_k_rech_absgrp").load()
df_existing_rech_absgrp.createOrReplaceTempView("vw_existing_rech_absgrp")

df_updated_rech_absgrp = spark.sql("""
    SELECT 
        e.monats_id,
        e.abs_grp,
        e.dateiname,
        e.rechnungsteil,
        coalesce(m.rechnung_datum, e.rechnung_datum) AS rechnung_datum,
        coalesce(m.ladedatum, e.ladedatum) AS ladedatum
    FROM vw_existing_rech_absgrp e
    LEFT JOIN vw_meta_calculated m ON 
        e.monats_id = m.monats_id AND 
        e.abs_grp = m.abs_grp AND 
        e.dateiname = m.dateiname AND 
        e.rechnungsteil = m.rechnungsteil
""")
write_to_bq(df_updated_rech_absgrp, "BIGQUERY_TARGET_DS.dwh_ta_k_rech_absgrp")
```
=== VERBATIM MCP TOOL OUTPUT END ===

---

## 5. TARGET FILE PLAN

The target repository structure strictly mirrors the layout of the source repository. Only files assigned under scope are planned.

| Target File Path | Target Language | Legacy Source Path | Target Description / Role |
| :--- | :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | PySpark | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | Main PySpark execution logic covering validation, master contract join mapping, deduplication, and transactional multi-channel target tables insert. |

---

## 6. RISKS & MANUAL ACTIONS

The following risk factors, gaps, or logic challenges have been identified during this design phase. Defer these to the Build/Deployment phase for explicit testing or manual resolution:

1. **Transactional Reload Lock Mitigation (Critical):**
   * **Legacy Action:** The graph deletes existing records in target tables match-by-match (using business keys) in memory and then inserts.
   * **GCP Target Impact:** Executing row-by-row deletions on BigQuery is inefficient and can cause transaction locking or high costs. 
   * **Mitigation Strategy:** The PySpark implementation maps these operations into bulk `LEFT ANTI JOIN` blocks. This computes the net delta in-memory before issuing a clean rewrite partition or a optimized overwrite.

2. **DML Verification Failures (`raise_error` Aborts):**
   * **Legacy Action:** The graph utilizes `force_error()` which causes immediate graph abort on invalid dates or format errors.
   * **GCP Target Impact:** In PySpark, raising runtime exceptions stops the Spark cluster executor immediately.
   * **Manual Step:** The data ingestion pipeline should implement a "quarantine" or bad-records path to collect faulty rows in GCS rather than blindly calling `raise_error()` which crashes production runs.

3. **Dynamic Path Substitutions:**
   * **Details:** Legacy relies on KSH environmental path evaluation (`$DW_DIR_IMP_SAP/crs/work/`).
   * **Manual Step:** Confirm the environment mount point mapping on the Cloud Composer Airflow workers and the Dataproc cluster variables configurations. Ensure that read operations cleanly target `gs://[environment]-dwh-import-sap/crs/work/` buckets. Retain strict path cases to prevent file-not-found exceptions.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

## 1. FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Migrates the KornShell execution wrapper to Python 3. Parses files, maps metadata, performs dynamic pre-load/post-load SQL deletes and audit updates against BigQuery, and submits the PySpark job (representing the Ab Initio graph) to Google Cloud Dataproc Serverless. |

---

## 2. TARGET FILE PLAN

### Target File: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py`
* **Source Path:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`
* **Language:** Python 3
* **Description:** A production-ready Python orchestration script that replaces the legacy Ab Initio wrapper script. It handles environment configurations, sources metadata specifications, executes transactional BigQuery DML operations (such as idempotency deletes, trailer logging, and group metadata audits), and submits the core PySpark pipeline to a Dataproc Serverless cluster.
* **Folder Integrity:** Preserves folder structures exactly under `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/`.

---

## 3. VERBATIM MCP TOOL OUTPUT

Below is the complete design document and Python pseudocode returned by the `ksh_design_python` converter:

```markdown
# DESIGN DOCUMENT: map_rpos_carmen_import Conversion

## 1. SCRIPT OVERVIEW
This script is a GDE-deployed Ab Initio execution wrapper for the graph `map_rpos_carmen_import`. Its primary purpose is to orchestrate the ETL pipeline that imports retail point-of-sale (RPOS) billing and invoice data from the Carmen financial/leasing system into the Data Warehouse (DWH). It reads incoming flat files containing RPOS transaction details, correlates them with master contract data, applies complex filtering/reconciliations, and writes the resulting transaction sets to various target database tables while executing housekeeping deletions and audit updates.

## 2. INVOCATION CONTEXT
* **Caller:** UC4/Automic Scheduler (typically a Unix job object under a path like `TMD_processing/BHB/BD_PROC/run/`).
* **Command Line / Arguments:** Sourced/executed with positional arguments:
  * `$1`: `-help` (displays help/exits) or `-reposit-tracking` (initiates enterprise metadata repository tracking).
* **UC4 Native Includes:**
  * None explicitly referenced via `:inc` syntax in this source.
* **Environment Files Sourced:**
  * `$_AB_PROJECT_KSH` (resolved dynamically as `${_AB_SAVED_PROJECT_DIR}/.project.ksh`). 
    * `# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
  * `ab_catalog_functions.ksh` (sourced conditionally if present in `PATH`/`AB_HOME`).
    * `# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables/functions it defines are unknown`
  * `./${_AB_PROXY_DIR}/GDE-Parameters` (locally generated parameters file containing GDE environment exports).

## 3. PARAMETERS / INPUTS
### Command Line & Shell Parameters
* **`$1` (Positional):** 
  * Source: UC4 execution argument.
  * Usage: Evaluated for `-reposit-tracking` (triggers EME sandbox registration and execution via `run-and-reposit`) or `-help` (exits with status 1).
  * Python: Handled via `sys.argv` or `argparse`.
* **`AB_HOME` / `MPOWERHOME`:** 
  * Source: Environment variable.
  * Usage: Points to the Ab Initio installation directory (defaults to `/appl/local/abinitio/abinitio`).
  * Python: `os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")`.
* **`AB_AIR_HOME`:** 
  * Source: Environment variable.
  * Usage: Points to the Ab Initio EME directory.
  * Python: `os.environ.get("AB_AIR_HOME")`.
* **`PROJECT_DIR`:** 
  * Source: Environment variable or derived from script path `$0`.
  * Usage: Root project directory path.
  * Python: `os.environ.get("PROJECT_DIR")` or computed via `os.path.dirname(os.path.abspath(__file__))`.

### DB Connection Parameters (Cross-Referenced Convention)
The following database environment parameters are validated in the script body:
* **`DB_TNS_NAME_DWH` / `DB_USER_DWH` / `DB_PASSWD_DWH`:** 
  * DB-connection-style parameters for the main Data Warehouse schema. Used to run queries and updates against `DWH$TA_*` tables.
* **`DB_TNS_NAME_CRS` / `DB_USER_CRS` / `DB_PASSWD_CRS`:**
  * DB-connection-style parameters for CRS database.
* **`DB_TNS_NAME_SGM` / `DB_USER_SGM` / `DB_PASSWD_SGM`:**
  * DB-connection-style parameters for SGM database.
* **`DB_TNS_NAME_CADS` / `DB_USER_CADS` / `DB_PASSWD_CADS`:**
  * DB-connection-style parameters for CADS database.
* **`DB_TNS_NAME_CACM` / `DB_USER_CACM` / `DB_PASSWD_CACM`:**
  * DB-connection-style parameters for CACM database.

### Framework Configuration Parameters
* **`BHB_Projektverzeichnis`**: Root directory of the BHB application.
* **`BHB_Graph`**: Name of the target graph.
* **`BHB_Prozesstyp`**: Process categorization.
* **`BHB_Eintragsnr`**: Entry audit log tracking identifier (maps to `entrynr` / `eintragsnr`).
* **`BHB_Quellverzeichnis`**: Directory containing source files.
* **`BHB_Zielverzeichnis`**: Target output file directory.
* **`BHB_Dateimaske`**: File pattern filter.
* **`BHB_Kopfdatensatzkennung`**: Header record identifier.
* **`BHB_Nutzdatensatzkennung`**: Data record identifier.
* **`BHB_Endedatensatzkennung`**: Trailer/Footer record identifier.
* **`BHB_Dateiname`**: Specific filename to be processed (passed to the read interface).

All framework parameters are evaluated in the shell. If evaluation fails (return code `!= 0`), the script terminates immediately with the corresponding exit code.

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
* **`uname`**
  * Verbatim: `` `uname` `` / `$(uname)`
  * Purpose: Platform detection (Windows vs Cywin vs UNIX).
  * Python: Native `platform.system()` or `sys.platform`.
* **`air sandbox find`**
  * Verbatim: `air sandbox find "${PROJECT_DIR}" -project`
  * Purpose: Find project path inside the EME Datastore.
  * Python: `subprocess.run(...)` if Ab Initio command-line utilities are available in the runtime.
* **`run-and-reposit`**
  * Verbatim: `${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/map_rpos_carmen_import.mp' "${_AB_PROJECT_NAME}" _abort "$0" "$@"`
  * Purpose: Standard Ab Initio utility to execute a graph while tracking catalog statistics in the EME.
  * Python: Retained as a `subprocess.run` call.
* **`mp` suite (`mp job`, `mp layout`, `mp metadata`, `mp straight-flow`, etc.)**
  * Verbatim: Numerous lines starting with `mp ` to compile layouts, declare metadata types, and connect flows.
  * Purpose: Ab Initio orchestration commands to dynamically compile the graph pipeline.
  * Python: Kept as opaque `subprocess.run` calls or handled by high-level pipeline scheduling tools.
  * `# REVIEW-STRUCT: launcher [mp] invoked — internal execution depends on Ab Initio Co>Operating System; confirm environment availability or execute migration of the graph logic to Python (e.g., PySpark/Pandas) before finalizing the conversion`

## 5. EMBEDDED SQL
The script outputs several SQL files to a local proxy directory, which are subsequently called during execution via Ab Initio database load components (`mp db-update`, `mp db-lookup`, etc.).

### SQL Statement 1
* **Source:** `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql`
* **Verbatim SQL:**
```sql
DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```
* **Type:** DELETE
* **Tables Touched:** `DWH$TA_F_RPOS_CARM`
* **Dialect:** Unambiguously Oracle SQL (indicated by Oracle parameter binding notation `:variable`).

### SQL Statement 2
* **Source:** `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql`
* **Verbatim SQL:**
```sql
DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```
* **Type:** DELETE
* **Tables Touched:** `DWH$TA_F_GPOS_FACT_CARM`
* **Dialect:** Oracle SQL

### SQL Statement 3
* **Source:** `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql`
* **Verbatim SQL:**
```sql
DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```
* **Type:** DELETE
* **Tables Touched:** `DWH$TA_F_RPOS_CARM`
* **Dialect:** Oracle SQL

### SQL Statement 4
* **Source:** `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql`
* **Verbatim SQL:**
```sql
DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```
* **Type:** DELETE
* **Tables Touched:** `DWH$TA_F_RPOS_FACT_CARM`
* **Dialect:** Oracle SQL

### SQL Statement 5
* **Source:** `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql`
* **Verbatim SQL:**
```sql
DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```
* **Type:** DELETE
* **Tables Touched:** `DWH$TA_F_RPOS_RESELLING_CARM`
* **Dialect:** Oracle SQL

### SQL Statement 6
* **Source:** `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql`
* **Verbatim SQL:**
```sql
DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
```
* **Type:** DELETE
* **Tables Touched:** `DWH$TA_T_RPOS_CARM`
* **Dialect:** Oracle SQL

### SQL Statement 7
* **Source:** `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql`
* **Verbatim SQL:**
```sql
UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil
```
* **Type:** UPDATE
* **Tables Touched:** `DWH$TA_K_RECH_ABSGRP`
* **Dialect:** Oracle SQL

### SQL Statement 8
* **Source:** `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql`
* **Verbatim SQL:**
```sql
INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)
```
* **Type:** INSERT
* **Tables Touched:** `DWH$TA_K_RECH_ABSGRP`
* **Dialect:** Oracle SQL

### SQL Statement 9
* **Source:** `${_AB_PROXY_DIR}/Update_DWH_TA_K_MELDUNGEN-74.sql`
* **Verbatim SQL:**
```sql
update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
```
* **Type:** UPDATE
* **Tables Touched:** `DWH$TA_K_MELDUNGEN`
* **Dialect:** Oracle SQL

### SQL Statement 10 (Embedded in Graph Lookup Commands)
* **Source:** `mp db-lookup` / `mp itable` definitions
* **Verbatim SQL:**
```sql
select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('20050401', 'YYYYMMDD')
```
* **Type:** SELECT
* **Tables Touched:** `DWH$TA_C_VERTRAG`
* **Dialect:** Oracle SQL (utilizes `to_date` function and `(+)` syntax in outer join references in metadata).

## 6. CONTROL FLOW
1. **Environment Initialization:** Set paths and environment parameters (`AB_HOME`, `PATH`, etc.).
2. **Platform Constraints Detection:** Parse `uname` and adjust `PATH` for Cygwin/Windows if necessary.
3. **Internal Helper Declarations:** Define Ab Initio system functions (`__AB_INVOKE_PROJECT`, `__AB_dirname`, etc.).
4. **Project Directory Computation:** Extract `PROJECT_DIR` based on path structure and verify absolute canonical path.
5. **Proxy Directory Construction:** Generate unique temporary directory name (`map_rpos_carmen_import-ProxyDir-$$`), create it, and register trapping logic to ensure immediate clean up on exit or signal termination (HUP, INT, QUIT, TERM).
6. **EME Sandbox Reposit-Tracking Evaluation:**
   * Query the Ab Initio EME environment tracking status (`m_env`).
   * If enabled, call `air sandbox find` to get EME path and delegate execution to `run-and-reposit`, terminating wrapper execution with its return status.
7. **Pre-execution Project Sourcing:** Call `.project.ksh` with arguments `execute start`.
8. **Help Parameter Interception:** Exit immediately with status 1 if parameter `$1` equals `-help`.
9. **Parameter Validation & Export:** Validate each database and business framework parameter. If evaluation of any variable generates an error, exit immediately with the corresponding failure code.
10. **Custom Script Hook execution:** Set Oracle session variable `NLS_NUMERIC_CHARACTERS = ". "`.
11. **Proxy Files Provisioning:** Build and output transformation metadata (`.dml`), mapping logic (`.xfr`), and target database query execution scripts (`.sql`) to the proxy directory.
12. **Catalog Creation:** Erase any existing session catalog (`m_rmcatalog`) and initialize a fresh lookup catalog (`m_mkcatalog`).
13. **Ab Initio Graph Compilation & Topology Layout:** Define logical layouts, pipeline flows (`mp metadata`, `mp layout`), and step sequences.
14. **Graph Pipeline Execution (`mp run`):** Direct the Co>Operating System orchestration engine to execute data reading, contract matching joins, delta deletes, record mappings, and parallel inserts.
15. **Status Capture:** Store the execution exit code of `mp run` in `mpjret`.
16. **Environment Reset:** Wipe local session lookup catalogs (`m_rmcatalog`) and restore system catalog configurations.
17. **Post-execution Project Sourcing:** Invoke `.project.ksh` with arguments `execute end`.
18. **Cleanup & Exit:** Execute trap-handler to destroy the proxy directory and return state `mpjret`.

## 7. ERROR HANDLING & EXIT CODES
* **Validation Assertions:** Evaluation of each environment/DB parameter is systematically checked: `mpjret=$?`. If `mpjret != 0`, the script prints an evaluation error message and exits with the returned code.
* **Orchestration Failure Catching:** System failure or internal engine crash in `mp run` is caught via return state `mpjret=$?`.
* **Trap Housekeeping:** Active signals (HUP, INT, QUIT, TERM) trigger immediate execution of `__AB_CLEANUP_PROXY_FILES` before propagating the original status code to prevent leftover temporary files in production.
* **Python Mapping:** Standardize subprocess executions using `subprocess.run(..., check=True)` which translates non-zero exit codes into raising `subprocess.CalledProcessError`. Wrap parameter setup and database transactions inside `try...except...finally` blocks where the `finally` block replaces shell traps to guarantee cleanup.

## 8. OUTPUTS / SIDE EFFECTS
* **Database Updates:**
  * Multiple rows deleted from:
    * `DWH$TA_F_RPOS_CARM`
    * `DWH$TA_F_GPOS_FACT_CARM`
    * `DWH$TA_F_RPOS_FACT_CARM`
    * `DWH$TA_F_RPOS_RESELLING_CARM`
    * `DWH$TA_T_RPOS_CARM`
  * New data inserted/loaded into:
    * `DWH$TA_F_RPOS_CARM`
    * `DWH$TA_T_RPOS_CARM`
    * `DWH$TA_F_GPOS_FACT_CARM`
    * `DWH$TA_F_RPOS_FACT_CARM`
    * `DWH$TA_F_RPOS_RESELLING_CARM`
  * Audit logs inserted/updated in:
    * `DWH$TA_K_RECH_ABSGRP` (Tracking processed billing files per group)
    * `DWH$TA_K_MELDUNGEN` (Auditing dataset counts and file trailer verification logs)
* **Local Filesystem:**
  * Creation/destruction of the temporary directory `${AB_JOB}-map_rpos_carmen_import-ProxyDir` containing the generated `.dml`, `.xfr`, and `.sql` assets.

## 9. BUSINESS SUMMARY
* **ETL Consolidation:** Integrates point-of-sale incoming billing data from the external leasing platform "Carmen" with current DWH active contracts.
* **Financial Category Routing:** Classifies processed transaction lines into Factoring Invoices ("Factoring Rechnungen"), Factoring Credits ("Factoring Gutschriften"), or Reselling categories based on key indicators such as contract structure ID, invoice code type (`RABATT`), and validity flags.
* **Strict History Alignment:** Performs historical joins against the master contract table (`DWH$TA_C_VERTRAG`). Records must overlap with specific validity periods (`gueltig_von`/`gueltig_bis`) to maintain precise contract state reporting.
* **Automated Reconciliation/Idempotency:** Ensures strict target table state by deleting any previous runs matching incoming transaction keys (`rechnung_id`, `rechnung_datum`, etc.) before loading new datasets.
* **Control Record Compliance:** Validates overall data integrity by reading file trailer metadata, matching dataset counts against the DWH logging schema (`DWH$TA_K_MELDUNGEN`), and auditing billing group properties (`DWH$TA_K_RECH_ABSGRP`).

---

# PYTHON PSEUDOCODE OUTLINE

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Modernized Python 3 script replacing map_rpos_carmen_import legacy ksh orchestration pipeline

import os
import sys
import shutil
import tempfile
import platform
import subprocess
import traceback

# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
# Establish database client connection using standard Oracle drivers (oracledb / cx_Oracle)
try:
    import oracledb as db_client
except ImportError:
    import cx_Oracle as db_client  # Fallback for older environments

# Step 1: Initialize System & Environment Settings
def setup_environment():
    os.environ["AB_HOME"] = os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")
    os.environ["MPOWERHOME"] = os.environ.get("MPOWERHOME", os.environ["AB_HOME"])
    
    # Adjust path according to OS Platform
    current_system = platform.system()
    if current_system.startswith("Windows"):
        os.environ["PATH"] = f"{os.environ['AB_HOME']}/bin;{os.environ.get('PATH', '')}"
    elif "CYGWIN" in current_system:
        # Mimic cygpath conversion under Cygwin environments
        cyg_path = subprocess.run(["cygpath", os.environ["AB_HOME"]], capture_output=True, text=True).stdout.strip()
        os.environ["PATH"] = f"{cyg_path}/bin:/usr/local/bin:/usr/bin:/bin:{os.environ.get('PATH', '')}"
    else:
        os.environ["PATH"] = f"{os.environ['AB_HOME']}/bin:{os.environ.get('PATH', '')}"

    os.environ["AB_REPORT"] = os.environ.get("AB_REPORT", "monitor=60 processes scroll=true")
    os.environ["AB_AIR_HOME"] = os.environ.get("AB_AIR_HOME", "/appl/local/abinitio/abinitio-V2-14")
    os.environ["AB_COMPATIBILITY"] = "2.14.59"
    os.environ["AB_GRAPH_NAME"] = "map_rpos_carmen_import"
    
    # Session locale variables
    os.environ["NLS_NUMERIC_CHARACTERS"] = ". "

# Step 2: Validate Required Environment Parameters
def check_parameters():
    required_params = [
        "DB_TNS_NAME_DWH", "DB_USER_DWH", "DB_PASSWD_DWH",
        "DB_TNS_NAME_CRS", "DB_USER_CRS", "DB_PASSWD_CRS",
        "DB_TNS_NAME_SGM", "DB_USER_SGM", "DB_PASSWD_SGM",
        "DB_TNS_NAME_CADS", "DB_USER_CADS", "DB_PASSWD_CADS",
        "DB_TNS_NAME_CACM", "DB_USER_CACM", "DB_PASSWD_CACM",
        "BHB_Projektverzeichnis", "BHB_Graph", "BHB_Prozesstyp", "BHB_Eintragsnr",
        "BHB_Quellverzeichnis", "BHB_Zielverzeichnis", "BHB_Dateimaske",
        "BHB_Kopfdatensatzkennung", "BHB_Nutzdatensatzkennung", "BHB_Endedatensatzkennung",
        "BHB_Dateiname"
    ]
    
    for param in required_params:
        if param not in os.environ:
            print(f"Error evaluating: parameter {param} of map_rpos_carmen_import", file=sys.stderr)
            sys.exit(1)

# Step 3: Parse Command-Line Options
def parse_arguments():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == "-help":
            print("Displaying Ab Initio Wrapper Help Options...", file=sys.stderr)
            sys.exit(1)
        elif arg == "-reposit-tracking":
            # Reposit tracking process via EME Sandbox
            # Execute sandbox identification
            project_dir = os.environ.get("PROJECT_DIR", ".")
            try:
                # REVIEW-STRUCT: launcher [air] invoked — internal EME integration; confirm air CLI environment is valid
                res = subprocess.run(["air", "sandbox", "find", project_dir, "-project"], capture_output=True, text=True, check=True)
                project_name = res.stdout.strip()
                
                # Execute run-and-reposit pipeline
                os.environ["AB_GRAPH_SCRIPT_REPOSIT_TRACKING"] = "false"
                run_cmd = [
                    f"{os.environ['AB_HOME']}/bin/run-and-reposit",
                    f"{project_name}/mp/map_rpos_carmen_import.mp",
                    project_name,
                    sys.argv[0]
                ] + sys.argv[2:]
                
                reposit_res = subprocess.run(run_cmd, check=True)
                sys.exit(reposit_res.returncode)
            except subprocess.CalledProcessError as e:
                print(f"EME Sandbox Tracking Registration Failure: {e}", file=sys.stderr)
                sys.exit(1)

# Step 4: Write Generated SQL Files
def generate_sql_files(proxy_dir):
    # Statement 1 & 3: Delete RPOS records
    delete_rpos_sql = """DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    with open(os.path.join(proxy_dir, "Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql"), "w") as f:
        f.write(delete_rpos_sql)

    with open(os.path.join(proxy_dir, "Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql"), "w") as f:
        f.write(delete_rpos_sql)

    # Statement 2: Delete Factoring GPOS Records
    delete_gpos_fact_sql = """DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    with open(os.path.join(proxy_dir, "Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql"), "w") as f:
        f.write(delete_gpos_fact_sql)

    # Statement 4: Delete Factoring RPOS Records
    delete_rpos_fact_sql = """DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    with open(os.path.join(proxy_dir, "Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql"), "w") as f:
        f.write(delete_rpos_fact_sql)

    # Statement 5: Delete Reselling RPOS Records
    delete_reselling_sql = """DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    with open(os.path.join(proxy_dir, "Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql"), "w") as f:
        f.write(delete_reselling_sql)

    # Statement 6: Delete Temporary RPOS Records
    delete_temp_rpos_sql = """DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id"""
    with open(os.path.join(proxy_dir, "Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql"), "w") as f:
        f.write(delete_temp_rpos_sql)

    # Statement 7 & 8: Update and Insert Tracking Billing Group Information
    update_absgrp_sql = """UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil"""
    with open(os.path.join(proxy_dir, "Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql"), "w") as f:
        f.write(update_absgrp_sql)

    insert_absgrp_sql = """INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)"""
    with open(os.path.join(proxy_dir, "Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql"), "w") as f:
        f.write(insert_absgrp_sql)

    # Statement 9: Update Audit Log Reporting State
    update_meldungen_sql = """update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr"""
    with open(os.path.join(proxy_dir, "Update_DWH_TA_K_MELDUNGEN-74.sql"), "w") as f:
        f.write(update_meldungen_sql)

# Step 5: Execute Main Pipeline Execution
def execute_pipeline(proxy_dir):
    # Define Job ID context
    ab_job = os.environ.get("AB_JOB_PREFIX", "") + "map_rpos_carmen_import"
    os.environ["AB_JOB"] = ab_job
    
    # Rename tracking proxy directory for uniqueness in file access
    final_proxy_dir = f"{ab_job}-map_rpos_carmen_import-ProxyDir"
    if os.path.exists(final_proxy_dir):
        shutil.rmtree(final_proxy_dir)
    shutil.move(proxy_dir, final_proxy_dir)
    
    try:
        # Setup lookup catalog files
        subprocess.run(["m_rmcatalog", f"GDE-map_rpos_carmen_import-{ab_job}.cat"], capture_output=True)
        subprocess.run(["m_mkcatalog", "-catalog", f"GDE-map_rpos_carmen_import-{ab_job}.cat"], check=True)
        
        # Sourcing catalog and layout properties
        os.environ["AB_CATALOG"] = f"GDE-map_rpos_carmen_import-{ab_job}.cat"
        
        # Layout allocations via M-Power Command Suite
        # REVIEW-STRUCT: launcher [mp] invoked — downstream graph execution orchestration
        subprocess.run(["mp", "job", ab_job], check=True)
        subprocess.run(["mp", "layout", "layout1", "-hosts"] + ["localhost"]*16, check=True)
        subprocess.run(["mp", "layout", "layout2", "."], check=True)
        
        # Metadata allocation references mapping DML formats
        subprocess.run(["mp", "metadata", "metadata1", "-file", f"{final_proxy_dir}/dwh_ta_c_vertrag-12.dml"], check=True)
        # (... Subsequent layouts & metadata components mapping to mp executable configurations)
        
        # Trigger execution of the dynamically compiled pipeline graph
        print("Executing Carmen Import ETL pipeline...", sys.stderr)
        mp_run_result = subprocess.run(["mp", "run"], check=True)
        return mp_run_result.returncode
        
    finally:
        # Final session cleanup tasks
        subprocess.run(["mp", "reset"], capture_output=True)
        subprocess.run(["m_rmcatalog"], capture_output=True)
        if os.path.exists(final_proxy_dir):
            shutil.rmtree(final_proxy_dir)

# Step 6: Master Orchestration Main Control
def main():
    # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown
    # Setup baseline configurations and arguments checking
    setup_environment()
    parse_arguments()
    check_parameters()
    
    # Establish local isolated temp state folder
    base_temp_dir = tempfile.mkdtemp(prefix="map_rpos_carmen_import-ProxyDir-")
    
    exit_code = 0
    try:
        # Sourcing project variables
        project_ksh = os.path.join(os.environ.get("PROJECT_DIR", "."), ".project.ksh")
        if os.path.exists(project_ksh):
            # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown
            subprocess.run([project_ksh, os.environ.get("PROJECT_DIR", "."), "execute", "start"], check=True)
        
        # Generate processing metadata artifacts
        generate_sql_files(base_temp_dir)
        
        # Run import graph operations
        exit_code = execute_pipeline(base_temp_dir)
        
        # Post-execution status sourcing hook
        if os.path.exists(project_ksh):
            # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown
            subprocess.run([project_ksh, os.environ.get("PROJECT_DIR", "."), "execute", "end"], check=True)
            
    except subprocess.CalledProcessError as err:
        print(f"Pipeline Stage execution failed: {err}", file=sys.stderr)
        traceback.print_exc()
        exit_code = err.returncode if err.returncode is not None else 1
    except Exception as general_err:
        print(f"Execution Error occurred: {general_err}", file=sys.stderr)
        traceback.print_exc()
        exit_code = 1
    finally:
        # Ensure temporary directories are wiped out regardless of execution state
        if os.path.exists(base_temp_dir):
            shutil.rmtree(base_temp_dir)
            
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
```
```

---

## 4. ADDED CONTEXT (ENVIRONMENT SPECIFICS)

### A. Job Dependencies & Lineage Edges
* **Upstream Job Dependencies:**
  * **Shared File Dependency:** `abinitio_pyspark_linked_job/isccr/abinitio/bin` has already been migrated and merged (GitHub PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/764`). Specifically, the utility script `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` is imported/referenced rather than re-designed.
  * In the Google Cloud environment, the translated Python orchestration script references this shared utility using Cloud Composer Airflow imports or common PySpark utility paths.
* **Downstream Job Dependencies:**
  * None discovered in the legacy job dependencies context.

### B. Execution Sequence & Preservation
The target pipeline MUST execute the logical components in the following chronological sequence to match legacy dependency execution order:
1. **UC4 Orchestration Layer:** Sourced/Scheduled by Google Cloud Composer DAG (`DW.RPOS_CARM_IMPORT`).
2. **Parameters Loading:** Reads and validates properties equivalent to `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` (translated as Airflow DAG `params` or runtime configuration dictionaries).
3. **Idempotency Actions (Wrapper):** Performs delete operations on BigQuery (`DWH$TA_F_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, `DWH$TA_T_RPOS_CARM`) to clean up records for the active period.
4. **PySpark core ETL Job Execution:** Launches the PySpark transformation (migrated from the Ab Initio Graph `map_rpos_carmen_import.mp`) via a Dataproc Serverless Operator task in the DAG.
5. **Auditing and Housekeeping Updates:** Sourced files metadata and trailer verification checks write metrics back to metadata logging tables (`DWH$TA_K_RECH_ABSGRP` and `DWH$TA_K_MELDUNGEN`).

### C. Scheduling & Trigger Mechanisms
* **Schedule Context:** Sourced by the upstream UC4 coordinator.
* **Target Scheduling Construct:** Converted to an Airflow Cron-based schedule or event-based trigger sensor in Cloud Composer depending on global orchestration guidelines (e.g. daily/monthly triggers matching incoming Carmen pos file arrival events).

### D. External System Replacements
* **Legacy Flat File Directories:**
  * Source Directory (`$DW_DIR_IMP_SAP/crs/work/`) maps to a GCS work bucket path: `gs://{GCS_BUCKET}/crs/work/`.
  * Target/Archive Directory (`$DW_DIR_IMP_SAP/crs/store/`) maps to a GCS store bucket path: `gs://{GCS_BUCKET}/crs/store/`.
* **Legacy Databases / Connection SIDs:**
  * Oracle TNS Connection names (`DB_TNS_NAME_DWH`, `DB_TNS_NAME_CRS`, `DB_TNS_NAME_SGM`, `DB_TNS_NAME_CADS`, `DB_TNS_NAME_CACM`) are decommissioned and unified inside BigQuery dataset structures. All SQL operations execute natively on BigQuery schemas.

### E. Environment-Specific Values (Global vs. Job-Specific)

Following the environment variables classification policy, variables used by the legacy shell script are mapped to target mechanisms below:

#### 1. GLOBAL (Environment-Wide Infrastructure Constants)
The values below remain the same for every job in this deployment environment (Dev/Test/Prod). They are resolved dynamically at runtime:
* **`GCP_PROJECT`**: The target Google Cloud Project ID.
* **`GCS_BUCKET`**: Shared Cloud Storage Bucket replacing `$DW_DIR_IMP_SAP` paths.
* **`BQ_DATASET`**: Target BigQuery Dataset where the warehouse tables (`DWH$TA_*`) reside. Sourced using native Airflow variables or parameter bindings.
* **`DATAPROC_REGION` / `DATAPROC_CLUSTER`**: Target region and cluster information for executing the serverless PySpark execution engine.

*Source Mechanism:*
* In Python tasks: `os.environ.get("GCP_PROJECT")`, etc.
* In Cloud Composer Airflow DAG: Sourced from `from airflow.models import Variable` using `Variable.get("GCP_PROJECT")`.
* In BigQuery SQL tasks: Substituted dynamically by the calling Python script using parameterized query formatting (e.g., `@{gcp_project}`).

#### 2. JOB-SPECIFIC (Parameters Particular to This Job)
The values below are particular to `map_rpos_carmen_import` and are stored inside Composer DAG params or job config objects:
* **`BHB_Projektverzeichnis`**: `/Projects/TMD/processing/BHB/BD_PROC`
* **`BHB_Graph`**: `map_rpos_carmen_import`
* **`BHB_Prozesstyp`**: `D`
* **`BHB_Dateimaske`**: `CARMEN_B_*_pos.fix`
* **`BHB_Kopfdatensatzkennung`**: `H`
* **`BHB_Nutzdatensatzkennung`**: `P`
* **`BHB_Endedatensatzkennung`**: `X`
* **`BHB_Eintragsnr`**: Provided at runtime by Composer orchestration trigger.
* **`BHB_Dateiname`**: Provided dynamically based on GCS file list match.

---

## 5. RISKS & MANUAL ACTIONS

* **SOURCE: DECOMMISSIONED/NOT REQUIRED — `AB_CATALOG_FUNCTIONS.KSH` — no candidate**
  * *Notes:* Lineage indicates reference to `AB_CATALOG_FUNCTIONS.KSH`. This has been reviewed by domain experts and confirmed as **not needed** (Resolution signed off by guru on 2026-07-24). No manual conversion is required.
* **SOURCE: DECOMMISSIONED/NOT REQUIRED — `.project.ksh` / `.project` environment scripts**
  * *Notes:* Legacys project-level shell scripts are decommissioned. Sourced environment variables are mapped to BigQuery parameter overrides and Cloud Composer Airflow parameters.
* **Ab Initio Graph Code Decoupling:**
  * *Notes:* The wrapper executes an Ab Initio graph `map_rpos_carmen_import.mp` via `mp run`. Designing and converting the graph's internal transformation components to PySpark forms a separate, independent design pass. The target Python script must call this PySpark pipeline via Dataproc Serverless Operators. Ensure that the separate PySpark migration design is completed before integration testing.
* **Language/Literal String Retention Compliance:**
  * *Notes:* To comply with the Output/Print Literal Rule, all German error descriptions and trailer comments printed by the graph metadata mapping blocks (e.g., `"Invalid data format in monats_id"`, `"Invalid Data in field debitor_id"`, etc.) are retained character-for-character inside the PySpark mapping logic without any translation.