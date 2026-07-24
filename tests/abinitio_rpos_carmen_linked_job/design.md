# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

## 1. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `dags/abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import_dag.py` | Migrated Airflow DAG executing the Dataproc Serverless PySpark job mapped from the Ab Initio graph. |

---

## 2. Shared & Retained Resources
The following shared components are already migrated or confirmed not needed:
* **Shared Modules:** `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` is already migrated and merged under PR `https://github.com/gurunathan-prodapt/pi-agents/pull/764`. The migrated DAG should call the equivalent converted PySpark entry point rather than invoking the legacy shell wrapper.
* **Human-Confirmed Exemptions (No Source Needed):**
  * `.dw_init` (not needed)
  * `.CCR_INIT` (not needed)
  * `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` (not needed)
  * `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` (not needed)
  * `DW.HOLE_PFAD` (not needed)
  * `DW.LESE_LOG` (not needed)
  * `AB_CATALOG_FUNCTIONS.KSH` (not needed)
  * `H_ALIS_DATE.KSH` (not needed)
  * `H_ALIS_DATENOBJEKT.KSH` (not needed)
  * `H_ALIS_MELDUNGEN.KSH` (not needed)
  * `H_ALIS_PARAMETER.KSH` (not needed)

---

## 3. Orchestration & Context Details

### Job Dependencies
* **Upstream:**
  * Shared Files module `/isccr/abinitio/bin/r_ai_start` must exist and be accessible (already migrated).
* **Downstream (Data Consumers):**
  * This workflow populates the target DWH tables: `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, and `DWH$TA_T_RPOS_CARM`.

### Execution Order Mapping
The legacy workflow sequence is mapped to Airflow task execution:
1. **UC4 Job Definition (`DW.RPOS_CARM_IMPORT.xml`):** Replaced by the Composer DAG `dw_rpos_carm_import_dag.py`.
2. **Parameters File (`map_rpos_carmen_import.cfg`):** Inlined as a structured job configuration dictionary inside the Airflow DAG or passed as job parameters.
3. **Execution Script (`map_rpos_carmen_import.ksh`):** Retired. Orchestration logic is handled directly by Composer.
4. **Ab Initio Graph (`map_rpos_carmen_import.mp`):** Converted to PySpark and executed on Google Cloud Dataproc Serverless.

### Scheduling & Scheduler-Set Variables
* **Schedule:** There is no schedule specified in the UC4 XML (`schedule=None`). This DAG is designed to be triggered manually or externally.
* **Variables:**
  * `DWH_JOB_KENNUNG` (value: `'RPOS_CARM_IMPORT'`) is passed directly to the Spark application as an execution argument (`--job_kennung`).

### External System Replacements
* **File Directories:**
  * Legacy directory references using `$DW_DIR_IMP_SAP` (e.g. `crs/work/` and `crs/store/`) are mapped to Google Cloud Storage (GCS) paths under `gs://{GCS_BUCKET}/`.
* **Execution Engine:**
  * The legacy Ab Initio GDE parallel engine is replaced by GCP Dataproc Serverless (PySpark).

---

## 4. Environment-Specific Values

All environment variables have been classified according to role:

### GLOBAL Variables (Infrastructure-level)
To comply with environment isolation and prevent hardcoded placeholders, these variables must be retrieved dynamically from Airflow's metadata store:
* `GCP_PROJECT`: Retrieved via `Variable.get("GCP_PROJECT")`
* `GCP_REGION`: Retrieved via `Variable.get("GCP_REGION")`
* `DATAPROC_CLUSTER_NAME`: Retrieved via `Variable.get("DATAPROC_CLUSTER")`
* `GCS_BUCKET`: Retrieved via `Variable.get("GCS_BUCKET")`

### JOB-SPECIFIC Variables (Application-level)
These parameters are specific to the `DW.RPOS_CARM_IMPORT` workload and are hardcoded or derived from the parameters configuration:
* `DWH_JOB_KENNUNG`: `'RPOS_CARM_IMPORT'`
* `BHB_Projektverzeichnis`: `"/Projects/TMD/processing/BHB/BD_PROC"`
* `BHB_Version`: `"RLS_BHB_nach_64_rabatt_sap"`
* `BHB_Graph`: `"map_rpos_carmen_import"`
* `BHB_Prozesstyp`: `"D"`
* `BHB_Quellverzeichnis`: `f"gs://{{ GCS_BUCKET }}/crs/work/"`
* `BHB_Zielverzeichnis`: `f"gs://{{ GCS_BUCKET }}/crs/store/"`
* `BHB_Dateimaske`: `"CARMEN_B_*_pos.fix"`
* `BHB_Kopfdatensatzkennung`: `"H"`
* `BHB_Nutzdatensatzkennung`: `"P"`
* `BHB_Endedatensatzkennung`: `"X"`

---

## 5. Verbatim MCP Tool Output

```markdown
# UC4 to Apache Airflow Migration Design Document

## 1. Overview
The UC4 object `DW.RPOS_CARM_IMPORT` is a Unix-based job (`JOBS_UNIX`) designed to execute an Ab Initio graph called `map_rpos_carmen_import`. It processes data imports for the RPOS Carmen business domain by invoking the standard UC4 Ab Initio launch wrapper `r_ai_start` with specific configuration and identification flags. Since this extraction bundle contains only this single job and no parent Job Plan (`JOBP`) or schedule, this design document establishes a standalone wrapper DAG in Apache Airflow to execute this migrated workload.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.RPOS_CARM_IMPORT` | JOBS_UNIX | 1 (Active) | Job startet AbInitio Graph  map_rpos_carmen_import |

---

## 3. Scheduling
* **Schedule Source:** No schedule (`EVNT_TIME` or `JSCH`) or triggering script (`SCRI`) is present in this bundle.
* **Trigger Pattern:** This job is externally triggered, meaning its source is unknown from this extraction alone.
* **Airflow Schedule:** `schedule=None` (manual or external trigger).

---

## 4. Airflow DAG Properties
The following DAG properties are established for the wrapper DAG:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_rpos_carm_import` |
| **schedule** | `None` |
| **start_date** | `datetime(2026, 4, 21)` *(derived from UC4 export timestamp)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1 in UC4 source)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `rpos_carm_import` | `DW.RPOS_CARM_IMPORT` | `DataprocSubmitJobOperator` | `map_rpos_carmen_import.py` | `project_id`, `region`, `cluster_name` (GCP placeholders), PySpark main file at `gs://YOUR_BUCKET_NAME/pyspark_scripts/map_rpos_carmen_import.py` | 1 | 5 min | None | None | N/A | None | # REVIEW-STRUCT: Target PySpark script migrated from Ab Initio graph `map_rpos_carmen_import`. |

---

## 6. Task Dependency Map
As a single-task DAG, the execution chain is linear:
```python
rpos_carm_import
```

---

## 7. Sync / Concurrency Analysis
No sync keys, mutual exclusion locks, or concurrency pools are defined for this object.
* **Recommendation:** Standard single-concurrency protection is applied using `max_active_runs=1` on the DAG.

---

## 8. Error Handling and Retry Strategy
* **Retry Count:** Configured to retry once (`retries=1`) with a 5-minute interval (`retry_delay=timedelta(minutes=5)`).
* **Failure Trigger Rules:** Relies on default Airflow behavior (`all_success`), requiring all tasks to complete successfully.

---

## 9. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'RPOS_CARM_IMPORT'` | Set as a labels parameter in Dataproc job metadata |
| `$HOME/aktuell/abinitio/cfg/...` | Ab Initio Configuration | Embedded or parameterized within the PySpark script/GCS runtime configuration |

---

## 10. Developer Notes
* **GCP Infrastructure Placeholders:** The GCS bucket (`YOUR_BUCKET_NAME`), GCP Project ID (`YOUR_PROJECT_ID`), Dataproc cluster name (`YOUR_CLUSTER_NAME`), and region (`YOUR_REGION`) must be replaced with target cloud environment parameters.
* **# REVIEW-STRUCT: Unresolved Calling Context:** The parent Job Plan (JOBP) is missing from this extraction. This job has been wrapped into a standalone Airflow DAG. Confirm upstream dependencies in the master workflow scheduler.
* **Ab Initio Migration Path:** The job has been mapped from an Ab Initio Unix launcher (`r_ai_start`) to a Google Cloud Dataproc PySpark job operator, assuming a prior conversion of the Ab Initio graph to PySpark.

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# REVIEW: Configure these variables with your cloud environment targets
GCP_PROJECT_ID = "YOUR_PROJECT_ID"
GCP_REGION = "YOUR_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# REVIEW-STRUCT: Parent JOBP not supplied; wrapped inside a standalone DAG.
with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=DEFAULT_ARGS,
    description="Wrapper DAG for migrated Ab Initio graph map_rpos_carmen_import",
    schedule_interval=None,
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "abinitio", "rpos"],
) as dag:

    # ── Task: rpos_carm_import ───────────────────────────
    # Migrated from UC4 JOBS_UNIX object: DW.RPOS_CARM_IMPORT
    # Launcher type: abinitio_graph (r_ai_start)
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT",
            ],
        },
        "labels": {
            "uc4_object_name": "dw_rpos_carm_import",
            "job_kennung": "rpos_carm_import"
        }
    }

    rpos_carm_import = DataprocSubmitJobOperator(
        task_id="rpos_carm_import",
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-step job, no upstream/downstream dependencies inside this DAG context.
    rpos_carm_import
```
```

---

## 6. Target File Plan & Implementation-Ready DAG

In accordance with the **FOLDER INTEGRITY RULE**, the target file resides in the exact mirrored path. 

### Target File: `dags/abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import_dag.py`
This production-ready implementation resolves all infrastructure placeholders dynamically using Airflow variables to adhere strictly to the environment isolation policy, avoiding hardcoded strings.

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── Dynamic Environment Variables ────────────────────────
# Global dynamic configurations loaded from Airflow Variable Store
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Job-Specific Parameters ──────────────────────────────
JOB_CONFIG = {
    "DWH_JOB_KENNUNG": "RPOS_CARM_IMPORT",
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Quellverzeichnis": f"gs://{GCS_BUCKET}/crs/work/",
    "BHB_Zielverzeichnis": f"gs://{GCS_BUCKET}/crs/store/",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X",
}

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# Mirrored Orchestration of DW.RPOS_CARM_IMPORT
with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=DEFAULT_ARGS,
    description="Job startet AbInitio Graph  map_rpos_carmen_import", # OUTPUT/PRINT LITERAL RULE: Verbatim German title preserved
    schedule_interval=None,
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "abinitio", "rpos", "carmen"],
) as dag:

    # ── Task: rpos_carm_import ───────────────────────────
    # Submits the migrated PySpark script representing map_rpos_carmen_import.mp
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", JOB_CONFIG["DWH_JOB_KENNUNG"],
                "--project_dir", JOB_CONFIG["BHB_Projektverzeichnis"],
                "--version", JOB_CONFIG["BHB_Version"],
                "--graph", JOB_CONFIG["BHB_Graph"],
                "--process_type", JOB_CONFIG["BHB_Prozesstyp"],
                "--source_dir", JOB_CONFIG["BHB_Quellverzeichnis"],
                "--target_dir", JOB_CONFIG["BHB_Zielverzeichnis"],
                "--file_mask", JOB_CONFIG["BHB_Dateimaske"],
                "--header_id", JOB_CONFIG["BHB_Kopfdatensatzkennung"],
                "--data_id", JOB_CONFIG["BHB_Nutzdatensatzkennung"],
                "--trailer_id", JOB_CONFIG["BHB_Endedatensatzkennung"]
            ],
        },
        "labels": {
            "uc4_object_name": "dw_rpos_carm_import",
            "job_kennung": "rpos_carm_import"
        }
    }

    rpos_carm_import = DataprocSubmitJobOperator(
        task_id="rpos_carm_import",
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    rpos_carm_import
```

---

## 7. Risks & Manual Actions
* **Missing Parent Plan (JOBP):** The UC4 extraction does not include the master schedule or job chain. A manual task remains to verify upstream orchestration triggers.
* **GCS Path Structure Verification:** Verify that input files matching `CARMEN_B_*_pos.fix` are placed in the migrated GCS directory `gs://{GCS_BUCKET}/crs/work/` ahead of triggering this task.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

## 1. EXECUTIVE SUMMARY & DISPOSITION

This migration design document covers the transformation of the legacy Ab Initio graph `map_rpos_carmen_import` into a PySpark pipeline optimized for Dataproc Serverless and BigQuery. The graph is responsible for reading, validating, joining, filtering, and importing commercial billing/factoring transaction records into multiple Data Warehouse (DWH) tables under a full-refresh reload cycle (DELETE-then-INSERT).

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | PySpark application mapping the complete Ab Initio data flow, filtering, historical joining, and target persistence logic. |

---

## 2. TARGET DESIGN & ORCHESTRATION CONTEXT

### Scheduling & Execution Order
In the legacy environment, the execution of this job follows a multi-step sequence managed via UC4 and KornShell wrappers:
1. **UC4 Job Definition** (`DW.RPOS_CARM_IMPORT.xml`) schedules and starts the pipeline.
2. **Configuration Loader** (`map_rpos_carmen_import.cfg`) exports environment settings and processing constants.
3. **KornShell Script** (`map_rpos_carmen_import.ksh`) acts as the primary runtime execution wrapper.
4. **Ab Initio Graph** (`map_rpos_carmen_import.mp`) executes the data-processing pipeline.

In the target platform (Google Cloud Platform), this orchestration is modernized using **Cloud Composer (Airflow)** and **Dataproc Serverless (PySpark)**:
- **Cloud Composer Orchestration**: A central Airflow DAG represents the UC4 schedule.
- **Dataproc PySpark Operators**: An Airflow operator submits the migrated PySpark script (`map_rpos_carmen_import.py`) as a Serverless batch job.
- **Execution Order Mapping**:
  - Task 1: Check/Sensor for incoming file in GCS matching mask `CARMEN_B_*_pos.fix` (replacing legacy `BHB_Dateimaske`).
  - Task 2: Invoke Dataproc Serverless job running `map_rpos_carmen_import.py`.
  - Task 3: Move processed file from staging to archive path.

### Upstream and Downstream Job Dependencies
- **Upstream Shared Files**: 
  - `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` is already migrated and merged into the target environment (PR #764). The target PySpark pipeline should import or reference utility helpers from this merged library for any common startup/initialization routines.
- **Human-Confirmed Resolutions**:
  - The following components have been reviewed and determined as **Not Needed** for migration. They are deprecated or superseded by standard GCP / Spark native operators:
    - `.CCR_INIT`
    - `.DW_INIT`
    - `AB_CATALOG_FUNCTIONS.KSH`
    - `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`
    - `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`
    - `DW.HOLE_PFAD`
    - `DW.LESE_LOG`
    - `H_ALIS_DATE.KSH`
    - `H_ALIS_DATENOBJEKT.KSH`
    - `H_ALIS_MELDUNGEN.KSH`
    - `H_ALIS_PARAMETER.KSH`

### Lineage & External System Replacements
- **File System**: Legacy localized files under Unix storage are relocated to Google Cloud Storage (GCS) buckets.
- **Database Storage**: Oracle database schemas are mapped directly to Google BigQuery datasets.
- **Cross-File Dependencies**: The job reads metadata/contracts from `dwh_ta_c_vertrag` and manages target load boundaries.

---

## 3. ENVIRONMENT-SPECIFIC VALUES & CONFIGURATIONS

The following configuration parameters must be externalized and resolved at runtime. No hardcoded environment paths, project IDs, or connection credentials are permitted.

| Legacy Variable | Target Name | Classification | Resolution Mechanism | Purpose / Usage |
| :--- | :--- | :--- | :--- | :--- |
| `DB_TNS_NAME_DWH` | `GCP_PROJECT` | GLOBAL | Airflow Variable / `os.environ` | Target GCP project containing destination BigQuery datasets. |
| - | `BQ_DATASET` | GLOBAL | Airflow Variable / `os.environ` | Target BigQuery dataset (e.g. `dwh_dataset`). |
| `DW_DIR_IMP_SAP` | `GCS_BUCKET` | GLOBAL | Airflow Variable / `os.environ` | Root cloud storage bucket where data files are uploaded. |
| `BHB_Quellverzeichnis` | `BHB_Quellverzeichnis` | JOB-SPECIFIC | Job Parameter / Config Dict | Path to staging folder: `gs://{GCS_BUCKET}/crs/work/`. |
| `BHB_Zielverzeichnis` | `BHB_Zielverzeichnis` | JOB-SPECIFIC | Job Parameter / Config Dict | Path to archive folder: `gs://{GCS_BUCKET}/crs/store/`. |
| `BHB_Dateimaske` | `BHB_Dateimaske` | JOB-SPECIFIC | Job Parameter / Config Dict | Mask used to identify execution targets: `CARMEN_B_*_pos.fix`. |
| `BHB_Kopfdatensatzkennung` | `BHB_Kopfdatensatzkennung` | JOB-SPECIFIC | Job Parameter / Config Dict | File row identifier for header data (`H`). |
| `BHB_Nutzdatensatzkennung` | `BHB_Nutzdatensatzkennung` | JOB-SPECIFIC | Job Parameter / Config Dict | File row identifier for transaction payload rows (`P`). |
| `BHB_Endedatensatzkennung` | `BHB_Endedatensatzkennung` | JOB-SPECIFIC | Job Parameter / Config Dict | File row identifier for operational footer (`X`). |

---

## 4. VERBATIM MCP TRANSLATION & ANALYSIS

Below is the complete, unmodified output from the primary conversion tool `abinitio_design_pyspark`:

```text
GRAPH: tmpjbw5fv1j

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


# 1. GRAPH OVERVIEW

The overall purpose of this graph is to load, validate, process, and route commercial invoice position data (RPOS CARM) from an incoming CSV data file into various database tables based on business type rules. 

Specifically, the graph:
- Reads an incoming CSV file containing transaction payload records and a processing summary footer.
- Validates the payload records and splits them based on payload vs. footer records.
- Joins payload records with contract history data (`dwh$ta_c_vertrag`) to append operational context, resolving historical records via specific date ranges.
- Filters and splits validated records into four business streams based on business transaction indicators: Factoring Invoices, Factoring Credit Notes (Gutschriften), Reselling, or Temporary position data.
- Executes a full-refresh reload cycle (DELETE matching keys before INSERT) on five target destination tables.
- Parses metadata footer records to update file logging and registration tables (`DWH$TA_K_MELDUNGEN`, `DWH$TA_K_RECH_ABSGRP`).

---

# 2. SOURCES

### dwh$ta_c_vertrag
- **Label**: `dwh$ta_c_vertrag`
- **Kind**: select
- **SQL**:
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

### DWH$TA_F_RPOS_CARM (Source 1)
- **Label**: `DWH$TA_F_RPOS_CARM`
- **Kind**: select
- **SQL**:
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM
```

### DWH$TA_F_RPOS_CARM (Source 2)
- **Label**: `DWH$TA_F_RPOS_CARM-2`
- **Kind**: select
- **SQL**:
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM
```

### DWH$TA_F_RPOS_FACT_CARM (Source 1)
- **Label**: `DWH$TA_F_RPOS_FACT_CARM`
- **Kind**: select
- **SQL**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM
```

### DWH$TA_F_RPOS_FACT_CARM (Source 2)
- **Label**: `DWH$TA_F_RPOS_FACT_CARM - 2`
- **Kind**: select
- **SQL**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM
```

### DWH$TA_F_RPOS_RESELLING_CARM (Source 1)
- **Label**: `DWH$TA_F_RPOS_RESELLING_CARM`
- **Kind**: select
- **SQL**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM
```

### DWH$TA_F_RPOS_RESELLING_CARM (Source 2)
- **Label**: `DWH$TA_F_RPOS_RESELLING_CARM-1`
- **Kind**: select
- **SQL**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
```

### Read File (Incoming CSV Source File)
- **Label**: `Read File`
- **Kind**: file
- **Path**: Path extracted via parent variable `/path/to/incoming/csv_file`

---

# 3. TRANSFORMS

### Reformat rechnung_datum to datetime for Delete
- **Type**: `reformat`
- **Expression**:
```cozy
out.* :: in.*;
```
- **Description**: Standard pass-through mapping used to align variables before a delete cycle.

### Validate Records
- **Type**: `reformat`
- **Expression**:
```cozy
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
- **Description**: Validates schema compliance of decimal/numeric fields in the input record, throwing a hard error if invalid formatting is found.

### replace ',' by '.'
- **Type**: `reformat`
- **Expression**:
```cozy
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
```
- **Description**: Sanitizes incoming line metadata by converting European decimal commas into standard points.

### Reformat Referencerecord
- **Type**: `reformat`
- **Expression**:
```cozy
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: in.datensatz_rest;
```
- **Description**: Simple structure mapping of key and payload data.

### Reformat for delete (Port 1 & 2)
- **Type**: `reformat`
- **Expression**:
```cozy
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
```
- **Description**: Isolates primary key fields necessary for locating and deleting target records.

### Proof Join - criterias gueltig_von and gueltig_bis
- **Type**: `reformat`
- **Expression**:
```cozy
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
- **Description**: Nullifies contract details if the transaction's month-end date falls outside the valid range specified by `gueltig_von` and `gueltig_bis`.

### Reformat for insert "fact data"
- **Type**: `reformat`
- **Expression**:
```cozy
  out.* :: in.*;
  out.rahmenvertrag :: in.rahmenvertrag_id;
```
- **Description**: Prepares transactional factual data by mapping `rahmenvertrag_id` to its target database name `rahmenvertrag`.

### Reformat for insert "temporary data"
- **Type**: `reformat`
- **Expression**:
```cozy
  let datetime("YYYYMMDDHH24MISS") mindate =(datetime('YYYYMMDDHH24MISS'))(string(14))'19000101000000';

  out.* :: in.*;
  out.bearbeitung_datum :: mindate;
```
- **Description**: Prepares temporary positions for insertions, setting metadata column `bearbeitung_datum` to standard epoch timestamp `1900-01-01 00:00:00`.

### Proof Join-criteriase gueltig_von and gueltig_bis
- **Type**: `reformat`
- **Expression**:
```cozy
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
- **Description**: Alternate/paired validity proof join operation matching specific contract records.

### Reformat for insert "Factoring Gutschriften"
- **Type**: `reformat`
- **Expression**:
```cozy
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rahmenvertrag :: in.rahmenvertrag_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
```
- **Description**: Limits `rech_leistung_id_carm` to 9 characters and assigns `rahmenvertrag_id` as the final insert attribute `rahmenvertrag`.

### Reformat for insert "Factoring Rechnungen"
- **Type**: `reformat`
- **Expression**:
```cozy
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
```
- **Description**: Standardizes Factoring Invoices attributes, applying a length constraint of 9 to the item code.

### Reformat for insert "Reselling"
- **Type**: `reformat`
- **Expression**:
```cozy
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
```
- **Description**: Standardizes Reselling items formatting for final ingestion.

### Reformat Enderecord for Processing
- **Type**: `reformat`
- **Expression**:
```cozy
  out.kennzeichen :: in.kennzeichen;
  out.bemerkung :: in.bemerkung;
  out.stichtag :: in.stichtag;
  out.anzahl :: in.anzahl;
  out.inhalt :: in.inhalt;
  out.erstellt_am :: (string_index(in.erstellt_am, ";") == 0) ? in.erstellt_am : string_substring(in.erstellt_am, 1, string_length(in.erstellt_am)-1);
```
- **Description**: Extracts and cleans the trailer/footer attributes, removing trailing semicolons if present in metadata attributes.

### Reformat for DB and Filter out where Kompl_Kennzeichen != L
- **Type**: `reformat`
- **Expression**:
```cozy
  out.monats_id :: (string(6))(date("YYYYMM"))date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1);
  out.abs_grp :: string_substring(in.bemerkung,10,5) ;
  out.dateiname :: in.bemerkung;
  out.rechnung_datum :: (date("YYYYMMDD")) in.stichtag;
  out.rechnungsteil :: (string(1))"P";
  out.ladedatum :: now();
```
- **Description**: Generates database run logging attributes, shifting target billing month (`monats_id`) one month back from the reporting deadline.

---

# 4. IN-MEMORY LOOKUPS

*(No in-memory lookups were extracted from this graph config)*

---

# 5. FILTERS (select_expr)

### Filter by Expression (Rabatt Filter)
- **Label**: `Filter by Expression`
- **Expression**: `rech_leistung_id_carm == "RABATT"`
- **Effect**: Retains only rows representing active discounts.

### Split Data
- **Label**: `Split Data`
- **Expression**: `kennzeichen == "${BHB_Nutzdatensatzkennung}"`
- **Effect**: Routes payload records containing actual business data onwards.

### Filter by Expression (Delete filter)
- **Label**: `Filter by Expression`
- **Expression**: `delete_flag == 1`
- **Effect**: Filters downstream pipeline datasets to locate elements explicitly flagged for deletion.

### Select "Positionen auf Debitorenebene" (temporary Data)
- **Label**: `Select "Positionen auf Debitorenebene" (temporary Data)`
- **Expression**: `typ == 'T'`
- **Effect**: Filters and redirects temporary items to designated target structures.

### Select "Factoring Gutschriften"
- **Label**: `Select "Factoring Gutschriften"`
- **Expression**: `rpos_geschaftsform_kenn == 'G'`
- **Effect**: Routes business transaction positions containing credit indicators to factoring targets.

### Select "Factoring Rechnungen"
- **Label**: `Select "Factoring Rechnungen"`
- **Expression**: `rpos_geschaftsform_kenn == 'F'`
- **Effect**: Isolates traditional invoice items from other processing formats.

### Select "Reselling"
- **Label**: `Select "Reselling"`
- **Expression**: `rpos_geschaftsform_kenn == 'R'`
- **Effect**: Isolates reselling activities from traditional commercial invoices.

### Split Metadata
- **Label**: `Split Metadata`
- **Expression**: `kennzeichen == "${BHB_Endedatensatzkennung}"`
- **Effect**: Isolates files metadata footer row for control validation.

---

# 6. OUTPUT TARGETS

### Paired Reload Operations (Full Refresh Pattern)

The following tables follow a paired **DELETE-then-INSERT** reload sequence. For each paired target, rows matching the incoming keys (isolated via `Reformat for delete` / `Reformat`) are deleted from the destination database tables prior to inserting the new record batches.

#### Paired Target 1: dwh_ta_f_rpos_carm
- **Label**: `DWH$TA_F_RPOS_CARM`
- **Kind**: delete + insert
- **Table**: `dwh_ta_f_rpos_carm`
- **Execution Order**: Delete matching keys before performing insertions.
- **SQL (Delete)**:
```sql
DELETE FROM dwh_ta_f_rpos_carm 
WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm) IN (
  SELECT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm FROM incoming_deletes
)
```
- **SQL (Insert)**: Explicit insert of processed payload streams.

#### Paired Target 2: dwh_ta_f_rpos_fact_carm
- **Label**: `DWH$TA_F_RPOS_FACT_CARM`
- **Kind**: delete + insert
- **Table**: `dwh_ta_f_rpos_fact_carm`
- **Execution Order**: Delete matching keys before performing insertions.
- **SQL (Delete)**:
```sql
DELETE FROM dwh_ta_f_rpos_fact_carm 
WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm) IN (
  SELECT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm FROM incoming_deletes
)
```
- **SQL (Insert)**: Ingestion of Factoring Invoice records.

#### Paired Target 3: dwh_ta_f_gpos_fact_carm
- **Label**: `DWH$TA_F_GPOS_FACT_CARM`
- **Kind**: delete + insert
- **Table**: `dwh_ta_f_gpos_fact_carm`
- **Execution Order**: Delete matching keys before performing insertions.
- **SQL (Delete)**:
```sql
DELETE FROM dwh_ta_f_gpos_fact_carm 
WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm) IN (
  SELECT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm FROM incoming_deletes
)
```
- **SQL (Insert)**: Ingestion of Factoring Credit Notes (Gutschriften).

#### Paired Target 4: dwh_ta_f_rpos_reselling_carm
- **Label**: `DWH$TA_F_RPOS_RESELLING_CARM`
- **Kind**: delete + insert
- **Table**: `dwh_ta_f_rpos_reselling_carm`
- **Execution Order**: Delete matching keys before performing insertions.
- **SQL (Delete)**:
```sql
DELETE FROM dwh_ta_f_rpos_reselling_carm 
WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm) IN (
  SELECT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm FROM incoming_deletes
)
```
- **SQL (Insert)**: Ingestion of Reselling records.

#### Paired Target 5: dwh_ta_t_rpos_carm
- **Label**: `DWH$TA_T_RPOS_CARM`
- **Kind**: delete + insert
- **Table**: `dwh_ta_t_rpos_carm`
- **Execution Order**: Delete matching keys before performing insertions.
- **SQL (Delete)**:
```sql
DELETE FROM dwh_ta_t_rpos_carm 
WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm) IN (
  SELECT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm FROM incoming_deletes
)
```
- **SQL (Insert)**: Ingestion of Temporary transaction entries.

---

### Update Operational Targets

#### Update DWH$TA_K_MELDUNGEN
- **Label**: `Update DWH$TA_K_MELDUNGEN`
- **Kind**: update
- **Table**: `dwh$ta_k_meldungen`
- **SQL Statement**:
```sql
update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
```

#### Update / Insert DWH$TA_K_RECH_ABSGRP
- **Label**: `Update / Insert DWH$TA_K_RECH_ABSGRP`
- **Kind**: update
- **Table**: `DWH$TA_K_RECH_ABSGRP`
- **SQL Statement**:
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

# 7. DB JOINS

*(No external Parameterized DB joins found in the extraction)*

---

# 8. BUSINESS SUMMARY

1. **Source Parsing and File Split**: The graph ingests an incoming CSV data stream. It replaces decimal commas with points, and splits the stream into payload transaction records (where `kennzeichen` matches the system-configured `BHB_Nutzdatensatzkennung`) and operational file footer metadata (where `kennzeichen` matches `BHB_Endedatensatzkennung`).
2. **Contract History Synchronization**: Payload data is integrated with the historical master ledger `dwh$ta_c_vertrag`. To handle overlapping valid-to and valid-from dates, the master ledger is grouped by key and ranked, taking the most recent version. A chronological validation check (`Proof Join`) confirms whether the transaction month-end date fits within the active window of the assigned contract history range. If valid, contract mappings are retained; if invalid, they are set to null.
3. **Business Stream Categorization**: Validated transactions are categorized based on indicators:
   - Records with `typ == 'T'` are identified as temporary.
   - Factoring indicators `rpos_geschaftsform_kenn` classify transactions as Gutschriften ('G') or Rechnungen ('F').
   - Reselling indicators classify transactions as 'R'.
   - Remaining elements flow to general commercial outputs.
4. **Target Clearance and Full-Refresh Loading**: To ensure data consistency and prevent duplication, a full-refresh reload cycle is triggered: matching keys are isolated from the processed records, deleted from the active target tables (`dwh_ta_f_rpos_carm`, `dwh_ta_f_rpos_fact_carm`, `dwh_ta_f_gpos_fact_carm`, `dwh_ta_f_rpos_reselling_carm`, `dwh_ta_t_rpos_carm`), and then the freshly computed entries are appended.
5. **Metadata Verification and Control Ingestion**: Concurrently, the operational footer metadata is parsed to extract record tallies, creation dates, and descriptions. These metrics are persisted to audit targets `dwh$ta_k_meldungen` and `DWH$TA_K_RECH_ABSGRP` to log the process run execution details.

---

# SPARK SQL PSEUDOCODE OUTLINE

```python
# Setup BigQuery Sources
df_c_vertrag_src = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_c_vertrag")
df_rpos_carm_src = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_carm")
df_rpos_fact_src = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_fact_carm")
df_rpos_reselling_src = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_reselling_carm")

# Read Input CSV file
df_raw_file = spark.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", ";") \
    .load("/path/to/incoming/csv_file")

df_raw_file.createOrReplaceTempView("raw_file_view")

# Step 1: replace ',' by '.' to sanitize decimals
df_sanitized = spark.sql("""
    SELECT 
        _c0 AS kennzeichen,
        replace(_c1, ',', '.') AS datensatz_rest,
        _c2 AS monats_id,
        _c3 AS rechnung_datum,
        _c4 AS standardvertrags_id,
        _c5 AS vertrags_id,
        _c6 AS rech_leistung_id_carm,
        _c7 AS rechpos_brutto_eur,
        _c8 AS rechpos_netto_eur,
        _c9 AS rechpos_mwst_eur,
        _c10 AS rpos_geschaftsform_kenn,
        _c11 AS typ,
        _c12 AS stichtag,
        _c13 AS bemerkung,
        _c14 AS anzahl,
        _c15 AS inhalt,
        _c16 AS erstellt_am
    FROM raw_file_view
""")
df_sanitized.createOrReplaceTempView("sanitized_view")

# Step 2: Split Payload and Metadata Trailer
df_payload = spark.sql("""
    SELECT * FROM sanitized_view 
    WHERE kennzeichen = '${BHB_Nutzdatensatzkennung}'
""")
df_payload.createOrReplaceTempView("payload_view")

df_trailer = spark.sql("""
    SELECT * FROM sanitized_view 
    WHERE kennzeichen = '${BHB_Endedatensatzkennung}'
""")
df_trailer.createOrReplaceTempView("trailer_view")

# Step 3: Validate Records on Payload
df_validated = spark.sql("""
    SELECT 
        CASE WHEN monats_id IS NULL THEN raise_error("Invalid data format in monats_id") ELSE monats_id END AS monats_id,
        CASE WHEN rechnung_datum IS NULL THEN raise_error("Invalid data format in rechnung_datum") ELSE rechnung_datum END AS rechnung_datum,
        CASE WHEN standardvertrags_id IS NULL THEN raise_error("Invalid data format in standardvertrags_id") ELSE standardvertrags_id END AS standardvertrags_id,
        CASE WHEN vertrags_id IS NULL THEN raise_error("Invalid data format in vertrags_id") ELSE vertrags_id END AS vertrags_id,
        CASE WHEN rechpos_brutto_eur IS NULL THEN raise_error("Invalid data format in rechpos_brutto_eur") ELSE rechpos_brutto_eur END AS rechpos_brutto_eur,
        CASE WHEN rechpos_netto_eur IS NULL THEN raise_error("Invalid data format in rechpos_netto_eur") ELSE rechpos_netto_eur END AS rechpos_netto_eur,
        CASE WHEN rechpos_mwst_eur IS NULL THEN raise_error("Invalid data format in rechpos_mwst_eur") ELSE rechpos_mwst_eur END AS rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ
    FROM payload_view
""")
df_validated.createOrReplaceTempView("validated_view")

# Step 4: Process and Dedup Master Contract Ledger (dwh_ta_c_vertrag)
df_c_vertrag_filtered = df_c_vertrag_src.filter("gueltig_bis >= to_date('20050401', 'yyyyMMdd')")
df_c_vertrag_filtered.createOrReplaceTempView("c_vertrag_raw")

df_contract_ranked = spark.sql("""
    SELECT *,
           row_number() OVER (
               PARTITION BY vertrag_id_carmen 
               ORDER BY gueltig_von DESC, dwh_vertrag_id DESC
           ) as rankindex
    FROM c_vertrag_raw
""")
df_contract_ranked.createOrReplaceTempView("contract_ranked_view")

df_contract_dedup = spark.sql("""
    SELECT * FROM contract_ranked_view WHERE rankindex = 1
""")
df_contract_dedup.createOrReplaceTempView("contract_dedup_view")

# Step 5: Join Payload with Deduped Master Ledger and Apply Proof Join Date Ranges
df_joined_payload = spark.sql("""
    SELECT 
        p.*,
        v.rahmenvertrag_id,
        v.dwh_vertrag_id,
        v.dwh_gp_id,
        v.dwh_konto_id,
        v.dwh_tarifgr_id,
        v.vo_kenn,
        v.zv_id,
        v.gueltig_von,
        v.gueltig_bis
    FROM validated_view p
    LEFT JOIN contract_dedup_view v ON p.vertrags_id = v.vertrag_id_carmen
""")
df_joined_payload.createOrReplaceTempView("joined_payload_view")

# Apply proof validation ranges mapping
df_proofed_payload = spark.sql("""
    SELECT 
        j.*,
        CASE 
            WHEN (j.gueltig_von IS NULL OR last_day(to_date(j.monats_id, 'yyyyMM')) > j.gueltig_von)
                 AND (j.gueltig_bis IS NULL OR last_day(to_date(j.monats_id, 'yyyyMM')) <= j.gueltig_bis)
            THEN j.rahmenvertrag_id 
            ELSE NULL 
        END AS checked_rahmenvertrag_id
    FROM joined_payload_view j
""")
df_proofed_payload.createOrReplaceTempView("proofed_payload_view")

# Step 6: Split Payload into Destination Streams
# Stream A: Factoring Gutschriften (G)
df_factoring_g = spark.sql("""
    SELECT *, 
           substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm_short,
           checked_rahmenvertrag_id AS rahmenvertrag
    FROM proofed_payload_view
    WHERE rpos_geschaftsform_kenn = 'G'
""")
df_factoring_g.createOrReplaceTempView("factoring_g_view")

# Stream B: Factoring Rechnungen (F)
df_factoring_f = spark.sql("""
    SELECT *,
           substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm_short,
           checked_rahmenvertrag_id AS rahmenvertrag
    FROM proofed_payload_view
    WHERE rpos_geschaftsform_kenn = 'F'
""")
df_factoring_f.createOrReplaceTempView("factoring_f_view")

# Stream C: Reselling (R)
df_reselling = spark.sql("""
    SELECT *,
           substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm_short,
           checked_rahmenvertrag_id AS rahmenvertrag
    FROM proofed_payload_view
    WHERE rpos_geschaftsform_kenn = 'R'
""")
df_reselling.createOrReplaceTempView("reselling_view")

# Stream D: Temporary positions
df_temp_data = spark.sql("""
    SELECT *,
           to_timestamp('19000101000000', 'yyyyMMddHHmmss') AS bearbeitung_datum
    FROM proofed_payload_view
    WHERE typ = 'T'
""")
df_temp_data.createOrReplaceTempView("temp_data_view")

# Stream E: General RPOS CARM Ingestion
df_general_carm = spark.sql("""
    SELECT *,
           checked_rahmenvertrag_id AS rahmenvertrag
    FROM proofed_payload_view
""")
df_general_carm.createOrReplaceTempView("general_carm_view")


# ==========================================
# PARED RELOAD OPERATIONS (DELETE THEN INSERT)
# ==========================================

# Target 1: dwh_ta_f_rpos_carm Reload Cycle
df_keys_to_delete_carm = spark.sql("""
    SELECT DISTINCT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm 
    FROM general_carm_view
""")
# Execute deletion logic on target dwh_ta_f_rpos_carm via Left Anti Join
df_active_target_carm = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_carm")
df_cleared_carm = df_active_target_carm.join(
    df_keys_to_delete_carm,
    on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
    how="leftanti"
)
df_final_insert_carm = df_cleared_carm.unionByName(df_general_carm, allowMissingColumns=True)
write_to_bq(df_final_insert_carm, "dwh_ta_f_rpos_carm")


# Target 2: dwh_ta_f_rpos_fact_carm Reload Cycle (Factoring Rechnungen)
df_keys_to_delete_fact_f = spark.sql("""
    SELECT DISTINCT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm 
    FROM factoring_f_view
""")
df_active_target_fact_f = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_fact_carm")
df_cleared_fact_f = df_active_target_fact_f.join(
    df_keys_to_delete_fact_f,
    on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
    how="leftanti"
)
df_final_insert_fact_f = df_cleared_fact_f.unionByName(df_factoring_f, allowMissingColumns=True)
write_to_bq(df_final_insert_fact_f, "dwh_ta_f_rpos_fact_carm")


# Target 3: dwh_ta_f_gpos_fact_carm Reload Cycle (Factoring Gutschriften)
df_keys_to_delete_fact_g = spark.sql("""
    SELECT DISTINCT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm 
    FROM factoring_g_view
""")
df_active_target_fact_g = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_gpos_fact_carm")
df_cleared_fact_g = df_active_target_fact_g.join(
    df_keys_to_delete_fact_g,
    on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
    how="leftanti"
)
df_final_insert_fact_g = df_cleared_fact_g.unionByName(df_factoring_g, allowMissingColumns=True)
write_to_bq(df_final_insert_fact_g, "dwh_ta_f_gpos_fact_carm")


# Target 4: dwh_ta_f_rpos_reselling_carm Reload Cycle (Reselling)
df_keys_to_delete_reselling = spark.sql("""
    SELECT DISTINCT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm 
    FROM reselling_view
""")
df_active_target_reselling = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_reselling_carm")
df_cleared_reselling = df_active_target_reselling.join(
    df_keys_to_delete_reselling,
    on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
    how="leftanti"
)
df_final_insert_reselling = df_cleared_reselling.unionByName(df_reselling, allowMissingColumns=True)
write_to_bq(df_final_insert_reselling, "dwh_ta_f_rpos_reselling_carm")


# Target 5: dwh_ta_t_rpos_carm Reload Cycle (Temporary Data)
df_keys_to_delete_temp = spark.sql("""
    SELECT DISTINCT rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm 
    FROM temp_data_view
""")
df_active_target_temp = spark.read.format("bigquery").load("BIGQUERY_SOURCE_DS.dwh_ta_t_rpos_carm")
df_cleared_temp = df_active_target_temp.join(
    df_keys_to_delete_temp,
    on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
    how="leftanti"
)
df_final_insert_temp = df_cleared_temp.unionByName(df_temp_data, allowMissingColumns=True)
write_to_bq(df_final_insert_temp, "dwh_ta_t_rpos_carm")


# ==========================================
# METADATA TRAILER / OPERATIONAL TARGETS
# ==========================================

# Step 7: Reformat Enderecord Metadata for processing
df_trailer_processed = spark.sql("""
    SELECT 
        kennzeichen,
        bemerkung AS dateiname,
        bemerkung AS zusatzinfo,
        stichtag,
        anzahl AS anzahl_ds_eof,
        inhalt AS enderecord_text,
        CASE 
            WHEN instr(erstellt_am, ';') = 0 THEN erstellt_am 
            ELSE substring(erstellt_am, 1, length(erstellt_am) - 1) 
        END AS erstellt_am
    FROM trailer_view
""")
df_trailer_processed.createOrReplaceTempView("trailer_processed_view")

# Step 8: Update Operational Run Logging Table (DWH$TA_K_MELDUNGEN)
df_meldungen_update = spark.sql("""
    SELECT 
        t.anzahl_ds_eof,
        t.dateiname,
        t.enderecord_text,
        t.zusatzinfo,
        m.entrynr
    FROM BIGQUERY_SOURCE_DS.dwh_ta_k_meldungen m
    CROSS JOIN trailer_processed_view t
""")
# Update logic writeback
write_to_bq(df_meldungen_update, "dwh_ta_k_meldungen")

# Step 9: Process and Save Run Logging (DWH$TA_K_RECH_ABSGRP)
df_absgrp_processed = spark.sql("""
    SELECT 
        date_format(add_months(to_date(substring(stichtag, 1, 6), 'yyyyMM'), -1), 'yyyyMM') AS monats_id,
        substring(dateiname, 10, 5) AS abs_grp,
        dateiname,
        to_date(stichtag, 'yyyyMMdd') AS rechnung_datum,
        'P' AS rechnungsteil,
        current_timestamp() AS ladedatum
    FROM trailer_processed_view
""")
df_absgrp_processed.createOrReplaceTempView("absgrp_processed_view")

write_to_bq(df_absgrp_processed, "DWH$TA_K_RECH_ABSGRP")
```

---

## 5. TARGET FILE PLAN

As dictated by the **Folder Integrity Rule**, the target folder structure mirrors the relative source path. Distinct source folders must not merge their outputs.

### Target Component Files

- **File Path**: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py`
  - **Language**: Python (PySpark)
  - **Primary Source**: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
  - **Description**: Contains the Spark session initialization, schema definitions, input CSV staging logic, record parsing/decimal translation, data splits, dates-proofing join on `dwh_ta_c_vertrag`, delete-anti-joins for the five target tables, and logging writebacks to `dwh_ta_k_meldungen` / `DWH$TA_K_RECH_ABSGRP`.

---

## 6. RISKS, MANUAL ACTIONS & MITIGATION

### 1. Hard Errors on Validation (Validation Crash Strategy)
- **Risk**: The legacy Ab Initio graph contains `force_error()` calls inside key validation routines (such as when `monats_id` or `vertrags_id` fail schema formatting). If unhandled, this could crash a production PySpark run mid-process.
- **Mitigation / Implementation Requirement**: In the PySpark translation, these validation filters must raise custom exceptions or call Spark SQL's `raise_error()` function. For robust auditing, invalid formatting errors must be captured inside the PySpark script, logged, and trigger a graceful task failure inside the Airflow orchestrator.

### 2. Output/Print Literal Constraint
- **Rule**: No validation messages, logs, or error strings are to be localized or translated.
- **Action**: Ensure that strings like `"Invalid data format in monats_id"`, `"Invalid data format in rechnung_datum"`, and `"Invalid data format in standardvertrags_id"` are translated into the PySpark codebase verbatim without any modifications to text, casing, or punctuation.

### 3. Date Validation Calculation Logic (Proof Join)
- **Risk**: The logic determining whether a transaction's month-end date is inside the contract active range is complex:
  `month_last_day = datetime_add(monats_id, date_month_end(...))`
- **Mitigation**: Translate this using native PySpark SQL date functions:
  `last_day(to_date(monats_id, 'yyyyMM'))` 
  Use standard BigQuery-compatible date evaluation constraints during execution to avoid off-by-one errors.

### 4. Database Deletions in BigQuery (DML Quota)
- **Risk**: BigQuery table mutations via continuous DML deletes/inserts are more expensive and slower than staging tables or partition swap patterns.
- **Mitigation**: Under high volume, a left-anti-join swap pattern (as shown in the Spark outline) is used. The script reads the target table, joins with payload keys to filter out records (anti-join), unions with new inserts, and overwrites the target table. Alternatively, partition-level drops should be implemented if target tables are partitioned by `rechnung_datum`. Ensure proper staging and table write transactions are managed via Cloud Composer task bounds.

---

# MIGRATION DESIGN DOCUMENT: `DW.RPOS_CARM_IMPORT`

## 1. FILE DISPOSITION TABLE

The following table lists every source file within this job scope and its planned target disposition. In accordance with the **FOLDER INTEGRITY RULE**, the target folder structure mirrors the source folder structure.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Migrates the legacy KornShell wrapper logic to Python 3. The target script validates runtime environment parameters, monitors input source file availability on GCS, and orchestrates/submits the Dataproc Serverless PySpark batch job replacing the legacy Ab Initio `.mp` graph pipeline. |

---

## 2. VERBATIM MCP TOOL OUTPUT
The following section is the exact, unaltered output of the `ksh_design_python` tool. Do not modify or restructure the pseudocode or translation logic below.

```markdown
=== FILE: abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh ===
# Design Document: Migration of `map_rpos_carmen_import.ksh` to Python 3

This document outlines the design for migrating the legacy KornShell (.ksh) execution script `map_rpos_carmen_import.ksh` (which wraps an Ab Initio Co-Operating System multi-phase graph pipeline) to a native, modern Python 3 script.

---

## 1. SCRIPT OVERVIEW

- **Purpose**: The script wraps the Ab Initio multi-phase graph `map_rpos_carmen_import` which parses, validates, filters, and loads legacy Carmen billing transaction files (RPOS) into an Oracle Data Warehouse (DWH).
- **Triggers**: Executed daily or monthly as part of batch billing pipelines, managed by a UC4 / Automic job scheduler.
- **Reads**: 
  - Source Carmen invoice/transaction billing CSV data files (configured via environment variables `BHB_Quellverzeichnis` and `BHB_Dateiname`).
  - Reference contract master data table `DWH$TA_C_VERTRAG` from the DWH database.
- **Writes**: 
  - Dynamic inserts/updates to auditing and load status control tables `DWH$TA_K_RECH_ABSGRP` and `DWH$TA_K_MELDUNGEN`.
  - Delete operations to clean existing runs in target fact/temporary tables (for idempotency).
  - Bulk loading of cleaned transaction records into multiple physical database tables: `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, and `DWH$TA_T_RPOS_CARM`.

---

## 2. INVOCATION CONTEXT

- **Caller**: Schedulers call this script via UC4/Automic using a UNIX Job object (e.g. `JOBS_UNIX.BHB_MAP_RPOS_CARMEN_IMPORT`).
- **Command Line / Arguments**: Schedulers invoke this script with optional positional parameters (e.g. `./map_rpos_carmen_import.ksh -reposit-tracking` or with runtime parameter overrides).
- **UC4 Includes**: No native UC4 inclusions (`:inc ...`) are present in this specific wrapper file.
- **Environment Files Sourced**:
  - `# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
  - `. ./${_AB_PROXY_DIR}/GDE-Parameters`
    - `# REVIEW-STRUCT: include [GDE-Parameters] body not supplied — behaviour unknown`
  - `.project.ksh` via:
    - `# REVIEW-STRUCT: include [.project.ksh] body not supplied — behaviour unknown`

---

## 3. PARAMETERS / INPUTS

The script references environment parameters for configuration, file locations, and database connections:

| Parameter Name | Source | Purpose | Used in Body? | Python Surface Strategy |
| :--- | :--- | :--- | :---: | :--- |
| `DB_TNS_NAME_DWH` | Env Var | Oracle DWH Database TNS Alias | Yes | `os.environ.get("DB_TNS_NAME_DWH")` |
| `DB_USER_DWH` | Env Var | Oracle DWH Database Username | Yes | `os.environ.get("DB_USER_DWH")` |
| `DB_PASSWD_DWH` | Env Var | Oracle DWH Database Password | Yes | `os.environ.get("DB_PASSWD_DWH")` |
| `DB_TNS_NAME_CRS` | Env Var | Oracle CRS Database TNS Alias | Yes | `os.environ.get("DB_TNS_NAME_CRS")` |
| `DB_USER_CRS` | Env Var | Oracle CRS Database Username | Yes | `os.environ.get("DB_USER_CRS")` |
| `DB_PASSWD_CRS` | Env Var | Oracle CRS Database Password | Yes | `os.environ.get("DB_PASSWD_CRS")` |
| `DB_TNS_NAME_SGM` | Env Var | Oracle SGM Database TNS Alias | Yes | `os.environ.get("DB_TNS_NAME_SGM")` |
| `DB_USER_SGM` | Env Var | Oracle SGM Database Username | Yes | `os.environ.get("DB_USER_SGM")` |
| `DB_PASSWD_SGM` | Env Var | Oracle SGM Database Password | Yes | `os.environ.get("DB_PASSWD_SGM")` |
| `DB_TNS_NAME_CADS` | Env Var | Oracle CADS Database TNS Alias | Yes | `os.environ.get("DB_TNS_NAME_CADS")` |
| `DB_USER_CADS` | Env Var | Oracle CADS Database Username | Yes | `os.environ.get("DB_USER_CADS")` |
| `DB_PASSWD_CADS` | Env Var | Oracle CADS Database Password | Yes | `os.environ.get("DB_PASSWD_CADS")` |
| `DB_TNS_NAME_CACM` | Env Var | Oracle CACM Database TNS Alias | Yes | `os.environ.get("DB_TNS_NAME_CACM")` |
| `DB_USER_CACM` | Env Var | Oracle CACM Database Username | Yes | `os.environ.get("DB_USER_CACM")` |
| `DB_PASSWD_CACM` | Env Var | Oracle CACM Database Password | Yes | `os.environ.get("DB_PASSWD_CACM")` |
| `BHB_Projektverzeichnis`| Env Var | Working Root Project Directory | Yes | `os.environ.get("BHB_Projektverzeichnis")` |
| `BHB_Graph` | Env Var | Executing Graph Identity | Yes | `os.environ.get("BHB_Graph")` |
| `BHB_Prozesstyp` | Env Var | Process type identification | Yes | `os.environ.get("BHB_Prozesstyp")` |
| `BHB_Eintragsnr` | Env Var | Auditing Log entry sequence number | Yes | `os.environ.get("BHB_Eintragsnr")` |
| `BHB_Quellverzeichnis` | Env Var | Raw file input source folder directory | Yes | `os.environ.get("BHB_Quellverzeichnis")` |
| `BHB_Zielverzeichnis` | Env Var | Processed file output archive directory | Yes | `os.environ.get("BHB_Zielverzeichnis")` |
| `BHB_Dateimaske` | Env Var | File name glob mask for source discovery | Yes | `os.environ.get("BHB_Dateimaske")` |
| `BHB_Kopfdatensatzkennung`| Env Var| Header identifier token in input CSV | Yes | `os.environ.get("BHB_Kopfdatensatzkennung")`|
| `BHB_Nutzdatensatzkennung`| Env Var| Main data row identifier token in CSV | Yes | `os.environ.get("BHB_Nutzdatensatzkennung")`|
| `BHB_Endedatensatzkennung`| Env Var| Footer identifier token in CSV | Yes | `os.environ.get("BHB_Endedatensatzkennung")`|
| `BHB_Dateiname` | Env Var | Source CSV base filename | Yes | `os.environ.get("BHB_Dateiname")` |
| `BHB_DB` | Env Var | Path reference pointing to `.dbc` profile | Yes | `os.environ.get("BHB_DB")` |
| `BHB_DML` | Env Var | Folder path containing standard physical DML metadata schemas | Yes | `os.environ.get("BHB_DML")` |
| `BHB_SAP_DML` | Env Var | Folder path containing Carmen RPOS metadata schemas | Yes | `os.environ.get("BHB_SAP_DML")` |
| Positional `$1`, `$2` | CLI Args | Optional positional execution flags (e.g. `-help`, `-reposit-tracking`) | Yes | parsed via `sys.argv` / `argparse` |

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED

The legacy wrapper interacts heavily with Ab Initio Co-Operating System commands:

| Command Line | Purpose | Subprocess or Native? | Resolvable Launcher? |
| :--- | :--- | :--- | :--- |
| `uname` | Operating System validation (`Windows_*`, `CYGWIN_*` or standard UNIX fallback). | Native Python (`platform.system()`). | N/A |
| `m_env -get ...` | Queries Ab Initio environment variable flags. | Native Python configuration variables. | N/A |
| `air sandbox find` | Determines path mappings inside Ab Initio Enterprise Meta-Environment (EME). | Sourced from static config or environment variable configurations. | No |
| `run-and-reposit` | Controls sandbox versions for compiled graphs in EME. | Obsoleted (all EME synchronization is replaced by Git/CI/CD in the modern pipeline). | No |
| `m_mkcatalog`, `m_rmcatalog`| Generates/Removes temporary operational catalogs for lookups. | Sourced as native Python dictionary lookups or temporary memory structures. | No |
| `mp` commands (`mp job`, `mp layout`, `mp metadata`, `mp straight-flow`, `mp run`) | Core execution runtime coordinating data partitions, pipeline maps, and loading. | **Must be replaced by native Python ETL processing logic** (`pandas`, `oracledb`). | No (does not qualify as a simple SQL wrapper launcher; it's a massive ETL multi-phase runtime). |

---

## 5. EMBEDDED SQL

The pipeline references several native SQL statement templates compiled to proxy directory files (`_AB_PROXY_DIR`) for database cleanup and final updates. 

### Dialect Identification
All database operations target Oracle database instances (`NLS_NUMERIC_CHARACTERS` environment checks, `to_date('20050401', 'YYYYMMDD')` constructs, and `(+)` Oracle-specific outer join syntax are present). 

### SQL Statement Registry

#### 1. Delete rows from `DWH$TA_F_RPOS_CARM` (from file `/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql`)
- **Statement Type**: `DELETE`
- **Tables Touched**: `DWH$TA_F_RPOS_CARM`
- **Text**:
```sql
DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### 2. Delete rows from `DWH$TA_F_GPOS_FACT_CARM` (from file `/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql`)
- **Statement Type**: `DELETE`
- **Tables Touched**: `DWH$TA_F_GPOS_FACT_CARM`
- **Text**:
```sql
DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### 3. Delete rows from `DWH$TA_F_RPOS_CARM` Alternative (from file `/Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql`)
- **Statement Type**: `DELETE`
- **Tables Touched**: `DWH$TA_F_RPOS_CARM`
- **Text**:
```sql
DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### 4. Delete rows from `DWH$TA_F_RPOS_FACT_CARM` (from file `/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql`)
- **Statement Type**: `DELETE`
- **Tables Touched**: `DWH$TA_F_RPOS_FACT_CARM`
- **Text**:
```sql
DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### 5. Delete rows from `DWH$TA_F_RPOS_RESELLING_CARM` (from file `/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql`)
- **Statement Type**: `DELETE`
- **Tables Touched**: `DWH$TA_F_RPOS_RESELLING_CARM`
- **Text**:
```sql
DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### 6. Delete rows from `DWH$TA_T_RPOS_CARM` (from file `/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql`)
- **Statement Type**: `DELETE`
- **Tables Touched**: `DWH$TA_T_RPOS_CARM`
- **Text**:
```sql
DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
```

#### 7. Update auditing ABSGRP details (from file `/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql`)
- **Statement Type**: `UPDATE`
- **Tables Touched**: `DWH$TA_K_RECH_ABSGRP`
- **Text**:
```sql
UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil
```

#### 8. Insert auditing ABSGRP details (from file `/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql`)
- **Statement Type**: `INSERT`
- **Tables Touched**: `DWH$TA_K_RECH_ABSGRP`
- **Text**:
```sql
INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)
```

#### 9. Update Job Control Message Audit (from file `/Update_DWH_TA_K_MELDUNGEN-74.sql`)
- **Statement Type**: `UPDATE`
- **Tables Touched**: `DWH$TA_K_MELDUNGEN`
- **Text**:
```sql
update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
```

#### 10. Sourced master reference query (`Insert_Routine.dwh_ta_c_vertrag__table_` lookup)
- **Statement Type**: `SELECT`
- **Tables Touched**: `dwh$ta_c_vertrag`
- **Text**:
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

---

## 6. CONTROL FLOW

The modern Python script must sequentially execute the following processing phases:

```
+-------------------------------------------------------------+
| 1. INITIALIZATION & PARAMETER VALIDATION                     |
|    - Load environment configuration                         |
|    - Verify inputs (source CSV, directories, DB env vars)   |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 2. CONNECT TO ORACLE DATABASE                               |
|    - Connect to DWH using oracledb Client                   |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 3. INGEST REFERENCE MASTER DATA                             |
|    - Read contract master (DWH$TA_C_VERTRAG) into memory    |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 4. READ & PREPROCESS CARMEN SOURCE FILE                     |
|    - Verify existence of input file                         |
|    - Stream file and isolate blocks (Header, Nutz, End)     |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 5. VALIDATION, ENRICHMENT & ETL TRANSFORMS                  |
|    - Parse fields, clean commas to decimals                 |
|    - Perform contract state temporal join (gueltig_von/bis) |
|    - Aggregate "RABATT" types (Rollups)                     |
|    - Separate data into target streams (Factoring/Reselling)|
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 6. TARGET DWH CLEANUP (IDEMPOTENCY STEP)                     |
|    - Isolate unique overlapping transactions keys          |
|    - Execute bulk parameter-driven DELETEs in target tables |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 7. PERSIST CLEAN DATA TO DWH TARGETS (BULK LOAD)            |
|    - Executemany batch inserts for loaded frames            |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 8. CONTROL AUDITING RECORDS STAGE                           |
|    - Update/Insert records in DWH$TA_K_RECH_ABSGRP          |
|    - Update DWH$TA_K_MELDUNGEN execution log                |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 9. TRANSACTION COMMIT & TEARDOWN                            |
|    - Commit database transactions                           |
|    - Clear dynamic local workspace traces                   |
+-------------------------------------------------------------+
```

---

## 7. ERROR HANDLING & EXIT CODES

- **Error Detection**:
  - Legacy validation checks return code `$mpjret` after `mp run` and parameters verification. If any check fails, it calls exit immediatley (`exit $mpjret`).
- **Python Translation Strategy**:
  - Wrap database connection blocks, parsing sequences, and batch loads inside standard Python `try...except...finally` block patterns.
  - Sourced connection issues (e.g. `oracledb.DatabaseError`) and missing transaction files must raise explicit fatal exceptions.
  - Subprocess calls (if wrapping minor legacy command utilities) must enforce `check=True` to raise `subprocess.CalledProcessError`.
  - Enforce cleanup of local files in the `finally:` block.
  - On standard normal execution, exit code is `0`. On caught exceptions, return explicit codes:
    - Parameter validation failures: Exit `1`
    - Ingestion/Missing files: Exit `2`
    - DB Connection/SQL execution errors: Exit `3`

---

## 8. OUTPUTS / SIDE EFFECTS

- **Database Table Inserts & Deletes**:
  - `DWH$TA_F_RPOS_CARM` (Data persistence)
  - `DWH$TA_F_GPOS_FACT_CARM` (Data persistence)
  - `DWH$TA_F_RPOS_FACT_CARM` (Data persistence)
  - `DWH$TA_F_RPOS_RESELLING_CARM` (Data persistence)
  - `DWH$TA_T_RPOS_CARM` (Data persistence)
- **Log / Auditing Databases Operations**:
  - `DWH$TA_K_RECH_ABSGRP` (Run auditing update)
  - `DWH$TA_K_MELDUNGEN` (Auditing entry synchronization)
- **Local Files**:
  - Temporary workspace folder created at execution start and deleted securely on success/error teardown.

---

## 9. BUSINESS SUMMARY

- **Carmen RPOS Integration**: Consumes monthly or batch transaction file deliveries of Carmen contract billing invoices.
- **Contract Synchronization**: Correlates Carmen invoices to corresponding company contract identifiers inside Oracle CRM master registers (`DWH$TA_C_VERTRAG`).
- **Idempotency Execution Rules**: Ensures safety from dual-runs by searching and wiping historical records of any invoices belonging to the identical billing period from tables before executing fresh loads.
- **Partitioned Load Architecture**: Splices records into billing category tables (Invoices, Credits, Reselling) based on transaction properties and business codes.
- **Reporting Integrity Tracking**: Maintains synchronization with general audit systems by updating operational batch numbers in the meldung control directories.

---

## Python 3 Translation Pseudocode Outline

```python
# Step 1: Imports and environment parameters checks
import os
import sys
import glob
import logging
import shutil
import tempfile
from datetime import datetime
import pandas as pd
import oracledb # Modern replacement for cx_Oracle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Step 2: Validate Parameter Environment
def validate_parameters():
    required_vars = [
        "DB_TNS_NAME_DWH", "DB_USER_DWH", "DB_PASSWD_DWH",
        "BHB_Quellverzeichnis", "BHB_Dateiname", "BHB_Eintragsnr",
        "BHB_Kopfdatensatzkennung", "BHB_Nutzdatensatzkennung", "BHB_Endedatensatzkennung"
    ]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logging.error(f"Missing required environment variables: {missing_vars}")
        sys.exit(1)

# Step 3: Run Main Execution Flow
def main():
    validate_parameters()
    
    # Configure oracle numeric environment
    os.environ["NLS_NUMERIC_CHARACTERS"] = ". "
    
    # Get parameters
    dwh_tns = os.environ.get("DB_TNS_NAME_DWH")
    dwh_user = os.environ.get("DB_USER_DWH")
    dwh_passwd = os.environ.get("DB_PASSWD_DWH")
    
    source_dir = os.environ.get("BHB_Quellverzeichnis")
    filename_base = os.environ.get("BHB_Dateiname")
    source_path = os.path.join(source_dir, filename_base)
    
    entry_nr = os.environ.get("BHB_Eintragsnr")
    
    # Temp workspace allocation
    temp_dir = tempfile.mkdtemp(prefix="map_rpos_carmen_import_")
    logging.info(f"Created secure processing workspace: {temp_dir}")
    
    connection = None
    try:
        # Step 4: Establish DB Session
        logging.info("Connecting to DWH Target Oracle Database...")
        connection = oracledb.connect(user=dwh_user, password=dwh_passwd, dsn=dwh_tns)
        cursor = connection.cursor()
        
        # Step 5: Read and Cache DWH Contract Master Data
        logging.info("Caching Contract Master Table reference (DWH$TA_C_VERTRAG)...")
        reference_query = """
            SELECT 
                rahmenvertrag_id, vertrag_id_carmen, dwh_vertrag_id, dwh_gp_id, 
                dwh_konto_id, dwh_tarifgr_id, vo_kenn, zv_id, gueltig_von, gueltig_bis
            FROM dwh$ta_c_vertrag
            WHERE gueltig_bis >= TO_DATE('20050401', 'YYYYMMDD')
        """
        cursor.execute(reference_query)
        df_vertrag = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        
        # Step 6: Load and Split Carmen Input Source
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Missing transaction billing file: {source_path}")
            
        header_indicator = os.environ.get("BHB_Kopfdatensatzkennung")
        data_indicator = os.environ.get("BHB_Nutzdatensatzkennung")
        footer_indicator = os.environ.get("BHB_Endedatensatzkennung")
        
        raw_data_rows = []
        footer_rows = []
        
        # Parse stream
        with open(source_path, "r", encoding="latin1") as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line.startswith(data_indicator):
                    raw_data_rows.append(stripped_line)
                elif stripped_line.startswith(footer_indicator):
                    footer_rows.append(stripped_line)
                    
        if not raw_data_rows:
            logging.warning("No data transactions (Nutz) rows located in file!")
            
        # Step 7: Parse and Structure Ingest Dataframe
        # Clean commas to periods, map formats
        parsed_records = []
        for raw_line in raw_data_rows:
            # Custom split logic replacing Ab Initio DML parsing.
            # Replace ',' to '.' inside decimal columns
            fields = raw_line.split(";") 
            # Implement validation schema parsing mapping to Reformat_for_DB details
            parsed_records.append(fields)
            
        df_raw = pd.DataFrame(parsed_records)
        # Perform transformations equivalent to `Reformat_for_DB-20.xfr` & `Validate_Records-22.xfr`
        # Execute rolling aggregations and business rule splits (Factoring, Reselling etc.)
        
        # Step 8: Idempotent Deletion Phase
        logging.info("Initiating historical target cleanups (Deletes)...")
        # Isolate unique deletion keys from structured records: rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id
        # Execute transactional batches:
        delete_sql = """
            DELETE FROM DWH$TA_F_RPOS_CARM
            WHERE rechnung_id = :1 AND rechnung_datum = :2 AND standardvertrags_id = :3 AND vertrags_id = :4
        """
        # Batch execute deletes: cursor.executemany(delete_sql, delete_keys_list)
        
        # Step 9: Bulk Persist Clean Target Frames
        # Insert target Factoring Fact table
        insert_fact_sql = """
            INSERT INTO DWH$TA_F_RPOS_CARM (monats_id, debitor_id, rechnung_id, ...) VALUES (...)
        """
        # Batch load execution: cursor.executemany(insert_fact_sql, insert_records_list)
        
        # Step 10: Control Auditing Table Updates
        logging.info("Updating Audit Track logs...")
        # Update run variables into DWH$TA_K_RECH_ABSGRP & DWH$TA_K_MELDUNGEN
        
        # Commit transactional states
        connection.commit()
        logging.info("Data workflow loaded successfully. Committed transaction.")
        
    except Exception as e:
        logging.error(f"Execution Error occurred: {str(e)}")
        if connection:
            logging.info("Rolling back open database transactions.")
            connection.rollback()
        sys.exit(3)
        
    finally:
        # Step 11: Workspace Teardown
        if connection:
            connection.close()
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.info("Secure workspace purged.")

if __name__ == "__main__":
    main()
```
```

---

## 3. BIGQUERY / GCP TARGET PLATFORM ADAPTATION

This section describes how the Oracle-specific and local-filesystem execution context from the legacy KornShell script maps into a GCP native stack (**Cloud Composer** orchestrating **Dataproc Serverless PySpark** pipelines and **BigQuery**).

### 3.1 Orchestration and Run Architecture
- **Legacy Framework**: KornShell script initialized variables, processed input text blocks, and executed an Ab Initio graph (`map_rpos_carmen_import.mp`) which performed high-throughput parsing and loading into Oracle.
- **GCP Target Architecture**:
  1. **Airflow Orchestrator**: A Cloud Composer DAG (migrated separately from the UC4 orchestration XML) triggers this job.
  2. **GCP Storage**: The input transaction billing files are stored in a GCS bucket instead of the local unix filesystem directory (`$DW_DIR_IMP_SAP/crs/work/`).
  3. **Dataproc Serverless PySpark**: The Python wrapper script (`map_rpos_carmen_import.py`) submits a PySpark batch application executing on Google Cloud Dataproc Serverless. This PySpark program encapsulates the data processing graph logic (migrated from the `.mp` graph file).
  4. **BigQuery target loading**: The PySpark script processes records in parallel, caches reference data from BigQuery tables, performs the joins and aggregations, and writes clean results using the Spark BigQuery connector.

### 3.2 SQL & Query Translation
Oracle SQL scripts and statements must be adapted to Standard BigQuery SQL:
- **Binding Variables**: Oracle syntax `:rechnung_id`, `:rechnung_datum` maps to named query parameters `@rechnung_id`, `@rechnung_datum` or BigQuery scripting variable substitutions.
- **Idempotency Deletion**: BigQuery supports `DELETE` syntax, but standard practice in BigQuery DWH pipelines uses a split/merge or partition overwriting mechanism. Given that transaction tables are likely partitioned by date, we can either:
  - Run a DML `DELETE` statement in BigQuery prior to appending the new batch.
  - Use `MERGE` statements or overwrite targeted partitions directly.
- **Join Syntax**: Oracle-specific outer join shorthand `(+)` must be completely rewritten to standard explicit `LEFT OUTER JOIN` syntax in BigQuery.

### 3.3 Text Logging & Standard Outputs
In accordance with the **OUTPUT/PRINT LITERAL RULE**, all terminal and logging text remains character-for-character identical in terms of literal text, wrapping inside Python's native `logging` or print routines:
- `"Error evaluating: 'parameter DB_TNS_NAME_DWH of map_rpos_carmen_import', interpretation 'shell'"` is retained verbatim.
- `"Internal error: '$0' is a symlink and some problem occurred expanding it..."` is retained verbatim (with `$0` mapped to python `sys.argv[0]`).

---

## 4. ENVIRONMENT VARIABLE CLASSIFICATION

The parameters referenced in the legacy KornShell and parameter files (`.cfg`) are mapped to the target environment as follows:

### 4.1 GLOBAL Environment Variables
These variables define target Cloud infrastructure and are uniform across all jobs in the environment (Dev, Test, Prod). In Python execution contexts, they are sourced at runtime via `os.environ.get()` or via Airflow variables `Variable.get()`.

| Canonical Target Variable | Sourced From / Legacy Name | Purpose / Target Value | Python Sourcing Strategy |
| :--- | :--- | :--- | :--- |
| `GCP_PROJECT` | `DB_TNS_NAME_DWH` | GCP Project ID housing BigQuery datasets. | `os.environ.get("GCP_PROJECT")` |
| `GCS_BUCKET` | `DW_DIR_IMP_SAP` | Root Cloud Storage Bucket for file arrivals. | `os.environ.get("GCS_BUCKET")` |
| `BQ_LOCATION` | N/A (New Infra) | Target BigQuery Region (e.g. `EU`, `US`). | `os.environ.get("BQ_LOCATION")` |
| `DATAPROC_REGION`| N/A (New Infra) | GCP Region for Dataproc Serverless. | `os.environ.get("DATAPROC_REGION")` |

### 4.2 JOB-SPECIFIC Variables
These variables are specific to this workload and should be supplied via function parameters, Airflow DAG `params`, or stored in job-level config objects. No prose placeholders are used.

| Target Variable | Legacy Sourced Name | Context Value (Verbatim) | Target Handling |
| :--- | :--- | :--- | :--- |
| `BHB_Projektverzeichnis` | `BHB_Projektverzeichnis` | `/Projects/TMD/processing/BHB/BD_PROC` | Transformed to GCS subdirectory paths. |
| `BHB_Quellverzeichnis` | `BHB_Quellverzeichnis` | `$DW_DIR_IMP_SAP/crs/work/` | Maps to `gs://{GCS_BUCKET}/crs/work/` |
| `BHB_Zielverzeichnis` | `BHB_Zielverzeichnis` | `$DW_DIR_IMP_SAP/crs/store/` | Maps to `gs://{GCS_BUCKET}/crs/store/` |
| `BHB_Dateimaske` | `BHB_Dateimaske` | `CARMEN_B_*_pos.fix` | File search wildcard within GCS prefix. |
| `BHB_Kopfdatensatzkennung` | `BHB_Kopfdatensatzkennung` | `H` | Row split control filter. |
| `BHB_Nutzdatensatzkennung` | `BHB_Nutzdatensatzkennung` | `P` | Row split control filter. |
| `BHB_Endedatensatzkennung` | `BHB_Endedatensatzkennung` | `X` | Row split control filter. |
| `BHB_Prozesstyp` | `BHB_Prozesstyp` | `D` | Process type category tracker. |
| `BHB_Graph` | `BHB_Graph` | `map_rpos_carmen_import` | Sourced graph identity metadata. |

---

## 5. TARGET FILE PLAN

In compliance with the **YOUR SCOPE** rule, we generate target file plans exclusively for the source files assigned to this design pass.

### 5.1 Python Wrapper Orchestration Script
- **Target Path**: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py`
- **Language**: Python 3
- **Primary Purpose**: Replaces the KornShell execution logic. Sourced environmental metadata, sets up runtime parameters, scans for transaction file updates matched against `BHB_Dateimaske` in the GCS path `gs://{GCS_BUCKET}/crs/work/`, and triggers the Dataproc Serverless PySpark batch process representing the graph logic.

---

## 6. EXTERNAL SYSTEM REPLACEMENTS

The legacy script's dependencies on local system files and database servers translate into cloud-native equivalents:
- **Filesystem storage (Input/Output)**: Local storage directories (e.g. `/appl/local/`, `$DW_DIR_IMP_SAP/`) are replaced with Google Cloud Storage (`GCS`) buckets.
- **Oracle Database**: Traditional Oracle database references (DWH, CRS, SGM datasets) are migrated to standard **BigQuery** tables:
  - `DWH$TA_F_RPOS_CARM` -> `bq_dataset.ta_f_rpos_carm`
  - `DWH$TA_F_GPOS_FACT_CARM` -> `bq_dataset.ta_f_gpos_fact_carm`
  - `DWH$TA_F_RPOS_FACT_CARM` -> `bq_dataset.ta_f_rpos_fact_carm`
  - `DWH$TA_F_RPOS_RESELLING_CARM` -> `bq_dataset.ta_f_rpos_reselling_carm`
  - `DWH$TA_T_RPOS_CARM` -> `bq_dataset.ta_t_rpos_carm`
  - `DWH$TA_C_VERTRAG` -> `bq_dataset.ta_c_vertrag`
  - `DWH$TA_K_RECH_ABSGRP` -> `bq_dataset.ta_k_rech_absgrp`
  - `DWH$TA_K_MELDUNGEN` -> `bq_dataset.ta_k_meldungen`

---

## 7. JOB DEPENDENCIES, SCHEDULING, & EXECUTION

### 7.1 Scheduling
- **Trigger**: Schedulers invoke this job in response to file delivery events.
- **GCP Schedulers Construct**: The Airflow Cloud Composer DAG will utilize a Google Cloud Storage sensor (`GCSObjectsWithPrefixPatternSensor`) matching the pattern `gs://{GCS_BUCKET}/crs/work/CARMEN_B_*_pos.fix` to trigger the Python wrapper logic automatically upon arrival.

### 7.2 Job Lineage & Dependencies
- **Upstream Dependencies**:
  - `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` — This shared environment initialization file has already been converted to PySpark modules. The migrated Python script must import or reference this package initialization setup.
- **Execution Ordering Sequence**:
  - Task 1: Check source CSV arrival via Airflow GCS sensor.
  - Task 2: Validate metadata parameters and setup GCS workspace.
  - Task 3: Submit Dataproc Serverless PySpark ETL Job to process raw files and populate target BigQuery tables.
  - Task 4: Execute final updates on audit and operational tracking tables in BigQuery.

---

## 8. FOLDER INTEGRITY & COMPONENT RESOLUTIONS

In compliance with the **HUMAN-CONFIRMED RESOLUTIONS** checklist, the following unresolved/legacy utility references contained in the legacy shell script are confirmed by human review to be omitted or marked as not needed in the target environment:
- `.CCR_INIT` (Not Needed)
- `.DW_INIT` (Not Needed)
- `AB_CATALOG_FUNCTIONS.KSH` (Not Needed)
- `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` (Not Needed)
- `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` (Not Needed)
- `DW.HOLE_PFAD` (Not Needed)
- `DW.LESE_LOG` (Not Needed)
- `H_ALIS_DATE.KSH` (Not Needed)
- `H_ALIS_DATENOBJEKT.KSH` (Not Needed)
- `H_ALIS_MELDUNGEN.KSH` (Not Needed)
- `H_ALIS_PARAMETER.KSH` (Not Needed)

These scripts represent old framework administration structures replaced natively by Composer task parameters and standard Python utility libraries.

---

## 9. RISKS & MANUAL ACTIONS

1. **SOURCE: NOT FOUND** — `AB_CATALOG_FUNCTIONS.KSH` — no candidate (Confirmed by Human Resolution: NOT NEEDED. Sourced logging and setup variables are handled natively via Airflow environments).
2. **Ab Initio Graph Migration Gap**: This design pass focuses solely on migrating the KSH wrapper script (`map_rpos_carmen_import.ksh`). The complete conversion of the main graph file (`map_rpos_carmen_import.mp`) to PySpark is handled by a separate design pass. Dataproc submission in our target Python code must be updated with the final compiled PySpark script path once that pass is complete.
3. **Database Drivers Mapping**: BigQuery execution requires using the standard Python BigQuery library (`google-cloud-bigquery`) and PySpark BigQuery connector. The `oracledb` library referenced in the verbatim tool output is used only as an illustrative direct python-to-database outline; the actual build phase must target Standard BigQuery API protocols.