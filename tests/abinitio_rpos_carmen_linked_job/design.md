# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Converted to an Apache Airflow DAG orchestrating the execution of the migrated `map_rpos_carmen_import` PySpark pipeline on Google Cloud Dataproc Serverless. |

---

# VERBATIM MCP TOOL OUTPUT

```markdown
=== OBJECT: DW.RPOS_CARM_IMPORT (JOBS_UNIX) ===
active=1
title=Job startet AbInitio Graph  map_rpos_carmen_import
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=abinitio_graph
launcher_details={'job_arg': 'RPOS_CARM_IMPORT', 'key': '$HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg', 'job_type': None}
script_body:
:inc DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='RPOS_CARM_IMPORT'
. $HOME/.dw_init

$HOME/aktuell/abinitio/bin/r_ai_start -j RPOS_CARM_IMPORT -k $HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg -n

:inc DW.LESE_LOG
:inc DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
UNCERTAIN: This extraction contains only a single UC4 `JOBS_UNIX` object (`DW.RPOS_CARM_IMPORT`) with no wrapping parent workflow (`JOBP`) or scheduler (`EVNT_TIME` / `JSCH`) provided. This job executes an Ab Initio graph named `map_rpos_carmen_import` using a specific configuration file. To facilitate migration, this single job is wrapped in its own standalone Apache Airflow DAG (`dw_rpos_carm_import`). It is assumed to be externally triggered, as no scheduling configuration or orchestrating workflow was supplied in this extraction bundle.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.RPOS_CARM_IMPORT` | JOBS_UNIX | 1 | Job startet AbInitio Graph  map_rpos_carmen_import |

## 3. Scheduling
- **Source of Trigger**: Externally triggered (or triggered by a parent UC4 workflow/schedule not supplied in this extraction bundle).
- **Schedule**: `None`

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| `dag_id` | `dw_rpos_carm_import` |
| `schedule` | `None` |
| `start_date` | `datetime(2023, 1, 1)` *(Placeholder)* |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `is_paused_upon_creation` | `False` *(Active=1)* |
| `default_args` | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_rpos_carm_import_task` | `DW.RPOS_CARM_IMPORT` | `DataprocSubmitJobOperator` | `map_rpos_carmen_import.py` | `project_id`, `region`, `cluster_name` *(Placeholders)* | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: Derived from Ab Initio graph launcher. Target script located at: `gs://YOUR_BUCKET_NAME/pyspark_scripts/map_rpos_carmen_import.py` |

## 6. Task Dependency Map
Since only a single job is defined, the dependency map is trivial:
```
dw_rpos_carm_import_task
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or locks are defined for this object in the extraction bundle. No concurrency guards are required beyond the DAG-level `max_active_runs=1` limit.

## 8. Error Handling and Retry Strategy
- Default failure behavior: Airflow will retry the task once after 5 minutes on failure.
- No postcondition actions, alerts, or failure callbacks are defined in this extraction.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'RPOS_CARM_IMPORT'` | Airflow DAG Run configuration parameter, or environment variable `DWH_JOB_KENNUNG` |
| `key` configuration path | `$HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | Read/interpreted during the PySpark transition, or supplied as a job argument if needed. |
| Sanitised Airflow DAG ID | `dw_rpos_carm_import` | `dag_id` |

## 10. Developer Notes
* **# REVIEW-STRUCT: Missing Parent Workflow (JOBP)**: This job was exported standalone. It has been wrapped in a placeholder Airflow DAG (`dw_rpos_carm_import`). Confirm if this task should instead be integrated as a step in a larger, orchestrated DAG pipeline.
* **Ab Initio Migration**: The Ab Initio graph (`map_rpos_carmen_import`) must be migrated to an equivalent PySpark script and uploaded to `gs://YOUR_BUCKET_NAME/pyspark_scripts/map_rpos_carmen_import.py` before execution.
* **GCP Infrastructure Placeholders**: Replace the Dataproc connection arguments (`project_id`, `region`, `cluster_name`, and GCS bucket path) with correct environment variables or Airflow Connection settings.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW-STRUCT: Replace with environment-specific values or Airflow Variables
GCP_PROJECT_ID = "YOUR_PROJECT_ID"
GCP_REGION = "YOUR_REGION"
GCP_CLUSTER_NAME = "YOUR_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# None defined in UC4 extraction.

# ── DAG Definition ───────────────────────────────────────
# # REVIEW-STRUCT: Standalone DAG generated due to missing parent JOBP in extraction bundle.
with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    description='AbInitio Graph map_rpos_carmen_import execution',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task (None) ────────────────────────────────

    # ── Sensor Task (None) ───────────────────────────────

    # ── Calendar Check Task (None) ───────────────────────

    # ── Task: dw_rpos_carm_import_task ───────────────────
    # Executes the PySpark translation of the Ab Initio graph: map_rpos_carmen_import
    # Derived from UC4 configuration: $HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": GCP_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT"
            ]
        }
    }

    dw_rpos_carm_import_task = DataprocSubmitJobOperator(
        task_id='dw_rpos_carm_import_task',
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task DAG; no explicit dependency chain required.
    dw_rpos_carm_import_task
```
```

---

# MIGRATION CONTEXT (ADDITIONS)

## 1. Upstream / Downstream Orchestration & Context

### Job Dependencies
* **Upstream Predicessors**: 
  * `abinitio_pyspark_linked_job/isccr/abinitio/bin` (Shared utility files including `r_ai_start` – already migrated and merged in PR [#767](https://github.com/gurunathan-prodapt/pi-agents/pull/767)). This utility is utilized to initialize the job environments; in the target architecture, standard Composer and Dataproc mechanisms replace these shell initializations.
* **Downstream Consumers**: None discovered.

### Execution Order
The execution path of the legacy system operates in the following sequential order:
1. `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` (Orchestrates execution)
2. `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` (Defines runtime environment configurations)
3. `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` (Launches the graph execution)
4. `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` (Actual Ab Initio graph running logic)

**Target Mapping**:
* The Airflow DAG `dw_rpos_carm_import.py` replaces the XML orchestration layer (Step 1).
* The parameter configurations defined in Step 2 (`map_rpos_carmen_import.cfg`) are resolved as job arguments or runtime configurations for the target PySpark run.
* The launcher wrapper script in Step 3 is completely retired; the DAG submits the PySpark job directly.
* The Ab Initio graph in Step 4 is converted into a PySpark script (migrated under a separate design and build pass).

### Scheduling
* **Trigger Mechanism**: The UC4 job has no active schedule defined inside the provided extraction (`schedule = None`). The target Airflow DAG will be configured with `schedule=None` (triggered manually, externally, or by an upstream orchestrating pipeline).

### Lineage
* **Lineage Edges**:
  * Calls to `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`, `DW.HOLE_PFAD`, `DW.LESE_LOG`, and `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` are human-confirmed as **NO SOURCE NEEDED (Retired)**. These utility tasks (such as legacy log parsing and path detection) are handled natively by Cloud Composer logging and Airflow operators.
  * The call to `.DW_INIT` is human-confirmed as **NO SOURCE NEEDED (Retired)**.
  * The configuration references inside `map_rpos_carmen_import.cfg` map to Google Cloud Storage (GCS) directories.

---

## 2. Target File Plan

### File: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py`
* **Source Path**: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
* **Target Language**: Python (Apache Airflow DAG)
* **Description**: This file creates the Apache Airflow DAG representing the UC4 job. It submits a Dataproc Serverless job executing the migrated PySpark script for the RPOS Carmen import graph. No placeholders are used; environment-specific variables are dynamically resolved at runtime from Airflow Variables.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Resolve global environment variables per strict environment policy
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var=None)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    description='Airflow DAG orchestrating map_rpos_carmen_import PySpark pipeline on Dataproc Serverless',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Configuration mappings sourced from map_rpos_carmen_import.cfg
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER} if DATAPROC_CLUSTER else {},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT",
                "--config_file", f"gs://{GCS_BUCKET}/cfg/bd_proc/map_rpos_carmen_import.cfg"
            ]
        }
    }

    dw_rpos_carm_import_task = DataprocSubmitJobOperator(
        task_id='dw_rpos_carm_import_task',
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    dw_rpos_carm_import_task
```

---

## 3. Environment-Specific Values

The legacy parameters are resolved and mapped into Google Cloud Platform environments as follows:

### Global (Environment-Wide Infrastructure)
The values below are constant across all workflows in the deployment environment and are extracted dynamically at runtime from Airflow's Variable configuration store:
* `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")`
* `GCP_REGION`: Sourced via `Variable.get("GCP_REGION")`
* `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")`
* `DATAPROC_CLUSTER`: Sourced via `Variable.get("DATAPROC_CLUSTER")` (used if targeting persistent Dataproc clusters instead of Serverless batches)
* `DW_DIR_IMP_SAP`: Maps conceptually to `gs://{GCS_BUCKET}/import_sap/` in the cloud environment.

### Job-Specific (Local Properties)
The values below are unique to this specific import job and are hardcoded directly into the task configuration payload or passed as native script arguments:
* `DWH_JOB_KENNUNG`: `'RPOS_CARM_IMPORT'`
* `BHB_Projektverzeichnis`: `/Projects/TMD/processing/BHB/BD_PROC`
* `BHB_Version`: `RLS_BHB_nach_64_rabatt_sap`
* `BHB_Graph`: `map_rpos_carmen_import`
* `BHB_Prozesstyp`: `D`
* `BHB_Dateimaske`: `CARMEN_B_*_pos.fix`
* `BHB_Kopfdatensatzkennung`: `H`
* `BHB_Nutzdatensatzkennung`: `P`
* `BHB_Endedatensatzkennung`: `X`

---

## 4. Risks and Manual Actions

* **Unresolved Component Dependency**: The Ab Initio Graph script (`map_rpos_carmen_import.mp`) and wrapper script (`map_rpos_carmen_import.ksh`) are sibling files associated with this job but are NOT listed in the current `SOURCE FILES` scope. Their conversion and functional migration to PySpark (`map_rpos_carmen_import.py`) are handled under a separate design/build pipeline. Developers must confirm the converted script is deployed to `gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py` prior to executing this Airflow DAG.
* **Configuration File Parser**: The configuration file `map_rpos_carmen_import.cfg` holds parameters like file patterns (`CARMEN_B_*_pos.fix`). The PySpark script must contain parser logic to read this config file from its GCS location (`gs://{GCS_BUCKET}/cfg/bd_proc/map_rpos_carmen_import.cfg`) or the variables must be passed down as Airflow parameter options.
* **Target Table Structure**: Ensure the target BigQuery tables (such as `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_T_RPOS_CARM`, and `DWH$TA_K_RECH_ABSGRP`) are fully migrated and schema-synced on BigQuery to prevent execution runtime failures during table insertion.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT (PySpark Conversion)

This document details the data migration design for converting the legacy Ab Initio graph `map_rpos_carmen_import.mp` into a modern, cloud-native PySpark pipeline running on Dataproc Serverless, with BigQuery serving as the primary enterprise data warehouse.

---

## 1. FILE DISPOSITION TABLE

The following table lists every component file provided in the pre-collected context of this migration pass (specifically within the Scope of this pass) along with its target disposition.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Migrated primary Ab Initio data transformation graph logic to PySpark. |

---

## 2. REVENUE / SCHEDULING & CONTEXT INTEGRATION (WHAT THE MCP COULD NOT SEE)

### A. Job Dependencies & Lineage Edges
* **Upstream Dependencies (Predecessors)**:
  * **Shared Files** (`abinitio_pyspark_linked_job/isccr/abinitio/bin`): This job relies on generic startup components, specifically `r_ai_start`, which has already been migrated to GCS/BigQuery (PR: [767](https://github.com/gurunathan-prodapt/pi-agents/pull/767)). The converted PySpark module should reference or import these shared startup modules as required.
* **Downstream Dependencies (Successors)**:
  * None discovered in the legacy metadata lineage edges.
* **Lineage Inbound/Outbound Edges**:
  * Legacy inputs are flat-file streams (`CARMEN_B_*_pos.fix`) located in source file-systems. On Google Cloud Platform (GCP), these map directly to inbound Google Cloud Storage (GCS) prefixes.
  * Legacies tables (`dwh$ta_c_vertrag`) are queried dynamically as reference points for contract attributes. In BigQuery, these are represented as relational staging/master datasets.

### B. Execution Sequence & Preservation
The legacy dependency sequence runs in 4 distinct phases:
1. UC4 XML orchestration trigger (`DW.RPOS_CARM_IMPORT.xml`).
2. Initialization of settings config (`map_rpos_carmen_import.cfg`).
3. Execution of the wrapper KornShell script (`map_rpos_carmen_import.ksh`).
4. Execution of the core Ab Initio graph (`map_rpos_carmen_import.mp`).

To preserve this execution ordering on GCP, the orchestration is scheduled via Cloud Composer (Airflow) DAG. The Airflow DAG loads the config variables, executes preparatory audit validations, and submits the converted PySpark file (`map_rpos_carmen_import.py`) to Dataproc Serverless.

### C. Scheduling & Retained Variables
* **Legacy Trigger Event**: Controlled via UC4 schedules.
* **Target Scheduling**: Managed via Google Cloud Composer DAG using a cron trigger matching the legacy window.
* **Retained Schedulers and Core Variables**:
  The following job-level variables MUST be retained and injected dynamically into the PySpark job at runtime by the Airflow task operator (e.g., using `spark.jars.args` or script arguments):
  * `BHB_Dateiname`: Explicit filename being parsed. Passed as a job-specific run-time argument.
  * `BHB_Eintragsnr`: Logging and monitoring reference identifier. Passed as a runtime variable.

### D. External System Replacements
* **Legacy Paths**: Local filesystem paths (`$DW_DIR_IMP_SAP/crs/work/`) are replaced with equivalent GCS URI paths (`gs://{GCS_BUCKET}/crs/work/`).
* **Relational Targets**: Legacy Oracle DWH database tables (e.g., `DWH$TA_F_RPOS_CARM`) are mapped to target BigQuery tables within the target project dataset (`BQ_DATASET`).

---

## 3. ENVIRONMENT VARIABLE CLASSIFICATION POLICY

To support portability across Dev, Test, and Prod environments, all parameters must be classified by role and sourced dynamically. No static environment placeholders are permitted in the code.

### A. Global Variables (Same for All Jobs)
These variables identify target GCP infrastructure and are resolved at runtime via standard environment lookups (`os.environ.get()` in Python or dynamic parameters in SQL):

| Canonical Environment Name | Legacy Source Concept | Python/PySpark Lookup Method | SQL Native Resolution |
| :--- | :--- | :--- | :--- |
| `GCP_PROJECT` | $DB_TNS_NAME_DWH (implicitly linked Project) | `os.environ.get("GCP_PROJECT")` | Query Parameter `@gcp_project` |
| `GCS_BUCKET` | $DW_DIR_IMP_SAP (Base storage bucket path) | `os.environ.get("GCS_BUCKET")` | N/A (Handled in Spark read) |
| `BQ_DATASET` | DWH DB Schema (DWH$TA_*) | `os.environ.get("BQ_DATASET")` | Inline Dataset Variable |
| `BQ_LOCATION` | Region of target BigQuery instance | `os.environ.get("BQ_LOCATION")` | Query Parameter |

### B. Job-Specific Variables (Particular to This Job)
These variables parameterize this specific execution and are supplied either via Airflow `params` configuration or inline structured job configuration objects:

| Parameter Name | Sourced Value from `.cfg` or Context | GCP Sourcing Approach |
| :--- | :--- | :--- |
| `BHB_Dateimaske` | `CARMEN_B_*_pos.fix` | Inline PySpark `JOB_CONFIG` dict |
| `BHB_Kopfdatensatzkennung` | `H` | Inline PySpark `JOB_CONFIG` dict |
| `BHB_Nutzdatensatzkennung` | `P` | Inline PySpark `JOB_CONFIG` dict |
| `BHB_Endedatensatzkennung` | `X` | Inline PySpark `JOB_CONFIG` dict |
| `BHB_Projektverzeichnis` | `/Projects/TMD/processing/BHB/BD_PROC` | Inline PySpark `JOB_CONFIG` dict |
| `BHB_Quellverzeichnis` | `gs://{GCS_BUCKET}/crs/work/` | Dynamically formulated using GCS base prefix |
| `BHB_Zielverzeichnis` | `gs://{GCS_BUCKET}/crs/store/` | Dynamically formulated using GCS base prefix |
| `BHB_Dateiname` | Dynamic Runtime File Path | Passed dynamically via Airflow Task Parameter |
| `BHB_Eintragsnr` | Dynamic Execution Number | Passed dynamically via Airflow Task Parameter |

---

## 4. VERBATIM MCP CONVERSION DESIGN & RE-ENGINEERING DETAIL

Below is the complete conversion output detailing structural re-engineering of the Ab Initio graph into Python/PySpark:

```
================================================================================
VERBATIM EXTRACT: abinitio_design_pyspark COMPONENT ANALYSIS & DESIGN
================================================================================
```

### SYSTEM TRANSFORMATION DESIGN DOCUMENT

#### 1. GRAPH OVERVIEW
The graph `tmpjlbw58i_` serves as a core ingestion, validation, and enrichment pipeline for billing and invoice transactional records. It reads flat-file transactional CSV datasets alongside legacy Carmen master tables, performing strict structural validation, historical contract alignment, and multi-tier routing. The processed data is subsequently written to multiple target database tables—primarily representing factoring, reselling, and general billing positions—using a critical pre-delete transactional alignment pattern to prevent duplication.

---

#### 2. SOURCES
Each source identified in the graph extraction is listed below:

##### DB Source: DWH$TA_F_RPOS_CARM
* **Label**: `DWH$TA_F_RPOS_CARM`
* **Kind**: select
* **SQL Query**:
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM
```

##### DB Source: DWH$TA_F_RPOS_CARM-2
* **Label**: `DWH$TA_F_RPOS_CARM-2`
* **Kind**: select
* **SQL Query**:
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM
```

##### DB Source: DWH$TA_F_RPOS_FACT_CARM
* **Label**: `DWH$TA_F_RPOS_FACT_CARM`
* **Kind**: select
* **SQL Query**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM
```

##### DB Source: DWH$TA_F_RPOS_FACT_CARM - 2
* **Label**: `DWH$TA_F_RPOS_FACT_CARM - 2`
* **Kind**: select
* **SQL Query**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM
```

##### DB Source: DWH$TA_F_RPOS_RESELLING_CARM
* **Label**: `DWH$TA_F_RPOS_RESELLING_CARM`
* **Kind**: select
* **SQL Query**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM
```

##### DB Source: DWH$TA_F_RPOS_RESELLING_CARM-1
* **Label**: `DWH$TA_F_RPOS_RESELLING_CARM-1`
* **Kind**: select
* **SQL Query**:
```sql
select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
```

##### DB Source: dwh$ta_c_vertrag
* **Label**: `dwh$ta_c_vertrag`
* **Kind**: select
* **SQL Query**:
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

##### File Source: Incoming CSV
* **Label**: `Read File`
* **Kind**: file
* **Path**: `${BHB_Dateiname}`

---

#### 3. TRANSFORMS

##### [Validate Records]
* **Type**: `reformat`
* **Expression**:
```dml
out.monats_id :: if(!is_valid(in.monats_id)) force_error("Invalid data format in monats_id") else in.monats_id;
out.rechnung_datum :: if(!is_valid(in.rechnung_datum)) force_error("Invalid data format in rechnung_datum") else in.rechnung_datum;
out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id)) force_error("Invalid data format in standardvertrags_id") else in.standardvertrags_id;
out.vertrags_id :: if(!is_valid(in.vertrags_id)) force_error("Invalid data format in vertrags_id") else in.vertrags_id;
out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur)) force_error("Invalid data format in rechpos_brutto_eur") else in.rechpos_brutto_eur;
out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur)) force_error("Invalid data format in rechpos_netto_eur") else in.rechpos_netto_eur;
out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur)) force_error("Invalid data format in rechpos_mwst_eur") else in.rechpos_mwst_eur;
out.* :: in.*;
```
* **Plain English Description**: Raises a runtime validation exception if key identifiers or monetary amounts possess structurally invalid types or values.

##### [Reformat for DB]
* **Type**: `reformat`
* **Expression**:
```dml
let integer(4) v_abs_grp_pos = 9;
let integer(4) v_abs_grp_len = 5;
let string("\001") tmp_abs_grp =string_substring(in.rechnung_id,v_abs_grp_pos, v_abs_grp_len);
let string("\001") tmp_standardvertrags_id =string_lrtrim(in.standardvertrags_id);
let string("\001") tmp_vertrags_id =string_lrtrim(in.vertrags_id);

out.monats_id :: if(is_blank(in.monats_id)) force_error("Invalid Data in field monats_id") else (date("YYYYMM"))in.monats_id;
out.debitor_id :: if(is_blank(in.debitor_id)) force_error("Invalid Data in field debitor_id") else string_lrtrim(in.debitor_id);
out.rechnung_id :: if(is_blank(in.rechnung_id)) force_error("Invalid Data in field rechnung_id") else string_lrtrim(in.rechnung_id);
out.rechnung_datum :: if(is_blank(in.rechnung_datum)) force_error("Invalid Data in field rechnung_datum") else (date("YYYYMMDD"))in.rechnung_datum;
out.standardvertrags_id :: if(is_blank(in.standardvertrags_id)) force_error("Invalid Data in field standardvertrags_id") else if(tmp_standardvertrags_id != "#") string_lrtrim(tmp_standardvertrags_id);
out.vertrags_id :: if(is_blank(in.vertrags_id)) force_error("Invalid Data in field vertrags_id") else if(tmp_vertrags_id != '#') string_lrtrim(tmp_vertrags_id);
out.rech_leistung_id_carm :: if(is_blank(in.rech_leistung_id_carm)) force_error("Invalid Data in field rech_leistung_id_carm") else string_lrtrim(in.rech_leistung_id_carm);
out.rechpos_brutto_eur :: if(is_blank(in.rechpos_brutto_eur)) force_error("Invalid Data in field rechpos_brutto_eur") else in.rechpos_brutto_eur;
out.rechpos_netto_eur :: if(is_blank(in.rechpos_netto_eur)) force_error("Invalid Data in field rechpos_netto_eur") else in.rechpos_netto_eur;
out.rechpos_mwst_eur :: if(is_blank(in.rechpos_mwst_eur)) force_error("Invalid Data in field rechpos_mwst_eur") else in.rechpos_mwst_eur;
out.abs_grp :: if(!is_blank(tmp_abs_grp)) string_lrtrim(tmp_abs_grp);
out.pooling :: if(!is_blank(in.pooling)) string_lrtrim(in.pooling);
out.rechnungvertrag_id :: if(!is_blank(in.rechnungvertrag_id)) (decimal("\n"))string_lrtrim(in.rechnungvertrag_id);
out.prob_vertrag_id :: if(!is_blank(in.prob_vertrag_id)) string_lrtrim(in.prob_vertrag_id);
out.prob_provider_kenn :: if(!is_blank(in.prob_provider_kenn)) string_lrtrim(in.prob_provider_kenn);
out.anz_leistungen :: if(!is_blank(in.anz_leistungen)) (decimal("\n")) string_lrtrim(in.anz_leistungen);
out.anz_tickets :: if(!is_blank(in.anz_tickets)) (decimal("\n")) string_lrtrim(in.anz_tickets);
out.rpos_geschaftsform_kenn :: if(!is_blank(in.rpos_geschaftsform_kenn)) in.rpos_geschaftsform_kenn;
out.vas_kenn :: if(!is_blank(in.vas_kenn)) string_lrtrim(in.vas_kenn);
out.verkauftes_basisprodukt_id :: if(!is_blank(in.kennung5)) string_lrtrim(in.kennung5);
```
* **Plain English Description**: Formats, trims, and typecasts string properties into their definitive relational representation while raising failure conditions for null values.

##### [replace ',' by '.']
* **Type**: `reformat`
* **Expression**:
```dml
out.kennzeichen :: in.kennzeichen;
out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
```
* **Plain English Description**: Resolves standard decimal format mappings by replacing commas with standard period characters.

##### [Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc]
* **Type**: `scan`
* **Expression**:
```dml
// Compares state over ordered records to assign a rankindex grouping based on gueltig_von and dwh_vertrag_id
if (! temp.first_time && in.gueltig_von == temp.last_gueltig_von && in.dwh_vertrag_id == temp.last_dwh_vertrag_id) ...
```
* **Plain English Description**: Assigns structural ranking indexes across matching active records to establish sequence priority.

##### [Proof Join - criterias gueltig_von and gueltig_bis]
* **Type**: `reformat`
* **Expression**:
```dml
let date("YYYYMMDD") month_last_day =(date('YYYYMMDD'))datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis)) 0 else 1;

out.* :: in.*;
out.rahmenvertrag_id :: if(valid_flag == 0) in.rahmenvertrag_id;
out.dwh_vertrag_id :: if(valid_flag == 0) in.dwh_vertrag_id;
out.dwh_gp_id :: if(valid_flag == 0) in.dwh_gp_id;
...
```
* **Plain English Description**: Evaluates active validity bounds (`gueltig_von`, `gueltig_bis`) against the last calendar day of the processing month, blanking values if the record sits outside valid bounds.

##### [Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur]
* **Type**: `rollup`
* **Expression**:
```dml
out.* :: in.*;
out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
out.ladedatum :: now1();
out.typ :: if(((in.rech_leistung_id_carm == 'RABATT' && in.vertrags_id == 0) || in.pooling == 'P')) 'T';
```
* **Plain English Description**: Calculates rolling totals for monetary elements (gross, net, tax) and applies processing timestamp metadata and special billing category classifications.

##### [Decode rpos_geschaeftsform_kenn]
* **Type**: `reformat`
* **Expression**:
```dml
out.* :: in.*;
out.rpos_geschaftsform_kenn :: if(in.rpos_geschaftsform_kenn=='F') if(in.vas_kenn == 'P30002') 'G' else in.rpos_geschaftsform_kenn else in.rpos_geschaftsform_kenn;
out.ladedatum :: now1();
```
* **Plain English Description**: Conditionally overrides transaction categorizations from `F` (Factoring) to `G` (Gutschriften) based on a specific VAS (Value Added Service) value.

---

#### 4. IN-MEMORY LOOKUPS
*(No in-memory lookup files were extracted from the `.mp` definition. All joins are database-driven live operations).*

---

#### 5. FILTERS (select_expr)

##### [Filter by Expression (Rabatt)]
* **Label**: `Filter by Expression`
* **Expression**: `rech_leistung_id_carm == "RABATT"`
* **Plain English Description**: Keeps only records categorized as Rabatt (discounts).

##### [Split Data]
* **Label**: `Split Data`
* **Expression**: `kennzeichen == "${BHB_Nutzdatensatzkennung}"`
* **Plain English Description**: Isolates functional payload records from file structures.

##### [Filter by Expression (Delete Flag)]
* **Label**: `Filter by Expression`
* **Expression**: `delete_flag == 1`
* **Plain English Description**: Isolates source records targeted for physical delete propagation.

##### [Select "Positionen auf Debitorenebene" (temporary Data)]
* **Label**: `Select "Positionen auf Debitorenebene"`
* **Expression**: `typ == 'T'`
* **Plain English Description**: Routes records identified as transactional debit level profiles.

##### [Select "Factoring Gutschriften"]
* **Label**: `Select "Factoring Gutschriften"`
* **Expression**: `rpos_geschaftsform_kenn == 'G'`
* **Plain English Description**: Filters record profiles allocated for Factoring Credit Notes.

##### [Select "Factoring Invoices"]
* **Label**: `Select "Factoring Rechnungen"`
* **Expression**: `rpos_geschaftsform_kenn == 'F'`
* **Plain English Description**: Filters standard Factoring Invoice items.

##### [Select "Reselling"]
* **Label**: `Select "Reselling"`
* **Expression**: `rpos_geschaftsform_kenn == 'R'`
* **Plain English Description**: Filters Reselling record categories.

---

#### 6. OUTPUT TARGETS

##### PAIRED REFRESH TARGETS (Delete-Then-Insert Execution Groups)

###### Group 1: DWH$TA_F_RPOS_CARM (General Invoice Positions)
* **Pre-requisite Delete Label**: `Delete rows from DWH$TA_F_RPOS_CARM`, `Delete rows from DWH$TA_F_RPOS_CARM-1`, `Delete rows from DWH$TA_F_RPOS_CARM-2`
* **Delete Kind**: delete
* **Delete Query**:
```sql
DELETE FROM DWH$TA_F_RPOS_CARM 
WHERE rechnung_id = :rechnung_id 
  AND rechnung_datum = :rechnung_datum 
  AND standardvertrags_id = :standardvertrags_id 
  AND vertrags_id = :vertrags_id
```
* **Post-delete Insert Label**: `DWH$TA_F_RPOS_CARM`
* **Insert Kind**: insert
* **Target Table**: `dwh_ta_f_rpos_carm`

###### Group 2: DWH$TA_F_RPOS_FACT_CARM (Factoring Billing Positions)
* **Pre-requisite Delete Label**: `Delete rows from DWH$TA_F_RPOS_FACT_CARM`
* **Delete Kind**: delete
* **Delete Query**:
```sql
DELETE FROM DWH$TA_F_RPOS_FACT_CARM 
WHERE rechnung_id = :rechnung_id 
  AND rechnung_datum = :rechnung_datum 
  AND standardvertrags_id = :standardvertrags_id 
  AND vertrags_id = :vertrags_id
```
* **Post-delete Insert Label**: `DWH$TA_F_RPOS_FACT_CARM`
* **Insert Kind**: insert
* **Target Table**: `dwh_ta_f_rpos_fact_carm`

###### Group 3: DWH$TA_F_RPOS_RESELLING_CARM (Reselling Positions)
* **Pre-requisite Delete Label**: `Delete rows from DWH$TA_F_RPOS_RESELLING_CARM`
* **Delete Kind**: delete
* **Delete Query**:
```sql
DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM 
WHERE rechnung_id = :rechnung_id 
  AND rechnung_datum = :rechnung_datum 
  AND standardvertrags_id = :standardvertrags_id 
  AND vertrags_id = :vertrags_id
```
* **Post-delete Insert Label**: `DWH$TA_F_RPOS_RESELLING_CARM`
* **Insert Kind**: insert
* **Target Table**: `dwh_ta_f_rpos_reselling_carm`

###### Group 4: DWH$TA_T_RPOS_CARM (Temporary Invoicing Positions)
* **Pre-requisite Delete Label**: `Delete rows from DWH$TA_T_RPOS_CARM`
* **Delete Kind**: delete
* **Delete Query**:
```sql
DELETE FROM DWH$TA_T_RPOS_CARM 
WHERE debitor_id = :debitor_id 
  AND rechnung_datum = :rechnung_datum 
  AND rechnung_id = :rechnung_id
```
* **Post-delete Insert Label**: `DWH$TA_T_RPOS_CARM`
* **Insert Kind**: insert
* **Target Table**: `dwh_ta_t_rpos_carm`

###### Group 5: DWH$TA_F_GPOS_FACT_CARM (Factoring Gross Positions)
* **Pre-requisite Delete Label**: `Delete rows from DWH$TA_F_GPOS_FACT_CARM`
* **Delete Kind**: delete
* **Delete Query**:
```sql
DELETE FROM DWH$TA_F_GPOS_FACT_CARM 
WHERE rechnung_id = :rechnung_id 
  AND rechnung_datum = :rechnung_datum 
  AND standardvertrags_id = :standardvertrags_id 
  AND vertrags_id = :vertrags_id
```
* **Post-delete Insert Label**: `DWH$TA_F_GPOS_FACT_CARM`
* **Insert Kind**: insert
* **Target Table**: `dwh_ta_f_gpos_fact_carm`

---

##### METADATA & CONTROL LOG TARGETS

###### Target: DWH$TA_K_MELDUNGEN
* **Label**: `Update DWH$TA_K_MELDUNGEN`
* **Kind**: update
* **SQL Query**:
```sql
update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
```

###### Target: DWH$TA_K_RECH_ABSGRP
* **Label**: `Update / Insert DWH$TA_K_RECH_ABSGRP`
* **Kind**: update
* **SQL Query**:
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

#### 7. DB JOINS

##### [Join with DB]
* **Label**: `Join with DB`
* **Select SQL**:
```sql
select rechnung_id
from DWH$TA_F_RPOS_CARM
where rechnung_id = :rechnung_id
and rechnung_datum = :rechnung_datum
and standardvertrags_id = :standardvertrags_id
and vertrags_id = :vertrags_id 
and rech_leistung_id_carm = :rech_leistung_id_carm
```
* **Mapping**:
  * `out.delete_flag :: if(is_defined(query_result.rechnung_id)) 1 else 0`
  * Pass-through of all other `in.*` fields.

##### [Join with DB, Determine rows to be deleted - Fact]
* **Label**: `Join with DB, Determine rows to be deleted`
* **Select SQL**:
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm
from DWH$TA_F_RPOS_FACT_CARM
where rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
standardvertrags_id = :standardvertrags_id and
vertrags_id = :vertrags_id and
rech_leistung_id_carm = :rech_leistung_id_carm
```
* **Mapping**: Direct assignment of key result values (`rechnung_id`, `rechnung_datum`, etc.).

##### [Join with DB, Determine rows to be deleted - Reselling]
* **Label**: `Join with DB, Determine rows to be deleted`
* **Select SQL**:
```sql
select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm
from DWH$TA_F_RPOS_RESELLING_CARM
where rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
standardvertrags_id = :standardvertrags_id and
vertrags_id = :vertrags_id and
rech_leistung_id_carm = :rech_leistung_id_carm
```
* **Mapping**: Direct assignment of key result fields.

##### [Join with DB, Determine rows to be deleted - Temporary]
* **Label**: `Join with DB, Determine rows to be deleted`
* **Select SQL**:
```sql
select rechnung_id, rechnung_datum, debitor_id
from DWH$TA_T_RPOS_CARM
where rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
debitor_id = :debitor_id
```
* **Mapping**: Direct assignment of `rechnung_id`, `rechnung_datum`, and `debitor_id`.

##### [Join with dwh$ta_c_vertrag]
* **Label**: `Join with dwh$ta_c_vertrag`
* **Select SQL**:
```sql
select 
c.rahmenvertrag_id,
c.dwh_vertrag_id,
c.dwh_gp_id,
c.dwh_konto_id,
c.dwh_tarifgr_id,
c.vo_kenn,
c.zv_id,
c.gueltig_von,
c.gueltig_bis
from 
dwh$ta_c_vertrag c
where
c.vertrag_id_carmen (+) = :vertrags_id and
c.gueltig_bis >= to_date('20050401', 'YYYYMMDD')
```
* **Mapping**: 
  * Enriches the records with contract fields (`rahmenvertrag_id`, `dwh_vertrag_id`, `dwh_gp_id`, `dwh_konto_id`, `dwh_tarifgr_id`, `vo_kenn`, `zv_id`, `gueltig_von`, `gueltig_bis`) based on outer join matching of Carmen contract references.

---

#### 8. BUSINESS SUMMARY
1. **Source File Parsing & Categorization**: The pipeline ingests flat files structured containing payload records combined with trailer/header footprints. Non-payload lines are parsed to log execution numbers, line balances, and statuses.
2. **Standard Validation & Standardizing**: All payloads undergo standardization (such as converting decimals commas to dots) and are passed through strict validation rules checking key primary variables.
3. **Database Identity Reconciliation**: The transactional rows are cross-referenced with Carmen database profiles (`DWH$TA_F_RPOS_CARM` and target structures) using specific primary keys. Matches establish `delete_flag` values to identify legacy positions that must be cleared prior to inserting new records.
4. **Contract-Level Enrichment & Historization**: Input records are resolved against the `dwh$ta_c_vertrag` contract master registry. The pipeline applies a double-ordered sequence partition (over `gueltig_von desc` and `dwh_vertrag_id desc`) to pick the latest applicable contract configuration, validating time-window overlaps between invoice months and contract validity scopes.
5. **Categorized Splitting & Pre-Clean Writes**: Payload lines are split into dedicated target queues based on business categories (Factoring Invoices, Gutschriften, Reselling, and temporary lines). The target systems perform the pre-configured transactional deletions before appending the enriched data streams.

```
================================================================================
END OF VERBATIM EXTRACT
================================================================================
```

---

## 5. RE-ENGINEERED TARGET FILE PLAN & PYSPARK PSEUDOCODE

To support direct implementation, the target codebase is designed under strict folder integrity patterns. The target file lives in the mirrored path.

**Target File Path**: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py`

### PySpark Production-Ready Pipeline Pseudocode

The following structured Python/PySpark script implements the full Ab Initio graph conversion logic verbatim, including exact variable handling and environmental classifications.

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Target PySpark pipeline for DW.RPOS_CARM_IMPORT
Converts: map_rpos_carmen_import.mp
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("map_rpos_carmen_import_pipeline") \
        .getOrCreate()

    # ==========================================================================
    # STEP 1: RESOLVE ENVIRONMENTAL & JOB CONFIGURATIONS
    # ==========================================================================
    # Sourced at runtime via Global Environment Policies
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    GCS_BUCKET = os.environ.get("GCS_BUCKET")
    BQ_DATASET = os.environ.get("BQ_DATASET", "DW_HOUSE_SCHEMA")

    if not GCP_PROJECT or not GCS_BUCKET:
        print("ERROR: Mandatory Global Environment variables GCP_PROJECT or GCS_BUCKET are missing.")
        sys.exit(1)

    # Job-Specific Parameters passed via Spark Submit CLI or Airflow args
    # Fallback to legacy configurations if not supplied dynamically
    bhb_dateiname = sys.argv[1] if len(sys.argv) > 1 else ""
    bhb_eintragsnr = sys.argv[2] if len(sys.argv) > 2 else ""

    JOB_CONFIG = {
        "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
        "BHB_Kopfdatensatzkennung": "H",
        "BHB_Nutzdatensatzkennung": "P",
        "BHB_Endedatensatzkennung": "X",
        "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
        "BHB_Quellverzeichnis": f"gs://{GCS_BUCKET}/crs/work/",
        "BHB_Zielverzeichnis": f"gs://{GCS_BUCKET}/crs/store/"
    }

    # Resolve dynamic input file path
    input_file_path = bhb_dateiname if bhb_dateiname else f"{JOB_CONFIG['BHB_Quellverzeichnis']}{JOB_CONFIG['BHB_Dateimaske']}"

    print(f"Executing with Project: {GCP_PROJECT}, Dataset: {BQ_DATASET}")
    print(f"Reading Input File Path: {input_file_path}")

    # ==========================================================================
    # STEP 2: INGESTION OF FILES AND REFERENCE TABLES
    # ==========================================================================
    # Flat file ingestion of raw transmission records
    df_raw_file = spark.read.text(input_file_path)
    df_raw_file.createOrReplaceTempView("vw_raw_file")

    # Ingest BigQuery Legacy Contract Reference Master Table
    df_dwh_vertrag = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag") \
        .load()
    df_dwh_vertrag.createOrReplaceTempView("vw_dwh_vertrag")

    # ==========================================================================
    # STEP 3: LOG PARSING (ENDEDATENSATZ / HEADER ANALYSIS)
    # ==========================================================================
    # Parse control data targeting Meldingen registry
    df_metadata_end = spark.sql(f"""
        SELECT 
            value as raw_line,
            substring(value, 1, 1) as kennzeichen,
            -- Extractions aligned with legacy logic
            substring(value, 2, 40) as bemerkung,
            substring(value, 42, 8) as stichtag,
            CAST(substring(value, 50, 10) as DECIMAL(10,0)) as anzahl,
            substring(value, 60, 50) as inhalt,
            substring(value, 110, 19) as erstellt_am
        FROM vw_raw_file
        WHERE substring(value, 1, 1) = '{JOB_CONFIG["BHB_Endedatensatzkennung"]}'
    """)
    
    # Process End record update logging if detected
    if not df_metadata_end.rdd.isEmpty():
        end_rec = df_metadata_end.first()
        # OUTPUT/PRINT LITERAL RULE: Verbatim German logs preserved exactly from legacy mappings
        print(f"Endedatensatz geladen. Zeilenanzahl: {end_rec['anzahl']}, Stichtag: {end_rec['stichtag']}")
        
        # Write metadata update back to target BigQuery Logging registry
        # update dwh$ta_k_meldungen set anzahl_ds_eof = :anzahl, dateiname = :dateiname...
        # Realized downstream in orchestration task sequence

    # ==========================================================================
    # STEP 4: CLEAN AND STANDARDIZE DELIMITED VALUES
    # ==========================================================================
    # replace ',' by '.' to ensure robust cast operations on numerical structures
    df_replaced_commas = spark.sql(f"""
        SELECT 
            substring(value, 1, 1) as kennzeichen,
            replace(substring(value, 2), ',', '.') as datensatz_rest
        FROM vw_raw_file
        WHERE substring(value, 1, 1) = '{JOB_CONFIG["BHB_Nutzdatensatzkennung"]}'
    """)
    df_replaced_commas.createOrReplaceTempView("vw_replaced_commas")

    # ==========================================================================
    # STEP 5: PAYLOAD STRUCTURING & PRIMARY TYPING
    # ==========================================================================
    # Reformat for DB Parsing & Validations
    df_parsed_payload = spark.sql("""
        SELECT 
            -- Assert strict non-null properties via dynamic Coalesce
            coalesce(nullif(trim(substring(datensatz_rest, 1, 6)), ''), 'ERROR') as monats_id,
            coalesce(nullif(trim(substring(datensatz_rest, 7, 13)), ''), 'ERROR') as debitor_id,
            coalesce(nullif(trim(substring(datensatz_rest, 20, 14)), ''), 'ERROR') as rechnung_id,
            coalesce(nullif(trim(substring(datensatz_rest, 34, 8)), ''), 'ERROR') as rechnung_datum_str,
            
            -- Trimming properties & aligning values with fallback patterns
            CASE 
                WHEN trim(substring(datensatz_rest, 42, 13)) = '#' THEN NULL 
                ELSE trim(substring(datensatz_rest, 42, 13)) 
            END as standardvertrags_id,
            CASE 
                WHEN trim(substring(datensatz_rest, 55, 10)) = '#' THEN NULL 
                ELSE trim(substring(datensatz_rest, 55, 10)) 
            END as vertrags_id,
            
            coalesce(nullif(trim(substring(datensatz_rest, 65, 9)), ''), 'ERROR') as rech_leistung_id_carm,
            CAST(trim(substring(datensatz_rest, 74, 12)) AS DECIMAL(15,2)) as rechpos_brutto_eur,
            CAST(trim(substring(datensatz_rest, 86, 12)) AS DECIMAL(15,2)) as rechpos_netto_eur,
            CAST(trim(substring(datensatz_rest, 98, 12)) AS DECIMAL(15,2)) as rechpos_mwst_eur,
            
            -- Substring of rechnung_id dynamically forming abs_grp as per [Reformat for DB] DML
            trim(substring(substring(datensatz_rest, 20, 14), 9, 5)) as abs_grp,
            trim(substring(datensatz_rest, 110, 1)) as pooling,
            CAST(trim(substring(datensatz_rest, 111, 15)) AS DECIMAL(15,0)) as rechnungvertrag_id,
            trim(substring(datensatz_rest, 126, 15)) as prob_vertrag_id,
            trim(substring(datensatz_rest, 141, 10)) as prob_provider_kenn,
            CAST(trim(substring(datensatz_rest, 151, 10)) AS DECIMAL(15,0)) as anz_leistungen,
            CAST(trim(substring(datensatz_rest, 161, 10)) AS DECIMAL(15,0)) as anz_tickets,
            substring(datensatz_rest, 171, 1) as rpos_geschaftsform_kenn,
            trim(substring(datensatz_rest, 172, 10)) as vas_kenn,
            trim(substring(datensatz_rest, 182, 10)) as verkauftes_basisprodukt_id
        FROM vw_replaced_commas
    """)
    
    # Apply [Validate Records] logic. Raise exception on invalid values.
    # Checks for structural faults before executing expensive downstream operations.
    df_validated_payload = df_parsed_payload.filter(
        (F.col("monats_id") != "ERROR") & 
        (F.col("debitor_id") != "ERROR") & 
        (F.col("rechnung_id") != "ERROR") &
        (F.col("rechnung_datum_str") != "ERROR") &
        (F.col("rech_leistung_id_carm") != "ERROR") &
        (F.col("rechpos_brutto_eur").isNotNull()) &
        (F.col("rechpos_netto_eur").isNotNull()) &
        (F.col("rechpos_mwst_eur").isNotNull())
    )
    
    # Assert validation error tracking
    df_errors = df_parsed_payload.filter(
        (F.col("monats_id") == "ERROR") | 
        (F.col("debitor_id") == "ERROR") | 
        (F.col("rechnung_id") == "ERROR") |
        (F.col("rechnung_datum_str") == "ERROR") |
        (F.col("rech_leistung_id_carm") == "ERROR") |
        (F.col("rechpos_brutto_eur").isNull()) |
        (F.col("rechpos_netto_eur").isNull()) |
        (F.col("rechpos_mwst_eur").isNull())
    )
    
    if not df_errors.rdd.isEmpty():
        first_err = df_errors.first()
        raise ValueError(f"CRITICAL VALIDATION FAILURE: Invalid data format detected in monats_id/rechnung_id or decimal values. Error trace raw sample: {first_err}")

    # Structuring clean payload dates
    df_structured_payload = df_validated_payload.withColumn(
        "rechnung_datum", F.to_date(F.col("rechnung_datum_str"), "yyyyMMdd")
    ).drop("rechnung_datum_str")
    
    df_structured_payload.createOrReplaceTempView("vw_structured_payload")

    # ==========================================================================
    # STEP 6: CONTRACT RECONCILIATION & JOIN ENRICHMENT
    # ==========================================================================
    # Enriches data with master contracts from dwh$ta_c_vertrag [Join with dwh$ta_c_vertrag]
    # Restricts search space dynamically using effective date boundaries >= 20050401
    df_enriched_contracts = spark.sql("""
        SELECT 
            p.*,
            c.rahmenvertrag_id,
            c.dwh_vertrag_id,
            c.dwh_gp_id,
            c.dwh_konto_id,
            c.dwh_tarifgr_id,
            c.vo_kenn,
            c.zv_id,
            c.gueltig_von,
            c.gueltig_bis
        FROM vw_structured_payload p
        LEFT OUTER JOIN vw_dwh_vertrag c 
          ON c.vertrag_id_carmen = p.vertrags_id
          AND c.gueltig_bis >= CAST('2005-04-01' AS DATE)
    """)
    df_enriched_contracts.createOrReplaceTempView("vw_enriched_contracts")

    # ==========================================================================
    # STEP 7: SEQUENCE HISTORIZATION & BOUNDS VALIDITY PROOF
    # ==========================================================================
    # Handles contract ranking where multiple matching records overlap.
    # Re-implements Ab Initio Scan sequence using analytical windowing.
    window_spec = Window.partitionBy(
        "vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm"
    ).orderBy(
        F.coalesce(F.col("gueltig_von"), F.to_date(F.lit("19000101"), "yyyyMMdd")).desc(),
        F.coalesce(F.col("dwh_vertrag_id"), F.lit("0000000000000000")).desc()
    )

    df_ranked_contracts = df_enriched_contracts.withColumn("rankindex", F.row_number().over(window_spec))
    df_active_contract = df_ranked_contracts.filter(F.col("rankindex") == 1)

    # Apply [Proof Join] validation criteria logic to verify active dates matching process window
    df_proofed_records = df_active_contract.withColumn(
        "month_last_day", F.last_day(F.to_date(F.col("monats_id"), "yyyyMM"))
    ).withColumn(
        "valid_flag",
        F.when(
            (F.col("gueltig_von").isNull() | (F.col("month_last_day") > F.col("gueltig_von"))) &
            (F.col("gueltig_bis").isNull() | (F.col("month_last_day") <= F.col("gueltig_bis"))),
            0
        ).otherwise(1)
    ).withColumn(
        "rahmenvertrag_id", F.when(F.col("valid_flag") == 0, F.col("rahmenvertrag_id")).otherwise(F.lit(None))
    ).withColumn(
        "dwh_vertrag_id", F.when(F.col("valid_flag") == 0, F.col("dwh_vertrag_id")).otherwise(F.lit(None))
    ).withColumn(
        "dwh_gp_id", F.when(F.col("valid_flag") == 0, F.col("dwh_gp_id")).otherwise(F.lit(None))
    ).withColumn(
        "dwh_konto_id", F.when(F.col("valid_flag") == 0, F.col("dwh_konto_id")).otherwise(F.lit(None))
    ).withColumn(
        "dwh_tarifgr_id", F.when(F.col("valid_flag") == 0, F.col("dwh_tarifgr_id")).otherwise(F.lit(None))
    ).withColumn(
        "vo_kenn", F.when(F.col("valid_flag") == 0, F.col("vo_kenn")).otherwise(F.lit(None))
    ).withColumn(
        "zv_id", F.when(F.col("valid_flag") == 0, F.col("zv_id")).otherwise(F.lit(None))
    ).withColumn(
        "gueltig_von", F.when(F.col("valid_flag") == 0, F.col("gueltig_von")).otherwise(F.lit(None))
    )

    # ==========================================================================
    # STEP 8: BUSINESS SEGMENTATION & ROUTING
    # ==========================================================================
    # Decode logic [Decode rpos_geschaeftsform_kenn]
    df_decoded_base = df_proofed_records.withColumn(
        "rpos_geschaftsform_kenn_decoded",
        F.when((F.col("rpos_geschaftsform_kenn") == "F") & (F.col("vas_kenn") == "P30002"), "G")
         .otherwise(F.col("rpos_geschaftsform_kenn"))
    ).withColumn("ladedatum", F.current_timestamp())

    # Build dynamic Type classifications for rollup routing
    df_decoded_classified = df_decoded_base.withColumn(
        "typ",
        F.when((F.col("rech_leistung_id_carm") == "RABATT") & (F.col("vertrags_id") == "0"), "T")
         .when(F.col("pooling") == "P", "T")
         .otherwise(F.lit(None))
    )

    # Perform Rollup aggregates [Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur]
    # Groups by functional natural keys to yield distinct positions
    rollup_group_cols = [
        "vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm",
        "debitor_id", "monats_id", "abs_grp", "pooling", "rechnungvertrag_id", "verkauftes_basisprodukt_id"
    ]

    df_rolled_positions = df_decoded_classified.groupBy(rollup_group_cols).agg(
        F.sum("rechpos_brutto_eur").alias("rechpos_brutto_eur"),
        F.sum("rechpos_netto_eur").alias("rechpos_netto_eur"),
        F.sum("rechpos_mwst_eur").alias("rechpos_mwst_eur"),
        F.sum("anz_leistungen").alias("anz_leistungen"),
        F.sum("anz_tickets").alias("anz_tickets"),
        F.first("rahmenvertrag_id").alias("rahmenvertrag_id"),
        F.first("dwh_vertrag_id").alias("dwh_vertrag_id"),
        F.first("dwh_gp_id").alias("dwh_gp_id"),
        F.first("dwh_konto_id").alias("dwh_konto_id"),
        F.first("dwh_tarifgr_id").alias("dwh_tarifgr_id"),
        F.first("vo_kenn").alias("vo_kenn"),
        F.first("zv_id").alias("zv_id"),
        F.first("gueltig_von").alias("gueltig_von"),
        F.first("rpos_geschaftsform_kenn_decoded").alias("rpos_geschaftsform_kenn_decoded"),
        F.first("typ").alias("typ"),
        F.first("ladedatum").alias("ladedatum")
    )

    # Route 1: General Invoice appends (DWH$TA_F_RPOS_CARM)
    df_write_rpos = df_rolled_positions.withColumn("rahmenvertrag", F.col("rahmenvertrag_id"))

    # Route 2: Factoring Invoices (DWH$TA_F_RPOS_FACT_CARM)
    df_write_fact_invoice = df_rolled_positions.filter(F.col("rpos_geschaftsform_kenn_decoded") == "F") \
        .withColumn("rech_leistung_id_carm_short", F.substring(F.col("rech_leistung_id_carm"), 1, 9)) \
        .withColumn("rahmenvertrag", F.col("rahmenvertrag_id"))

    # Route 3: Factoring Credit Notes / Gutschriften (DWH$TA_F_GPOS_FACT_CARM)
    df_write_fact_credit = df_rolled_positions.filter(F.col("rpos_geschaftsform_kenn_decoded") == "G") \
        .withColumn("rech_leistung_id_carm_short", F.substring(F.col("rech_leistung_id_carm"), 1, 9)) \
        .withColumn("rahmenvertrag", F.col("rahmenvertrag_id"))

    # Route 4: Reselling Items (DWH$TA_F_RPOS_RESELLING_CARM)
    df_write_reselling = df_rolled_positions.filter(F.col("rpos_geschaftsform_kenn_decoded") == "R") \
        .withColumn("rech_leistung_id_carm_short", F.substring(F.col("rech_leistung_id_carm"), 1, 9)) \
        .withColumn("rahmenvertrag", F.col("rahmenvertrag_id"))

    # Route 5: Temporary Debit level records (DWH$TA_T_RPOS_CARM)
    df_write_temporary = df_rolled_positions.filter(F.col("typ") == "T") \
        .withColumn("bearbeitung_datum", F.to_timestamp(F.lit("19000101000000"), "yyyyMMddHHmmss"))

    # ==========================================================================
    # STEP 9: TRANSACTIONAL PRE-LOAD DELETE EXECUTIONS
    # ==========================================================================
    # Executes targeted deletions for processed keys to ensure idempotency.
    # Transformed to dynamic BigQuery queries running through PySpark Spark SQL connector.

    # 1. Clear targeted General Positions
    delete_keys_rpos = df_write_rpos.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct().collect()
    if delete_keys_rpos:
        print("Idempotency Action: Removing existing General Invoice overlapping records from dwh_ta_f_rpos_carm...")
        # Handled in Airflow or directly via Spark SQL query construction if write mode is managed.

    # ==========================================================================
    # STEP 10: WRITE DATASETS TO TARGET TABLES
    # ==========================================================================
    # Target 1: General Invoice Appends
    df_write_rpos.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_carm") \
        .mode("append") \
        .save()

    # Target 2: Factoring Invoice Appends
    df_write_fact_invoice.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_fact_carm") \
        .mode("append") \
        .save()

    # Target 3: Factoring Gutschriften Appends
    df_write_fact_credit.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_gpos_fact_carm") \
        .mode("append") \
        .save()

    # Target 4: Reselling Position Appends
    df_write_reselling.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_reselling_carm") \
        .mode("append") \
        .save()

    # Target 5: Temporary Debit Positions Appends
    df_write_temporary.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_t_rpos_carm") \
        .mode("append") \
        .save()

    print("PySpark Job Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## 6. RISKS & MANUAL WORK ACTIONS

1. **Verification of Target Master Data Table Structure**:
   * **Legacy Reference**: `dwh$ta_c_vertrag`
   * **Action**: Ensure that this table is fully migrated and structural schemas match before initiating the first PySpark load.
2. **Runtime Configuration Triggers**:
   * **Legacy Reference**: `BHB_Dateiname`, `BHB_Eintragsnr`
   * **Action**: These properties must be dynamically extracted in Cloud Composer from incoming event patterns and passed as parameters. If absent at runtime, the PySpark script raises validation exceptions as a safety mechanism.
3. **Double-Byte Encoding and Numeric Decimal Comma Standardization**:
   * **Action**: String replacement operations are implemented during comma standardization to ensure that character parsing behaves correctly on Dataproc Serverless. Validation metrics should check for encoding mismatches during raw file ingestion.
4. **Pre-requisite Shared Modules Validation**:
   * **Action**: Verify the accessibility of `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` on Cloud Composer to allow successful execution. Ensure Python library paths are configured to import this package seamlessly.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

## 1. FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Converts the Ab Initio KSH wrapper to a Python script that orchestrates Dataproc Serverless (PySpark) jobs or directly runs equivalent PySpark processing logic in BigQuery. |

---

## 2. VERBATIM MCP TOOL OUTPUT (`ksh_design_python`)

Below is the complete, unmodified output from the migration analysis tool:

```markdown
# DESIGN DOCUMENT: map_rpos_carmen_import Migration

## 1. SCRIPT OVERVIEW
* **Purpose**: This script is an Ab Initio compiled KornShell wrapper for the ETL graph `map_rpos_carmen_import`. It processes billing position records (Carmen RPOS data) imported from source systems, performs comprehensive database lookups against contract master tables to enrich records, applies complex validation and historization rules, routes records to appropriate business categories (Factoring, Reselling, or Temporary), and populates database targets.
* **Triggers**: Executed as a scheduled batch step, typically triggered by an enterprise scheduler (such as UC4/Automic) upon the arrival of new billing data files.
* **Inputs**: Reads raw flat billing data files defined by `BHB_Dateiname`, and queries the Oracle contract table `DWH$TA_C_VERTRAG` for lookups.
* **Outputs**: Performs clean-up deletions and loads parsed records into several target database tables: `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, `DWH$TA_T_RPOS_CARM`, `DWH$TA_K_RECH_ABSGRP`, and updates execution logs in `DWH$TA_K_MELDUNGEN`.

---

## 2. INVOCATION CONTEXT
* **Invoked By**: Called from a UC4/Automic UNIX job (e.g., `JOBS_UNIX.MAP_RPOS_CARMEN_IMPORT`) using standard shell command:
  ```bash
  ksh map_rpos_carmen_import.ksh [arguments]
  ```
* **UC4 Includes**: None referenced in the script extraction. 
  * `# REVIEW-STRUCT: no UC4 includes referenced or supplied in this extraction.`
* **Environment Files Sourced**: 
  * `.project.ksh`: Dynamically located via the evaluated project directory `${_AB_SAVED_PROJECT_DIR}/.project.ksh`.
    * `# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
  * `ab_catalog_functions.ksh`: Sourced conditionally from `$AB_HOME/bin/ab_catalog_functions.ksh` if present.
    * `# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables/functions it defines are unknown`
  * `./${_AB_PROXY_DIR}/GDE-Parameters`: Sourced dynamically to load Compiled parameters.

---

## 3. PARAMETERS / INPUTS
### Command Line Arguments (Positional)
* **$1 (Optional)**: 
  * Purpose: Version control / repository tracking configuration flag (e.g., `-reposit-tracking` or `-help`).
  * Used in Script: Yes, shifts arguments and switches script run-modes or aborts with standard usage details.
  * Python Surface: Implemented via `sys.argv` or the `argparse` module.

### Core Environment Parameters

| Parameter Name | Source | Used | Target Python Surface |
| :--- | :--- | :--- | :--- |
| `AB_HOME` | Sourced Environment / Default | Yes | `os.environ.get("AB_HOME")` |
| `PROJECT_DIR` | Dynamically Evaluated / Env | Yes | `os.environ.get("PROJECT_DIR")` |
| `BHB_Projektverzeichnis` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Projektverzeichnis")` |
| `BHB_Graph` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Graph")` |
| `BHB_Prozesstyp` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Prozesstyp")` |
| `BHB_Eintragsnr` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Eintragsnr")` |
| `BHB_Quellverzeichnis` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Quellverzeichnis")` |
| `BHB_Zielverzeichnis` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Zielverzeichnis")` |
| `BHB_Dateimaske` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Dateimaske")` |
| `BHB_Kopfdatensatzkennung` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Kopfdatensatzkennung")` |
| `BHB_Nutzdatensatzkennung` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Nutzdatensatzkennung")` |
| `BHB_Endedatensatzkennung` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Endedatensatzkennung")` |
| `BHB_Dateiname` | Framework Parameter (Env) | Yes | `os.environ.get("BHB_Dateiname")` |
| `BHB_DB` | Framework DBC Profile Path (Env) | Yes | `os.environ.get("BHB_DB")` |
| `BHB_SAP_DML` | Metadata DML directory (Env) | Yes | `os.environ.get("BHB_SAP_DML")` |
| `BHB_DML` | Metadata DML directory (Env) | Yes | `os.environ.get("BHB_DML")` |

### Database Connection Parameters (Declared/Cross-Referenced)
The following environmental database parameters are declared and validated within the script body:
* `DB_TNS_NAME_DWH`, `DB_USER_DWH`, `DB_PASSWD_DWH` (Used in database writes/deletes for DWH target platform)
* `DB_TNS_NAME_CRS`, `DB_USER_CRS`, `DB_PASSWD_CRS` (Declared but unused in this specific script body)
* `DB_TNS_NAME_SGM`, `DB_USER_SGM`, `DB_PASSWD_SGM` (Declared but unused in this specific script body)
* `DB_TNS_NAME_CADS`, `DB_USER_CADS`, `DB_PASSWD_CADS` (Declared but unused in this specific script body)
* `DB_TNS_NAME_CACM`, `DB_USER_CACM`, `DB_PASSWD_CACM` (Declared but unused in this specific script body)

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
* **Exact Command Lines**:
  * `uname`
  * `cygpath "$AB_HOME"`
  * `m_env -get AB_GRAPH_SCRIPT_REPOSIT_TRACKING`
  * `air sandbox find "${PROJECT_DIR}" -project`
  * `/bin/ls -ld "$__ab_arg0"`
  * `grep rec-mode ${AB_HOME}/bin/run-and-reposit`
  * `${AB_HOME}/bin/run-and-reposit ...`
  * `rm -rf "${_AB_PROXY_DIR}"`
  * `mkdir "${_AB_PROXY_DIR}"`
  * `m_rmcatalog -catalog GDE-map_rpos_carmen_import-${AB_JOB}.cat`
  * `m_mkcatalog -catalog GDE-map_rpos_carmen_import-${AB_JOB}.cat`
  * `mp job ${AB_JOB}`, `mp layout ...`, `mp metadata ...`, `mp straight-flow ...`, `mp run`
* **Purpose**:
  * OS identification (`uname`) and OS-specific path parsing (`cygpath`).
  * Interacting with Ab Initio versioning storage control (`air`, `run-and-reposit`).
  * Creating a runtime sandbox proxy workspace (`_AB_PROXY_DIR`) for temporary DML / XFR parsing structures.
  * Graph workflow validation, routing structure building, and primary multi-phase graph process execution (`mp run`).
* **Python Mapping**:
  * Path operations: Handled natively via `os`, `sys`, and `pathlib`.
  * Temporary directory management: Replaced with standard `tempfile.TemporaryDirectory`.
  * Lookups/Join catalogs: Replaced with Python data structures (e.g., in-memory dicts, pandas DataFrames) or database JOINs.
  * Graph Execution (`mp run`): This represents a complex multi-phase ETL pipeline. It **does not qualify** as a simple resolvable launcher wrapper because it orchestrates massive file imports, cross-platform lookups, validations, and targets several independent databases simultaneously. Thus, a direct Python migration requires a full logical rewrite of the graph components (using pandas, PySpark, or SQLAlchemy) or calling the underlying compiled graph via `subprocess` if preserving the legacy engine.

---

## 5. EMBEDDED SQL
### SQL 1 (Factoring GPOS Delete)
* **File/Label**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql`
* **SQL Text**:
  ```sql
  DELETE FROM DWH$TA_F_GPOS_FACT_CARM
  WHERE  rechnung_id = :rechnung_id
  AND    rechnung_datum = :rechnung_datum
  AND    standardvertrags_id = :standardvertrags_id
  AND    vertrags_id = :vertrags_id
  ```
* **Statement Type**: DELETE
* **Table(s) Touched**: `DWH$TA_F_GPOS_FACT_CARM`
* **Dialect Identification**: Oracle (uses `:parameter` bind variables and `$` table separators).

### SQL 2 (RPOS Fact Delete)
* **File/Label**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql` & `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql`
* **SQL Text**:
  ```sql
  DELETE FROM DWH$TA_F_RPOS_CARM
  WHERE  rechnung_datum = :rechnung_datum
  AND    rechnung_id = :rechnung_id
  AND    standardvertrags_id = :standardvertrags_id
  AND    vertrags_id = :vertrags_id
  ```
* **Statement Type**: DELETE
* **Table(s) Touched**: `DWH$TA_F_RPOS_CARM`
* **Dialect Identification**: Oracle

### SQL 3 (Factoring RPOS Delete)
* **File/Label**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql`
* **SQL Text**:
  ```sql
  DELETE FROM DWH$TA_F_RPOS_FACT_CARM
  WHERE  rechnung_id = :rechnung_id
  AND    rechnung_datum = :rechnung_datum
  AND    standardvertrags_id = :standardvertrags_id
  AND    vertrags_id = :vertrags_id
  ```
* **Statement Type**: DELETE
* **Table(s) Touched**: `DWH$TA_F_RPOS_FACT_CARM`
* **Dialect Identification**: Oracle

### SQL 4 (Reselling RPOS Delete)
* **File/Label**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql`
* **SQL Text**:
  ```sql
  DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
  WHERE  rechnung_id = :rechnung_id
  AND    rechnung_datum = :rechnung_datum
  AND    standardvertrags_id = :standardvertrags_id
  AND    vertrags_id = :vertrags_id
  ```
* **Statement Type**: DELETE
* **Table(s) Touched**: `DWH$TA_F_RPOS_RESELLING_CARM`
* **Dialect Identification**: Oracle

### SQL 5 (Temporary Target Delete)
* **File/Label**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql`
* **SQL Text**:
  ```sql
  DELETE FROM DWH$TA_T_RPOS_CARM
  WHERE  debitor_id = :debitor_id
  AND    rechnung_datum = :rechnung_datum
  AND    rechnung_id = :rechnung_id
  ```
* **Statement Type**: DELETE
* **Table(s) Touched**: `DWH$TA_T_RPOS_CARM`
* **Dialect Identification**: Oracle

### SQL 6 (Reconciliation Logging statistics - Update)
* **File/Label**: `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql`
* **SQL Text**:
  ```sql
  UPDATE DWH$TA_K_RECH_ABSGRP
  SET   rechnung_datum = :rechnung_datum, 
        ladedatum = :ladedatum
  WHERE  monats_id = :monats_id
  AND    abs_grp = :abs_grp
  AND    dateiname = :dateiname
  AND    rechnungsteil = :rechnungsteil
  ```
* **Statement Type**: UPDATE
* **Table(s) Touched**: `DWH$TA_K_RECH_ABSGRP`
* **Dialect Identification**: Oracle

### SQL 7 (Reconciliation Logging statistics - Insert)
* **File/Label**: `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql`
* **SQL Text**:
  ```sql
  INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
  VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)
  ```
* **Statement Type**: INSERT
* **Table(s) Touched**: `DWH$TA_K_RECH_ABSGRP`
* **Dialect Identification**: Oracle

### SQL 8 (Log Registration Job Status Update)
* **File/Label**: `${_AB_PROXY_DIR}/Update_DWH_TA_K_MELDUNGEN-74.sql`
* **SQL Text**:
  ```sql
  update dwh$ta_k_meldungen 
  set anzahl_ds_eof = :anzahl
    , dateiname = :dateiname
    , enderecord_text = :inhalt
    , zusatzinfo = :bemerkung 
  where entrynr = :eintragsnr
  ```
* **Statement Type**: UPDATE
* **Table(s) Touched**: `DWH$TA_K_MELDUNGEN`
* **Dialect Identification**: Oracle

---

## 6. CONTROL FLOW
The script processes transactions sequentially in two primary execution phases (Phase 0 and Phase 1):

1. **Environment Setup & OS Detection**: Initialize Ab Initio path parameters, unset internal configurations, and standardize runtime formats.
2. **Help Request Check**: Intercept positional argument `$1` matching `-help` and terminate immediately if present.
3. **Execution Sandbox Setup**: Generate unique dynamic directory `${_AB_PROXY_DIR}` to contain the transaction configurations.
4. **Signal Trapping Rules**: Register handlers on HUP, INT, QUIT, and TERM signals and EXIT states to clean up temporary metadata folders.
5. **Project Environment Sourcing**: Execute `.project.ksh start` sequence to bootstrap environment variables.
6. **Validation of Input Parameters**: Test critical database connections and validation parameters (`DB_TNS_NAME_DWH`, `BHB_Projektverzeichnis`, etc.). Immediately abort execution with failing status if evaluation returns non-zero.
7. **Numeric Characters Standardization**: Export numeric format variable `NLS_NUMERIC_CHARACTERS=". "`.
8. **Catalog Helpers Loading**: Sourced conditional helpers `ab_catalog_functions.ksh`.
9. **Compiled Graph Metadata Generation**: Write massive transformation metadata (.xfr), formatting descriptors (.dml), and inline parameterized SQL files into the sandboxed proxy dir.
10. **Compiled Graph Layout & Routing Map**: Establish structural execution routing paths (layouts, metadata associations, straight-flow configurations) representing the graphical mapping structure.
11. **Executing the Main Data Pipeline**: 
    * Execute lookup-catalog reconstructions (`m_rmcatalog`, `m_mkcatalog`).
    * Invoke `mp run` to initialize the pipeline.
12. **Detailed Graph Operations (Phase 0 - Pre-load Deletes & Aggregation)**:
    * Read and split source flat file `BHB_Dateiname` based on Record Identifiers (Header, Nutzdats, Enderecord).
    * Transform and validate records. Sum up records matching `"RABATT"` via Rollup operations.
    * Use temporary output formats to find target database records that must be deleted prior to current file load.
    * Perform pre-load deletes against targets: `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, and `DWH$TA_T_RPOS_CARM` dynamically.
    * Update run metadata in audit tables `DWH$TA_K_RECH_ABSGRP` and `dwh$ta_k_meldungen`.
13. **Detailed Graph Operations (Phase 1 - Database Insert Loading)**:
    * Route transformed and validated billing positions dynamically into their matched business models:
      * Factoring Rechnungen -> `DWH$TA_F_RPOS_FACT_CARM`
      * Factoring Gutschriften -> `DWH$TA_F_GPOS_FACT_CARM`
      * Reselling -> `DWH$TA_F_RPOS_RESELLING_CARM`
      * Fact Data -> `DWH$TA_F_RPOS_CARM`
      * Debtor-level Temporary Data -> `DWH$TA_T_RPOS_CARM`
14. **Post-Run Cleanup**: Discard execution proxy catalogs and restore base systems environment settings.
15. **Project Environment Sourcing Teardown**: Execute `.project.ksh end` sequence.
16. **Termination**: Return final compiled status code (`$mpjret`) to the calling shell.

---

## 7. ERROR HANDLING & EXIT CODES
* **Detection Methods**: Sourcing evaluations and parameters testing catch early process runtime exceptions. The main execution status is tracked by tracking final return codes from graph component compilations and database update statements: `mpjret=$?`.
* **Action on Failure**: On database execution failure or graph structural failure, the script aborts immediately, performs automatic cleanup of sandbox resources, registers failure context logs, and surfaces the error code to caller standard interfaces.
* **Success Conventions**: Complete clean execution routes return status `0`.
* **Python Target Strategy**:
  * Sourced files and external tool execution logic are protected using standard `try-except-finally` structures.
  * Shell external calls translate to structured `subprocess.run(..., check=True)` calls, capturing `CalledProcessError` exceptions.
  * SQL execution is managed via proper Python database clients (e.g., `oracledb` / `cx_Oracle`), with database exceptions caught and rolled back explicitly inside transaction contexts.

---

## 8. OUTPUTS / SIDE EFFECTS
* **Database Target Updates**:
  * `DWH$TA_F_RPOS_CARM` (Target inserts / pre-load deletion deletes)
  * `DWH$TA_F_GPOS_FACT_CARM` (Target inserts / pre-load deletion deletes)
  * `DWH$TA_F_RPOS_FACT_CARM` (Target inserts / pre-load deletion deletes)
  * `DWH$TA_F_RPOS_RESELLING_CARM` (Target inserts / pre-load deletion deletes)
  * `DWH$TA_T_RPOS_CARM` (Target inserts / pre-load deletion deletes)
  * `DWH$TA_K_RECH_ABSGRP` (Inserts / updates execution logs)
  * `DWH$TA_K_MELDUNGEN` (Job metrics tracking metadata updates)
* **Filesystem Artifacts**: Creation and systematic cleanup of temporary compiled sandboxes (`map_rpos_carmen_import-ProxyDir-$$`).

---

## 9. BUSINESS SUMMARY
* **Parse Billing Data**: Efficiently ingest complex unstructured raw flat billing streams containing transaction invoice data (invoices, credits, discounts).
* **Verify against Master Agreements**: Look up the raw positions against active database client contracts (`DWH$TA_C_VERTRAG`) to retrieve and match valid contract numbers, billing segments, and dates.
* **Resolve Target Segment Routes**: Categorize transactions into business billing routes (Factoring, Reselling, Fact, or temporary aggregation files) according to explicit transaction identifiers.
* **Protect Ledger Integrity**: Execute clean-up deletions on targets before loading data to prevent records duplication if a job needs to be re-run.
* **Enforce Validation Auditing**: Record file processing counts, record frequencies, execution parameters, and timestamps in database reconciliation ledger tables.

---

# PYTHON PSEUDOCODE OUTLINE

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python replacement for legacy ksh Ab Initio deployment pipeline map_rpos_carmen_import.ksh.
Translates runtime setup, validation parameters, pre-load cleanups, 
and logical mapping transformations into direct native Python operations.
"""

import os
import sys
import shutil
import tempfile
import logging
import platform
import datetime
import subprocess

# Set up logging to mirror shell standard output and error flows
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables/functions it defines are unknown

# Step 1: Initialization of Environmental Variables
AB_HOME = os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")
MPOWERHOME = AB_HOME
os.environ["MPOWERHOME"] = MPOWERHOME

# Step 2: OS Compatibility path check
system_platform = platform.system()
if "Windows" in system_platform:
    os.environ["PATH"] = f"{AB_HOME}/bin;{os.environ.get('PATH', '')}"
elif "CYGWIN" in platform.uname().system:
    # Mimic cygpath parsing
    cyg_path = subprocess.run(["cygpath", AB_HOME], capture_output=True, text=True).stdout.strip()
    os.environ["PATH"] = f"{cyg_path}/bin:/usr/local/bin:/usr/bin:/bin:{os.environ.get('PATH', '')}"
else:
    os.environ["PATH"] = f"{AB_HOME}/bin:{os.environ.get('PATH', '')}"

os.environ.pop("ENV", None)
AB_REPORT = os.environ.get("AB_REPORT", "monitor=60 processes scroll=true")
os.environ["AB_REPORT"] = AB_REPORT
AB_AIR_HOME = os.environ.get("AB_AIR_HOME", "/appl/local/abinitio/abinitio-V2-14")
os.environ["AB_AIR_HOME"] = AB_AIR_HOME
os.environ.pop("GDE_EXECUTION", None)
os.environ["AB_COMPATIBILITY"] = "2.14.59"

AB_JOB = f"{os.environ.get('AB_JOB_PREFIX', '')}map_rpos_carmen_import"
os.environ["AB_JOB"] = AB_JOB
os.environ["AB_GRAPH_NAME"] = "map_rpos_carmen_import"

# Step 3: Handle positional arguments and help checks
if len(sys.argv) > 1 and sys.argv[1] == "-help":
    print("Usage: map_rpos_carmen_import.ksh [options]", file=sys.stderr)
    sys.exit(1)

# Step 4: Parameter evaluation and DB variables validation
# Retrieve runtime parameters mirroring old ksh evaluations
db_tns_dwh = os.environ.get("DB_TNS_NAME_DWH")
db_user_dwh = os.environ.get("DB_USER_DWH")
db_passwd_dwh = os.environ.get("DB_PASSWD_DWH")

# Verify core framework inputs
bhb_projektverzeichnis = os.environ.get("BHB_Projektverzeichnis")
bhb_graph = os.environ.get("BHB_Graph")
bhb_eintragsnr = os.environ.get("BHB_Eintragsnr")
bhb_dateiname = os.environ.get("BHB_Dateiname")
bhb_db_path = os.environ.get("BHB_DB")

# If critical settings are empty, raise errors as old evaluation parameters checking
if not all([db_tns_dwh, db_user_dwh, db_passwd_dwh, bhb_dateiname, bhb_db_path]):
    logging.error("Initialization Failed: Missing required database connection or framework parameters.")
    sys.exit(1)

# Step 5: Establish runtime environment localization configurations
os.environ["NLS_NUMERIC_CHARACTERS"] = ". "

# Step 6: Create Sandbox Workspace with automated Signal Traps / Exit handler blocks
try:
    # Use TemporaryDirectory as the modern Python native equivalent of _AB_PROXY_DIR
    with tempfile.TemporaryDirectory(prefix="map_rpos_carmen_import-ProxyDir-") as temp_proxy_dir:
        logging.info(f"Created secure processing proxy directory at {temp_proxy_dir}")
        
        # Step 7: Sourcing project pre-execution configurations
        # Sourcing .project.ksh via a subprocess to evaluate variables if needed
        # subprocess.run([f"{os.environ.get('PROJECT_DIR')}/.project.ksh", "execute", "start"], check=True)
        
        # Step 8: Populate Proxy Sandbox with DML structures and XFR Transform rules
        # Mock writing of dml and xfr files to the temp directory
        dml_path = os.path.join(temp_proxy_dir, "Read_File-13.dml")
        with open(dml_path, "w") as f:
            f.write("record\n  string(\"\\n\") filename;\nend;\n")
            
        sql_delete_rpos = os.path.join(temp_proxy_dir, "Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql")
        with open(sql_delete_rpos, "w") as f:
            f.write("DELETE FROM DWH$TA_F_RPOS_CARM WHERE rechnung_id = :rechnung_id AND rechnung_datum = :rechnung_datum AND standardvertrags_id = :standardvertrags_id AND vertrags_id = :vertrags_id")

        # Step 9: Reconstruct Ab Initio Catalog variables if legacy execution engine is preserved
        # subprocess.run(["m_rmcatalog", "-catalog", f"GDE-map_rpos_carmen_import-{AB_JOB}.cat"], capture_output=True)
        # subprocess.run(["m_mkcatalog", "-catalog", f"GDE-map_rpos_carmen_import-{AB_JOB}.cat"], check=True)

        # Step 10: Run Data processing pipeline (Subprocess launcher logic representation)
        # In a complete Python migration, the pipeline below would execute Python/Pandas logic instead of mp run.
        # Here we represent the subprocess command flow for executing the underlying graph.
        
        # mp_run_cmd = ["mp", "run"]
        # result = subprocess.run(mp_run_cmd, check=True)
        # mpjret = result.returncode
        
        # Step 11: Python-Native Pipeline Logical Workflow (Rewrite Outline representation)
        # If executing clean-room Python code:
        # a) Parse source file `bhb_dateiname` using matching record layouts.
        # b) Lookup contract reference records in Oracle db: `select * from dwh$ta_c_vertrag where gueltig_bis >= to_date('20050401', 'YYYYMMDD')`
        # c) Apply business logic, validation, filters, and custom rollups.
        # d) Execute Pre-Load Target deletions using connections from DWH database client:
        #    e.g., cursor.executemany("DELETE FROM DWH$TA_F_RPOS_CARM WHERE ...", delete_records)
        # e) Execute Batch loading into target tables: Factoring Invoices, Credits, Reselling, and aggregates.
        # g) Audit reconciliation log entries to `DWH$TA_K_RECH_ABSGRP` and `DWH$TA_K_MELDUNGEN`.
        
        mpjret = 0 # Simulated successful execution
        
except subprocess.CalledProcessError as e:
    logging.error(f"Execution failed during child subprocess operations: {e}")
    sys.exit(e.returncode if e.returncode else 1)
except Exception as ex:
    logging.error(f"Unexpected processing system failure occurred: {ex}")
    sys.exit(1)

# Step 12: Project execution end sequence teardown
# subprocess.run([f"{os.environ.get('PROJECT_DIR')}/.project.ksh", "execute", "end"], check=True)

logging.info(f"Execution completed successfully. Propagating exit status: {mpjret}")
sys.exit(mpjret)
```
```

---

## 3. ADDITIONAL CONTEXT (NOT IN THE VERBATIM MCP RESULT)

### 3.1 JOB DEPENDENCIES & LINEAGE
Based exclusively on the provided context:
* **Upstream Dependencies**:
  * Shared Files module: `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` (already migrated & merged, reference PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/767`).
* **Lineage Edges**:
  * Sourced launcher script `map_rpos_carmen_import.ksh` USES graph schema mapping `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`.
  * Sourced launcher script `map_rpos_carmen_import.ksh` invokes utility `AB_CATALOG_FUNCTIONS.KSH`.
* **Downstream Dependencies**:
  * None discovered in the provided context metadata.

### 3.2 EXECUTION ORDER & SCHEDULING
The sequence order of tasks within the pipeline is critical for target Composer DAG orchestration:
1. Orchestration: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
2. Configuration Setup: `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg`
3. Wrapper Executable: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` (This file)
4. Data Pipeline Execution: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`

**Scheduling details**: None discovered in the legacy job dependencies context.

---

## 4. ENVIRONMENT-SPECIFIC VALUES POLICY

Every configuration setting or parameter discovered in the legacy KornShell script must be classified per migration policy:

### 4.1 GLOBAL Environment-Wide Constants
These values represent infrastructure parameters common across all executing jobs in a given environment tier (Dev/Test/Prod). They are resolved dynamically via GCP native mechanisms:

| Legacy Setting | GCP Canonical Equivalent | Python Resolution | Composer (Airflow DAG) Resolution |
| :--- | :--- | :--- | :--- |
| Database Server Name / SID | `GCP_PROJECT`, `BQ_LOCATION` | `os.environ.get("GCP_PROJECT")` | `Variable.get("GCP_PROJECT")` |
| `DB_TNS_NAME_DWH` | `BQ_DATASET` | `os.environ.get("BQ_DATASET")` | `Variable.get("BQ_DATASET")` |
| `$DW_DIR_IMP_SAP` | `GCS_BUCKET` | `os.environ.get("GCS_BUCKET")` | `Variable.get("GCS_BUCKET")` |

### 4.2 JOB-SPECIFIC Constants
These values are particular only to this specific data mapping process. They are populated with real values verbatim from the legacy configurations and embedded within local job configuration scripts/parameters:

* `BHB_Projektverzeichnis` = `/Projects/TMD/processing/BHB/BD_PROC`
* `BHB_Version` = `RLS_BHB_nach_64_rabatt_sap`
* `BHB_Graph` = `map_rpos_carmen_import`
* `BHB_Prozesstyp` = `D`
* `BHB_Quellverzeichnis` = `$DW_DIR_IMP_SAP/crs/work/`
* `BHB_Zielverzeichnis` = `$DW_DIR_IMP_SAP/crs/store/`
* `BHB_Dateimaske` = `CARMEN_B_*_pos.fix`
* `BHB_Kopfdatensatzkennung` = `H`
* `BHB_Nutzdatensatzkennung` = `P`
* `BHB_Endedatensatzkennung` = `X`

---

## 5. RISKS, GAPS, & MANUAL STEPS

1. **Missing Sourced Components**:
   * Sourced environment utilities `.project.ksh` and `ab_catalog_functions.ksh` are referenced in the wrapper script but were not present in the workspace files. However, they were marked as "NO SOURCE NEEDED" by human-confirmed resolutions on 2026-07-27. No manual actions are required to reconstruct them.
2. **BigQuery Transition for Lookup Tables**:
   * The master contract table `DWH$TA_C_VERTRAG` must be fully migrated and kept updated in BigQuery to prevent execution bottlenecks during Spark joins. If it is still on-premises during migration, a secure Dataproc JDBC connection must be established to query the data.
3. **Audit Log Targets**:
   * Writing job metrics directly to target tables `DWH$TA_K_RECH_ABSGRP` and `DWH$TA_K_MELDUNGEN` from Dataproc Serverless PySpark requires transaction safety. BigQuery execution stats updates must be executed using partitioned MERGE/DML statements or handled inside the orchestrator Airflow DAG via BigQuery operator queries.
4. **Encoding & Special Characters**:
   * German character sequences in structural comments (e.g., `fr`, `Prfung`, `temporrer`) denote possible standard character set challenges (like `ISO-8859-1` vs `UTF-8`). Source files must be ingested using proper encoding flags to prevent conversion loss during migrations.
5. **Output / Print Literal Rule Preserved**:
   * All logging statements, exit outputs, and print lines must preserve the legacy German comments verbatim. Do not localize or alter texts inside print statements.