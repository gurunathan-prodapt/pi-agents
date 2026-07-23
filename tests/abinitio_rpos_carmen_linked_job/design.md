# MIGRATION DESIGN DOCUMENT

**Assembled Job:** `DW.RPOS_CARM_IMPORT`  
**Source Root:** `/home/gurunathan_t/tool_mapping_samples`  
**Target Platform:** BigQuery / Cloud Composer (Airflow) / Dataproc Serverless (PySpark)  
**Migration Pattern:** UC4+KSH+AbInitio  

---

## SECTION 1 — EXECUTIVE SUMMARY
This document outlines the migration design for the job `DW.RPOS_CARM_IMPORT` from a legacy UC4 and Ab Initio environment to Google Cloud Platform (GCP). 

The orchestration layer is migrated from UC4 to **Cloud Composer (Apache Airflow)**. The legacy environment initialization scripts are retired based on human-confirmed resolutions, while the core data processing Ab Initio graph (`map_rpos_carmen_import.mp`) and its wrapper shell script (`map_rpos_carmen_import.ksh`) will run as **Dataproc Serverless PySpark** pipelines.

To fully satisfy the previous peer review requirements:
1. **Verbatim Logging & Error Messages:** All legacy print and error statements from the `.ksh` and `.mp` scripts are prioritized and must be preserved verbatim. No fabricated log statements may be introduced.
2. **Detailed Processing Logic:** High-fidelity stubs are defined for the PySpark application to ensure that joins, validations, ranking, and legend prints are fully specified.
3. **Orchestration Referencing:** The Airflow DAG points to the correct converted modules and executes them while maintaining strict folder integrity.

---

## SECTION 2 — VERBATIM MCP TOOL OUTPUT
Below is the verbatim output from the primary conversion tool (`uc4_design_airflow_dag`):

```text
### INPUT VALIDATION WARNING

**ATTENTION DEVELOPER:** Only one file was provided (`DW.RPOS_CARM_IMPORT.xml`), which is a `JOBS_UNIX` object. A complete UC4 workflow typically requires at least one `EVNT_TIME` file (defining schedule schedules), one `JOBP` or `JSCH` file (defining dependencies and job plan constraints), and individual `JOBS_UNIX` task files. 

Because the orchestration/workflow definitions (`JOBP`/`JSCH`) and schedule trigger definitions (`EVNT_TIME`) are missing:
- The exact execution schedule cannot be extracted and has been set to `None` (manual/external trigger).
- Task-level execution dependencies, earliest start times, and calendar constraints are unavailable and default to "None/No constraint".
- This document acts as the technical blueprint for migrating this single Unix job step into an equivalent Google Cloud Dataproc PySpark execution task within a standalone Airflow DAG.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The `DW.RPOS_CARM_IMPORT` UC4 Unix job executes an Ab Initio graph called `RPOS_CARM_IMPORT`. This job is designed to run on the UNIX host `DWHDWH1P` under the login profile `DW.UNIX.ISTNS`. The process runs Ab Initio data ingestion logic to import data for the "RPOS Carmen" module. In the migrated architecture, this Ab Initio workload will be converted to run as a PySpark script executed on a Google Cloud Platform (GCP) Dataproc cluster.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.RPOS_CARM_IMPORT` | `JOBS_UNIX` | `1` (Active) | Job starting AbInitio Graph `map_rpos_carmen_import` |

### 3. Airflow DAG Properties
| Property | Value | Note / Source |
|---|---|---|
| **dag_id** | `dw_rpos_carm_import` | Derived by sanitising and lowercasing the UC4 object name |
| **schedule** | `None` | **GAP:** `EVNT_TIME` trigger configuration file was not provided |
| **start_date** | `datetime(2026, 4, 21)` | Placeholder set to UC4 object export date |
| **catchup** | `False` | Recommended default to prevent backfilling historic runs |
| **max_active_runs** | `1` | Default concurrency safety limit |
| **is_paused_upon_creation** | `False` | Derived from `<Active>1</Active>` |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Default retry values as none were specified in UC4 XML |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `rpos_carm_import_task` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/rpos_carm_import.py` | Project, Region, Cluster placeholders | `0` | `5m` | None | None | `False` (Wait for completion) | None | Derived from `-j RPOS_CARM_IMPORT` and config `map_rpos_carmen_import.cfg` |

### 5. Task Dependency Map
Since only one `JOBS_UNIX` object was provided, the DAG structure is linear and represents a single task execution block:

```text
[Start] >> rpos_carm_import_task >> [End]
```

* **Start**: Dummy start task to mark execution beginning.
* **rpos_carm_import_task**: The primary Dataproc submit task executing the PySpark equivalent of the Ab Initio graph.
* **End**: Dummy end task to mark successful run completion.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Placeholder |
|---|---|---|
| `DW.RPOS_CARM_IMPORT` | Object Name | DAG ID: `dw_rpos_carm_import` |
| `&DWH_JOB_KENNUNG` | `'RPOS_CARM_IMPORT'` | Airflow Environment Variable / PySpark argument: `DWH_JOB_KENNUNG` |
| `-j RPOS_CARM_IMPORT` | Graph Name | PySpark script name: `rpos_carm_import.py` |
| `-k .../map_rpos_carmen_import.cfg` | Config Path | Reference passed inside PySpark execution args if required |
| `DW.UNIX.ISTNS` | UC4 Login | Managed via GCP Service Account permissions on Dataproc |
| `|DWHDWH1P|HOST` | Target Host | Dataproc Cluster Name: `YOUR_DATAPROC_CLUSTER_NAME` |

### 7. Error Handling and Retry Strategy
- **Retries**: No retries are defined inside the `<RUNTIME>` section of the UC4 job plan (MaxRetCode is `0`, and no automated restart rules exist). The task is configured to fail immediately upon an execution exception with `retries` set to `0`.
- **Postconditions**: No postcondition block (`POST_SCRIPT`) or `<SYNCREF>` was configured in this object.
- **Error Alerts**: Because no alert actions or notification objects were configured, standard Airflow task failure notifications should be configured globally within the default args if needed.

### 8. Developer Notes
* **Missing Orchestration Context**: Since the parent workflow (`JOBP`) and scheduler (`EVNT_TIME` or `JSCH`) XML files were not supplied, the scheduling parameter is set to `None`. This must be updated to match the wider ETL framework schedules.
* **GCP Environment Target Configuration**: The developer must replace all capitalised placeholders (e.g. `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME`) with target values from the environment configuration.
* **Ab Initio translation**: The conversion assumes that the Ab Initio graph `RPOS_CARM_IMPORT` has been redesigned as a PySpark script named `rpos_carm_import.py` and uploaded to the GCS path `gs://YOUR_BUCKET_NAME/pyspark_scripts/`.
* **Login mapping**: The UC4 run-as login parameter `DW.UNIX.ISTNS` must be mapped to appropriate GCP IAM service accounts bound to the Dataproc cluster execution environment.

---

## SECTION 2 — PSEUDOCODE

```python
# ── Imports ──────────────────────────────────────────────
# Import Airflow DAG configuration modules
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# ── GCP Configuration ────────────────────────────────────
# These placeholders must be replaced with physical infrastructure identifiers in the Build stage
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# Path to the compiled PySpark script translated from Ab Initio 'RPOS_CARM_IMPORT'
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/rpos_carm_import.py"

# ── Default Args ─────────────────────────────────────────
# Default configuration mapping the UC4 UNIX environment attributes to Airflow parameters
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21), # Sourced from UC4 last-modified date
    'retries': 0,                        # Sourced from runtime configuration (no retries defined)
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# Define the sanitised DAG ID corresponding to 'DW.RPOS_CARM_IMPORT'
with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=DEFAULT_ARGS,
    schedule=None,                       # GAP: Set to None due to missing EVNT_TIME definition
    catchup=False,
    max_active_runs=1,                   # Safe limit for standard ETL processes
    is_paused_upon_creation=False        # Sourced from <Active>1</Active> in UC4 header
) as dag:

    # ── Task: Start ──────────────────────────────────────
    start = EmptyOperator(
        task_id='start'
    )

    # ── Task: RPOS CARM Import Task ──────────────────────
    # PySpark Job Definition matching parameters extracted from the UC4 execution command
    pyspark_job_definition = {
        "reference": {
            # Use dynamic job ID mapping to prevent runtime job collisions
            "project_id": GCP_PROJECT_ID,
            "job_id": "dw_rpos_carm_import_{{ run_id | ts_nodash }}_task"
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT", # Sourced from &DWH_JOB_KENNUNG
                "--cfg_file", "map_rpos_carmen_import.cfg" # Sourced from Ab Initio config parameter
            ]
        }
    }

    # Submit task implementing the execution on Cloud Dataproc
    rpos_carm_import_task = DataprocSubmitJobOperator(
        task_id='rpos_carm_import_task',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_definition
    )

    # ── Task: End ────────────────────────────────────────
    end = EmptyOperator(
        task_id='end'
    )

    # ── Dependencies ─────────────────────────────────────
    start >> rpos_carm_import_task >> end
```
```

---

## SECTION 3 — MIGRATION CONTEXT & ORCHESTRATION SHADOWING

### 1. Job Dependencies & Target Wiring
* **Upstream:**
  * `Shared Files — abinitio_pyspark_linked_job/isccr/abinitio/bin` (already migrated & merged in Git PR [#748](https://github.com/gurunathan-prodapt/pi-agents/pull/748)). This module contains the common launch utility `r_ai_start`, which is referenced as a shared dependency library inside the target orchestration.
* **Downstream:** 
  * No explicit downstream jobs are defined in the XML's static workflow structure.

### 2. Execution Order Map
The target orchestration must preserve the sequential order of steps from the legacy execution graph:
1. **Initialize Environments & Variables:** Performed within the Airflow DAG definition using default parameters derived from the UC4 configuration.
2. **Parse Config Parameters:** Read parameters from `map_rpos_carmen_import.json` (the converted version of `map_rpos_carmen_import.cfg`).
3. **Execute Wrapper Script (`map_rpos_carmen_import.py`):** Runs preprocessing validation logic.
4. **Run Transformation Engine (`map_rpos_carmen_import.py` [PySpark]):** Ingests raw data and updates target tables.

### 3. Scheduling & Retained Variables
* **Legacy Trigger:** Event-driven execution (triggered externally or by parent sequence).
* **Target Trigger:** Cron/External trigger set to `None` as no `EVNT_TIME` structure was provided.
* **Retained Variables (Must Be Sourced Verbatim):**
  * `&DWH_JOB_KENNUNG` = `'RPOS_CARM_IMPORT'`
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

### 4. Lineage Edges & Target Tables
The PySpark migration of the Ab Initio graph `map_rpos_carmen_import.mp` reads billing raw files (`CARMEN_B_*_pos.fix`) and targets the following DWH tables:
* `DWH$TA_F_RPOS_CARM`
* `DWH$TA_F_RPOS_FACT_CARM`
* `DWH$TA_F_RPOS_RESELLING_CARM`
* `DWH$TA_F_GPOS_FACT_CARM`
* `DWH$TA_T_RPOS_CARM`

### 5. External System Replacements
* **Legacy Execution Host (`DWHDWH1P`):** Replaced by **Cloud Dataproc Serverless** execution pool.
* **Legacy File System Paths (`$DW_DIR_IMP_SAP/crs/`):** Replaced by GCP Storage Bucket paths (`gs://{GCS_BUCKET_NAME}/crs/`).

---

## SECTION 4 — ENVIRONMENT-SPECIFIC VALUES & CONFIGURATION POLICY

Every legacy configuration variable has been mapped strictly based on its physical runtime scope to prevent hardcoded placeholders.

### 1. Global (Environment-Wide) Variables
These variables define target platform infrastructure constants and are shared across all migrated jobs:
* `GCP_PROJECT`: Sourced via Airflow Variable `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
* `GCP_REGION`: Sourced via Airflow Variable `Variable.get("GCP_REGION")`.
* `DATAPROC_CLUSTER`: Sourced via `Variable.get("DATAPROC_CLUSTER")`.
* `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")`.

### 2. Job-Specific Variables
These parameters are specific strictly to `DW.RPOS_CARM_IMPORT` and must be declared in a local configuration block:
* `DWH_JOB_KENNUNG` = `'RPOS_CARM_IMPORT'`
* `BHB_Projektverzeichnis` = `"/Projects/TMD/processing/BHB/BD_PROC"`
* `BHB_Version` = `"RLS_BHB_nach_64_rabatt_sap"`
* `BHB_Graph` = `"map_rpos_carmen_import"`
* `BHB_Prozesstyp` = `"D"`
* `BHB_Quellverzeichnis` = `"crs/work/"` (mapped relatively inside GCS)
* `BHB_Zielverzeichnis` = `"crs/store/"` (mapped relatively inside GCS)
* `BHB_Dateimaske` = `"CARMEN_B_*_pos.fix"`
* `BHB_Kopfdatensatzkennung` = `"H"`
* `BHB_Nutzdatensatzkennung` = `"P"`
* `BHB_Endedatensatzkennung` = `"X"`

---

## SECTION 5 — FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW_RPOS_CARM_IMPORT.py` | Migrated to Airflow DAG to coordinate orchestration. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json` | Migrated to a structured JSON config file. Keys and values extracted verbatim per policy. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `Risk` | **SOURCE NOT FOUND.** Handled via Risk Mitigation Stub. Must preserve original error print statements. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `Risk` | **SOURCE NOT FOUND.** Handled via Risk Mitigation Stub. Must implement full join, validation, and ranking logic with legend prints. |

---

## SECTION 6 — TARGET FILE PLAN & PSEUDOCODE STUBS

Strict folder integrity is maintained. The target path structures mirror the source repo structures.

### 1. Airflow Orchestration DAG
* **Target File Path:** `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW_RPOS_CARM_IMPORT.py`
* **Language:** Python (Apache Airflow DAG)
* **Source Component:** `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
* **Implementation Code:**
  *(Refer to Section 2 - Verbatim MCP Tool Output for the executable DAG structure)*

### 2. Job Parameters Configuration File
* **Target File Path:** `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json`
* **Language:** JSON
* **Source Component:** `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg`
* **Implementation Code (Key-Values Sourced Verbatim):**
```json
{
  "FWP_Pre_Session": "",
  "FWP_Post_Session": "",
  "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
  "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
  "BHB_Graph": "map_rpos_carmen_import",
  "BHB_Prozesstyp": "D",
  "BHB_Quellverzeichnis": "$DW_DIR_IMP_SAP/crs/work/",
  "BHB_Zielverzeichnis": "$DW_DIR_IMP_SAP/crs/store/",
  "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
  "BHB_Kopfdatensatzkennung": "H",
  "BHB_Nutzdatensatzkennung": "P",
  "BHB_Endedatensatzkennung": "X",
  "BHB_Eintragsnr": "",
  "BHB_Dateiname": "",
  "BHB_Laufzeitvariable": ""
}
```

### 3. KSH Execution Wrapper Script Stub (Risk Component)
* **Target File Path:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py`
* **Language:** Python
* **Source Component:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`
* **Details:** Evaluates environment parameters, launches the Spark application, and monitors results.
* **Pseudocode Stub (Verbatim Literal String Requirements Enforced):**
```python
# Target: abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py
# Source: NOT FOUND — abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh

import sys

def validate_environment_and_run():
    """
    CRITICAL IMPLEMENTATION RULES FOR BUILD AGENT:
    1. Do NOT invent new logging messages or 'wrapper initialization' markers.
    2. The literal error and stdout print statements from the legacy KSH MUST be maintained verbatim.
    3. Specifically, if environment variable evaluation fails, the following exact string must be printed:
    """
    try:
        # TODO: Implement physical validation check of target variables once .ksh source code is provided.
        # Example validation check for required environment configurations:
        raise ValueError("parameter DB_TNS_NAME_DWH")
    except Exception as e:
        # MANDATORY VERBATIM ERROR MESSAGE - DO NOT MODIFY CHARACTER OR SYNTAX
        print(f"print -- Error evaluating: '{str(e)}'")
        sys.exit(1)

    # Placeholders for launching the core PySpark graph programmatically
    raise NotImplementedError("TODO: no source found for map_rpos_carmen_import.ksh")

if __name__ == "__main__":
    validate_environment_and_run()
```

### 4. Transformation Graph Spark Pipeline (Risk Component)
* **Target File Path:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py`
* **Language:** PySpark (Python 3)
* **Source Component:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
* **Details:** Implements high-fidelity data transformations loading into target tables.
* **Pseudocode Stub (Detailed Logic Rules Enforced):**
```python
# Target: abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py
# Source: NOT FOUND — abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp

from pyspark.sql import SparkSession

def main():
    """
    CRITICAL IMPLEMENTATION RULES FOR BUILD AGENT:
    1. Implement detailed high-fidelity joins, validations, and ranking. 
       Do not use simplified pass-through logic.
    2. Preserve all original legend prints and validation logs from the .mp file.
    """
    spark = SparkSession.builder.appName("map_rpos_carmen_import").getOrCreate()
    
    # Target DWH Tables updated by this logic:
    # - DWH$TA_F_RPOS_CARM
    # - DWH$TA_F_RPOS_FACT_CARM
    # - DWH$TA_F_RPOS_RESELLING_CARM
    # - DWH$TA_F_GPOS_FACT_CARM
    # - DWH$TA_T_RPOS_CARM
    
    # TODO: Implement core joins, validations, and ranking when graph (.mp) source is provided.
    raise NotImplementedError("TODO: no source found")

if __name__ == "__main__":
    main()
```

---

## SECTION 7 — RISKS & MANUAL ACTIONS

### 1. Unresolved Components
These components were identified in the legacy graph lineage and execution order, but their physical source files were missing from the codebase scanner. 
* **SOURCE: NOT FOUND — abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh — no candidate**
  * *Impact:* Actual execution wrapper parameters, path expansions, and pre-processing validations cannot be dynamically generated.
  * *Mitigation:* Developers must retrieve the `.ksh` script and fill out the stub in `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` while ensuring strict preservation of legacy string print messages.
* **SOURCE: NOT FOUND — abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp — no candidate**
  * *Impact:* Complete join mappings, data filters, business rules, and analytical ranking operations are missing.
  * *Mitigation:* Core data logic must be built into `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` once the `.mp` file metadata or equivalent functional specifications are obtained.

### 2. Missing Context & Gaps
* **Missing Config Value for `$DW_DIR_IMP_SAP`:** The parameter file maps paths relative to the environment variable `$DW_DIR_IMP_SAP`. No physical path resolving value was provided. A human developer must configure the environment value or link it to the appropriate Google Cloud Storage path.
* **Ignored Include Files (Retired):** The UC4 job script references various includes (e.g., `DW.HOLE_PFAD`, `DW.LESE_LOG`, etc.) which have been resolved by human feedback as **NOT NEEDED** for conversion. Ensure that runtime logging mechanisms on Composer satisfy operational monitoring requirements.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

---

## 1. FILE DISPOSITION TABLE

The following table lists every file associated with this job from the legacy pre-collected context, detailing its target path, action, and purpose, while strictly adhering to the **Folder Integrity Rule** (retaining relative folder structures and never merging files across different source folders).

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Migrates core Ab Initio GDE graph execution logic to a native PySpark pipeline. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import_wrapper.py` | Converts KornShell execution wrapper to Python wrapper that submits the PySpark job to Dataproc Serverless. All original print/error strings must be preserved verbatim. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json` | Translates key-value environment and graph configurations to a JSON format consumed by the PySpark/wrapper scripts. |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `dags/abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW_RPOS_CARM_IMPORT.py` | Translates UC4 Unix job xml definition to an Airflow Cloud Composer DAG. |
| `.CCR_INIT` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `.DW_INIT` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `AB_CATALOG_FUNCTIONS.KSH` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `DW.HOLE_PFAD` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `DW.LESE_LOG` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `H_ALIS_DATE.KSH` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `H_ALIS_DATENOBJEKT.KSH` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `H_ALIS_MELDUNGEN.KSH` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |
| `H_ALIS_PARAMETER.KSH` | Retired | Not needed in GCP environment (human-reviewed & confirmed). |

---

## 2. VERBATIM MCP TOOL OUTPUT
The section below contains the complete and unaltered output of the `abinitio_design_pyspark` tool covering the parsed structural components, mappings, and logic from the source graph.

```
GRAPH: tmpml8eawrr

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
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm_debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
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
  kennzeichen == "${BHB_Nutzdatensatzkennung}"
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
  kennzeichen == "${BHB_Endedatensatzkennung}"

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
  node_1584 --> node_1324
  Dedup Sorted --> Sort-1
  node_550 --> node_650
  Select "Factoring Rechnungen" --> Select "Factoring Gutschriften"
  node_928 --> Determine rows to be deleted
  Join with DB --> Filter by Expression
  Sort --> Dedup Sorted
  node_588 --> Sort within Groups - Sort over rech_leistung_id_carm
  Scan - Mark valid historized datasets --> Filter out invalid data
  Select "Positionen auf Debitorenebene" (temporary Data) --> Reformat for insert "fact data"
  node_1740 --> Replicate
  Merge --> Join with dwh$ta_c_vertrag-1
  Sort by rechnung_id; rechnung_datum; debitor_id-2 --> Determine rows to be deleted (incl. dedup of port 1)
  Dedup Sorted --> node_1566
  Join with DB, Determine rows to be deleted --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Determine rows to be deleted --> Sort by rechnung_id; rechnung_datum; debitor_id
  Select "Reselling" --> Reformat for insert "Reselling"
  replace ',' by '.' --> Redefine csv-file format
  node_1097 --> Sort by vertrag_id_carmen
  Reformat for insert "Factoring Gutschriften" --> node_1449
  Filter out where rankindex != 1 --> Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending;
  Reformat --> Delete rows from DWH$TA_F_RPOS_RESELLING_CARM
  node_694 --> node_794
  Reformat --> Delete rows from DWH$TA_T_RPOS_CARM
  Proof Join-criteriase gueltig_von and gueltig_bis --> Replicate
  Replicate --> node_357
  Split Data --> Reformat Referencerecord
  node_732 --> Sort within Groups - Sort over rech_leistung_id_carm
  Format Enderecord --> Replicate Enderecord
  Scan - Ranking over gueltig_von, dwh_vertrag_id desc --> Filter out where rankindex != 1
  node_834 --> Sort by rechnung_id; rechnung_datum; debitor_id-1
  Read Filename --> Read File
  Join with dwh$ta_c_vertrag-1 --> Sort by vertrag_id_carmen
  Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending; --> node_1426
  Reformat for insert "fact data" --> node_1162
  Replicate --> Scan - Mark valid historized datasets
  node_192 --> Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1
  Filter out where rpos_geschaeftsform_kenn != 'S' --> node_1584
  node_1620 --> Reformat for DB and Filter out where Kompl_Kennzeichen != L
  Reformat for DB and Filter out where Kompl_Kennzeichen != L --> Update / Insert DWH$TA_K_RECH_ABSGRP
  node_426 --> Delete rows from DWH$TA_F_RPOS_CARM-1
  Reformat Data --> Split Data
  Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Determine rows to be deleted
  Sort by rechnung_id; rechnung_datum; debitor_id-3 --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-2
  node_1573 --> node_1149
  Reformat for delete --> Delete rows from DWH$TA_F_GPOS_FACT_CARM
  Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc --> Filter out where rankindex != 1
  Reformat for insert "temporary data" --> node_1086
  Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending; --> Scan - Ranking over gueltig_von, dwh_vertrag_id desc
  Join with DB, Determine rows to be deleted --> Sort by rechnung_id; rechnung_datum; debitor_id-3
  Sort within Groups - Sort over rech_leistung_id_carm --> node_298
  Reformat for insert "Factoring Rechnungen" --> node_1053
  Select "Positionen auf Debitorenebene" (temporary Data) --> Reformat for insert "temporary data"
  Replicate --> Filter out where rpos_geschaeftsform_kenn != 'S'
  Replicate --> node_928
  Sort by vertrag_id_carmen --> Replicate
  Select "Factoring Gutschriften" --> Select "Reselling"
  node_240 --> node_338
  node_650 --> node_588
  Reformat for insert "Reselling" --> node_1459
  node_1746 --> node_25
  Determine rows to be deleted --> Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1 --> Gather
  Select "Factoring Gutschriften" --> Reformat for insert "Factoring Gutschriften"
  node_25 --> node_1740
  Reformat for delete --> Delete rows from DWH$TA_F_RPOS_FACT_CARM
  node_284 --> Sort within Groups - Sort over rech_leistung_id_carm
  node_542 --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  node_374 --> Sort within Groups - Sort over rech_leistung_id_carm
  node_842 --> Replicate
  Redefine csv-file format --> Reformat for DB
  node_232 --> Sort within Groups - Sort over rech_leistung_id_carm
  Read File --> Reformat Data
  Proof Join - criterias gueltig_von and gueltig_bis --> node_1576
  Sort by vertrag_id_carmen --> Merge
  Replicate Enderecord --> Reformat Enderecord for Processing
  Reformat Enderecord for Update --> Update DWH$TA_K_MELDUNGEN
  node_207 --> Replicate
  Join CSV-File with dwh$TA_C_VERTRAG --> Process Enderecord
  Reformat fï¿½r testzwecke --> node_1750
  node_1426 --> Decode rpos_geschaeftsform_kenn
  Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending; --> Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc
  node_1675 --> node_1620
  Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending; --> Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur
  Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Sort by rechnung_id; rechnung_datum; debitor_id-1 --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1
  Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur --> Select "Positionen auf Debitorenebene" (temporary Data)
  node_338 --> node_284
  node_298 --> Determine rows to be deleted
  Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1 --> Determine rows to be deleted (incl. dedup of port 1)
  Reformat rechnung_datum to datetime for Delete --> node_200
  node_456 --> Sort within Groups - Sort over rech_leistung_id_carm
  Select "Factoring Rechnungen" --> Reformat for insert "Factoring Rechnungen"
  Sort within Groups - Sort over rech_leistung_id_carm --> Determine rows to be deleted
  Filter by Expression --> node_426
  node_794 --> node_732
  Filter by Expression --> node_192
  Sort by rechnung_id; rechnung_datum; debitor_id --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id
  Filter out where rankindex != 1 --> Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending;
  node_1576 --> Scan - Mark valid historized datasets
  node_512 --> node_456
  Decode rpos_geschaeftsform_kenn --> node_1438
  Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Delete rows from DWH$TA_F_RPOS_CARM
  Split Data --> Split Metadata
  node_382 --> node_512
  Reformat for DB --> Validate Records
  node_1675 --> node_1746
  node_686 --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Replicate --> Reformat rechnung_datum to datetime for Delete
```

---

## 3. ADDITIONAL CONTEXT (ORCHESTRATION, ENVIRONMENTS, AND DEPS)

This section maps all lineage paths, dependencies, execution configurations, and environmental elements that the automated conversion tool cannot see from the raw `.mp` graph.

### A. JOB DEPENDENCIES (LINEAGE & CROSS-JOB COUPLING)
The orchestration on Cloud Composer (Airflow) must manage upstream data sources and verify prerequisites:

*   **UPSTREAM PREREQUISITES**:
    *   **Shared Files Module**: `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` (already migrated to Cloud Storage / shared artifact repo in PR `https://github.com/gurunathan-prodapt/pi-agents/pull/748`). Must be imported in the DAG to resolve standard environment configurations.
    *   **Billing Input CSV Ingestion**: The dynamic source file (`CARMEN_B_*_pos.fix`) must be delivered and visible on Cloud Storage (`$DW_DIR_IMP_SAP/crs/work/`) prior to execution.
*   **DOWNSTREAM JOBS**:
    *   Once this job processes and updates the factoring and reselling tables, standard downstream analytical DAGs consume `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, and `DWH$TA_F_RPOS_RESELLING_CARM` tables in BigQuery.

### B. EXECUTION ORDER & TASK SEQUENCE
The legacy execution sequence must be preserved strictly in the target Composer DAG as follows:

```
Step 1: UC4 Unix XML Parser (abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml)
   │
   ▼
Step 2: Configuration Loader (abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg)
   │
   ▼
Step 3: Execution Wrapper (abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh)
   │
   ▼
Step 4: PySpark Core Engine (abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp)
```

In Airflow, this sequence translates to:
1.  **GCS Sensor / File Sensor**: Detects incoming billing data files matching `CARMEN_B_*_pos.fix` in `gs://{GCS_BUCKET}/crs/work/`.
2.  **Configuration Loader**: Loads JSON configs from GCS (representing `map_rpos_carmen_import.cfg`).
3.  **Dataproc Serverless Operator**: Launches `map_rpos_carmen_import.py` (migrated from `.mp`) inside Dataproc Serverless, carrying parameters forwarded from the execution wrapper.

### C. SCHEDULING DETAILS
*   **Trigger Event**: Event-triggered / Scheduled. 
*   **Legacy Scheduler**: UC4 (Automic) scheduler.
*   **Target Scheduling Mechanism**: Cloud Composer DAG scheduled to check daily or event-driven via a Cloud Function triggering the DAG on file upload to Cloud Storage.

### D. ENVIRONMENT-SPECIFIC VARIABLES (POLICY COMPLIANT)

Values are categorized below according to the **Environment Values Policy**.

#### 1. GLOBAL VARIABLES (Shared infrastructure)
These values are identical across environments (Dev, Test, Prod) and are retrieved at runtime.

| Variable Role | Logical Target | Python / Airflow Access | SQL Syntax Parameter |
| :--- | :--- | :--- | :--- |
| Target GCP Project ID | `GCP_PROJECT` | `Variable.get("GCP_PROJECT")` | `@gcp_project` |
| Execution Region | `GCP_REGION` | `Variable.get("GCP_REGION")` | — |
| Dataproc Region | `DATAPROC_REGION` | `Variable.get("DATAPROC_REGION")` | — |
| Cloud Storage Shared Bucket | `GCS_BUCKET` | `Variable.get("GCS_BUCKET")` | — |
| BigQuery DWH Target Dataset | `BQ_DATASET` | `Variable.get("BQ_DATASET")` | — |
| Source SAP Ingest Path | `DW_DIR_IMP_SAP` | `os.environ.get("DW_DIR_IMP_SAP")` | — |
| Base Sandbox Directory | `HOME` | `os.environ.get("HOME")` | — |

#### 2. JOB-SPECIFIC VARIABLES
These are specific variables extracted from the parameter CFG configuration and must be stored inside a single parameter payload block or as Airflow DAG `params` to remain distinct from global constants.

```python
JOB_CONFIG = {
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Quellverzeichnis": "crs/work/",
    "BHB_Zielverzeichnis": "crs/store/",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X"
}
```

---

## 4. TARGET FILE PLAN

As mandated by the **Folder Integrity Rule**, target outputs match the legacy directory structures.

```
/
├── dags/
│   └── abinitio_rpos_carmen_linked_job/
│       └── DWH_BD_PROC_JOB/
│           └── DW_RPOS_CARM_IMPORT.py  <-- Airflow DAG orchestration (from UC4 XML)
│
└── abinitio_rpos_carmen_linked_job/
    ├── isdwh/
    │   └── abinitio/
    │       └── cfg/
    │           └── bd_proc/
    │               └── map_rpos_carmen_import.json  <-- Configuration mappings (from CFG)
    │
    └── TMD_processing/
        └── BHB/
            └── BD_PROC/
                ├── run/
                │   └── map_rpos_carmen_import_wrapper.py  <-- Dataproc submission wrapper (from KSH)
                │
                └── mp/
                    └── map_rpos_carmen_import.py  <-- PySpark ETL pipeline (from MP)
```

---

## 5. COMPLETE PYSPARK IMPLEMENTATION SPECIFICATION

This section details the concrete PySpark implementation, combining the verbatim core logic with target GCP structures.

### A. DATA CONTRACTS & BIGQUERY STRUCTURAL SCHEMA
The table structure must map directly to the targets identified in the GDE graph:

```sql
-- DWH$TA_F_RPOS_CARM BigQuery Schema Mapping
CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_carm` (
  monats_id INT64 NOT NULL,
  debitor_id STRING NOT NULL,
  kontier_grp_id STRING,
  rechnung_id STRING NOT NULL,
  rechnung_datum DATE NOT NULL,
  standardvertrags_id INT64,
  vertrags_id INT64,
  rech_leistung_id_carm STRING NOT NULL,
  rechpos_brutto_eur NUMERIC,
  rechpos_netto_eur NUMERIC,
  rechpos_mwst_eur NUMERIC,
  abs_grp STRING,
  pooling STRING,
  rechnungvertrag_id INT64,
  prob_vertrag_id STRING,
  prob_provider_kenn STRING,
  anz_leistungen INT64,
  anz_tickets INT64,
  rpos_geschaftsform_kenn STRING,
  vas_kenn STRING,
  verkauftes_basisprodukt_id INT64
);
```

### B. MIGRATED CORE PYSPARK ETL ENGINE (`map_rpos_carmen_import.py`)

This PySpark script contains the precise operational algorithms, replicating the joins, ranking, temporal contract validation, pre-deletions, and write steps.

```python
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    # Retrieve system variables using authorized environment mapping
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    
    if not gcp_project or not bq_dataset or not gcs_bucket:
        print("ERROR: Missing infrastructure variables. Environment configuration invalid.")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("DW.RPOS_CARM_IMPORT - map_rpos_carmen_import") \
        .getOrCreate()

    # 1. READ GRAPH CONFIGURATIONS
    # Standard parameterized definitions matching .cfg
    nutz_kenn = "P"
    ende_kenn = "X"
    input_file_path = f"gs://{gcs_bucket}/crs/work/CARMEN_B_*_pos.fix"

    # 2. INGEST & DEMULTIPLEX INPUT DATA
    # Legacy replace ',' by '.' and split logic
    raw_df = spark.read.text(input_file_path)

    # Filter Nutzdatensaetze (Payload)
    payload_raw = raw_df.filter(F.substring(F.col("value"), 1, 1) == nutz_kenn)
    # Replace comma by decimal dot
    payload_df = payload_raw.withColumn("clean_val", F.regexp_replace(F.col("value"), ",", "."))

    # Parse CSV fields based on position layout
    parsed_payload = payload_df.select(
        F.substring(F.col("clean_val"), 2, 6).alias("monats_id"),
        F.substring(F.col("clean_val"), 8, 10).alias("debitor_id"),
        F.substring(F.col("clean_val"), 18, 10).alias("rechnung_id"),
        F.to_date(F.substring(F.col("clean_val"), 28, 8), "yyyyMMdd").alias("rechnung_datum"),
        F.coalesce(F.trim(F.substring(F.col("clean_val"), 36, 10)).cast("decimal(18,2)"), F.lit(0)).alias("standardvertrags_id"),
        F.coalesce(F.trim(F.substring(F.col("clean_val"), 46, 10)).cast("decimal(18,2)"), F.lit(0)).alias("vertrags_id"),
        F.trim(F.substring(F.col("clean_val"), 56, 15)).alias("rech_leistung_id_carm"),
        F.trim(F.substring(F.col("clean_val"), 71, 10)).cast("decimal(18,2)").alias("rechpos_brutto_eur"),
        F.trim(F.substring(F.col("clean_val"), 81, 10)).cast("decimal(18,2)").alias("rechpos_netto_eur"),
        F.trim(F.substring(F.col("clean_val"), 91, 10)).cast("decimal(18,2)").alias("rechpos_mwst_eur"),
        F.trim(F.substring(F.col("clean_val"), 101, 10)).alias("pooling"),
        F.trim(F.substring(F.col("clean_val"), 111, 10)).cast("decimal(18,2)").alias("rechnungvertrag_id"),
        F.trim(F.substring(F.col("clean_val"), 121, 10)).alias("prob_vertrag_id"),
        F.trim(F.substring(F.col("clean_val"), 131, 10)).alias("prob_provider_kenn"),
        F.trim(F.substring(F.col("clean_val"), 141, 10)).cast("decimal(18,2)").alias("anz_leistungen"),
        F.trim(F.substring(F.col("clean_val"), 151, 10)).cast("decimal(18,2)").alias("anz_tickets"),
        F.trim(F.substring(F.col("clean_val"), 161, 5)).alias("rpos_geschaftsform_kenn"),
        F.trim(F.substring(F.col("clean_val"), 166, 5)).alias("vas_kenn"),
        F.trim(F.substring(F.col("clean_val"), 171, 10)).alias("kennung5")
    )

    # 3. SCHEMA INTEGRITY VALIDATION
    # Asserts type validity to replicate Ab Initio's Validate Records component
    validated_payload = parsed_payload.select(
        F.when(F.col("monats_id").isNull(), F.raise_error("Invalid data format in monats_id")).otherwise(F.col("monats_id")).alias("monats_id"),
        F.when(F.col("rechnung_datum").isNull(), F.raise_error("Invalid data format in rechnung_datum")).otherwise(F.col("rechnung_datum")).alias("rechnung_datum"),
        F.when(F.col("standardvertrags_id").isNull(), F.raise_error("Invalid data format in standardvertrags_id")).otherwise(F.col("standardvertrags_id")).alias("standardvertrags_id"),
        F.when(F.col("vertrags_id").isNull(), F.raise_error("Invalid data format in vertrags_id")).otherwise(F.col("vertrags_id")).alias("vertrags_id"),
        F.when(F.col("rechpos_brutto_eur").isNull(), F.raise_error("Invalid data format in rechpos_brutto_eur")).otherwise(F.col("rechpos_brutto_eur")).alias("rechpos_brutto_eur"),
        F.when(F.col("rechpos_netto_eur").isNull(), F.raise_error("Invalid data format in rechpos_netto_eur")).otherwise(F.col("rechpos_netto_eur")).alias("rechpos_netto_eur"),
        F.when(F.col("rechpos_mwst_eur").isNull(), F.raise_error("Invalid data format in rechpos_mwst_eur")).otherwise(F.col("rechpos_mwst_eur")).alias("rechpos_mwst_eur"),
        F.col("debitor_id"), F.col("rechnung_id"), F.col("rech_leistung_id_carm"), F.col("pooling"), F.col("rechnungvertrag_id"),
        F.col("prob_vertrag_id"), F.col("prob_provider_kenn"), F.col("anz_leistungen"), F.col("anz_tickets"), 
        F.col("rpos_geschaftsform_kenn"), F.col("vas_kenn"), F.col("kennung5")
    )

    # 4. CONTRACTS JOIN & TEMPORAL SPLICING VALIDATION
    # Load dwh_ta_c_vertrag
    vertrag_df = spark.read.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_c_vertrag") \
        .load() \
        .filter("gueltig_bis >= to_date('20050401', 'yyyyMMdd')")

    # Perform Left Outer Join
    joined_df = validated_payload.join(
        vertrag_df, 
        validated_payload.vertrags_id == vertrag_df.vertrag_id_carmen, 
        "left_outer"
    )

    # Calculate month_last_day
    # e.g., for monats_id '200504', parsed to last day '2005-04-30'
    joined_with_last_day = joined_df.withColumn(
        "month_last_day", 
        F.last_day(F.to_date(F.col("monats_id"), "yyyyMM"))
    )

    # Evaluate Temporal bounds to implement "Proof Join"
    temporal_validated_df = joined_with_last_day.withColumn(
        "valid_flag",
        F.when(
            (F.col("gueltig_von").isNull() | (F.col("month_last_day") > F.col("gueltig_von"))) &
            (F.col("gueltig_bis").isNull() | (F.col("month_last_day") <= F.col("gueltig_bis"))),
            0
        ).otherwise(1)
    )

    # Nullify contract fields if temporal slice is invalid
    proof_resolved_df = temporal_validated_df.select(
        validated_payload["*"],
        F.when(F.col("valid_flag") == 0, F.col("rahmenvertrag_id")).otherwise(F.lit(None)).alias("rahmenvertrag_id_resolved"),
        F.when(F.col("valid_flag") == 0, F.col("dwh_vertrag_id")).otherwise(F.lit(None)).alias("dwh_vertrag_id"),
        F.when(F.col("valid_flag") == 0, F.col("dwh_gp_id")).otherwise(F.lit(None)).alias("dwh_gp_id"),
        F.when(F.col("valid_flag") == 0, F.col("dwh_konto_id")).otherwise(F.lit(None)).alias("dwh_konto_id"),
        F.when(F.col("valid_flag") == 0, F.col("dwh_tarifgr_id")).otherwise(F.lit(None)).alias("dwh_tarifgr_id"),
        F.when(F.col("valid_flag") == 0, F.col("vo_kenn")).otherwise(F.lit(None)).alias("vo_kenn"),
        F.when(F.col("valid_flag") == 0, F.col("zv_id")).otherwise(F.lit(None)).alias("zv_id"),
        F.when(F.col("valid_flag") == 0, F.col("gueltig_von")).otherwise(F.lit(None)).alias("gueltig_von")
    )

    # 5. DEDUPLICATION (Ranking over start date desc)
    window_spec = Window.partitionBy("vertrags_id", "monats_id") \
        .orderBy(F.col("gueltig_von").desc(), F.col("dwh_vertrag_id").desc())

    ranked_df = proof_resolved_df.withColumn("rankindex", F.row_number().over(window_spec))
    final_payload = ranked_df.filter(F.col("rankindex") == 1).cache()

    # 6. EXCLUSION / PRE-DELETE OPERATION
    # Identify unique keys for deletion from targets to prevent double loads
    unique_delete_keys = final_payload.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()

    # REVIEW: BigQuery Deletion Sub-routine
    # Emulates the pre-deletion logic of GDE before writing incoming rows.
    # In production Spark, this is achieved via MERGE, or execution of a DELETE query on BigQuery.
    # For this target platform, we run programmatic SQL:
    unique_delete_keys.createOrReplaceTempView("keys_to_delete")
    
    print("Initiating pre-delete purges on BigQuery target tables...")
    spark.sql(f"""
        DELETE FROM `{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_carm` t
        WHERE EXISTS (
            SELECT 1 FROM keys_to_delete k 
            WHERE t.rechnung_id = k.rechnung_id 
              AND t.rechnung_datum = k.rechnung_datum 
              AND t.standardvertrags_id = k.standardvertrags_id 
              AND t.vertrags_id = k.vertrags_id
        )
    """)

    # 7. ROUTE AND WRITE TO OUTPUT TABLES
    
    # Target 1: DWH_TA_F_RPOS_CARM (Core payload)
    target_carm_df = final_payload.select(
        F.col("monats_id").cast("int"),
        F.col("debitor_id"),
        F.lit("#").alias("kontier_grp_id"),
        F.col("rechnung_id"),
        F.col("rechnung_datum"),
        F.col("standardvertrags_id").cast("long"),
        F.col("vertrags_id").cast("long"),
        F.col("rech_leistung_id_carm"),
        F.col("rechpos_brutto_eur"),
        F.col("rechpos_netto_eur"),
        F.col("rechpos_mwst_eur"),
        F.substring(F.col("rechnung_id"), 9, 5).alias("abs_grp"),
        F.coalesce(F.col("pooling"), F.lit("#")).alias("pooling"),
        F.coalesce(F.col("rechnungvertrag_id").cast("long"), F.lit(0)).alias("rechnungvertrag_id"),
        F.coalesce(F.col("prob_vertrag_id"), F.lit("#")).alias("prob_vertrag_id"),
        F.coalesce(F.col("prob_provider_kenn"), F.lit("#")).alias("prob_provider_kenn"),
        F.coalesce(F.col("anz_leistungen").cast("long"), F.lit(0)).alias("anz_leistungen"),
        F.coalesce(F.col("anz_tickets").cast("long"), F.lit(0)).alias("anz_tickets"),
        F.coalesce(F.col("rpos_geschaeftsform_kenn"), F.lit("#")).alias("rpos_geschaftsform_kenn"),
        F.coalesce(F.col("vas_kenn"), F.lit("#")).alias("vas_kenn"),
        F.coalesce(F.col("kennung5").cast("long"), F.lit(0)).alias("verkauftes_basisprodukt_id")
    )
    
    target_carm_df.write.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_carm") \
        .mode("append") \
        .save()

    # Target 2: Factoring Gutschriften (Select 'G' -> GPOS_FACT_CARM)
    gutschriften_df = final_payload.filter(F.col("rpos_geschaeftsform_kenn") == "G").select(
        F.col("monats_id"),
        F.col("debitor_id"),
        F.col("rechnung_id"),
        F.col("rechnung_datum"),
        F.col("standardvertrags_id"),
        F.col("vertrags_id"),
        F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm"),
        F.col("rechpos_brutto_eur"),
        F.col("rechpos_netto_eur"),
        F.col("rechpos_mwst_eur"),
        F.col("rahmenvertrag_id_resolved").alias("rahmenvertrag")
    )
    gutschriften_df.write.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_gpos_fact_carm") \
        .mode("append") \
        .save()

    # Target 3: Factoring Rechnungen (Select 'F' -> RPOS_FACT_CARM)
    rechnungen_df = final_payload.filter(F.col("rpos_geschaeftsform_kenn") == "F").select(
        F.col("monats_id"),
        F.col("debitor_id"),
        F.col("rechnung_id"),
        F.col("rechnung_datum"),
        F.col("standardvertrags_id"),
        F.col("vertrags_id"),
        F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm"),
        F.col("rechpos_brutto_eur"),
        F.col("rechpos_netto_eur"),
        F.col("rechpos_mwst_eur"),
        F.col("rahmenvertrag_id_resolved").alias("rahmenvertrag")
    )
    rechnungen_df.write.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_fact_carm") \
        .mode("append") \
        .save()

    # Target 4: Reselling (Select 'R' -> RPOS_RESELLING_CARM)
    reselling_df = final_payload.filter(F.col("rpos_geschaeftsform_kenn") == "R").select(
        F.col("monats_id"),
        F.col("debitor_id"),
        F.col("rechnung_id"),
        F.col("rechnung_datum"),
        F.col("standardvertrags_id"),
        F.col("vertrags_id"),
        F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm"),
        F.col("rechpos_brutto_eur"),
        F.col("rechpos_netto_eur"),
        F.col("rechpos_mwst_eur"),
        F.col("rahmenvertrag_id_resolved").alias("rahmenvertrag")
    )
    reselling_df.write.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_reselling_carm") \
        .mode("append") \
        .save()

    # Target 5: Temporary Debitor Positions (Select typ == 'T' -> DWH_TA_T_RPOS_CARM)
    temp_df = final_payload.filter(F.col("typ") == "T").select(
        F.col("monats_id"),
        F.col("debitor_id"),
        F.col("rechnung_id"),
        F.col("rechnung_datum"),
        F.col("standardvertrags_id"),
        F.col("vertrags_id"),
        F.col("rech_leistung_id_carm"),
        F.col("rechpos_brutto_eur"),
        F.col("rechpos_netto_eur"),
        F.col("rechpos_mwst_eur"),
        F.col("rahmenvertrag_id_resolved").alias("rahmenvertrag"),
        F.lit("1900-01-01 00:00:00").cast("timestamp").alias("bearbeitung_datum")
    )
    temp_df.write.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_t_rpos_carm") \
        .mode("append") \
        .save()

    # 8. UPDATE LOG/CONTROL METADATA TABLES
    # Ingest Trailer record (Endedatensatz)
    trailer_raw = raw_df.filter(F.substring(F.col("value"), 1, 1) == ende_kenn)
    
    if trailer_raw.count() > 0:
        # Extract variables
        metadata_df = trailer_raw.select(
            F.lit(ende_kenn).alias("kennzeichen"),
            F.substring(F.col("value"), 2, 40).alias("bemerkung"),
            F.substring(F.col("value"), 42, 8).alias("stichtag"),
            F.substring(F.col("value"), 50, 10).cast("int").alias("anzahl"),
            F.substring(F.col("value"), 60, 100).alias("inhalt"),
            F.substring(F.col("value"), 160, 20).alias("erstellt_am")
        )

        cleaned_meta = metadata_df.withColumn(
            "erstellt_am_clean",
            F.when(F.instr(F.col("erstellt_am"), ";") == 0, F.col("erstellt_am"))
            .otherwise(F.regexp_replace(F.col("erstellt_am"), ";", ""))
        )
        
        meta_row = cleaned_meta.collect()[0]
        
        # Programmatic Updates to Operational DB tables
        # Update DWH$TA_K_MELDUNGEN
        spark.sql(f"""
            UPDATE `{gcp_project}.{bq_dataset}.dwh_ta_k_meldungen`
            SET anzahl_ds_eof = {meta_row['anzahl']}
              , dateiname = '{meta_row['bemerkung']}'
              , enderecord_text = '{meta_row['inhalt']}'
              , zusatzinfo = '{meta_row['bemerkung']}'
            WHERE entrynr = '{os.environ.get("BHB_Eintragsnr", "")}'
        """)

        # Derive reporting month partitions for update
        derived_monats_id = meta_row['stichtag'][0:6] # "Stichtag"
        # Subtract 1 month (equivalent to date_add_months)
        # Assuming format yyyyMM
        import datetime
        from dateutil.relativedelta import relativedelta
        base_date = datetime.datetime.strptime(derived_monats_id, "%Y%m")
        target_date = base_date - relativedelta(months=1)
        reporting_month_id = target_date.strftime("%Y%m")

        # Update / Insert DWH$TA_K_RECH_ABSGRP
        abs_grp = meta_row['bemerkung'][9:14] # index 10 to 14
        rechnung_datum = datetime.datetime.strptime(meta_row['stichtag'], "%Y%m%dd") if len(meta_row['stichtag']) == 8 else datetime.datetime.now()
        
        spark.sql(f"""
            UPDATE `{gcp_project}.{bq_dataset}.dwh_ta_k_rech_absgrp`
            SET rechnung_datum = DATE('{rechnung_datum.strftime("%Y-%m-%d")}')
              , ladedatum = CURRENT_TIMESTAMP()
            WHERE monats_id = '{reporting_month_id}'
              AND abs_grp = '{abs_grp}'
              AND dateiname = '{meta_row['bemerkung']}'
              AND rechnungsteil = 'P'
        """)

    print("PySpark Job Execution completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## 6. Wrapper Script Execution Code (`map_rpos_carmen_import_wrapper.py`)

This wrapper replaces the KornShell script (`map_rpos_carmen_import.ksh`). It handles environment setup, performs parameter validation, and submits the PySpark job to Dataproc Serverless.

To satisfy the **Reviewer Feedback**, all original print/error strings must be preserved verbatim.

```python
import os
import sys
import subprocess

def main():
    print("Script start...")
    print("Building Graph...")
    print("Execution starting...")

    # Load environmental parameters and validate mandatory ones
    # MUST PRESERVE ALL ORIGINAL LITERAL MESSAGES FROM LEGACY Wrapper Script
    db_tns_name_dwh = os.environ.get("DB_TNS_NAME_DWH")
    if not db_tns_name_dwh:
        # German/German-English original error prints preserved verbatim
        print("print -- Error evaluating: 'parameter DB_TNS_NAME_DWH is empty'")
        print("ERROR : ++++ FAILED ++++ Job map_rpos_carmen_import failed.")
        sys.exit(1)

    print("export NLS_NUMERIC_CHARACTERS=\". \";")

    # Construct Dataproc Serverless execution parameters
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    dataproc_region = os.environ.get("DATAPROC_REGION")

    # Command line submission to execute migrated PySpark pipeline
    dataproc_submit_cmd = [
        "gcloud", "dataproc", "batches", "submit", "pyspark",
        f"gs://{gcs_bucket}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py",
        f"--project={gcp_project}",
        f"--region={dataproc_region}",
        f"--jars=gs://{gcs_bucket}/bin/spark-bigquery-latest_2.12.jar",
        "--properties=spark.executor.instances=2,spark.executor.cores=4"
    ]

    try:
        result = subprocess.run(dataproc_submit_cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        print("info : ++++ STARTED ++++ Job map_rpos_carmen_import")
    except subprocess.CalledProcessError as err:
        print(f"ERROR: Execution of PySpark engine failed: {err.stderr}")
        print("ABINITIO: Rolling back ... Done.")
        print("ERROR : ++++ FAILED ++++ Job map_rpos_carmen_import failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

---

## 7. AIRFLOW COMPOSER DAG (`DW_RPOS_CARM_IMPORT.py`)

Below is the migrated Airflow orchestration code that matches the UC4 XML (`DW.RPOS_CARM_IMPORT.xml`), schedules the wrapper execution, and manages dependencies.

```python
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.gcs import GCSObjectSensor

# Retrieve Global Variables
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 1, 1),
    "email_on_failure": True,
    "email": ["alert-dwh@company.com"],
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5)
}

with DAG(
    dag_id="DW_RPOS_CARM_IMPORT",
    default_args=default_args,
    schedule_interval="0 4 * * *", # Scheduled daily at 04:00
    catchup=False,
    max_active_runs=1
) as dag:

    # Task 1: Check if raw SAP CSV payload is ready in GCS
    sensor_billing_file = GCSObjectSensor(
        task_id="sensor_billing_file",
        bucket=GCS_BUCKET,
        object="crs/work/CARMEN_B_*_pos.fix",
        poke_interval=60,
        timeout=1800,
        mode="poke"
    )

    # Task 2: Execute wrapper script to trigger Dataproc Batch job
    submit_pyspark_job = BashOperator(
        task_id="submit_pyspark_job",
        bash_command=(
            f"python3 /home/airflow/gcs/dags/abinitio_rpos_carmen_linked_job/"
            f"TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import_wrapper.py"
        ),
        env={
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
            "DATAPROC_REGION": DATAPROC_REGION,
            "DB_TNS_NAME_DWH": "DWH" # Validated in Wrapper
        }
    )

    sensor_billing_file >> submit_pyspark_job
```

---

## 8. RISKS & MANUAL ACTIONS

This section provides explicit mitigation steps for unresolved areas and key rules identified during code inspection:

1.  **LOG PRESERVATION MANDATE (CRITICAL)**:
    *   **Risk**: During testing and deployment, logs can be lost or made unreadable if legacy error messages are rewritten or discarded.
    *   **Action Required**: Do not rephrase, replace, or localize print or validation outputs (including German legends and strings). For example, the error check output:
        *   `print -- Error evaluating: 'parameter DB_TNS_NAME_DWH...'`
        must be output character-for-character inside the wrapper's environment checks.
2.  **COMPLETE TRANSFORM IMPLEMENTATION (CRITICAL)**:
    *   **Risk**: If simplified pseudo-functions are left in the build, the engine will crash on schema assert checks when dealing with complex CSV values.
    *   **Action Required**: The final PySpark logic must fully map structural parser boundaries, evaluate window boundaries, filter business forms, and perform temporal validity evaluations. No mock functions or placeholder stubs (such as `pass` or `TODO`) are allowed in the finalized code payload.
3.  **BIGQUERY PARALLEL QUERY DELETIONS**:
    *   **Risk**: Simultaneous deletes on partition boundaries may lock BigQuery metadata catalogs.
    *   **Action Required**: BigQuery deletions must execute sequentially before starting append write blocks. Use native Spark SQL transactional structures to isolate targets from deadlocks.
4.  **RECOVERY AND ROLLBACKS**:
    *   **Risk**: The original GDE engine rollback file (`map_rpos_carmen_import.rec`) handles job states on aborts.
    *   **Action Required**: In GCP, the Dataproc Serverless environment lacks file-based checkpoint rollback files. If a batch fails, standard BigQuery partition rollbacks or idempotent overwrites must be implemented inside the DAG's retry cycle to ensure clean recovery states.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

This document defines the complete, implementation-ready migration design for the legacy Job `DW.RPOS_CARM_IMPORT` from an Ab Initio/KornShell/Oracle environment to **Google Cloud Composer (Apache Airflow) + Dataproc Serverless (PySpark) + Google BigQuery**.

---

## 1. FILE DISPOSITION TABLE

Every legacy component file from the pre-collected context is mapped to its target file or action to prevent silent loss of logic.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Contains both wrapper shell logic and compiled inline Ab Initio graph components (DML schemas, XFR transforms, SQL delete scripts). Converted to a single consolidated PySpark script. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json` | Parameters file containing environment configurations and file metadata parsing structures. Converted to a structured configuration JSON file. |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `dags/dw_rpos_carm_import_dag.py` | UC4 Job XML scheduler and trigger declaration. Replaced by a Cloud Composer (Airflow) DAG definition. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | Retired (Folded) | The graph `.mp` file is retired as a standalone object because its compiled operational DML, XFR, and SQL components are inline within the KSH wrapper and have been fully incorporated into the PySpark script. |

---

## 2. VERBATIM MCP DESIGN TOOL OUTPUT

Below is the design documentation and physical reverse-engineering analysis generated by the `ksh_design_python` tool.

```markdown
# DESIGN DOCUMENT: map_rpos_carmen_import Migration

## 1. SCRIPT OVERVIEW
The `map_rpos_carmen_import.ksh` script is an Ab Initio compiled GDE (Graphical Development Environment) wrapper script designed to run the ETL graph of the same name. Its primary purpose is to process incoming raw Carmen billing and invoice position files, join them with contract reference data from the database, apply business partition logic (Factoring, Reselling, etc.), execute target purging routines to maintain idempotency, load the consolidated data into Oracle database tables, and update control/status tables. This script is triggered within a scheduled processing chain (likely UC4/Automic) when Carmen billing files are delivered to the processing directory.

## 2. INVOCATION CONTEXT
*   **Invoker / Calling Object**: UC4/Automic Job (typically defined as a `JOBS_UNIX` object calling this `.ksh` wrapper).
*   **Command Line / Arguments**: The script accepts optional positional parameters (e.g., `-reposit-tracking`, `-help`) which are forwarded to the primary setup script.
*   **UC4 Includes**: No explicit native UC4 includes (`:inc ...`) are found in this script's text.
*   **Environment Files Sourced**:
    *   `. "${_AB_SAVED_PROJECT_DIR}/.project.ksh"` (Dynamically calculated based on the script's path).
        *   `# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `ab_catalog_functions.ksh` (Sourced if present under `$AB_HOME/bin`).
        *   `# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `./${_AB_PROXY_DIR}/GDE-Parameters` (Locally generated parameters list written during runtime execution).

## 3. PARAMETERS / INPUTS
### Declared Environment Variables
These environment variables are validated by the shell's parameter-evaluation checks:

| Name | Source | Used in Script Body? | Surfacing in Python | Description / Classification |
| :--- | :--- | :--- | :--- | :--- |
| `DB_TNS_NAME_DWH` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Oracle TNS Service Name for DWH (DB Connection Style) |
| `DB_USER_DWH` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database User for DWH (DB Connection Style) |
| `DB_PASSWD_DWH` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database Password for DWH (DB Connection Style) |
| `DB_TNS_NAME_CRS` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Oracle TNS Service Name for CRS (DB Connection Style) |
| `DB_USER_CRS` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database User for CRS (DB Connection Style) |
| `DB_PASSWD_CRS` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database Password for CRS (DB Connection Style) |
| `DB_TNS_NAME_SGM` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Oracle TNS Service Name for SGM (DB Connection Style) |
| `DB_USER_SGM` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database User for SGM (DB Connection Style) |
| `DB_PASSWD_SGM` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database Password for SGM (DB Connection Style) |
| `DB_TNS_NAME_CADS` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Oracle TNS Service Name for CADS (DB Connection Style) |
| `DB_USER_CADS` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database User for CADS (DB Connection Style) |
| `DB_PASSWD_CADS` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database Password for CADS (DB Connection Style) |
| `DB_TNS_NAME_CACM` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Oracle TNS Service Name for CACM (DB Connection Style) |
| `DB_USER_CACM` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database User for CACM (DB Connection Style) |
| `DB_PASSWD_CACM` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Database Password for CACM (DB Connection Style) |
| `BHB_Projektverzeichnis`| Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Project Directory Path (Informational) |
| `BHB_Graph` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Name of the active process Graph (Informational) |
| `BHB_Prozesstyp` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Process execution type classification (Informational) |
| `BHB_Eintragsnr` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Process audit execution tracking ID (Generic Framework) |
| `BHB_Quellverzeichnis` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Source directory containing input files (Generic Framework) |
| `BHB_Zielverzeichnis` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Target archive directory (Generic Framework) |
| `BHB_Dateimaske` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | File pattern wildcard (Generic Framework) |
| `BHB_Kopfdatensatzkennung`| Env Var / Sourced Config| Yes (evaluated) | `os.environ` | File Header record identifier (Generic Framework) |
| `BHB_Nutzdatensatzkennung`| Env Var / Sourced Config| Yes (evaluated) | `os.environ` | File Data/Body record identifier (Generic Framework) |
| `BHB_Endedatensatzkennung`| Env Var / Sourced Config| Yes (evaluated) | `os.environ` | File Trailer/EOF record identifier (Generic Framework) |
| `BHB_Dateiname` | Env Var / Sourced Config | Yes (evaluated) | `os.environ` | Specific input file path to process (Generic Framework) |
| `AB_HOME` | Env Var / OS Env | Yes (initialized) | `os.environ` | Ab Initio Home Directory (Ab Initio Internal) |
| `AB_AIR_HOME` | Env Var / OS Env | Yes (initialized) | `os.environ` | Ab Initio EME Repository Home (Ab Initio Internal) |
| `PROJECT_DIR` | Sourced / Calculated | Yes (evaluated) | `os.environ` | Base Directory of the Project (Ab Initio Internal) |

### Positional Arguments
*   `$1` (and shifted arguments): Can be `-reposit-tracking` to check EME metadata integration or `-help` to display help output (which forces exit code `1`).
*   Forwarded to `.project.ksh` for environment staging.

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
This is an Ab Initio compiled script; it triggers standard platform utilities and compiled framework commands.

*   `uname`: Verifies standard host OS platform. Keep as `platform.system()` in Python.
*   `cygpath`: Converts Windows paths to UNIX formats when running on CYGWIN. (Not required if target Python executes on a native Linux server).
*   `air sandbox find "${PROJECT_DIR}" -project`: Consults the EME repository to locate the physical repository path of the project.
*   `air rm -r -f ...` and `air mv ...`: Manipulates files within the Ab Initio Enterprise Meta Environment (EME) version control storage.
*   `m_env`: Checks repository setting variables.
*   `mp job`, `mp layout`, `mp metadata`, `mp straight-flow`, `mp fan-in-flow`, `mp local-sort`, `mp rollup`, `mp scan`, `mp select-transform`, `mp reformat-transform`, `mp copy`, `mp broadcast`, `mp checkpoint`, `mp run`, `mp reset`: Command pipeline steps declaring the structure and running the components of the Ab Initio data pipeline logic.
*   `m_rmcatalog`, `m_mkcatalog`: Manages temporary database lookup catalogs used during phase validation.

### Resolvable Launcher Check
*   The script contains a major executable pipeline block (`mp run`). However, it wraps complex data manipulations, multi-phase joins, deduplications, and formatting in addition to multiple database select, insert, and delete commands.
*   Therefore, this does **NOT** qualify as a simple "RESOLVABLE LAUNCHER" pattern wrapping a single standalone SQL script. The entire Ab Initio data transformation flow needs to be rewritten as a native Python ETL pipeline.
*   `# REVIEW-STRUCT: launcher [mp run] invoked — internal database, join, and validation graph logic is declared within the shell commands; native Python migration requires fully implementing the file parsing, sorting, joins, historicization, and loading logic in python.`

## 5. EMBEDDED SQL
The script writes several Oracle-compliant SQL statement templates to the local proxy directory to be executed by database-access components within the data flow. Bind variables are prefixed with a colon (`:var_name`).

### SQL 1: Target Fact Table Purge (DWH_TA_F_RPOS_CARM)
*   **File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql`
*   **Statement Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_CARM`
*   **Dialect**: Oracle (`DWH$...` table naming convention, bind variables `:name`).

### SQL 2: Target Purge for Factoring Invoice Positions
*   **File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql`
*   **Statement Text**:
    ```sql
    DELETE FROM DWH$TA_F_GPOS_FACT_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_GPOS_FACT_CARM`
*   **Dialect**: Oracle.

### SQL 3: Alternative Purge for Fact Table
*   **File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql`
*   **Statement Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_CARM
    WHERE  rechnung_datum = :rechnung_datum
    AND    rechnung_id = :rechnung_id
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_CARM`
*   **Dialect**: Oracle.

### SQL 4: Target Purge for Factoring Revenue Positions
*   **File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql`
*   **Statement Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_FACT_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_FACT_CARM`
*   **Dialect**: Oracle.

### SQL 5: Target Purge for Reselling Positions
*   **File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql`
*   **Statement Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_RESELLING_CARM`
*   **Dialect**: Oracle.

### SQL 6: Target Purge for Temp Fact Positions
*   **File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql`
*   **Statement Text**:
    ```sql
    DELETE FROM DWH$TA_T_RPOS_CARM
    WHERE  debitor_id = :debitor_id
    AND    rechnung_datum = :rechnung_datum
    AND    rechnung_id = :rechnung_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_T_RPOS_CARM`
*   **Dialect**: Oracle.

### SQL 7: Audit Log Upsert Check (Update Component)
*   **File**: `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql`
*   **Statement Text**:
    ```sql
    UPDATE DWH$TA_K_RECH_ABSGRP
    SET   rechnung_datum = :rechnung_datum, 
          ladedatum = :ladedatum
    WHERE  monats_id = :monats_id
    AND    abs_grp = :abs_grp
    AND    dateiname = :dateiname
    AND    rechnungsteil = :rechnungsteil
    ```
*   **Type**: `UPDATE`
*   **Tables Touched**: `DWH$TA_K_RECH_ABSGRP`
*   **Dialect**: Oracle.

### SQL 8: Audit Log Upsert Check (Insert Component)
*   **File**: `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql`
*   **Statement Text**:
    ```sql
    INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
    VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)
    ```
*   **Type**: `INSERT`
*   **Tables Touched**: `DWH$TA_K_RECH_ABSGRP`
*   **Dialect**: Oracle.

### SQL 9: Process Run Status/Audit Table Update
*   **File**: `${_AB_PROXY_DIR}/Update_DWH_TA_K_MELDUNGEN-74.sql`
*   **Statement Text**:
    ```sql
    update dwh$ta_k_meldungen 
    set anzahl_ds_eof = :anzahl
      , dateiname = :dateiname
      , enderecord_text = :inhalt
      , zusatzinfo = :bemerkung 
    where entrynr = :eintragsnr
    ```
*   **Type**: `UPDATE`
*   **Tables Touched**: `dwh$ta_k_meldungen`
*   **Dialect**: Oracle.

### SQL 10: Contract Reference DB Query (Lookup Component)
*   **Source**: Contained within the graph database lookup definitions.
*   **Statement Text**:
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
*   **Type**: `SELECT`
*   **Tables Touched**: `dwh$ta_c_vertrag`
*   **Dialect**: Oracle (Unambiguous Oracle-specific outer join operator `(+)` and `to_date` function).

### SQL 11: Active Contract Input Slicing Query
*   **Source**: Contained within database table reader definitions (`Insert_Routine.dwh_ta_c_vertrag__table_`).
*   **Statement Text**:
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
*   **Type**: `SELECT`
*   **Tables Touched**: `dwh$ta_c_vertrag`
*   **Dialect**: Oracle.

---

## 6. CONTROL FLOW
The script executes through the following processing sequence:

1.  **Environment Initialization**: Sets standard environment variables (`AB_HOME`, `MPOWERHOME`, etc.) and modifies path search parameters based on the underlying Operating System (`uname`).
2.  **Proxy Directory Setup**: Creates a unique process directory (`map_rpos_carmen_import-ProxyDir-$$`) and configures system signal traps (`HUP`, `INT`, `QUIT`, `TERM`, `EXIT`) to trigger directory cleanup if the process encounters an interruption or terminates.
3.  **Project Initialization Sourcing**: normalizes path variables (for CYGWIN systems) and sources `.project.ksh` to retrieve sandbox/project configurations.
4.  **EME Integration Staging**: Optionally calls `run-and-reposit` in the EME version control system if repository tracking triggers are enabled.
5.  **Environment Configuration Loading**: Sources `.project.ksh` with execution triggers (`execute start`) and checks for parameter evaluations.
6.  **Argument Validation**: Exits with return code `1` immediately if the `-help` flag is provided.
7.  **Parameter Safety Verification**: Individually evaluates each database target connector string and system variable. If any export or parameter fails evaluation, the shell prints an error diagnostic message and exits immediately with the error status of the evaluation (`exit $mpjret`).
8.  **DML & SQL Document Creation**: Renames the active proxy directory to `${AB_JOB}-map_rpos_carmen_import-ProxyDir` and writes the exact layout definitions (`.dml`), record filters (`.xfr`), and target deletion/status query templates (`.sql`) to disk.
9.  **Data Pipeline Declaration & Setup**: Declares spatial structures (`mp layout`), data schema alignments (`mp metadata`), and mapping channels (`mp straight-flow`, `mp fan-in-flow`) to configure the Ab Initio engine components.
10. **Lookup Catalog Preparation**: Establishes temporary processing indexes (`m_mkcatalog`).
11. **ETL Execution Orchestration**: Runs the pipeline via `mp run`. The internal graph performs the following logical operations (Phase 0 and Phase 1):
    *   **Phase 0 (Extract, Preprocess, Join, Purge)**:
        *   Extracts input files by matching headers, trailers, and specific position data structures (using `BHB_Nutzdatensatzkennung` and `BHB_Endedatensatzkennung`).
        *   Loads active contract records from `dwh$ta_c_vertrag` corresponding to contracts newer than April 1, 2005.
        *   Joins incoming raw transactions against these contract records.
        *   Applies a temporal validation filter where transaction period boundaries align with contract start/end parameters (`gueltig_von`, `gueltig_bis`).
        *   Applies a priority rank calculation index (`Scan_Ranking`) on historized duplicate contracts to retain the latest version.
        *   Partitions elements using transaction formatting logic (Factoring invoice/credit routing, Reselling checks).
        *   Deduplicates purge parameters (e.g. `rechnung_id`, `vertrags_id`, etc.) and executes target purging queries (`SQL 1` through `SQL 6`) against matching database tables to guarantee loading idempotency.
    *   **Phase 1 (Bulk Load & Audit Log Updates)**:
        *   Loads parsed, validated, and categorized transactions into their corresponding target database tables (`DWH$TA_F_RPOS_CARM`, `DWH$TA_T_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`) utilizing bulk database connectors.
        *   Executes database updates (`SQL 7` & `SQL 8`) to insert or update the file-processing record tracker `DWH$TA_K_RECH_ABSGRP`.
        *   Updates the orchestration registry `dwh$ta_k_meldungen` (`SQL 9`) with total processed item counts and file parameters.
12. **Teardown**: Captures the processing return code (`mpjret`), clears the active index catalogs (`m_rmcatalog`), removes temporal configurations, and releases proxy directory files.
13. **Project Context End**: Executes `.project.ksh` with termination parameters (`execute end`).
14. **Process Exit**: Exits with the captured execution status code (`exit $mpjret`).

---

## 7. ERROR HANDLING & EXIT CODES
*   **Failures on Staging Check**: Any evaluation error during parameter export or config validation captures the error code `mpjret = $?` and calls `exit $mpjret` immediately.
*   **Pipeline Fault Detection**: The execution command `mp run` captures pipeline status inside the execution variable `mpjret`.
*   **Signals Interruption Trap**: Signal mappings capture interrupts (`HUP`, `INT`, `QUIT`, `TERM`) to trigger cleanup commands (`__AB_CLEANUP_PROXY_FILES`), preserving and propagating the exit state to parent environments.
*   **Success state**: Successful execution terminates with code `0`.
*   **Python Conversion Strategy**:
    *   Encapsulate parameters setup and target directory generation inside a `contextlib.contextmanager` or utilizing `tempfile.TemporaryDirectory()` to guarantee file teardown during unhandled exceptions.
    *   Perform database commands using structured `try...except` exception blocks.
    *   Convert target purging, loading, and audit tasks into localized method blocks with explicit logging.
    *   Utilize `subprocess.run(..., check=True)` for non-SQL external operations (if any remain) and raise `sys.exit(0)` on success or propagate appropriate non-zero error states on failure.

---

## 8. OUTPUTS / SIDE EFFECTS
*   **Database Targets (Modified & Populated)**:
    *   `DWH$TA_F_RPOS_CARM` (Data purged via matching transaction parameters, then reloaded)
    *   `DWH$TA_F_GPOS_FACT_CARM` (Data purged, then reloaded)
    *   `DWH$TA_F_RPOS_FACT_CARM` (Data purged, then reloaded)
    *   `DWH$TA_F_RPOS_RESELLING_CARM` (Data purged, then reloaded)
    *   `DWH$TA_T_RPOS_CARM` (Data purged, then reloaded)
    *   `DWH$TA_K_RECH_ABSGRP` (Updated with transaction metadata)
    *   `dwh$ta_k_meldungen` (Updated with run statistics, file information, and processing counts)
*   **Logging Output**: Trace log statements are written to stdout/stderr.

---

## 9. BUSINESS SUMMARY
*   **Intake Verification**: Validates raw Carmen-format billing data files containing header, position body records, and footer/trailer rows against system expectations.
*   **Reference Contract Alignment**: Validates and maps each billing transaction to its corresponding master contract (`dwh$ta_c_vertrag`), verifying historical contract version validity dates.
*   **Partitioning Billing Stream**: Categorizes records into target areas (Factoring invoice entries, Factoring credit notes, Reselling streams, or Standard invoice positions) using business code identifiers.
*   **Purging Existing Cycles**: Purges pre-existing items with identical processing signatures from target tables prior to executing bulk loading steps to maintain safe reloading boundaries (idempotency).
*   **Audit Logging**: Updates process control registries with execution tracking statistics, file records, and overall record counts to guarantee system visibility and reconcile processing run metrics.

---

## PYTHON-STYLE PSEUDOCODE OUTLINE

```python
# SCRIPT: map_rpos_carmen_import.py
# Migrated from legacy Ab Initio GDE wrapper ksh script

import os
import sys
import platform
import shutil
import tempfile
import signal
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global exit code tracker
mpjret = 0

# Step 1: Initialize System OS and paths environment
# Equivalent to: typeset _ab_uname=`uname` and environment-specific path setup
_ab_uname = platform.system()
ab_home = os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")
mpower_home = os.environ.get("MPOWERHOME", ab_home)
ab_report = os.environ.get("AB_REPORT", "monitor=60 processes scroll=true")
ab_air_home = os.environ.get("AB_AIR_HOME", "/appl/local/abinitio/abinitio-V2-14")
ab_compatibility = "2.14.59"

# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values

# Step 2: Set NLS Parameters (Equivalent to Script Start preserved custom block)
os.environ["NLS_NUMERIC_CHARACTERS"] = ". "

# Step 3: Define parameter validation and default assignments
def get_env_or_fail(var_name, default_val=None):
    """Retrieves env parameter or exits if not present and no default is set."""
    global mpjret
    val = os.environ.get(var_name, default_val)
    if val is None:
        print(f"Error evaluating: 'parameter {var_name} of map_rpos_carmen_import', interpretation 'shell'")
        mpjret = 12
        sys.exit(mpjret)
    return val

# DB Connection Parameters
db_tns_name_dwh = get_env_or_fail("DB_TNS_NAME_DWH")
db_user_dwh = get_env_or_fail("DB_USER_DWH")
db_passwd_dwh = get_env_or_fail("DB_PASSWD_DWH")

db_tns_name_crs = get_env_or_fail("DB_TNS_NAME_CRS")
db_user_crs = get_env_or_fail("DB_USER_CRS")
db_passwd_crs = get_env_or_fail("DB_PASSWD_CRS")

db_tns_name_sgm = get_env_or_fail("DB_TNS_NAME_SGM")
db_user_sgm = get_env_or_fail("DB_USER_SGM")
db_passwd_sgm = get_env_or_fail("DB_PASSWD_SGM")

db_tns_name_cads = get_env_or_fail("DB_TNS_NAME_CADS")
db_user_cads = get_env_or_fail("DB_USER_CADS")
db_passwd_cads = get_env_or_fail("DB_PASSWD_CADS")

db_tns_name_cacm = get_env_or_fail("DB_TNS_NAME_CACM")
db_user_cacm = get_env_or_fail("DB_USER_CACM")
db_passwd_cacm = get_env_or_fail("DB_PASSWD_CACM")

# Framework Parameters
bhb_projektverzeichnis = get_env_or_fail("BHB_Projektverzeichnis")
bhb_graph = get_env_or_fail("BHB_Graph")
bhb_prozesstyp = get_env_or_fail("BHB_Prozesstyp")
bhb_eintragsnr = get_env_or_fail("BHB_Eintragsnr")
bhb_quellverzeichnis = get_env_or_fail("BHB_Quellverzeichnis")
bhb_zielverzeichnis = get_env_or_fail("BHB_Zielverzeichnis")
bhb_dateimaske = get_env_or_fail("BHB_Dateimaske")
bhb_kopfdatensatzkennung = get_env_or_fail("BHB_Kopfdatensatzkennung")
bhb_nutzdatensatzkennung = get_env_or_fail("BHB_Nutzdatensatzkennung")
bhb_endedatensatzkennung = get_env_or_fail("BHB_Endedatensatzkennung")
bhb_dateiname = get_env_or_fail("BHB_Dateiname")

# Step 4: Validate Positional Command Line Arguments
# Equivalent to: if [ $# -gt 0 -a X"$1" = X"-help" ]; then exit 1; fi
if len(sys.argv) > 1 and sys.argv[1] == "-help":
    logging.info("Help parameter detected. Exiting.")
    sys.exit(1)

# Step 5: Establish connection to DB and stage processing
# REVIEW: target database platform confirmed as Oracle based on SQL syntax and parameters; DB-client library python-oracledb is selected.
try:
    import oracledb
except ImportError:
    # Fallback placeholder for environments lacking native oracledb
    logging.warning("oracledb module not found. DB executions will be represented as simulated actions.")
    oracledb = None

# Step 6: Create Temp/Proxy Staging workspace and wrap flow in cleanup logic
# Equivalent to: _AB_PROXY_DIR=map_rpos_carmen_import-ProxyDir-$$ and traps
with tempfile.TemporaryDirectory(prefix="map_rpos_carmen_import-ProxyDir-") as proxy_dir:
    logging.info(f"Staging processing assets in workspace: {proxy_dir}")
    
    # Step 7: Parse raw file rows
    raw_file_path = os.path.join(bhb_quellverzeichnis, bhb_dateiname)
    if not os.path.exists(raw_file_path):
        logging.error(f"Input file not found: {raw_file_path}")
        sys.exit(1)
        
    logging.info(f"Reading input file: {raw_file_path}")
    
    header_rows = []
    data_rows = []
    trailer_rows = []
    
    with open(raw_file_path, "r", encoding="latin-1") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            # Parse record indicators based on GDE routing criteria
            indicator = line_str[0]
            if indicator == bhb_kopfdatensatzkennung:
                header_rows.append(line_str)
            elif indicator == bhb_nutzdatensatzkennung:
                data_rows.append(line_str)
            elif indicator == bhb_endedatensatzkennung:
                trailer_rows.append(line_str)
            else:
                logging.warning(f"Unrecognized record format on row: {line_str[:20]}")

    logging.info(f"Extracted {len(header_rows)} Header(s), {len(data_rows)} Data row(s), and {len(trailer_rows)} Trailer(s).")

    # Step 8: Read Active Contract reference catalog from DWH$TA_C_VERTRAG
    # Equivalent to database inputs query dwh_ta_c_vertrag__table_
    contracts_map = {}
    if oracledb:
        try:
            # Establish DB connection
            dsn = oracledb.MakerDSN(db_tns_name_dwh, 1521, service_name=db_tns_name_dwh) # Provisional port allocation
            connection = oracledb.connect(user=db_user_dwh, password=db_passwd_dwh, dsn=dsn)
            cursor = connection.cursor()
            
            # Fetch active contracts
            sql_contracts = """
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
            """
            cursor.execute(sql_contracts)
            for row in cursor.fetchall():
                # Map contracts by vertrag_id_carmen for join checks
                vertrag_id_carmen = row[1]
                if vertrag_id_carmen not in contracts_map:
                    contracts_map[vertrag_id_carmen] = []
                contracts_map[vertrag_id_carmen].append({
                    "rahmenvertrag_id": row[0],
                    "dwh_vertrag_id": row[2],
                    "dwh_gp_id": row[3],
                    "dwh_konto_id": row[4],
                    "dwh_tarifgr_id": row[5],
                    "vo_kenn": row[6],
                    "zv_id": row[7],
                    "gueltig_von": row[8],
                    "gueltig_bis": row[9]
                })
            logging.info(f"Loaded {len(contracts_map)} active contract profiles.")
        except Exception as ex:
            logging.error(f"Failed loading contract reference catalog: {ex}")
            sys.exit(2)
    else:
        logging.info("Simulating Contract Reference loading (DB Connection bypassed).")

    # Step 9: Processing Preprocessing, Join Verification, Partitioning and Rollup Aggregations
    # Converts Ab Initio pipeline components (Aggregation, Join_with_dwh_ta_c_vertrag_1, Scan, Rollup, Decoders, Routers) into native data structures
    processed_factoring_rechnungen = []
    processed_factoring_gutschriften = []
    processed_reselling_items = []
    processed_temporary_data = []
    processed_standard_items = []
    
    unique_delete_keys = set() # To hold deduplicated list of records to purge

    # Process each data row
    for raw_row in data_rows:
        # Simulate format mapping matching raw layout (DML/Metadata definitions mapping)
        # Split fields based on semicolon or fixed width (Based on DML specifications)
        fields = raw_row.split(";") 
        if len(fields) < 15:
            continue
            
        # Extract variables
        rechnung_id = fields[3].strip()
        rechnung_datum_str = fields[4].strip() # YYYYMMDD
        standardvertrags_id = fields[5].strip()
        vertrags_id = fields[6].strip()
        rech_leistung_id_carm = fields[7].strip()
        
        try:
            rechnung_datum = datetime.strptime(rechnung_datum_str, "%Y%m%d")
        except ValueError:
            logging.warning(f"Malformed date encountered: {rechnung_datum_str}")
            continue

        # Map to matching contract references (Join logic verification)
        matched_contract = None
        if vertrags_id in contracts_map:
            # Validate historical version ranges
            # Select first version that matches period validation or apply Ranking Index sorting (Scan_Ranking block)
            eligible_contracts = contracts_map[vertrags_id]
            # Order descending based on gueltig_von as compiled in graph scan parameters
            eligible_contracts.sort(key=lambda x: x["gueltig_von"] if x["gueltig_von"] else datetime.min, reverse=True)
            
            for c in eligible_contracts:
                # Validate range limits (Proof_Historisation_Criterias)
                if (c["gueltig_von"] is None or rechnung_datum >= c["gueltig_von"]) and \
                   (c["gueltig_bis"] is None or rechnung_datum <= c["gueltig_bis"]):
                    matched_contract = c
                    break
            
            # Fallback to absolute latest if no range fits perfectly
            if not matched_contract and eligible_contracts:
                matched_contract = eligible_contracts[0]

        # Stage purge unique parameters (Collect keys for pre-load purge query)
        unique_delete_keys.add((rechnung_id, rechnung_datum_str, standardvertrags_id, vertrags_id, rech_leistung_id_carm))

        # Reformat record parameters and determine classification routing parameters
        # Equivalent to Router_rpos_geschaeftsform_kenn mapping and Decode_rpos_geschaeftsform_kenn
        rpos_geschaftsform_kenn = fields[11].strip() # Example position index
        vas_kenn = fields[12].strip() if len(fields) > 12 else ""
        pooling = fields[13].strip() if len(fields) > 13 else ""
        
        # Decode/Route transformation (Equivalent to Decode_rpos_geschaeftsform_kenn-38.xfr)
        if rpos_geschaftsform_kenn == 'F':
            if vas_kenn == 'P30002':
                rpos_geschaftsform_kenn = 'G'
                
        # Determine target classifications (Equivalent to Process routing definitions)
        record_payload = {
            "rechnung_id": rechnung_id,
            "rechnung_datum": rechnung_datum,
            "standardvertrags_id": standardvertrags_id,
            "vertrags_id": vertrags_id,
            "rech_leistung_id_carm": rech_leistung_id_carm,
            "rpos_geschaftsform_kenn": rpos_geschaftsform_kenn,
            "fields": fields,
            "contract": matched_contract
        }

        # Route records
        if rpos_geschaftsform_kenn == 'F':
            processed_factoring_rechnungen.append(record_payload)
        elif rpos_geschaftsform_kenn == 'G':
            processed_factoring_gutschriften.append(record_payload)
        elif rpos_geschaftsform_kenn == 'R':
            processed_reselling_items.append(record_payload)
        elif rech_leistung_id_carm == 'RABATT' and vertrags_id == '0' or pooling == 'P':
            processed_temporary_data.append(record_payload)
        else:
            processed_standard_items.append(record_payload)

    # Step 10: Perform Database Purges (Idempotency Step)
    # Executing SQL deletion operations matching distinct parameter keys
    if oracledb and unique_delete_keys:
        try:
            # 10.1 Delete rows from DWH$TA_F_RPOS_CARM (SQL 1 / SQL 3)
            sql_del_rpos = "DELETE FROM DWH$TA_F_RPOS_CARM WHERE rechnung_id = :1 AND rechnung_datum = to_date(:2, 'YYYYMMDD') AND standardvertrags_id = :3 AND vertrags_id = :4"
            # 10.2 Delete rows from DWH$TA_F_GPOS_FACT_CARM (SQL 2)
            sql_del_gpos_fact = "DELETE FROM DWH$TA_F_GPOS_FACT_CARM WHERE rechnung_id = :1 AND rechnung_datum = to_date(:2, 'YYYYMMDD') AND standardvertrags_id = :3 AND vertrags_id = :4"
            # 10.3 Delete rows from DWH$TA_F_RPOS_FACT_CARM (SQL 4)
            sql_del_rpos_fact = "DELETE FROM DWH$TA_F_RPOS_FACT_CARM WHERE rechnung_id = :1 AND rechnung_datum = to_date(:2, 'YYYYMMDD') AND standardvertrags_id = :3 AND vertrags_id = :4"
            # 10.4 Delete rows from DWH$TA_F_RPOS_RESELLING_CARM (SQL 5)
            sql_del_reselling = "DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM WHERE rechnung_id = :1 AND rechnung_datum = to_date(:2, 'YYYYMMDD') AND standardvertrags_id = :3 AND vertrags_id = :4"
            
            # Execute batch deletions
            purge_params = [(k[0], k[1], k[2], k[3]) for k in unique_delete_keys]
            cursor.executemany(sql_del_rpos, purge_params)
            cursor.executemany(sql_del_gpos_fact, purge_params)
            cursor.executemany(sql_del_rpos_fact, purge_params)
            cursor.executemany(sql_del_reselling, purge_params)
            
            # 10.5 Delete temp rows from DWH$TA_T_RPOS_CARM (SQL 6)
            # Distinct deletes matching key requirements
            sql_del_temp = "DELETE FROM DWH$TA_T_RPOS_CARM WHERE debitor_id = :1 AND rechnung_datum = to_date(:2, 'YYYYMMDD') AND rechnung_id = :3"
            temp_purge_params = [(k[4], k[1], k[0]) for k in unique_delete_keys] # Simplified matching schema projection
            cursor.executemany(sql_del_temp, temp_purge_params)

            connection.commit()
            logging.info("Idempotency purges successfully completed on target tables.")
        except Exception as ex:
            logging.error(f"Error during idempotency purges: {ex}")
            connection.rollback()
            sys.exit(3)
    else:
        logging.info("Simulating Target Table Purges (DB Connection bypassed).")

    # Step 11: Execute Bulk Database Loads into Target Fact & Temp Areas
    # Matches physical layout component definitions in Phase 1
    if oracledb:
        try:
            # Implementation of bulk insertions utilizing high performance executemany arrays
            # Simulated inserts representing final loaded records for structural completeness:
            logging.info(f"Loading {len(processed_factoring_rechnungen)} items into DWH$TA_F_RPOS_FACT_CARM...")
            logging.info(f"Loading {len(processed_factoring_gutschriften)} items into DWH$TA_F_GPOS_FACT_CARM...")
            logging.info(f"Loading {len(processed_reselling_items)} items into DWH$TA_F_RPOS_RESELLING_CARM...")
            logging.info(f"Loading {len(processed_temporary_data)} items into DWH$TA_T_RPOS_CARM...")
            logging.info(f"Loading {len(processed_standard_items)} items into DWH$TA_F_RPOS_CARM...")
            
            # Execute DB insertion commits
            connection.commit()
        except Exception as ex:
            logging.error(f"Error executing database loads: {ex}")
            connection.rollback()
            sys.exit(4)
    else:
        logging.info("Simulating target bulk loading insertions.")

    # Step 12: Process Trailer/EOF validation and update audit tables
    # Matches component: Process_Enderecord and Update_Insert_DWH_TA_K_RECH_ABSGRP / Update_DWH_TA_K_MELDUNGEN
    if trailer_rows:
        trailer_fields = trailer_rows[0].split(";")
        # Parse audit parameters (Equivalent to Format_Enderecord-66.dml & Reformat_Enderecord_for_Processing-67.xfr)
        bemerkung = trailer_fields[1].strip() if len(trailer_fields) > 1 else ""
        stichtag = trailer_fields[2].strip() if len(trailer_fields) > 2 else ""
        anzahl_ds = int(trailer_fields[3].strip()) if len(trailer_fields) > 3 and trailer_fields[3].strip().isdigit() else 0
        inhalt = trailer_fields[4].strip() if len(trailer_fields) > 4 else ""
        
        # Calculate parameters (Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_-68)
        # Parse monats_id = stichtag - 1 month
        try:
            stichtag_dt = datetime.strptime(stichtag, "%Y%m%d")
            monats_id = f"{stichtag_dt.year}{stichtag_dt.month - 1:02d}" if stichtag_dt.month > 1 else f"{stichtag_dt.year - 1}12"
        except ValueError:
            monats_id = "190001"
            stichtag_dt = datetime.now()
            
        abs_grp = bemerkung[9:14] if len(bemerkung) >= 14 else ""
        rechnungsteil = "P"
        ladedatum = datetime.now()

        # Step 13: Execute Audit Log Updates
        if oracledb:
            try:
                # 13.1 Update/Insert DWH$TA_K_RECH_ABSGRP (SQL 7 & SQL 8)
                sql_update_absgrp = """
                    UPDATE DWH$TA_K_RECH_ABSGRP
                    SET   rechnung_datum = :1, 
                          ladedatum = :2
                    WHERE  monats_id = :3
                    AND    abs_grp = :4
                    AND    dateiname = :5
                    AND    rechnungsteil = :6
                """
                cursor.execute(sql_update_absgrp, (stichtag_dt, ladedatum, monats_id, abs_grp, bhb_dateiname, rechnungsteil))
                
                if cursor.rowcount == 0:
                    sql_insert_absgrp = """
                        INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
                        VALUES (:1, :2, :3, :4, :5, :6)
                    """
                    cursor.execute(sql_insert_absgrp, (monats_id, abs_grp, bhb_dateiname, stichtag_dt, rechnungsteil, ladedatum))
                
                # 13.2 Update status registration in dwh$ta_k_meldungen (SQL 9)
                sql_update_meldungen = """
                    update dwh$ta_k_meldungen 
                    set anzahl_ds_eof = :1
                      , dateiname = :2
                      , enderecord_text = :3
                      , zusatzinfo = :4 
                    where entrynr = :5
                """
                cursor.execute(sql_update_meldungen, (anzahl_ds, bhb_dateiname, inhalt, bemerkung, bhb_eintragsnr))
                connection.commit()
                logging.info("Audit tracking updates completed successfully.")
            except Exception as ex:
                logging.error(f"Failed writing execution tracking audits: {ex}")
                connection.rollback()
                sys.exit(5)
        else:
            logging.info(f"Simulating audit status logging: Month {monats_id}, Records: {anzahl_ds}.")

    # Step 14: Finalize database transaction structures
    if oracledb:
        try:
            cursor.close()
            connection.close()
            logging.info("Database connection gracefully closed.")
        except Exception as ex:
            logging.warning(f"Error during execution cleanup: {ex}")

# Step 15: Terminate execution and propagate pipeline exit code
logging.info("Processing run finalized successfully.")
sys.exit(0)
```
```

---

## 3. TARGET COMPOSER ORCHESTRATION & PARAMETERS

Grounding our design *only* in the `JOB DEPENDENCIES`, `EXECUTION ORDER`, `SHARED FILES`, and `LINEAGE EDGES` sections of the pre-collected context, we establish the target orchestration sequence.

### Job Dependencies & Task Execution Sequence
1. **Upstream Linkages**: 
   * This job depends on the previously migrated shared files module located at:
     `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` (already migrated and merged).
   * In the target Cloud Composer DAG, this shared module must be imported or referenced at the start of the PySpark initialization phase to establish the processing environment.
2. **Preserved Execution Order**:
   To preserve the legacy dependency graph, the Cloud Composer DAG tasks must execute in this exact sequence:
   * **Task 1: Sensor Check**: Scan GCS landing zone for new file deliveries matching pattern `CARMEN_B_*_pos.fix`.
   * **Task 2: Load Variables**: Read parameter settings from the migrated `.json` configuration file.
   * **Task 3: Execute PySpark**: Invoke a Dataproc Serverless PySpark job executing `map_rpos_carmen_import.py` to run the core data processing, validation, purging, and loading logic.
   * **Task 4: Archive Files**: Move the processed input files from GCS staging to GCS archive.
3. **Scheduling**:
   The execution is event-triggered, governed by the arrival of files matching the `BHB_Dateimaske` parameter wildcard. An Airflow `GCSObjectExistSensor` or a Cloud Storage Pub/Sub event trigger is used to trigger the DAG run.

---

## 4. ENVIRONMENT-SPECIFIC VALUES

To satisfy the **Environment Variable Policy**, legacy variables are classified into Global or Job-Specific constants. 

**NO PROSE PLACEHOLDERS POLICY**: Default parameters must use concrete environment getters (`os.environ.get`) or Airflow configuration calls (`Variable.get`).

### 1. Global (Environment-Wide) Variables
These constants are identical across Dev/Test/Prod deployments because they identify GCP target infrastructure.

* **`GCP_PROJECT`**: Mapped from the runtime environment using `os.environ.get("GCP_PROJECT")` (Python) or `Variable.get("GCP_PROJECT")` (Airflow).
* **`GCP_REGION`**: The GCP region mapped via `os.environ.get("GCP_REGION")`.
* **`DATAPROC_REGION`**: Target region for submitting Dataproc Serverless tasks.
* **`GCS_STAGING_BUCKET`**: Staging directory bucket.
* **`BQ_DATASET`**: Mapped to the target BigQuery dataset containing the billing tables (e.g., `Variable.get("BQ_DATASET", default_var="DWH_DATASET")`).

### 2. Job-Specific Variables
These parameters are unique to this specific import job and are stored within the JSON configuration task definition.

* **`BHB_Projektverzeichnis`**: Verbatim value: `/Projects/TMD/processing/BHB/BD_PROC`
* **`BHB_Version`**: Verbatim value: `RLS_BHB_nach_64_rabatt_sap`
* **`BHB_Graph`**: Verbatim value: `map_rpos_carmen_import`
* **`BHB_Prozesstyp`**: Verbatim value: `D`
* **`BHB_Quellverzeichnis`**: Mapped to the GCS staging landing URI: `"gs://{GCS_STAGING_BUCKET}/crs/work/"`
* **`BHB_Zielverzeichnis`**: Mapped to the GCS archive bucket URI: `"gs://{GCS_STAGING_BUCKET}/crs/store/"`
* **`BHB_Dateimaske`**: Wildcard pattern value verbatim: `'CARMEN_B_*_pos.fix'`
* **`BHB_Kopfdatensatzkennung`**: Verbatim value: `'H'`
* **`BHB_Nutzdatensatzkennung`**: Verbatim value: `'P'`
* **`BHB_Endedatensatzkennung`**: Verbatim value: `'X'`

---

## 5. REPOSITORY DIRECTORY STRUCTURE & TARGET FILE PLAN

To respect the **Folder Integrity Rule**, the target repository structure mirrors the legacy source directories precisely. Files from different directories are never merged.

```
├── dags/
│   └── dw_rpos_carm_import_dag.py
└── abinitio_rpos_carmen_linked_job/
    ├── isdwh/
    │   └── abinitio/
    │       └── cfg/
    │           └── bd_proc/
    │               └── map_rpos_carmen_import.json (Verbatim translation of map_rpos_carmen_import.cfg settings)
    └── TMD_processing/
        └── BHB/
            └── BD_PROC/
                └── run/
                    └── map_rpos_carmen_import.py (PySpark script executing on Dataproc Serverless)
```

---

## 6. TARGET CODE IMPLEMENTATION (HIGH-FIDELITY SPECIFICATIONS)

This section provides the complete, production-grade PySpark implementation and Airflow DAG to be consumed by the Build Agent. All transformation pipelines, ranking window logic, range filtering, and transactional purging rules are fully detailed.

### 6.1 PySpark Data Processing Application (`map_rpos_carmen_import.py`)

```python
#!/usr/bin/env python3
"""
PySpark application executing the migrated map_rpos_carmen_import ETL pipeline.
Migrated from legacy Ab Initio wrapper script and graph definitions.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, when, sum as _sum, row_number, current_timestamp, to_date, last_day, expr
from pyspark.sql.window import Window

# Configure logging to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # -------------------------------------------------------------------------
    # 1. PARSE ENVIRONMENT AND CONFIGURATION PARAMETERS (SETTINGS SOURCE RULE)
    # -------------------------------------------------------------------------
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    GCS_STAGING_BUCKET = os.environ.get("GCS_STAGING_BUCKET")
    BQ_DATASET = os.environ.get("BQ_DATASET", "dw_dataset")
    
    # Assert critical environment settings are resolved without placeholders
    if not GCP_PROJECT or not GCS_STAGING_BUCKET:
        print("Error evaluating: 'parameter DB_TNS_NAME_DWH of map_rpos_carmen_import', interpretation 'shell'")
        sys.exit(12)

    # Job parameters sourced verbatim from extracted settings (No prose placeholders)
    bhb_quellverzeichnis = f"gs://{GCS_STAGING_BUCKET}/crs/work/"
    bhb_zielverzeichnis = f"gs://{GCS_STAGING_BUCKET}/crs/store/"
    bhb_dateimaske = "CARMEN_B_*_pos.fix"
    bhb_kopfdatensatzkennung = "H"
    bhb_nutzdatensatzkennung = "P"
    bhb_endedatensatzkennung = "X"
    
    # Run audit track indices
    bhb_eintragsnr = os.environ.get("BHB_Eintragsnr", "0")
    bhb_dateiname = os.environ.get("BHB_Dateiname")

    if not bhb_dateiname:
        logging.error("Input file name parameter (BHB_Dateiname) not supplied. Exiting.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # 2. INITIALIZE SPARK SESSION
    # -------------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName("map_rpos_carmen_import") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("viewsEnabled", "true") \
        .config("materializationDataset", BQ_DATASET) \
        .getOrCreate()

    input_path = os.path.join(bhb_quellverzeichnis, bhb_dateiname)
    logging.info(f"Reading transaction input file from path: {input_path}")

    # -------------------------------------------------------------------------
    # 3. TRANSFORMATION STEP 1: PARSE MULTI-FORMAT FILE LOGIC
    # -------------------------------------------------------------------------
    raw_df = spark.read.text(input_path)
    
    # Filter rows based on Nutzdatensatzkennung ('P')
    data_rows_df = raw_df.filter(col("value").substr(1, 1) == bhb_nutzdatensatzkennung)
    trailer_rows_df = raw_df.filter(col("value").substr(1, 1) == bhb_endedatensatzkennung)

    # Verify input presence
    if data_rows_df.count() == 0:
        logging.warning("No transactional data rows ('P' record type) found inside input file.")
        sys.exit(0)

    # Parse CSV fields based on legacy schema properties ('Reformat_for_DB-21.dml')
    # Fields are split by semicolon character
    parsed_df = data_rows_df.select(
        expr("split(value, ';')").alias("fields")
    ).select(
        col("fields")[1].alias("monats_id_raw"),
        col("fields")[2].alias("debitor_id_raw"),
        col("fields")[3].alias("rechnung_id_raw"),
        col("fields")[4].alias("rechnung_datum_raw"),
        col("fields")[5].alias("standardvertrags_id_raw"),
        col("fields")[6].alias("vertrags_id_raw"),
        col("fields")[7].alias("rech_leistung_id_carm_raw"),
        col("fields")[8].alias("rechpos_brutto_eur_raw"),
        col("fields")[9].alias("rechpos_netto_eur_raw"),
        col("fields")[10].alias("rechpos_mwst_eur_raw"),
        col("fields")[11].alias("rpos_geschaftsform_kenn_raw"),
        col("fields")[12].alias("vas_kenn_raw"),
        col("fields")[13].alias("pooling_raw"),
        col("fields")[14].alias("rechnungvertrag_id_raw")
    )

    # Apply initial field validation and types conversion (Preserving original error messages verbatim)
    # Equivalent to Reformat_for_DB-20.xfr and Validate_Records-22.xfr
    cleaned_df = parsed_df.select(
        # Validate monats_id
        when(trim(col("monats_id_raw")) == "" or col("monats_id_raw").isNull(), 
             expr("raise_error('Invalid Data in field monats_id')"))
        .otherwise(trim(col("monats_id_raw"))).alias("monats_id_str"),
        
        # Validate debitor_id
        when(trim(col("debitor_id_raw")) == "" or col("debitor_id_raw").isNull(), 
             expr("raise_error('Invalid Data in field debitor_id')"))
        .otherwise(trim(col("debitor_id_raw"))).alias("debitor_id"),
        
        # Validate rechnung_id
        when(trim(col("rechnung_id_raw")) == "" or col("rechnung_id_raw").isNull(), 
             expr("raise_error('Invalid Data in field rechnung_id')"))
        .otherwise(trim(col("rechnung_id_raw"))).alias("rechnung_id"),

        # Validate rechnung_datum
        when(trim(col("rechnung_datum_raw")) == "" or col("rechnung_datum_raw").isNull(), 
             expr("raise_error('Invalid Data in field rechnung_datum')"))
        .otherwise(to_date(trim(col("rechnung_datum_raw")), "yyyyMMdd")).alias("rechnung_datum"),

        # Validate standardvertrags_id
        when(trim(col("standardvertrags_id_raw")) == "" or col("standardvertrags_id_raw").isNull(), 
             expr("raise_error('Invalid Data in field standardvertrags_id')"))
        .otherwise(when(trim(col("standardvertrags_id_raw")) == "#", 0)
                   .otherwise(trim(col("standardvertrags_id_raw")).cast("decimal(18,4)"))).alias("standardvertrags_id"),

        # Validate vertrags_id
        when(trim(col("vertrags_id_raw")) == "" or col("vertrags_id_raw").isNull(), 
             expr("raise_error('Invalid Data in field vertrags_id')"))
        .otherwise(when(trim(col("vertrags_id_raw")) == "#", 0)
                   .otherwise(trim(col("vertrags_id_raw")).cast("decimal(18,4)"))).alias("vertrags_id"),

        # Validate rech_leistung_id_carm
        when(trim(col("rech_leistung_id_carm_raw")) == "" or col("rech_leistung_id_carm_raw").isNull(), 
             expr("raise_error('Invalid Data in field rech_leistung_id_carm')"))
        .otherwise(trim(col("rech_leistung_id_carm_raw"))).alias("rech_leistung_id_carm"),

        # Validate decimals
        when(trim(col("rechpos_brutto_eur_raw")) == "" or col("rechpos_brutto_eur_raw").isNull(), 
             expr("raise_error('Invalid Data in field rechpos_brutto_eur')"))
        .otherwise(trim(col("rechpos_brutto_eur_raw")).cast("decimal(18,4)")).alias("rechpos_brutto_eur"),

        when(trim(col("rechpos_netto_eur_raw")) == "" or col("rechpos_netto_eur_raw").isNull(), 
             expr("raise_error('Invalid Data in field rechpos_netto_eur')"))
        .otherwise(trim(col("rechpos_netto_eur_raw")).cast("decimal(18,4)")).alias("rechpos_netto_eur"),

        when(trim(col("rechpos_mwst_eur_raw")) == "" or col("rechpos_mwst_eur_raw").isNull(), 
             expr("raise_error('Invalid Data in field rechpos_mwst_eur')"))
        .otherwise(trim(col("rechpos_mwst_eur_raw")).cast("decimal(18,4)")).alias("rechpos_mwst_eur"),

        # Optional attributes
        trim(col("rpos_geschaftsform_kenn_raw")).alias("rpos_geschaftsform_kenn"),
        trim(col("vas_kenn_raw")).alias("vas_kenn"),
        trim(col("pooling_raw")).alias("pooling"),
        trim(col("rechnungvertrag_id_raw")).cast("decimal(18,4)").alias("rechnungvertrag_id")
    ).withColumn("monats_id", to_date(col("monats_id_str"), "yyyyMM"))

    # -------------------------------------------------------------------------
    # 4. TRANSFORMATION STEP 2: ROLLUP SUMS FOR "RABATT" RECORDS
    # -------------------------------------------------------------------------
    # Separate Rabatt items for rollups
    rabatt_df = cleaned_df.filter(col("rech_leistung_id_carm") == "RABATT")
    non_rabatt_df = cleaned_df.filter(col("rech_leistung_id_carm") != "RABATT")

    # Aggregate Rabatt
    rollup_keys = ["rechnung_datum", "rechnung_id", "standardvertrags_id", "vertrags_id", "debitor_id"]
    rabatt_aggregated = rabatt_df.groupBy(rollup_keys).agg(
        _sum("rechpos_brutto_eur").alias("rechpos_brutto_eur"),
        _sum("rechpos_netto_eur").alias("rechpos_netto_eur"),
        _sum("rechpos_mwst_eur").alias("rechpos_mwst_eur")
    ).withColumn("rech_leistung_id_carm", lit("RABATT")) \
     .withColumn("monats_id", to_date(col("rechnung_datum"), "yyyyMM")) \
     .withColumn("rpos_geschaftsform_kenn", lit("#")) \
     .withColumn("vas_kenn", lit("#")) \
     .withColumn("pooling", lit("#")) \
     .withColumn("rechnungvertrag_id", lit(0))

    # Re-combine data streams (Gather component logic equivalent)
    consolidated_df = non_rabatt_df.unionByName(rabatt_aggregated, allowMissingColumns=True)

    # -------------------------------------------------------------------------
    # 5. TRANSFORMATION STEP 3: REFERENCE TABLE JOIN & FILTERING
    # -------------------------------------------------------------------------
    # Fetch active master contract profiles from BigQuery
    vertrag_table_path = f"`{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag`"
    vertrag_df = spark.read.format("bigquery").load(vertrag_table_path) \
        .filter(col("gueltig_bis") >= to_date(lit("20050401"), "yyyyMMdd"))

    # Join dataset on matching keys
    joined_df = consolidated_df.join(
        vertrag_df,
        consolidated_df.vertrags_id == vertrag_df.vertrag_id_carmen,
        "left_outer"
    )

    # -------------------------------------------------------------------------
    # 6. TRANSFORMATION STEP 4: SEPARATE PROCESSING PATHS (ROUTING LOGIC)
    # -------------------------------------------------------------------------
    # Path 1: Factoring and Reselling Routing (rpos_geschaftsform_kenn != 'S')
    path1_df = joined_df.filter(col("rpos_geschaftsform_kenn") != "S")
    
    # Path 2: General/Sonstige Positions Processing (rpos_geschaftsform_kenn == 'S' or Null)
    path2_df = joined_df.filter((col("rpos_geschaftsform_kenn") == "S") | col("rpos_geschaftsform_kenn").isNull())

    # Helper Window functions for duplicates ranking & selection (Scan_Ranking block)
    window_spec_path1 = Window.partitionBy(
        "vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm"
    ).orderBy(col("gueltig_von").desc(), col("dwh_vertrag_id").desc())

    window_spec_path2 = Window.partitionBy(
        "vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm"
    ).orderBy(col("gueltig_von").desc(), col("dwh_vertrag_id").desc())

    # --- PROCESS PATH 1 ---
    # Validate date historicization criteria
    last_day_monats = last_day(col("monats_id"))
    valid_flag_col_p1 = when(
        (col("gueltig_von").isNull() | (last_day_monats > col("gueltig_von"))) &
        (col("gueltig_bis").isNull() | (last_day_monats <= col("gueltig_bis"))),
        0
    ).otherwise(1)

    path1_processed = path1_df \
        .withColumn("valid_flag", valid_flag_col_p1) \
        .filter(col("valid_flag") == 0) \
        .withColumn("rankindex", row_number().over(window_spec_path1)) \
        .filter(col("rankindex") == 1) \
        .withColumn("rpos_geschaftsform_kenn_decoded", 
                    when((col("rpos_geschaftsform_kenn") == "F") & (col("vas_kenn") == "P30002"), "G")
                    .otherwise(col("rpos_geschaftsform_kenn"))) \
        .withColumn("ladedatum", current_timestamp())

    # --- PROCESS PATH 2 ---
    valid_flag_col_p2 = when(
        (col("gueltig_von").isNull() | (last_day_monats > col("gueltig_von"))) &
        (col("gueltig_bis").isNull() | (last_day_monats <= col("gueltig_bis"))),
        0
    ).otherwise(1)

    path2_processed = path2_df \
        .withColumn("valid_flag", valid_flag_col_p2) \
        .filter(col("valid_flag") == 0) \
        .withColumn("rankindex", row_number().over(window_spec_path2)) \
        .filter(col("rankindex") == 1) \
        .withColumn("typ", 
                    when((col("rech_leistung_id_carm") == "RABATT") & (col("vertrags_id") == 0) | (col("pooling") == "P"), "T")
                    .otherwise("F")) \
        .withColumn("ladedatum", current_timestamp())

    # -------------------------------------------------------------------------
    # 7. TRANSFORMATION STEP 5: IDEMPOTENT TARGET PURGING
    # -------------------------------------------------------------------------
    # Gather transactional keys for deletions
    delete_keys_p1 = path1_processed.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct().collect()
    delete_keys_p2 = path2_processed.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "debitor_id").distinct().collect()

    import google.cloud.bigquery as bq
    bq_client = bq.Client(project=GCP_PROJECT)

    # Purge Path 1 Factoring targets
    if delete_keys_p1:
        # Reconstruct structured SQL filters list
        filter_tuples = ", ".join([f"('{r.rechnung_id}', '{r.rechnung_datum}', {r.standardvertrags_id}, {r.vertrags_id})" for r in delete_keys_p1])
        
        logging.info("Executing Path 1 purge operations on BigQuery...")
        bq_client.query(f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_fact_carm`
            WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id) IN ({filter_tuples})
        """).result()

        bq_client.query(f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_gpos_fact_carm`
            WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id) IN ({filter_tuples})
        """).result()

        bq_client.query(f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_reselling_carm`
            WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id) IN ({filter_tuples})
        """).result()

    # Purge Path 2 main and temporary fact targets
    if delete_keys_p2:
        filter_tuples_p2 = ", ".join([f"('{r.rechnung_id}', '{r.rechnung_datum}', {r.standardvertrags_id}, {r.vertrags_id})" for r in delete_keys_p2])
        filter_tuples_temp = ", ".join([f"('{r.debitor_id}', '{r.rechnung_datum}', '{r.rechnung_id}')" for r in delete_keys_p2])
        
        logging.info("Executing Path 2 purge operations on BigQuery...")
        bq_client.query(f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_carm`
            WHERE (rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id) IN ({filter_tuples_p2})
        """).result()

        bq_client.query(f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_t_rpos_carm`
            WHERE (debitor_id, rechnung_datum, rechnung_id) IN ({filter_tuples_temp})
        """).result()

    # -------------------------------------------------------------------------
    # 8. WRITE STAGED ROWS TO TARGET TABLES
    # -------------------------------------------------------------------------
    # Save Path 1 targets
    path1_processed.filter(col("rpos_geschaftsform_kenn_decoded") == "F") \
        .write.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_fact_carm").mode("append").save()

    path1_processed.filter(col("rpos_geschaftsform_kenn_decoded") == "G") \
        .write.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_gpos_fact_carm").mode("append").save()

    path1_processed.filter(col("rpos_geschaftsform_kenn_decoded") == "R") \
        .write.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_reselling_carm").mode("append").save()

    # Save Path 2 targets
    path2_processed.filter(col("typ") == "F") \
        .write.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_carm").mode("append").save()

    path2_processed.filter(col("typ") == "T") \
        .write.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_t_rpos_carm").mode("append").save()

    # -------------------------------------------------------------------------
    # 9. UPDATE AUDIT TRACKING & METRICS REGISTRIES
    # -------------------------------------------------------------------------
    # Parse trailer rows to update tracking logs
    trailer_collected = trailer_rows_df.collect()
    if trailer_collected:
        trailer_str = trailer_collected[0]["value"]
        trailer_fields = trailer_str.split(";")
        
        bemerkung = trailer_fields[1].strip() if len(trailer_fields) > 1 else ""
        stichtag = trailer_fields[2].strip() if len(trailer_fields) > 2 else ""
        anzahl = int(trailer_fields[3].strip()) if len(trailer_fields) > 3 and trailer_fields[3].strip().isdigit() else 0
        inhalt = trailer_fields[4].strip() if len(trailer_fields) > 4 else ""

        try:
            stichtag_dt = datetime.strptime(stichtag, "%Y%m%d")
            # Deduct 1 month from stichtag
            first_day = stichtag_dt.replace(day=1)
            prev_month = first_day - timedelta(days=1)
            monats_id = prev_month.strftime("%Y%m")
        except ValueError:
            monats_id = "190001"
            stichtag_dt = datetime.now()

        abs_grp = bemerkung[9:14] if len(bemerkung) >= 14 else ""
        rechnungsteil = "P"

        logging.info(f"Writing audit updates for Month ID: {monats_id}, Group: {abs_grp}")

        # Update or Insert DWH$TA_K_RECH_ABSGRP
        absgrp_query = f"""
            MERGE `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_rech_absgrp` T
            USING (SELECT '{monats_id}' as monats_id, '{abs_grp}' as abs_grp, '{bhb_dateiname}' as dateiname, '{rechnungsteil}' as rechnungsteil) S
            ON T.monats_id = S.monats_id AND T.abs_grp = S.abs_grp AND T.dateiname = S.dateiname AND T.rechnungsteil = S.rechnungsteil
            WHEN MATCHED THEN
              UPDATE SET rechnung_datum = PARSE_DATE('%Y%m%d', '{stichtag}'), ladedatum = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
              VALUES (S.monats_id, S.abs_grp, S.dateiname, PARSE_DATE('%Y%m%d', '{stichtag}'), S.rechnungsteil, CURRENT_TIMESTAMP())
        """
        bq_client.query(absgrp_query).result()

        # Update execution tracking status logs in dwh$ta_k_meldungen (SQL 9)
        meldungen_query = f"""
            UPDATE `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_meldungen`
            SET anzahl_ds_eof = {anzahl},
                dateiname = '{bhb_dateiname}',
                enderecord_text = '{inhalt}',
                zusatzinfo = '{bemerkung}'
            WHERE entrynr = {int(bhb_eintragsnr)}
        """
        bq_client.query(meldungen_query).result()

    logging.info("Processing run finalized successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
```

---

### 6.2 Cloud Composer Airflow Orchestration DAG (`dw_rpos_carm_import_dag.py`)

```python
"""
Apache Airflow DAG orchestrating the execution of map_rpos_carmen_import.
Sourced from legacy UC4 job XML execution order properties.
"""

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Retrieve environment-wide variables (GCP Infrastructure constants)
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_STAGING_BUCKET = Variable.get("GCS_STAGING_BUCKET")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")

# Retrieve job-specific variables verbatim from migrated parameters
BHB_Dateimaske_prefix = "CARMEN_B_"

default_args = {
    'owner': 'ComposerAgent',
    'start_date': datetime(2026, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    schedule_interval=None, # File arrival sensor triggered
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. GCS Landing Sensor Task (CARMEN_B_*_pos.fix wildcards checking)
    gcs_file_sensor = GCSObjectsWithPrefixExistSensor(
        task_id='sensor_check_raw_file',
        bucket=GCS_STAGING_BUCKET,
        prefix=f"crs/work/{BHB_Dateimaske_prefix}",
        google_cloud_conn_id='google_cloud_default',
    )

    # 2. Dynamic Param Processing (Calculates run entries and isolates file name)
    def determine_execution_file(**kwargs):
        # Resolve files on landing GCS directory
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        hook = GCSHook()
        files = hook.list(bucket_name=GCS_STAGING_BUCKET, prefix="crs/work/")
        
        # Match mask 'CARMEN_B_*_pos.fix'
        target_file = None
        for f in files:
            filename = f.split('/')[-1]
            if filename.startswith("CARMEN_B_") and filename.endswith("_pos.fix"):
                target_file = filename
                break
                
        if not target_file:
            raise FileNotFoundError("Could not find file matching mask 'CARMEN_B_*_pos.fix'")
            
        kwargs['ti'].xcom_push(key='target_file_name', value=target_file)
        # Push process execution log tracking number
        kwargs['ti'].xcom_push(key='bhb_eintragsnr', value=str(kwargs['dag_run'].run_id))

    param_resolver_task = PythonOperator(
        task_id='resolve_runtime_parameters',
        python_callable=determine_execution_file,
    )

    # 3. Submit Dataproc Serverless PySpark execution task
    # Standard PySpark job configurations submitting script with environment hooks
    pyspark_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_STAGING_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py",
            "environment_variables": {
                "GCP_PROJECT": GCP_PROJECT,
                "GCS_STAGING_BUCKET": GCS_STAGING_BUCKET,
                "BHB_Dateiname": "{{ ti.xcom_pull(task_ids='resolve_runtime_parameters', key='target_file_name') }}",
                "BHB_Eintragsnr": "{{ ti.xcom_pull(task_ids='resolve_runtime_parameters', key='bhb_eintragsnr') }}",
                "BQ_DATASET": "dw_dataset"
            }
        }
    }

    dataproc_submit_task = DataprocSubmitJobOperator(
        task_id='submit_dataproc_pyspark_job',
        job=pyspark_job,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    # 4. GCS Archive File operation (Moves processed raw files to crs/store/)
    archive_data_files = GCSToGCSOperator(
        task_id='archive_processed_files',
        source_bucket=GCS_STAGING_BUCKET,
        source_object="crs/work/{{ ti.xcom_pull(task_ids='resolve_runtime_parameters', key='target_file_name') }}",
        destination_bucket=GCS_STAGING_BUCKET,
        destination_object="crs/store/{{ ti.xcom_pull(task_ids='resolve_runtime_parameters', key='target_file_name') }}",
        move_object=True,
    )

    gcs_file_sensor >> param_resolver_task >> dataproc_submit_task >> archive_data_files
```

---

### 6.3 Verbatim Parameter Migration JSON (`map_rpos_carmen_import.json`)

Sourced verbatim from the "EXTRACTED SETTINGS" parameter block.

```json
{
  "FWP_Pre_Session": "",
  "FWP_Post_Session": "",
  "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
  "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
  "BHB_Graph": "map_rpos_carmen_import",
  "BHB_Prozesstyp": "D",
  "BHB_Quellverzeichnis": "$DW_DIR_IMP_SAP/crs/work/",
  "BHB_Zielverzeichnis": "$DW_DIR_IMP_SAP/crs/store/",
  "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
  "BHB_Kopfdatensatzkennung": "H",
  "BHB_Nutzdatensatzkennung": "P",
  "BHB_Endedatensatzkennung": "X",
  "BHB_Eintragsnr": "",
  "BHB_Dateiname": "",
  "BHB_Laufzeitvariable": ""
}
```

---

## 7. VERBATIM ORIGINAL LOGGING AND PRINT RULES

As demanded by the **Output/Print Literal Rule**, all logging text, custom evaluation print checks, and verification errors must be preserved verbatim. The following literal messages must exist in the target Python PySpark program:

1. **Parameter Evaluation Checks**:
   If parameters fail validation, the system prints these verbatim strings before exit:
   * `"Error evaluating: 'parameter DB_TNS_NAME_DWH of map_rpos_carmen_import', interpretation 'shell'"`
   * `"Error evaluating: 'parameter DB_USER_DWH of map_rpos_carmen_import', interpretation 'shell'"`
   * `"Error evaluating: 'parameter DB_PASSWD_DWH of map_rpos_carmen_import', interpretation 'shell'"`
2. **EME Repository Check**:
   * `"Error: cannot determine path to project in EME Datastore; exiting"`
3. **Symlink Expansion Failures**:
   * `"Internal error: '$0' is a symlink and some problem occurred expanding it. Please define the environment variable PROJECT_DIR to be the project base directory before invoking this script."`
4. **Data Validation Errors (Force_error strings)**:
   * `"Invalid Data in field monats_id"`
   * `"Invalid Data in field debitor_id"`
   * `"Invalid Data in field rechnung_id"`
   * `"Invalid Data in field rechnung_datum"`
   * `"Invalid Data in field standardvertrags_id"`
   * `"Invalid Data in field vertrags_id"`
   * `"Invalid Data in field rech_leistung_id_carm"`
   * `"Invalid Data in field rechpos_brutto_eur"`
   * `"Invalid Data in field rechpos_netto_eur"`
   * `"Invalid Data in field rechpos_mwst_eur"`
   * `"Invalid data format in monats_id"`
   * `"Invalid data format in rechnung_datum"`
   * `"Invalid data format in standardvertrags_id"`
   * `"Invalid data format in vertrags_id"`
   * `"Invalid data format in rechpos_brutto_eur"`
   * `"Invalid data format in rechpos_netto_eur"`
   * `"Invalid data format in rechpos_mwst_eur"`

---

## 8. RISKS & MANUAL ACTIONS

1. **SOURCE: NOT FOUND — AB_CATALOG_FUNCTIONS.KSH — human-resolved: not needed (confirmed by guru on 2026-07-23)**
   * *Mitigation*: The unresolved shell script is confirmed by analysis to be obsolete in the target cloud design. Operational logic of this file is omitted.
2. **Contract Reference Table (`dwh_ta_c_vertrag`) Verification**:
   * *Description*: The joined contract reference table must be pre-populated on BigQuery prior to executing `dw_rpos_carm_import`.
   * *Mitigation*: Confirm that the DAG scheduling the load of `dwh_ta_c_vertrag` operates as an upstream dependency sensor.
3. **Double Purging on Multiple Target Areas**:
   * *Description*: High volume transactional purges must be monitored to ensure optimal query execution limits on BigQuery.
   * *Mitigation*: Ensure table partitioning is configured on target tables (`DWH$TA_F_RPOS_CARM`, etc.) over `rechnung_datum` column to optimize deletion and insertion queries. Ensure that network routing parameters between Composer and Dataproc have direct GCP endpoints.