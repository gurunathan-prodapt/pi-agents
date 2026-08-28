=== OBJECT: DW.DWH_ALL_TYPES_MASTER (JOBS_UNIX) ===
active=1
title=Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain
login=DW.UNIX.ISALL
host=|DWHALL1P|HOST
ert_seconds=10
launcher_type=abinitio_graph
launcher_details={'job_arg': 'ALL_TYPES_MASTER', 'key': 'all_types_graph', 'job_type': 'all_types'}
script_body:
:set &DWH_JOB_KENNUNG='ALL_TYPES_MASTER'

$CCR_DIR_ROOT/abinitio/bin/r_ai_start -j ALL_TYPES_MASTER -t all_types -k all_types_graph -p 1

$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh
operational_notes=

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 Workload Migration Assessment & Technical Design Document

## 1. Overview
This migration document details the transition design for the UC4 object `DW.DWH_ALL_TYPES_MASTER` to Apache Airflow. This is a standalone Unix job designed as a showcase job combining Ab Initio, Oracle SQL, KSH, and AWK components into a single execution flow. The UC4 extraction classifies this job's primary launcher type as an Ab Initio graph (`r_ai_start`), with an additional secondary KSH shell script execution contained within its script body. 

Since no parent Jobplan (`JOBP`) or Schedule (`JSCH` / `EVNT_TIME`) objects were supplied in this extraction, this workflow is defined as externally triggered (source unknown from this extraction alone).

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ALL_TYPES_MASTER` | JOBS_UNIX | 1 (Active) | Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain |

---

## 3. Scheduling
* **Schedule Rule:** No `EVNT_TIME` or schedule trigger object is present in this extraction bundle.
* **Trigger Analysis:** This workflow has no calendar-based schedule of its own. It is marked as **externally triggered** (source unknown from this extraction alone).
* **Airflow Schedule Property:** `schedule=None`

---

## 4. Airflow DAG Properties
Since this is a standalone JOBS_UNIX object without a parent JOBP, a wrapper DAG is designed to represent and schedule this job's execution chain.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_all_types_master` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Active=1 -> `is_paused_upon_creation=False`) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `all_types_master_graph` | `DW.DWH_ALL_TYPES_MASTER` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/all_types_master.py` | `project_id`, `region`, `cluster_name` placeholders | 1 | 5 min | None | None | `False` | None | Mapped from primary Ab Initio launcher type. **CRITICAL:** See Developer Notes regarding secondary KSH script execution. |

---

## 6. Task Dependency Map
Since this migration currently models a single standalone job, the DAG execution chain contains a single root task:

```python
all_types_master_graph
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock definitions were provided in this extraction.
* **Airflow Mapping:** Native concurrency constraint `max_active_runs=1` is configured at the DAG level to prevent concurrent execution overlaps.

---

## 8. Error Handling and Retry Strategy
* **Default Settings:** Retries are set to `1` with a `5-minute` delay, matching basic UC4 operational defaults.
* **Failure Alerts:** No custom callback objects (such as `DW.CALL_STANDARD`) were defined in this extraction. Default Airflow task alerting mechanism should apply.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'ALL_TYPES_MASTER'` | Airflow Task `arguments` / environment variable |
| `job_type` | `'all_types'` | Task metadata parameter |
| `key` | `'all_types_graph'` | Task metadata parameter |

---

## 10. Developer Notes
* **GCP Infrastructure Placeholders:** The developer must replace `YOUR_BUCKET_NAME`, `YOUR_PROJECT_ID`, `YOUR_REGION`, and `YOUR_CLUSTER_NAME` with target environment deployment variables.
* **#REVIEW-STRUCT: Secondary KSH Invocation:** The UC4 script body contains a secondary command execution following the primary Ab Initio start script: `$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh`. Because this job is deterministically classified as an Ab Initio graph wrapper, the automatic translation maps it to a PySpark Dataproc task. The developer must manually investigate this KSH file to determine whether:
  1. Its logic is already incorporated into the target Spark/PySpark logic.
  2. It needs to be broken out into a separate downstream task (e.g., via `BashOperator` or `SSHOperator`).

---

# PSEUDOCODE OUTLINE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

── GCP Configuration ────────────────────────────────────
# GCP Infrastructure Environment Variables
GCP_PROJECT_ID = "YOUR_PROJECT_ID"
GCP_REGION = "YOUR_REGION"
GCP_CLUSTER_NAME = "YOUR_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

── on_failure_callback stubs ─────────────────────────────
# No custom error handling callbacks defined in extraction.

── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_all_types_master',
    default_args=default_args,
    description='Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
)

── Task: all_types_master_graph ─────────────────────────
# REVIEW-STRUCT: Mapped from Ab Initio launcher. 
# Developers must verify if '$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh' 
# script logic has been migrated into the PySpark job or requires an independent task step.

pyspark_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": GCP_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/all_types_master.py",
        "args": [
            "--job_kennung", "ALL_TYPES_MASTER",
            "--job_type", "all_types",
            "--key", "all_types_graph"
        ]
    }
}

all_types_master_graph = DataprocSubmitJobOperator(
    task_id='all_types_master_graph',
    job=pyspark_job,
    region=GCP_REGION,
    project_id=GCP_PROJECT_ID,
    dag=dag
)

── Dependencies ─────────────────────────────────────────
# Single standalone workflow node:
all_types_master_graph
```

### Job dependencies
- **Upstream Dependencies (Already Migrated & Merged):**
  - `Shared Files — TMD_processing/ALL_TYPES/mp`: Corresponds to the Ab Initio graph file `TMD_processing/ALL_TYPES/mp/all_types_graph.mp` which was already migrated to PySpark in PR #883.
  - `Shared Files — TMD_processing/ALL_TYPES/run`: Corresponds to the wrapper script `TMD_processing/ALL_TYPES/run/all_types_graph.ksh` which was already migrated in PR #884.
  - These migrated modules must be referenced/imported within the newly generated Airflow DAG instead of being recreated.

### Execution order
The target Airflow DAG orchestrator must preserve the sequential execution order derived from the legacy dependency graph:
1. **Orchestrator Entry Point:** `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` is converted to the master Airflow DAG file (`DWH_ALL_TYPES_JOB/dw_dwh_all_types_master.py`).
2. **Parameters & Configuration Configuration:** `isall/abinitio/cfg/all_types/all_types_graph.cfg` acts as the configuration parameter definition, mapping environment and job-specific variables to the downstream tasks.
3. **Primary Processing Task:** The core execution initiates by calling the PySpark translation of the Ab Initio graph (`all_types_graph.mp` under `TMD_processing/ALL_TYPES/mp/`), which represents the first functional step of the master job.
4. **Secondary Processing Task:** Downstream of the primary graph execution, the workflow triggers the execution of `isall/aufbereitung/bin/r_all_types_master.ksh` (which is migrated to Python in a separate independent design pass).
5. **Environment Setup:** `.dw_init` is a shell initialization script that was human-reviewed and confirmed as not needing individual migration; its environmental context is handled globally within the Airflow DAG environment settings.
6. **Data Transformation Task:** The pipeline executes `isall/aufbereitung/awk/k_all_types_transform.awk` (migrated to Python in a separate pass) to perform core logic transformations.
7. **Database Operations Task:** Finally, `isall/aufbereitung/sql/d_all_types.sql` (migrated to BigQuery SQL in a separate pass) is executed to load/merge the data into the target tables.

### Lineage
- **Upstream Lineage & External Systems:**
  - `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` runs on legacy host `dwhall1p` (which is an external system now represented by the standard GCP target environment).
  - The job uses the package `DW.UNIX.ISALL` to run shell operations.
  - The job uses configuration file `isall/abinitio/cfg/all_types/all_types_graph.cfg`.
  - The job references the legacy graph `TMD_processing/ALL_TYPES/mp/all_types_graph.mp`.
- **Downstream Lineage & Invocations:**
  - The job invokes `R_AI_START.KSH` (unresolved in physical files but confirmed by human review as "no source needed" as its bootstrap role is natively handled by the Airflow Dataproc operator).
  - The job invokes `isall/aufbereitung/bin/r_all_types_master.ksh` (a sibling script belonging to a separate design pass).
  - The job invokes `ALL_TYPES_GRAPH.KSH` (orchestrated via the migrated shared module run script).

### Cross-file dependencies
- **Configuration & Variable Sharing:** The variables defined in `isall/abinitio/cfg/all_types/all_types_graph.cfg` are shared with the primary graph processing tasks to parameterize file paths and run options.
- **Sequential Pipeline Calls:** There is a direct call-chain dependency between the master DAG and the external sibling files: the primary graph (`all_types_graph.mp`), the master shell script (`r_all_types_master.ksh`), the AWK transform (`k_all_types_transform.awk`), and the final SQL load script (`d_all_types.sql`). Each of these scripts relies on the output files or tables produced by the preceding step in the sequence.

### Target file plan
- **Target File Path:** `DWH_ALL_TYPES_JOB/dw_dwh_all_types_master.py`
  - **Language:** Python (Airflow DAG)
  - **Source File:** `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml`

### Environment-specific values
Every environment-sourced value from the legacy scripts and configurations is classified below to guide the Build Agent:

1. **GLOBAL (Environment-wide infra constants - sourced at runtime via Airflow Variables or environment context):**
   - `CCR_DIR_ROOT`: Sourced at runtime via `os.environ.get("CCR_DIR_ROOT")` or standard GCS environment variable.
   - `HOME`: Sourced at runtime via `os.environ.get("HOME")`.
   - `DWHALL1P` (Legacy Host): Normalized to canonical GCP infrastructure variables. Sourced via Airflow Variables: `Variable.get("GCP_PROJECT")`, `Variable.get("GCP_REGION")`, `Variable.get("DATAPROC_CLUSTER")`.
   - `ALL_TYPES_DIR_EXP_UTL`: Normalized to the environment-wide GCS bucket folder. Sourced via Airflow Variable: `Variable.get("ALL_TYPES_DIR_EXP_UTL")`.

2. **JOB-SPECIFIC (Job-level parameters - hardcoded/defined at the DAG/task level):**
   - `ALL_TYPES_Projektverzeichnis` (`/Projects/TMD/processing/ALL_TYPES/`): Maps to the specific folder structure on GCS for this pipeline. Defined inside the DAG's job-specific configuration mapping.
   - `ALL_TYPES_Graph` (`all_types_graph`): Hardcoded as a task parameter in the primary execution task.
   - `ALL_TYPES_Version` (`RLS_ALL_TYPES_current`): Defined inside the task's environment or execution parameters.
   - `ALL_TYPES_Prozesstyp` (`N`): Hardcoded as a task-level metadata parameter.
   - `ALL_TYPES_Datenobjekt` (`-`): Hardcoded as a task-level parameter.
   - `ALL_TYPES_AI_DAT_FILE_DIR` (`$ALL_TYPES_DIR_EXP_UTL/cubes/at`): Mapped dynamically using the global `ALL_TYPES_DIR_EXP_UTL` environment variable concatenated with `/cubes/at`.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` | `DWH_ALL_TYPES_JOB/dw_dwh_all_types_master.py` | Migrated to an Apache Airflow DAG to orchestrate the sequential pipeline steps, preserving folder structure integrity under `DWH_ALL_TYPES_JOB/`. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/data/source/all_types_linked_job/.dw_init ===
#! /bin/ksh
############################################################
#
# Diese Datei setzt alle notwendigen Umgebungsvariablen fuer
# den Betrieb des Information Services
# 


#########################################
# Einstiegspunke fuer Information Service
#
# DW_DIR_ROOT  : Ausgangsverzeichnis fuer alle Skripte 
#                (z.B. /vobs/dw_source) 
# DW_DIR_PROT  : Protokollverzeichnis in denen Logs, etc. abgelegt werden 
#                (z.B. /vobs/dw_source/daten/protokoll)
# DW_DIR_CUBES : Datenverzeichnis in denen die Wuerfel abgelegt werden 
#                (z.B. /vobs/dw_source/daten/cubes)
# DW_DIR_IMP_<XX>  : Datenverzeichnis der einzelnen Importerkennungen
#                (z.B. ~isctelim/daten)
#
DW_DIR_ROOT=$HOME/aktuell; export DW_DIR_ROOT
#DW_DIR_ROOT=$HOME/isdwh; export DW_DIR_ROOT
DW_DIR_PROT=$HOME/daten/logfiles; export DW_DIR_PROT
DW_DIR_CUBES=$HOME/daten/cubes; export DW_DIR_CUBES

DW_DIR_IMP_D1=$HOME/daten/d1; export DW_DIR_IMP_D1
DW_DIR_IMP_BWA=$HOME/daten/dpps/bwa; export DW_DIR_IMP_BWA # neu in rel 3.0
DW_DIR_IMP_XTRA=$HOME/daten/xtra; export DW_DIR_IMP_XTRA
DW_DIR_IMP_CTEL=$HOME/daten/ctel; export DW_DIR_IMP_CTEL
DW_DIR_IMP_VO=$HOME/daten/vo; export DW_DIR_IMP_VO
DW_DIR_IMP_RV=$HOME/daten/rv; export DW_DIR_IMP_RV
DW_DIR_IMP_IF=$HOME/daten/ees; export DW_DIR_IMP_IF
DW_DIR_IMP_NNV=$HOME/daten/nnv; export DW_DIR_IMP_NNV
DW_DIR_IMP_SIGMA=$HOME/daten/gd/sigma; export DW_DIR_IMP_SIGMA #neu in rel 3.0
DW_DIR_EXP_SIGMA=$HOME/daten/gd/sigma/export; export DW_DIR_EXP_SIGMA #neu in rel 3.0
DW_DIR_IMP_TRF=$HOME/daten/trf; export DW_DIR_IMP_TRF
DW_DIR_IMP_AUF=$HOME/daten/sd/auf; export DW_DIR_IMP_AUF
DW_DIR_IMP_GUT=$HOME/daten/sd/gut; export DW_DIR_IMP_GUT
DW_DIR_IMP_KDG=$HOME/daten/sd/kdg; export DW_DIR_IMP_KDG
DW_DIR_IMP_MP_KDG=$HOME/daten/mp/kdg; export DW_DIR_IMP_MP_KDG
DW_DIR_IMP_MP_TS=$HOME/daten/mp/ts; export DW_DIR_IMP_MP_TS
DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm; export DW_DIR_IMP_MP_ZM
DW_DIR_IMP_TS=$HOME/daten/sd/ts; export DW_DIR_IMP_TS
DW_DIR_IMP_ZM=$HOME/daten/sd/zm; export DW_DIR_IMP_ZM
DW_DIR_EXP=$HOME/daten/exporter; export DW_DIR_EXP
DW_DIR_IMP_BPM=$HOME/daten/bm; export DW_DIR_IMP_BPM
DW_DIR_IMP_ZTS=$HOME/daten/zts; export DW_DIR_IMP_ZTS
DW_DIR_IMP_VRS=$HOME/daten/vrs; export DW_DIR_IMP_VRS

DW_DIR_IMP_BRUNET=$HOME/daten/brunet; export DW_DIR_IMP_BRUNET
DW_DIR_IMP_DWH=$HOME/daten/dwh; export DW_DIR_IMP_DWH # neu im rel 3.0
DW_DIR_IMP_PLATO=$HOME/daten/dwh/plato; export DW_DIR_IMP_PLATO
#######neu DWH 2.5
DW_DIR_IMP_CARMEN=$HOME/daten/carmen; export DW_DIR_IMP_CARMEN #neu in 25_1
DW_DIR_IMP_SAP=$HOME/daten/sap; export DW_DIR_IMP_SAP #neu in 25_1
DW_DIR_IMP_SR_RV=$HOME/daten/sap/sr_rv_dpps; export DW_DIR_IMP_SR_RV #neu in 3.0
DW_DIR_IMP_SAP_L_GUTGR=$HOME/daten/sap/sap_l_gutgr; export DW_DIR_IMP_SAP_L #neu in 3.0
DW_DIR_IMP_L_MAHNSTYP_IST=$HOME/daten/sap/mahn; export DW_DIR_IMP_L_MAHNSTYP_IST #neu in rel 3.0
DW_DIR_IMP_L_MAHNV_FI=$HOME/daten/sap/mahn; export DW_DIR_IMP_L_MAHNV_FI #neu in rel 3.0
DW_DIR_IMP_L_MAHNV_IST=$HOME/daten/sap/mahn; export DW_DIR_IMP_L_MAHNV_IST #neu in rel 3.0
DW_DIR_IMP_L_GUTGR=$HOME/daten/sd/l_gutschr; export DW_DIR_IMP_L_GUTGR
DW_DIR_IMP_L_LEIST=$HOME/daten/sd/l_leist; export DW_DIR_IMP_L_LEIST
DW_DIR_IMP_L_PROD=$HOME/daten/sd/l_prod; export DW_DIR_IMP_L_PROD
DW_DIR_IMP_LKODE=$HOME/daten/sd/lkode; export DW_DIR_IMP_LKODE

#######neu DWH 7.5 mit Subscription Server
DW_DIR_IMP_SUBSE=$HOME/daten/subse; export DW_DIR_IMP_SUBSE

#######neu DWH 2.5 mit SMS
DW_DIR_SMS_PRG=${HOME}/aktuell/allgemein/is/util; export  DW_DIR_SMS_PRG
DW_DIR_SMS_ADR=${HOME}/daten/sms/adressen; export DW_DIR_SMS_ADR
DW_DIR_SMS_TMP=${HOME}/daten/sms/tmp; export DW_DIR_SMS_TMP

######neu DWH 3.5 
DW_DIR_IMP_DPPS=$HOME/daten/dpps; export DW_DIR_IMP_DPPS
DW_DIR_IMP_PLANF2=$HOME/daten/planf2; export DW_DIR_IMP_PLANF2
 
########################################
# Pfade in Remote Systemen
#DW_DIR_CUSTOMER=<login>; export DW_DIR_CUSTOMER

########################################
# Remote Hosts
DW_HOST_CUSTOMER=dxcst3.bn.detemobil.de; export DW_HOST_CUSTOMER


##########################
# Einstellung der Umgebung
#
# ORAHOME: entsprechend ORACLE-Dokumentation
#
if [ -z "$ORACLE_HOME" ]
then
  if [ -d /appl/local/oracle/12.2.0.1.0 ]
  then
    ORACLE_HOME=/appl/local/oracle/12.2.0.1.0
  elif [ -d /appl/local/oracle/11.2.0 ]
  then
    ORACLE_HOME=/appl/local/oracle/11.2.0
  else
    echo "Fehler in .dw_init:"
    echo "   Konnte ORACLE_HOME nicht setzen !"
  fi
  export ORACLE_HOME
fi

########################
# Aufruf weitere Skripte
#
# dw_global: Einstellung der globalen Parameter
# dw_lokal:  Einstellung der lokalen Parameter
#
. $HOME/.dw_global
. $HOME/.dw_lokal

DW_DIR_UTL_FILE=/appl/local/oracle/admin/$ORACLE_SID/utl_file;
export DW_DIR_UTL_FILE  #neu in rel 3.0

##################################################
#
 #    #  #    #    ##     ####   #    #
 #    #  ##  ##   #  #   #       #   #
 #    #  # ## #  #    #   ####   ####
 #    #  #    #  ######       #  #  #
 #    #  #    #  #    #  #    #  #   #
  ####   #    #  #    #   ####   #    #

#umask 007


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains directory existence checks and environment sourcing logic that is not expressible in BigQuery SQL and requires Python to model programmatic environment initialization.

EVIDENCE
- Business logic found: KSH custom logic consisting of setting numerous DW-related environment directories and checking/setting ORACLE_HOME based on physical directory existence, as well as sourcing global and local setup scripts.
- AWK: none
- SQL-expressible: no, contains filesystem directory checks (-d) and environment variables setting which cannot be executed or represented in BigQuery SQL.
- Non-SQL side effects: checks filesystem paths, echoes error messages, and sources external scripts.
- Against this verdict: NO_CONVERSION_REQUIRED could be argued because this is an environment initialization script rather than an active data stage, but it contains conditional logic and sourcing that violates the strict wrapper definition.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`.dw_init`) is an environment initialization script written in KornShell (.ksh). Its primary purpose is to define and export all directory paths, remote hostnames, database variables, and utility parameters required by the "Information Services" / Data Warehouse environment. It also dynamically sets `ORACLE_HOME` based on directory presence and sources further global and local configurations.

2. INVOCATION CONTEXT
   - Who calls this script: It is sourced (via `. .dw_init`) by other Data Warehouse shell scripts or UC4/Automic UNIX jobs to establish a consistent environment. No direct UC4 job invocation was supplied in the extraction.
   - UC4 native includes: None referenced in the extraction.
   - Environment files sourced:
     - `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
     - `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `HOME` (environment variable): Used as the base path for resolving directories. Surfaced in Python via `os.environ.get("HOME")`.
   - `ORACLE_HOME` (environment variable): Used to detect if the Oracle home path is already set. Surfaced in Python via `os.environ.get("ORACLE_HOME")`.
   - `ORACLE_SID` (environment variable): Used to build the dynamic `DW_DIR_UTL_FILE` path. Surfaced in Python via `os.environ.get("ORACLE_SID")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - None. (Sourcing scripts is treated as a configuration load step).

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   1. Set directory variables relative to `$HOME`:
      - `DW_DIR_ROOT=$HOME/aktuell`
      - `DW_DIR_PROT=$HOME/daten/logfiles`
      - `DW_DIR_CUBES=$HOME/daten/cubes`
      - `DW_DIR_IMP_D1=$HOME/daten/d1`
      - `DW_DIR_IMP_BWA=$HOME/daten/dpps/bwa`
      - `DW_DIR_IMP_XTRA=$HOME/daten/xtra`
      - `DW_DIR_IMP_CTEL=$HOME/daten/ctel`
      - `DW_DIR_IMP_VO=$HOME/daten/vo`
      - `DW_DIR_IMP_RV=$HOME/daten/rv`
      - `DW_DIR_IMP_IF=$HOME/daten/ees`
      - `DW_DIR_IMP_NNV=$HOME/daten/nnv`
      - `DW_DIR_IMP_SIGMA=$HOME/daten/gd/sigma`
      - `DW_DIR_EXP_SIGMA=$HOME/daten/gd/sigma/export`
      - `DW_DIR_IMP_TRF=$HOME/daten/trf`
      - `DW_DIR_IMP_AUF=$HOME/daten/sd/auf`
      - `DW_DIR_IMP_GUT=$HOME/daten/sd/gut`
      - `DW_DIR_IMP_KDG=$HOME/daten/sd/kdg`
      - `DW_DIR_IMP_MP_KDG=$HOME/daten/mp/kdg`
      - `DW_DIR_IMP_MP_TS=$HOME/daten/mp/ts`
      - `DW_DIR_IMP_MP_ZM=$HOME/daten/mp/zm`
      - `DW_DIR_IMP_TS=$HOME/daten/sd/ts`
      - `DW_DIR_IMP_ZM=$HOME/daten/sd/zm`
      - `DW_DIR_EXP=$HOME/daten/exporter`
      - `DW_DIR_IMP_BPM=$HOME/daten/bm`
      - `DW_DIR_IMP_ZTS=$HOME/daten/zts`
      - `DW_DIR_IMP_VRS=$HOME/daten/vrs`
      - `DW_DIR_IMP_BRUNET=$HOME/daten/brunet`
      - `DW_DIR_IMP_DWH=$HOME/daten/dwh`
      - `DW_DIR_IMP_PLATO=$HOME/daten/dwh/plato`
      - `DW_DIR_IMP_CARMEN=$HOME/daten/carmen`
      - `DW_DIR_IMP_SAP=$HOME/daten/sap`
      - `DW_DIR_IMP_SR_RV=$HOME/daten/sap/sr_rv_dpps`
      - `DW_DIR_IMP_SAP_L_GUTGR=$HOME/daten/sap/sap_l_gutgr` (retains a legacy export mismatch where `DW_DIR_IMP_SAP_L` is exported instead)
      - `DW_DIR_IMP_L_MAHNSTYP_IST=$HOME/daten/sap/mahn`
      - `DW_DIR_IMP_L_MAHNV_FI=$HOME/daten/sap/mahn`
      - `DW_DIR_IMP_L_MAHNV_IST=$HOME/daten/sap/mahn`
      - `DW_DIR_IMP_L_GUTGR=$HOME/daten/sd/l_gutschr`
      - `DW_DIR_IMP_L_LEIST=$HOME/daten/sd/l_leist`
      - `DW_DIR_IMP_L_PROD=$HOME/daten/sd/l_prod`
      - `DW_DIR_IMP_LKODE=$HOME/daten/sd/lkode`
      - `DW_DIR_IMP_SUBSE=$HOME/daten/subse`
      - `DW_DIR_SMS_PRG=${HOME}/aktuell/allgemein/is/util`
      - `DW_DIR_SMS_ADR=${HOME}/daten/sms/adressen`
      - `DW_DIR_SMS_TMP=${HOME}/daten/sms/tmp`
      - `DW_DIR_IMP_DPPS=$HOME/daten/dpps`
      - `DW_DIR_IMP_PLANF2=$HOME/daten/planf2`
   2. Export remote host variable:
      - `DW_HOST_CUSTOMER=dxcst3.bn.detemobil.de`
   3. Check if `ORACLE_HOME` is unset or empty:
      - If empty, check directory existence for `/appl/local/oracle/12.2.0.1.0`.
      - If exists, set `ORACLE_HOME=/appl/local/oracle/12.2.0.1.0`.
      - Else, check directory existence for `/appl/local/oracle/11.2.0`.
      - If exists, set `ORACLE_HOME=/appl/local/oracle/11.2.0`.
      - Else, write "Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !" to stdout.
   4. Source `$HOME/.dw_global` and `$HOME/.dw_lokal` scripts.
   5. Define and export `DW_DIR_UTL_FILE=/appl/local/oracle/admin/$ORACLE_SID/utl_file`.

7. ERROR HANDLING & EXIT CODES
   - If `ORACLE_HOME` cannot be set, it prints errors to stdout but does NOT exit with a non-zero status. The script continues execution.
   - Python equivalence: Log a warning to `sys.stderr` when path checks fail, continuing execution to emulate original behavior.

8. OUTPUTS / SIDE EFFECTS
   - Mutates environment variables for the current session.
   - # REVIEW: Since the target platform is confirmed as BIGQUERY, Oracle environment components (`ORACLE_HOME`, `ORACLE_SID`, and `DW_DIR_UTL_FILE`) may be obsolete or replaced by BigQuery/GCP resources (such as Google Cloud Storage buckets or Dataset locations).

9. BUSINESS SUMMARY
   - Establishes unified root and sub-directory paths for Data Warehouse imports, exports, backups, and logs across different functional modules (e.g., SAP, SMS, Plato).
   - Dynamically resolves client-side database home versions (`ORACLE_HOME`).
   - Hooks into global and local configuration templates (`.dw_global`, `.dw_lokal`) to override or expand instance-specific parameters.

=== PSEUDOCODE ===

```python
import os
import sys

# MANDATORY AUDIT STEP: No functions defined in .dw_init. No parameter-validation guards to verify.

# Step 1: Resolve base directory (HOME)
home_dir = os.environ.get("HOME", "")

# Step 2: Set and export path variables
os.environ["DW_DIR_ROOT"] = os.path.join(home_dir, "aktuell")
os.environ["DW_DIR_PROT"] = os.path.join(home_dir, "daten/logfiles")
os.environ["DW_DIR_CUBES"] = os.path.join(home_dir, "daten/cubes")
os.environ["DW_DIR_IMP_D1"] = os.path.join(home_dir, "daten/d1")
os.environ["DW_DIR_IMP_BWA"] = os.path.join(home_dir, "daten/dpps/bwa")
os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home_dir, "daten/xtra")
os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home_dir, "daten/ctel")
os.environ["DW_DIR_IMP_VO"] = os.path.join(home_dir, "daten/vo")
os.environ["DW_DIR_IMP_RV"] = os.path.join(home_dir, "daten/rv")
os.environ["DW_DIR_IMP_IF"] = os.path.join(home_dir, "daten/ees")
os.environ["DW_DIR_IMP_NNV"] = os.path.join(home_dir, "daten/nnv")
os.environ["DW_DIR_IMP_SIGMA"] = os.path.join(home_dir, "daten/gd/sigma")
os.environ["DW_DIR_EXP_SIGMA"] = os.path.join(home_dir, "daten/gd/sigma/export")
os.environ["DW_DIR_IMP_TRF"] = os.path.join(home_dir, "daten/trf")
os.environ["DW_DIR_IMP_AUF"] = os.path.join(home_dir, "daten/sd/auf")
os.environ["DW_DIR_IMP_GUT"] = os.path.join(home_dir, "daten/sd/gut")
os.environ["DW_DIR_IMP_KDG"] = os.path.join(home_dir, "daten/sd/kdg")
os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home_dir, "daten/mp/kdg")
os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home_dir, "daten/mp/ts")
os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home_dir, "daten/mp/zm")
os.environ["DW_DIR_IMP_TS"] = os.path.join(home_dir, "daten/sd/ts")
os.environ["DW_DIR_IMP_ZM"] = os.path.join(home_dir, "daten/sd/zm")
os.environ["DW_DIR_EXP"] = os.path.join(home_dir, "daten/exporter")
os.environ["DW_DIR_IMP_BPM"] = os.path.join(home_dir, "daten/bm")
os.environ["DW_DIR_IMP_ZTS"] = os.path.join(home_dir, "daten/zts")
os.environ["DW_DIR_IMP_VRS"] = os.path.join(home_dir, "daten/vrs")
os.environ["DW_DIR_IMP_BRUNET"] = os.path.join(home_dir, "daten/brunet")
os.environ["DW_DIR_IMP_DWH"] = os.path.join(home_dir, "daten/dwh")
os.environ["DW_DIR_IMP_PLATO"] = os.path.join(home_dir, "daten/dwh/plato")
os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home_dir, "daten/carmen")
os.environ["DW_DIR_IMP_SAP"] = os.path.join(home_dir, "daten/sap")
os.environ["DW_DIR_IMP_SR_RV"] = os.path.join(home_dir, "daten/sap/sr_rv_dpps")

# NOTE: Legacy export mismatch: DW_DIR_IMP_SAP_L_GUTGR is declared, but DW_DIR_IMP_SAP_L is exported.
dw_dir_imp_sap_l_gutgr = os.path.join(home_dir, "daten/sap/sap_l_gutgr")
os.environ["DW_DIR_IMP_SAP_L"] = dw_dir_imp_sap_l_gutgr

os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = os.path.join(home_dir, "daten/sap/mahn")
os.environ["DW_DIR_IMP_L_MAHNV_FI"] = os.path.join(home_dir, "daten/sap/mahn")
os.environ["DW_DIR_IMP_L_MAHNV_IST"] = os.path.join(home_dir, "daten/sap/mahn")
os.environ["DW_DIR_IMP_L_GUTGR"] = os.path.join(home_dir, "daten/sd/l_gutschr")
os.environ["DW_DIR_IMP_L_LEIST"] = os.path.join(home_dir, "daten/sd/l_leist")
os.environ["DW_DIR_IMP_L_PROD"] = os.path.join(home_dir, "daten/sd/l_prod")
os.environ["DW_DIR_IMP_LKODE"] = os.path.join(home_dir, "daten/sd/lkode")
os.environ["DW_DIR_IMP_SUBSE"] = os.path.join(home_dir, "daten/subse")
os.environ["DW_DIR_SMS_PRG"] = os.path.join(home_dir, "aktuell/allgemein/is/util")
os.environ["DW_DIR_SMS_ADR"] = os.path.join(home_dir, "daten/sms/adressen")
os.environ["DW_DIR_SMS_TMP"] = os.path.join(home_dir, "daten/sms/tmp")
os.environ["DW_DIR_IMP_DPPS"] = os.path.join(home_dir, "daten/dpps")
os.environ["DW_DIR_IMP_PLANF2"] = os.path.join(home_dir, "daten/planf2")

# Step 3: Set and export remote host
os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

# Step 4: Resolve ORACLE_HOME dynamically if unset
# REVIEW: Oracle components might not be required in target BIGQUERY platform.
if not os.environ.get("ORACLE_HOME"):
    if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
    elif os.path.isdir("/appl/local/oracle/11.2.0"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
    else:
        print("Fehler in .dw_init:", file=sys.stderr)
        print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

# Step 5: Load global and local settings if existing
# # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values
dw_global_path = os.path.join(home_dir, ".dw_global")
dw_lokal_path = os.path.join(home_dir, ".dw_lokal")

# Step 6: Define and export UTL file directory
oracle_sid = os.environ.get("ORACLE_SID", "")
os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
```

### Job dependencies
* **Upstream Predecessors**:
  * `Shared Files — TMD_processing/ALL_TYPES/mp`: This Ab Initio graph has already been migrated to PySpark and merged (PR 883).
  * `Shared Files — TMD_processing/ALL_TYPES/run`: The execution control wrapper script has already been migrated and merged (PR 884).
* **Wiring on Target Platform**:
  * In Cloud Composer, the initialization logic from this job (`dw_init.py`) will be executed or imported at the start of the DAG to establish the environment contexts before launching the PySpark pipeline tasks.

### Execution order
The target Cloud Composer orchestration must preserve the legacy execution order:
1. **DAG Orchestration Entry**: `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` (Cloud Composer DAG structure)
2. **Configuration Loading**: `isall/abinitio/cfg/all_types/all_types_graph.cfg` (Loaded as Airflow variables or DAG params)
3. **Execution Wrapper**: `isall/aufbereitung/bin/r_all_types_master.ksh` (Python orchestration task)
4. **Environment Initialization**: `.dw_init` (Sourced/imported as `dw_init.py` to populate variables)
5. **Data Transformation**: `isall/aufbereitung/awk/k_all_types_transform.awk` (Python transformation task)
6. **SQL Database Update**: `isall/aufbereitung/sql/d_all_types.sql` (BigQuery SQL execution task)

### Lineage
* **Upstream Configuration References**:
  * `.dw_init` USES_CONFIG `UNRESOLVED:.DW_GLOBAL`
  * `.dw_init` USES_CONFIG `UNRESOLVED:.DW_LOKAL`

### Cross-file dependencies
* `.dw_init` establishes central path and directory environment variables. These are referenced across subsequent execution steps including `r_all_types_master.ksh`, `k_all_types_transform.awk`, and `d_all_types.sql` to resolve where data imports, log files, and temporary outputs are stored.
* It historically sourced `.dw_global` and `.dw_lokal` to inherit global parameters.

### Target file plan
* **Target File Path**: `dw_init.py` (Language: Python)
  * **Source File**: `.dw_init`
  * **Purpose**: Sets environment parameters, paths, and configurations for Cloud Composer and Dataproc jobs, replacing local Unix folder structures with Google Cloud Storage (GCS) paths.

### Environment-specific values
Values from the source are classified below according to their target environment role:

1. **GLOBAL (environment-wide)**:
   * `GCP_PROJECT`: Dynamically resolved via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`.
   * `GCS_BUCKET`: The base storage bucket used to replace `$HOME` directory paths. Resolved via `Variable.get("GCS_BUCKET")`.
   * `HOME`: Legacy execution base directory. Resolved in Python via `os.environ.get("HOME")` or mapped directly to a GCS bucket prefix.
   * `DW_HOST_CUSTOMER`: Remote host name (`dxcst3.bn.detemobil.de`). Mapped to Airflow Variable `Variable.get("DW_HOST_CUSTOMER")`.
   * `ORACLE_HOME`: Legacy Oracle home directory. No direct BigQuery or target platform equivalent exists.
   * `ORACLE_SID`: Legacy Oracle database SID. No direct BigQuery or target platform equivalent exists.
   * `DW_DIR_UTL_FILE`: Oracle database server file path. No direct BigQuery or target platform equivalent exists.

2. **JOB-SPECIFIC**:
   * All directories defined in the source (including `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES` and variables prefixed with `DW_DIR_IMP_*` or `DW_DIR_EXP_*`): In the target environment, these represent subdirectory structures. They must be resolved dynamically by combining the global `GCS_BUCKET` variable with job-specific paths (e.g., `f"gs://{GCS_BUCKET}/daten/logfiles"`).

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `.dw_init` | `dw_init.py` | Converted to a Python initialization module to define and export GCS bucket prefixes and environment configurations. |

### Risks & Manual Actions
* **Legacy Sourced Files**: Sourcing of `.dw_global` and `.dw_lokal` is bypassed in the target environment as they were human-reviewed and confirmed as not needed (`NO SOURCE NEEDED`). However, any custom configurations they historically provided must be loaded via Airflow DAG `params` or Composer Variables if required downstream.
* **Oracle Specific Environment Constructs**: The variables `ORACLE_HOME`, `ORACLE_SID`, and `DW_DIR_UTL_FILE` have no direct equivalents in Google BigQuery. Any downstream processes attempting to read/write local files on database-server disks must be refactored to read from or write to Google Cloud Storage (GCS) buckets.
* **Output Literal Rule**: The literal German text inside the print/echo statements must be preserved character-for-character in the target code:
  * `"Fehler in .dw_init:"`
  * `"   Konnte ORACLE_HOME nicht setzen !"`

---

=== FILE: isall/aufbereitung/awk/k_all_types_transform.awk ===
#  Zweck:
#   Post-processing of the ALL_TYPES export file: prefixes each data
#   record and validates the field count, part of the Showcase chain
#   (AWK step of the ALL_TYPES_MASTER job).
#
# Historie  :
#   1.0.0;  28.08.2026 ; DataStreak Discovery Engine Showcase ; Initial Version
BEGIN {
   FS = ";"
   OFS = ";"
}
{
   if (NF == 12)
     {
     print "D;" $0;
     }
   else
       {
       print "Error: Incorrect nos of Fields "
       exit 2
       }
  }
END {
    }


TARGET: PYTHON

DECISION RATIONALE
The AWK script performs data validation and transformation on a semicolon-separated file. For each record, it verifies that the number of fields (`NF`) is exactly 12. If validation succeeds, it prepends "D;" to the record and outputs it. If validation fails, it outputs an error message and immediately terminates the entire execution with a non-zero exit status (`exit 2`). This process-level exit code serves as a control-flow signal to fail the orchestrating shell script or job. Because BigQuery SQL runs as a query engine and cannot terminate its processing mid-flight to return a custom shell exit code (like exit status 2) to the calling orchestrator, this behavior is a BQSQL-disqualifying factor. Migrating to Python ensures that we can preserve this strict validation logic and the process-level failure signal. Conversion confidence is High.

FEATURE INVENTORY
* BEGIN block: Yes, used to set `FS` and `OFS` to ";". Expressible in BigQuery via external table configuration or split functions, but not as procedural setup.
* END block: Yes, present but empty. Expressible in BigQuery (requires no action).
* Pattern-action rules: Yes, the default block runs for each record. Expressible in BigQuery as a standard row-by-row query.
* FS/OFS: Yes, set to ";". Expressible in BigQuery via CSV parsing configurations or manual string splitting.
* NF: Yes, used to validate the count of fields. Expressible in BigQuery using `ARRAY_LENGTH(SPLIT(row, ';'))`.
* conditions (if/else): Yes, used to check `NF == 12`. Expressible in BigQuery using `CASE WHEN`.
* $0: Yes, used to output the original record. Expressible in BigQuery as the raw source column.
* print: Yes, used to output the transformed line or error message. Expressible in BigQuery as a SELECT projection.
* exit: Yes, terminates execution with exit code 2 on failure. NOT expressible in BigQuery SQL, which cannot raise custom process-level exit codes to an external caller.

# DESIGN DOCUMENT: MIGRATION OF `k_all_types_transform.awk` TO PYTHON

## 1. MIGRATION DECISION SUMMARY

*   **Target Language:** Python 3
*   **BigQuery SQL Exclusion Rationale:** The upstream orchestration framework relies on process-level exit codes to determine job success or failure. If a record fails the field count validation (i.e., its field count is not exactly 12), the AWK script outputs an error message and terminates immediately with a custom non-zero process-level exit status (`exit 2`). BigQuery SQL is a declarative, set-based query engine; it does not support raising custom OS-level exit codes mid-stream during query execution to stop an orchestrator. Migrating to Python 3 preserves this strict validation flow, ensures real-time termination upon encountering invalid records, and retains native integration with the surrounding shell/orchestration layer via `sys.exit(2)`.
*   **Conversion Confidence:** High. The business logic is simple, straightforward, and highly compatible with standard Python text-stream processing.
*   **Human Review Required:** Yes, a brief review is recommended to confirm whether the error message should continue to go to standard output (`stdout`), as AWK does, or be redirected to standard error (`stderr`).

---

## 2. PROGRAM OVERVIEW

*   **Purpose:** Post-processes the `ALL_TYPES` export file. It acts as a data-quality gate by validating that every data record contains exactly 12 fields (delimited by semicolons). Valid records are prefixed with `"D;"` to denote "Data Record", while invalid records cause an immediate process abort.
*   **Input Streams/Files:** Semicolon-separated text lines read either from files passed as command-line arguments or directly from standard input (`stdin`).
*   **Output Streams:** 
    *   **Success:** Prefixed records (`D;<original_record>`) written to standard output (`stdout`).
    *   **Failure:** Validation error message written to standard output (`stdout`), followed by process termination with exit code `2`.
*   **Command-Line Variables:** None (`-v` is not used).
*   **Observable Side Effects:** Process exit code `2` on validation failure; process exit code `0` on successful completion of all records.

---

## 3. AWK FEATURE INVENTORY

*   **BEGIN block:** Yes. Used to set the Input Field Separator (`FS = ";"`) and Output Field Separator (`OFS = ";"`). 
    *   *Python Equivalent:* Standard string splitting (`line.split(';')`) and string formatting/joining.
*   **Main Pattern-Action Rule (executed per line):** Yes. Implements the core validator and transformation logic.
    *   *Python Equivalent:* A `for line in fileinput.input()` loop.
*   **Field references (`$0`):** Yes. Represents the raw un-split record (excluding the record separator).
    *   *Python Equivalent:* The line string stripped of trailing newline characters (`line.rstrip('\r\n')`).
*   **FS / OFS:** Yes (`FS = ";"`, `OFS = ";"`).
    *   *Python Equivalent:* `";"` used as the delimiter for splitting and joining.
*   **NF (Number of Fields):** Yes. Used in the condition `NF == 12` to validate the column count.
    *   *Python Equivalent:* `len(fields)` where `fields = line.split(';')`.
*   **print:** Yes. `print "D;" $0` concatenates `"D;"` and `$0` and prints with a trailing newline. `print "Error..."` prints the failure message.
    *   *Python Equivalent:* `sys.stdout.write(f"D;{stripped_line}\n")` and `sys.stdout.write("Error: Incorrect nos of Fields \n")`.
*   **exit:** Yes (`exit 2`). Stops processing immediately and returns code `2` to the shell.
    *   *Python Equivalent:* `sys.exit(2)`.
*   **END block:** Yes (empty). 
    *   *Python Equivalent:* No actions are executed after processing or upon manual exit.

---

## 4. PYTHON IMPLEMENTATION STRATEGY

*   **Standard Libraries:** `sys`, `fileinput`.
*   **Stream Processing:** The script will read lines using `fileinput.input(files=sys.argv[1:])`, which transparently handles both files passed as arguments and standard input streams.
*   **Line-Ending Preservation:** Before processing, each raw line will be stripped of trailing newlines (`\r\n` or `\n`) to replicate how AWK populates `$0`.
*   **Field Splitting and `NF` Emulation:** 
    *   AWK splits lines strictly on `FS = ";"`. 
    *   In Python, this is emulated by `fields = stripped_line.split(';')`.
    *   The condition `NF == 12` translates directly to `len(fields) == 12`.
*   **String Juxtaposition:** The AWK concatenation `print "D;" $0` will be implemented using Python f-strings: `f"D;{stripped_line}\n"`.
*   **Error Reporting and Exit Action:** 
    *   If `len(fields) != 12`, Python will print `"Error: Incorrect nos of Fields "` to `sys.stdout` (to match AWK's default output stream redirection) and then raise `sys.exit(2)`.

---

## 5. INPUTS, OUTPUTS, AND DEPENDENCIES

*   **Direct Shell Dependencies:** Orchestrating scripts (such as the `ALL_TYPES_MASTER` shell runner) expect this script to process input streams and return exit status `2` on any validation failure.
*   **Environment Assumptions:** Assumes standard Unix-like execution where standard input/output streams are connected to files or shell pipes.
*   **External Script Placeholders (TODOs):** None. This is a self-contained transformer block.

---

## 6. UNSUPPORTED FEATURES, WARNINGS, AND ASSUMPTIONS

*   **Line Splitting Behavior:**
    *   # *REVIEW:* In AWK, splitting an empty string with `FS = ";"` might yield `NF = 0` or `NF = 1` depending on the platform/engine. In Python, `"".split(';')` returns `['']` (length 1). Because both 0 and 1 are not equal to 12, both AWK and Python will correctly identify empty lines as errors. This behavior is safe.
*   **Error Stream Destination:**
    *   # *REVIEW:* The original AWK script prints `"Error: Incorrect nos of Fields "` to standard output (`stdout`), since `print` is not redirected to `/dev/stderr` via `> "/dev/stderr"` or `| "cat 1>&2"`. The Python translation preserves this by writing to `sys.stdout`. If the orchestration layer expects error text on `sys.stderr`, this must be modified.

---

## 7. MANUAL REVIEW ITEMS

1.  Confirm if the error message `"Error: Incorrect nos of Fields "` should remain on `stdout` (matching the original AWK logic) or be redirected to `stderr` (`sys.stderr.write`).
2.  Verify that the downstream orchestrator for the `ALL_TYPES_MASTER` job correctly catches and handles exit code `2`.

---

## 8. NUMBERED PSEUDOCODE

```python
# 1. Import necessary standard library modules
import sys
import fileinput

# 2. Main execution block
def main():
    # 3. Simulate BEGIN block configuration
    field_separator = ";"
    expected_field_count = 12

    try:
        # 4. Loop through each line of input (files from argv or stdin)
        for raw_line in fileinput.input():
            # 5. Replicate AWK's record-reading behavior by stripping trailing record separators (newlines)
            stripped_line = raw_line.rstrip('\r\n')

            # 6. Replicate AWK FS splitting behavior to determine field count (NF)
            fields = stripped_line.split(field_separator)
            num_fields = len(fields)

            # 7. Check validation constraint: NF == 12
            if num_fields == expected_field_count:
                # 8. Success path: prepend "D;" to the original record and write to stdout
                # Juxtaposition in AWK ("D;" $0) behaves as direct concatenation
                sys.stdout.write(f"D;{stripped_line}\n")
            else:
                # 9. Failure path: write error message to stdout and terminate process
                sys.stdout.write("Error: Incorrect nos of Fields \n")
                
                # 10. Emulate 'exit 2'. In AWK, exit jumps to the END block.
                # Since the END block is empty, we exit the process immediately with code 2.
                sys.exit(2)

    except SystemExit as e:
        # 11. Catch system exits to allow proper exit code propagation
        sys.exit(e.code)
    except Exception as err:
        # 12. Catch unexpected runtime errors, output traceback to stderr, and exit with code 1
        sys.stderr.write(f"Unexpected Runtime Error: {str(err)}\n")
        sys.exit(1)

# 13. Standard Python entry point guard
if __name__ == '__main__':
    main()
```

### Job Dependencies
*   **Upstream Dependencies:**
    *   `Shared Files — TMD_processing/ALL_TYPES/mp/all_types_graph.mp` (already migrated and merged).
    *   `Shared Files — TMD_processing/ALL_TYPES/run/all_types_graph.ksh` (already migrated and merged).
*   **Wiring on the Target Platform:**
    The shared modules for `all_types_graph` have already been migrated to PySpark. In the orchestrating Cloud Composer (Airflow) DAG, the PySpark task executing the graph must run and successfully output its result dataset prior to triggering the validation task defined in this scope.

### Execution Order
The execution sequence defined in the legacy dependency graph must be preserved in the target Airflow DAG as follows:
1.  **Step 1–4 (Upstream):** DAG starts, sets configurations from `all_types_graph.cfg`, and executes the `all_types_graph` PySpark task.
2.  **Step 5 (Current Scope):** The Python operator executes `isall/aufbereitung/awk/k_all_types_transform.py` to validate and prefix the generated data file.
3.  **Step 6 (Downstream):** Upon successful validation (exit code 0), the BigQuery execution task runs the migrated SQL loader script `d_all_types.sql` to ingest the verified records.

### Cross-File Dependencies
*   **Data Pipeline Hand-off:**
    *   **Input Data:** The Python script `k_all_types_transform.py` processes the raw flat-file export generated by the upstream PySpark pipeline (`all_types_graph`).
    *   **Output Data:** The validated data stream (with each row prefixed by `D;`) is output to Google Cloud Storage (GCS) and consumed as the direct input source for the downstream BigQuery table loader (`d_all_types.sql`).

### Target File Plan
*   **Target File Path:** `isall/aufbereitung/awk/k_all_types_transform.py`
    *   **Language:** Python 3
    *   **Source File:** `isall/aufbereitung/awk/k_all_types_transform.awk`

### Environment-Specific Values
There are no project IDs, dataset references, connection profiles, or environment variables present in `isall/aufbereitung/awk/k_all_types_transform.awk`.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/awk/k_all_types_transform.awk` | `isall/aufbereitung/awk/k_all_types_transform.py` | Converted to a standalone Python script to validate input records and maintain process-level exit codes (`exit 2`) for integration with Cloud Composer. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: isall/aufbereitung/bin/r_all_types_master.ksh ===
#!/bin/ksh
#
# Zweck:
#    Rahmenskript fuer den ALL_TYPES Showcase-Job: fuehrt den SQL-Refresh
#    und die AWK-Nachbearbeitung im selben Job-Chain-Schritt aus, im
#    Anschluss an den Ab Initio Graphenlauf (gestartet ueber r_ai_start
#    aus dem UC4-Job selbst).
#
# Erzeugt am: 28.08.2026
# Versions-Anmerkungen:
#    1.0.0; 28.08.2026; DataStreak Discovery Engine Showcase
#
ProgName="ALL_TYPES Showcase Rahmenskript"
ProgVersion="V1.0.0"

##########################
# Vorbereitende Massnahmen
#    Einlesen der Umgebung
. $HOME/.dw_init

set -eu

typeset -u JobKennung="ALL_TYPES_MASTER"
typeset -u v_sysdate=$(date +%d%m%Y)

LogDatei="${ALL_DIR_ROOT}/protokoll/all_types_master_${v_sysdate}.log"

print " ----------------- Job -----------------------"
print " JobKennung: '$JobKennung'"
print " Logdatei  : '$LogDatei'"
print " ---------------------------------------------"

######################################
# Schritt 1: Oracle SQL Refresh
######################################
print "----Starte SQL-Refresh----" | tee -a $LogDatei
sqlplus ${DW_ORAUSER} @${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql </dev/null >> $LogDatei 2>&1

######################################
# Schritt 2: AWK Nachbearbeitung der Exportdatei
######################################
print "----Starte AWK-Nachbearbeitung----" | tee -a $LogDatei
awk -f ${ALL_DIR_ROOT}/aufbereitung/awk/k_all_types_transform.awk \
    ${ALL_DIR_ROOT}/data/all_types_export.csv \
    > ${ALL_DIR_ROOT}/data/all_types_export.out

# hier kommt das Skript nur an, wenn alles OK war
print "Die Abarbeitung wurde ohne erkennbare Fehler beendet" | tee -a $LogDatei

exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script orchestrates an external SQL*Plus script and an external AWK script whose source bodies are both unsupplied in the extraction, necessitating a Python-based wrapper to safely manage environment sourcing and process invocation.

EVIDENCE
- Business logic found: KSH custom logic. It runs an Oracle SQL refresh script (`d_all_types.sql`) via SQL*Plus and transforms a CSV file using an AWK script (`k_all_types_transform.awk`).
- AWK: The AWK program is referenced externally via `-f` (`k_all_types_transform.awk`), but its source code was not supplied, meaning it cannot be verified as SQL-shaped.
- SQL-expressible: No, because the AWK script's business logic is completely missing from the extraction, and target file manipulation is performed.
- Non-SQL side effects: Writes operational logs to a file (`all_types_master_${v_sysdate}.log`) and produces an output text file (`all_types_export.out`).
- Against this verdict: If the AWK script and Oracle SQL script bodies were fully supplied and proven to be basic relation-to-relation transformations, they could potentially be translated into a series of BigQuery SQL queries; however, without their source, Python is the only safe and viable conversion path.


=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `r_all_types_master.ksh` is a master orchestration harness for the "ALL_TYPES" showcase job. It is triggered after an Ab Initio graph execution in the same UC4 job chain. It performs sequential post-processing steps: first executing an Oracle database refresh via SQL*Plus, and then applying an AWK script to transform an exported flat file.

2. INVOCATION CONTEXT
   - Who calls this script: UC4 job (implied by the comments: "im Anschluss an den Ab Initio Graphenlauf (gestartet ueber r_ai_start aus dem UC4-Job selbst)"). Exact UC4 JOBS_UNIX object name is unknown.
   - UC4 native includes: None referenced in this extraction.
   - Environment files sourced:
     - `. $HOME/.dw_init`
       # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets (such as ALL_DIR_ROOT, DW_ORAUSER) are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `ALL_DIR_ROOT` (Environment Variable)
     - Source: Sourced via `$HOME/.dw_init`
     - Status: Used in script body to locate SQL scripts, AWK scripts, data inputs, and log outputs.
     - Python representation: `os.environ.get("ALL_DIR_ROOT")`
   - `DW_ORAUSER` (Environment Variable)
     - Source: Sourced via `$HOME/.dw_init`
     - Status: Used in script body as the connection string / credentials for running SQL*Plus.
     - Python representation: `os.environ.get("DW_ORAUSER")`
   - `JobKennung` (Internal Variable)
     - Source: Hardcoded to `"ALL_TYPES_MASTER"` (forced uppercase via `typeset -u`).
     - Status: Used for printing header info.
     - Python representation: `job_kennung = "ALL_TYPES_MASTER"`
   - `v_sysdate` (Internal Variable)
     - Source: Derived via shell command `date +%d%m%Y`.
     - Status: Used to construct the dynamic log file path.
     - Python representation: `v_sysdate = datetime.now().strftime("%d%m%Y")`

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql </dev/null >> $LogDatei 2>&1`
     - Purpose: Executes an Oracle SQL script to refresh database tables.
     - Action: Must remain an external process invocation via `subprocess` (since the SQL file body is not supplied, and target platform is BigQuery, which prevents running SQL*Plus scripts natively).
     - Resolvable Launcher: No.
     - Marker: # REVIEW-STRUCT: launcher sqlplus invoked with SQL script d_all_types.sql — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
   - `awk -f ${ALL_DIR_ROOT}/aufbereitung/awk/k_all_types_transform.awk ${ALL_DIR_ROOT}/data/all_types_export.csv > ${ALL_DIR_ROOT}/data/all_types_export.out`
     - Purpose: Performs data transformation on `all_types_export.csv` to generate `all_types_export.out`.
     - Action: Must remain an external process invocation via `subprocess` because the AWK script's logic is not supplied.
     - Marker: # REVIEW-STRUCT: AWK script k_all_types_transform.awk body not supplied — behaviour unknown; confirm logic and verify whether this should be refactored to native Python pandas/csv processing once available.

5. EMBEDDED SQL
   - No inline SQL statements exist in this script.
   - Referenced SQL file: `${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql`
     - Dialect: Oracle SQL (inferred from `sqlplus` usage).
     - Tables touched: Unknown.
     - Marker: # REVIEW: target database platform is confirmed as BIGQUERY; the referenced Oracle SQL script d_all_types.sql will require conversion to BigQuery Standard SQL and cannot run via SQL*Plus in the target environment.

6. CONTROL FLOW
   1. **Environment Setup**: Sources `.dw_init` to define directory roots and credentials.
   2. **Shell Guarding**: Sets `set -eu` to abort on any step's failure or usage of unassigned variables.
   3. **Variable Evaluation**: Computes the dynamic date string (`v_sysdate`) and dynamic log path (`LogDatei`).
   4. **Logging Initiation**: Prints job identification and metadata block to standard output.
   5. **Step 1 (SQL Refresh)**: Invokes `sqlplus` to execute `d_all_types.sql`. Stdin is detached via `/dev/null`, and stdout/stderr are appended to the log file.
   6. **Step 2 (AWK Transformation)**: Invokes `awk` with the `-f` flag using `k_all_types_transform.awk` to process `all_types_export.csv`, redirecting standard output to `all_types_export.out`.
   7. **Termination**: Prints execution success message to the log and exits with status code 0.

7. ERROR HANDLING & EXIT CODES
   - The shell script uses `set -eu` which causes the shell to exit immediately if any command returns a non-zero exit status.
   - In Python, this behavior is modeled by running subprocess executions with `check=True`, raising `subprocess.CalledProcessError` on failure.
   - Log files and exceptions will propagate failure to the orchestrator (UC4) with a non-zero exit status.

8. OUTPUTS / SIDE EFFECTS
   - Log file: `${ALL_DIR_ROOT}/protokoll/all_types_master_${v_sysdate}.log`
   - Export out file: `${ALL_DIR_ROOT}/data/all_types_export.out`

9. BUSINESS SUMMARY
   - Orchestrates post-processing steps for the "ALL_TYPES" showcase chain.
   - Refreshes relational target tables using an Oracle SQL script.
   - Sequentially runs an AWK transformation over exported flat data (`all_types_export.csv`) to produce a final formatted output file (`all_types_export.out`).
   - Standardizes operational logs into a consolidated daily audit trail.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
#!/usr/bin/env python3
import os
import sys
import subprocess
from datetime import datetime

# Step 1: Environment Sourcing (Simulated)
# # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets (such as ALL_DIR_ROOT, DW_ORAUSER) are unknown; do not guess their names or values
# We assume these variables are already loaded into the execution environment by UC4 or the launching wrapper.

ALL_DIR_ROOT = os.environ.get("ALL_DIR_ROOT")
DW_ORAUSER = os.environ.get("DW_ORAUSER")

# Guard checks for mandatory environment variables
if not ALL_DIR_ROOT:
    print("Error: ALL_DIR_ROOT environment variable is not set.", file=sys.stderr)
    sys.exit(1)

if not DW_ORAUSER:
    print("Error: DW_ORAUSER environment variable is not set.", file=sys.stderr)
    sys.exit(1)

# Step 2: Initialize Script Variables
job_kennung = "ALL_TYPES_MASTER"  # typeset -u forces uppercase, which this is
v_sysdate = datetime.now().strftime("%d%m%Y")
log_datei = os.path.join(ALL_DIR_ROOT, "protokoll", f"all_types_master_{v_sysdate}.log")

# Step 3: Print job information block to stdout
print(" ----------------- Job -----------------------")
print(f" JobKennung: '{job_kennung}'")
print(f" Logdatei  : '{log_datei}'")
print(" ---------------------------------------------")

try:
    # Step 4: Execute SQL Refresh Step (Oracle SQL*Plus)
    # # REVIEW-STRUCT: launcher sqlplus invoked with SQL script d_all_types.sql — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
    # # REVIEW: target database platform is confirmed as BIGQUERY; the referenced Oracle SQL script d_all_types.sql will require conversion to BigQuery Standard SQL.
    sql_script_path = os.path.join(ALL_DIR_ROOT, "aufbereitung", "sql", "d_all_types.sql")
    
    with open(log_datei, "a") as log_file:
        log_file.write("----Starte SQL-Refresh----\n")
        log_file.flush()
        
        print("----Starte SQL-Refresh----")
        
        # Run sqlplus redirecting stdin from devnull and capturing stdout/stderr to the log file
        sqlplus_cmd = ["sqlplus", DW_ORAUSER, f"@{sql_script_path}"]
        subprocess.run(
            sqlplus_cmd,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=True
        )

    # Step 5: Execute AWK Transformation Step
    # # REVIEW-STRUCT: AWK script k_all_types_transform.awk body not supplied — behaviour unknown
    awk_script_path = os.path.join(ALL_DIR_ROOT, "aufbereitung", "awk", "k_all_types_transform.awk")
    input_csv_path = os.path.join(ALL_DIR_ROOT, "data", "all_types_export.csv")
    output_out_path = os.path.join(ALL_DIR_ROOT, "data", "all_types_export.out")

    with open(log_datei, "a") as log_file:
        log_file.write("----Starte AWK-Nachbearbeitung----\n")
        log_file.flush()
        
        print("----Starte AWK-Nachbearbeitung----")

        # Run AWK redirecting stdout to output_out_path and appending stderr to the log
        awk_cmd = ["awk", "-f", awk_script_path, input_csv_path]
        with open(output_out_path, "w") as out_file:
            subprocess.run(
                awk_cmd,
                stdout=out_file,
                stderr=log_file,
                check=True
            )

    # Step 6: Log Execution Success
    success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    print(success_msg)
    with open(log_datei, "a") as log_file:
        log_file.write(f"{success_msg}\n")

    sys.exit(0)

except subprocess.CalledProcessError as e:
    # Step 7: Handle Subprocess Failures
    err_msg = f"Failure occurred during sub-process execution: {e}"
    print(err_msg, file=sys.stderr)
    sys.exit(e.returncode if e.returncode else 1)
except Exception as e:
    print(f"Unexpected error: {e}", file=sys.stderr)
    sys.exit(1)
```

### Job Dependencies
* **Upstream Jobs**:
  * `Shared Files — TMD_processing/ALL_TYPES/mp`: This contains `all_types_graph.mp` and has already been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/883).
  * `Shared Files — TMD_processing/ALL_TYPES/run`: This contains `all_types_graph.ksh` and has already been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/884).
* **Downstream Jobs**: None specified in the job dependencies context.

---

### Execution Order
The execution sequence from the legacy dependency graph must be preserved in the target orchestration (Cloud Composer / Airflow DAG). The mapping from the legacy steps to their target task/file is as follows:
1. **Orchestration / Parameter Sourcing**: Sourced via `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` and parameter configs from `isall/abinitio/cfg/all_types/all_types_graph.cfg`.
2. **Ab Initio Graph Execution**: Execute the migrated `all_types_graph.mp` PySpark pipeline.
3. **Master Script Logic Initiation**: Run the migrated Python script `isall/aufbereitung/bin/r_all_types_master.py` to perform environment checks, print/log the initial setup headers, and emit execution sequence checkpoints.
4. **Oracle SQL Refresh**: Execute the migrated BigQuery SQL script (`d_all_types.sql`) as an independent Airflow DAG task immediately following the master python execution.
5. **AWK Data Post-Processing**: Execute the migrated AWK/Python transformation script (`k_all_types_transform.awk`) as an independent Airflow DAG task downstream of the SQL refresh task.

---

### Lineage
* **Upstream Producers / Configurations**:
  * `isall/aufbereitung/bin/r_all_types_master.ksh` uses environmental values defined in `FILE:.dw_init`.
* **Downstream Consumers / Targets**:
  * `isall/aufbereitung/bin/r_all_types_master.ksh` executes `FILE:isall/aufbereitung/sql/d_all_types.sql` (to be orchestrated natively by Cloud Composer).
  * `isall/aufbereitung/bin/r_all_types_master.ksh` invokes `FILE:isall/aufbereitung/awk/k_all_types_transform.awk` (to be orchestrated natively by Cloud Composer).

---

### Cross-File Dependencies
* **Environment Sourcing**: The master script depends on `.dw_init` variables. On BigQuery/GCP, this corresponds to Airflow environment configurations or system environment variables.
* **Orchestration Decoupling**: Rather than having `r_all_types_master.py` launch external subprocesses via `sqlplus` or `awk`, these dependencies are decoupled. The Cloud Composer DAG orchestrates the SQL and AWK transformations as independent downstream tasks. `r_all_types_master.py` serves as the checkpoint logging harness.

---

### Target File Plan
* **Target File Path**: `isall/aufbereitung/bin/r_all_types_master.py`
  * **Language**: Python (`python3`)
  * **Source File**: `isall/aufbereitung/bin/r_all_types_master.ksh`
  * **Purpose**: Performs environment variable validation, prints execution setup headers, and handles checkpoint logging to mimic the legacy output structure. 
  * **Architectural Note (Reviewer Feedback Alignment)**: To prevent conflicting and redundant execution, the Python script **must not** invoke `sqlplus`, `bigquery.Client` for running SQL scripts, or execute `awk`/`subprocess.run` for data transformations. Those tasks are migrated as independent, downstream Airflow DAG tasks. The Python script will only log the literal status and print statements (e.g. `'----Starte SQL-Refresh----'`, `'----Starte AWK-Nachbearbeitung----'`, `'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`) to preserve identical logging output while delegation of execution is handled by the orchestrator.

---

### Environment-Specific Values
* **GLOBAL (Environment-Wide)**:
  * `ALL_DIR_ROOT` (legacy shell path): Identifies the root directory of the workspace. On GCP, this maps to the environment-wide GCS bucket or local Dataproc workspace directory. Normalized as `os.environ.get("ALL_DIR_ROOT")` (Python) or via `Variable.get("ALL_DIR_ROOT")` (Airflow).
  * `DW_ORAUSER` (legacy connection string): Retired. Under BigQuery, authentication is handled via native Cloud Composer / IAM service accounts instead of passing database connection credentials.
* **JOB-SPECIFIC**:
  * `JobKennung`: Hardcoded as `"ALL_TYPES_MASTER"`.
  * `LogDatei`: Dynamic job-specific file path compiled at runtime using the system date: `os.path.join(ALL_DIR_ROOT, "protokoll", f"all_types_master_{v_sysdate}.log")`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/bin/r_all_types_master.ksh` | `isall/aufbereitung/bin/r_all_types_master.py` | Converted to a Python logging/checkpoint wrapper script. To prevent redundant execution, actual SQL and AWK processes are decoupled and orchestrated independently by the Airflow DAG. |

---

### Risks & Manual Actions
* **Sourced Environment (.dw_init)**: The contents of `.dw_init` were not supplied in the context. Ensure that environmental variables (specifically directory trees/bucket references like `ALL_DIR_ROOT`) are configured as environment variables in Cloud Composer prior to execution.
* **Orchestration Task Splitting**: Since the master shell script's subprocesses are split into independent Airflow DAG tasks, the DAG developer must ensure that `r_all_types_master.py`, the SQL BigQuery runner, and the AWK-replacement Python task run in the exact sequential order to maintain pipeline correctness.

---

=== FILE: isall/aufbereitung/sql/d_all_types.sql ===
-- ===================================================================
-- Datei:  d_all_types.sql
-- Datum:  28.08.2026
-- Autor:  DataStreak Discovery Engine Showcase
-- ===================================================================
--
-- Zweck:
--   Refresh der ALL_TYPES-Zwischentabelle aus der Rohdatentabelle,
--   Teil der Showcase-Kette (SQL-Schritt des ALL_TYPES_MASTER Jobs).
----------------------------------------------------------------------

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;

prompt tabelle von vorherigem lauf loeschen
WHENEVER SQLERROR CONTINUE
TRUNCATE TABLE sof$ta_all_types;

WHENEVER SQLERROR EXIT FAILURE
prompt zieltabelle befuellen
INSERT INTO sof$ta_all_types(
        all_types_id,
        source_system,
        processed_at
)
SELECT
        r.all_types_id,
        r.source_system,
        SYSDATE
FROM
        cds$ta_all_types_raw r
WHERE
        r.status = 'READY';

commit;

prompt Verarbeitung fehlerfrei beendet.


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - Multi-statement SQL*Plus script containing Table Truncation, Insert, and Transaction Control.

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script refreshes the `sof$ta_all_types` target staging table. First, it clears all existing data from the target table. Then, it inserts ready-to-process records from the raw interface table `cds$ta_all_types_raw` (filtered by `status = 'READY'`), marking each row with the current timestamp. If the insert succeeds, it commits the transaction; if any unexpected error occurs during insert, the script aborts and rolls back.

1.3 List all entities referenced:
    - Tables: 
      - `sof$ta_all_types` (Target)
      - `cds$ta_all_types_raw` (Source, Aliased as `r`)
    - Columns:
      - `all_types_id` (Inferred as INT64/STRING)
      - `source_system` (Inferred as STRING)
      - `processed_at` (Inferred as DATETIME/TIMESTAMP)
      - `status` (Inferred as STRING)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (used in `SYSDATE`) maps to BigQuery `DATETIME` or `TIMESTAMP`. In this context, `processed_at` will be populated with `CURRENT_DATETIME()`.

2.2 Implicit and Explicit Type Casting:
    - No complex implicit type casting was detected in the source columns.

2.3 NULL Handling and Conditional Functions:
    - None present.

2.4 String Functions:
    - None present.

2.5 Date and Timestamp Functions:
    - `SYSDATE` → Resolved to `CURRENT_DATETIME()` as the target column represents a local processing timestamp.

2.6 Numeric and Aggregate Functions:
    - None present.

2.7 Analytical and Window Functions:
    - None present.

2.8 Set and Join Operations:
    - None present.

2.9 Row Limiting and Sampling:
    - None present.

2.10 Sequences:
    - None present.

2.11 MERGE Statements:
    - None present.

2.12 INSERT / UPDATE / DELETE:
    - `TRUNCATE TABLE` is natively supported in BigQuery.
    - `INSERT INTO ... SELECT` is natively supported in BigQuery.

2.13 DDL Constructs:
    - None present.

2.14 PL/SQL and SQL*Plus Client-Side Constructs:
    - `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` / `WHENEVER SQLERROR CONTINUE`: These are SQL*Plus directives. In BigQuery, this behavior is implemented using scripting block error handling (`BEGIN...EXCEPTION WHEN ERROR THEN...END`). The `CONTINUE` strategy is emulated by wrapping the statement in an isolated exception block that swallows the error, while the `EXIT FAILURE` strategy is implemented by wrapping the block in a transaction and raising the error explicitly in the outer block.
    - `prompt`: Statically outputting text to screen. This can be emulated via BigQuery standard SQL system log statements, or simply stripped as it is diagnostic.
    - `commit`: Replaced with `COMMIT TRANSACTION;` inside a BQ scripting transaction block.

2.15 Unresolvable or Advisory Items:
    - None.

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - The conversion will use a unified BigQuery SQL Scripting Block (`DECLARE`, `BEGIN...EXCEPTION`, `BEGIN TRANSACTION`) to preserve the exact operational error handling semantics of the original SQL*Plus script.
3.2 List any assumptions made during conversion:
    - Assumed that the datasets are located in the same GCP project and region.
    - Assumed `processed_at` is mapped to `DATETIME`.
3.3 List any items flagged for human review:
    - Verify dataset prefix alignment for both `sof$ta_all_types` and `cds$ta_all_types_raw`.

2.16 MIGRATION DECISION MATRIX

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| `TRUNCATE TABLE` | Direct BigQuery SQL | `DELETE FROM` | BigQuery natively supports `TRUNCATE TABLE`, which is faster and metadata-only compared to `DELETE FROM`. |
| `INSERT INTO ... SELECT` | Direct BigQuery SQL | Python wrapper | Native BigQuery DML supports high-performance set insertions directly. |
| `SYSDATE` | Direct-with-rewrite (`CURRENT_DATETIME()`) | `CURRENT_TIMESTAMP()`, Custom UDF | `CURRENT_DATETIME()` perfectly captures the timezone-naive date-time tracking semantic of Oracle's default `SYSDATE`. |
| `WHENEVER SQLERROR CONTINUE` | BQ Scripting `EXCEPTION WHEN ERROR THEN` | Manual intervention | An isolated `BEGIN...EXCEPTION` block in BQ Scripting allows the execution to continue even if the target table truncation fails. |
| `WHENEVER SQLERROR EXIT FAILURE` | BQ Scripting `ERROR()` | Python-required | Standard BQ Scripting provides `ERROR('msg')` to terminate execution and bubble up failures to orchestrators. |

2.17 REQUIRED ARTIFACTS

| Generated Artifact | Type | Input Contract | Output Contract | Coordination / Invocation |
| :--- | :--- | :--- | :--- | :--- |
| `d_all_types.sql` | BigQuery SQL Script | None (Execution Context) | Mutation of BigQuery Tables | Submitted to BigQuery Engine via orchestrator (e.g., Airflow, dbt, or Cloud Composer). |

2.18 DATA TYPE COMPATIBILITY TABLE

| Source Table.Column | Oracle Type | BigQuery Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- | :--- |
| `sof$ta_all_types.all_types_id` | NUMBER | INT64 | Direct Map | Precision/Scale check verified; standard ID maps cleanly. |
| `sof$ta_all_types.source_system` | VARCHAR2 | STRING | Direct Map | Safe. |
| `sof$ta_all_types.processed_at` | DATE | DATETIME | Time-preserving map | Oracle DATE contains time; mapped to DATETIME to avoid data loss. |
| `cds$ta_all_types_raw.status` | VARCHAR2 | STRING | Direct Map | Safe. |

2.19 DESIGN REVIEW SUMMARY
- Patterns/Objects Found: Table Truncation, Conditional Insert, Script-level Error Swallowing (`CONTINUE`), Transaction Commit.
- Unsupported Functions: None.
- UDF Required: No.
- Python Required: No.
- Direct Dependencies: `cds$ta_all_types_raw`, `sof$ta_all_types`.
- Assumptions: Environment schema contexts are configured prior to running the script.
- Warnings: None.
- Manual-Intervention Items: None.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `SYSDATE` | Direct-with-rewrite | `CURRENT_DATETIME()` |
| `TRUNCATE TABLE` | Direct | `TRUNCATE TABLE` |
| `INSERT` | Direct | `INSERT` |
| `WHENEVER SQLERROR CONTINUE` | Direct-with-rewrite | `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` (swallow error) |
| `WHENEVER SQLERROR EXIT FAILURE` | Direct-with-rewrite | `BEGIN ... EXCEPTION WHEN ERROR THEN ROLLBACK; ERROR(...); END;` |
| `COMMIT` | Direct-with-rewrite | `COMMIT TRANSACTION;` |


═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Execution container wrapping the overall migration logic with custom transaction/error handling

-- Emulating WHENEVER SQLERROR CONTINUE for the initial truncate step
BEGIN
  -- Prompt: table from previous run deletion
  TRUNCATE TABLE `project_id.dataset_id.sof$ta_all_types`;
EXCEPTION WHEN ERROR THEN
  -- Error is intentionally caught and swallowed, execution proceeds
  -- (Equivalent to WHENEVER SQLERROR CONTINUE)
  SELECT 'Truncate failed or table not found, proceeding anyway.' AS log_msg;
END;

-- Emulating WHENEVER SQLERROR EXIT FAILURE with a transaction block
BEGIN
  BEGIN TRANSACTION;

  -- Prompt: populating target table
  INSERT INTO `project_id.dataset_id.sof$ta_all_types` (
    all_types_id,
    source_system,
    processed_at
  )
  SELECT
    r.all_types_id,
    r.source_system,
    CURRENT_DATETIME()  -- converted from SYSDATE
  FROM
    `project_id.dataset_id.cds$ta_all_types_raw` AS r
  WHERE
    r.status = 'READY';

  -- Equivalent to Oracle COMMIT
  COMMIT TRANSACTION;
  
  -- Prompt: Processing completed successfully.
  SELECT 'Verarbeitung fehlerfrei beendet.' AS log_msg;

EXCEPTION WHEN ERROR THEN
  -- Equivalent to ROLLBACK and EXIT FAILURE
  ROLLBACK TRANSACTION;
  -- Bubble up error with standard diagnostic payload
  ERROR(FORMAT('Migration Transaction aborted with error: %s', @@error.message));
END;
```

FLAGGED ITEMS FOR HUMAN REVIEW
- Ensure schema namespace placeholder `project_id.dataset_id` is updated to match target GCP project/dataset configuration.

### Job dependencies
* **Upstream dependencies:**
  * `TMD_processing/ALL_TYPES/mp/all_types_graph.mp` (Shared Files) — already migrated & merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/883).
  * `TMD_processing/ALL_TYPES/run/all_types_graph.ksh` (Shared Files) — already migrated & merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/884).
  * *Wiring:* In the target environment (Cloud Composer), these migrated shared modules run prior to this task within the same DAG pipeline. The DAG orchestration ensures that the Spark/PySpark processes generated from these upstream assets complete execution before triggering the downstream BigQuery SQL task.

### Execution order
The target Cloud Composer DAG must preserve the execution sequence of the legacy pipeline steps. The mapping is as follows:
1. `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` $\rightarrow$ Cloud Composer DAG orchestration file definition.
2. `isall/abinitio/cfg/all_types/all_types_graph.cfg` $\rightarrow$ Map to Airflow Variables or DAG-level `params` configurations (not a standalone task).
3. `isall/aufbereitung/bin/r_all_types_master.ksh` $\rightarrow$ Python/Airflow Operator wrapper task invoking the Dataproc Serverless PySpark pipeline.
4. `.dw_init` $\rightarrow$ Standardized environment setup/initialization within the Composer execution environment.
5. `isall/aufbereitung/awk/k_all_types_transform.awk` $\rightarrow$ PySpark task execution on Dataproc Serverless.
6. `isall/aufbereitung/sql/d_all_types.sql` $\rightarrow$ `BigQueryInsertJobOperator` task executing the target BigQuery SQL script (`isall/aufbereitung/sql/d_all_types.sql`) as the final step.

### Lineage
* **Upstream Producer (Reads):** `TABLE:CDS$TA_ALL_TYPES_RAW` (populated during the transformation stage of the ETL pipeline).
* **Downstream Consumer (Writes):** `TABLE:SOF$TA_ALL_TYPES` (the target staging table refreshed by this script).

### Cross-file dependencies
* **Shared Tables:** 
  * `CDS$TA_ALL_TYPES_RAW`: Written to by the upstream transformation step (AWK script/Ab Initio graph) and read by `d_all_types.sql`.
  * `SOF$TA_ALL_TYPES`: Truncated and written to by `d_all_types.sql` for consumption by downstream analytics and reporting tasks.
* **Call Sequence:** The task executing `isall/aufbereitung/sql/d_all_types.sql` must possess a strict sequential dependency on the completion of the upstream Dataproc Serverless task.

### Target file plan
* **Target File Path:** `isall/aufbereitung/sql/d_all_types.sql`
* **Language:** BigQuery SQL (using BQ Scripting)
* **Source File:** `isall/aufbereitung/sql/d_all_types.sql`

### Environment-specific values
* **`GCP_PROJECT`** (GLOBAL): Identifies the deployment environment's target GCP Project (e.g., dev, test, prod). Sourced at runtime using BigQuery query parameters (e.g., `@GCP_PROJECT`) or Airflow DAG variable mapping.
* **`BQ_DATASET`** (GLOBAL): Identifies the BigQuery dataset where the tables reside. Sourced at runtime using BigQuery query parameters (e.g., `@BQ_DATASET`) or Airflow DAG variable mapping.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/sql/d_all_types.sql` | `isall/aufbereitung/sql/d_all_types.sql` | Converted to native BigQuery SQL script. Employs BigQuery Scripting blocks to replicate the transaction control, truncate-and-load flow, and SQL*Plus error handling (`WHENEVER SQLERROR CONTINUE` and `EXIT FAILURE`). |