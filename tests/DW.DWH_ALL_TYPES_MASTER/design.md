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


# UC4/Automic Migration Design Document: DW.DWH_ALL_TYPES_MASTER

This design document outlines the migration metadata, structural translation patterns, and Airflow DAG target definitions for the UC4 object `DW.DWH_ALL_TYPES_MASTER` extracted from the source system.

---

## 1. Overview
The workload consists of a single Unix-based UC4 job (`DW.DWH_ALL_TYPES_MASTER`) acting as a showcase workflow that combines Ab Initio graph components and an auxiliary Korn Shell (KSH) script. The job sets an environment metadata variable `DWH_JOB_KENNUNG` to `'ALL_TYPES_MASTER'`, launches an Ab Initio graph named `all_types_graph` via the `r_ai_start` utility, and then executes a downstream preparation script `r_all_types_master.ksh`. In the target environment, this job is migrated into an Apache Airflow DAG where the Ab Initio execution maps to a PySpark application executed on Google Cloud Dataproc.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ALL_TYPES_MASTER` | JOBS_UNIX | 1 | Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain |

---

## 3. Scheduling
* **Calendar Schedule**: No `EVNT_TIME` or schedule metadata objects are present in this extraction.
* **Trigger Mechanics**: No parent `JOBP` workflow or activation `SCRI` objects were supplied. This object is marked as **externally triggered / source unknown** from this extraction alone.
* **Airflow Schedule Property**: `schedule=None` (manual or external orchestration trigger).

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_all_types_master` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active flag in source = 1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `all_types_graph` | `DW.DWH_ALL_TYPES_MASTER` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/all_types_graph.py` | `project_id`, `region`, `cluster_name` placeholders | 1 | 5m | N/A | None | N/A | None | # REVIEW: The source script body contains a secondary KSH command execution: `$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh`. Confirm if its business logic must be merged into the PySpark job or split out. |

---

## 6. Task Dependency Map
Since this migration wraps a standalone `JOBS_UNIX` object supplied without an enclosing parent workflow, the resulting DAG contains a single workload execution block:

```
all_types_graph
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock declarations were detected in the object export metadata.

| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | N/A | None |

---

## 8. Error Handling and Retry Strategy
* **Retry Strategy**: The default task-level configuration of 1 retry with a 5-minute cooldown is mapped via `default_args`. No special UC4 post-conditions or custom event triggers were present.
* **Execution Flow**: Standard `ALL_SUCCESS` trigger rules apply.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'ALL_TYPES_MASTER'` | Passed as an environment variable or job argument: `--job_kennung=ALL_TYPES_MASTER` |
| `all_types` | `launcher_details['job_type']` | Passed as PySpark job argument: `--job_type=all_types` |
| `all_types_graph` | `launcher_details['key']` | Maps directly to the target Python script execution payload |

---

## 10. Developer Notes
* **#REVIEW-STRUCT:** This JOBS_UNIX object was supplied standalone without an parent `JOBP` workflow or calendar definition. The workflow has been structured as a standalone wrapper DAG.
* **#REVIEW:** The source shell script execution block contains an explicit KSH call downstream of the Ab Initio execution: `$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh`. The developer must analyze this shell script to determine if its data logic is being migrated to PySpark, or if a separate `BashOperator` or `SSHOperator` is required immediately downstream of the `all_types_graph` task.
* **GCP Infrastructure Placeholders:** Ensure that `YOUR_BUCKET_NAME`, `YOUR_PROJECT_ID`, `YOUR_REGION`, and `YOUR_CLUSTER_NAME` are populated via Airflow Variables or environment configurations before deployment.

---

# Migration Target Code Outline (Pseudocode)

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW: Configure target GCP environment placeholders
GCP_PROJECT_ID = "YOUR_PROJECT_ID"
GCP_REGION = "YOUR_REGION"
GCP_CLUSTER_NAME = "YOUR_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# # REVIEW-STRUCT: Standalone JOBS_UNIX representation
with DAG(
    dag_id="dw_dwh_all_types_master",
    default_args=default_args,
    description="Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # ── Task: all_types_graph ────────────────────────────
    # Maps to source Ab Initio Graph 'all_types_graph'
    # # REVIEW: Verify if downstream KSH script 'r_all_types_master.ksh' should be appended here
    pyspark_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": GCP_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/all_types_graph.py",
            "args": [
                "--job_arg=ALL_TYPES_MASTER",
                "--job_type=all_types",
                "--key=all_types_graph",
                "--job_kennung=ALL_TYPES_MASTER"
            ]
        }
    }

    all_types_graph_task = DataprocSubmitJobOperator(
        task_id="all_types_graph",
        job=pyspark_job,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task workload execution chain
    all_types_graph_task
```

# Migration Design Document: DW.DWH_ALL_TYPES_MASTER

This document defines the migration and orchestration design for the UC4 job `DW.DWH_ALL_TYPES_MASTER` into Apache Airflow targeting Google Cloud Platform (Cloud Composer, Dataproc Serverless, and BigQuery).

---

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` | `dags/dw_dwh_all_types_master.py` | Migrates the UC4 orchestration logic into an Apache Airflow DAG. Consolidates the Ab Initio execution task (submitting a Dataproc Serverless job) and the downstream master shell invocation (executing the migrated Python script). |

---

## 1. Job Dependencies
* **Upstream dependencies**:
  * **Shared Files — TMD_processing/ALL_TYPES/mp**: Already migrated and merged (PR [#883](https://github.com/gurunathan-prodapt/pi-agents/pull/883)). This contains the core Ab Initio graph logic, which maps to the target PySpark script `all_types_graph.py` on Google Cloud Storage.
  * **Shared Files — TMD_processing/ALL_TYPES/run**: Already migrated and merged (PR [#884](https://github.com/gurunathan-prodapt/pi-agents/pull/884)).
* **Downstream dependencies**:
  * **isall/aufbereitung/bin/r_all_types_master.ksh**: Migrated as Python script `isall/aufbereitung/bin/r_all_types_master.py` in its own design pass. The DAG will execute this downstream component immediately after the graph execution task succeeds.

---

## 2. Execution Order
The legacy dependency graph’s execution sequence is preserved in the target Airflow DAG using explicit task dependencies:
1. **DAG Initialization**: Loads variables from the migrated configuration environment.
2. **Ab Initio Graph Execution** (`all_types_graph`): Invoked as a `DataprocSubmitJobOperator` executing the migrated PySpark script `all_types_graph.py`.
3. **Downstream Master Prep Execution** (`task_r_all_types_master`): Replaces the legacy `r_all_types_master.ksh` execution. It is invoked via a `BashOperator` executing `isall/aufbereitung/bin/r_all_types_master.py` (which internally coordinates the migrated SQL and AWK logic).

---

## 3. Scheduling
* **Trigger Event / Schedulers**: No calendar schedule exists in the source XML.
* **Target Scheduling**: Configured with `schedule=None` (external trigger or manual execution only).

---

## 4. Schedule & Variables — Must Be Retained
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` (Value: `'ALL_TYPES_MASTER'`): Used to identify the run. Passed as a job parameter to the Dataproc operator and as an environment variable to the Bash operator.
* **Configuration Parameters** (sourced from `isall/abinitio/cfg/all_types/all_types_graph.cfg`):
  * `ALL_TYPES_Projektverzeichnis` = `/Projects/TMD/processing/ALL_TYPES/`
  * `ALL_TYPES_Graph` = `all_types_graph`
  * `ALL_TYPES_Version` = `RLS_ALL_TYPES_current`
  * `ALL_TYPES_Prozesstyp` = `N`
  * `ALL_TYPES_Datenobjekt` = `-`
  * `ALL_TYPES_AI_DAT_FILE_DIR` = `$ALL_TYPES_DIR_EXP_UTL/cubes/at`
  
  These parameters are retrieved at runtime via Airflow Variables or a local DAG-level configuration dictionary and passed to the tasks.

---

## 5. Lineage
* **Upstream Source**: 
  * `TMD_processing/ALL_TYPES/mp/all_types_graph.mp` (Input design representing the raw schemas and flows).
  * `CDS$TA_ALL_TYPES_RAW` (Legacy source table, mapped to a BigQuery raw landing/ingestion table).
* **Downstream Target**:
  * `SOF$TA_ALL_TYPES` (Legacy final database target table, mapped to its BigQuery destination table).

---

## 6. Cross-File Dependencies
* The DAG orchestrates tasks that depend on the existence of `gs://{GCS_BUCKET}/pyspark_scripts/all_types_graph.py` (migrated PySpark graph script) and `/home/airflow/gcs/dags/isall/aufbereitung/bin/r_all_types_master.py` (migrated prep script).
* The execution of `r_all_types_master.py` relies on the outputs generated during the `all_types_graph` PySpark execution phase.

---

## 7. Target File Plan

### File: `dags/dw_dwh_all_types_master.py`
* **Language**: Python (Apache Airflow DAG)
* **Source File**: `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml`
* **Execution Flow**:
  1. `all_types_graph_task` (`DataprocSubmitJobOperator`) submitting the PySpark script `gs://{GCS_BUCKET}/pyspark_scripts/all_types_graph.py`.
  2. `task_r_all_types_master` (`BashOperator`) executing the command `python /home/airflow/gcs/dags/isall/aufbereitung/bin/r_all_types_master.py`.
* **Dependency Definition**:
  ```python
  all_types_graph_task >> task_r_all_types_master
  ```

---

## 8. Environment-Specific Values

All environment-specific variables are classified by role and sourced dynamically using Airflow's config mechanisms:

### GLOBAL (Environment-Wide Infrastructure)
* **GCP_PROJECT**: The target GCP project identifier. Sourced via `Variable.get("GCP_PROJECT")`.
* **GCP_REGION**: Target GCP region. Sourced via `Variable.get("GCP_REGION")`.
* **GCP_CLUSTER_NAME**: The target Dataproc cluster name. Sourced via `Variable.get("GCP_CLUSTER_NAME")`.
* **GCS_BUCKET**: The target GCS bucket containing script and data runtimes. Sourced via `Variable.get("GCS_BUCKET")`.
* **CCR_DIR_ROOT** / **HOME**: Legacy base directories. Sourced dynamically as `/home/airflow/gcs/dags` or via Airflow `Variable.get("GCS_BUCKET")` prefix references depending on script execution context.

### JOB-SPECIFIC (Job/Task level variables)
* **DW.UNIX.ISALL**: Legacy login/package scope. Handled by executing tasks under the configured IAM Service Account of the Cloud Composer environment.
* **DWH_JOB_KENNUNG**: Retained as a job parameter (`ALL_TYPES_MASTER`). Passed directly to the PySpark operator arguments and task environments.

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
VERDICT: NO_CONVERSION_REQUIRED
REASON: This is a pure environment initialization script (.dw_init) that only declares environment variables, checks the Oracle home directory, and sources other global and local configurations.

EVIDENCE
- Business logic found: None. The script is an environment initialization profile (`.dw_init`) used to define path variables, set a database remote host, and locate the `ORACLE_HOME` installation.
- AWK: none
- SQL-expressible: no, it contains only directory declarations, environment exports, file-system existence tests, and shell sourcing.
- Non-SQL side effects: none observed, other than printing warnings to stdout if ORACLE_HOME cannot be set.
- Against this verdict: The file contains a basic conditional structure (`if [ -z "$ORACLE_HOME" ]`) checking directory existence, but since it functions purely as a sourced dotfile to initialize a shell runtime, converting it to an independent Python or SQL script would be structurally meaningless.

ORCHESTRATION SUMMARY
- Purpose: To initialize environment variables, export standard data directories, detect `ORACLE_HOME`, and source global/local configurations for the Information Services shell runtime.
- Variables declared:
  - `DW_DIR_ROOT = $HOME/aktuell`
  - `DW_DIR_PROT = $HOME/daten/logfiles`
  - `DW_DIR_CUBES = $HOME/daten/cubes`
  - `DW_DIR_IMP_D1 = $HOME/daten/d1`
  - `DW_DIR_IMP_BWA = $HOME/daten/dpps/bwa`
  - `DW_DIR_IMP_XTRA = $HOME/daten/xtra`
  - `DW_DIR_IMP_CTEL = $HOME/daten/ctel`
  - `DW_DIR_IMP_VO = $HOME/daten/vo`
  - `DW_DIR_IMP_RV = $HOME/daten/rv`
  - `DW_DIR_IMP_IF = $HOME/daten/ees`
  - `DW_DIR_IMP_NNV = $HOME/daten/nnv`
  - `DW_DIR_IMP_SIGMA = $HOME/daten/gd/sigma`
  - `DW_DIR_EXP_SIGMA = $HOME/daten/gd/sigma/export`
  - `DW_DIR_IMP_TRF = $HOME/daten/trf`
  - `DW_DIR_IMP_AUF = $HOME/daten/sd/auf`
  - `DW_DIR_IMP_GUT = $HOME/daten/sd/gut`
  - `DW_DIR_IMP_KDG = $HOME/daten/sd/kdg`
  - `DW_DIR_IMP_MP_KDG = $HOME/daten/mp/kdg`
  - `DW_DIR_IMP_MP_TS = $HOME/daten/mp/ts`
  - `DW_DIR_IMP_MP_ZM = $HOME/daten/mp/zm`
  - `DW_DIR_IMP_TS = $HOME/daten/sd/ts`
  - `DW_DIR_IMP_ZM = $HOME/daten/sd/zm`
  - `DW_DIR_EXP = $HOME/daten/exporter`
  - `DW_DIR_IMP_BPM = $HOME/daten/bm`
  - `DW_DIR_IMP_ZTS = $HOME/daten/zts`
  - `DW_DIR_IMP_VRS = $HOME/daten/vrs`
  - `DW_DIR_IMP_BRUNET = $HOME/daten/brunet`
  - `DW_DIR_IMP_DWH = $HOME/daten/dwh`
  - `DW_DIR_IMP_PLATO = $HOME/daten/dwh/plato`
  - `DW_DIR_IMP_CARMEN = $HOME/daten/carmen`
  - `DW_DIR_IMP_SAP = $HOME/daten/sap`
  - `DW_DIR_IMP_SR_RV = $HOME/daten/sap/sr_rv_dpps`
  - `DW_DIR_IMP_SAP_L_GUTGR = $HOME/daten/sap/sap_l_gutgr` (exported as `DW_DIR_IMP_SAP_L`)
  - `DW_DIR_IMP_L_MAHNSTYP_IST = $HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_FI = $HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_IST = $HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_GUTGR = $HOME/daten/sd/l_gutschr`
  - `DW_DIR_IMP_L_LEIST = $HOME/daten/sd/l_leist`
  - `DW_DIR_IMP_L_PROD = $HOME/daten/sd/l_prod`
  - `DW_DIR_LKODE = $HOME/daten/sd/lkode`
  - `DW_DIR_IMP_SUBSE = $HOME/daten/subse`
  - `DW_DIR_SMS_PRG = ${HOME}/aktuell/allgemein/is/util`
  - `DW_DIR_SMS_ADR = ${HOME}/daten/sms/adressen`
  - `DW_DIR_SMS_TMP = ${HOME}/daten/sms/tmp`
  - `DW_DIR_IMP_DPPS = $HOME/daten/dpps`
  - `DW_DIR_IMP_PLANF2 = $HOME/daten/planf2`
  - `DW_HOST_CUSTOMER = dxcst3.bn.detemobil.de`
  - `ORACLE_HOME = /appl/local/oracle/12.2.0.1.0` or `/appl/local/oracle/11.2.0`
  - `DW_DIR_UTL_FILE = /appl/local/oracle/admin/$ORACLE_SID/utl_file`
- Environment files sourced:
  - `. $HOME/.dw_global`
  - `. $HOME/.dw_lokal`
- Invokes:
  - `. $HOME/.dw_global`
  - `. $HOME/.dw_lokal`
- Called by: unknown (sourced context script)
- Exit-code behaviour: No explicit exits; errors in setting ORACLE_HOME are output to stdout.
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion.

### Job dependencies
* **Upstream dependencies:**
  * Shared Files — `TMD_processing/ALL_TYPES/mp` (already migrated and merged via PR: https://github.com/gurunathan-prodapt/pi-agents/pull/883) — provides the Ab Initio graph components.
  * Shared Files — `TMD_processing/ALL_TYPES/run` (already migrated and merged via PR: https://github.com/gurunathan-prodapt/pi-agents/pull/884) — provides the orchestration wrapper.
  These upstream modules must exist on the target platform (Cloud Composer/GCS) and be imported or called in the final orchestration flow.

### Execution order
The legacy execution sequence consists of the following steps:
1. `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` (UC4 orchestration)
2. `isall/abinitio/cfg/all_types/all_types_graph.cfg` (Configuration setup)
3. `isall/aufbereitung/bin/r_all_types_master.ksh` (Master wrapper script)
4. `.dw_init` (Shell environment initialization profile)
5. `isall/aufbereitung/awk/k_all_types_transform.awk` (AWK data transform)
6. `isall/aufbereitung/sql/d_all_types.sql` (Oracle SQL operation)

**Target Mapping:**
* `.dw_init` (Step 4) acts purely as an environment profile setting up path variables and configurations for the shell runtime. In the target Cloud Composer (Airflow) DAG, these paths and configurations are managed natively as global or job-specific Airflow Variables/GCP environment variables, meaning `.dw_init` does not require a separate executable task in the final DAG run sequence.

### Lineage
* **Upstream configurations:**
  * `.dw_init` references and USES_CONFIG from `.DW_GLOBAL` and `.DW_LOKAL` (both resolved as not needing migration since their variables are managed as Airflow configurations).

### Cross-file dependencies
* **Configuration sharing:**
  * `.dw_init` sources `.DW_GLOBAL` and `.DW_LOKAL` during execution to establish system-wide defaults.
  * The directory structures exported in `.dw_init` are referenced downstream by the master wrapper `r_all_types_master.ksh` and Oracle operations.

### Target file plan
* There are no target files to generate for `.dw_init` as its procedural logic is empty, and its environment profile functions are retired in favor of native GCP/Airflow configuration management.

### Environment-specific values
Every path variable and external reference declared in the shell profile has been classified according to its target role on Google Cloud:

1. **GLOBAL** (Environment-wide infrastructure configs):
   * `DW_HOST_CUSTOMER` (Value: `dxcst3.bn.detemobil.de`): Normalized to GCP external connection configuration, retrieved dynamically from Airflow Connections or `Variable.get("DW_HOST_CUSTOMER")`.
   * `ORACLE_HOME` (Values: `/appl/local/oracle/12.2.0.1.0` or `/appl/local/oracle/11.2.0`): Retired as the target is BigQuery.
   * `DW_DIR_UTL_FILE` (Value: `/appl/local/oracle/admin/$ORACLE_SID/utl_file`): Retired.
   * Path Variables (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, and all 40+ `DW_DIR_IMP_*` / `DW_DIR_EXP_*` directory paths pointing to `$HOME/daten/...`): These represent environment-specific local directory mappings. On the target platform, these must be mapped to subdirectories within a shared Google Cloud Storage bucket (`GCS_BUCKET`). They should be retrieved at runtime via global Airflow Variables using `Variable.get("GCS_BUCKET")` and reconstructed as GCS URI schemes (`gs://{GCS_BUCKET}/...`).

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/data/source/all_types_linked_job/.dw_init` | `Retired` | Pure shell profile setting paths and Oracle database home locations. No executable business logic. Configs are replaced by native Airflow Variables and GCP environment configurations. |

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
The AWK script processes semicolon-separated records and performs a field count validation (`NF == 12`). If any record fails this validation, it outputs an error message and terminates immediately with a process-level exit code of `2` (`exit 2`). This use of a non-zero exit code is designed to signal a failure to an orchestrating shell or job runner (such as the `ALL_TYPES_MASTER` job). Standard BigQuery SQL cannot conditionally abort a query execution mid-stream to yield a specific shell-level exit status (like `2`) upon encountering a malformed row. Therefore, migrating this logic requires a procedural host environment like Python (e.g., using a Cloud Function, Cloud Run, or a Dataflow pipeline) to parse the file, validate fields, and exit with status code `2` if validation fails. 
Conversion Confidence: High.

FEATURE INVENTORY
- `BEGIN` block: Expressible in BQSQL. Field and output delimiters (`FS`, `OFS`) can be defined via external table configurations (e.g., CSV options).
- Pattern-action rules (main block): Expressible in BQSQL. Row-by-row logical evaluation can be mapped to SQL expressions.
- `FS` / `OFS`: Expressible in BQSQL. Handled at the schema definition level or via string functions.
- `NF`: Not directly expressible in BQSQL. BQSQL assumes structured schemas where column counts are pre-defined, though checking for nulls or splitting raw text strings is possible.
- `$0`: Expressible in BQSQL. Represents the entire raw input line, which can be queried if the source is loaded as a single string.
- `print`: Expressible in BQSQL. Replaced by a `SELECT` statement and string concatenation (e.g., `CONCAT('D;', raw_record)`).
- `conditions` (`if/else`): Expressible in BQSQL. Can be modeled using `CASE WHEN` conditional statements.
- `exit` (specifically `exit 2`): Not expressible in BQSQL. BigQuery cannot fail a query and return a specific, custom non-zero process exit code to the shell orchestration layer upon detecting a bad row.
- `END` block: Expressible in BQSQL. It is empty in this script, which maps to no-op or final query termination.

An explainable Design Document followed by numbered Python-oriented pseudocode is presented below.

---

### 1. MIGRATION DECISION SUMMARY

*   **Target Environment:** Python 3 (procedural script execution).
*   **BigQuery SQL Exclusion Rationale:** BigQuery SQL was ruled out because of the strict operational control flow required by the source program. Specifically, upon encountering any record with an invalid field count (`NF != 12`), the script must print an error message and immediately terminate the entire execution stream with a specific process exit code of `2` (`exit 2`). This non-zero exit code is critical to communicate failure back to the orchestrating parent job (`ALL_TYPES_MASTER`). Stateless BigQuery SQL transformations cannot abort mid-stream or produce explicit process-level operating system exit codes to signal orchestration control flow.
*   **Conversion Confidence:** High (100%). The logic is simple, structured, and easily replicated in a procedural Python script.
*   **Human Review Required:** Minimal. Review is only required to verify integration within the orchestration shell script (replacing the `awk` command with the Python execution).

---

### 2. PROGRAM OVERVIEW

*   **Purpose:** Post-processes the `ALL_TYPES` export file. It prefixes every valid data record with a row-type indicator (`D;`) and validates that each record contains exactly 12 fields. If a record is malformed, it halts the program and exits with an error code.
*   **Input Streams:** Standard input (`sys.stdin`) or files specified as command-line arguments.
*   **Expected Record Format:** Semicolon-separated values (delimiter `;`). Each valid row must contain exactly 12 fields.
*   **Output Streams:** 
    *   **Stdout:** Prefixed records (`D;<original_record>`) and error messages.
    *   **Stderr:** Not used by the original AWK script (the error message is written to `stdout` via standard `print`).
*   **Observable Side Effects:** Process termination with exit code `2` upon validation failure.

---

### 3. AWK FEATURE INVENTORY

*   `BEGIN` block: Sets the input field separator (`FS = ";"`) and output field separator (`OFS = ";"`). In Python, this is mapped to parsing strings using `.split(';')`.
*   `NF` (Number of Fields): Used to validate field count. In Python, this is mapped to checking `len(line.split(';'))`.
*   `$0` (Entire Record): Represents the raw input line. In Python, this is mapped to the read line string (with line endings preserved or handled appropriately).
*   `print`: In the main block, prefixes the record with `"D;"` and prints. In Python, this maps to printing `"D;" + raw_line`.
*   `if-else` condition: Used for logical validation. In Python, this is a standard `if-else` block.
*   `exit 2`: Halts execution and sets exit status to 2. In Python, this is mapped to `sys.exit(2)`.
*   `END` block: Empty block. No mapping required in Python.

---

### 4. PYTHON IMPLEMENTATION STRATEGY

*   **Libraries:** `sys` (for I/O and process exit handling). No other external libraries are needed.
*   **Streaming Loop:** Read lines from `sys.stdin` or specified files dynamically using standard iteration over file streams to maintain a low memory footprint.
*   **Field Splitting and Counting:** 
    *   To accurately mimic AWK's semicolon splitting, each line has its trailing newline character removed (e.g. `\n` or `\r\n`) and is split using `.split(';')`.
    *   The split count is checked against `12`.
*   **Preserving `$0`:** The original line content (retaining exact spacing and characters) is printed with `"D;"` prepended to it.
*   **Error Reporting:** The error message `"Error: Incorrect nos of Fields "` is printed to stdout to match the AWK script's precise behavior.

---

### 5. INPUTS, OUTPUTS, AND DEPENDENCIES

*   **Inputs:** Raw data records via standard input (`stdin`) or positional filename argument.
*   **Outputs:** Prefixed data or validation error text to standard output (`stdout`).
*   **Upstream Dependencies:** The output generation of the `ALL_TYPES` export script.
*   **Downstream Dependencies:** The subsequent execution step within the `ALL_TYPES_MASTER` orchestration chain.

---

### 6. UNSUPPORTED FEATURES, WARNINGS, AND ASSUMPTIONS

*   `# REVIEW:` **Standard Output for Error:** The original AWK script prints its error message `"Error: Incorrect nos of Fields "` to `stdout` instead of `stderr`. The Python implementation mimics this exactly to preserve behavioral compatibility. If downstream consumers expect only valid records on `stdout`, this should be redirected to `sys.stderr` instead.
*   `# REVIEW:` **Trailing Semicolons:** Note that standard AWK split behavior on a line ending in a semicolon (e.g. `a;b;...;`) will count the empty string after the trailing semicolon as a field. Standard Python `.split(';')` behaves identically (e.g. `'a;b;'.split(';')` returns `['a', 'b', '']`).

---

### 7. MANUAL REVIEW ITEMS

1.  **Orchestrator Integration:** Ensure that the wrapper script of `ALL_TYPES_MASTER` invokes the Python script and correctly captures the exit code `2` to halt downstream processing.
2.  **Output Destination:** Verify if the validation error message should remain on standard output or be redirected to standard error (`sys.stderr`).

---

### 8. NUMBERED PSEUDOCODE

```python
# 1. Import necessary system execution modules
import sys

# 2. Define the main execution routine
def main():
    # 3. Process records line-by-line from standard input (streaming mode)
    for line in sys.stdin:
        
        # 4. Strip the trailing newline characters for processing
        # Keep track of the original raw line ending (either \r\n or \n) to preserve it
        if line.endswith("\r\n"):
            line_ending = "\r\n"
            stripped_line = line[:-2]
        elif line.endswith("\n"):
            line_ending = "\n"
            stripped_line = line[:-1]
        else:
            line_ending = ""
            stripped_line = line

        # 5. Split the record by the field separator ';' to compute NF (Number of Fields)
        fields = stripped_line.split(";")
        num_fields = len(fields)

        # 6. Validate if the count of fields is exactly 12
        if num_fields == 12:
            # 7. Print the prefixed indicator "D;" followed by the original raw line
            sys.stdout.write("D;" + stripped_line + line_ending)
        else:
            # 8. Print error message exactly as output by AWK
            sys.stdout.write("Error: Incorrect nos of Fields \n")
            # 9. Terminate process execution with status code 2
            sys.exit(2)

# 10. Execute script standard entrypoint
if __name__ == "__main__":
    main()
```

### Job dependencies
*   **Upstream Dependencies:**
    *   **Shared Files — TMD_processing/ALL_TYPES/mp:** Converted PySpark pipeline representing `TMD_processing/ALL_TYPES/mp/all_types_graph.mp`, already migrated and merged under PR [#883](https://github.com/gurunathan-prodapt/pi-agents/pull/883).
    *   **Shared Files — TMD_processing/ALL_TYPES/run:** Converted wrapper script representing `TMD_processing/ALL_TYPES/run/all_types_graph.ksh`, already migrated and merged under PR [#884](https://github.com/gurunathan-prodapt/pi-agents/pull/884).
    *   **Wiring on Target Platform:** Within the Cloud Composer (Airflow) DAG orchestrating `DW.DWH_ALL_TYPES_MASTER`, the task executing the migrated PySpark pipeline (from `all_types_graph.mp`) must successfully complete and produce its export file before triggering the task for this post-processing Python validation script.

### Execution order
The target Airflow DAG task ordering must preserve the execution sequence established in the legacy job:
1.  **Orchestration Initializer:** XML definition `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` maps to the parent Cloud Composer DAG.
2.  **Configuration Loader:** `isall/abinitio/cfg/all_types/all_types_graph.cfg` parameters map to Airflow `params` or runtime configuration variables.
3.  **Job Driver wrapper:** `isall/aufbereitung/bin/r_all_types_master.ksh` maps to the master orchestrating Python operator.
4.  **Environment Initialization:** `.dw_init` maps to the DAG environment initialization task.
5.  **Post-Processing & Validation (This File):** `isall/aufbereitung/awk/k_all_types_transform.awk` maps to the Python-based execution task `isall/aufbereitung/awk/k_all_types_transform.py`.
6.  **Database Loading / Operations:** `isall/aufbereitung/sql/d_all_types.sql` maps to a BigQuery executing task (e.g., `BigQueryInsertJobOperator`).

### Cross-file dependencies
*   **Input Data Source:** This script processes the output file generated by the upstream Ab Initio graph (`all_types_graph.mp`), which runs as a PySpark job in the target platform.
*   **Output Data Consumer:** The output file with prefixed records (`D;...`) and validated schemas is subsequently loaded into BigQuery by the target SQL script replacing `isall/aufbereitung/sql/d_all_types.sql`.

### Target file plan
*   **Target File Path:** `isall/aufbereitung/awk/k_all_types_transform.py`
    *   **Language:** Python 3
    *   **Source File:** `isall/aufbereitung/awk/k_all_types_transform.awk`

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/awk/k_all_types_transform.awk` | `isall/aufbereitung/awk/k_all_types_transform.py` | Converted to a standalone Python script to perform streaming line-by-line record validation and enforce a custom process exit status (`exit 2`) on schema failure, which cannot be handled inside native BigQuery SQL. |

---

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
REASON: The script references an AWK script via -f whose source is not supplied in the extraction, making its SQL-expressibility unconfirmable.

EVIDENCE
- Business logic found: KSH custom logic runs a SQLPlus script (`d_all_types.sql`) and then runs an AWK script (`k_all_types_transform.awk`) to transform an exported CSV file.
- AWK: `awk -f ${ALL_DIR_ROOT}/aufbereitung/awk/k_all_types_transform.awk` is invoked, but its source is not supplied in the extraction.
- SQL-expressible: No, because the AWK code is missing and it reads/writes physical flat files.
- Non-SQL side effects: Logging to a file (`all_types_master_[date].log`) and reading/writing physical CSV files.
- Against this verdict: None, since the AWK program is not supplied, we cannot verify if its operations could be mapped to SQL.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `r_all_types_master.ksh` serves as the post-processing orchestration wrapper for the `ALL_TYPES` showcase job. It is executed following an Ab Initio graph run to coordinate a database-level refresh in Oracle followed by flat-file formatting. It executes an Oracle SQL script via SQL*Plus and then uses an AWK script to post-process an exported CSV data file, logging all steps to a date-stamped log file.

### 2. INVOCATION CONTEXT
- **Caller / Trigger**: Invoked by a UC4 / Automic job scheduler (associated with `JobKennung="ALL_TYPES_MASTER"`). 
- **UC4 Native Includes**: None referenced in this extraction.
- **Environment Files Sourced**:
  - `. $HOME/.dw_init`
    # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values

### 3. PARAMETERS / INPUTS
- **`ALL_DIR_ROOT`** (Environment Variable)
  - Source: Sourced via `.dw_init` or inherited from the environment.
  - Used: Yes, to construct paths for SQL, AWK, data, and log files.
  - Python mapping: `os.environ.get("ALL_DIR_ROOT")`
- **`DW_ORAUSER`** (Environment Variable)
  - Source: Sourced via `.dw_init` or inherited from the environment.
  - Used: Yes, passed as connection credentials to SQL*Plus.
  - Python mapping: `os.environ.get("DW_ORAUSER")`
- **`HOME`** (Environment Variable)
  - Source: System environment.
  - Used: Yes, to locate the environment initialization script (`.dw_init`).
  - Python mapping: `os.environ.get("HOME")`
- **`ProgName`** (Local Variable)
  - Value: `"ALL_TYPES Showcase Rahmenskript"`
  - Status: Declared but unused — confirm before dropping in target script.
- **`ProgVersion`** (Local Variable)
  - Value: `"V1.0.0"`
  - Status: Declared but unused — confirm before dropping in target script.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **Command 1**: `sqlplus ${DW_ORAUSER} @${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql </dev/null >> $LogDatei 2>&1`
  - Purpose: Executes an Oracle SQL script (`d_all_types.sql`) to refresh target tables.
  - Native Python Call vs Subprocess: Provisionally left as a subprocess invocation.
    # REVIEW: target database platform not specified; DB-client library choice below is provisional
    # REVIEW-STRUCT: SQL file d_all_types.sql not supplied in this extraction; contents are unknown.
  - Resolvable Launcher: No, because the SQL file's contents are not supplied, and DB connection parameters are unconfirmed.
- **Command 2**: `awk -f ${ALL_DIR_ROOT}/aufbereitung/awk/k_all_types_transform.awk ${ALL_DIR_ROOT}/data/all_types_export.csv > ${ALL_DIR_ROOT}/data/all_types_export.out`
  - Purpose: Transforms the exported CSV file into a finalized output structure.
  - Native Python Call vs Subprocess: Provisionally left as a subprocess invocation of AWK.
    # REVIEW-STRUCT: AWK file k_all_types_transform.awk not supplied — behavior is unknown. Must preserve as external command or rewrite once the AWK logic is available.
  - Resolvable Launcher: No.

### 5. EMBEDDED SQL
- **Source file**: `${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql`
  # REVIEW-STRUCT: SQL file d_all_types.sql not supplied in this extraction; contents are unknown.
- **Statement Type**: Unknown (refers to "SQL-Refresh" in comments).
- **Dialect**: Oracle SQL*Plus is implied by the `sqlplus` launcher.

### 6. CONTROL FLOW
1. **Environment Setup**: Source `$HOME/.dw_init` to define required variables.
2. **Execution Options**: Set shell parameters `set -eu` to abort on any error.
3. **Variable Assignment**:
   - `JobKennung` is set to `"ALL_TYPES_MASTER"` (forced to uppercase).
   - `v_sysdate` is set to the current date formatted as `DDMMYYYY`.
   - `LogDatei` is resolved to `${ALL_DIR_ROOT}/protokoll/all_types_master_${v_sysdate}.log`.
4. **Logging Initiation**: Prints job execution details (JobKennung, LogDatei) to standard output.
5. **Step 1 (SQL Refresh)**: Executes the Oracle SQL script via `sqlplus`, redirecting all stdout and stderr to `LogDatei`. Input is redirected from `/dev/null` to prevent interactive prompt hangs.
6. **Step 2 (AWK Transformation)**: Runs the AWK script on the input file `${ALL_DIR_ROOT}/data/all_types_export.csv`, writing the results to `${ALL_DIR_ROOT}/data/all_types_export.out`. A status message is logged using `tee` to write to both stdout and `LogDatei`.
7. **Success Tracking**: Logs a completion message ("Die Abarbeitung wurde ohne erkennbare Fehler beendet") to `LogDatei` and stdout.
8. **Exit**: Exits with status `0`.

### 7. ERROR HANDLING & EXIT CODES
- **Detection**: Standard shell exit on error (`set -e`) means any failed step immediately terminates the script with a non-zero exit code.
- **Action**: Failure propagates the exit code of the failing step (`sqlplus` or `awk`).
- **Success Convention**: Clean exit with code `0`.
- **Python Mapping**: All `subprocess.run` calls must be executed with `check=True` inside a `try...except CalledProcessError` block to capture, log, and propagate failure exit codes.

### 8. OUTPUTS / SIDE EFFECTS
- **Log File**: `${ALL_DIR_ROOT}/protokoll/all_types_master_${v_sysdate}.log`
- **Output File**: `${ALL_DIR_ROOT}/data/all_types_export.out`
- **Database state**: Refreshed tables via `d_all_types.sql` (unconfirmed).

### 9. BUSINESS SUMMARY
- Coordinates the post-processing phase of the `ALL_TYPES` showcase process flow.
- Triggers a database-level tables refresh within Oracle.
- Performs column-level and record-level formatting transformations on the exported flat file.
- Produces a post-processed data output file for downstream consumption.
- Records end-to-end trace logs for auditing and operational debugging.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Import required libraries
import os
import sys
import subprocess
import datetime

def main():
    # Step 2: Initialize environment and check source variables
    # # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values
    # In a real migration context, the variables below would be inherited from the scheduler environment.
    
    # ProgName and ProgVersion are declared but unused in the original script
    prog_name = "ALL_TYPES Showcase Rahmenskript"
    prog_version = "V1.0.0"
    
    all_dir_root = os.environ.get("ALL_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    
    if not all_dir_root or not dw_orauser:
        print("CRITICAL: ALL_DIR_ROOT or DW_ORAUSER environment variables are not set.", file=sys.stderr)
        sys.exit(1)
        
    # Step 3: Define internal variables
    job_kennung = "ALL_TYPES_MASTER".upper()
    v_sysdate = datetime.datetime.now().strftime("%d%m%Y")
    
    log_datei = os.path.join(all_dir_root, "protokoll", f"all_types_master_{v_sysdate}.log")
    
    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_datei), exist_ok=True)
    
    # Step 4: Write header metadata to console
    print(" ----------------- Job -----------------------")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")
    
    try:
        # Step 5: Execute SQL Refresh Step
        # # REVIEW: target database platform not specified; DB-client library choice below is provisional
        # # REVIEW-STRUCT: SQL file d_all_types.sql not supplied in this extraction; contents are unknown.
        sql_msg = "----Starte SQL-Refresh----\n"
        print(sql_msg, end="")
        with open(log_datei, "a") as log_file:
            log_file.write(sql_msg)
            
        sql_script_path = os.path.join(all_dir_root, "aufbereitung", "sql", "d_all_types.sql")
        
        # Invoke sqlplus appending both stdout and stderr to the log file
        with open(log_datei, "a") as log_file:
            subprocess.run(
                ["sqlplus", dw_orauser, f"@{sql_script_path}"],
                input="",  # Equivalent to </dev/null
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=True
            )
            
        # Step 6: Execute AWK Transformation Step
        # # REVIEW-STRUCT: AWK file k_all_types_transform.awk not supplied — behavior is unknown. Must preserve as external command or rewrite once the AWK logic is available.
        awk_msg = "----Starte AWK-Nachbearbeitung----\n"
        print(awk_msg, end="")
        with open(log_datei, "a") as log_file:
            log_file.write(awk_msg)
            
        awk_script = os.path.join(all_dir_root, "aufbereitung", "awk", "k_all_types_transform.awk")
        csv_input = os.path.join(all_dir_root, "data", "all_types_export.csv")
        csv_output = os.path.join(all_dir_root, "data", "all_types_export.out")
        
        with open(csv_output, "w") as out_file:
            subprocess.run(
                ["awk", "-f", awk_script, csv_input],
                stdout=out_file,
                check=True
            )
            
        # Step 7: Log execution success
        success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet\n"
        print(success_msg, end="")
        with open(log_datei, "a") as log_file:
            log_file.write(success_msg)
            
        sys.exit(0)
        
    except subprocess.CalledProcessError as e:
        error_msg = f"ERROR: Step execution failed with exit code {e.returncode}\n"
        print(error_msg, file=sys.stderr)
        with open(log_datei, "a") as log_file:
            log_file.write(error_msg)
        sys.exit(e.returncode)

if __name__ == "__main__":
    main()
```

### Job Dependencies
- **Upstream Predecessors**:
  - **Shared Files — TMD_processing/ALL_TYPES/mp** (Ab Initio graph `all_types_graph.mp`): Already migrated and merged (PR #883). In the target BigQuery architecture, this graph is converted to a PySpark pipeline running on Dataproc Serverless.
  - **Shared Files — TMD_processing/ALL_TYPES/run** (Ab Initio shell wrapper `all_types_graph.ksh`): Already migrated and merged (PR #884).
- **Target Orchestration Wiring**:
  - The orchestration of this job sequence is managed via a Google Cloud Composer (Airflow) DAG.
  - The DAG triggers the Dataproc Serverless task for `all_types_graph.py` (migrated from the Ab Initio graph) as the first step.
  - Upon successful completion of the Dataproc task, the Airflow DAG triggers the execution of the migrated master wrapper script `isall/aufbereitung/bin/r_all_types_master.py`.

---

### Execution Order
The legacy job's execution sequence is preserved and mapped to the Cloud Composer DAG and target execution steps as follows:
1. **UC4 Scheduler Definition** (`DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml`) maps to the orchestrating Cloud Composer Airflow DAG.
2. **Ab Initio Configuration** (`isall/abinitio/cfg/all_types/all_types_graph.cfg`) is resolved through Airflow DAG params and environment variables.
3. **KornShell Master Script** (`isall/aufbereitung/bin/r_all_types_master.ksh`) maps to the target Python script `isall/aufbereitung/bin/r_all_types_master.py`.
4. **Environment Initialization** (`.dw_init`) is replaced by Airflow environment configuration and runtime GCP variables.
5. **Database Refresh Step** (`isall/aufbereitung/sql/d_all_types.sql`): Initiated within the Python master script by reading and executing the migrated BigQuery SQL file using the `google.cloud.bigquery` client.
6. **Data Post-Processing Step** (`isall/aufbereitung/awk/k_all_types_transform.awk`): Executed as a subprocess calling the migrated Python file `isall/aufbereitung/awk/k_all_types_transform.py` using `python3`.

---

### Lineage
- **Upstream Dependencies (from Lineage Edges)**:
  - Consumes environment setup from `.dw_init`.
  - Reads and runs the SQL script `isall/aufbereitung/sql/d_all_types.sql` against the database.
  - Processes the exported flat-file dataset `${ALL_DIR_ROOT}/data/all_types_export.csv` (which is produced by the upstream Ab Initio graph run).
- **Downstream Output (from Lineage Edges)**:
  - Invokes the transformation program `isall/aufbereitung/awk/k_all_types_transform.awk` to generate the finalized export file `${ALL_DIR_ROOT}/data/all_types_export.out` for downstream ingestion.

---

### Cross-File Dependencies
- **Data Call Chain**: 
  - The upstream PySpark job (migrated from Ab Initio) extracts and writes data to a GCS bucket at `${ALL_DIR_ROOT}/data/all_types_export.csv`.
  - The master script `r_all_types_master.py` first calls BigQuery to execute `d_all_types.sql` (migrating from Oracle SQL*Plus to native BigQuery SQL) to refresh tables like `CDS$TA_ALL_TYPES_RAW` and `SOF$TA_ALL_TYPES`.
  - The master script then executes `k_all_types_transform.py` (migrated from AWK) to transform the CSV file into the target file `${ALL_DIR_ROOT}/data/all_types_export.out`.

---

### Target File Plan
- **Target File**: `isall/aufbereitung/bin/r_all_types_master.py`
  - **Language**: Python (`python3`)
  - **Source File**: `isall/aufbereitung/bin/r_all_types_master.ksh`
  - **Implementation Specification**:
    - Uses the `google.cloud.bigquery` API client to load and execute the queries defined in the migrated BigQuery SQL file `isall/aufbereitung/sql/d_all_types.sql`. This avoids external subprocess calls to legacy database clients (like `sqlplus`).
    - Uses `subprocess.run` to call the migrated AWK-replacement Python script `python3 isall/aufbereitung/awk/k_all_types_transform.py`.
    - Retains all original output and logging messages in German verbatim as required by the Output/Print Literal Rule:
      - `" ----------------- Job -----------------------"`
      - `" JobKennung: '{job_kennung}'"`
      - `" Logdatei  : '{log_datei}'"`
      - `" ---------------------------------------------"`
      - `"----Starte SQL-Refresh----"`
      - `"----Starte AWK-Nachbearbeitung----"`
      - `"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`

---

### Environment-Specific Values

1. **GLOBAL (Environment-Wide)**:
  - `HOME` $\rightarrow$ Map to `GCS_BUCKET` or local scratch space depending on execution environment. Sourced via `os.environ.get("HOME")`.
  - `ALL_DIR_ROOT` $\rightarrow$ Map to `GCS_BUCKET` (e.g., `gs://<your-bucket-name>/isall`). Sourced via `os.environ.get("ALL_DIR_ROOT")` or Composer variable.
  - `GCP_PROJECT` $\rightarrow$ Native GCP project ID where BigQuery is running. Sourced via `os.environ.get("GCP_PROJECT")` or BigQuery client defaults.
  - `BQ_DATASET` $\rightarrow$ Native BigQuery dataset replacing the Oracle schema prefix.

2. **JOB-SPECIFIC**:
  - `JobKennung` $\rightarrow$ Hardcoded inline within the script config as `"ALL_TYPES_MASTER"`.
  - `LogDatei` $\rightarrow$ Constructed dynamically at runtime using `os.path.join(ALL_DIR_ROOT, "protokoll", f"all_types_master_{v_sysdate}.log")`.

---

### Risks & Manual Actions
- **Airflow Environment Setup**: The environment variables (such as `ALL_DIR_ROOT` pointing to a local mount or GCS mount) must be populated in the Airflow environment configuration so they are correctly inherited by the Python script at runtime.
- **BigQuery IAM Roles**: The Service Account executing the Composer DAG and Python scripts must have the `roles/bigquery.jobUser` and `roles/bigquery.dataEditor` roles for the target dataset and tables.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/bin/r_all_types_master.ksh` | `isall/aufbereitung/bin/r_all_types_master.py` | Migrated to Python to orchestrate the BigQuery SQL execution and AWK Python post-processing on Google Cloud. |

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
    - Multi-statement SQL script with client-side SQL*Plus directives (e.g., `WHENEVER SQLERROR`, `prompt`).
1.2 Summarize the business logic and purpose of the script:
    - This script performs a full refresh of the `sof$ta_all_types` intermediate table. It first truncates the existing data in `sof$ta_all_types` (ignoring errors if it fails), then inserts active staging records from the raw source table `cds$ta_all_types_raw` where `status` is `'READY'`. The load timestamp is updated to the current system date/time.
1.3 List all entities referenced:
    - Tables:
        - `sof$ta_all_types` (Target staging table)
            - `all_types_id` (Inferred as `INT64` or `STRING`)
            - `source_system` (Inferred as `STRING`)
            - `processed_at` (Inferred as `DATETIME`)
        - `cds$ta_all_types_raw` (Source raw table, alias `r`)
            - `all_types_id` (Inferred as `INT64` or `STRING`)
            - `source_system` (Inferred as `STRING`)
            - `status` (Inferred as `STRING`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (loaded from `SYSDATE`) → Resolved to `DATETIME` (retains date and time component).
    - Oracle `VARCHAR2`/`CHAR` (assumed for table names, statuses, source systems) → `STRING`.

2.2 Implicit and Explicit Type Casting:
    - Direct assignment of current timestamp to a date column is safe; however, explicitly matching BQ type targets is resolved by using `CURRENT_DATETIME()`.

2.3 NULL Handling and Conditional Functions:
    - None used in source.

2.4 String Functions:
    - None used in source.

2.5 Date and Timestamp Functions:
    - `SYSDATE` → `CURRENT_DATETIME()` (Resolved to `DATETIME`).
    - Explicitly chosen to match Oracle's timezone-naive dynamic date/time tracking.

2.6 - 2.12 Numeric, Analytical, Joins, Sequences, and DML:
    - `TRUNCATE TABLE` → Directly supported in BigQuery.
    - `INSERT INTO ... SELECT` → Directly supported in BigQuery.
    - `COMMIT` → Handled implicitly by BigQuery’s auto-commit transactional model, or within explicit transaction boundaries if grouped in a scripting block.

2.13 DDL Constructs:
    - None present.

2.14 PL/SQL and Scripting:
    - SQL*Plus client commands:
        - `WHENEVER SQLERROR CONTINUE` → Resolved in BQ Scripting by wrapping the specific block in a `BEGIN ... EXCEPTION ... END` block that catches and ignores the error.
        - `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` → Resolved by default BQ scripting behavior (where unhandled block exceptions halt execution) or explicit nested transactions.
        - `prompt` → Resolved to simple `SELECT '...' AS log_message;` statements for audit/logging, or handled by the execution runner.

2.15 Unresolvable or Advisory Items:
    - None.

Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    - Deployed as a multi-statement BigQuery Scripting block (`BEGIN ... END`).
    - The truncation step is placed in a nested `BEGIN ... EXCEPTION` block to emulate the `WHENEVER SQLERROR CONTINUE` directive.
    - The main data insertion step is placed in a structured scripting transaction to ensure data atomicity, emulating the default transactional control.
3.2 Assumptions:
    - Target and raw tables are structured correctly in BigQuery with matching data types.
3.3 Flagged Items:
    - None.

2.16 MIGRATION DECISION MATRIX

| Oracle Statement / Construct | Selected BigQuery Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| `TRUNCATE TABLE` | Direct BQ SQL (`TRUNCATE TABLE`) | Python wrapper, DELETE FROM | `TRUNCATE TABLE` is fully supported natively in BQ and is high performance. |
| `SYSDATE` | Direct BQ SQL (`CURRENT_DATETIME()`) | `CURRENT_TIMESTAMP()` | `DATETIME` matches Oracle's timezone-neutral `DATE` behavior. |
| `WHENEVER SQLERROR CONTINUE` | BQ Scripting (`BEGIN ... EXCEPTION`) | External Python handler | In-SQL exception routing is cleaner and keeps the logic fully inside BigQuery SQL. |
| `prompt` | BQ SQL `SELECT 'message' AS log` | Python logger | Standard SQL log emission provides simple visual execution logs. |

2.17 REQUIRED ARTIFACTS

| Generated Artifact | Tech Stack | Input/Output/Invoker Contract |
| :--- | :--- | :--- |
| **BigQuery Scripting SQL File** | BigQuery Standard SQL | Runs sequentially via BQ API/Console. Performs Truncate-Load. |

2.18 DATA TYPE COMPATIBILITY TABLE

| Source (Oracle) Data Type | Target (BigQuery) Data Type | Conversion Rule / Logic | Warnings / Comments |
| :--- | :--- | :--- | :--- |
| `DATE` | `DATETIME` | Map to timezone-neutral date and time tracking. | Captures exact date and time. |
| `VARCHAR2` | `STRING` | Direct mapping. | No size constraints needed. |
| `NUMBER` (Assumed ID) | `INT64` | Direct mapping for integer keys. | - |

2.19 DESIGN REVIEW SUMMARY

- **Patterns/Objects Found**: Truncate-and-load pipeline, SQL*Plus control execution logic.
- **Unsupported Functions**: None.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Tables `sof$ta_all_types`, `cds$ta_all_types_raw`.
- **Warnings**: Ensure that the dataset references are correctly parameterized or hardcoded according to target environment schema setup.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `SYSDATE` | Direct-with-rewrite | `CURRENT_DATETIME()` |
| `TRUNCATE TABLE` | Direct | `TRUNCATE TABLE` |
| `WHENEVER SQLERROR` | Direct-with-rewrite | Nested `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` |
| `PROMPT` | Direct-with-rewrite | `SELECT 'message' AS log_message;` |


═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Emulate SQL*Plus prompt
SELECT 'tabelle von vorherigem lauf loeschen' AS log_message;

-- Emulate "WHENEVER SQLERROR CONTINUE" for the truncate table statement
BEGIN
  -- Execute TRUNCATE. If table doesn't exist or query fails, execution proceeds.
  TRUNCATE TABLE sof$ta_all_types;
EXCEPTION WHEN ERROR THEN
  -- Catch error and continue execution (emulating CONTINUE directive)
  SELECT 'Truncate failed or table not found; continuing execution.' AS log_message;
END;

-- Emulate standard error-exit behavior for the remaining script steps
BEGIN
  -- Emulate SQL*Plus prompt
  SELECT 'zieltabelle befuellen' AS log_message;

  -- Insert logic mapping Oracle types to BigQuery equivalent
  INSERT INTO sof$ta_all_types (
    all_types_id,
    source_system,
    processed_at
  )
  SELECT
    r.all_types_id,
    r.source_system,
    CURRENT_DATETIME()  -- converted from SYSDATE
  FROM
    cds$ta_all_types_raw AS r
  WHERE
    r.status = 'READY';

  -- Emulate SQL*Plus prompt
  SELECT 'Verarbeitung fehlerfrei beendet.' AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Raise error to enforce failure exit
  RAISE USING message = "Process failed during INSERT operation.";
END;
```

### FLAGGED ITEMS FOR HUMAN REVIEW
- **Schema Mapping**: Ensure that the schema of target table `sof$ta_all_types` and source table `cds$ta_all_types_raw` have been pre-created in BigQuery. If standard BQ project/dataset prefixes are required (e.g., `project.dataset.sof$ta_all_types`), update the table paths accordingly.

### Job dependencies
* **Upstream Job Dependencies**:
  * `Shared Files — TMD_processing/ALL_TYPES/mp`: This component (the Ab Initio graph) has already been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/883).
  * `Shared Files — TMD_processing/ALL_TYPES/run`: This component (the KSH runner script) has already been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/884).
* **Target Platform Wiring**:
  * In the target Cloud Composer environment, the BigQuery SQL script (`d_all_types.sql`) must be wired to execute immediately following the upstream PySpark Dataproc Serverless job (which replaces the Ab Initio graph and KSH runner). This is achieved via a downstream trigger using a `BigQueryInsertJobOperator` inside the unified Airflow DAG.

### Execution order
The target Airflow orchestration DAG must preserve the execution sequence defined in the legacy dependency graph:
1. `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` $\rightarrow$ Represents the starting DAG execution trigger in Airflow.
2. `isall/abinitio/cfg/all_types/all_types_graph.cfg` $\rightarrow$ Read and parsed at the beginning of the DAG execution to initialize environment and job parameters.
3. `isall/aufbereitung/bin/r_all_types_master.ksh` $\rightarrow$ Executed via a Python Operator to run initialization and coordinate execution steps.
4. `.dw_init` $\rightarrow$ Handled during the Airflow task execution initialization.
5. `isall/aufbereitung/awk/k_all_types_transform.awk` $\rightarrow$ Executed as a data-transformation task prior to database loading.
6. `isall/aufbereitung/sql/d_all_types.sql` $\rightarrow$ Executed as the final step of the DAG using a `BigQueryInsertJobOperator` to perform the Truncate-and-Load operation.

### Lineage
* **Upstream Producer**:
  * Reads from table `CDS$TA_ALL_TYPES_RAW` (lineage confidence: 0.80). This table is written by the upstream transformation step (`isall/aufbereitung/awk/k_all_types_transform.awk`).
* **Downstream Consumer**:
  * Writes to table `SOF$TA_ALL_TYPES` (lineage confidence: 0.80). This table is a staging/intermediate target refreshed by this script.

### Cross-file dependencies
* **Shared Tables**: 
  * Table `CDS$TA_ALL_TYPES_RAW` acts as a common data transfer point between the upstream AWK transformation and this SQL loading script.
  * Table `SOF$TA_ALL_TYPES` acts as the primary data target for this execution step.
* **Shared Parameters**: 
  * The execution is dependent on parameters originally sourced from `isall/abinitio/cfg/all_types/all_types_graph.cfg` to resolve directories and process details.

### Target file plan
* **Target File**: `isall/aufbereitung/sql/d_all_types.sql`
  * **Language**: BigQuery SQL (Scripting)
  * **Source File**: `isall/aufbereitung/sql/d_all_types.sql`

### Environment-specific values
These values identify project-level and dataset-level configurations in the Google Cloud Platform (GCP) target environment. They must be resolved dynamically rather than being hardcoded.

1. **GLOBAL (Environment-Wide)**:
   * `GCP_PROJECT`: Identifies the target Google Cloud Project ID. 
     * *Source Method (Airflow DAG)*: `Variable.get("GCP_PROJECT")`
     * *Source Method (SQL)*: Referenced via query parameters `@gcp_project` or dynamically templated in the `BigQueryInsertJobOperator`.
   * `BQ_DATASET`: Identifies the specific BigQuery dataset containing the raw and target tables.
     * *Source Method (Airflow DAG)*: `Variable.get("BQ_DATASET")`
     * *Source Method (SQL)*: Referenced via query parameters `@bq_dataset` or dynamically templated in the `BigQueryInsertJobOperator`.

2. **JOB-SPECIFIC**:
   * *None identified.* All variables in this script are structural dataset/table mappings which fall under the Global environment-wide category.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/sql/d_all_types.sql` | `isall/aufbereitung/sql/d_all_types.sql` | Mirrored target BigQuery SQL script performing the TRUNCATE and INSERT sequence natively in BigQuery. |