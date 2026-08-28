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


# UC4 WORKFLOW MIGRATION DESIGN DOCUMENT
**Source System:** UC4 / Automic Object Export
**Target Environment:** Apache Airflow on GCP (Cloud Composer)

---

## 1. Overview
The extracted bundle contains a single, standalone UC4 Unix job (`DW.DWH_ALL_TYPES_MASTER`) designed to orchestrate a data processing chain. This job wraps an Ab Initio graph execution (`all_types_graph`) via a native wrapper script and sequentially invokes a custom KornShell (KSH) processing script. UNCERTAIN: Because this extraction does not include an enclosing JOBP (Workflow/Jobplan) container or schedule definitions, this task is assumed to be an isolated component triggered externally or run as part of a wider ecosystem not visible in this extraction.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ALL_TYPES_MASTER` | JOBS_UNIX | 1 (Active) | Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain |

---

## 3. Scheduling
- **Trigger Source:** No schedule (`EVNT_TIME`), workflow container (`JOBP`), or trigger script (`SCRI`) is present in this extraction bundle. This object is marked as **externally triggered** (source unknown from this extraction alone).
- **DAG Schedule Property:** `schedule=None` (No cron expression is inferred or invented).

---

## 4. Airflow DAG Properties
Since this is a standalone `JOBS_UNIX` object, it is wrapped in its own dedicated Airflow DAG to allow independent execution, scheduling, and monitoring.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_all_types_master` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Derived from Active=1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `jobs_unix_dw_dwh_all_types_master` | `DW.DWH_ALL_TYPES_MASTER` | `DataprocSubmitJobOperator` | `all_types_graph.py` | `project_id`, `region`, `cluster_name` placeholders | 1 | 5 mins | None | None | False | None | `# REVIEW-STRUCT:` Launcher classified as `abinitio_graph`. The script also contains a secondary shell script call (`r_all_types_master.ksh`) that must be evaluated manually. |

---

## 6. Task Dependency Map
As this DAG contains only a single task wrapping the standalone Unix job, there is no multi-task execution chain.

```
jobs_unix_dw_dwh_all_types_master
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock patterns were present on this object. 
- **Recommendation:** Standard single-concurrency default (`max_active_runs=1`) applied to the DAG to prevent overlapping executions if triggered manually in rapid succession.

---

## 8. Error Handling and Retry Strategy
- **Retries:** Inherits the standard default of 1 retry with a 5-minute delay.
- **Failures:** Standard task-level failure reporting. No `on_failure_callback` has been specified in the source extraction.
- **Trigger Rules:** Default standard `all_success` behavior.

---

## 9. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'ALL_TYPES_MASTER'` | Airflow Task Env Variable / Spark Argument (if required) |
| N/A | Target DAG ID | `dw_dwh_all_types_master` |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Standalone Unix Job Migration:** The extraction consists solely of a single `JOBS_UNIX` object without a parent `JOBP` workflow. It has been encapsulated in a single-task DAG `dw_dwh_all_types_master`.
* **# REVIEW-STRUCT: Dual Execution Script Body:** While classified as `abinitio_graph` (which maps to a `DataprocSubmitJobOperator` targeting PySpark code translated from the Ab Initio graph `all_types_graph`), the script body also executes a manual KornShell script: `$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh`. The developer **must verify** whether this secondary shell script needs to be converted into its own subsequent Airflow task (e.g., `BashOperator` or `SSHOperator`), or if its logic was already consolidated into the PySpark conversion.
* **Placeholder Replacement Required:** The GCS path `gs://YOUR_BUCKET_NAME/pyspark_scripts/all_types_graph.py` and Dataproc cluster variables (`project_id`, `region`, `cluster_name`) are configured as placeholders and must be filled in with target environment details.

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# TODO: Replace placeholders with environment-specific variables
GCP_PROJECT_ID = "YOUR_PROJECT_ID"
GCP_REGION = "YOUR_REGION"
DATAPROC_CLUSTER = "YOUR_CLUSTER_NAME"
PYSPARK_SCRIPT_URI = "gs://YOUR_BUCKET_NAME/pyspark_scripts/all_types_graph.py"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_all_types_master",
    default_args=DEFAULT_ARGS,
    description="Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: jobs_unix_dw_dwh_all_types_master ──────────
    # # REVIEW-STRUCT: Maps launcher_type 'abinitio_graph' -> DataprocSubmitJobOperator.
    # Note: The original UC4 script also executed a shell utility manually:
    # '$HOME/aktuell/aufbereitung/bin/r_all_types_master.ksh'.
    # Ensure this secondary utility's logic is either consolidated into the PySpark script 
    # or appended as a separate downstream task (e.g., via BashOperator).
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": ["--job_kennung", "ALL_TYPES_MASTER"],
        },
    }

    jobs_unix_dw_dwh_all_types_master = DataprocSubmitJobOperator(
        task_id="jobs_unix_dw_dwh_all_types_master",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=pyspark_job_config,
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single standalone task execution
    jobs_unix_dw_dwh_all_types_master
```

# MIGRATION DESIGN DOCUMENT — SUPPLEMENTAL CONTEXT

### Job dependencies
- **Upstream Dependencies:**
  - `Shared Files — TMD_processing/ALL_TYPES/mp`: Already migrated and merged via PR #883. In the target environment, the Ab Initio graph maps to a Dataproc Serverless PySpark pipeline (`all_types_graph.py`) stored in GCS.
  - `Shared Files — TMD_processing/ALL_TYPES/run`: Already migrated and merged via PR #884. This legacy execution wrapper is replaced directly by the Cloud Composer DAG's orchestration.
- **Wiring on Target Platform:**
  - The migrated graph job is launched directly from the Airflow DAG via a `DataprocSubmitJobOperator` referencing the pre-migrated `all_types_graph.py` script.
  - There are no downstream cross-job dependencies listed in the dependency context for this specific job.

### Execution order
The Airflow DAG preserves the execution sequence defined in the legacy dependency graph by structuring the workflow into exactly two sequential tasks:
1. **Task 1: Ab Initio Graph Execution**
   - **Legacy Steps:** `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` (initiating the Ab Initio graph) and config resolution using `isall/abinitio/cfg/all_types/all_types_graph.cfg`.
   - **Target Mapping:** `DataprocSubmitJobOperator` executing the migrated PySpark script (`all_types_graph.py`).
2. **Task 2: Post-Processing Master Execution**
   - **Legacy Steps:** Invocation of `isall/aufbereitung/bin/r_all_types_master.ksh` (which sequentially executes `.dw_init`, `isall/aufbereitung/awk/k_all_types_transform.awk`, and `isall/aufbereitung/sql/d_all_types.sql`).
   - **Target Mapping:** `BashOperator` executing the migrated Python wrapper script `/isall/aufbereitung/bin/r_all_types_master.py`. This preserves the original sequential execution and logging behavior of the wrapper and internally coordinates the AWK and BigQuery SQL operations as required by previous reviewer feedback.

- **Task Sequence:** `DataprocSubmitJobOperator` >> `BashOperator`

### Lineage
- **Upstream Lineage:**
  - The job `DWH_ALL_TYPES_MASTER.xml` invokes the legacy utility `R_AI_START.KSH` (which has a confirmed human resolution of "NO SOURCE NEEDED / Not Needed" and is omitted from the target).
  - The job invokes the post-processing wrapper script `isall/aufbereitung/bin/r_all_types_master.ksh` (mapped to the migrated Python script `/isall/aufbereitung/bin/r_all_types_master.py`).
  - The job runs on the host `dwhall1p` and references the package `DW.UNIX.ISALL`. In the target cloud platform, this translates to executing within Cloud Composer's GKE-backed worker environments.

### Cross-file dependencies
- **Database Tables:**
  - `TABLE:CDS$TA_ALL_TYPES_RAW` (Raw source table).
  - `TABLE:SOF$TA_ALL_TYPES` (Target table populated by the processing pipeline).
- **Orchestration Bindings:**
  - The DAG reads graph configurations from the migrated python equivalent of the legacy `all_types_graph.cfg` to set execution parameters for the PySpark task.

### Target file plan
- **Target File Path:** `dags/dw_dwh_all_types_master.py`
  - **Language:** Python (Apache Airflow DAG)
  - **Source File:** `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml`
  - **Description:** An Airflow DAG configured with exactly two sequential tasks: a `DataprocSubmitJobOperator` to submit the migrated PySpark graph, followed by a `BashOperator` to execute the migrated Python wrapper script `/isall/aufbereitung/bin/r_all_types_master.py`. Separate tasks or empty stubs for the AWK and SQL files are excluded, as their execution is orchestrated internally by the master Python wrapper.

### Environment-specific values

1. **GLOBAL (Environment-wide)**
   - `GCP_PROJECT`: The GCP project ID for BigQuery and Dataproc resources. Sourced at runtime via `Variable.get("GCP_PROJECT")` within the Airflow DAG.
   - `GCP_REGION`: The GCP execution region. Sourced via `Variable.get("GCP_REGION")`.
   - `DATAPROC_CLUSTER`: The name of the Dataproc cluster or Serverless batch template. Sourced via `Variable.get("DATAPROC_CLUSTER")`.
   - `GCS_BUCKET`: The GCS bucket containing the migrated PySpark code. Sourced via `Variable.get("GCS_BUCKET")`.

2. **JOB-SPECIFIC**
   - `DWH_JOB_KENNUNG`: Inherited from the legacy UC4 script variable `:set &DWH_JOB_KENNUNG='ALL_TYPES_MASTER'`. Value: `'ALL_TYPES_MASTER'`. Sourced inside the DAG script as a local constant and passed as a job parameter to the Dataproc operator.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` | `dags/dw_dwh_all_types_master.py` | Migrates the legacy UC4 orchestration job into an Airflow DAG. Following reviewer feedback, it defines exactly two tasks: `DataprocSubmitJobOperator` for the graph, and a `BashOperator` executing the migrated wrapper script `r_all_types_master.py`. |

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
REASON: The script is an environment initialization file that only defines directory paths, sets environment variables, and sources other configurations.

EVIDENCE
- Business logic found: None. The script only sets environment variables, defines local directory paths, and conditionally sets ORACLE_HOME.
- AWK: none
- SQL-expressible: No, this is configuration and environment setup, not data transformation.
- Non-SQL side effects: none observed
- Against this verdict: The presence of an `if` block for checking the directory existence of `/appl/local/oracle/12.2.0.1.0` and `/appl/local/oracle/11.2.0` technically violates the "no if/while/for/case block" rule for pure wrappers, but since this script performs zero business or processing logic, converting it to Python or SQL is unnecessary.

ORCHESTRATION SUMMARY
- Purpose: This script (`.dw_init`) initializes environment variables and directories for the Information Services / Data Warehouse (DWH) environment, as well as locating ORACLE_HOME and sourcing global/local profile scripts.
- Variables declared:
  - `DW_DIR_ROOT` = `$HOME/aktuell`
  - `DW_DIR_PROT` = `$HOME/daten/logfiles`
  - `DW_DIR_CUBES` = `$HOME/daten/cubes`
  - `DW_DIR_IMP_D1` = `$HOME/daten/d1`
  - `DW_DIR_IMP_BWA` = `$HOME/daten/dpps/bwa`
  - `DW_DIR_IMP_XTRA` = `$HOME/daten/xtra`
  - `DW_DIR_IMP_CTEL` = `$HOME/daten/ctel`
  - `DW_DIR_IMP_VO` = `$HOME/daten/vo`
  - `DW_DIR_IMP_RV` = `$HOME/daten/rv`
  - `DW_DIR_IMP_IF` = `$HOME/daten/ees`
  - `DW_DIR_IMP_NNV` = `$HOME/daten/nnv`
  - `DW_DIR_IMP_SIGMA` = `$HOME/daten/gd/sigma`
  - `DW_DIR_EXP_SIGMA` = `$HOME/daten/gd/sigma/export`
  - `DW_DIR_IMP_TRF` = `$HOME/daten/trf`
  - `DW_DIR_IMP_AUF` = `$HOME/daten/sd/auf`
  - `DW_DIR_IMP_GUT` = `$HOME/daten/sd/gut`
  - `DW_DIR_IMP_KDG` = `$HOME/daten/sd/kdg`
  - `DW_DIR_IMP_MP_KDG` = `$HOME/daten/mp/kdg`
  - `DW_DIR_IMP_MP_TS` = `$HOME/daten/mp/ts`
  - `DW_DIR_IMP_MP_ZM` = `$HOME/daten/mp/zm`
  - `DW_DIR_IMP_TS` = `$HOME/daten/sd/ts`
  - `DW_DIR_IMP_ZM` = `$HOME/daten/sd/zm`
  - `DW_DIR_EXP` = `$HOME/daten/exporter`
  - `DW_DIR_IMP_BPM` = `$HOME/daten/bm`
  - `DW_DIR_IMP_ZTS` = `$HOME/daten/zts`
  - `DW_DIR_IMP_VRS` = `$HOME/daten/vrs`
  - `DW_DIR_IMP_BRUNET` = `$HOME/daten/brunet`
  - `DW_DIR_IMP_DWH` = `$HOME/daten/dwh`
  - `DW_DIR_IMP_PLATO` = `$HOME/daten/dwh/plato`
  - `DW_DIR_IMP_CARMEN` = `$HOME/daten/carmen`
  - `DW_DIR_IMP_SAP` = `$HOME/daten/sap`
  - `DW_DIR_IMP_SR_RV` = `$HOME/daten/sap/sr_rv_dpps`
  - `DW_DIR_IMP_SAP_L` = `$HOME/daten/sap/sap_l_gutgr` (assigned from `DW_DIR_IMP_SAP_L_GUTGR`)
  - `DW_DIR_IMP_L_MAHNSTYP_IST` = `$HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_FI` = `$HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_IST` = `$HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_GUTGR` = `$HOME/daten/sd/l_gutschr`
  - `DW_DIR_IMP_L_LEIST` = `$HOME/daten/sd/l_leist`
  - `DW_DIR_IMP_L_PROD` = `$HOME/daten/sd/l_prod`
  - `DW_DIR_LKODE` = `$HOME/daten/sd/lkode`
  - `DW_DIR_IMP_SUBSE` = `$HOME/daten/subse`
  - `DW_DIR_SMS_PRG` = `${HOME}/aktuell/allgemein/is/util`
  - `DW_DIR_SMS_ADR` = `${HOME}/daten/sms/adressen`
  - `DW_DIR_SMS_TMP` = `${HOME}/daten/sms/tmp`
  - `DW_DIR_IMP_DPPS` = `$HOME/daten/dpps`
  - `DW_DIR_IMP_PLANF2` = `$HOME/daten/planf2`
  - `DW_HOST_CUSTOMER` = `dxcst3.bn.detemobil.de`
  - `ORACLE_HOME` = `/appl/local/oracle/12.2.0.1.0` or `/appl/local/oracle/11.2.0` (evaluated conditionally)
  - `DW_DIR_UTL_FILE` = `/appl/local/oracle/admin/$ORACLE_SID/utl_file`
- Environment files sourced:
  - `. $HOME/.dw_global`
  - `. $HOME/.dw_lokal`
- Invokes:
  - `. $HOME/.dw_global`
  - `. $HOME/.dw_lokal`
- Called by: Sourced as a profile or environment script by various execution elements in the legacy environment.
- Exit-code behaviour: Propagates the exit code of the final evaluated statement (sourcing of `.dw_lokal`).
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion.

### Job Dependencies
* **Upstream Jobs**:
  * Shared Files — `TMD_processing/ALL_TYPES/mp`: This has been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/883).
  * Shared Files — `TMD_processing/ALL_TYPES/run`: This has been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/884).
* **Target Wiring**: The upstream Ab Initio graph components (`all_types_graph.mp` and `all_types_graph.ksh`) have already been converted to PySpark pipelines / Python operators. In the target Cloud Composer environment, the orchestrating DAG will directly import and run these pre-migrated PySpark modules using standard Airflow Operators (e.g., `DataprocStartClusterOperator` or `DataprocSubmitJobOperator`).

### Execution Order
The legacy orchestration executes in the following sequence, which must be preserved in the target Airflow DAG:
1. `DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml` (UC4 Job Definition) -> Mapped to the master Airflow DAG orchestration.
2. `isall/abinitio/cfg/all_types/all_types_graph.cfg` (Ab Initio configuration) -> Mapped to DAG parameters/configurations for the PySpark tasks.
3. `isall/aufbereitung/bin/r_all_types_master.ksh` (Master wrapper script) -> Mapped to Airflow Python/Bash Operators executing tasks in sequence.
4. `.dw_init` (Environment initialization script) -> Retired; environment setup is handled natively by Cloud Composer/Airflow variables.
5. `isall/aufbereitung/awk/k_all_types_transform.awk` (AWK transform script) -> Mapped to PySpark transformation logic or BigQuery SQL.
6. `isall/aufbereitung/sql/d_all_types.sql` (Oracle SQL script) -> Mapped to BigQuery SQL tasks (`BigQueryInsertJobOperator`).

### Lineage
* **Upstream Producers**: 
  * `.dw_init` references and sources `.DW_GLOBAL` (unresolved; confirmed by human review as "not needed")
  * `.dw_init` references and sources `.DW_LOKAL` (unresolved; confirmed by human review as "not needed")

### Cross-File Dependencies
* **Sourcing and Configuration**: The file `.dw_init` historically sourced `.dw_global` and `.dw_lokal` to establish the global and local settings for the environment. These physical files are eliminated as they are human-confirmed as "not needed", and the required variables are consolidated directly into environment variables or Airflow Variables in Cloud Composer.

### Target File Plan
* **Target Action**: None. The script `.dw_init` is classified as **Retired**. There is no dedicated target file generated for this script because environment and path variables are configured directly via Cloud Composer Environment Variables, Airflow Variables, and Terraform-managed infrastructure parameters rather than flat-file shell scripting.

### Environment-Specific Values
The environment variables from `.dw_init` are classified below according to their role in the target Cloud Composer / BigQuery environment:

#### 1. GLOBAL (Environment-Wide)
These values are environment-wide and identify the target infrastructure. They must be sourced at runtime from Airflow configurations or OS environment variables:
* `DW_DIR_ROOT` -> `GCS_BUCKET` / `gcs_root_path` (Sourced via `Variable.get("gcs_root_path")` or standard environment variables).
* `DW_DIR_PROT` -> `GCS_BUCKET` / `gcs_log_path` (Sourced via `Variable.get("gcs_log_path")`).
* `DW_DIR_CUBES` -> `GCS_BUCKET` / `gcs_cubes_path` (Sourced via `Variable.get("gcs_cubes_path")`).
* `DW_HOST_CUSTOMER` (`dxcst3.bn.detemobil.de`) -> `CUSTOMER_SFTP_HOST` (Sourced via Airflow Connection ID or `Variable.get("customer_sftp_host")`).

#### 2. JOB-SPECIFIC
These parameters are specific to individual input directory tracks and can be passed as DAG-level or task-level parameters:
* `DW_DIR_IMP_*` (e.g., `DW_DIR_IMP_D1`, `DW_DIR_IMP_BWA`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `DW_DIR_IMP_VO`, `DW_DIR_IMP_RV`, `DW_DIR_IMP_IF`, `DW_DIR_IMP_NNV`, `DW_DIR_IMP_SIGMA`, `DW_DIR_EXP_SIGMA`, `DW_DIR_IMP_TRF`, `DW_DIR_IMP_AUF`, `DW_DIR_IMP_GUT`, `DW_DIR_IMP_KDG`, `DW_DIR_IMP_MP_KDG`, `DW_DIR_IMP_MP_TS`, `DW_DIR_IMP_MP_ZM`, `DW_DIR_IMP_TS`, `DW_DIR_IMP_ZM`, `DW_DIR_EXP`, `DW_DIR_IMP_BPM`, `DW_DIR_IMP_ZTS`, `DW_DIR_IMP_VRS`, `DW_DIR_IMP_BRUNET`, `DW_DIR_IMP_DWH`, `DW_DIR_IMP_PLATO`, `DW_DIR_IMP_CARMEN`, `DW_DIR_IMP_SAP`, `DW_DIR_IMP_SR_RV`, `DW_DIR_IMP_SAP_L`, `DW_DIR_IMP_L_MAHNSTYP_IST`, `DW_DIR_IMP_L_MAHNV_FI`, `DW_DIR_IMP_L_MAHNV_IST`, `DW_DIR_IMP_L_GUTGR`, `DW_DIR_IMP_L_LEIST`, `DW_DIR_IMP_L_PROD`, `DW_DIR_LKODE`, `DW_DIR_IMP_SUBSE`, `DW_DIR_SMS_PRG`, `DW_DIR_SMS_ADR`, `DW_DIR_SMS_TMP`, `DW_DIR_IMP_DPPS`, `DW_DIR_IMP_PLANF2`) -> Mapped to specific Cloud Storage bucket paths representing each incoming feed and maintained as a configuration dictionary in the Airflow DAG or loaded via Airflow variables.

#### 3. OBSOLETE (No Direct Equivalent)
These Oracle-specific variables have no direct target-platform equivalent on BigQuery and are retired:
* `ORACLE_HOME` (No direct equivalent exists for BigQuery).
* `DW_DIR_UTL_FILE` (No direct equivalent exists for BigQuery).

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/data/source/all_types_linked_job/.dw_init` | **Retired** | Environment initialization settings (directories, environment variables, oracle pathing) are retired and replaced by native GCP environment variables, Airflow variables, and Composer configuration rather than being run as a script. |

---

### Risks & Manual Actions
* **Configuration Sync**: The `.dw_global` and `.dw_lokal` files have been marked "not needed" by human resolutions; however, a manual verification must be made during the deployment of the Cloud Composer environment to ensure no critical pathing configurations from those files were missed.
* **Remote SFTP Host**: The script references a customer host (`DW_HOST_CUSTOMER=dxcst3.bn.detemobil.de`). If files are actively pulled or pushed to this remote host, an Airflow Connection (such as `SFTPHook` or `SSHHook` credentials) must be configured in Cloud Composer manually.

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
The AWK script processes a semicolon-separated file, validating that every record contains exactly 12 fields (NF == 12). If a record has the correct number of fields, it prepends "D;" to the line and prints it. If any record violates this requirement, it outputs an error message and terminates immediately with a non-zero exit status (`exit 2`). 

This non-zero exit code is a critical orchestrator signal designed to abort the entire ETL pipeline ("ALL_TYPES_MASTER job") upon validation failure. BigQuery SQL queries run as database transactions and cannot raise custom process-level exit codes (like `exit 2`) to the calling shell or orchestrator mid-query based on row validation rules. Python can easily implement this control flow using the `csv` module and `sys.exit(2)`. Therefore, Python is the required target.
Conversion confidence: High.

FEATURE INVENTORY
* `BEGIN` block: Expressible in Python (used for initial configuration, e.g., setting up csv reader/writer parameters). Not directly applicable in pure BQSQL without schema definitions.
* `END` block: Expressible in Python as a no-op or final cleanup. It is empty in this script.
* `FS` / `OFS`: Expressible in Python via the `csv` module with `delimiter=';'`. In BQSQL, these are defined during external table creation or via split operations.
* `NF` (Field Count): Expressible in Python as `len(row)`. In BQSQL, verifying field counts on raw delimited strings requires complex string manipulation (e.g., counting occurrences of ';'), whereas Python handles it natively.
* `conditions` (`if/else`): Expressible in Python (`if len(row) == 12:`). In BQSQL, this can be represented via `CASE` expressions, but BQSQL cannot execute side-effects like aborting the process in the `else` branch.
* `print` / `$0` (Record referencing): Expressible in Python by writing the string or row to stdout/file. In BQSQL, this is represented by selecting and concatenating columns or string fields.
* `exit` (with non-zero code): BQSQL-disqualifying. Expressible in Python via `sys.exit(2)`. BigQuery SQL cannot terminate a query execution with a specific, custom OS-level exit code to signal orchestrator failure.

An explainable Design Document followed by numbered Python-oriented pseudocode for the migration of `k_all_types_transform.awk` to Python 3 is detailed below.

---

### 1. MIGRATION DECISION SUMMARY

*   **Target Language:** Python 3
*   **BigQuery SQL Ruling:** Ruled out. The primary business reason is that this script serves as an inline validation gate within an orchestration chain (`ALL_TYPES_MASTER` job). If any record fails the validation rule (field count != 12), the script must immediately halt processing of the stream and exit with a distinct operating-system level return code (`exit 2`). A standard declarative BigQuery SQL query cannot abort mid-execution and raise custom process-level exit codes to a calling shell or orchestrator. Thus, a Python-based streaming script is required to maintain this system control-flow behavior.
*   **Conversion Confidence:** High.
*   **Human Review:** Required to verify downstream orchestration behavior when handling standard output streams upon a mid-stream process failure (exit code 2).

---

### 2. PROGRAM OVERVIEW

*   **Purpose:** Post-processes the `ALL_TYPES` export file. It prefixes every valid 12-field data record with the string `"D;"` and validates the structure. It acts as an early-exit validator for malformed data.
*   **Input Streams:** Standard input (`stdin`) or files specified as command-line arguments (default AWK stream behavior). Semicolon-delimited rows.
*   **Output Streams:** 
    *   Standard Output (`stdout`): Processed valid records prefixed with `"D;"`, as well as the error string `"Error: Incorrect nos of Fields "` upon failure.
*   **Command-line Variables:** None.
*   **Expected Record Format:** 12 semicolon-separated fields.
*   **Observable Side-Effects:** Halts processing immediately with process exit code `2` on the first record that does not contain exactly 12 fields.

---

### 3. AWK FEATURE INVENTORY

| AWK Feature | AWK Source Behavior | Proposed Python Equivalent |
| :--- | :--- | :--- |
| `BEGIN` block | Sets `FS = ";"` and `OFS = ";"` | Not strictly needed for split/join, but maps to initializing file/stdin readers with semicolon delimiters. |
| `FS` / `OFS` | Semicolon input/output field separators. | Emulated via string split on `';'` (`line.split(';')`) and string concatenation (`"D;" + line`). |
| `NF` | Number of fields in current record. | `len(line.split(';'))` |
| `$0` | The unmodified input line (excluding the record separator). | `line.rstrip('\r\n')` |
| `print` | Writes to `stdout` with `ORS` (newline). | `print()` function (which defaults to newline terminator). |
| `exit 2` | Aborts processing immediately, returning exit code 2. | `sys.exit(2)` |
| `END` block | Empty. | Omitted in Python. |

---

### 4. PYTHON IMPLEMENTATION STRATEGY

*   **Standard Libraries:** `sys` (for reading `stdin`, handling arguments, and calling `exit()`).
*   **Streaming Loop:** Read lines sequentially from `sys.stdin` (or files passed in `sys.argv[1:]`) to ensure low memory footprint.
*   **Field Splitting and Validation:**
    *   For each raw line, strip the trailing record separators (`\r\n` or `\n`). This represents `$0`.
    *   Perform a split on the semicolon character `';'`. 
    *   Count the resulting array length. This represents `NF`.
*   **Coercion and Typing:** None required for this structural check.
*   **Error Handling:** If the array length is not 12, print the exact string `"Error: Incorrect nos of Fields "` to `stdout` (to match AWK output target) and call `sys.exit(2)`.

---

### 5. INPUTS, OUTPUTS, AND DEPENDENCIES

*   **Upstream Inputs:** Raw export file (semicolon-delimited) piped to `stdin` or passed as a file path argument.
*   **Downstream Outputs:** Semicolon-delimited records prefixed with `D;` written to `stdout`.
*   **External Orchestration Dependency:** The orchestrator of `ALL_TYPES_MASTER` must monitor the exit status of this Python script and fail the pipeline step if the status is non-zero (specifically `2`).

---

### 6. UNSUPPORTED FEATURES, WARNINGS, AND ASSUMPTIONS

*   **# REVIEW:** In AWK, `print "Error: Incorrect nos of Fields "` outputs to `stdout`. In typical UNIX applications, error messages are written to `stderr` (`sys.stderr.write`). The pseudocode preserves writing to `stdout` to maintain exact stream compatibility, but this should be confirmed with the system architect.
*   **Assumption:** There are no embedded semicolons inside double-quoted text blocks (i.e. standard CSV escaping rules do not apply, and simple splitting is correct). AWK's `FS=";"` splits purely on the character `;` regardless of quotes, which we replicate.

---

### 7. MANUAL REVIEW ITEMS

1.  **Orchestrator Integration:** Ensure that the wrapper bash script or orchestrator (Airflow, Control-M, etc.) captures the exit code `2` of this Python script in the exact same manner as the AWK script.
2.  **Stream Routing:** Confirm whether the error message should be redirected to `sys.stderr` instead of `sys.stdout`.

---

### 8. NUMBERED PSEUDOCODE

```python
# 1. Import necessary system modules
import sys

# 2. Define the main execution block
def main():
    # 3. Determine input sources: standard input or command-line file arguments
    input_sources = sys.argv[1:]
    
    # 4. If files are provided, open and process each sequentially; else read stdin
    if input_sources:
        for file_path in input_sources:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    process_stream(f)
            except FileNotFoundError:
                sys.stderr.write(f"Error: File not found {file_path}\n")
                sys.exit(1)
    else:
        process_stream(sys.stdin)

# 5. Define the streaming processor function
def process_stream(stream):
    for line in stream:
        # 6. Extract raw record line without trailing line endings ($0 equivalent)
        stripped_line = line.rstrip('\r\n')
        
        # 7. Split line by delimiter to calculate field count (NF equivalent)
        fields = stripped_line.split(';')
        field_count = len(fields)
        
        # 8. Check if the line has exactly 12 fields
        if field_count == 12:
            # 9. Print valid row prefixed with "D;" to stdout
            print(f"D;{stripped_line}")
        else:
            # 10. Print the exact error message to stdout (retaining AWK standard target)
            print("Error: Incorrect nos of Fields ")
            # 11. Exit immediately with status code 2 to signal orchestrator failure
            sys.exit(2)

# 12. Script entry point execution
if __name__ == '__main__':
    main()
```

### Job dependencies
* **Upstream Dependencies:**
  * **Shared Files — TMD_processing/ALL_TYPES/mp:** The legacy Ab Initio graph (`all_types_graph.mp`) has already been migrated and merged (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/883`).
  * **Shared Files — TMD_processing/ALL_TYPES/run:** The legacy Ab Initio execution wrapper (`all_types_graph.ksh`) has already been migrated and merged (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/884`).
  These upstream modules must be successfully run on the target platform (Cloud Composer) as preceding tasks before triggering the task representing the migrated AWK script.

### Execution order
The legacy execution order of the `DW.DWH_ALL_TYPES_MASTER` job must be preserved on the target BigQuery/Cloud Composer platform:
1. **DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml** -> Mapped to the Cloud Composer DAG orchestration file.
2. **isall/abinitio/cfg/all_types/all_types_graph.cfg** -> Mapped to DAG-level Airflow Variables or a parameter file passed to Dataproc Serverless.
3. **isall/aufbereitung/bin/r_all_types_master.ksh** -> Mapped to the sequential orchestration tasks within the Airflow DAG.
4. **.dw_init** -> Mapped to a DAG initialization/bootstrap task.
5. **isall/aufbereitung/awk/k_all_types_transform.awk** -> Mapped to a Python virtualenv task / standard Python Operator executing the migrated script `isall/aufbereitung/awk/k_all_types_transform.py`.
6. **isall/aufbereitung/sql/d_all_types.sql** -> Mapped to a BigQueryInsertJobOperator executing the BigQuery SQL transformation.

### Cross-file dependencies
* **Graph Output to Post-Processing Script:** The output export file generated by the Ab Initio graph execution (`all_types_graph.ksh`) is the direct input to `isall/aufbereitung/awk/k_all_types_transform.awk`. This script performs the validation check and adds the `"D;"` prefix.
* **Post-Processing Output to BigQuery Loading:** The validated output generated by the Python script (with `"D;"` prepended) serves as the downstream source to be loaded into the BigQuery raw/target tables via the queries migrated from `isall/aufbereitung/sql/d_all_types.sql`.

### Target file plan
* **isall/aufbereitung/awk/k_all_types_transform.py**
  * **Language:** Python 3
  * **Source File:** `isall/aufbereitung/awk/k_all_types_transform.awk`

### Environment-specific values
No environmental variables or target-platform identifiers are used directly in `isall/aufbereitung/awk/k_all_types_transform.awk`. However, for the associated parameter file `isall/abinitio/cfg/all_types/all_types_graph.cfg`, the following mappings apply:
* **ALL_TYPES_Projektverzeichnis** (`/Projects/TMD/processing/ALL_TYPES/`): Classified as **JOB-SPECIFIC**. Parameterized at runtime as an Airflow DAG-level parameter.
* **ALL_TYPES_AI_DAT_FILE_DIR** (`$ALL_TYPES_DIR_EXP_UTL/cubes/at`): Classified as **JOB-SPECIFIC**. Uses the path suffix `cubes/at` relative to the root environment-wide bucket variable.
* **ALL_TYPES_DIR_EXP_UTL** (referenced env var): Classified as **GLOBAL**. It represents a shared environment path mapping to a Google Cloud Storage bucket (`GCS_BUCKET`). This value must be resolved at runtime via `Variable.get("GCS_BUCKET")` in the Airflow DAG.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| isall/aufbereitung/awk/k_all_types_transform.awk | isall/aufbereitung/awk/k_all_types_transform.py | Migrated to a Python 3 script to maintain structural record validations (verifying field count is exactly 12) and to cleanly support custom process-level exit signaling (exit code 2) required to fail the orchestrator pipeline upon encountering bad data. |

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
REASON: The script invokes an external AWK transformation and an Oracle SQL script whose source files were not supplied, requiring a Python orchestration conversion.

EVIDENCE
- Business logic found: KSH custom logic executes an Oracle SQL script via sqlplus and subsequently executes an AWK program to transform a CSV export file.
- AWK: One AWK program 'k_all_types_transform.awk' is invoked, but because its source code was not supplied, its SQL-expressibility cannot be verified.
- SQL-expressible: No, because the AWK post-processing logic and raw CSV target output are unsupplied and currently live outside the database.
- Non-SQL side effects: Writes structured data directly to a flat file ('all_types_export.out') and appends status logs to a file via 'tee'.
- Against this verdict: If both the AWK and SQL scripts were supplied and confirmed to be simple relational mappings, the entire workflow could be consolidated into a BigQuery SQL script.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script (`r_all_types_master.ksh`) acts as a master orchestration wrapper for the "ALL_TYPES Showcase" job. It executes a database-level SQL refresh using Oracle SQL*Plus and then post-processes a CSV data export using an AWK script. The script is designed to run in sequence immediately after an Ab Initio graph execution to finalize the showcases' data preparation.

### 2. INVOCATION CONTEXT
- **Caller**: Typically invoked within a UC4/Automic job chain (JOBS_UNIX object) following an Ab Initio graph run.
- **Command Line / Arguments**: Called with no positional arguments.
- **UC4 Native Includes**: None referenced in the extraction.
- **Environment Files Sourced**: 
  - `. $HOME/.dw_init`
  - # REVIEW-STRUCT: environment file $HOME/.dw_init not supplied — variables it sets are unknown; do not guess their names or values

### 3. PARAMETERS / INPUTS
- **JobKennung** (Local variable): Sourced from hardcoded script definition (`"ALL_TYPES_MASTER"`), cast to uppercase. Used for logging.
- **v_sysdate** (Local variable): Sourced from system date command (`date +%d%m%Y`). Used to timestamp the log file name.
- **ALL_DIR_ROOT** (Environment variable): Expected to be sourced from `$HOME/.dw_init`. Defines root directory paths for SQL, AWK, data, and log files.
- **DW_ORAUSER** (Environment variable): Expected to be sourced from `$HOME/.dw_init`. Represents database credentials. This parameter indicates an Oracle database source, which serves as a cross-referenced connection convention.
- **Audit Checklist**: No KSH functions are declared in this script, and no parameter-validation guards are present in the KSH source.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **Command 1**: `sqlplus ${DW_ORAUSER} @${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql </dev/null >> $LogDatei 2>&1`
  - *Purpose*: Executes the SQL refresh script `d_all_types.sql` against the database.
  - *Translation*: Standard external process invocation. Because the target platform is confirmed as BigQuery, this would ideally run as a native BigQuery Python client execution of the translated SQL file. However, since the SQL file body is not supplied, it fails the "RESOLVABLE LAUNCHER" pattern condition 1.
  - *Status*: # REVIEW-STRUCT: launcher sqlplus invoked — internal behaviour of @${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
- **Command 2**: `awk -f ${ALL_DIR_ROOT}/aufbereitung/awk/k_all_types_transform.awk ${ALL_DIR_ROOT}/data/all_types_export.csv > ${ALL_DIR_ROOT}/data/all_types_export.out`
  - *Purpose*: Processes the CSV export using the AWK program and writes the transformed results to `all_types_export.out`.
  - *Translation*: Must remain an external call (e.g. executing `awk` via `subprocess.run` or invoking a Python port of the AWK script).
  - *Status*: # REVIEW-STRUCT: AWK script k_all_types_transform.awk body not supplied — behaviour unknown; check the transformation rules before finalizing the conversion.

### 5. EMBEDDED SQL
- **Source File**: `${ALL_DIR_ROOT}/aufbereitung/sql/d_all_types.sql`
- **Full SQL Text**: Not supplied in the extraction.
- **Statement Type**: Unknown.
- **Tables Touched**: Unknown.
- **Dialect**: Oracle SQL (indicated by `sqlplus` launcher and connection syntax).
- **Target Platform Note**: The target platform is confirmed as BigQuery. When the `.sql` source is retrieved, it must be compiled into BigQuery Standard SQL dialects (e.g. replacing Oracle-specific syntax).

### 6. CONTROL FLOW
1. **Environment Setup**: Sourced `. $HOME/.dw_init` (defines `ALL_DIR_ROOT` and `DW_ORAUSER`).
2. **Execution Flags**: Enabled `set -eu` to ensure immediate script exit on error or unbound variables.
3. **Variable Assignment**:
   - `JobKennung` initialized to `"ALL_TYPES_MASTER"`.
   - `v_sysdate` initialized to the current date in `DDMMYYYY` format.
   - `LogDatei` path defined under `${ALL_DIR_ROOT}/protokoll/`.
4. **Header Logging**: Print job metadata to standard output.
5. **Step 1 (SQL Execution)**: Call `sqlplus` to execute `d_all_types.sql`, redirecting all standard output and errors to `LogDatei`.
6. **Step 2 (Data Transformation)**: Call `awk` with the transformation script to parse `all_types_export.csv` and write the result to `all_types_export.out`.
7. **Success Logging**: Log final success message and exit with code `0`.

### 7. ERROR HANDLING & EXIT CODES
- **Detection**: The script relies on KSH `set -e` to terminate on any non-zero exit code of a foreground command.
- **Propagation**: If `sqlplus` or `awk` returns a non-zero exit status, execution terminates instantly and propagates that status code to the caller.
- **Success Code**: `0` on successful completion.
- **Python Mapping**: Map subprocess executions to `subprocess.run(..., check=True)`, which automatically raises `subprocess.CalledProcessError` on failure. Uncaught exceptions will terminate the Python interpreter with a non-zero exit code.

### 8. OUTPUTS / SIDE EFFECTS
- **Log Files**: Append and write tracking logs to `${ALL_DIR_ROOT}/protokoll/all_types_master_${v_sysdate}.log`.
- **Data Files**: Overwrites or creates `${ALL_DIR_ROOT}/data/all_types_export.out`.

### 9. BUSINESS SUMMARY
- Orchestrates final stages of the `ALL_TYPES` Showcase process sequence.
- Refreshes database staging/reporting structures within the database.
- Transforms raw CSV exports to output formats suitable for downstream ingestion.
- Ensures robust, sequential execution where processing ceases immediately if either the database refresh or the data-formatting steps fail.

---

### Python Pseudocode Outline

```python
import os
import sys
import subprocess
from datetime import datetime

def main():
    # Step 1: Env setup & verification
    # # REVIEW-STRUCT: environment file $HOME/.dw_init not supplied — variables it sets are unknown; do not guess their names or values
    # We expect variables such as ALL_DIR_ROOT and DW_ORAUSER to be present in the runtime environment.
    all_dir_root = os.environ.get("ALL_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    
    if not all_dir_root:
        print("Error: ALL_DIR_ROOT environment variable not set.", file=sys.stderr)
        sys.exit(1)
        
    if not dw_orauser:
        print("Error: DW_ORAUSER environment variable not set.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Establish local orchestration variables
    job_kennung = "ALL_TYPES_MASTER".upper()
    v_sysdate = datetime.now().strftime("%d%m%Y")
    log_datei = os.path.join(all_dir_root, "protokoll", f"all_types_master_{v_sysdate}.log")

    # Step 3: Print job run header details
    print(" ----------------- Job -----------------------")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")

    try:
        # Step 4: Step 1 - Execute Oracle SQL script
        print("----Starte SQL-Refresh----")
        sql_script_path = os.path.join(all_dir_root, "aufbereitung", "sql", "d_all_types.sql")
        
        # # REVIEW-STRUCT: launcher sqlplus invoked — internal behaviour of d_all_types.sql not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
        # Note: Confirmed target platform is BigQuery. When migrating, this step should be refactored to execute target BigQuery SQL statements.
        with open(log_datei, "a") as log_file:
            log_file.write("----Starte SQL-Refresh----\n")
            log_file.flush()
            
            # Execute SQL via sqlplus
            sqlplus_cmd = ["sqlplus", dw_orauser, f"@{sql_script_path}"]
            subprocess.run(
                sqlplus_cmd, 
                stdin=subprocess.DEVNULL, 
                stdout=log_file, 
                stderr=subprocess.STDOUT, 
                check=True
            )

        # Step 5: Step 2 - Execute AWK data transformation
        print("----Starte AWK-Nachbearbeitung----")
        awk_script_path = os.path.join(all_dir_root, "aufbereitung", "awk", "k_all_types_transform.awk")
        input_csv_path = os.path.join(all_dir_root, "data", "all_types_export.csv")
        output_out_path = os.path.join(all_dir_root, "data", "all_types_export.out")
        
        # # REVIEW-STRUCT: AWK script k_all_types_transform.awk body not supplied — behaviour unknown; check transformation rules before finalizing.
        with open(log_datei, "a") as log_file:
            log_file.write("----Starte AWK-Nachbearbeitung----\n")
            log_file.flush()
            
            awk_cmd = ["awk", "-f", awk_script_path, input_csv_path]
            with open(output_out_path, "w") as out_file:
                subprocess.run(
                    awk_cmd,
                    stdout=out_file,
                    stderr=subprocess.PIPE, # Capture errors to log if needed
                    check=True
                )

        # Step 6: Log successful termination
        success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
        print(success_msg)
        with open(log_datei, "a") as log_file:
            log_file.write(f"{success_msg}\n")
            
    except subprocess.CalledProcessError as err:
        error_msg = f"Failure detected during execution of command: {err.cmd} (Exit Code: {err.returncode})"
        print(error_msg, file=sys.stderr)
        # Attempt to write error trace to log file if path exists
        try:
            with open(log_datei, "a") as log_file:
                log_file.write(f"ERROR: {error_msg}\n")
                if err.stderr:
                    log_file.write(f"STDERR: {err.stderr.decode()}\n")
        except Exception:
            pass
        sys.exit(err.returncode)
    except Exception as ex:
        print(f"Unexpected error: {str(ex)}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
```

# MIGRATION DESIGN DOCUMENT: DW.DWH_ALL_TYPES_MASTER

### Job dependencies
* **Upstream**:
  * `TMD_processing/ALL_TYPES/mp` (Ab Initio graph, migrated to Dataproc PySpark pipeline): Already migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/883).
  * `TMD_processing/ALL_TYPES/run` (KSH wrapper for graph run): Already migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/884).
* **Target Platform Wiring**:
  * In the target Cloud Composer environment, the master DAG replacing the UC4 orchestration (`DWH_ALL_TYPES_MASTER.xml`) must first execute the migrated Dataproc PySpark job (`all_types_graph.mp`).
  * Once the Dataproc job completes successfully, the DAG will trigger the execution of the migrated master Python script (`isall/aufbereitung/bin/r_all_types_master.py`).

### Execution order
The target orchestration must preserve the sequential flow of the legacy dependency graph:
1. **Airflow DAG Activation**: Replaces the UC4 scheduling/trigger layer (`DWH_ALL_TYPES_MASTER.xml`).
2. **Environment/Parameter Setup**: Sourced from Airflow Variables/OS Environment variables (replacing the legacy `.dw_init` and `all_types_graph.cfg`).
3. **Primary Processing**: Executes the migrated Ab Initio Dataproc PySpark pipeline (`all_types_graph.mp`).
4. **Post-Processing Orchestration**: Executes `isall/aufbereitung/bin/r_all_types_master.py`.
5. **Post-Processing - Step 1**: Inside `r_all_types_master.py`, executes BigQuery SQL (`d_all_types.sql`) with parameters.
6. **Post-Processing - Step 2**: Inside `r_all_types_master.py`, executes the AWK-migrated Python script (`k_all_types_transform.py`) using `sys.executable`.

### Lineage
* **Upstream Producers**:
  * Environment configuration variables defined in the system.
  * Transformed/staged data from the Ab Initio Dataproc PySpark run (loaded into the intermediate database layers).
* **Downstream Consumers**:
  * The final processed CSV export file `all_types_export.out` (stored at `gs://<GCS_BUCKET>/data/all_types_export.out`).

### Cross-file dependencies
* `isall/aufbereitung/bin/r_all_types_master.ksh` (migrating to `isall/aufbereitung/bin/r_all_types_master.py`) relies on:
  * `isall/aufbereitung/sql/d_all_types.sql`: Contains the SQL logic, migrated to BigQuery SQL, executed via Python's BigQuery Client.
  * `isall/aufbereitung/awk/k_all_types_transform.awk` (migrated to `isall/aufbereitung/awk/k_all_types_transform.py`): Post-processing data logic.
  * `.dw_init`: Sourced environment script, replaced by OS environment variables and Airflow-configured parameters.

### Target file plan
* **Target File Path**: `isall/aufbereitung/bin/r_all_types_master.py`
  * **Language**: Python
  * **Source File**: `isall/aufbereitung/bin/r_all_types_master.ksh`
  * **Design Specification**:
    * Act as the canonical Python orchestrator for post-processing steps.
    * Load and execute the BigQuery SQL script (`d_all_types.sql`) using `bigquery.Client().query()`.
    * Pass query parameters `@gcp_project` and `@bq_dataset` to the query call using `bigquery.ScalarQueryParameter` sourced from `GCP_PROJECT` and `BQ_DATASET` environment variables.
    * Execute the migrated Python script `k_all_types_transform.py` using `sys.executable` (via `subprocess.run([sys.executable, ...], check=True)`) rather than running a legacy `awk` command.
    * Preserve all logging literals from the legacy shell script character-for-character, including:
      * `" ----------------- Job -----------------------"`
      * `" JobKennung: 'ALL_TYPES_MASTER'"`
      * `" Logdatei  : ..."`
      * `" ---------------------------------------------"`
      * `"----Starte SQL-Refresh----"`
      * `"----Starte AWK-Nachbearbeitung----"`
      * `"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`

### Environment-specific values
* `GCP_PROJECT`: **GLOBAL**
  * *Target Sourcing*: Sourced via `os.environ.get("GCP_PROJECT")`.
* `GCP_REGION`: **GLOBAL**
  * *Target Sourcing*: Sourced via `os.environ.get("GCP_REGION")`.
* `BQ_DATASET`: **GLOBAL**
  * *Target Sourcing*: Sourced via `os.environ.get("BQ_DATASET")`.
* `GCS_BUCKET`: **GLOBAL**
  * *Target Sourcing*: Sourced via `os.environ.get("GCS_BUCKET")`.
* `ALL_DIR_ROOT`: **JOB-SPECIFIC**
  * *Target Sourcing*: Sourced from environment or mapped directly to `/workspace` (or appropriate mount/base directory structure in Airflow).
* `DW_ORAUSER`: **RETIRED**
  * *Reason*: Legacy Oracle credential, obsolete on Google Cloud Platform.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/bin/r_all_types_master.ksh` | `isall/aufbereitung/bin/r_all_types_master.py` | Migrates legacy KSH wrapper to Python to orchestrate the parameterized BigQuery execution and AWK-converted Python logic in sequence. |

### Risks & Manual Actions
* **Verify Sister Migrations**: Ensure that the sibling migrations for `isall/aufbereitung/sql/d_all_types.sql` and `isall/aufbereitung/awk/k_all_types_transform.py` are present in their mirrored target directory paths relative to `/workspace` (or the environment base directory) before initiating the build execution.
* **GCS Path Adjustments**: Local file system paths (e.g., `data/all_types_export.csv`) must be adjusted to either run on persistent local workspace directories or read/write directly to GCS buckets (`gs://<GCS_BUCKET>/data/...`), depending on how the Composer worker environment is configured.

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
    - This is a multi-statement Oracle SQL*Plus script containing DDL (TRUNCATE), DML (INSERT with SELECT), transaction control (COMMIT), and client-side utility/flow control directives (WHENEVER SQLERROR, PROMPT).

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script refreshes the `sof$ta_all_types` staging/target table by first clearing its existing data using a `TRUNCATE` statement. 
    - It then extracts and loads records from a raw source table, `cds$ta_all_types_raw`, filtering for records with a `status` of 'READY'.
    - During insertion, it captures the primary identifier (`all_types_id`), the source identification string (`source_system`), and stamps the load execution timestamp using the system date/time.

1.3 List all entities referenced:
    - Target Table: `sof$ta_all_types` (columns: `all_types_id`, `source_system`, `processed_at`)
    - Source Table: `cds$ta_all_types_raw` with alias `r` (columns: `r.all_types_id`, `r.source_system`, `r.status`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` on `processed_at` → Map to BigQuery `DATETIME` using `CURRENT_DATETIME()` to preserve the time component without time-zone offsets, or `TIMESTAMP` with `CURRENT_TIMESTAMP()` if the warehouse standard dictates UTC tracking. Let's map to `DATETIME` as a direct representation of Oracle's local `SYSDATE` behavior.
    - `all_types_id` (Inferred generic identifier) → Map to `INT64` (if numeric) or `STRING` (if alphanumeric).
    - `source_system` (Inferred source code) → Map to `STRING`.
    - `status` (Inferred status flag) → Map to `STRING`.

2.2 Implicit and Explicit Type Casting:
    - No implicit type casts are detected in the SQL expression logic.

2.3 NULL Handling and Conditional Functions:
    - None used.

2.4 String Functions:
    - None used.

2.5 Date and Timestamp Functions:
    - `SYSDATE` → Resolve to `CURRENT_DATETIME()` to preserve date and time information natively in BigQuery.
    - Semantic validation: `CURRENT_DATETIME()` returns a `DATETIME` type corresponding to the current system execution time, which aligns exactly with Oracle's default `SYSDATE` semantics without requiring timezone alignment unless explicitly parameterized.

2.6-2.10: None used.

2.11 MERGE Statements:
    - None used.

2.12 INSERT / UPDATE / DELETE:
    - `TRUNCATE TABLE` → Supported natively in BigQuery.
    - `INSERT INTO ... SELECT` → Supported natively in BigQuery.

2.13 DDL Constructs (if present):
    - None.

2.14 PL/SQL (if present):
    - `WHENEVER SQLERROR EXIT FAILURE ROLLBACK;` and `WHENEVER SQLERROR CONTINUE` → These are SQL*Plus client session-level configurations. BigQuery SQL does not run in a SQL*Plus environment. We will resolve this execution flow control by wrapping the script operations in a BigQuery scripting block (`BEGIN ... EXCEPTION ... END`). This handles failures structurally, simulating the fail-on-error behavior.
    - `COMMIT` → BigQuery auto-commits individual statements. When using explicit transaction scripts, a `COMMIT TRANSACTION` can be used. In this non-transactional staging refresh context, explicit commits are redundant but can be omitted or handled by the wrapper block.
    - `PROMPT` → SQL*Plus terminal reporting utility. Map to standard `SELECT ... AS log_message;` statements to preserve execution logging within BigQuery's execution history.

2.15 Unresolvable or Advisory Items:
    - None.

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - Wrap the execution steps inside a BigQuery Standard SQL Scripting `BEGIN ... EXCEPTION ... END` block.
    - Convert SQL*Plus `PROMPT` commands into diagnostic `SELECT` queries so that steps are written to BigQuery's query results or execution logs.
    - Convert `SYSDATE` to `CURRENT_DATETIME()`.
    - Handle exception routing natively via BigQuery's built-in `EXCEPTION` block, logging and propagating errors explicitly to match the `WHENEVER SQLERROR EXIT FAILURE` logic.

3.2 List any assumptions made during conversion:
    - Assumed the `processed_at` column is of type `DATETIME` in BigQuery to host the converted `SYSDATE` values.
    - Assumed target and source tables exist in the same dataset scope or will be executed within a default dataset configuration.

3.3 List any items flagged for human review before the build stage proceeds:
    - Validate whether target table `sof$ta_all_types` needs to be partitioned or clustered to optimize BigQuery pricing and scan limits.

═══════════════════════════════════════════
MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| `WHENEVER SQLERROR` | BigQuery Scripting `BEGIN...EXCEPTION` Block | Standard Unhandled SQL Script | BigQuery scripting exception blocks allow structured handling of failures, matching SQL*Plus abort behavior. |
| `PROMPT` | `SELECT '...' AS log_msg` | Remove entirely | Preserves observability and logging inside the BigQuery execution console without external workflow engines. |
| `TRUNCATE TABLE` | Direct BigQuery `TRUNCATE TABLE` | `DELETE FROM` | `TRUNCATE` is supported natively in BigQuery, is faster, and does not incur DML cost based on rows scanned. |
| `SYSDATE` | `CURRENT_DATETIME()` | `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` | `SYSDATE` in Oracle contains time; `CURRENT_DATETIME()` preserves time without requiring a specific UTC timezone context. |
| `COMMIT` | Omitted / Implicit commit | `COMMIT TRANSACTION` | BigQuery auto-commits statements outside explicit multi-statement transaction blocks. |

═══════════════════════════════════════════
REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **BigQuery SQL Script (.sql)**: Contains the scripting logic, target logging, staging truncation, and loading process within a structured error handling block.

═══════════════════════════════════════════
DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column / Construct | Oracle Type | BigQuery Type | Conversion Rule | Warning / Assessment |
| :--- | :--- | :--- | :--- | :--- |
| `all_types_id` | `NUMBER` / `VARCHAR2` | `INT64` / `STRING` | Direct mapping | Confirm the key schema from source to ensure type safety. |
| `source_system` | `VARCHAR2` | `STRING` | Direct mapping | Fully compatible. |
| `status` | `VARCHAR2` | `STRING` | Direct mapping | Fully compatible. |
| `SYSDATE` | `DATE` | `DATETIME` | Map to `CURRENT_DATETIME()` | Assumes time zone handling relies on default system-level local settings. |

═══════════════════════════════════════════
DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: SQL*Plus session directives, system timestamping, multi-statement DDL/DML sequence.
- **Unsupported Functions**: None (SQL*Plus features resolved via BQ Scripting equivalents).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Tables `sof$ta_all_types`, `cds$ta_all_types_raw`.
- **Assumptions**: The environment utilizes dataset-scoped execution or appropriate dataset paths will be appended.
- **Warnings**: Ensure schema constraints on the target table are handled externally (BigQuery does not enforce constraints).

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

═══════════════════════════════════════════
ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `WHENEVER SQLERROR` | Direct-with-rewrite | Scripting block: `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` |
| `PROMPT` | Direct-with-rewrite | `SELECT 'string_message' AS log_message;` |
| `TRUNCATE TABLE` | Direct | `TRUNCATE TABLE` |
| `INSERT INTO` | Direct | `INSERT INTO` |
| `SYSDATE` | Direct-with-rewrite | `CURRENT_DATETIME()` |
| `COMMIT` | Direct-with-rewrite | Omitted (handled natively by transaction auto-commit) |

No PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Enclosing procedure/script block simulating transactional protection and tracking
BEGIN

  -- Log progress of truncate step
  -- Converted from: prompt tabelle von vorherigem lauf loeschen
  SELECT 'tabelle von vorherigem lauf loeschen' AS execution_step;

  -- Clear staging target table
  TRUNCATE TABLE sof$ta_all_types;

  -- Log progress of insert step
  -- Converted from: prompt zieltabelle befuellen
  SELECT 'zieltabelle befuellen' AS execution_step;

  -- Populating the target staging table with qualified raw data
  INSERT INTO sof$ta_all_types (
    all_types_id,
    source_system,
    processed_at
  )
  SELECT
    r.all_types_id,
    r.source_system,
    CURRENT_DATETIME() -- Converted from SYSDATE
  FROM
    cds$ta_all_types_raw AS r
  WHERE
    r.status = 'READY';

  -- Log successful completion
  -- Converted from: prompt Verarbeitung fehlerfrei beendet.
  SELECT 'Verarbeitung fehlerfrei beendet.' AS execution_status;

EXCEPTION WHEN ERROR THEN
  -- Implements equivalent of WHENEVER SQLERROR EXIT FAILURE
  SELECT 
    CONCAT(
      'Script Execution Failed. Error: ', @@error.message, 
      ' | Code: ', CAST(@@error.code AS STRING), 
      ' | Statement: ', @@error.statement_text
    ) AS error_diagnostic;
  
  -- Re-throw exception to guarantee the process fails the orchestrator run
  RAISE USING MESSAGE = CONCAT('ALL_TYPES_MASTER Job Step Failed: ', @@error.message);
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Target Table Schema Mapping**: Ensure `processed_at` column in `sof$ta_all_types` is declared as `DATETIME` or `TIMESTAMP` in BigQuery. If standardizing to UTC, we must change `CURRENT_DATETIME()` to `CURRENT_TIMESTAMP()`.
2. **Execution Context**: Confirm that target dataset boundaries (e.g., `project_id.dataset_id.sof$ta_all_types`) are correctly resolved in the runner configuration.

### Job dependencies
- **Upstream Predecessors (already migrated & merged)**:
  - `TMD_processing/ALL_TYPES/mp/all_types_graph.mp` (Graph logic - PR #883)
  - `TMD_processing/ALL_TYPES/run/all_types_graph.ksh` (Wrapper script - PR #884)
- **Downstream**: No downstream jobs or consumers are declared in the job dependencies metadata.
- **Wiring on Target Platform**: The target orchestrator (Cloud Composer / Airflow DAG) will execute the BigQuery SQL transformation task representing this step downstream of the already-migrated Ab Initio graph components.

### Execution order
The target Cloud Composer orchestration DAG must preserve the sequence established in the legacy dependency graph:
1. **DWH_ALL_TYPES_JOB/DW.DWH_ALL_TYPES_MASTER.xml** -> Mapped to the main Cloud Composer DAG orchestrator.
2. **isall/abinitio/cfg/all_types/all_types_graph.cfg** -> Mapped to Composer environment variables or DAG parameter configurations.
3. **isall/aufbereitung/bin/r_all_types_master.ksh** -> Mapped to a Python operator wrapper in the DAG.
4. **.dw_init** -> Mapped to initialization steps in the Composer DAG.
5. **isall/aufbereitung/awk/k_all_types_transform.awk** -> Mapped to its converted BQSQL/Python task in the DAG (handled by a separate design pass).
6. **isall/aufbereitung/sql/d_all_types.sql** -> Mapped to a `BigQueryInsertJobOperator` task executing `isall/aufbereitung/sql/d_all_types.sql` (designed in this pass).

### Lineage
- **Upstream Producer Table**: `TABLE:CDS$TA_ALL_TYPES_RAW` (read via SELECT)
- **Downstream Consumer Table**: `TABLE:SOF$TA_ALL_TYPES` (written via TRUNCATE and INSERT)

### Cross-file dependencies
- **Shared Tables**: 
  - `CDS$TA_ALL_TYPES_RAW`: Populated by the preceding AWK transformation step (`isall/aufbereitung/awk/k_all_types_transform.awk`) and read by this SQL script.
  - `SOF$TA_ALL_TYPES`: Populated by this script as the final state staging/target table.

### Target file plan
- **Target File Path**: `isall/aufbereitung/sql/d_all_types.sql`
  - **Language**: BigQuery Standard SQL
  - **Source File**: `isall/aufbereitung/sql/d_all_types.sql`

### Environment-specific values
- **GCP_PROJECT** (GLOBAL): Identifies the target Google Cloud project. Sourced at runtime via the Airflow connection or execution environment settings.
- **BQ_DATASET** (GLOBAL): Identifies the target BigQuery dataset where `sof$ta_all_types` and `cds$ta_all_types_raw` reside. Sourced at runtime via execution environment configuration.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isall/aufbereitung/sql/d_all_types.sql` | `isall/aufbereitung/sql/d_all_types.sql` | Re-written to BigQuery Standard SQL with a `BEGIN ... EXCEPTION` block to handle transactional auto-commits and error propagation natively in BigQuery. |

### Risks & Manual Actions
- **Schema Validation**: Verify that the columns (`all_types_id`, `source_system`, `processed_at`) on the target BigQuery tables are aligned. Specifically, `processed_at` must be defined as `DATETIME` or `TIMESTAMP` (depending on local vs. UTC storage standard).
- **German Logging Output**: Under the OUTPUT/PRINT LITERAL RULE, all German literal text from the original SQL*Plus prompts must be maintained character-for-character within the logged output statements in BigQuery (specifically: `'tabelle von vorherigem lauf loeschen'`, `'zieltabelle befuellen'`, and `'Verarbeitung fehlerfrei beendet.'`).