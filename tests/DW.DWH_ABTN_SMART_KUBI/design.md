=== OBJECT: DW.DWH_ABTN_SMART_KUBI (JOBS_UNIX) ===
active=1
title=Populate temp table
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=2838
launcher_type=sql_script
launcher_details={'job_arg': 'ABTN_SMART_KUBI', 'sql_path': '$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='ABTN_SMART_KUBI'
. $HOME/.dw_init

!***************************************
:set &cdate  = SYS_DATE("YYYYMMDD")
:set &cmonth = SUBSTR(&cdate,1,6)
:set &cday   = SUBSTR(&cdate,7,2)

:if  &cday  < '15'
:     set &first = '01'
:     set &cmonth = "&cmonth&first"
:     set &cmonth = SUB_DAYS(&cmonth,1)
:     set &cmonth = SUBSTR(&cmonth,1,6)
:endif

:set &MONATSID = &cmonth
!***************************************

:print Berichtsmonat:  &MONATSID

$HOME/aktuell/allgemein/is/util/bin/r_sqlscript -j ABTN_SMART_KUBI -f $HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql -i &MONATSID
!$HOME/aktuell/allgemein/is/util/bin/r_sqlscript -j ABTN_SMART_KUBI -f $HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql -i 201707


:inc DW.LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

---

## 1. Overview
This migration document details the conversion of the UC4 job **`DW.DWH_ABTN_SMART_KUBI`** into an equivalent Apache Airflow pipeline. 
This job executes a SQL script (`d_abtn_x_smart_kubi.sql`) to populate a temporary table. The job dynamically calculates a reporting month parameter (`MONATSID`) based on the execution date: if the day of execution is before the 15th, it targets the previous month; otherwise, it targets the current month. 
Because this job was supplied as a standalone native UNIX job without an accompanying JOBP parent workflow or SCRI triggers, it is structured as a standalone Airflow DAG.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | `1` (Active) | Populate temp table |

---

## 3. Scheduling
* **Schedule Rule**: No `EVNT_TIME` or schedule metadata is present in this extraction bundle.
* **Trigger Mechanism**: Externally triggered (source unknown from this extraction alone).
* **Airflow Schedule**: `schedule=None` (manual or external trigger).

---

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_abtn_smart_kubi_task` | `DW.DWH_ABTN_SMART_KUBI` | `BashOperator` | `gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py` | Pass computed `MONATSID` as command-line parameter | 1 | 5 min | N/A | N/A | N/A | N/A | Translates SQL launcher script to equivalent Python script executing in GCP |

---

## 6. Task Dependency Map
Since this DAG contains only a single task, the execution flow is:

```
dw_dwh_abtn_smart_kubi_task
```

---

## 7. Sync / Concurrency Analysis
No sync keys, resource locks, or cross-dependencies (`sync_rows`) were defined on this object. Standard concurrent run safety is maintained by setting `max_active_runs=1` on the DAG level.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Standard single-retry logic is applied (1 retry, 5-minute interval).
* **Error Handling**: No special postcondition actions or alert objects were defined in the UC4 source script. Standard Airflow failures will propagate naturally.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&MONATSID` | Calculated dynamically via date logic | `{{ user_defined_macros.get_monatsid(logical_date) }}` passed to execution task |
| `&DWH_JOB_KENNUNG` | `'ABTN_SMART_KUBI'` | Standard environment variable or metadata tag |

---

## 10. Developer Notes
* **Dynamic Parameter Calculation**: The dynamic variable `&MONATSID` is calculated based on the day of execution. In Airflow, this is replicated using a Python macro function executing within the context of the running DAG run (`logical_date` / `execution_date`).
* **Execution Script**: The SQL execution utility (`r_sqlscript`) is migrated to run as a Python runner executing the converted SQL logic (`d_abtn_x_smart_kubi.py`) located on Google Cloud Storage.
* # REVIEW: Confirm whether this job needs to be triggered by an external scheduler or if it should be integrated into a larger parent pipeline not included in this migration bundle.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── GCP Configuration ────────────────────────────────────
# Placeholder bucket for Python migration runners
GCS_BUCKET = "YOUR_BUCKET_NAME"
PYTHON_SCRIPT_PATH = f"gs://{GCS_BUCKET}/python_scripts/d_abtn_x_smart_kubi.py"

# ── Dynamic Parameter Logic (UC4 Date Mapping) ───────────
def get_monatsid(logical_date):
    """
    Replicates UC4 Date parsing:
    If execution day is < 15, use the previous month (YYYYMM format).
    Otherwise, use the current month (YYYYMM format).
    """
    if logical_date.day < 15:
        # Subtract one month
        first_of_this_month = logical_date.replace(day=1)
        prev_month_date = first_of_this_month - timedelta(days=1)
        return prev_month_date.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 1),
}

# ── DAG Definition ──────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    schedule=None,  # Standalone manual/externally triggered task
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "get_monatsid": get_monatsid
    },
    tags=["uc4_migration", "sql_script"],
) as dag:

    # ── Task: dw_dwh_abtn_smart_kubi_task ──────────────────
    # Runs the converted target Python execution harness
    # Pass calculated MONATSID dynamically using the jinja macro
    dw_dwh_abtn_smart_kubi_task = BashOperator(
        task_id="dw_dwh_abtn_smart_kubi_task",
        bash_command=(
            f"python3 -m google.cloud.storage cp {PYTHON_SCRIPT_PATH} ./ && "
            "python3 d_abtn_x_smart_kubi.py --monatsid {{ user_defined_macros.get_monatsid(logical_date) }}"
        ),
        env={
            "DWH_JOB_KENNUNG": "ABTN_SMART_KUBI",
            "LOGIN_USER": "DW.UNIX.ISTNS"
        }
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task pipeline: no downstream dependencies defined.
    dw_dwh_abtn_smart_kubi_task
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
|:---|:---|:---|
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `dags/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi_dag.py` | Migrates the UC4 orchestration job into an Airflow DAG that calculates the reporting month dynamically and triggers the SQL script execution runner. |

---

# Execution Order

The execution sequence from the legacy dependency graph must be preserved in the target Airflow orchestration:
1. **`DW.DWH_ABTN_SMART_KUBI.xml` (UC4 Job)**: Represented by the overall Airflow DAG lifecycle, defining execution metadata, setting environment variables, and calculating execution-time date variables.
2. **`.dw_init` (Environment Initialization)**: Maps to a common Python module `dw_init.py` that is imported at runtime to configure environment paths and parameters.
3. **`d_abtn_x_smart_kubi.sql` (Database PL/SQL)**: Maps to a BigQuery SQL script or compiled Dataform SQLX pipeline.
4. **`r_sqlscript` (Shell execution wrapper)**: Maps to the Python runner `r_sqlscript.py` which executes the target SQL query on BigQuery.
5. **`f_alis_msgerr.ksh` (Error messaging script)**: Maps to a shared Python module `f_alis_msgerr.py` imported by `r_sqlscript.py` to handle logging and alerts.
6. **`h_alis_sqlplus.ksh` (SQL execution engine)**: Maps to a shared Python helper module `h_alis_sqlplus.py` imported by `r_sqlscript.py` to manage database client operations.

---

# Scheduling

* **Orchestration Event**: The UC4 job does not define any active scheduled events (`EVNT_TIME`) in this XML export. It is historically triggered manually or as part of a larger parent workflow (e.g. `DW.DWH_SMART_KUBI_ZUGANG_MONATLICH_JP`).
* **Target Scheduling**: Replicated as a standalone Airflow DAG with `schedule=None`. It is intended to be triggered manually, through the Airflow REST API, or by an upstream orchestration DAG utilizing an `ExternalTaskMarker` / `ExternalTaskSensor` or a `TriggerDagRunOperator`.

---

# Schedule & Variables

### Scheduler-Set Variables
The variables defined and used in the legacy scheduler script are mapped as follows:
* **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`
  * **Airflow Mechanism**: Set as a task-level environment variable (`env={"DWH_JOB_KENNUNG": "ABTN_SMART_KUBI"}`) or a runtime task parameter.
* **`cdate`**, **`cmonth`**, **`cday`**, **`first`**, and **`MONATSID`** (dynamic date variables)
  * **Airflow Mechanism**: Handled dynamically using a Python function wrapped as a Jinja macro in the Airflow DAG context. It uses the DAG's `logical_date` (or `execution_date`) to calculate the reporting month (`MONATSID`) at run-time:
    * If `logical_date.day` is less than 15, the variable `MONATSID` is calculated as the previous month in `YYYYMM` format.
    * Otherwise, `MONATSID` is calculated as the current month in `YYYYMM` format.
    * The calculated string is passed into the python execution runner command line: `{{ user_defined_macros.get_monatsid(logical_date) }}`.

---

# Lineage

* **Upstream Inclusions / Dependencies**:
  * `DW.HOLE_PFAD` (Unresolved shell include, marked as "NO SOURCE NEEDED" by human-reviewed resolution).
  * `DW.LESE_LOG` (Unresolved shell include, marked as "NO SOURCE NEEDED" by human-reviewed resolution).
  * `.dw_init` (Sourced environment initialization script) -> Maps to python-migrated module `dw_init.py` loaded at runtime.
* **Invocations**:
  * `r_sqlscript` (UNIX shell wrapper script) -> Maps to python-migrated script `r_sqlscript.py`.
  * `d_abtn_x_smart_kubi.sql` (Target PL/SQL file) -> Maps to BigQuery SQL query executed via `r_sqlscript.py`.
* **Platform Execution Host**:
  * Legacy execution on host `dwhdwh1p` -> Maps to Google Cloud Composer GKE environment.
* **Package Context**:
  * Legacy login using `DW.UNIX.ISTNS` -> Maps to the standard GCP IAM service account credential running the Cloud Composer DAG.

---

# External System Replacements

* **Oracle Database (`sqlplus`)**: Replaced completely by Google Cloud **BigQuery**.
* **UNIX Host (`dwhdwh1p` / `$HOME`)**: Replaced by Google **Cloud Storage (GCS)** for storing migrated files and assets, and **Cloud Composer (Airflow)** for execution.

---

# Cross-File Dependencies

* **`r_sqlscript.py`**: The central runner script executed by this DAG task. It imports and depends directly on:
  * `f_alis_msgerr.py` (Error handling and messaging)
  * `h_alis_sqlplus.py` (SQL compilation and BigQuery interaction helper)
  * `dw_init.py` (Environment variables and initialization)
* **`d_abtn_x_smart_kubi.sql`**: The query references the following tables and views in the database:
  * `DWH$TA_F_D1_TWVV_TN` (Source table)
  * `DWH$TA_T_SMART_KUBI` (Target table truncated and populated)
  * `BL_D_TARIF` (Source dimension table)
  * `DWH$VI_L_MAP_FA_TARIF` (Source mapping view)

---

# Target File Plan

* **Target File**: `dags/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi_dag.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`

---

# Environment-Specific Values

* **`GCS_BUCKET`** (GLOBAL): The shared GCS bucket containing the python scripts, configurations, and logs. Sourced via Airflow Variable: `Variable.get("GCS_BUCKET")`.
* **`GCP_PROJECT`** (GLOBAL): The Google Cloud Project ID. Sourced via Airflow Variable: `Variable.get("GCP_PROJECT")`.
* **`BQ_DATASET`** (GLOBAL): The target BigQuery dataset containing the source and target tables. Sourced via Airflow Variable: `Variable.get("BQ_DATASET")`.
* **`DWH_JOB_KENNUNG`** (JOB-SPECIFIC): Set to `'ABTN_SMART_KUBI'` in the DAG task environment.
* **`LOGIN_USER`** (JOB-SPECIFIC): Set to `'DW.UNIX.ISTNS'` in the DAG task environment.

---

# Risks & Manual Steps

* **Reviewer Feedback Alignment**: The design of the Airflow DAG task must invoke `r_sqlscript.py` ensuring that `r_sqlscript.py` dynamically imports and utilizes `f_alis_msgerr.py`, `h_alis_sqlplus.py`, and `dw_init.py` instead of implementing separate stubs or duplicate logic. Additionally, `h_alis_sqlplus.py` must use the native BigQuery Python client instead of invoking local `sqlplus` binary, establishing a consistent BigQuery execution pipeline across all jobs.
* **Human-Confirmed Resolutions**: `DW.HOLE_PFAD` and `DW.LESE_LOG` are confirmed by manual review to be unnecessary in the cloud environment and have been excluded from migration plans. No Python stubs or imports are required for these.
* **Literal Log Text**: As per the Output/Print Literal Rule, the original German logging text `Berichtsmonat:` printed during the execution of the UC4 script must be retained character-for-character within the Airflow task execution log output:
  `print(f"Berichtsmonat: {monats_id}")`
* **Dynamic Parameter Validation**: Ensure that the Python-implemented macro for calculating `MONATSID` is thoroughly tested for month-end and leap-year boundaries to ensure the date logic matches the legacy UC4 scheduler behavior exactly.

---

=== FILE: local/home/gurunathan_t/kubi/.dw_init ===
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
REASON: The script is a standard environment initialization file (.dw_init) that only declares environment variables and sources other configuration files, with no executable business logic.

EVIDENCE
- Business logic found: none — the script only sets environment directory paths and sources other environment setup scripts.
- AWK: none
- SQL-expressible: no (the script contains only directory mapping variables and environment definitions, not query or transformation logic).
- Non-SQL side effects: none (it only populates environment variables for the shell session).
- Against this verdict: An argument could be made to convert it into a Python module containing a dictionary of variables, but since environment variables set in child Python processes cannot propagate back to a calling shell, converting it to Python is an anti-pattern; it is best left as a standard environment configuration file (.env, ConfigMap, or native orchestration variables).

ORCHESTRATION SUMMARY
- Purpose: Set environment variables and path mappings for the Information Services data warehouse environment, determining the locations of cubes, log files, interface import/export directories, and Oracle homes.
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
  - `DW_DIR_IMP_SAP_L` = `$HOME/daten/sap/sap_l_gutgr`
  - `DW_DIR_IMP_L_MAHNSTYP_IST` = `$HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_FI` = `$HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_IST` = `$HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_GUTGR` = `$HOME/daten/sd/l_gutschr`
  - `DW_DIR_IMP_L_LEIST` = `$HOME/daten/sd/l_leist`
  - `DW_DIR_IMP_L_PROD` = `$HOME/daten/sd/l_prod`
  - `DW_DIR_IMP_LKODE` = `$HOME/daten/sd/lkode`
  - `DW_DIR_IMP_SUBSE` = `$HOME/daten/subse`
  - `DW_DIR_SMS_PRG` = `${HOME}/aktuell/allgemein/is/util`
  - `DW_DIR_SMS_ADR` = `${HOME}/daten/sms/adressen`
  - `DW_DIR_SMS_TMP` = `${HOME}/daten/sms/tmp`
  - `DW_DIR_IMP_DPPS` = `$HOME/daten/dpps`
  - `DW_DIR_IMP_PLANF2` = `$HOME/daten/planf2`
  - `DW_HOST_CUSTOMER` = `dxcst3.bn.detemobil.de`
  - `ORACLE_HOME` = `/appl/local/oracle/12.2.0.1.0` or `/appl/local/oracle/11.2.0` (conditional fallback)
  - `DW_DIR_UTL_FILE` = `/appl/local/oracle/admin/$ORACLE_SID/utl_file`
- Environment files sourced:
  - `. $HOME/.dw_global`
  - `. $HOME/.dw_lokal`
- Invokes: None
- Called by: Sourced at the beginning of legacy .ksh scripts or UC4 job executions to configure the environment.
- Exit-code behaviour: Standard sequential assignment; returns control implicitly (no explicit exits).
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion. Sourced environment configurations should instead map onto target cloud environment definitions (e.g. Airflow env-vars, Kubernetes ConfigMaps, or .env files in the new orchestration environment).

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `dw_init.py` | Migrated to a Python module that initializes and exports required environment variables and GCS path mappings, replacing legacy shell-based environment exports. |

# Migration Design Document: DW.DWH_ABTN_SMART_KUBI (Environment Setup Group)

## 1. Execution Order
The legacy orchestration executes steps in the following order:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 Orchestration)
2. `d_abtn_x_smart_kubi.sql` (PL/SQL transformation)
3. `r_sqlscript` (Shell script wrapper)
4. `.dw_init` (Sourced environment initialization script)
5. `f_alis_msgerr.ksh` (Error reporting shell utility)
6. `h_alis_sqlplus.ksh` (SQL execution helper)

In the target environment (Cloud Composer / Airflow), `.dw_init` is converted to `dw_init.py`. This python module must be imported at the start of all sibling Python modules (`r_sqlscript.py`, `f_alis_msgerr.py`, `h_alis_sqlplus.py`) to resolve and initialize environment-wide path configurations, database connectivity, and logger contexts before executing any business logic.

## 2. Schedule & Variables
The legacy UC4 scheduler sets the following variables, which must be dynamically calculated and passed by Cloud Composer / Airflow:
- **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`: Passed as a DAG-level parameter or task environment variable.
- **`cdate`** = `SYS_DATE("YYYYMMDD")`: Calculated in Airflow using the DAG execution date macro `{{ ds_nodash }}`.
- **`cmonth`** = `SUBSTR(&cdate,1,6)`: Extracted year and month (YYYYMM). Derived via Airflow macro `{{ execution_date.strftime('%Y%m') }}`.
- **`cday`** = `SUBSTR(&cdate,7,2)`: Extracted day of the month. Derived via Airflow macro `{{ execution_date.strftime('%d') }}`.
- **`first`** = `'01'`: Static parameter representing the first day of the month.
- **`MONATSID`** = `&cmonth`: Mapped to the reporting month ID. This is calculated dynamically in Airflow to represent the previous month (YYYYMM): `{{ (execution_date.replace(day=1) - macros.datetime.timedelta(days=1)).strftime('%Y%m') }}`.

## 3. Lineage
- **Upstream Configuration Dependencies**:
  - `.dw_init` historically sources `.dw_global` (`$HOME/.dw_global`) and `.dw_lokal` (`$HOME/.dw_lokal`). Both components have human-confirmed resolutions of **NO SOURCE NEEDED** and are retired as standalone scripts, with their global parameters absorbed directly into GCS/Airflow config or `dw_init.py`.
- **Downstream Consumers**:
  - `r_sqlscript` (job: `DW.DWH_ABTN_SMART_KUBI`)
  - `f_alis_msgerr.ksh` (job: `DW.DWH_ABTN_SMART_KUBI`)
  - `h_alis_sqlplus.ksh` (job: `DW.DWH_ABTN_SMART_KUBI`)
  - These downstream scripts import `dw_init.py` at runtime to initialize their directories and Oracle/BigQuery transition variables.

## 4. Cross-File Dependencies
- `dw_init.py` serves as the shared configuration provider for all Python tasks inside the `DW.DWH_ABTN_SMART_KUBI` DAG. The sibling files `r_sqlscript.py`, `f_alis_msgerr.py`, and `h_alis_sqlplus.py` (which are owned by separate design passes) depend directly on `dw_init.py` to fetch log directory paths (`DW_DIR_PROT`) and resolve storage mappings.

## 5. Target File Plan
- **Target File Path**: `dw_init.py`
  - **Language**: Python
  - **Source File**: `local/home/gurunathan_t/kubi/.dw_init`
  - **Purpose**: Establishes directory environment variables using Python's `os.environ` and maps legacy home folders to equivalent Google Cloud Storage (GCS) paths.

## 6. Environment-Specific Values
All directory paths and configuration details from `.dw_init` are classified as environment-wide **GLOBAL** variables because they describe the target architecture and must be consistent across dev, test, and prod:
- **`GCP_PROJECT`**: Global infrastructure constant. Sourced via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`.
- **`GCS_BUCKET`**: Global storage bucket representing the equivalent of `$HOME/daten`. Sourced via `os.environ.get("GCS_BUCKET")`.
- **Directory Mappings**: Mapped dynamically relative to the `GCS_BUCKET` base path. Examples:
  - `DW_DIR_PROT` = `f"gs://{os.environ.get('GCS_BUCKET')}/daten/logfiles"`
  - `DW_DIR_CUBES` = `f"gs://{os.environ.get('GCS_BUCKET')}/daten/cubes"`
  - `DW_DIR_IMP_D1` = `f"gs://{os.environ.get('GCS_BUCKET')}/daten/d1"`
  - `DW_DIR_IMP_BWA` = `f"gs://{os.environ.get('GCS_BUCKET')}/daten/dpps/bwa"`
- **`DW_HOST_CUSTOMER`**: Global hostname. Mapped to `os.environ.get("DW_HOST_CUSTOMER")` or an Airflow variable.
- **`ORACLE_HOME`**: Mapped to `os.environ.get("ORACLE_HOME")`. As the database transitions to BigQuery, actual Oracle Home configurations are kept as dummy/no-op values for compatibility during co-existence phases.
- **`DW_DIR_UTL_FILE`**: Mapped to GCS bucket location `f"gs://{os.environ.get('GCS_BUCKET')}/utl_file"`.

*Strict Rule Enforcement*: No placeholder literals such as `"your-production-bucket"` or `"CHANGE_ME"` are allowed in the code; all environment variables fallback to standard configuration values or raise a descriptive environment exception if missing.

## 7. Risks & Manual Steps
- **Strict Adherence to Output/Print Literal Rule**: The original `.dw_init` contains German echo warnings:
  - `"Fehler in .dw_init:"`
  - `"   Konnte ORACLE_HOME nicht setzen !"`
  These literal string outputs must be maintained exactly character-for-character within the error logging block of the migrated `dw_init.py` python script.
- **Shared Python Integration**: Sibling modules like `r_sqlscript.py` and `h_alis_sqlplus.py` must import `dw_init.py` rather than re-declaring local environment variables. Ensure the deployment pipeline packages `dw_init.py` in the same Python PYTHONPATH or DAGs folder so it is visible to the import statements of sibling tasks.
- **Oracle Path Deprecation**: Sibling components still searching for local file structures or relying on standard Oracle `sqlplus` commands must be aligned to use the BigQuery client and GCS pathways defined in this design module. Any attempt to write to local directories like `DW_DIR_UTL_FILE` should be redirected to GCS.

---

=== FILE: local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql ===
-- Discription :Aggregation Job to load data into DWH$TA_T_SMART_KUBI table
-- Erstellt  : Ankita Suvarna
-- Datum     : 18.09.2015
-- Language  : PL/SQL
-- Version   : 16.1.0.
------------------------------------------------------
WHENEVER oserror EXIT failure;
WHENEVER sqlerror EXIT failure; 
---
SET timing ON;
SET serveroutput ON;
SET echo OFF;
DECLARE 
	v_anzahl_ds pls_integer := 0;
	l_monats_id number := to_number('&1');
	EintragsNr  number := to_number('&2');
	lv_str      varchar2(300);
	l_monats_date DATE := ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1);
BEGIN
  LV_STR:= 'Truncate table DWH$TA_T_SMART_KUBI'; 
  dwpa_util_skript.runstatement(eintragsnr, lv_str); 

INSERT 
       /*+ Append */ 
INTO   dwh$ta_t_smart_kubi 
       ( 
              monats_id, 
              kundennummer, 
              tarif_id, 
              tarif_id_alt, 
              vo_kennung, 
              test_gp, 
              anzahl, 
              kennzahl_id 
       ) 
with temp AS 
       ( 
		   SELECT
				  /*+ parallel(t,4) full(t) parallel(tar,4) full(tar) */
				  t.tarif_id,
				  t.dwh_tarif_id,
				  t.gueltig_von,
				  t.gueltig_bis,
				  tar.mp_geschaeftsfeld_id
		   FROM   dwh$vi_l_map_fa_tarif T,
				  bl_d_tarif TAR
		   WHERE  t.tarif_id = tar.tarif_id
		   AND    t.gueltig_bis = To_date('4712-12-31', 'YYYY-MM-DD')
		)
SELECT /*+ full(fact) parallel(fact,4) full(d) parallel(d,4) use_hash(t1,t2,fact,d)*/ 
         l_monats_id                                    								  AS monats_id,
         Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)                      kundennummer,
         Nvl(t_new.tarif_id,0)                                                            AS tarif_id,
         Nvl(t_old.tarif_id,0)                                                            AS tarif_id_alt,
         Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)  vo_kennung,
         d.test_gp, 
         sum(fact.zugang) AS anzahl, 
         fact.kennzahl_id 
FROM     dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1) fact, 
         temp t_new, 
         temp t_old,
         dwh$ta_c_vertrag d 
WHERE    to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id) 
AND      fact.kennzahl_id IN ('VVLREIN', 
                              'VVLTWC2C', 
                              'MIGP2CBF') 
AND      fact.dwh_tarif_id_neu  = t_new.dwh_tarif_id (+) 
AND      fact.dwh_tarif_id_alt = t_old.dwh_tarif_id (+) 
AND      fact.dwh_vertrag_id=d.dwh_vertrag_id(+) 
AND      l_monats_date > d.gueltig_von(+) 
AND      l_monats_date <= d.gueltig_bis(+) 
GROUP BY decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer), 
         nvl(t_new.tarif_id,0), 
         nvl(t_old.tarif_id,0), 
         decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb), 
         d.test_gp, 
         fact.kennzahl_id;
				  
v_anzahl_ds := SQL%ROWCOUNT;
COMMIT;
  
dbms_output.put_line(TO_CHAR(v_anzahl_ds) || ' rows inserted in DWH$TA_T_SMART_KUBI');
EXCEPTION
WHEN OTHERS THEN
  -- unbekannte bzw. nicht erwartete Exception koennen auch
  -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
  -- der Zusatzfehlertext kann vorher ermittelt werden.
  ROLLBACK;
  --
  DECLARE
    ErrText  VARCHAR2(512);
    ErrC     NUMBER;
    FehlerNr NUMBER := dwpa_globals.k_alis_err_unknown;
  BEGIN
    ErrText := SQLERRM;
    ErrC    := SQLCODE;
    dwpa_meldung.fehler ('F', EintragsNr, FehlerNr, ErrText, TO_CHAR(ErrC));
    raise_application_error(FehlerNr, ErrText);
  END;
END;
/ 

═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Script Type:
    - PL/SQL Anonymous block containing dynamic SQL (Truncate) and an aggregated INSERT statement with CTEs, LEFT OUTER JOINs, and Oracle outer join syntax `(+)`.
1.2 Business Logic Summary:
    - Truncates the target reporting table `DWH$TA_T_SMART_KUBI`.
    - Aggregates access data (`zugang`) from a partitioned source fact table (`dwh$ta_f_d1_twvv_tn`) based on target monthly partitions (`l_monats_id`).
    - Maps target records against active contract details (`dwh$ta_c_vertrag`) and current/old pricing tariff dimensions (`dwh$vi_l_map_fa_tarif` and `bl_d_tarif`) based on date logic (`l_monats_date`).
    - Tracks inserted rows, commits the transaction, and logs success or manages exception rollback and custom reporting.
1.3 Entities Referenced:
    - `dwh$ta_t_smart_kubi` (Target table)
    - `dwh$vi_l_map_fa_tarif` (Source view/table, alias `t`)
    - `bl_d_tarif` (Source table, alias `tar`)
    - `dwh$ta_f_d1_twvv_tn` (Source partitioned fact table, alias `fact`)
    - `dwh$ta_c_vertrag` (Source table, alias `d`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → `INT64`
    - `NUMBER` → `INT64` (for identifiers `l_monats_id`, `EintragsNr`)
    - `VARCHAR2(300)` / `VARCHAR2(512)` → `STRING`
    - `DATE` → `DATE` (No time-of-day component is processed; date precision suffices)

2.2 Implicit and Explicit Type Casting:
    - `to_char(fact.gueltigkeitszeitpunkt,'yyyymm') = to_char(l_monats_id)`
      In BigQuery, performing function transformations on partition columns breaks partition-pruning. We will resolve this to a range query using the target month boundaries to ensure type safety and peak processing performance:
      `fact.gueltigkeitszeitpunkt >= l_monats_start_date AND fact.gueltigkeitszeitpunkt < l_monats_end_date`

2.3 NULL Handling and Conditional Functions:
    - `Nvl(t_new.tarif_id,0)` → `COALESCE(t_new.tarif_id, 0)`
    - `Nvl(t_old.tarif_id,0)` → `COALESCE(t_old.tarif_id, 0)`
    - `Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)`
      → `CASE t_new.mp_geschaeftsfeld_id WHEN 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)`
      → `CASE WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn ELSE fact.vo_kenn_bearb END`

2.4 String Functions:
    - `ltrim(rtrim(col))` → `TRIM(col)`
    - `TO_CHAR(v_anzahl_ds)` → `CAST(v_anzahl_ds AS STRING)`

2.5 Date and Timestamp Functions:
    - `TO_DATE('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`
    - `TO_DATE(l_monats_id, 'YYYYMM')` → `PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`
    - `ADD_MONTHS(..., 1)` → `DATE_ADD(..., INTERVAL 1 MONTH)`

2.6-2.10 (Not directly applicable / no sequences / no model clauses found)

2.11 MERGE Statements:
    - Not applicable.

2.12 INSERT / UPDATE / DELETE:
    - Dynamic truncation of `DWH$TA_T_SMART_KUBI` will be handled as an explicit standard `TRUNCATE TABLE` DML script execution.

2.13 DDL Constructs:
    - The target table partition scheme is assumed to be managed within BigQuery. Partition referencing in the FROM clause `partition(dwh$ta_f_d1_twvv_tn_&1)` is replaced by direct table queries filtered on partition boundary limits.

2.14 PL/SQL:
    - `DECLARE ... BEGIN ... EXCEPTION ... END` anonymous PL/SQL block is converted to a native BigQuery SQL scripting block containing `DECLARE`, `BEGIN...EXCEPTION...END` flow control.
    - `SQL%ROWCOUNT` → `@@row_count` system variable.
    - `COMMIT` / `ROLLBACK` → Transaction markers `COMMIT TRANSACTION` and `ROLLBACK TRANSACTION`. Note that transactions must start with `BEGIN TRANSACTION`.

2.15 Unresolvable or Advisory Items:
    - Oracle optimizer hints `/*+ parallel */`, `/*+ Append */` are completely stripped.
    - Custom log utility calls `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` are native schema packages and are replaced with procedural placeholders/logging SELECTs for human review.

Step 3: Conversion Strategy Summary
3.1 Conversion Approach:
    - Standard BigQuery SQL Scripting block using structural transaction handling (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION`).
3.2 Assumptions:
    - Variables `&1` (Month ID) and `&2` (Log Entry Number) are represented as SQL script parameters.
    - Input month `l_monats_id` is numeric/integer formatted as `YYYYMM` (e.g., 201509).

2.16 MIGRATION DECISION MATRIX

| Source Object / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| **Anonymous Procedural Block** | BigQuery Scripting | Python Wrapper | BigQuery scripting natively supports transaction management, exception blocks, variables, and procedural logic execution inside SQL. |
| **Dynamic Table Truncate** | Native SQL `TRUNCATE` | Python Script | Executing a direct `TRUNCATE TABLE` statement in the script is cleaner and matches the performance profile of custom dynamic truncation utilities. |
| **Oracle Partition References** | Standard Column Filters | Dynamic SQL Queries | Querying partition subsets using native boundaries (`>=` and `<`) on the partition column allows BigQuery's optimizer to perform automatic partition pruning without hardcoded metadata names. |
| **Custom Logging Packages** | SQL/Audit Logging Placeholder | Python Orchestration | Proprietary procedural logging frameworks (`dwpa_meldung`) do not exist in BQ. Placeholders inside procedural blocks are sufficient for migration. |

2.17 REQUIRED ARTIFACTS

- **Artifact Name**: `d_abtn_x_smart_kubi.sql` (BigQuery SQL Script)
- **Generation Output**: A single BigQuery SQL script file (.sql) containing variables, DML statements, exception management, and transactional markers.
- **System Requirements**: Executed directly via BigQuery console, CLI (`bq query`), or scheduled in an orchestration engine (like Cloud Composer/Airflow).

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Type | BigQuery Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- |
| `PLS_INTEGER` | `INT64` | Direct numeric conversion. | No precision loss. |
| `NUMBER` (ID/Count) | `INT64` | Native integer conversion. | No decimal loss expected. |
| `VARCHAR2(300)` | `STRING` | Standard string conversion. | BigQuery standard length limits are dynamically handled. |
| `DATE` (No Time Component) | `DATE` | Map strictly to BQ `DATE`. | Confirmed through logical checks that only date parts are processed. |

2.19 DESIGN REVIEW SUMMARY

- **Patterns/Objects Found**: PL/SQL procedural orchestration, Custom schemas logging package, explicit partition scans, and dynamic table truncating.
- **Unsupported Functions**: Optimizer hints, custom PL/SQL enterprise library dependencies (`dwpa_globals`, `dwpa_meldung`).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: `dwh$ta_t_smart_kubi`, `dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh$ta_f_d1_twvv_tn`, `dwh$ta_c_vertrag`.
- **Assumptions**: Downstream scheduler supplies inputs as variable parameters `l_monats_id_param` and `eintragsnr_param`.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.20 PACKAGE ANALYSIS — not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(expression AS INT64)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', ...)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(date, INTERVAL n MONTH)` |
| `DECODE` | Direct-with-rewrite | `CASE WHEN...THEN...ELSE END` |
| `NVL` | Direct-with-rewrite | `COALESCE(x, y)` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM(x)` |
| `TO_CHAR` | Direct-with-rewrite | `CAST(x AS STRING)` or `FORMAT_DATE` / `FORMAT_DATETIME` |
| `(+)` (Join operator) | Direct-with-rewrite | `LEFT OUTER JOIN` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` system variable |
| `DBMS_OUTPUT.PUT_LINE`| Direct-with-rewrite | `SELECT FORMAT(...)` as log feedback |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- BigQuery SQL Procedural Script
-- Target: Load aggregated data into DWH$TA_T_SMART_KUBI table

-- Declare and set variables equivalent to Oracle parameter inputs
DECLARE l_monats_id_param INT64 DEFAULT 201509; -- Placeholder for '&1'
DECLARE eintragsnr_param INT64 DEFAULT 12345;   -- Placeholder for '&2'

DECLARE v_anzahl_ds INT64 DEFAULT 0; -- converted from Oracle pls_integer
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING; -- converted from Oracle varchar2(300)
DECLARE l_monats_date DATE;
DECLARE l_monats_start_date DATE;
DECLARE l_monats_end_date DATE;

-- Initialize variables
SET l_monats_id = l_monats_id_param;
SET EintragsNr = eintragsnr_param;

-- l_monats_start_date: 1st of the target month
SET l_monats_start_date = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)); -- converted from TO_DATE

-- l_monats_date: Calculated as 1st of the month following the target month
SET l_monats_date = DATE_ADD(l_monats_start_date, INTERVAL 1 MONTH); -- converted from ADD_MONTHS

-- l_monats_end_date: Upper range limit for date filtering
SET l_monats_end_date = DATE_ADD(l_monats_start_date, INTERVAL 1 MONTH);

BEGIN
  -- Handle dynamic truncate table
  -- converted from dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')
  TRUNCATE TABLE dwh$ta_t_smart_kubi;

  -- Start transaction block
  BEGIN TRANSACTION;

  -- Aggregation query using standard CTEs and ANSI-compliant Joins
  INSERT INTO dwh$ta_t_smart_kubi 
         ( 
                monats_id, 
                kundennummer, 
                tarif_id, 
                tarif_id_alt, 
                vo_kennung, 
                test_gp, 
                anzahl, 
                kennzahl_id 
         )
  WITH temp AS 
         ( 
             SELECT
                    t.tarif_id,
                    t.dwh_tarif_id,
                    t.gueltig_von,
                    t.gueltig_bis,
                    tar.mp_geschaeftsfeld_id
             FROM   dwh$vi_l_map_fa_tarif T
             INNER JOIN bl_d_tarif TAR
                     ON t.tarif_id = tar.tarif_id
             WHERE  t.gueltig_bis = DATE '4712-12-31' -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
          )
  SELECT 
           l_monats_id                                                                          AS monats_id,
           CASE t_new.mp_geschaeftsfeld_id WHEN 2 THEN '-1' ELSE d.t_mobile_kundennummer END    AS kundennummer, -- converted from DECODE
           COALESCE(t_new.tarif_id, 0)                                                          AS tarif_id, -- converted from NVL
           COALESCE(t_old.tarif_id, 0)                                                          AS tarif_id_alt, -- converted from NVL
           CASE 
              WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
              WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
              ELSE fact.vo_kenn_bearb 
           END                                                                                  AS vo_kennung, -- converted from DECODE + LTRIM/RTRIM
           d.test_gp, 
           SUM(fact.zugang)                                                                     AS anzahl, 
           fact.kennzahl_id 
  FROM     dwh$ta_f_d1_twvv_tn fact -- Partition reference partition(dwh$ta_f_d1_twvv_tn_&1) removed
  LEFT OUTER JOIN temp t_new -- converted from Oracle (+) syntax
               ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp t_old -- converted from Oracle (+) syntax
               ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh$ta_c_vertrag d -- converted from Oracle (+) syntax
               ON fact.dwh_vertrag_id = d.dwh_vertrag_id 
              AND l_monats_date > CAST(d.gueltig_von AS DATE) 
              AND l_monats_date <= CAST(d.gueltig_bis AS DATE) 
  WHERE    fact.gueltigkeitszeitpunkt >= l_monats_start_date -- Optimized comparison instead of TO_CHAR formatting
  AND      fact.gueltigkeitszeitpunkt < l_monats_end_date     -- Ensures partition pruning on BigQuery
  AND      fact.kennzahl_id IN ('VVLREIN', 
                                'VVLTWC2C', 
                                'MIGP2CBF') 
  GROUP BY CASE t_new.mp_geschaeftsfeld_id WHEN 2 THEN '-1' ELSE d.t_mobile_kundennummer END, 
           COALESCE(t_new.tarif_id, 0), 
           COALESCE(t_old.tarif_id, 0), 
           CASE 
              WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
              WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
              ELSE fact.vo_kenn_bearb 
           END, 
           d.test_gp, 
           fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count; -- converted from SQL%ROWCOUNT
  
  COMMIT TRANSACTION;

  -- Log action output
  -- converted from dbms_output.put_line(TO_CHAR(v_anzahl_ds) || ' rows inserted...')
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Handle system errors and capture failure messages
  ROLLBACK TRANSACTION;
  
  DECLARE ErrText STRING;
  DECLARE ErrC STRING;
  -- Oracle system variables SQLERRM/SQLCODE resolution
  SET ErrText = @@error.message;
  SET ErrC = CAST(@@error.code AS STRING);

  -- Log Exception Placeholder
  -- converted from dwpa_meldung.fehler custom package
  SELECT FORMAT('EXCEPTION DETECTED. Error Code: %s, Message: %s', ErrC, ErrText) AS execution_error_log;
  
  -- Re-throw exception back to the orchestrator
  RAISE USING MESSAGE = ErrText;
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════

1. **Custom DB Logging Packages (`dwpa_meldung` & `dwpa_globals`)**:
   - The Oracle script utilizes custom package references `dwpa_meldung.fehler` and standard error parameters `dwpa_globals.k_alis_err_unknown` inside the `EXCEPTION` handler.
   - *Resolution Action*: In the converted BQ script, these have been replaced with standard scripting error variables (`@@error.message` / `@@error.code`) and standard logging placeholder queries. Review structural integration with the targeted enterprise migration logging table or alerting frameworks.
2. **Oracle Partition Override Suffix**:
   - The Oracle code hardcodes partition access using `dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1)`.
   - *Resolution Action*: This is resolved in BigQuery by removing the hardcoded partition access and executing a range-based date query (`fact.gueltigkeitszeitpunkt >= l_monats_start_date AND fact.gueltigkeitszeitpunkt < l_monats_end_date`). Verify that `gueltigkeitszeitpunkt` is configured as the partition key on `dwh$ta_f_d1_twvv_tn` to guarantee performance and cost control.

# File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | Migrated from Oracle PL/SQL scripting block to a native BigQuery SQL scripting block. |

***

# ADD CONTEXT THE MCP COULD NOT SEE

## Execution Order
The execution order defined in the legacy dependency graph must be preserved in the target orchestration (e.g., Cloud Composer / Airflow):
1. **UC4 Job Triggering**: `DW.DWH_ABTN_SMART_KUBI.xml` is the orchestration entry point. In the target environment, this maps to an Airflow DAG or task group.
2. **Variable Calculations**: The orchestration layer calculates the reporting month variables (see *Schedule & Variables*).
3. **Execution Wrapper**: The script `r_sqlscript` is invoked (via `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh` helper utilities) to launch the SQL script execution.
4. **SQL Execution**: The target SQL script `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` executes on BigQuery using the computed variables passed as scripting parameters.

## Scheduling
* **Trigger Event / Schedulers**: This job is triggered as a UC4 UNIX job. In Cloud Composer, this will be scheduled via a standard Airflow DAG schedule interval or an event-based trigger sensor depending on upstream dataset availability.
* **Target Mapping**: The legacy cron or event triggers must be converted to an Airflow cron schedule or `Dataset` triggers in Composer.

## Schedule & Variables — Must Be Retained
The scheduler-set variables must be calculated dynamically at runtime by the Airflow DAG and passed as arguments or query parameters to the BigQuery SQL script. 
The calculation logic for `MONATSID` (representing parameter `&1`) is as follows:
* `cdate` = Current system date in `YYYYMMDD` format (e.g., via Airflow macro `{{ ds_nodash }}`)
* `cmonth` = First 6 characters of `cdate` (`YYYYMM`)
* `cday` = Last 2 characters of `cdate` (`DD`)
* `first` = `'01'`
* `cmonth_temp` = Concatenation of `cmonth` and `first` (`YYYYMM01`)
* `sub_days_temp` = Subtract 1 day from `cmonth_temp` (yielding the last day of the previous month, e.g., `YYYYMMDD` where DD is 28, 30, or 31)
* `MONATSID` = First 6 characters of `sub_days_temp` (yielding the previous month as `YYYYMM`)

For example, if run on `20150918`, `MONATSID` evaluates to `201508` (August 2015). This value must be computed dynamically in Python (Airflow DAG) and passed as the first parameter (`&1`) to the SQL execution task.

## Lineage
* **Upstream Data Producers**:
  * `TABLE:DWH$VI_L_MAP_FA_TARIF` (Source mapping view)
  * `TABLE:BL_D_TARIF` (Source dimension table)
  * `TABLE:DWH$TA_F_D1_TWVV_TN` (Source fact table partitioned by month ID, e.g., suffix `_&1`)
  * `TABLE:DWH$TA_C_VERTREG` (Source contract table, referenced in lineage as `DWH$TA_C_VERTRAG` / `d`)
* **Downstream Data Consumers**:
  * `TABLE:DWH$TA_T_SMART_KUBI` (Target table written to by this script)

## Cross-File Dependencies
* **Orchestration & Utility Helpers**:
  * The execution of `d_abtn_x_smart_kubi.sql` depends on `r_sqlscript` (the wrapper shell utility) and the underlying shell libraries `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh`.
  * **Critical Reviewer Alignment**: The wrapper `r_sqlscript` and helpers are migrated to Python separately (as part of another design pass). They must be aligned to use the Google Cloud BigQuery client consistently to trigger this SQL script, rather than calling the `sqlplus` binary or implementing separate/disjointed execution mechanisms.
  * The functions from `f_alis_msgerr.py`, `h_alis_sqlplus.py`, and `dw_init.py` must be imported and utilized in the execution wrapper rather than re-implemented or stubbed out.
* **Shared Tables**:
  * `DWH$TA_T_SMART_KUBI` is truncated and then populated by this job. Any parallel jobs reading from or writing to this table must be locked or coordinated to avoid concurrency issues.

## Target File Plan
* **Target File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  * **Language**: BigQuery SQL (Scripting)
  * **Source File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`

## Environment-Specific Values
The environment-specific variables must be classified and resolved at runtime:

### Global (Environment-Wide Constants)
* **GCP_PROJECT**: The target Google Cloud Project ID hosting the datasets.
* **BQ_LOCATION**: The processing region for BigQuery (e.g., `EU` or `US`).
* **Shared Datasets**:
  * The schema prefix `DWH$` (e.g., `DWH$TA_T_SMART_KUBI`) should be normalized to the canonical BigQuery dataset ID mapping (e.g., `dwh_dataset.ta_t_smart_kubi`).

### Job-Specific
* **MONATSID (Parameter `&1`)**: Dynamically computed by the Airflow DAG using previous month logic and passed as a query parameter.
* **EintragsNr (Parameter `&2`)**: Dynamically computed/passed by the execution wrapper and orchestration logging layer.

## Risks and Manual Steps
* **Oracle Optimizer Hints**:
  * The Oracle script contains parallel and join hints: `/*+ Append */`, `/*+ parallel(t,4) full(t) parallel(tar,4) full(tar) */`, and `/*+ full(fact) parallel(fact,4) full(d) parallel(d,4) use_hash(t1,t2,fact,d)*/`. These must be stripped entirely, as BigQuery dynamically optimizes query execution. Performance should be verified on BigQuery post-migration.
* **Partition Pruning on `DWH$TA_F_D1_TWVV_TN`**:
  * The source query uses static partition references `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, this has been converted to standard date range filters: `gueltigkeitszeitpunkt >= l_monats_start_date AND gueltigkeitszeitpunkt < l_monats_end_date`. A manual step is required to verify that `gueltigkeitszeitpunkt` is designated as the partitioning column in the target BigQuery table metadata to ensure partition pruning works correctly and controls costs.
* **Custom Logging Framework (`DWPA_MELDUNG` / `DWPA_UTIL_SKRIPT`)**:
  * The PL/SQL blocks invoke `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` for transaction execution and exception reporting. Because these Oracle packages are omitted from the scope of this migration pass, the SQL script handles exceptions by raising errors back to the runner (`RAISE USING MESSAGE = ...`). The orchestration framework or wrapper `r_sqlscript.py` must capture these execution failures and log them to the target environment's logging repository.
* **Output / Print Literal Rule**:
  * Any literal text output during migration (such as log messages inside the `EXCEPTION` block or output strings) must preserve the original language and character casing (e.g., `'rows inserted in DWH$TA_T_SMART_KUBI'`) verbatim.

---

=== FILE: local/home/gurunathan_t/kubi/f_alis_msgerr.ksh ===
#! /bin/ksh
########################################################################
# Projekt         : Information Services
# Copyright       : 1997,1998 (DeTeMobil Deutsche Telekom MobilNet GmbH)
# Datei Name      : dwmsg.ksh
# Erstellt von    : Ralf Biermanns
# Erstellt am     : 
# Geaendert von   : $Author: Ralf Biermanns $
# Geaendert am    : $Date: $
# Version         : $Revision: 1.00 $
#
# HISTORY
# Zweck/Aufgabe
#     In dieser Datei werden ksh Hilfsroutinen zum Fehlermangement
#     in Information Services gesammelt. Sie dienen zur Vereinheitlichung 
#     und Vereinfachung des Fehlermanagment.
# 
#   DWMSG_Fehlerbehandlung <EintragsNr>
#     Funktion, die bei Auftreten eines Fehlers aufgerufen wird (falls
#     so konfiguriert). Sie regelt das Eintragen in der Meldungstabelle und 
#     ggf. das Anstoßen weiterer Aktionen wie Mail und Robomonbenachrichtigung
#     
#     Die Funktion ist für K-SH Skripte gedacht, die mit
#     trap <function> ERR dafür sorgen, daß bei einem aufgetretenem Fehler
#     (ReturnCode != 0) die Fehlerroutine angesprungen wird. 
#     Je nach Bedarf können einzelne Fehlercodes auch ignoriert werden.
#
#   DWMSG_SetzeStatusOk <EintragsNr>
#     Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich
#     beendet. Dies ist i.a. die letzte Anweisung im Rahmenskript
#
#   DWMSG_SetzeStatusAbbruch <EintragsNr>
#     Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
#     Dies wird von der Fehlerroutine aufgerufen.
#
#   DWMSG_ErmittleNr <VarName>
#     Funktion ermittelt durch Aufruf einer entsprechenden PL/SQl Routine
#     eine Nr, die für nachfolgende Aufrufe der Fehleroutinen benötigt wird. 
#     Diese Nummer ist eineindeutig. Die Nummer wird in der Variablen abgelegt
#     dessen Name als erster Parameter angegeben wurde
#
#   DWMSG_ErzeugeEintrag <EintragsNr> <JobKennung> <Programmname> <Logdatei>
#     Funktion erzeugt durch Aufruf einer entsprechenden PL/SQL Routine
#     einen Eintrag ind er Meldungstabelle zur Aufnahme von Fehlermeldungen
#     und weiteren Infos. Die <JobKennung> ist die Jobkennung des
#     Rahmenskriptes.
#      
#   DWMSG_MeldeFehler <EintragsNr> <Typ> <FehlerNr> [[<Zusatz1>] <Zusatz2>]
#     Funktion meldet einen Fehler durch Aufruf einer entsprechenden PL/SQL 
#     Routine. Diese regelt die Ausgabe der Meldung im entsprechendem Format
#     und den Eintrag in der Meldungstabelle (mit Eintragsnummer EintragsNr)
#     Typ kann folgende Ausprägungen haben F/E/W (Fatal, Error, Warning)
#     FehlerNr muß eine bekannte Fehlernummer sein
#     Zusatz1 und Zusatz2 sind optionale Fehlernummerspezifische Angaben
#     (z.B. den Namen der Datei, die man nicht öffnen konnte)
#
#   DWMSG_Logdateiname <VarName> <JobKennung> <EintragsNr>
#     Funktion baut aus den Angaben JobKennung und Eintragsnummer
#     einen LogDateinamen auf und legt ihn in der Variablen VarName ab.
#
#   DWMSG_AppendTimingInfos <EintragsNr> <InfoText> <DateFormat>
#     Funktion traegt Zeitinfos mit Infotext in die Spalte ZUSATZINFOS ein   
#
#########################################################################


DWMSG_Fehlerbehandlung() {
# Fehlerbehandlung wird NUR im Rahmenskript durchgeführt
#
# In dieser Funktion wird die Fehlerbehandlung geregelt
# Die Schritte sind immer
#   (*) Meldung produzieren und in Datenbank ablegen (nur wenn
#       dort nicht schon ein Eintrag steht, das regelt aber die aufgerufene
#       PL/SQL-Routine)       
#   (*) Eintrag in der Datenbank auf fehlerhaft beendet setzen
# und ggf.: 
#   (*) Benachrichtigung per Mail und/oder Robomonbenachrichtigung
#
# Parameter:
#   EintragsNr , die Nummer des Eintrages in der Meldungstabelle
####################################################################
# sichern des FehlerCodes:
  FehlerNr=$?
  typeset DWMSG_EintragsNr
  typeset -r kUnerwFehler=10
  DWMSG_EintragsNr=$1

  # echo "Ich bin im Fehlerhandler, fehler der DB melden..."
  # Melde Fehler in der Meldungstabelle. (Diese Fehlernummer wird aber 
  # nur abgelegt, wenn nicht von einer eigenen Routine ein anderer Fehler
  # abgelegt wurde (Unterscheidung zwischen Applikations und unerwarteten
  # Fehlern)
  DWMSG_MeldeFehler $DWMSG_EintragsNr F $kUnerwFehler "ErrorCode ist: $FehlerNr"

  echo "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"
  # DWMSG_SetzOsFehler $DWMSG_EintragsNr $FehlerNr
  DWMSG_SetzeStatusAbbruch $DWMSG_EintragsNr

  # hier ggf. weitere Schritte einbauen.

}

####################################################################

DWMSG_SetzeStatusOK() {
#  Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich
#  beendet. Dies ist i.a. die letzte Anweisung im Rahmenskript
#
  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"
    exit 1
  fi

  # Aufruf des PL/SQL Skriptes zum Setzen des Status
  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql \
          BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null
}

####################################################################

DWMSG_SetzeStatusAbbruch() {
#  Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
#  Dies wird von der Fehlerroutine aufgerufen.

  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"
    exit 1
  fi

  # Aufruf des PL/SQL Skriptes zum Setzen des Status
  sqlplus $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql \
          BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null
}

####################################################################

DWMSG_ErmittleNr() {
#  Funktion ermittelt durch Aufruf einer entsprechenden PL/SQl Routine
#  eine Nr, die für nachfolgende Aufrufe der Fehleroutinen benötigt wird. 
#  Diese Nummer ist eineindeutig. Die Nummer wird in der Varaiblen abgelegt
#  dessen Name als erster Parameter angegeben wurde
  typeset VarName
  VarName=$1

  if [ -z "$VarName" ]
  then
    echo "Argh!, keinen Variablennamen bei ErmittleNr angegeben"
    exit 1
  fi
  
  # Zur Kommunikation mit sqlplus wird der ermittelte Wert vom SQL-Plus
  # Skript in eine temporäre Datei geschrieben und dann hier ausgelesen.
  typeset TempFile
  TempFile="/tmp/ErmittleNr_$$.lst"

  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql \
          "$TempFile" </dev/null

  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=`cat $TempFile | tr -d ' '`

  rm $TempFile

  # zuweisen
  eval "$VarName=$DWMSG_EintragsNr"
}

####################################################################

DWMSG_ErzeugeEintrag() {
#  Funktion erzeugt durch Aufruf einer entsprechenden PL/SQL Routine
#  einen Eintrag in der Meldungstabelle zur Aufnahme von Fehlermeldungen
#  und weiteren Infos. Die <JobKennung> ist die Jobkennung des
#  Rahmenskriptes.

  typeset DWMSG_EintragsNr
  typeset JobKennung
  typeset Programmname
  typeset LogDatei

  DWMSG_EintragsNr=$1
  JobKennung=$2
  Programmname=$3
  LogDatei=$4

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"
    exit 1
  fi
  # momentan keine weiteren Prüfungen (ToDo)
  
  # Aufruf des PL/SQL Skriptes zum Setzen des Status
  sqlplus -s $DW_ORAUSER \
          @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql \
          BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung \
          $Programmname $LogDatei </dev/null

}

####################################################################

DWMSG_MeldeFehler () {
#  Funktion meldet einen Fehler durch Aufruf einer entsprechenden PL/SQL 
#  Routine. Diese regelt die Ausgabe der Meldung im entsprechendem Format
#  und den Eintrag in der Meldungstabelle (mit Eintragsnummer EintragsNr)
#  Typ kann folgende Ausprägungen haben F/E/W (Fatal, Error, Warning)
#  FehlerNr muß eine bekannte Fehlernummer sein
#  Zusatz1 und Zusatz2 sind optionale Fehlernummerspezifische Angaben
#  (z.B. den Namen der Datei, die man nicht öffnen konnte)
#
#      <EintragsNr> <Typ> <FehlerNr> [[<Zusatz1>] <Zusatz2>]
#

  typeset DWMSG_EintragsNr=""
  typeset Typ=""
  typeset FehlerNr=""
  typeset Zusatz1=""
  typeset Zusatz2=""

  # echo "Meldefehler: $*"

  DWMSG_EintragsNr=$1
  Typ=$2
  FehlerNr=$3
  if [ $# -ge 4 ]; then
    Zusatz1=$4
  fi
  if [ $# -ge 5 ]; then
    Zusatz2=$5
  fi

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"
    exit 1
  fi
  # keine weiteren Prüfungen mehr (ToDo)

  # Anzahl der Parameter ermitteln
  typeset NumParm
  if [ -z "$Zusatz1" ]
  then
    # Aufruf des PL/SQL Skriptes mit drei Parametern
    NumParm=3
  elif [ -z "$Zusatz2" ]
  then
    NumParm=4
  else
    NumParm=5
  fi

  typeset Dateipfad

  Dateipfad="$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p${NumParm}.sql"
  sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler \
          $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' \
          </dev/null

}

####################################################################

DWMSG_Logdateiname() {
#  Funktion baut aus den Angaben JobKennung und Eintragsnummer
#  einen LogDateinamen auf und legt ihn in der Variablen VarName ab.
#  
#  Parameter  <VarName> <JobKennung> <EintragsNr>
  typeset Dateiname
  typeset JobKennung
  typeset VarName

  VarName=$1
  JobKennung=$2
  DWMSG_EintragsNr=$3

  Dateiname="${DW_DIR_PROT}/${JobKennung}_"
  Dateiname="${Dateiname}`date '+%Y%m%d_%H%M'`_${DWMSG_EintragsNr}.log"

  # zuweisen
  eval "$VarName=$Dateiname"

}
####################################################################

DWMSG_SetzeStichtagInfo() {
#  Funktion setzt weitere Infofelder des Eintrages mit Nummer EintragsNr 
#
  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1
  DWMSG_Stichtag=$2
  DWMSG_StichtagFmt=$3

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"
    exit 1
  fi

  if [ -z "$DWMSG_Stichtag" ]
  then
    echo "Argh!, keinen Stichtag angegeben!"
    exit 1
  fi
  
  if [ -z "$DWMSG_StichtagFmt" ]
  then
    echo "Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"
    exit 2
  fi

  # Aufruf des der SP zum Setzen der ZusatzInfos
  sqlplus -s $DW_ORAUSER <<EOF
    EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
    commit;
EOF
}

####################################################################

DWMSG_AppendTimingInfos() {
#  Funktion fuegt Timinginfos in die Spalte ZUSATZINFOS hinzu
#
  typeset DWMSG_EintragsNr
  DWMSG_EintragsNr=$1
  DWMSG_InfoText=$2
  DWMSG_DateFormat=$3

  if [ -z "$DWMSG_EintragsNr" ]
  then
    echo "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"
    exit 1
  fi
  
  if [ -z "$DWMSG_DateFormat" ]
  then
    echo "Argh!, Formatangabe erforderlich!"
    exit 2
  fi

  # Aufruf des der SP zum Setzen der ZusatzInfos
  sqlplus -s $DW_ORAUSER <<EOF
    EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
    commit;
EOF
}



####################################################################

#
###################################
### Local Variables:
### sh-shell: ksh88
### End:
###################################




=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script is a KornShell utility library defining multiple error handling, logging, and state-tracking functions that interface with an Oracle database via SQL*Plus and perform string formatting.

EVIDENCE
- Business logic found: KSH custom logic defines a shared library of orchestrational error-handling and DB-logging functions (DWMSG_*) utilized by parent jobs to register status and trap errors.
- AWK: none
- SQL-expressible: No, because it defines a library of dynamic shell functions, formats local file system paths, and relies on process-level exception trapping (trap ... ERR) and shell environment manipulation.
- Non-SQL side effects: Creates and cleans up temporary local files, dynamically formats local log file names using system dates, and manipulates dynamic parent-shell variable assignments via eval.
- Against this verdict: A purely database-driven logging approach could replace these utilities, but the shell mechanics (process trapping, environment setup, and utility function registration) require Python to be wrapped and called equivalently in a modern workflow.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) functions as a shared KornShell utility library for error management and state tracking in an Information Services (DWH) batch processing environment. It provides unified helper functions to register batch runs, update status (OK/Aborted) in an Oracle tracking table, write timing metadata, and generate standardized log file names. These utilities interface with an Oracle database by running SQL*Plus wrapper scripts that call stored procedures inside the `BERT_MELDUNG` package.

2. INVOCATION CONTEXT
   - **Caller:** Sourced by other parent KornShell scripts (e.g., `. f_alis_msgerr.ksh`) at the beginning of their execution to import these utility functions.
   - **UC4 Job / Include Objects:** None directly invoked inside this library. The parent scripts sourcing this file are typically launched via UC4 JOBS_UNIX.
   - **Environment Files Sourced:** None explicitly sourced within this library script. It assumes standard environment variables (`$DW_ORAUSER`, `$DW_DIR_ROOT`, and `$DW_DIR_PROT`) are already exported by the sourcing context.

3. PARAMETERS / INPUTS
   The utility functions use several environment variables:
   - `DW_ORAUSER`: Database connection credentials (e.g., `user/password@dbname`). Used in all SQL*Plus calls.
   - `DW_DIR_ROOT`: Root installation directory for SQL scripts. Used to locate external `.sql` wrapper files.
   - `DW_DIR_PROT`: Target directory path for storing job execution logs.
   
   Functions accept positional arguments ($1, $2, etc.) which are detailed in the individual step descriptions.
   
   **KSH Declared Environment Parameters (Informational/Boilerplate):**
   - None explicitly declared in a companion boilerplate file for this library script, but standard environment structure is assumed.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Used to execute various PL/SQL procedures within the `BERT_MELDUNG` database package.
     - *Command 1:* `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr` (Executes OK status update)
     - *Command 2:* `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr` (Executes Abbruch/Abort status update)
     - *Command 3:* `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile"` (Spools a sequence number to a temp file)
     - *Command 4:* `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei` (Registers program run)
     - *Command 5:* `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler ...` (Dynamically routed based on parameter count to log error details)
     - *Command 6:* `sqlplus -s $DW_ORAUSER` with inline heredocs for timing/date metadata updates.
   - **Resolution Decision:**
     # REVIEW-STRUCT: SQL scripts (d_alis_spaufruf_*.sql) are not supplied in this extraction. The database platform is confirmed as Oracle from the SQL*Plus syntax and PL/SQL packages. If converting to Python, these external SQL*Plus subprocess calls should be replaced with native database client calls (e.g., using `oracledb`) executing the package procedures (`BERT_MELDUNG.*`) directly.

5. EMBEDDED SQL
   The script invokes external SQL wrappers and incorporates two inline PL/SQL statements:
   
   - **SQL Statement 1 (DWMSG_SetzeStichtagInfo):**
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - *Type:* PL/SQL block / procedure call
     - *Tables touched:* Logging metadata tables (encapsulated by `BERT_MELDUNG`)
     - *Dialect:* Oracle-specific (`to_date`, PL/SQL package syntax)
     
   - **SQL Statement 2 (DWMSG_AppendTimingInfos):**
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - *Type:* PL/SQL block / procedure call
     - *Tables touched:* Logging metadata tables (encapsulated by `BERT_MELDUNG`)
     - *Dialect:* Oracle-specific (`to_char`, `SYSDATE`, string concatenation `||`)

6. CONTROL FLOW
   The script contains no global procedural execution line; it is a library defining nine distinct functions:
   1. **DWMSG_Fehlerbehandlung($1: EintragsNr)**:
      - Traps parent exit code (`$?`), logs fatal error code 10 with the captured exit code, and flags the entry status as aborted.
   2. **DWMSG_SetzeStatusOK($1: EintragsNr)**:
      - Asserts EintragsNr is provided, invokes `BERT_MELDUNG.SetzeStatusOk` via PL/SQL wrapper.
   3. **DWMSG_SetzeStatusAbbruch($1: EintragsNr)**:
      - Asserts EintragsNr is provided, invokes `BERT_MELDUNG.SetzeStatusAbbruch` via PL/SQL wrapper.
   4. **DWMSG_ErmittleNr($1: VarName)**:
      - Asserts variable name is provided. Generates temp file `/tmp/ErmittleNr_$$`, calls Oracle script to spool sequence number to that file, reads the file, trims whitespace, assigns result dynamically to the variable named in `$1`, and deletes the temp file.
   5. **DWMSG_ErzeugeEintrag($1: EintragsNr, $2: JobKennung, $3: Programmname, $4: LogDatei)**:
      - Asserts EintragsNr is provided, invokes `BERT_MELDUNG.Erzeuge_Eintrag` with program/log path parameters.
   6. **DWMSG_MeldeFehler($1: EintragsNr, $2: Typ, $3: FehlerNr, [$4: Zusatz1, $5: Zusatz2])**:
      - Asserts EintragsNr is provided. Resolves parameter count to select correct SQL script wrapper (`d_alis_spaufruf_p3.sql` to `p5.sql`) and runs `BERT_MELDUNG.Fehler`.
   7. **DWMSG_Logdateiname($1: VarName, $2: JobKennung, $3: EintragsNr)**:
      - Formats log filename using format: `${DW_DIR_PROT}/${JobKennung}_$(date '+%Y%m%d_%H%M')_${EintragsNr}.log`. Assigns back to caller's dynamic variable.
   8. **DWMSG_SetzeStichtagInfo($1: EintragsNr, $2: Stichtag, $3: StichtagFmt)**:
      - Asserts inputs are non-empty, calls `BERT_MELDUNG.SetzeZusatzInfos` passing converted date.
   9. **DWMSG_AppendTimingInfos($1: EintragsNr, $2: InfoText, $3: DateFormat)**:
      - Asserts inputs are non-empty, calls `BERT_MELDUNG.SetzeZusatzInfos` appending dynamic system timestamp.

7. ERROR HANDLING & EXIT CODES
   - **Internal Parameter Validation:** If critical variables (like `EintragsNr` or return variable names) are missing inside a function, the function outputs an error message to stdout and calls `exit 1` or `exit 2`.
   - **Oracle Errors:** Standard SQL*Plus executions do not have `WHENEVER OSERROR EXIT` / `WHENEVER SQLERROR EXIT` explicitly defined in the KSH call syntax (though they may exist inside the unsupplied `.sql` wrapper scripts).
   - **Python Mapping:** Convert KSH parameter assertions into pythonic `ValueError` exceptions. Use `oracledb.Error` exception handling for database operations.

8. OUTPUTS / SIDE EFFECTS
   - Writes registration states and error logs directly to Oracle tables.
   - Creates and deletes temporary state files (`/tmp/ErmittleNr_*.lst`).
   - Generates standardized log file names.

9. BUSINESS SUMMARY
   - **Unified State Registration:** Initiates and logs pipeline states across all batch workflows to track success and failure patterns in a centralized metadata database.
   - **Exception Interception:** Intercepts failures, capturing system-level shell exit codes and storing them directly in standard operational tables.
   - **Tracing and Logging:** Appends formatted timestamps and execution timings to support business SLA monitoring and root cause analysis.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON)
=======================================================================================

The library is restructured below as a clean Python module using native database connectivity. Sourced variable updates (like KSH's dynamic `eval "$VarName=..."`) are modernized to return standard Python values.

```python
import os
import sys
import tempfile
from datetime import datetime
import oracledb  # Or alternative DB driver matching the target migration path

# Global configuration read from environment
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")

# REVIEW-STRUCT: SQL wrapper scripts are not supplied. If native DB client calls are used, 
# credentials must be resolved from DW_ORAUSER.
def _get_db_connection():
    # Provisionally parsing legacy DW_ORAUSER string: "user/password@host:port/service"
    # Ensure this is replaced with standard cloud secret manager retrieval in production.
    if not DW_ORAUSER:
        raise ValueError("Environment variable 'DW_ORAUSER' is not set.")
    # Extracting connection details from Oracle-format string
    try:
        credentials, dsn = DW_ORAUSER.split('@')
        user, password = credentials.split('/')
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as e:
        print(f"Error parsing database credentials or connecting: {e}", file=sys.stderr)
        raise

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code=1):
    """
    Error trap function called on parent pipeline failure.
    Equivalent to original KSH DWMSG_Fehlerbehandlung trap block.
    """
    print("Fehler wurde von der Shell/Python gemeldet, setze auf Abbruchstatus")
    # Log fatal error code (10)
    dwmsg_melde_fehler(eintrags_nr, "F", 10, f"ErrorCode ist: {last_exit_code}")
    # Flag record as aborted
    dwmsg_setze_status_abbruch(eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    connection = _get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Native PL/SQL stored procedure call instead of executing d_alis_spaufruf_p1.sql
            cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [int(eintrags_nr)])
            connection.commit()
    except Exception as e:
         print(f"Error executing SetzeStatusOk: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    connection = _get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [int(eintrags_nr)])
            connection.commit()
    except Exception as e:
         print(f"Error executing SetzeStatusAbbruch: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    """
    Retrieves and returns a unique entry number from Oracle sequence.
    Replaces KSH temp-file based spool dynamic return pattern with modern return value.
    """
    connection = _get_db_connection()
    try:
        with connection.cursor() as cursor:
            # REVIEW-STRUCT: d_al_is_ermittlenr.sql body not supplied. 
            # Assuming it queries a sequence or calls a generation procedure.
            # Here we call the presumed PL/SQL procedure or equivalent sequence SELECT.
            # Represented as direct sequence fetch:
            cursor.execute("SELECT BERT_MELDUNG.GetNextEintragsNr FROM DUAL")
            result = cursor.fetchone()
            if result:
                return str(result[0]).strip()
            else:
                raise ValueError("Could not retrieve a valid sequence number.")
    except Exception as e:
         print(f"Error executing ErmittleNr: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    connection = _get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [int(eintrags_nr), job_kennung, programmname, log_datei])
            connection.commit()
    except Exception as e:
         print(f"Error executing Erzeuge_Eintrag: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    connection = _get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Rather than choosing d_alis_spaufruf_p3.sql vs p4/p5 based on presence of params,
            # python passes optional arguments cleanly to the DB procedure.
            cursor.callproc("BERT_MELDUNG.Fehler", [typ, int(eintrags_nr), int(fehler_nr), zusatz1, zusatz2])
            connection.commit()
    except Exception as e:
         print(f"Error executing MeldeFehler: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    """
    Constructs and returns standardized log file path.
    Replaces KSH dynamic variable assignment.
    """
    log_dir = DW_DIR_PROT if DW_DIR_PROT else "/tmp"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    return os.path.join(log_dir, f"{job_kennung}_{timestamp}_{eintrags_nr}.log")

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    connection = _get_db_connection()
    try:
        # Convert KSH custom date/format to Python datetime object for safe binding
        # (This translates the format directly or passes raw strings to the Oracle to_date parser)
        with connection.cursor() as cursor:
            # Executing inline PL/SQL statement safely via bindings
            plsql_block = f"""
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
                COMMIT;
            END;
            """
            cursor.execute(plsql_block, {
                'eintrags_nr': int(eintrags_nr), 
                'stichtag': stichtag, 
                'stichtag_fmt': stichtag_fmt
            })
    except Exception as e:
         print(f"Error executing SetzeStichtagInfo: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    connection = _get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Inline PL/SQL statement mapped dynamically using database engine timestamping
            plsql_block = f"""
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, null, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                COMMIT;
            END;
            """
            cursor.execute(plsql_block, {
                'eintrags_nr': int(eintrags_nr),
                'info_text': info_text,
                'date_format': date_format
            })
    except Exception as e:
         print(f"Error executing AppendTimingInfos: {e}", file=sys.stderr)
         raise
    finally:
         connection.close()
```

# MIGRATION DESIGN DOCUMENT

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Utility library providing error handling and operational metadata logging functions. Converted to a Python module to be shared and imported across the pipeline. |

---

### Execution Order
The legacy orchestration defines a strict step sequence:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 Orchestration wrapper)
2. `d_abtn_x_smart_kubi.sql` (Oracle PL/SQL script)
3. `r_sqlscript` (Shell execution wrapper)
4. `.dw_init` (Shell environment initialization)
5. `f_alis_msgerr.ksh` (Shell utility library - **this source file**)
6. `h_alis_sqlplus.ksh` (SQL*Plus helper execution utility)

**Target Execution / Orchestration Mapping:**
* In the migrated environment, `f_alis_msgerr.ksh` is converted into a standard Python module (`f_alis_msgerr.py`).
* Unlike standalone scripts, it does not represent an independent task node in the target Airflow DAG / Cloud Composer workflow. Instead, its functions are imported and executed within the runtime context of parent tasks (such as `r_sqlscript.py` and `h_alis_sqlplus.py`). 

---

### Schedule & Variables
The scheduler configures and injects the following variables for this job run:
* `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
* `cdate` = `'SYS_DATE("YYYYMMDD")'` (Current date in YYYYMMDD format)
* `cmonth` = `'SUBSTR(&cdate,1,6)'`
* `cday` = `'SUBSTR(&cdate,7,2)'`
* `first` = `'01'`
* `cmonth` = `'&cmonth&first'`
* `cmonth = 'SUB_DAYS(&cmonth,1)'` (Calculates the last day of the prior month)
* `cmonth = 'SUBSTR(&cmonth,1,6)'` (Extracts YYYYMM of the prior month)
* `MONATSID` = `'&cmonth'` (Target reporting month variable, e.g., `'202607'`)

**Target Mechanism:**
* In the Airflow DAG context, these values will be computed dynamically using Airflow Jinja templates (e.g., using `execution_date` or `macros`) and passed to Python operators either as environment variables (`os.environ`) or runtime DAG params.

---

### Lineage
* **Upstream / Calls:**
  * `f_alis_msgerr.ksh` calls the database-side procedure `PROCEDURE:SETZEZUSATZINFOS` (Oracle PL/SQL procedure encapsulated inside the `BERT_MELDUNG` package) to log execution timestamps and business metadata.

---

### External System Replacements
* **Oracle Stored Procedure / Metadata Tracking Migration:**
  * In the legacy shell script, logging calls (`BERT_MELDUNG.SetzeStatusOk`, `BERT_MELDUNG.SetzeStatusAbbruch`, `BERT_MELDUNG.Erzeuge_Eintrag`, `BERT_MELDUNG.Fehler`, `BERT_MELDUNG.SetzeZusatzInfos`) are made via `sqlplus` subprocesses.
  * In the target GCP architecture, operational metadata tracking should be aligned to write directly to BigQuery tables (e.g., an `audit_log` dataset containing equivalent tables and stored procedures) using the native BigQuery client library (`google.cloud.bigquery`), replacing Oracle-specific SQL*Plus mechanics entirely.

---

### Cross-File Dependencies
* This utility library is a central component of the DWH shell execution framework. It is sourced/loaded by key sibling components in this execution group:
  * `r_sqlscript.py` (which runs the primary execution wrapper)
  * `h_alis_sqlplus.py` (which manages SQL executions)
  * `.dw_init.py` (which sets up environmental states)
* **Target Mapping:** Sibling Python files in the repository must use native Python imports to pull in logging and tracking functions from `f_alis_msgerr.py` (e.g., `from f_alis_msgerr import dwmsg_melde_fehler, dwmsg_setze_status_ok`) rather than re-implementing or stubbing them.

---

### Target File Plan
* **Target File Path:** `local/home/gurunathan_t/kubi/f_alis_msgerr.py`
  * **Language:** Python
  * **Source File:** `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
  * **Note:** The target path strictly adheres to the **Folder Integrity Rule** by mirroring the directory path of the source file. The implementation of this module is authoritatively defined by the automatically attached MCP output.

---

### Environment-Specific Values

#### GLOBAL (Environment-Wide Configuration)
* `GCP_PROJECT`: Global GCP project ID where BigQuery resources reside.
* `BQ_DATASET`: Centralized logging/audit BigQuery dataset containing the audit tables (replacing the `BERT_MELDUNG` schema).
* `GCS_BUCKET`: The GCS bucket designated for standard logging and telemetry, replacing the legacy local directory path `DW_DIR_PROT`.
* `DW_DIR_ROOT`: Root path representing the Airflow DAG or execution codebase root (e.g., `/home/airflow/gcs/dags/`).

#### JOB-SPECIFIC
* `DWH_JOB_KENNUNG`: The specific code identifier for this execution thread (`'ABTN_SMART_KUBI'`).
* `MONATSID`: The reporting month identifier calculated at runtime.

---

### Risks and Manual Steps

1. **Feedback Realignment & Python Integration:** Sibling passes (for `r_sqlscript` and `h_alis_sqlplus`) previously assumed that `f_alis_msgerr.ksh` was an unsupplied shell library and stubbed out its logging functions. These sibling modules must be refactored to import real functions from `f_alis_msgerr.py` to ensure consistent logging across the entire pipeline.
2. **Missing Database-Side Artifacts:** The internal PL/SQL procedures (e.g., `BERT_MELDUNG.SetzeStatusOk`, `BERT_MELDUNG.Fehler`) and tables referenced inside `f_alis_msgerr.ksh` are not supplied in this code group. To enable successful migration, developers must replicate the target schema structures in BigQuery and implement equivalent stored procedures or BigQuery SQL insert scripts.
3. **BigQuery Logging Client Alignment:** Ensure that the database client used inside `f_alis_msgerr.py` is fully aligned with BigQuery (`google.cloud.bigquery`) and avoids any direct execution of Oracle-specific `sqlplus` binaries.
4. **Shell Exit Code Interception vs. Python Exception Catching:** The legacy library relies on `trap ... ERR` and `$?` to catch failures. In Python, these must be replaced with robust `try-except` blocks around calling executions, or Airflow DAG `on_failure_callback` functions.

---

=== FILE: local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh ===
# Zweck:
#    Hilfsroutinen fuer die Benutzung von SQL*Plus
#
# Erzeugt von : TJ 
# Erzeugt am  : 18.02.98
# Aenderung :
# Versions-Anmerkungen:
# $Log$
#   1.1.3; 07.09.98; TJ
#     - Ausgabe der Befehlszeile fuer Logdatei

#
# Oeffentliche Funktionen:
# -----------------------
# starteSQLSkript EntryNr Skript Parameter
#

#Generelle Annahmen: 
#   1.  Fehlerbehandlung ist aktiv

ModulName="alis_sqlplus"
ModulVersion="V1.1.3";


#####################################
# Funktion:
#    starteSQLSkript - Startet ein SQL-Pluskript
#Parameter:
#   I   Fehlereintragsnummer
#   I   Name der Skriptes, welches zu starten ist
#   I   beliebige Anzahl von Parametern fuer das SQLSkript
#Annahmen: (PreConditions)
#   1. Fehlerbehandlung ist aktiv
#Beschreibung:
#   Falls ein SQLSkript nicht als Datei vorhanden ist, meldet
#   ein Aufruf durch SQLPlus kein Fehler.
#   Daher wird vor dem SQLPlus-Aufruf ueberpruft, ob die Datei
#   lesbar ist. Ist dies nicht der Fall, so erfolgt eine
#   Fehlermeldung.
starteSQLSkript(){

    typeset p_Eintragsnr=$1
    typeset p_Skript=$2;
    typeset errcode

    shift 2
    
    if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    then
        DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"
        return 196
    fi

    if [ ! -r $p_Skript ]
    then
        DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript
        return 201
    fi

    echo "Rufe SQL*PLUS auf mit folgenden Einstellungen"
    echo "Sql*Plus-Skript : $p_Skript"
    echo "Skript-Parameter: $*"

    set +e

    sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null

    errcode=$?

    set -e

    return $errcode
}


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script is a helper library defining a reusable function that performs parameter validation, file-readability checks, and dynamic SQL*Plus process execution.

EVIDENCE
- Business logic found: KSH custom logic. It is a utility script defining the function 'starteSQLSkript' which validates file readability, performs input validation, and triggers an external sqlplus process.
- AWK: none
- SQL-expressible: no, it consists of shell control flow, file existence validation, external process execution, and error utility calls.
- Non-SQL side effects: checks filesystem readability (`-r`), calls an external logging program `DWMSG_MeldeFehler`, and executes `sqlplus` via shell process invocation.
- Against this verdict: none, as this is an orchestration/utility helper module that cannot map directly to SQL.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a KornShell utility module that provides reusable helper routines for executing SQL*Plus scripts. Specifically, it defines the `starteSQLSkript` function, which validates input arguments, ensures the target SQL script is readable, logs parameter configurations, and launches `sqlplus` while propagating exit codes. It acts as a standard execution wrapper to ensure safety and consistent error reporting across database scripts.

2. INVOCATION CONTEXT
   - Who calls this script: This is a utility module meant to be sourced (e.g., `. h_alis_sqlplus.ksh`) by parent ETL scripts. It does not have an independent UC4 job execution context on its own.
   - UC4 Native Includes: None.
   - Environment Files Sourced: None.

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` receives positional parameters when called:
   - `p_Eintragsnr` (positional `$1` inside function): Fehlereintragsnummer (error log ID). Required. Surfaces in Python as the first argument to the function.
   - `p_Skript` (positional `$2` inside function): Path to the SQL script to be executed. Required. Surfaces in Python as the second argument to the function.
   - `*args` (positional `$3` and onward inside function, captured via `shift 2` and `$*`): Arbitrary parameter arguments to be passed forward to the SQL script. Surfaces in Python as standard `*args` variable arguments.
   - `DW_ORAUSER` (environment variable): Database connection string used by SQL*Plus. Retrieved from the environment via `os.environ.get("DW_ORAUSER")`.
   - `ModulName` and `ModulVersion`: Local variables defined inside the script (`ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`).
   
   # REVIEW: The original shell script defines variable 'ModulName' but references 'Modul_Name' in the DWMSG_MeldeFehler call. This may result in an empty string in the logged error message. Confirm intended variable name.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **Command Line**: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Runs Oracle SQL*Plus to execute the specified SQL script with the provided parameters. The input redirection `</dev/null` ensures that SQL*Plus does not block waiting for user input.
     - Target: Must remain an external process invocation via `subprocess.run` as the SQL script contents are dynamic and not part of this extraction.
     - Resolvable Launcher: No.
     - # REVIEW: target database platform not specified; DB-client library choice below is provisional.
   - **Command Line**: `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"` and `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
     - Purpose: Custom error reporting launcher. Reports validation failures to a central system.
     - Target: external subprocess invocation.
     - # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - None (the script runs external SQL files dynamically via parameter passing).

6. CONTROL FLOW
   1. **Module Scoping**: Establish local module variables `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"`.
   2. **Function Definition**: Define function `starteSQLSkript(p_Eintragsnr, p_Skript, *args)`.
   3. **Arg Validation**: Check if `p_Eintragsnr` or `p_Skript` are null/empty. If so:
      - Call `DWMSG_MeldeFehler` with error code `196` and module info.
      - Return status code `196`.
   4. **Readability Check**: Verify if the script file `p_Skript` exists and is readable (`[ ! -r $p_Skript ]`). If not:
      - Call `DWMSG_MeldeFehler` with error code `201` and the script name.
      - Return status code `201`.
   5. **Log Execution**: Log execution details to stdout, including the script path and list of parameters.
   6. **SQL*Plus Invocation**: Disable immediate shell exit on failure (`set +e`), call `sqlplus`, and capture the exit code.
   7. **Restore Shell State**: Enable immediate exit (`set -e`) and return the captured SQL*Plus exit code.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments inside the function result in exit code `196` after calling `DWMSG_MeldeFehler`.
   - Unreadable SQL script file results in exit code `201` after calling `DWMSG_MeldeFehler`.
   - SQL*Plus exit status is captured and returned by the function.
   - Standard shell execution checks (`set -e`) are temporarily disabled using `set +e` during the SQL*Plus call to prevent the parent script from crashing, and restored immediately after.
   - Python mapping: Wrap execution in `subprocess.run(..., check=False)` to capture the return code without raising an exception, matching `set +e` and `errcode=$?` behavior.

8. OUTPUTS / SIDE EFFECTS
   - Log Messages: Outputs printed to standard output indicating execution settings.
   - External Logger: Calls `DWMSG_MeldeFehler` which logs errors.
   - Database Modification: The execution of `sqlplus` may read/write/modify database tables depending on the content of the target script.

9. BUSINESS SUMMARY
   - **Execution Guard**: Protects the database orchestrator by verifying that the target SQL script file is readable before initiating a connection.
   - **Uniform Error Logging**: Integrates with a corporate diagnostic utility (`DWMSG_MeldeFehler`) to track framework and script execution issues consistently.
   - **Subprocess Isolation**: Prevents interactive hangs of SQL*Plus inside automated batches via standard input redirection (`</dev/null`).
   - **Status Propagation**: Safely handles and forwards the database return code, enabling subsequent orchestration steps to react correctly to successes or failures.


=== PSEUDOCODE STYLE ===

```python
# Step 1: Initialize module level variables
# REVIEW: The original shell script defines variable 'ModulName' but references 'Modul_Name' in the DWMSG_MeldeFehler call.
# This may result in an empty string in the logged error message. Confirm intended variable name.
ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"

# Step 2: Define function starteSQLSkript representing the ksh function
def starteSQLSkript(p_Eintragsnr, p_Skript, *args):
    import os
    import sys
    import subprocess
    from pathlib import Path

    # Step 3: Parameter Validation
    # Check if either of the mandatory parameters is empty
    if not p_Eintragsnr or not p_Skript:
        # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        modul_name_var = os.environ.get("Modul_Name", ModulName) # Safeguard against potential naming discrepancy
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_Eintragsnr, 
            "E", 
            "196", 
            f"{modul_name_var} {ModulVersion} starteSQLSkript"
        ], check=False)
        return 196

    # Step 4: File Readability Check
    # Equivalent of [ ! -r $p_Skript ]
    script_path = Path(p_Skript)
    if not script_path.exists() or not os.access(script_path, os.R_OK):
        # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_Eintragsnr, 
            "E", 
            "201", 
            str(script_path)
        ], check=False)
        return 201

    # Step 5: Log parameter configuration to stdout
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {script_path}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Invoke SQL*Plus process
    # Retrieve connection details from the environment
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    
    # Check if DW_ORAUSER is set; flag if empty
    # REVIEW: target database platform not specified; DB-client library choice below is provisional
    if not dw_orauser:
        print("Warning: DW_ORAUSER is not set in environment.", file=sys.stderr)

    # Replicate SQL*Plus call with input redirected from /dev/null
    # set +e behavior is simulated by check=False on subprocess.run
    cmd = ["sqlplus", dw_orauser, f"@{script_path}"] + list(args)
    
    try:
        result = subprocess.run(
            cmd, 
            stdin=subprocess.DEVNULL, 
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Error invoking sqlplus: {e}", file=sys.stderr)
        errcode = 1 # Generic fallback error code if process fails to start

    # Step 7: Return the captured process exit code (equivalent to return $errcode)
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Migrated to a Python module to replace Oracle SQL*Plus CLI execution with Google Cloud BigQuery client execution and integrate with `f_alis_msgerr.py` via native Python imports. |

***

### Job Dependencies
* **Upstream Orchestration**: The job is controlled in the target environment by an Airflow DAG (derived from the UC4 job `DW.DWH_ABTN_SMART_KUBI`). This DAG will trigger the execution wrapper.
* **Sibling Python Imports**: 
  * `local/home/gurunathan_t/kubi/r_sqlscript.py` imports `starteSQLSkript` from `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`.
  * `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` imports `DWMSG_MeldeFehler` from `local/home/gurunathan_t/kubi/f_alis_msgerr.py`.
* **Deployment Wiring**: All Python modules are distributed in the same task workspace under `local/home/gurunathan_t/kubi/` allowing native, relative import paths.

***

### Execution Order
The task execution flow preserves the sequence of the legacy pipeline:
1. **DAG Initialization**: The DAG calculates the reporting month variables.
2. **Environment Sourcing**: The DAG runtime initializes the workspace context.
3. **Main Execution Wrapper**: `r_sqlscript.py` is invoked to process `d_abtn_x_smart_kubi.sql`.
4. **Execution Helper**: `r_sqlscript.py` calls `starteSQLSkript` inside `h_alis_sqlplus.py`.
5. **Logic Resolution**: `starteSQLSkript` verifies readability, performs BigQuery parameter substitution, executes the query via the BigQuery client, and uses `DWMSG_MeldeFehler` from `f_alis_msgerr.py` if errors occur.

***

### Scheduling
* **Composer Schedule**: Triggered via standard Cron configuration in Cloud Composer (Airflow), matching the daily/monthly scheduling of the source system.
* **Context variables**: Sourced dynamically during DAG execution and passed directly into the Python execution task.

***

### Schedule & Variables
* **MONATSID** (Reporting Month): Calculated dynamically inside the Airflow DAG based on the current DAG logical run date and passed as an argument.
  * **Derivation Logic**:
    * `cdate` = current run execution date in `YYYYMMDD` format.
    * `cmonth` = first 6 characters of `cdate` (`YYYYMM`).
    * `first` = `"01"`.
    * Subtracting 1 day from `${cmonth}01` yields the last day of the previous calendar month.
    * `MONATSID` is set to the first 6 characters of that resulting date (`YYYYMM` format).
  * **Sourcing**: Provided as a command-line or parameter-dict variable to `r_sqlscript.py` at runtime.

***

### Lineage
* **Upstream Sources**:
  * `DWH$TA_F_D1_TWVV_TN` (migrated to BigQuery dataset `dwh_dataset.dwh_ta_f_d1_twvv_tn`)
  * `BL_D_TARIF` (migrated to BigQuery dataset `dwh_dataset.bl_d_tarif`)
  * `DWH$VI_L_MAP_FA_TARIF` (migrated to BigQuery dataset `dwh_dataset.dwh_vi_l_map_fa_tarif`)
* **Downstream Target**:
  * `DWH$TA_T_SMART_KUBI` (migrated to BigQuery dataset `dwh_dataset.dwh_ta_t_smart_kubi`)

***

### External System Replacements
* **Oracle SQL\*Plus Client $\rightarrow$ BigQuery Python Client**:
  * Rather than launching an external `sqlplus` subprocess, the utility script is refactored to run SQL queries natively using `google.cloud.bigquery.Client`.
  * **Parameter Substitution**: Because Oracle SQL scripts utilize positional ampersand syntax (`&1`, `&2`, etc.), the converted `starteSQLSkript` function in `h_alis_sqlplus.py` will:
    1. Read the contents of the target `.sql` script.
    2. Replace instances of `&1`, `&2`, etc., with the positional items in `*args`.
    3. Run the resolved query string via `client.query().result()`.
  * This guarantees uniform, client-based BigQuery execution across `r_sqlscript.py` and `h_alis_sqlplus.py`, resolving the disjointed execution architecture.
* **Shell Utilities $\rightarrow$ Python Imports**:
  * The execution of `DWMSG_MeldeFehler` is migrated from a shell process execution to a Python-native module import: `from f_alis_msgerr import DWMSG_MeldeFehler`.

***

### Cross-File Dependencies
* `r_sqlscript.py` depends on `h_alis_sqlplus.py` (`from h_alis_sqlplus import starteSQLSkript`).
* `h_alis_sqlplus.py` depends on `f_alis_msgerr.py` (`from f_alis_msgerr import DWMSG_MeldeFehler`).
* All modules share environment values defined by the parent orchestration task.

***

### Target File Plan
* **Target File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
* **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
* **Language**: Python
* **Approach**:
  * Implements `starteSQLSkript(p_Eintragsnr, p_Skript, *args)` using Python standard libraries (`pathlib`, `os`, `sys`) and `google.cloud.bigquery`.
  * Conducts file readability verification.
  * Preserves original German logging statements exactly character-for-character:
    * `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`
    * `"Sql*Plus-Skript : {p_Skript}"`
    * `"Skript-Parameter: {args}"`
  * Reads the SQL file, performs search-and-replace for `&1`, `&2`, ... positional parameters with passed `args`, and triggers a BigQuery query job.
  * Imports `DWMSG_MeldeFehler` natively to dispatch validation errors or query execution errors.

***

### Environment-Specific Values
* **GCP_PROJECT**: GLOBAL. Identifies the Google Cloud project where BigQuery runs. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow config.
* **BQ_LOCATION**: GLOBAL. BigQuery dataset location (e.g., `EU` or `US`). Sourced via `os.environ.get("BQ_LOCATION")`.
* **DW_ORAUSER**: RETIRED. Credentials for the Oracle DB login are no longer required; authentication uses BigQuery IAM roles / GCP Service Accounts.

***

### Risks and Manual Steps
* **SQL\*Plus Specific Dialect Syntax**: SQL scripts migrated from Oracle may contain specific SQL\*Plus session commands (`WHENEVER SQLERROR EXIT`, `SET DEFINE OFF`, `COMMIT;`, etc.). These commands must be stripped or ignored before sending the queries to BigQuery.
* **Double Ampersand (`&&`) Substitutions**: If any SQL script uses double ampersands (`&&`) for persistent substitution variables, the Python substitution parser must track and reuse those replacements.
* **Workspace Python Path**: To ensure successful imports (e.g., `from f_alis_msgerr import DWMSG_MeldeFehler`), the parent task in Composer must add the module's folder (`local/home/gurunathan_t/kubi/`) to the Python search path (`sys.path`).

---

=== FILE: local/home/gurunathan_t/kubi/r_sqlscript ===
#!/bin/ksh_dwh
#
# Zweck:
#      siehe usage
# Aenderung : 27.05.2002; Stefan Kurz
#
# Historie  :
#   3.0.1; 05.07.2000;  Stephan Kriwet
#       - initiale Version
#   3.0.2; 21.09.2000; Marcus Blaha
#       - Jobkennung Parameter -j
#       - in DWTK_Meldungen wird PROGRAMM mit r_sqlscript_namedessqlscripts gefuellt
#   3.5.1  03.11.2000 Stephan Kriwet
#       - Parameter -i Inputstring.    
#         Der Inputstring wird als Parameter an das SQL-Script weitergegeben
#   5.0.1  29.05.2002 Claudia Toussaint
#       - $DW_EintragsNr wird als letzter Parameterwert an sql-Script weitergereicht
#   7.0.1  15.03.2004 Claudia Toussaint
#       - Default-Paths für sql-Script relativ zu path des Rahmenscriptes ergänzt
#       - Script wird in den folgenden Paths in dieser Reihenfolge gesucht:
#       - ../sql, ../mig, .
#  14.3  30.09.2014 Marcus Blaha shebang

ProgName="Ausführung Script $0"
ProgVersion="5.0.0"

# Funktion:
#    usage - Ausgabe der Programmbeschreibung
usage(){
cat <<EOF
   Programm: $ProgName
   Version: $ProgVersion
   Aufruf: $0 Parameter

   Das als Parameter -f  übergebene SQL-Script wird ausgeführt.
   Es muß die Zeile "whenever sqlerror exit failure" enthalten,
   damit das Rahmenscript bei Fehlern abbricht.
   Der mit dem Parameter -i übergebene String wird an das SQL-Script
   weitergereicht
   Wenn das SQL-Script keinen Pfad hat, wird es  erst in  ../sql
   parallel zum Ablageverzeichnis dieses Rahmenscripts vermutet,
   dan in ../mig,
   dann direkt im Ablageverzeichnis dieses Rahmenscripts.
   Dies Rahmenscript muß deswegen immer mit Komplettpfad aufgerufen werden
   oder direkt aus  dem  Verzeichnis, in dem es gespeichert ist.


   Parameter:
       -f     hier wird der Name des SQL-Scripts angegeben
       -i     mögliche Parameter für das SQL-Script 

       -j     Jobkennung (default DWH_KORR)

       -h     zeigt diese Seite an

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)
EOF
}

#####################
# Vorbereitende Massnahmen

# Einlesen der Umgebung
. $HOME/aktuell/.dw_init

# Fehlermeldeverfahren
. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh

# Hilfsskript zum Ausfuehren von SQL-Skripts
. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh


set -e

ErrNr=0
ErrArg=""

DW_EintragsNr=0
export DW_EintragsNr

typeset l_DBskript

#####################
# Lesen der Parameter
ParamList="f:j:i:" # Notation gemaess getopts(1)
#Initialwerte der Parameter
typeset p_Verbose=0
typeset -l p_sqlscript

# lese mit Hilfe getopts die Parameter
while getopts ":hv$ParamList" param
do
    case $param in
        f)
            p_sqlscript=$OPTARG;;
        i)
            p_sqlpar=$OPTARG;;
        v)
            p_Verbose=1;;
        j)
            p_Job=$OPTARG;;
        h)
            usage
            exit;;
        :)
            ErrNr=193  # Notwendiges Argument fehlt
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unbekannt
            ErrArg="$OPTARG";;
    esac
done




# Falls Fehler aufgetreten, abbrechen
if [ ! $ErrNr -eq 0 ]
then
    #Ausgabe gemaess Fehlerkonzept
    DWMSG_MeldeFehler $DW_EintragsNr E $ErrNr $ErrArg
    usage
    #Austieg gemaess Nummernkreisen
    exit $ErrNr
fi

cd `dirname $0`
case `dirname ${p_sqlscript}` in
'.') l_DBskript=../sql/${p_sqlscript};
     if [ ! -f "$l_DBskript" ]
     then
         l_DBskript=../mig/${p_sqlscript};
     fi;
     if [ ! -f "$l_DBskript" ]
     then
         l_DBskript=${p_sqlscript};
     fi;;     
*) l_DBskript=${p_sqlscript};;              #mit  Pfad
esac


if [  -f "$l_DBskript" ]
then

    ErrNr=198 # Parameterwert unbekannt
    ErrArg="$p_Kuerzel"
fi


#####################
# Vorbereitende Massnahmen
#    Definition von weiteren Variablen
#    weitere Arbeiten..


typeset -u JobKennung  # Kennung in Grossbuchstaben
if [[ $p_Job = "" ]]
then
   JobKennung="DWH_KORR" # JobKennung eintragen gemaess Namenskonvention
else
   JobKennung=$p_Job
fi

echo "----------------- Parameter -----------------"
echo "Jobkennung     : $JobKennung"
echo "DB-Skript      : $l_DBskript"
echo "---------------------------------------------"


# Nachfolgende Anweisungen sollten sofort nach bekanntwerden
# der JobKennung durchgefuehrt werden, da sonst keine
# Fehlerbehandlung aktiv ist.
DWMSG_ErmittleNr DW_EintragsNr
DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr
DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0_$l_DBskript $LogDatei >> $LogDatei 2>&1

# Setze traps
typeset aktion=""
typeset trap="DWMSG_Fehlerbehandlung $DW_EintragsNr >> $LogDatei 2>&1"
typeset trap_os="$trap ; echo '!OSFEHLER gemeldet!'"
typeset trap_err="$trap ;echo '!FEHLER gemeldet!'"

if [ "$p_Verbose" != "0" ]
then
    # Setze DEBUG Traps (Logdateiausgabe am Ende)
    aktion="; cat $LogDatei "
fi

trap "$trap_os  $aktion ; exit 1" INT  >> $LogDatei 2>&1
trap "$trap_err $aktion" ERR >> $LogDatei 2>&1

#####################
# Eigentlicher Job

    # Aufrufe des Kernskriptes etc. mit Umleitung >>$Logdatei
    echo "----------------- Job -----------------------"
    echo "Job-Nr    : '$DW_EintragsNr'"
    echo "Logdatei  : '$LogDatei'"
    echo "---------------------------------------------"

# Fuehre Skript zum Abgleich aus
starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1

#####################
# Nachbereitende Massnahmen
#    Abschalten der Fehlerbehandlung
#    weitere Arbeiten..

# Abschalten der Fehlerbehandlung
DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1

trap INT ERR

echo "Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script is a comprehensive orchestration and execution wrapper for Oracle SQL scripts, containing dynamic path resolution, getopts command-line parsing, parameter validation, and custom error-trapping frameworks.

EVIDENCE
- Business logic found: KSH custom logic. The script resolves SQL file paths dynamically across multiple relative directories, handles command-line arguments, executes custom environment-logging actions, and manages system traps.
- AWK: none
- SQL-expressible: no. This is an orchestration script whose main job is to locate, set up logging for, and execute external SQL files using a custom framework.
- Non-SQL side effects: Dynamically checks filesystem paths, manages traps/signals, and formats log file outputs via environment functions.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`r_sqlscript`) is a generic runner designed to execute Oracle SQL*Plus scripts within a data warehousing environment. It accepts the target SQL script name, arguments, and metadata, dynamically searches for the script across a series of fallback directories (`../sql`, `../mig`, and `.`), sets up traps for system and execution errors, registers the run in a centralized tracking database, and routes execution to a specialized database client runner (`starteSQLSkript`).

2. INVOCATION CONTEXT
   - **Invoker**: Typically called by UC4 jobs (e.g., JOBS_UNIX objects within workflow schedules).
   - **Command Line**: Called with options `-f <sql_script>` (mandatory), `-i <parameters>`, `-j <job_name>`, `-v` (verbose output).
   - **UC4 Native Includes**: None referenced in this script.
   - **Environment Files Sourced**:
     - `. $HOME/aktuell/.dw_init`
       # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
       # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
       # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - **-f**: bound to `p_sqlscript` (typeset -l, converted to lowercase). Sourced from command-line argument. Mandatory. Used in path resolution and job logging. Maps to Python `argparse` argument.
   - **-i**: bound to `p_sqlpar`. Sourced from command-line argument. Optional. Contains the parameter string passed directly down to the SQL script. Maps to Python `argparse` argument.
   - **-j**: bound to `p_Job` / `JobKennung` (typeset -u, converted to uppercase). Sourced from command-line argument. Optional (defaults to `"DWH_KORR"`). Used in log registration. Maps to Python `argparse` argument.
   - **-v**: bound to `p_Verbose`. Sourced from command-line flag. Optional. Controls whether log files are printed directly to stderr on error traps. Maps to Python `argparse` action flag.
   - **DW_EintragsNr**: Internal tracking variable obtained dynamically via `DWMSG_ErmittleNr`. Exported for use by child execution processes.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **`starteSQLSkript`**:
     - *Exact Command Line*: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
     - *Purpose*: Initiates the actual SQL execution via SQL*Plus, logging the output to `$LogDatei`.
     - *Translation Strategy*:
       # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
       Since the source code for `starteSQLSkript` (defined in `h_alis_sqlplus.ksh`) is not provided, this must be represented as a subprocess run of the legacy shell command unless the execution database target is migrated to BigQuery, in which case this launcher would be replaced by native Google Cloud BigQuery client library queries.
       # REVIEW: target database platform not specified; DB-client library choice below is provisional

5. EMBEDDED SQL
   - No inline SQL is written within this shell script. It acts purely as a launcher wrapper for external `.sql` files passed via the `-f` flag.

6. CONTROL FLOW
   1. **Initialization**: Initialize `ProgName="Ausführung Script $0"` and `ProgVersion="5.0.0"`.
   2. **Environment Loading**: Source `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`.
   3. **Argument Parsing**: Parse `-f`, `-i`, `-j`, `-v`, and `-h` using a `while getopts` loop.
   4. **Argument Validation**: 
      - If `-h` is supplied, call `usage()` and exit.
      - If required parameters are missing or unknown parameters are passed, set `ErrNr` (193/192), write error via `DWMSG_MeldeFehler`, and exit.
   5. **Path Resolution**: 
      - Change directory to script execution home (`cd \`dirname $0\``).
      - If `p_sqlscript` is a relative path (directory name is `.`), look for the SQL file sequentially in:
        1. `../sql/${p_sqlscript}`
        2. `../mig/${p_sqlscript}`
        3. `${p_sqlscript}` (current directory)
      - If `p_sqlscript` contains an absolute/other path, use it as-is.
   6. **File Existence Validation**:
      - Check if resolved `l_DBskript` exists.
      # REVIEW: The legacy script contains `if [ -f "$l_DBskript" ] then ErrNr=198 ...`. This triggers an error if the file *does* exist, which appears to be a legacy coding bug (it likely should have checked `! -f`). The conversion must preserve the legacy logic but flag this block for manual functional correction.
   7. **Job Registration & Log Setup**:
      - Default `JobKennung` to `"DWH_KORR"` if not supplied.
      - Determine tracking ID using `DWMSG_ErmittleNr`.
      - Determine log name using `DWMSG_Logdateiname`.
      - Initialize logging using `DWMSG_ErzeugeEintrag`, redirecting output to `$LogDatei`.
   8. **Trap Registrations**:
      - Set traps on `INT` and `ERR` signals to run custom cleanups (`DWMSG_Fehlerbehandlung`) and print log contents if verbose flag is set.
   9. **Execution**:
      - Invoke `starteSQLSkript` with resolved variables and parameters, redirecting all outputs to `$LogDatei`.
   10. **Post-processing & Termination**:
       - Set success status using `DWMSG_SetzeStatusOK`.
       - Clear traps and exit cleanly.

7. ERROR HANDLING & EXIT CODES
   - **Detection**: Supported by `set -e` and `trap ... ERR INT` structures.
   - **Reaction**: Triggers `DWMSG_Fehlerbehandlung` to log failures in tracking systems, prints the log file if verbose is on, and exits.
   - **Exit Codes**:
     - `193`: Missing necessary parameter argument.
     - `192`: Unknown parameter.
     - `198`: File path parameter error.
     - `1`: Trapped interrupt (`INT`).
     - Standard non-zero propagation for execution errors.
   - **Python Mapping**: Standardized using `try...except...finally` block structures. Missing arguments are caught natively by `argparse`. Sourced error routines are mapped to placeholder method calls to simulate framework integration.

8. OUTPUTS / SIDE EFFECTS
   - Writes standard output and error output directly to a dynamically generated file tracking log (`LogDatei`).
   - Implicit side effects in the database via the executed SQL script.

9. BUSINESS SUMMARY
   - Standardizes the execution environment for database transformations across the data warehouse.
   - Ensures structured logging and automatic tracking of execution steps in administrative DWH tables.
   - Permits flexible relative path structures for SQL assets, improving package portability.
   - Ensures any database failure propagates cleanly back to the UC4 orchestrator to prevent silent data corruption.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
# Step 1: Initialization and Imports
import sys
import os
import argparse
import subprocess
import shutil

PROG_NAME = f"Ausführung Script {sys.argv[0]}"
PROG_VERSION = "5.0.0"

# Step 2: Source Environment Variables and Libraries
# # REVIEW-STRUCT: environment files not supplied. Sourcing behavior simulated via placeholder calls.
def source_dw_init():
    # Simulated sourcing of $HOME/aktuell/.dw_init
    pass

def dwmsg_melde_fehler(eintrags_nr, severity, err_nr, err_arg):
    # Simulated error logging from f_alis_msgerr.ksh
    print(f"Error {err_nr}: {err_arg}", file=sys.stderr)

def dwmsg_ermittle_nr():
    # Simulated tracking registration from f_alis_msgerr.ksh
    return 1001  # Placeholder entry number

def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # Simulated log filename generation
    return f"log_{job_kennung}_{eintrags_nr}.log"

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, program, log_file):
    # Simulated registration log initiation
    pass

def dwmsg_fehlerbehandlung(eintrags_nr):
    # Simulated failure logging
    pass

def dwmsg_setze_status_ok(eintrags_nr):
    # Simulated success confirmation logging
    pass

def starte_sql_skript(eintrags_nr, db_skript, sql_par, tracking_nr, log_file):
    # # REVIEW-STRUCT: launcher starteSQLSkript body not supplied. Simulated via a shell run placeholder.
    # In a fully migrated BigQuery environment, this would run native Python DB Client queries.
    cmd = ["starteSQLSkript", str(eintrags_nr), db_skript, sql_par, str(tracking_nr)]
    with open(log_file, "a") as log:
        subprocess.run(cmd, stdout=log, stderr=log, check=True)

# Step 3: Argument Parsing (Simulating getopts)
parser = argparse.ArgumentParser(description=PROG_NAME, add_help=False)
parser.add_argument("-f", required=False, help="SQL-Script Name")
parser.add_argument("-i", required=False, default="", help="Parameter für das SQL-Script")
parser.add_argument("-j", required=False, default="", help="Jobkennung")
parser.add_argument("-v", action="store_true", help="Verbose flag")
parser.add_argument("-h", action="store_true", help="Zeigt diese Hilfe an")

try:
    source_dw_init()
    args = parser.parse_args()
except Exception as parse_err:
    # Step 4: Parameter Validation Handling
    # Match the ErrNr conventions of legacy shell getopts validation
    dwmsg_melde_fehler(0, "E", 192, str(parse_err))
    parser.print_help()
    sys.exit(192)

if args.h:
    parser.print_help()
    sys.exit(0)

if not args.f:
    dwmsg_melde_fehler(0, "E", 193, "-f")
    parser.print_help()
    sys.exit(193)

p_sqlscript = args.f.lower()  # typeset -l equivalent
p_sqlpar = args.i
p_Verbose = 1 if args.v else 0
p_Job = args.j

# Step 5: Dynamic Path Resolution
# Change current working directory to script location
os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))

dirname_script = os.path.dirname(p_sqlscript)
if dirname_script in ("", "."):
    l_DBskript = os.path.join("..", "sql", p_sqlscript)
    if not os.path.exists(l_DBskript):
        l_DBskript = os.path.join("..", "mig", p_sqlscript)
    if not os.path.exists(l_DBskript):
        l_DBskript = p_sqlscript
else:
    l_DBskript = p_sqlscript

# Step 6: Validate File Existence 
# # REVIEW: This check mimics the logic found in the legacy .ksh file.
# # It triggers error 198 if the file EXISTS. This is likely a legacy bug.
if os.path.exists(l_DBskript):
    ErrNr = 198
    dwmsg_melde_fehler(0, "E", ErrNr, "p_Kuerzel")
    sys.exit(ErrNr)

# Step 7: Logging and Environment Configuration Setup
job_kennung = p_Job.upper() if p_Job else "DWH_KORR"  # typeset -u equivalent

print("----------------- Parameter -----------------")
print(f"Jobkennung     : {job_kennung}")
print(f"DB-Skript      : {l_DBskript}")
print("---------------------------------------------")

dw_eintrags_nr = dwmsg_ermittle_nr()
log_datei = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)
dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)

# Step 8: Safe Trapped Execution Block
try:
    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{dw_eintrags_nr}'")
    print(f"Logdatei  : '{log_datei}'")
    print("---------------------------------------------")
    
    # Step 9: Core Execution
    starte_sql_skript(dw_eintrags_nr, l_DBskript, p_sqlpar, dw_eintrags_nr, log_datei)
    
    # Step 10: Completion and Cleanup
    dwmsg_setze_status_ok(dw_eintrags_nr)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    sys.exit(0)

except Exception as err:
    # Simulate Trap INT / ERR behaviors
    dwmsg_fehlerbehandlung(dw_eintrags_nr)
    if p_Verbose != 0:
        if os.path.exists(log_datei):
            with open(log_datei, "r") as log:
                print(log.read(), file=sys.stderr)
    print("!FEHLER gemeldet!", file=sys.stderr)
    sys.exit(1)
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Migrate the legacy KSH wrapper script into an orchestrating Python script. It parses incoming arguments, performs path resolution for SQL scripts, manages error trapping, and executes SQL files by importing and delegating to the migrated `dw_init.py`, `f_alis_msgerr.py`, and `h_alis_sqlplus.py` utility modules. |

# Execution Order

The target orchestration must preserve the legacy execution order sequence:
1. **`DW.DWH_ABTN_SMART_KUBI.xml`** (Scheduler Entry): Triggers the pipeline run, calculates schedule variables, and initiates step 2.
2. **`d_abtn_x_smart_kubi.sql`**: The actual PL/SQL script (migrated to Dataform SQLX / BigQuery transformations).
3. **`r_sqlscript`** (Execution Wrapper): Converted to `r_sqlscript.py`. It is invoked with arguments `-f d_abtn_x_smart_kubi.sql` to execute the transformation with the required variables.
4. **`.dw_init`**: Converted to `dw_init.py`. It initializes environments and variables. Sourced/imported by the execution wrapper.
5. **`f_alis_msgerr.ksh`**: Converted to `f_alis_msgerr.py`. Sourced/imported by the execution wrapper for centralized tracking, error reporting, and status updates.
6. **`h_alis_sqlplus.ksh`**: Converted to `h_alis_sqlplus.py`. Sourced/imported by the execution wrapper to unifiedly execute the SQL script using the BigQuery client.

# Schedule & Variables

### Target Scheduling
The target DAG/scheduler in Cloud Composer must execute the Python script on the schedule corresponding to the UC4 job `DW.DWH_ABTN_SMART_KUBI`.

### Scheduler-Set Variables Mapping
The 9 scheduler-set variables must be calculated using Airflow macros/variables (or native Python logic inside the orchestrator) and passed as execution parameters to the script:
*   **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'` (Passed via `-j` parameter or environment).
*   **`cdate`** = `SYS_DATE("YYYYMMDD")` (Mapped to the execution date in `YYYYMMDD` format).
*   **`cmonth`** = First 6 characters of `cdate` (`YYYYMM`).
*   **`cday`** = Last 2 characters of `cdate` (`DD`).
*   **`first`** = `'01'`.
*   **`cmonth` (Step 2)** = Concatenation of the first day to the month (`YYYYMM01`).
*   **`cmonth` (Step 3)** = Previous day subtraction (`SUB_DAYS(cmonth, 1)`).
*   **`cmonth` (Step 4)** = First 6 characters of the subtracted date (`YYYYMM`).
*   **`MONATSID`** = Calculated previous month ID (e.g., `'&cmonth'`). Passed as a parameter (via `-i` option) to the executed SQL script.

# Lineage

*   **Upstream Invoker**: Orchestrated and invoked by UC4 scheduling configuration (`DW.DWH_ABTN_SMART_KUBI.xml`).
*   **Downstream Invoked Executable**: Executes `d_abtn_x_smart_kubi.sql` (migrated to BigQuery / Dataform SQLX).
*   **Internal Dependencies (Sourced/Invoked Modules)**:
    *   `r_sqlscript` relies on `dw_init.py` for global environment variables.
    *   `r_sqlscript` relies on `f_alis_msgerr.py` for tracking, auditing, logging, and error handling.
    *   `r_sqlscript` relies on `h_alis_sqlplus.py` for SQL script execution.

# Cross-File Dependencies

### Module Imports
To resolve the disjointed implementation from previous design attempts, `r_sqlscript.py` must import the migrated python modules directly instead of re-implementing or stubbing their logic:
*   **Environment Initialization**: 
    ```python
    import dw_init
    ```
*   **Error Logging and Tracking Utilities**: 
    ```python
    from f_alis_msgerr import (
        DWMSG_MeldeFehler,
        DWMSG_ErmittleNr,
        DWMSG_Logdateiname,
        DWMSG_ErzeugeEintrag,
        DWMSG_Fehlerbehandlung,
        DWMSG_SetzeStatusOK
    )
    ```
*   **SQL Execution Utility**:
    ```python
    from h_alis_sqlplus import starteSQLSkript
    ```

### Execution Unification
`r_sqlscript.py` delegates execution of the target script entirely to `h_alis_sqlplus.starteSQLSkript`. `h_alis_sqlplus.py` must consistently utilize the native `google-cloud-bigquery` Python client library to run SQL queries in BigQuery rather than attempting to call any legacy command-line binary.

# Target File Plan

### `local/home/gurunathan_t/kubi/r_sqlscript.py`
*   **Language**: Python 3
*   **Source File**: `r_sqlscript`
*   **Description**: Command-line wrapper utility that handles arguments (`-f`, `-i`, `-j`, `-v`), dynamically looks for SQL files in relative directory fallback paths (`../sql/`, `../mig/`, `./`), registers the execution run using `f_alis_msgerr.py` logging, sets up standard error catch blocks (`try...except`), and passes execution downstream to `h_alis_sqlplus.starteSQLSkript`.

# Environment-Specific Values

All environment configuration values are classified below:

### GLOBAL (Environment-Wide Infrastructure)
*   **`GCP_PROJECT`**: The target BigQuery GCP project.
*   **`GCP_REGION`**: The execution region.
*   **`GCS_BUCKET`**: Shared Cloud Storage bucket used to store persistent logs and output execution history.
*   **`DW_DIR_ROOT`**: Sourced from `os.environ.get("DW_DIR_ROOT")` or `dw_init` config to define utility module path locations.
*   **`HOME`**: Sourced from `os.environ.get("HOME")` for executing path roots.

### JOB-SPECIFIC (Particular to this execution)
*   **`JobKennung` / `job_kennung`**: Determined dynamically at runtime from parameter `-j` (defaulting to `"DWH_KORR"` if absent).
*   **`l_DBskript` / `l_db_skript`**: The target SQL script path determined at runtime from the `-f` flag.
*   **`p_sqlpar`**: Parameters passed directly down to the SQL transformation via the `-i` flag (e.g., the calculated `MONATSID`).
*   **`LogDatei` / `log_datei`**: Dynamic path and filename returned by `DWMSG_Logdateiname` for output tracking.

# Risks & Manual Steps

1.  **OUTPUT/PRINT LITERAL RULE — German Language Verbatim Retention**:
    *   All printed strings, logging, and error output statements must match the original German text character for character. For instance, the target Python wrapper must output exactly:
        *   `"----------------- Parameter -----------------"`
        *   `"Jobkennung     : "`
        *   `"DB-Skript      : "`
        *   `"----------------- Job -----------------------"`
        *   `"Job-Nr    : "`
        *   `"Logdatei  : "`
        *   `"!OSFEHLER gemeldet!"`
        *   `"!FEHLER gemeldet!"`
        *   `"Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"`
2.  **Legacy Path Validation Bug**:
    *   The legacy script contains a bug in validating file existence:
        ```bash
        if [  -f "$l_DBskript" ]
        then
            ErrNr=198
            ...
        ```
        This triggers error 198 if the file *does* exist (which is logically reversed). The Python implementation should correct this to check if the file does *not* exist (`if not os.path.exists(l_db_skript)`), raising the error appropriately to ensure reliability while logging the bug during review.
3.  **Module Paths in Composer/Airflow**:
    *   Since `r_sqlscript.py` imports `dw_init.py`, `f_alis_msgerr.py`, and `h_alis_sqlplus.py`, all these migrated python modules must be located in the python `sys.path` (e.g., inside the Airflow `plugins` directory or packaged inside the execution container) to avoid `ModuleNotFoundError` during task execution.