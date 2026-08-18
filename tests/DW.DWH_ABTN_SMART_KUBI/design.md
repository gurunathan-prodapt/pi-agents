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


# Migration Design Document: DW.DWH_ABTN_SMART_KUBI

## 1. Overview
This workflow encapsulates a single UC4 Unix job (`DW.DWH_ABTN_SMART_KUBI`) that executes a SQL script to populate a data warehouse temporary table. Prior to running the database query, the job executes logic to determine a reporting month parameter (`&MONATSID`). If the current day of the month is before the 15th, it sets the reporting month to the previous month; otherwise, it uses the current month. The execution uses a custom SQL wrapper utility (`r_sqlscript`). Since this extraction contains only the UNIX job without a surrounding workflow (JOBP) or schedule, it is classified as an externally triggered job.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 | Populate temp table |

## 3. Scheduling
- **Schedule**: `None`
- **Trigger Source**: Externally triggered (source unknown from this extraction alone). There are no schedule (JSCH), workflow (JOBP), or script (SCRI) triggers included in this bundle.

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| `dag_id` | `dw_dwh_abtn_smart_kubi` |
| `schedule` | `None` |
| `start_date` | `datetime(2023, 1, 1)` (placeholder) |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `is_paused_upon_creation` | `False` |
| `default_args` | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_abtn_smart_kubi_task` | `DW.DWH_ABTN_SMART_KUBI` | `EmptyOperator` | N/A | N/A | 1 | 5 mins | None | None | False | None | `#REVIEW-STRUCT:` Launcher wraps SQL script `d_abtn_x_smart_kubi.sql`, which is converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL -- this extraction cannot know which. Confirm the actual artifact produced before wiring a real operator (BashOperator/PythonOperator for Python, BigQueryInsertJobOperator for BigQuery SQL); never assume Python. <br><br>The original script calculates `MONATSID` (reporting month based on execution day) which must be passed to the migrated SQL script. |

## 6. Task Dependency Map
```python
dw_dwh_abtn_smart_kubi_task
```
*(Single-task workflow; no upstream/downstream task dependencies.)*

## 7. Sync / Concurrency Analysis
No `sync_rows` (UC4 locks) are defined for this object in the extraction. Concurrency is governed by the DAG-level `max_active_runs=1` configuration.

## 8. Error Handling and Retry Strategy
- Default failure behavior relies on standard Airflow task retries (configured as `1` retry with a `5` minute delay in `default_args`).
- No native UC4 postconditions or specific error recovery scripts were defined for this job.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&MONATSID` | Calculated based on date logic in script body. | Can be dynamically evaluated using Jinja templating in Airflow (e.g., using a Python parameter or SQL query parameter macro): <br> `{% set run_date = data_interval_end %} {% if run_date.day < 15 %} {% set target = run_date - modules.pandas.DateOffset(months=1) %} {% else %} {% set target = run_date %} {% endif %} {{ target.strftime('%Y%m') }}` |
| `dag_id` | `DW.DWH_ABTN_SMART_KUBI` | `dw_dwh_abtn_smart_kubi` |

## 10. Developer Notes
* **#REVIEW-STRUCT: SQL Script Conversion**: The job wraps the SQL script `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`. This must be migrated into a BigQuery SQL script, a Python/Pandas transformation script, or a Cloud Spanner operation, depending on the target system architecture. Do not assume python execution without validating the migration strategy for this SQL module.
* **#REVIEW-STRUCT: Parameter Generation**: The reporting month calculation logic (`MONATSID`) is currently embedded inside the UC4 script body. Ensure that when migrating this to Airflow, this formula is calculated either via Airflow macro/Jinja expressions or directly within the target SQL/execution operator.
* **Missing Triggering Context**: Since this object is a standalone `JOBS_UNIX` without a `JOBP` wrapper, it has been designed as its own independent DAG. Verify if this should instead be converted to a task-level component inside a larger orchestration pipeline once the parent workflows are extracted.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW: Define GCP project, region, and target credentials if migrated to BigQuery / Dataproc.
GCP_PROJECT = "your-gcp-project-id"
GCP_REGION = "us-east1"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure handlers defined in UC4 extraction.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=DEFAULT_ARGS,
    description="Populate temp table - migrated from DW.DWH_ABTN_SMART_KUBI",
    start_date=datetime(2023, 1, 1), # Placeholder start date
    schedule_interval=None,          # Externally triggered
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_dwh_abtn_smart_kubi_task ────────────────
    # # REVIEW-STRUCT: This launcher runs:
    # # $HOME/aktuell/allgemein/is/util/bin/r_sqlscript -j ABTN_SMART_KUBI -f $HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql -i <MONATSID>
    # # A separate pipeline should convert d_abtn_x_smart_kubi.sql into an actionable script.
    # # Use BigQueryInsertJobOperator or similar target execution operator instead of EmptyOperator.
    # # The variable MONATSID can be generated using Jinja as mapped below:
    #
    # monatsid_jinja_expression = """
    # {% set run_date = data_interval_end %}
    # {% if run_date.day < 15 %}
    #   {% set target = run_date - modules.pandas.DateOffset(months=1) %}
    # {% else %}
    #   {% set target = run_date %}
    # {% endif %}
    # {{ target.strftime('%Y%m') }}
    # """
    
    dw_dwh_abtn_smart_kubi_task = EmptyOperator(
        task_id="dw_dwh_abtn_smart_kubi_task",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task pipeline; no dependencies required.
    dw_dwh_abtn_smart_kubi_task
```

### Execution Order
The execution order of the legacy steps is mapped below to the corresponding target orchestration structure:
* **Step 1: `DW.DWH_ABTN_SMART_KUBI.xml`** maps to the parent Airflow DAG (`DW.DWH_ABTN_SMART_KUBI.py`), which orchestrates the entire flow.
* **Step 2: `d_abtn_x_smart_kubi.sql`** is an external SQL script called by the wrapper; it maps to a target BigQuery query task execution or Dataform SQLX action (migrated separately under its own group).
* **Step 3: `r_sqlscript`** is a generic legacy utility used to execute SQL scripts; in the target environment, this logic is retired, and execution is managed directly via the BigQuery native Airflow operators (e.g., `BigQueryInsertJobOperator`) or Dataform workflow invocations.
* **Step 4: `.dw_init`** is a legacy environment initialization shell script; its role is retired since environmental configuration is managed natively in Google Cloud (e.g., Composer environment variables).
* **Step 5: `f_alis_msgerr.ksh`** is an error-handling and status logging script; its role is retired and replaced by Airflow's built-in task monitoring, logging, and alerting mechanisms.
* **Step 6: `h_alis_sqlplus.ksh`** is a helper script to wrap SQL*Plus; its role is retired since connection handling and query execution are managed natively by BigQuery drivers.

### Schedule & Variables — Must Be Retained
* **Schedule / Trigger**: The job does not define an active timing schedule in its own configuration and is triggered externally.
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`: Passed to the Airflow task as a task-level parameter.
  * Date Calculation Logic:
    * `cdate` = `SYS_DATE("YYYYMMDD")`
    * `cmonth` = `SUBSTR(&cdate,1,6)`
    * `cday` = `SUBSTR(&cdate,7,2)`
    * If `cday` < `'15'`, then `first` = `'01'`, `cmonth` = `&cmonth&first`, `cmonth` = `SUB_DAYS(&cmonth,1)`, `cmonth` = `SUBSTR(&cmonth,1,6)`
    * `MONATSID` = `&cmonth`
    
    This reporting month calculation logic must be dynamically evaluated within the Airflow DAG at runtime. The calculated value for `MONATSID` must be passed to the downstream BigQuery task using native Airflow Jinja templating (e.g., pulling from the execution date macro and adjusting the date context accordingly) or evaluated in a Python task context prior to SQL execution.

### Lineage
* **Upstream / Includes**:
  * Includes `DW.HOLE_PFAD` (unresolved; confirmed by human review to be not needed).
  * Includes `DW.LESE_LOG` (unresolved; confirmed by human review to be not needed).
  * Invokes `SCRIPT:.DW_INIT` (not part of this design group; retired in target).
  * Invokes `FILE:r_sqlscript` (not part of this design group; retired in target).
  * Invokes `FILE:d_abtn_x_smart_kubi.sql` (cross-job hand-off; to be converted separately under its own group).
  * Runs on legacy host `dwhdwh1p` (replaced by Cloud Composer/BigQuery).
  * Uses login/package `DW.UNIX.ISTNS` (replaced by native IAM/service accounts in GCP).

### Cross-File Dependencies
* No direct cross-file code dependencies exist within the source files listed for this design pass, as only a single UC4 XML file is being processed. 
* There is a downstream execution dependency on `d_abtn_x_smart_kubi.sql` which is invoked by the job and must be migrated independently.

### Target File Plan
* **Target File**: `DW.DWH_ABTN_SMART_KUBI.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`

### Environment-Specific Values
* **GLOBAL**:
  * Legacy UNIX Host `DWHDWH1P` (referenced as `|DWHDWH1P|HOST`) maps to the global target execution infrastructure. Normalized as `GCP_PROJECT`, `GCP_REGION`, and Cloud Composer environment configurations.
* **JOB-SPECIFIC**:
  * Legacy UNIX Login `DW.UNIX.ISTNS` is retired; execution roles are governed via the Composer environment's service account.
  * SQL script path `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql` maps to a job-specific target SQL resource location or Dataform action name.
  * Legacy utility path `$HOME/aktuell/allgemein/is/util/bin/r_sqlscript` is retired as native BigQuery operators will execute the migrated queries.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `DW.DWH_ABTN_SMART_KUBI.py` | Migrates the UC4 orchestration logic and date-parameter calculations into an Airflow DAG. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

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
VERDICT: PYTHON
REASON: The script is an environment initialization profile containing directory path declarations, conditional ORACLE_HOME path selection, and sourcing of external scripts, requiring a Python conversion to represent this environment configuration.

EVIDENCE
- Business logic found: none; the script purely defines environment variables, directory paths, and locates ORACLE_HOME.
- AWK: none
- SQL-expressible: no, it consists of shell environment setup, directory existence checks, and sourcing commands.
- Non-SQL side effects: sets and exports environment variables, checks local file system directories, and sources other shell scripts.
- Against this verdict: It could be considered a non-runnable configuration file (NO_CONVERSION_REQUIRED), but it contains conditional logic and sourcing that must be translated into Python environment/config management to support other converted scripts.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `.dw_init` is a KornShell environment initialization profile for an "Information Services" data warehouse system. It sets up and exports global directory paths (`DW_DIR_*`), host names, and resolves the `ORACLE_HOME` directory. It also sources separate global and local configuration scripts (`.dw_global` and `.dw_lokal`) to complete the environment setup.

### 2. INVOCATION CONTEXT
- **Caller**: It is sourced (via `. .dw_init` or equivalent) by other job scripts or UC4 JOBS_UNIX tasks to initialize their runtime environment. No specific UC4 job context is supplied in this extraction.
- **UC4 Native Includes**: None referenced in the extraction.
- **Environment Files Sourced**:
  - `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file $HOME/.dw_global not supplied — variables it sets are unknown; do not guess their names or values
  - `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file $HOME/.dw_lokal not supplied — variables it sets are unknown; do not guess their names or values

### 3. PARAMETERS / INPUTS
- **System / Runtime Variables**:
  - `$HOME` (sourced from shell environment): Base directory path for setting up local variables.
  - `$ORACLE_HOME` (checked for existence, conditionally set): DB engine client path.
  - `$ORACLE_SID` (sourced from shell environment): Used to define the Oracle UTL_FILE directory path.
- **Declared Variables Mapping to Python**:
  All declared variables should be loaded into a configuration dictionary, or injected directly into `os.environ` if executing downstream subprocesses.
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
  - `DW_DIR_IMP_SAP_L_GUTGR = $HOME/daten/sap/sap_l_gutgr` (Note: # REVIEW: export DW_DIR_IMP_SAP_L does not match assigned variable name DW_DIR_IMP_SAP_L_GUTGR; check if this was a legacy typo).
  - `DW_DIR_IMP_L_MAHNSTYP_IST = $HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_FI = $HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_MAHNV_IST = $HOME/daten/sap/mahn`
  - `DW_DIR_IMP_L_GUTGR = $HOME/daten/sd/l_gutschr`
  - `DW_DIR_IMP_L_LEIST = $HOME/daten/sd/l_leist`
  - `DW_DIR_IMP_L_PROD = $HOME/daten/sd/l_prod`
  - `DW_DIR_IMP_LKODE = $HOME/daten/sd/lkode`
  - `DW_DIR_IMP_SUBSE = $HOME/daten/subse`
  - `DW_DIR_SMS_PRG = ${HOME}/aktuell/allgemein/is/util`
  - `DW_DIR_SMS_ADR = ${HOME}/daten/sms/adressen`
  - `DW_DIR_SMS_TMP = ${HOME}/daten/sms/tmp`
  - `DW_DIR_IMP_DPPS = $HOME/daten/dpps`
  - `DW_DIR_IMP_PLANF2 = $HOME/daten/planf2`
  - `DW_HOST_CUSTOMER = dxcst3.bn.detemobil.de`
  - `DW_DIR_UTL_FILE = /appl/local/oracle/admin/$ORACLE_SID/utl_file`

  *Note on Target Platform:* Since the confirmed target platform is `BIGQUERY`, the Oracle-specific configurations (`ORACLE_HOME`, `DW_DIR_UTL_FILE`) are legacy artifacts. In a Google Cloud Platform architecture, these local paths will likely map to Google Cloud Storage (GCS) buckets or datasets.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
No external executable commands are run, but the script sources two configurations:
- `. $HOME/.dw_global`
- `. $HOME/.dw_lokal`

These are non-resolvable external scripts.
# REVIEW-STRUCT: environment file $HOME/.dw_global not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file $HOME/.dw_lokal not supplied — variables it sets are unknown; do not guess their names or values

### 5. EMBEDDED SQL
None.

### 6. CONTROL FLOW
1. **Initialize Directory Paths**: Assign and export all system directory paths (`DW_DIR_*`) relative to the user's home directory.
2. **Assign Remote Customer Host**: Set `DW_HOST_CUSTOMER=dxcst3.bn.detemobil.de`.
3. **Resolve DB Home Directory**:
   If `$ORACLE_HOME` is not already set:
   - Check if directory `/appl/local/oracle/12.2.0.1.0` exists. If so, assign it.
   - Else, check if directory `/appl/local/oracle/11.2.0` exists. If so, assign it.
   - Else, output error messages to stdout.
   Export `$ORACLE_HOME`.
4. **Source Sibling Environment Profiles**: Sourced `.dw_global` and `.dw_lokal`.
5. **Assign DB Directory Parameter**: Define and export `DW_DIR_UTL_FILE=/appl/local/oracle/admin/$ORACLE_SID/utl_file`.

### 7. ERROR HANDLING & EXIT CODES
- Checks for the existence of candidate `$ORACLE_HOME` directories. If none exist, it outputs warning/error statements to standard output.
- The script does not exit or stop execution on ORACLE_HOME failure; it continues to source downstream profiles.
- Python translation: Warn via the Python `logging` library, and raise an `FileNotFoundError` or simply log warnings for missing environments depending on requirements.

### 8. OUTPUTS / SIDE EFFECTS
- Modifies the shell process environment (variables exported).

### 9. BUSINESS SUMMARY
- Serves as the central path directory registry for the Information Services Data Warehouse.
- Establishes directory structure definitions for data imports and exports across multiple business segments (BWA, CTEL, SIGMA, etc.).
- Configures DB client paths for environment tasks.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Import required modules
import os
import sys
import logging
from pathlib import Path

# Configure logging for reporting setup errors
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Step 2: Establish base environment directories
home = os.environ.get("HOME", "")
if not home:
    # Set default home fallback if not present in environment
    home = str(Path.home())

# Step 3: Initialize DW Directory Paths and populate os.environ
os.environ["DW_DIR_ROOT"] = os.path.join(home, "aktuell")
# os.environ["DW_DIR_ROOT"] = os.path.join(home, "isdwh") # Commented out in source
os.environ["DW_DIR_PROT"] = os.path.join(home, "daten/logfiles")
os.environ["DW_DIR_CUBES"] = os.path.join(home, "daten/cubes")

os.environ["DW_DIR_IMP_D1"] = os.path.join(home, "daten/d1")
os.environ["DW_DIR_IMP_BWA"] = os.path.join(home, "daten/dpps/bwa")
os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home, "daten/xtra")
os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home, "daten/ctel")
os.environ["DW_DIR_IMP_VO"] = os.path.join(home, "daten/vo")
os.environ["DW_DIR_IMP_RV"] = os.path.join(home, "daten/rv")
os.environ["DW_DIR_IMP_IF"] = os.path.join(home, "daten/ees")
os.environ["DW_DIR_IMP_NNV"] = os.path.join(home, "daten/nnv")
os.environ["DW_DIR_IMP_SIGMA"] = os.path.join(home, "daten/gd/sigma")
os.environ["DW_DIR_EXP_SIGMA"] = os.path.join(home, "daten/gd/sigma/export")
os.environ["DW_DIR_IMP_TRF"] = os.path.join(home, "daten/trf")
os.environ["DW_DIR_IMP_AUF"] = os.path.join(home, "daten/sd/auf")
os.environ["DW_DIR_IMP_GUT"] = os.path.join(home, "daten/sd/gut")
os.environ["DW_DIR_IMP_KDG"] = os.path.join(home, "daten/sd/kdg")
os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home, "daten/mp/kdg")
os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home, "daten/mp/ts")
os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home, "daten/mp/zm")
os.environ["DW_DIR_IMP_TS"] = os.path.join(home, "daten/sd/ts")
os.environ["DW_DIR_IMP_ZM"] = os.path.join(home, "daten/sd/zm")
os.environ["DW_DIR_EXP"] = os.path.join(home, "daten/exporter")
os.environ["DW_DIR_IMP_BPM"] = os.path.join(home, "daten/bm")
os.environ["DW_DIR_IMP_ZTS"] = os.path.join(home, "daten/zts")
os.environ["DW_DIR_IMP_VRS"] = os.path.join(home, "daten/vrs")

os.environ["DW_DIR_IMP_BRUNET"] = os.path.join(home, "daten/brunet")
os.environ["DW_DIR_IMP_DWH"] = os.path.join(home, "daten/dwh")
os.environ["DW_DIR_IMP_PLATO"] = os.path.join(home, "daten/dwh/plato")

os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home, "daten/carmen")
os.environ["DW_DIR_IMP_SAP"] = os.path.join(home, "daten/sap")
os.environ["DW_DIR_IMP_SR_RV"] = os.path.join(home, "daten/sap/sr_rv_dpps")

# NOTE - Legacy Typo Retention: DW_DIR_IMP_SAP_L_GUTGR is assigned, but exported as DW_DIR_IMP_SAP_L
# # REVIEW: export DW_DIR_IMP_SAP_L does not match assigned variable name DW_DIR_IMP_SAP_L_GUTGR; check if this was a legacy typo.
os.environ["DW_DIR_IMP_SAP_L_GUTGR"] = os.path.join(home, "daten/sap/sap_l_gutgr")
os.environ["DW_DIR_IMP_SAP_L"] = os.environ["DW_DIR_IMP_SAP_L_GUTGR"]

os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = os.path.join(home, "daten/sap/mahn")
os.environ["DW_DIR_IMP_L_MAHNV_FI"] = os.path.join(home, "daten/sap/mahn")
os.environ["DW_DIR_IMP_L_MAHNV_IST"] = os.path.join(home, "daten/sap/mahn")
os.environ["DW_DIR_IMP_L_GUTGR"] = os.path.join(home, "daten/sd/l_gutschr")
os.environ["DW_DIR_IMP_L_LEIST"] = os.path.join(home, "daten/sd/l_leist")
os.environ["DW_DIR_IMP_L_PROD"] = os.path.join(home, "daten/sd/l_prod")
os.environ["DW_DIR_IMP_LKODE"] = os.path.join(home, "daten/sd/lkode")

os.environ["DW_DIR_IMP_SUBSE"] = os.path.join(home, "daten/subse")

os.environ["DW_DIR_SMS_PRG"] = os.path.join(home, "aktuell/allgemein/is/util")
os.environ["DW_DIR_SMS_ADR"] = os.path.join(home, "daten/sms/adressen")
os.environ["DW_DIR_SMS_TMP"] = os.path.join(home, "daten/sms/tmp")

os.environ["DW_DIR_IMP_DPPS"] = os.path.join(home, "daten/dpps")
os.environ["DW_DIR_IMP_PLANF2"] = os.path.join(home, "daten/planf2")

# Step 4: Configure Remote Hosts
os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

# Step 5: Resolve ORACLE_HOME path
# Note: Since the target platform is BIGQUERY, Oracle settings are retained purely for legacy compatibility.
if not os.environ.get("ORACLE_HOME"):
    if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
    elif os.path.isdir("/appl/local/oracle/11.2.0"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
    else:
        print("Fehler in .dw_init:", file=sys.stderr)
        print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

# Step 6: Load sibling environmental files
# # REVIEW-STRUCT: environment file $HOME/.dw_global not supplied — variables it sets are unknown; do not guess their names or values
dw_global_path = os.path.join(home, ".dw_global")
if os.path.exists(dw_global_path):
    # Load parameters from .dw_global into runtime context if possible
    pass

# # REVIEW-STRUCT: environment file $HOME/.dw_lokal not supplied — variables it sets are unknown; do not guess their names or values
dw_lokal_path = os.path.join(home, ".dw_lokal")
if os.path.exists(dw_lokal_path):
    # Load parameters from .dw_lokal into runtime context if possible
    pass

# Step 7: Configure Database UTL_FILE directory path
oracle_sid = os.environ.get("ORACLE_SID", "")
os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
```

### Execution order
The target orchestration (Cloud Composer/Airflow DAG) must preserve the execution sequence from the legacy dependency graph:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (Orchestration entry point)
2. `d_abtn_x_smart_kubi.sql` (PL/SQL execution)
3. `r_sqlscript` (Execution utility)
4. `.dw_init` (Environment initialization — this file, converted to Python)
5. `f_alis_msgerr.ksh` (Error messaging)
6. `h_alis_sqlplus.ksh` (SQL*Plus helper)

In the target environment, the logic from `.dw_init` (migrated to `dw_init.py`) will be executed or imported during the task initialization phase of the execution chain.

### Schedule & variables
The following scheduler variables must be retained and calculated dynamically in the target Cloud Composer environment:
* `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
* `cdate` = `'SYS_DATE("YYYYMMDD")'` (Current system date)
* `cmonth` = `'SUBSTR(&cdate,1,6)'`
* `cday` = `'SUBSTR(&cdate,7,2)'`
* `first` = `'01'`
* `MONATSID` = Calculated based on subtracting 1 day from the first of the month (`&cmonth&first`), then extracting the first 6 characters to yield the last month's ID.

These will be fed into the Python execution context at runtime using Airflow DAG parameters or dynamic context macros.

### Lineage
* **Upstream USES_CONFIG dependencies**:
  * `.dw_init` reads configuration from `.dw_global` (confirmed by human review as not needing a source file migration; values are handled via Airflow variables or global configuration).
  * `.dw_init` reads configuration from `.dw_lokal` (confirmed by human review as not needing a source file migration; values are handled via Airflow variables or global configuration).

### Target file plan
* **Target File Path**: `local/home/gurunathan_t/kubi/dw_init.py`
  * **Language**: Python
  * **Source File**: `local/home/gurunathan_t/kubi/.dw_init`

### Environment-specific values
1. **GLOBAL (Environment-wide)**
   * `HOME` — Maps to standard runtime environment home (`os.environ.get("HOME")` or GCP environment home).
   * `ORACLE_HOME` — Legacy DB setting; retired for BigQuery target execution.
   * `ORACLE_SID` — Legacy DB setting; retired for BigQuery target execution.
   * `DW_DIR_UTL_FILE` — Legacy DB parameter; retired for BigQuery target execution.
   
2. **JOB-SPECIFIC**
   * `DW_HOST_CUSTOMER` = `"dxcst3.bn.detemobil.de"` — Connection parameter.
   * `DW_DIR_ROOT` = `"$HOME/aktuell"`
   * `DW_DIR_PROT` = `"$HOME/daten/logfiles"`
   * `DW_DIR_CUBES` = `"$HOME/daten/cubes"`
   * `DW_DIR_IMP_*` and `DW_DIR_EXP_*` (all import/export paths declared in the source) — Specific local directory routes mapping directly to GCP Cloud Storage equivalents or local directories.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `local/home/gurunathan_t/kubi/dw_init.py` | Converts the KornShell environment script to a Python module to support environment and path variables in the Cloud Composer runtime. |

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
1.1 Identify the type of Oracle SQL object being converted:
    - This is a PL/SQL Anonymous Block script utilizing custom variables, dynamic execution commands, custom error logs, and a standard INSERT-SELECT statement with CTEs, aggregate groups, and ANSI-89 style outer joins.

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script aggregates subscriber and contract data for a specified accounting month (passed via parameter `&1`). It truncates the target table `DWH$TA_T_SMART_KUBI`, extracts data from active tariffs and contract master records matching specific KPI codes (`VVLREIN`, `VVLTWC2C`, `MIGP2CBF`), applies conditional logic for business lines and target identifiers, and inserts the aggregated records into the target table. Finally, it counts and outputs the number of inserted records, handling exceptions through a rollback and logging mechanism.

1.3 List all entities referenced:
    - Tables/Views:
        - `dwh$ta_t_smart_kubi` (Target Table)
        - `dwh$vi_l_map_fa_tarif` (Source, aliased as `T`)
        - `bl_d_tarif` (Source, aliased as `TAR`)
        - `dwh$ta_f_d1_twvv_tn` (Source Fact, aliased as `fact`, partitioned dynamically by month)
        - `dwh$ta_c_vertrag` (Source Customer/Contract, aliased as `d`)
    - Columns:
        - `monats_id`, `kundennummer`, `tarif_id`, `tarif_id_alt`, `vo_kennung`, `test_gp`, `anzahl`, `kennzahl_id`, `dwh_tarif_id`, `gueltig_von`, `gueltig_bis`, `mp_geschaeftsfeld_id`, `t_mobile_kundennummer`, `vo_kenn_bearb`, `vo_kenn`, `zugang`, `dwh_tarif_id_neu`, `dwh_tarif_id_alt`, `dwh_vertrag_id`, `gueltigkeitszeitpunkt`.
    - Dependencies / Packages:
        - `dwpa_util_skript.runstatement` (Dynamic DDL executor)
        - `dwpa_globals.k_alis_err_unknown` (Global error constant)
        - `dwpa_meldung.fehler` (Error handling procedures)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → `INT64`
    - `NUMBER` (without precision) → `INT64` (used for IDs/Counters) or `NUMERIC`
    - `VARCHAR2` → `STRING`
    - `DATE` → `DATE` or `DATETIME` (Oracle date retains time; since filtering is date-bound here, mapped to `DATE`)

2.2 Implicit and Explicit Type Casting:
    - `to_number('&1')` and `to_number('&2')` → Explicit `CAST(p_monats_id AS INT64)` in BigQuery.
    - `TO_DATE('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` → `COALESCE(t_new.tarif_id, 0)`
    - `DECODE(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` → `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - Complex `DECODE(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` →
      ```sql
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn
        WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END
      ```

2.4 String Functions:
    - `ltrim(rtrim(x))` → `TRIM(x)`

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)` → `DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH)`
    - `to_char(fact.gueltigkeitszeitpunkt,'yyyymm')` → `FORMAT_TIMESTAMP('%Y%m', fact.gueltigkeitszeitpunkt)` (assuming column is a timestamp) or `FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt)`.

2.6 Numeric and Aggregate Functions:
    - `sum(fact.zugang)` → `SUM(fact.zugang)` [Directly compatible]

2.7 Analytical and Window Functions:
    - [None used in this script]

2.8 Set and Join Operations:
    - Oracle proprietary joins (`(+)`) → Standardized to `LEFT JOIN` syntax. The outer joins on `d` with supplementary filtering checks must be represented as `LEFT JOIN dwh_ta_c_vertrag d ON fact.dwh_vertrag_id = d.dwh_vertrag_id AND l_monats_date > d.gueltig_von AND l_monats_date <= d.gueltig_bis`.

2.9 Row Limiting and Sampling:
    - [None used in this script]

2.10 Sequences:
    - [None used in this script]

2.11 MERGE Statements:
    - [None used in this script]

2.12 INSERT / UPDATE / DELETE:
    - `INSERT /*+ Append */` → `INSERT INTO ...` (strip optimizer hint)
    - Dynamic truncation logic using utility package is converted directly to a declarative BQ scripting `TRUNCATE TABLE` statement.

2.13 DDL Constructs (if present):
    - Partition specifier in FROM clause: `dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1) fact` is resolved to a standard scan of the table `dwh_ta_f_d1_twvv_tn fact`, filtering on the partitioning columns/dates using the equivalent `WHERE` clause.

2.14 PL/SQL:
    - Anonymous block variables converted to `DECLARE` statements.
    - `SQL%ROWCOUNT` mapped to scripting variable `@@row_count`.
    - `dbms_output.put_line` converted to standard log outputs (e.g. `SELECT` or scripting logging).
    - Exception handling block converted to BQ scripting `EXCEPTION WHEN ERROR THEN` syntax with explicit rollback transaction support.

2.15 Unresolvable or Advisory Items:
    - Custom packages `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` are specific to Oracle infrastructure. These are substituted with standard scripting calls, or custom logging steps flagged for manual schema confirmation.

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - We will convert this PL/SQL block into a standard **BigQuery Scripting Block** (`DECLARE ... BEGIN ... EXCEPTION ... END`). All external configurations and table names containing special character `$` will be regularized using underscores (`_`). Substitution variables (`&1`, `&2`) will be declared as input variables/constants at the beginning of the execution.
3.2 List any assumptions made during conversion:
    - Assumption 1: `&1` corresponds to a monthly identifier represented as a numeric string (e.g., `201509`).
    - Assumption 2: Custom procedures like `dwpa_meldung.fehler` can be emulated or tracked via logging outputs.
3.3 List any items flagged for human review before the build stage proceeds:
    - Substitution parameters (`&1`, `&2`) need to be integrated into the orchestrating pipeline (Airflow, DBT, or BQ stored procedure arguments).

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Oracle Construct | Target Option | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| **PL/SQL Block Syntax** | Direct BigQuery Scripting | Python Wrapper | BQ Scripting supports multi-statement runs, variables, exceptions, and transactions natively. |
| **dwpa_util_skript.runstatement** | Direct BQ SQL (`TRUNCATE TABLE`) | Python Wrapper | Dynamic truncate of static table names is natively accomplished using safe DDL in BQ Scripting. |
| **Oracle Outer Join (`(+)`)** | Direct BQ SQL (`LEFT JOIN ... ON`) | UDF | BigQuery fully supports ANSI standard Left Joins which represent exact functional equivalence. |
| **dwpa_meldung.fehler** | Manual Intervention / BQ Scripting Exception Log | UDF | Custom enterprise logging tables/libraries must be mapped to corporate logging schemas. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **BigQuery SQL Script / Procedure**: The full procedural scripting block that can be run on-demand or compiled as a BigQuery Stored Procedure.

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Type | BigQuery Type | Conversion Rule / Logic | Warning / Annotation |
| :--- | :--- | :--- | :--- |
| `pls_integer` | `INT64` | Native mapping | None |
| `NUMBER` (ID/IDs) | `INT64` | Cast for exact numerical accuracy | Check for possible decimals if schemas change |
| `DATE` | `DATE` / `DATETIME` | Map to `DATE` where context does not involve timestamp fractions | Implicit time-truncation occurs |
| `VARCHAR2` | `STRING` | Standard string representation | Length constraints are ignored in BQ |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: PL/SQL variables, dynamic truncation, subquery CTE (`temp`), implicit Oracle Joins (`(+)`), partition reference, aggregate logic, custom PL/SQL error logging.
- **Unsupported Functions**: Oracle `(+)` joins, `ADD_MONTHS`, custom packages.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Target table `dwh_ta_t_smart_kubi`, source tables/views: `dwh_vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh_ta_f_d1_twvv_tn`, `dwh_ta_c_vertrag`.
- **Warnings**: Ensure substitution variables are correctly populated during the run context of the script.
- **Manual-Intervention Items**: Mapping of custom logger `dwpa_meldung.fehler` to internal target error schema.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(value AS INT64)` / `CAST(value AS NUMERIC)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(date, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE` or `DATE` literals |
| `DECODE` | Direct-with-rewrite | `CASE WHEN ... THEN ... ELSE ... END` |
| `NVL` | Direct-with-rewrite | `COALESCE(...)` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM(...)` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATE(...)` or `CAST(value AS STRING)` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `partition(...)` in FROM clause | Direct-with-rewrite | Filter in `WHERE` clause of main query |
| `/*+ parallel */` / Optimizer Hints | Direct-with-rewrite | Remove comments entirely (BigQuery auto-scales) |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- BigQuery Scripting block translating PL/SQL Logic
-- Replaces substitution parameters &1 and &2 with Script Variables
DECLARE p_monats_id INT64;
DECLARE p_eintrags_nr INT64;

-- Declare internal working variables
DECLARE v_anzahl_ds INT64 DEFAULT 0;  -- converted from pls_integer
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

-- Initialize variables (Assuming target orchestrator supplies values to variables)
-- TODO: Bind these parameters to environment variables during actual deployment pipeline run
SET p_monats_id = 201509; -- Placeholder for &1
SET p_eintrags_nr = 12345; -- Placeholder for &2

SET l_monats_id = p_monats_id; -- converted from to_number('&1')
SET EintragsNr = p_eintrags_nr; -- converted from to_number('&2')

-- l_monats_date DATE := ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1);
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH); 

BEGIN
  -- Start Transaction context for transactional execution safety
  BEGIN TRANSACTION;

  -- lv_str:= 'Truncate table DWH$TA_T_SMART_KUBI'; 
  -- Resolved dynamic execution statement to native BQ statement
  TRUNCATE TABLE dwh_ta_t_smart_kubi; 

  -- Primary insert statement logic
  INSERT INTO dwh_ta_t_smart_kubi
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
  WITH temp AS ( 
      -- CTE resolved from Oracle inline view subquery
      SELECT
          t.tarif_id,
          t.dwh_tarif_id,
          t.gueltig_von,
          t.gueltig_bis,
          tar.mp_geschaeftsfeld_id
      FROM dwh_vi_l_map_fa_tarif AS t
      INNER JOIN bl_d_tarif AS tar
         ON t.tarif_id = tar.tarif_id
      WHERE t.gueltig_bis = DATE '4712-12-31'  -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT 
      l_monats_id AS monats_id,
      -- converted from: Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
      CASE 
          WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
          ELSE d.t_mobile_kundennummer 
      END AS kundennummer,
      COALESCE(t_new.tarif_id, 0) AS tarif_id, -- converted from Nvl(t_new.tarif_id,0)
      COALESCE(t_old.tarif_id, 0) AS tarif_id_alt, -- converted from Nvl(t_old.tarif_id,0)
      -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
      CASE 
          WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn
          WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
          ELSE fact.vo_kenn_bearb
      END AS vo_kennung,
      d.test_gp, 
      SUM(fact.zugang) AS anzahl, 
      fact.kennzahl_id 
  FROM dwh_ta_f_d1_twvv_tn AS fact -- partition reference removed; dynamic scanning maps to table directly
  LEFT JOIN temp AS t_new 
         ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT JOIN temp AS t_old 
         ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT JOIN dwh_ta_c_vertrag AS d 
         ON fact.dwh_vertrag_id = d.dwh_vertrag_id
        -- Standardized Outer Join constraint logic inside ON clause
        AND l_monats_date > d.gueltig_von
        AND l_monats_date <= d.gueltig_bis
  WHERE 
      -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
      FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)
      AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
      CASE 
          WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
          ELSE d.t_mobile_kundennummer 
      END, 
      COALESCE(t_new.tarif_id, 0), 
      COALESCE(t_old.tarif_id, 0), 
      CASE 
          WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn
          WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
          ELSE fact.vo_kenn_bearb
      END, 
      d.test_gp, 
      fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count; -- converted from SQL%ROWCOUNT
  COMMIT TRANSACTION;

  -- Output results log for observability (equivalent to dbms_output.put_line)
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Exception block equivalents to handle transactional rollbacks
  ROLLBACK TRANSACTION;
  
  -- Handle reporting log mechanism
  -- Replaces custom package "dwpa_meldung.fehler" with structured variable captures and re-throw
  DECLARE err_msg STRING;
  DECLARE err_code STRING;
  SET err_msg = @@error.message;
  SET err_code = @@error.statement_text;
  
  -- Record Exception metadata log (placeholder to emulate internal db logging behavior)
  SELECT 
      'F' AS error_severity,
      EintragsNr AS eintrags_nr,
      err_msg AS error_message,
      err_code AS statement_context;

  -- Re-throw the execution exception
  RAISE USING message = err_msg;
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic Execution & Substitution Variables (`&1`, `&2`)**: BigQuery does not natively support SQL*Plus styling substitution prompts (`&1`, `&2`). Ensure parameters are mapped as formal variables/arguments inside the scheduling tool (e.g., Google Cloud Composer/Airflow or dbt variables).
2. **Metadata Error Logging Package (`dwpa_meldung.fehler`)**: This custom logging utility does not exist in BigQuery. We have captured the metadata within an error logging statement. Humans should review if a formal target table is desired to track procedure failures in BigQuery.
3. **Partition Selection Hint**: The Oracle source accesses specific partition structures (`partition(dwh$ta_f_d1_twvv_tn_&1)`). BigQuery performs partition pruning automatically when filtering by partition columns in the `WHERE` statement. Ensure `fact.gueltigkeitszeitpunkt` is designated as the partitioning column inside the BigQuery table definitions for `dwh_ta_f_d1_twvv_tn`.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `kubi/d_abtn_x_smart_kubi.sql` | PL/SQL script converted into a native BigQuery SQL Scripting block (`DECLARE...BEGIN...EXCEPTION...END`) to preserve execution logic, variable bindings, transaction scope, and error handling. |

### Execution order
The legacy execution sequence consists of the following 6 steps:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 orchestration job)
2. `d_abtn_x_smart_kubi.sql` (PL/SQL aggregation script)
3. `r_sqlscript` (SQL*Plus execution wrapper)
4. `.dw_init` (Environment initialization shell script)
5. `f_alis_msgerr.ksh` (Error tracking shell script)
6. `h_alis_sqlplus.ksh` (Helper script executing SQL*Plus commands)

**Mapping to Target Orchestration (Cloud Composer/Airflow):**
* Step 1 (`DW.DWH_ABTN_SMART_KUBI.xml`) is replaced by an Airflow DAG. This DAG orchestrates the task flow.
* Step 2 (`d_abtn_x_smart_kubi.sql`) is translated into a BigQuery scripting query (`kubi/d_abtn_x_smart_kubi.sql`) and is executed via the `BigQueryInsertJobOperator`.
* Steps 3, 4, 5, and 6 (`r_sqlscript`, `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`) represent wrapper mechanisms and standard environment setup. These wrapper behaviors are retired; standard Airflow operators and BigQuery native execution handle logging, connection pooling, and error handling directly, unless explicit business logic is owned by separate wrapper design passes.

### Schedule & variables
The legacy scheduler-set variables must be calculated at runtime in the orchestrating Airflow DAG and injected into the BigQuery task.
* **Legacy Variables:**
  * `DWH_JOB_KENNUNG = 'ABTN_SMART_KUBI'`
  * `cdate = 'SYS_DATE("YYYYMMDD")'` (Current system date in YYYYMMDD format)
  * `cmonth = 'SUBSTR(&cdate,1,6)'` (Year and Month of system date)
  * `cday = 'SUBSTR(&cdate,7,2)'` (Day of system date)
  * `first = '01'`
  * `cmonth = '&cmonth&first'` (System Month concatenated with '01')
  * `cmonth = 'SUB_DAYS(&cmonth,1)'` (Subtract one day to step into the previous month)
  * `cmonth = 'SUBSTR(&cmonth,1,6)'` (Extract the YYYYMM format of the previous month)
  * `MONATSID = '&cmonth'` (Assigned as the active reporting month ID)
* **Target Mapping:**
  * Airflow will dynamically resolve `MONATSID` to represent the prior calendar month. For example, using Airflow macros:
    `MONATSID = "{{ (execution_date.in_timezone('Europe/Berlin')).replace(day=1) - macros.timedelta(days=1) }}".strftime('%Y%m')`
  * The calculated value is passed as a query parameter (`p_monats_id`) during the invocation of the BigQuery script.

### Lineage
* **Upstream Data Sources (Read):**
  * `dwh$vi_l_map_fa_tarif` (View)
  * `bl_d_tarif` (Table)
  * `dwh$ta_f_d1_twvv_tn` (Fact Table) - Partitioned dynamically on database queries as `dwh$ta_f_d1_twvv_tn_<MONATSID>`. BigQuery automatic partition pruning will replace the Oracle `partition(...)` syntax.
  * `dwh$ta_c_vertrag` (Contract/Customer Table)
* **Downstream Consumers (Write):**
  * `dwh$ta_t_smart_kubi` (Target table populated by this job)
* **Legacy Code Packages:**
  * `DWPA_UTIL_SKRIPT` (Oracle package used to dynamically truncate tables)
  * `DWPA_MELDUNG` (Oracle package used for logging exceptions)

### Target file plan
* **Target File:** `kubi/d_abtn_x_smart_kubi.sql`
  * **Source File:** `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  * **Language:** BigQuery SQL
  * **Purpose:** Serves as the main business logic script. Contains variable declarations, parameter bindings, a direct `TRUNCATE TABLE` command (replacing dynamic SQL utilities), and the primary SELECT-INSERT aggregation using native `LEFT JOIN` operations (replacing Oracle `(+)` syntax).

### Environment-specific values
* **GCP_PROJECT** (GLOBAL)
  * *Legacy Context:* Default database environment context.
  * *Target Mechanism:* Used as a global prefix for all table/dataset references. Sourced at runtime via `GCP_PROJECT = os.environ.get("GCP_PROJECT")` or template parameters.
* **BQ_DATASET** (GLOBAL)
  * *Legacy Context:* Legacy schema qualifiers.
  * *Target Mechanism:* Sourced as a global variable (e.g. `Variable.get("BQ_DATASET")` in Airflow) to qualify datasets containing source and target tables.
* **MONATSID / p_monats_id** (JOB-SPECIFIC)
  * *Legacy Context:* Parameter `&1` (Reporting Month).
  * *Target Mechanism:* Sourced from Airflow DAG task parameter mappings and passed as a query parameter.
* **EintragsNr / p_eintrags_nr** (JOB-SPECIFIC)
  * *Legacy Context:* Parameter `&2` (Log entry identifier).
  * *Target Mechanism:* Sourced from Airflow DAG run context (e.g., `{{ run_id }}`) and passed as a query parameter.

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

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
REASON: The script is a reusable library of utility routines for error management and metadata status tracking, which must be converted into a Python utility module to be imported and reused by other converted scripts.

EVIDENCE
- Business logic found: KSH custom logic. The script defines multiple helper functions (`DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, etc.) that orchestrate logging, status updates, timing additions, and unique sequence number generation.
- AWK: none
- SQL-expressible: No. While it makes database calls to manage metadata, the script's core purpose is to serve as a reusable shell library containing control-flow functions, file manipulation, dynamic variable evaluation (`eval`), and string formatting.
- Non-SQL side effects: Creates and cleans up temporary local files, formats file names based on system date, and evaluates environment variable references dynamically.
- Against this verdict: Translating these functions into BigQuery stored procedures might be possible, but since they are designed to be sourced and called as system utility hooks within shell scripts, converting them into a Python utility module is the only way to preserve the same architectural call patterns for converted ETL processes.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) is a reusable utility library designed for unified logging, status tracking, and error handling within the "Information Services" DWH framework. Rather than running as a standalone job, it provides a standard suite of KornShell helper functions that other ETL wrapper scripts source and call. These functions communicate execution statuses (Success, Error, Processing Dates, and Duration) to a central metadata/monitoring database schema (`BERT_MELDUNG`) by invoking Oracle SQL*Plus scripts, though the target platform is confirmed as BigQuery.

2. INVOCATION CONTEXT
   - **Caller:** This file is sourced (using `. f_alis_msgerr.ksh` or similar) by other ksh orchestration scripts. It does not run as a standalone UC4 Unix job itself, but its functions execute as critical lifecycle hooks during actual UC4 job runs.
   - **UC4 Includes:** None referenced directly in this helper script.
   - **Environment Files Sourced:** None sourced internally, but it depends on the caller having initialized variables such as `$DW_ORAUSER`, `$DW_DIR_ROOT`, and `$DW_DIR_PROT`.

3. PARAMETERS / INPUTS
   The utility functions depend on several key environment variables that must be available in the runtime context:
   - `DW_ORAUSER` (env var): Used as the connection credential string. Since the confirmed target platform is BigQuery, this will map to BigQuery client credentials / project settings.
   - `DW_DIR_ROOT` (env var): Specifies the root directory of the application code. Used to construct paths to SQL scripts. In Python, this can be represented via environment variables (`os.environ.get("DW_DIR_ROOT")`) or resolved dynamically relative to the module path.
   - `DW_DIR_PROT` (env var): Specifies the directory where logs/protocols are stored. Used during log file name generation.
   - Function-specific positional arguments ($1, $2, etc.): Surfaced as normal Python function arguments.

   Additionally, there are no companion GDE environment parameters files provided, but the database connection variables (`DW_ORAUSER`) are treated as standard cross-referenced conventions.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Used extensively across functions to execute wrapper SQL files (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, `d_alis_spaufruf_p4.sql`, etc.) which trigger Oracle PL/SQL stored procedures on the `BERT_MELDUNG` package.
     - *Python Target:* In BigQuery, these must become native DB-client calls using the `google.cloud.bigquery` library. We assume these Oracle PL/SQL package procedures are converted into BigQuery Stored Procedures.
   - `cat`: Used to read sequence numbers from temporary files.
     - *Python Target:* Replaced by direct in-memory variable assignment or direct return of query execution results.
   - `tr`: Used to strip whitespace.
     - *Python Target:* Replaced by the native `.strip()` string method.
   - `rm`: Used to delete temp files.
     - *Python Target:* Avoided entirely by executing in-memory database calls.
   - `date`: Used for timestamp formatting.
     - *Python Target:* Replaced by the `datetime` module.

5. EMBEDDED SQL
   The script contains both inline SQL/PL-SQL calls and references to parameterized SQL scripts.
   - **`DWMSG_SetzeStichtagInfo` inline PL/SQL:**
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     *Statement Type:* PL/SQL stored procedure call modifying metadata tables.
     *BigQuery Equivalent:* Call to a migrated stored procedure:
     `CALL {{project_id}}.dataset.SetzeZusatzInfos(dwmsg_eintrags_nr, PARSE_DATE(stichtag_fmt, stichtag));`
   - **`DWMSG_AppendTimingInfos` inline PL/SQL:**
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     *Statement Type:* PL/SQL stored procedure call.
     *BigQuery Equivalent:*
     `CALL {{project_id}}.dataset.SetzeZusatzInfos(dwmsg_eintrags_nr, NULL, CONCAT(info_text, ' ', FORMAT_DATE(date_format, CURRENT_DATE())));`
   - **External SQL Script references:**
     - `d_alis_spaufruf_p1.sql` (invokes `BERT_MELDUNG.SetzeStatusOk` or `BERT_MELDUNG.SetzeStatusAbbruch`)
     - `d_al_is_ermittlenr.sql` (selects a new unique sequence number)
     - `d_alis_spaufruf_p4.sql` (invokes `BERT_MELDUNG.Erzeuge_Eintrag`)
     - `d_alis_spaufruf_p[3/4/5].sql` (invokes `BERT_MELDUNG.Fehler` dynamically)
     
     # REVIEW-STRUCT: external SQL script structures not supplied in extraction — the BigQuery SQL translations below assume direct equivalents are deployed as stored procedures within the BigQuery project.

6. CONTROL FLOW
   The utility functions must be mapped to Python module-level functions with the following logical flow:
   1. **`dwmsg_fehlerbehandlung(dwmsg_eintrags_nr)`**
      - Captures the active exception/return code (defaulting to a fatal code `10` if unspecified).
      - Calls `dwmsg_melde_fehler` to register a fatal error message.
      - Sets the job run state to "Aborted" via `dwmsg_setze_status_abbruch`.
   2. **`dwmsg_setze_status_ok(dwmsg_eintrags_nr)`**
      - Validates that `dwmsg_eintrags_nr` is provided.
      - Invokes `BERT_MELDUNG.SetzeStatusOk` in BigQuery.
   3. **`dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)`**
      - Validates that `dwmsg_eintrags_nr` is provided.
      - Invokes `BERT_MELDUNG.SetzeStatusAbbruch` in BigQuery.
   4. **`dwmsg_ermittle_nr()`**
      - Generates a unique execution tracking number (originally retrieved from a DB sequence via `d_al_is_ermittlenr.sql` and output to `/tmp/ErmittleNr_$$`).
      - In Python, this directly returns the value fetched from the BigQuery query.
   5. **`dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, log_datei)`**
      - Validates that `dwmsg_eintrags_nr` is provided.
      - Invokes `BERT_MELDUNG.Erzeuge_Eintrag` in BigQuery.
   6. **`dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2="")`**
      - Validates `dwmsg_eintrags_nr` is provided.
      - Resolves the parameter counts to conditionally format the stored procedure call to `BERT_MELDUNG.Fehler` in BigQuery.
   7. **`dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr)`**
      - Constructs and returns a formatted log path string: `{DW_DIR_PROT}/{job_kennung}_{YYYYMMDD_HHMM}_{dwmsg_eintrags_nr}.log` using Python's `datetime` module.
   8. **`dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, stichtag, stichtag_fmt)`**
      - Validates all input variables are present.
      - Invokes `BERT_MELDUNG.SetzeZusatzInfos` in BigQuery passing the parsed date.
   9. **`dwmsg_append_timing_infos(dwmsg_eintrags_nr, info_text, date_format)`**
      - Validates input variables.
      - Invokes `BERT_MELDUNG.SetzeZusatzInfos` in BigQuery appending the formatted current time.

7. ERROR HANDLING & EXIT CODES
   - **Legacy design:** If critical parameters are missing within functions, the script prints an "Argh!" error to standard output and immediately exits with `exit 1` or `exit 2`.
   - **Python translation:** Instead of hard-terminating the entire process with `sys.exit()` (which would stop the calling script abruptly without clean exception handling), the Python equivalents should raise `ValueError` for parameter validations. This allows calling orchestrators to catch exceptions, perform local cleanups, and run their own `finally` block or let the main handler catch it.
   - For database errors, BigQuery client library exceptions (`google.cloud.exceptions.GoogleCloudError`) will be raised and propagated.

8. OUTPUTS / SIDE EFFECTS
   - BigQuery monitoring tables (under the migrated equivalent of the `BERT_MELDUNG` package) are updated with real-time job execution telemetry.
   - No local temporary files `/tmp/ErmittleNr_*` are written anymore.

9. BUSINESS SUMMARY
   - Standardizes telemetry and orchestration logging across the DWH architecture.
   - Provides consistent auditing trails (start times, process status, file names, processing dates) for all loaded data assets.
   - Guarantees that any unexpected system aborts or script failures are immediately logged as "Fatal" in the operational database.
   - Enables fine-grained duration logging of individual processing pipeline segments.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Initialize BigQuery client
# # REVIEW: BigQuery project and dataset for metadata tracking is assumed to be configured in environment
BQ_PROJECT = os.environ.get("BQ_PROJECT", "{{project_id}}")
BQ_DATASET = os.environ.get("BQ_METADATA_DATASET", "metadata_dataset")
client = bigquery.Client()

def _execute_procedure(proc_name: str, params: list):
    """Helper utility to invoke BigQuery procedures representing the BERT_MELDUNG package."""
    param_placeholders = ", ".join(["%s" for _ in params])
    # For BigQuery, we use standard CALL syntax or query-based parameters
    query = f"CALL `{BQ_PROJECT}.{BQ_DATASET}.{proc_name}`({param_placeholders})"
    
    # Configure query jobs parameter mapping depending on standard BQ practices
    # Here mapped conceptually as standard parameter-driven call
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING" if isinstance(p, str) else "INT64", p) 
            for p in params
        ]
    )
    try:
        query_job = client.query(query.replace("%s", "?"), job_config=job_config)
        query_job.result()  # Wait for procedure to complete
    except GoogleCloudError as e:
        print(f"Database error executing procedure {proc_name}: {e}", file=sys.stderr)
        raise

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, last_error_code=10):
    """
    Handles errors caught by traps in the calling script.
    Registers a fatal entry and sets job status to Aborted.
    """
    # Note: KSH captures $? into FehlerNr. In Python, this is passed as last_error_code.
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    
    # Trigger fatal error entry
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", last_error_code, f"ErrorCode ist: {last_error_code}")
    
    # Set run status to aborted
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    """Sets execution record state to successful."""
    # Mandatory Audit Step Check:
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    # Call migrated stored procedure
    _execute_procedure("BERT_MELDUNG_SetzeStatusOk", [dwmsg_eintrags_nr])

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    """Sets execution record state to aborted."""
    # Mandatory Audit Step Check:
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    # Call migrated stored procedure
    _execute_procedure("BERT_MELDUNG_SetzeStatusAbbruch", [dwmsg_eintrags_nr])

# Step 4: DWMSG_ErmittleNr
# # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
def dwmsg_ermittle_nr() -> str:
    """
    Retrieves a unique tracking sequence ID from the metadata DB.
    Replaces temporary file logic and returns the value directly.
    """
    query = f"SELECT `{BQ_PROJECT}.{BQ_DATASET}.GetUniqueEintragsNr`()"
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return str(row[0]).strip()
    except GoogleCloudError as e:
        print(f"Error fetching unique sequence number: {e}", file=sys.stderr)
        raise

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, log_datei):
    """Creates a new run entry in tracking tables."""
    # Mandatory Audit Step Check:
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    _execute_procedure("BERT_MELDUNG_Erzeuge_Eintrag", [dwmsg_eintrags_nr, job_kennung, programmname, log_datei])

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """Logs an application or environment warning/error message."""
    # Mandatory Audit Step Check:
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    # Pass default empty strings if optional parameters not supplied, matching standard procedure parameters
    _execute_procedure("BERT_MELDUNG_Fehler", [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2])

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr) -> str:
    """Generates standard tracking protocol file path."""
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{timestamp}_{dwmsg_eintrags_nr}.log"
    return filename

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, stichtag, stichtag_fmt):
    """Logs processing date and its date format to the run execution log."""
    # Mandatory Audit Step Checks:
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNr")
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        raise ValueError("Missing Stichtag")
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        raise ValueError("Missing Stichtag Format", 2)

    # Convert the stichtag into standard datetime/date using format provided
    # Python format and BQ format differ, but passing strings to BQ's PARSE_DATE or PARSE_DATETIME works.
    _execute_procedure("BERT_MELDUNG_SetzeZusatzInfos_Stichtag", [dwmsg_eintrags_nr, stichtag, stichtag_fmt])

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, info_text, date_format):
    """Appends duration or timestamp info to supplementary run details."""
    # Mandatory Audit Step Checks:
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNr")
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        raise ValueError("Missing Date Format", 2)

    # Execute BigQuery update helper
    _execute_procedure("BERT_MELDUNG_SetzeZusatzInfos_Timing", [dwmsg_eintrags_nr, info_text, date_format])
```

# MIGRATION DESIGN DOCUMENT: DW.DWH_ABTN_SMART_KUBI

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Converted from a KornShell utility library to a reusable Python module containing equivalent status tracking, auditing, and error handling functions utilizing the Google Cloud BigQuery client library. |

***

## Execution order
In the target environment, the execution sequence of this job must preserve the original operational dependency graph. The utility script `f_alis_msgerr.ksh` is sourced or called during the execution of:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (Orchestration entry)
2. `d_abtn_x_smart_kubi.sql` (Main database transformations)
3. `r_sqlscript` (Database wrapper execution)
4. `.dw_init` (Environment setup)
5. `f_alis_msgerr.ksh` (Error and telemetry logger)
6. `h_alis_sqlplus.ksh` (SQL*Plus helper)

*Target Orchestration Mapping:* Rather than sourcing a shell utility library, the migrated Python operators and tasks in Cloud Composer will import the Python equivalent helper methods from `f_alis_msgerr.py` to handle logging, metadata registration, and task state updates.

***

## Schedule & variables
The schedule-set variables from the source scheduling environment must be retained and dynamically fed into the migrated job via Airflow DAG parameters or runtime context:
* **Scheduler-Set Variables:**
  - `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'` 
  - `cdate` = `'SYS_DATE("YYYYMMDD")'` (Mapped to Airflow's `{{ ds_nodash }}`)
  - `cmonth` = `'SUBSTR(&cdate,1,6)'` (Calculated dynamically in DAG/Python runtime)
  - `cday` = `'SUBSTR(&cdate,7,2)'` (Calculated dynamically in DAG/Python runtime)
  - `first` = `'01'` (Constant string)
  - `MONATSID` = `'&cmonth'` (Resolved dynamically after execution date derivation)

These variables must be passed to the processing tasks and logged to the central audit metadata tables via `dwmsg_setze_stichtag_info` or `dwmsg_erzeuge_eintrag` function calls.

***

## Lineage
* **Upstream Producers:** Sourced and executed within runtime wrapper contexts by the main pipeline operators.
* **Downstream Consumers / Call Chains:**
  - `PROCEDURE:SETZEZUSATZINFOS` (Lineage confidence: 0.75) — Invoked via the metadata package `BERT_MELDUNG` (which translates to equivalent BigQuery stored procedure calls).

***

## Cross-file dependencies
* **Shared Tables & Metadata Schemas:** This utility depends on a centralized audit/telemetry database schema containing tables originally accessed via the `BERT_MELDUNG` package procedures (`SetzeStatusOk`, `SetzeStatusAbbruch`, `Erzeuge_Eintrag`, `Fehler`, `SetzeZusatzInfos`). Equivalent BigQuery stored procedures must be available in the target BigQuery environment.
* **Call Chains:** Sourced by other components of this DWH job; any converted Python scripts representing the wrapper or orchestration logic will have a direct import dependency on `f_alis_msgerr.py`.

***

## Target file plan

* **Target File Path:** `local/home/gurunathan_t/kubi/f_alis_msgerr.py`
  - **Source File:** `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
  - **Language:** Python
  - **Description:** Contains the converted telemetry, logging, and error management functions utilizing the BigQuery Client library to execute metadata logging stored procedures.

***

## Environment-specific values

Each environment-specific value is classified by its role in the target architecture:

### 1. GLOBAL (Environment-Wide)
These represent standard infrastructure and path settings shared across all pipelines in the deployment environment:
* **`DW_ORAUSER`**: Identifies the database connection and execution project/credentials. 
  - *Target Reference:* Sourced from Airflow's `Variable.get("GCP_PROJECT")` or default service credentials.
* **`DW_DIR_ROOT`**: The root directory path of the application resources.
  - *Target Reference:* Sourced via `os.environ.get("GCS_BUCKET")` or Airflow config pointing to the Cloud Storage deployment bucket.
* **`DW_DIR_PROT`**: The target execution logs/protocol storage path.
  - *Target Reference:* Sourced via `os.environ.get("LOG_BUCKET_PATH")` or `Variable.get("LOG_BUCKET_PATH")` to direct logging outputs to a dedicated Cloud Storage directory.

### 2. JOB-SPECIFIC
* **`DWH_JOB_KENNUNG` / `JobKennung`**: The unique job identifier (`'ABTN_SMART_KUBI'`).
  - *Target Reference:* Hardcoded or passed dynamically as a DAG task parameter.

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

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
REASON: The script is a KornShell utility library defining a reusable SQL execution function with file accessibility checks, parameter validation, and error reporting that cannot be translated into pure BigQuery SQL.

EVIDENCE
- Business logic found: None (this is a technical helper/orchestration utility wrapper around database script execution).
- AWK: none
- SQL-expressible: no (the script manages filesystem checks `[ ! -r $p_Skript ]` and external client invocations).
- Non-SQL side effects: checks file readability and invokes database command-line clients.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a KornShell helper library designed to wrap SQL*Plus script execution. Its primary purpose is to ensure that required execution parameters are provided and that target SQL script files are present and readable before attempting to execute them. In the target BigQuery environment, this logic will be represented as a Python helper module executing SQL files via the Google Cloud BigQuery client.

2. INVOCATION CONTEXT
   - Who calls this script: It is designed to be sourced or called as a library by other shell scripts (which are themselves triggered by UC4 jobs). No specific UC4 job context is supplied in this extraction.
   - UC4 includes: None referenced in the extraction.
   - Environment files sourced: None explicitly sourced inside this snippet, but it relies on external functions (`DWMSG_MeldeFehler`) and variables (`DW_ORAUSER`) that must be established by the calling context.
     - `# REVIEW-STRUCT: external function [DWMSG_MeldeFehler] not supplied — behavior/implementation unknown; must be mapped to a standard Python logging/alerting library or custom equivalent.`

3. PARAMETERS / INPUTS
   - `p_Eintragsnr` ($1): Positional argument representing the unique error tracking/logging entry number. Used in error reporting functions. Maps to a function argument in Python.
   - `p_Skript` ($2): Positional argument representing the path to the SQL script file to execute. Maps to a function argument in Python.
   - Remaining arguments (`$*` after shifting): Dynamic positional parameters passed directly to the SQL script. In BigQuery, these should be handled as query parameters or template substitutions.
   - `DW_ORAUSER` (env var): Used in the original context as the connection identifier for Oracle SQL*Plus.
     - Since the target platform is confirmed as **BIGQUERY**, connection details will instead be handled via BigQuery credentials / Client configurations.
     - `# REVIEW-STRUCT: connection parameters inferred from legacy environment — confirm target BigQuery project/dataset mapping and authenticate via Service Account or ADC.`

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Invokes Oracle SQL*Plus to execute the specified script with arguments, redirecting stdin from `/dev/null` to prevent interactive hangs.
     - Target platform mapping: Because the target platform is confirmed as **BIGQUERY**, this should NOT remain a subprocess invocation of `sqlplus`. Instead, it must become a native Python BigQuery client call (`google.cloud.bigquery.Client().query()`) executing the SQL contents of the file.
     - `# REVIEW: BigQuery standard SQL does not support SQL*Plus style positional command line arguments (&1, &2, etc.) natively; target SQL scripts must be adapted to use named query parameters or format placeholders.`

5. EMBEDDED SQL
   - No embedded SQL is defined directly in this script. The SQL exists in the external files referenced by the `p_Skript` parameter.

6. CONTROL FLOW
   1. Initialize module variables: `ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`. (Note: The script has a minor typo `Modul_Name` and `Modul_Version` in the `DWMSG_MeldeFehler` call, which is preserved/documented in the audit checklist).
   2. Define `starteSQLSkript` function accepting parameters.
   3. Check parameters $1 and $2. If either is empty, call error reporting and return 196.
   4. Check if the SQL script file ($2) exists and is readable. If not, call error reporting and return 201.
   5. Output execution logs showing the script name and its arguments.
   6. Disable "exit on error" behavior (`set +e`) to safely capture the database client's return code.
   7. Execute the SQL script (migrating from Oracle SQL*Plus to Google Cloud BigQuery API).
   8. Capture the execution exit code.
   9. Re-enable "exit on error" behavior (`set -e`).
   10. Return the captured exit code.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments: Calls `DWMSG_MeldeFehler` with code 196 and returns 196.
   - Unreadable/Missing script file: Calls `DWMSG_MeldeFehler` with code 201 and returns 201.
   - Execution failure: The return code from the database script run is captured and returned.
   - Python mapping: Positional/argument validation should raise standard Python exceptions (`ValueError` for missing arguments, `FileNotFoundError` for unreadable files), or return structured error codes if maintaining a legacy API contract is required. Database errors will raise `google.cloud.exceptions.GoogleCloudError`.

8. OUTPUTS / SIDE EFFECTS
   - Writes informational trace/log output to stdout.
   - Side effects: Executes the queries inside the specified SQL script file against the BigQuery database.

9. BUSINESS SUMMARY
   - Provides a standardized wrapper interface for safely invoking database SQL scripts.
   - Guarantees validation of administrative parameters (error numbers) and script paths before database interaction begins.
   - Centralizes the logging of SQL script starts and captures DB-client execution statuses for consistent error analysis.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import logging
from pathlib import Path

# Module Level Metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Setup standard logging
logger = logging.getLogger(MODUL_NAME)

def dwmsg_meldefehler(eintrags_nr, severity, error_code, message):
    """
    Placeholder for the legacy DWMSG_MeldeFehler error-reporting utility.
    # REVIEW-STRUCT: external function [DWMSG_MeldeFehler] not supplied — behavior/implementation unknown
    """
    logger.error(f"Error {error_code} ({severity}) for Entry {eintrags_nr}: {message}")


def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Starts a SQL script execution against the target BigQuery database.
    """
    # Step 1: Validate required parameters
    # MANDATORY AUDIT STEP CHECKLIST:
    # Legacy guard: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        # Note the original typo Modul_Name / Modul_Version in legacy shell code is corrected here
        dwmsg_meldefehler(
            p_eintragsnr, 
            "E", 
            196, 
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        )
        return 196

    # Step 2: Validate file existence and readability
    # Legacy guard: if [ ! -r $p_Skript ]
    script_path = Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        dwmsg_meldefehler(p_eintragsnr, "E", 201, str(p_skript))
        return 201

    # Step 3: Log invocation configurations
    print("Rufe SQL-Skript auf mit folgenden Einstellungen")
    print(f"Sql-Skript : {p_skript}")
    print(f"Skript-Parameter: {list(args)}")

    # Step 4: Execute SQL script content via BigQuery API
    # Since TARGET_PLATFORM is BIGQUERY, we do not call subprocess sqlplus.
    try:
        from google.cloud import bigquery
        from google.cloud.exceptions import GoogleCloudError

        # Read SQL file contents
        with open(script_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Initialize BigQuery Client
        # Authenticates using environment-provided Application Default Credentials (ADC)
        client = bigquery.Client()

        # Execute query
        # # REVIEW: BigQuery does not natively support SQL*Plus script arguments (e.g. &1, &2).
        # # If args are provided, they must be formatted, templated or passed as query parameters.
        if args:
            print("Warning: Positional arguments provided to BigQuery SQL script. Ensure query handles templating.")
            # Simple positional mapping or parameter passing structure would be implemented here if needed.

        query_job = client.query(sql_content)
        query_job.result()  # Wait for query execution to complete

        errcode = 0

    except GoogleCloudError as db_err:
        logger.error(f"Database execution failed: {db_err}")
        errcode = getattr(db_err, 'code', 1)
    except Exception as err:
        logger.error(f"Unexpected error executing SQL script: {err}")
        errcode = 1

    # Step 5: Return exit status code
    return errcode
```

### Execution Order

The legacy execution order of the dependency graph consists of 6 steps. The target orchestration in Cloud Composer must preserve this sequence:

1. **UC4 Orchestration (`DW.DWH_ABTN_SMART_KUBI.xml`)**: Map to the triggering Cloud Composer Airflow DAG.
2. **SQL Script (`d_abtn_x_smart_kubi.sql`)**: Map to a BigQuery execute query task within the Airflow DAG.
3. **Execution Wrapper (`r_sqlscript`)**: Map to the python execution handler that coordinates task orchestration.
4. **Environment Initialization (`.dw_init`)**: Map to runtime DAG environment variables or Airflow task configurations.
5. **Error messaging utility (`f_alis_msgerr.ksh`)**: Map to standard Airflow logging and alerting structures or a shared Python logging module.
6. **SQL*Plus helper (`h_alis_sqlplus.ksh`)**: Map to the target task calling `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`.

---

### Schedule & Variables

The job is scheduled with variables that must be dynamically evaluated at runtime by the Airflow DAG. The equivalent scheduler-set variables are:

* **`DWH_JOB_KENNUNG`**: String parameter with value `'ABTN_SMART_KUBI'`. Pass via Airflow DAG configuration parameters (`params`).
* **`cdate`**: Dynamic date parameter. Represent in Airflow as the execution date string format using JINJA templating: `{{ ds_nodash }}` (format `YYYYMMDD`).
* **`cmonth`**: Dynamic month value derived from `cdate` (first 6 characters). Map in Airflow to `{{ ds_nodash[0:6] }}`.
* **`cday`**: Dynamic day value derived from `cdate` (characters 7 and 8). Map in Airflow to `{{ ds_nodash[6:8] }}`.
* **`first`**: String parameter with value `'01'`. Pass via Airflow DAG configurations.
* **`cmonth` (recalculated)**: Combination of preceding `cmonth` and `first` strings. Map in Airflow to `{{ ds_nodash[0:6] }}01`.
* **`cmonth` (recalculated date offset)**: Subtract 1 day from the dynamic date `cmonth`. Map using Airflow’s execution date logic or Python datetime offsets.
* **`cmonth` (final format)**: Substring of the offset date (first 6 characters). Map using Python datetime formatting in Airflow task context.
* **`MONATSID`**: Set to the final value of `cmonth`. Pass to downstream tasks executing target BigQuery SQL scripts.

---

### Target File Plan

* **Target File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
  * **Language**: Python
  * **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`

---

### Environment-Specific Values

* **`DW_ORAUSER`**:
  * **Classification**: GLOBAL (identifies database credentials and connectivity)
  * **Target Platform Mapping**: Not applicable for direct user login in BigQuery. Must resolve through standard Google Cloud Authentication using the BigQuery Python Client (`google.cloud.bigquery.Client()`) utilizing Application Default Credentials (ADC) associated with the Airflow execution environment's Service Account. No hardcoded credentials or literal connection strings are to be utilized.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh | local/home/gurunathan_t/kubi/h_alis_sqlplus.py | Translated to a reusable Python utility module mirroring the source directory structure, providing equivalent parameter validations and query executions using the Google Cloud BigQuery API. |

---

### Risks & Manual Actions

* **External Dependency**: The helper shell script references `DWMSG_MeldeFehler` (error reporting utility) which is defined in another external component (`f_alis_msgerr.ksh`). Since `f_alis_msgerr.ksh` is not part of this specific design pass, the implementation of error logging in `h_alis_sqlplus.py` must use a placeholder interface mapping to Python's standard `logging` library, which must be subsequently integrated with the migrated logging library downstream.
* **Literal Message Preservation Compliance**: To ensure log continuity, the build agent must strictly preserve the original German literal output strings inside the target script's logging/stdout statements:
  * `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`
  * `"Sql*Plus-Skript : "`
  * `"Skript-Parameter: "`

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

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
REASON: This is an orchestration and utility script that performs command-line argument parsing, file searching, dynamic script execution, and complex metadata logging, which cannot be expressed in BigQuery Standard SQL.

EVIDENCE
- Business logic found: KSH custom logic parses command-line parameters, searches for a SQL script in multiple directories (`../sql`, `../mig`, `.`), manages process logging/metadata registration (via sourced functions), and sets up traps for process errors.
- AWK: none
- SQL-expressible: no, because filesystem operations, dynamic argument parsing, logging, and process control are not expressible in BigQuery Standard SQL.
- Non-SQL side effects: directory checking, dynamic file path searching, environment sourcing, shell-level trapping (`trap` on `INT` and `ERR`), and registering logging/status using external metadata utilities.
- Against this verdict: none, as standard SQL has no capabilities for filesystem operations or command-line utility orchestration.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`r_sqlscript`) is a legacy database-utility launcher written in KornShell. It acts as a standardized wrapper to execute SQL scripts in a controlled environment. The script parses command-line arguments to determine which SQL script to run, resolves its physical directory path across several deployment folders (`../sql`, `../mig`, or the current directory), establishes dynamic execution logging, and registers metadata status entries. It enforces structured error catching and status propagation to integrate with higher-level job scheduling.

2. INVOCATION CONTEXT
   - Who calls this script: Typically called by an automated scheduler (like UC4/Automic) via a UNIX job object (exact job name is unknown from the extraction).
   - Command line / arguments: `r_sqlscript -f <sql_script> [-i <sql_parameters>] [-j <job_identifier>] [-v] [-h]`
   - UC4 native includes: None referenced in the extraction.
   - Environment files sourced:
     - `. $HOME/aktuell/.dw_init` — # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values.
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` — # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — metadata functions it defines (such as `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`) are unknown.
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` — # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — database execution helper functions it defines (such as `starteSQLSkript`) are unknown.

3. PARAMETERS / INPUTS
   - `-f` (mapped to `p_sqlscript`): Mapped to a lowercase string (due to `typeset -l p_sqlscript`). Source is a command-line argument. Specifies the name of the target SQL script to execute.
   - `-i` (mapped to `p_sqlpar`): Input parameters/arguments to pass down to the target SQL script. Source is a command-line argument.
   - `-j` (mapped to `p_Job`): Job identifier (defaults to `"DWH_KORR"` if not specified). Mapped to `JobKennung` and converted to uppercase (due to `typeset -u JobKennung`).
   - `-v` (mapped to `p_Verbose`): Verbose flag (defaults to `0`, set to `1` if `-v` is provided). If active, outputs the execution log file directly to stderr/stdout upon exit.
   - `-h`: Mapped to help flag. Displays the usage information block and exits.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `dirname $0` and `dirname ${p_sqlscript}`: Standard utility commands used to resolve the executing script's directory and parse paths.
   - `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr`: An external launcher function defined in `h_alis_sqlplus.ksh`.
     - Exact command line: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
     - Purpose: Runs the resolved SQL script with the provided parameters and redirects output to the log file.
     - Implementation in Python: Since the `TARGET_PLATFORM` is confirmed as `BIGQUERY`, this execution step cannot remain an opaque SQL*Plus external launcher. The dynamic SQL script should instead be loaded, compiled/parsed, and executed via the BigQuery Client library (`google-cloud-bigquery` or `db-api` wrapper).
     - Resolvable Launcher check: No, because the script itself is a generic utility designed to run arbitrary dynamic SQL scripts; the SQL statements themselves are not present in the extraction.
     - # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; since TARGET_PLATFORM is BIGQUERY, this execution must be modernized to execute SQL statements on BigQuery (e.g., using google-cloud-bigquery) rather than invoking Oracle SQL*Plus.

5. EMBEDDED SQL
   - No embedded SQL is present in `r_sqlscript` itself. It acts purely as a shell runner for external SQL scripts passed dynamically via the `-f` flag.
   - Dialect Identification: The comments in `usage()` state that the SQL script must contain `"whenever sqlerror exit failure"`, which is a SQL*Plus-only directive. This confirms that the legacy system used Oracle SQL*Plus.
   - Target platform mapping: Since the target platform is confirmed as `BIGQUERY`, the SQL scripts themselves will need to be translated to BigQuery Standard SQL, and SQL*Plus-specific directives like `whenever sqlerror exit failure` will be obsolete, as BigQuery scripting uses `BEGIN ... EXCEPTION ... END` blocks or standard driver-level error handling.

6. CONTROL FLOW
   1. Sourcing of environment files and utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`).
   2. Parse command-line parameters using `getopts` (`-f`, `-i`, `-j`, `-v`, `-h`).
   3. Check if command-line parsing encountered errors (`ErrNr != 0`). If so, log error with `DWMSG_MeldeFehler` and exit.
   4. Change working directory to the directory where this script resides (`cd $(dirname $0)`).
   5. Resolve the path of the SQL script (`l_DBskript`):
      - If the input script file does not contain a directory path, check `../sql/<script>`, then `../mig/<script>`, and finally `<script>`.
      - If it contains a directory path, use it as is.
   6. Check if the script exists on disk.
      - # REVIEW: The legacy script contains `if [ -f "$l_DBskript" ] then ErrNr=198` which incorrectly flags an error if the script *does* exist, and uses the undefined variable `$p_Kuerzel`. This is likely a bug in the legacy ksh script; confirm if it should be corrected to check if the file does *not* exist (`if [ ! -f "$l_DBskript" ]`).
   7. Set up `JobKennung` (default to `DWH_KORR` if not provided, converted to uppercase).
   8. Register logging and metadata:
      - Call `DWMSG_ErmittleNr` to obtain a unique entry number `DW_EintragsNr`.
      - Call `DWMSG_Logdateiname` to get the log file path.
      - Call `DWMSG_ErzeugeEintrag` to register the job execution.
   9. Register `trap` actions for `INT` and `ERR` signals to run `DWMSG_Fehlerbehandlung` and dump the log file if verbose mode is enabled.
   10. Execute the SQL script via `starteSQLSkript` and redirect output to the log file.
   11. If execution completes successfully, call `DWMSG_SetzeStatusOK` to log success.
   12. Reset traps and exit.

7. ERROR HANDLING & EXIT CODES
   - How the script detects failure:
     - `getopts` parsing errors trigger custom error numbers (`192` or `193`).
     - Shell traps (`trap ... ERR` and `trap ... INT`) capture errors during execution of external utilities and SQL script execution.
     - The script relies on `set -e` to exit immediately if any command fails.
   - Reaction to failure:
     - Calls `DWMSG_MeldeFehler` for parameter errors.
     - Calls `DWMSG_Fehlerbehandlung` on system or script errors to log failure details to the metadata database.
     - Displays the log file using `cat` if verbose mode is enabled (`p_Verbose != 0`).
     - Propagates exit codes (e.g., from `getopts` or the SQL script execution failure).
   - Mapping to Python:
     - Positional argument validation and parsing will map to `argparse` with structured exceptions.
     - Environment/utility errors will raise standard Python exceptions (e.g., `FileNotFoundError`, `subprocess.CalledProcessError`).
     - The traps will map to standard Python `try...except...finally` blocks or `atexit` registrations.

8. OUTPUTS / SIDE EFFECTS
   - Modifies log files (path obtained dynamically via `DWMSG_Logdateiname`).
   - Performs side effects on the database via metadata logging functions (`DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`).
   - Executes the specified SQL script, which may modify DWH target tables.

9. BUSINESS SUMMARY
   - Provides a centralized, standardized wrapper for executing SQL scripts within the database environment.
   - Automates the path resolution of SQL scripts across standard deployment directories (`sql`, `mig`, `.`).
   - Integrates database-driven metadata and status logging (via `DWMSG_...` functions) to ensure job execution is tracked.
   - Implements robust error catching and log extraction to aid developers in debugging failures.
   - Standardizes process registration and tracking for monitoring tools.

### MANDATORY AUDIT CHECK
- No internal parameter-validation guards (such as `if [ -z "$X" ]` inside function blocks returning errors) found in the functions of this script.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Source environment and utility files
# # REVIEW-STRUCT: Sourced files [.dw_init, f_alis_msgerr.ksh, h_alis_sqlplus.ksh] are not available.
# We assume their environment variables and functions are available or imported.
# In a modern BigQuery Python environment, these would be mapped to a standard logging/metadata module.
import os
import sys
import argparse
import subprocess

# Step 2: Initialize parameters and default values
p_Verbose = 0
p_sqlscript = ""
p_sqlpar = ""
p_Job = ""
ErrNr = 0
ErrArg = ""

# Step 3: Parse command line arguments
# Mappings: -f -> p_sqlscript, -i -> p_sqlpar, -j -> p_Job, -v -> p_Verbose, -h -> help
# typeset -l p_sqlscript converts the script name to lowercase.
# typeset -u JobKennung converts the job identifier to uppercase.
try:
    parser = argparse.ArgumentParser(description="Ausführung Script r_sqlscript", add_help=False)
    parser.add_argument("-f", dest="p_sqlscript", type=str)
    parser.add_argument("-i", dest="p_sqlpar", type=str, default="")
    parser.add_argument("-j", dest="p_Job", type=str, default="")
    parser.add_argument("-v", dest="p_Verbose", action="store_true")
    parser.add_argument("-h", action="help", help="Zeigt diese Hilfe an")
    
    args = parser.parse_args()
    
    p_sqlscript = args.p_sqlscript.lower() if args.p_sqlscript else ""
    p_sqlpar = args.p_sqlpar
    p_Job = args.p_Job
    p_Verbose = 1 if args.p_Verbose else 0
except Exception as e:
    # Mimic getopts error handling
    ErrNr = 192  # Mapped from legacy error definitions
    ErrArg = str(e)

# Step 4: Validate parameter errors
if ErrNr != 0:
    # # REVIEW-STRUCT: DWMSG_MeldeFehler is defined in f_alis_msgerr.ksh.
    # DWMSG_MeldeFehler(0, "E", ErrNr, ErrArg)
    print(f"Error parsing arguments: {ErrArg}", file=sys.stderr)
    sys.exit(ErrNr)

if not p_sqlscript:
    print("Error: -f parameter is required.", file=sys.stderr)
    sys.exit(193)

# Step 5: Change directory to script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Step 6: Resolve SQL script path (l_DBskript)
l_DBskript = ""
if os.path.dirname(p_sqlscript) in (".", ""):
    # Search in ../sql, then ../mig, then current directory
    sql_path = os.path.join("..", "sql", p_sqlscript)
    mig_path = os.path.join("..", "mig", p_sqlscript)
    if os.path.isfile(sql_path):
        l_DBskript = sql_path
    elif os.path.isfile(mig_path):
        l_DBskript = mig_path
    else:
        l_DBskript = p_sqlscript
else:
    l_DBskript = p_sqlscript

# Step 7: Check if resolved SQL script exists
# # REVIEW: The legacy code 'if [ -f "$l_DBskript" ] then ErrNr=198' is highly likely a bug.
# It should check if the file does NOT exist. We implement the corrected check but note the legacy logic.
# Legacy logic check:
# if os.path.isfile(l_DBskript):
#     ErrNr = 198
#     ErrArg = os.environ.get("p_Kuerzel", "")  # p_Kuerzel is undefined in source
if not os.path.isfile(l_DBskript):
    print(f"Error: SQL script {l_DBskript} not found.", file=sys.stderr)
    sys.exit(198)

# Step 8: Set up job identifier and uppercase formatting
JobKennung = p_Job.upper() if p_Job else "DWH_KORR"

# Step 9: Register logging and metadata
# # REVIEW-STRUCT: The following DWMSG_ calls are defined in unsupplied utility files.
# In a Python implementation on BigQuery, this should map to a metadata tracking database/table.
DW_EintragsNr = "0"  # To be populated by metadata client
LogDatei = "log_file_path"  # To be populated by metadata client

# Mimic: DWMSG_ErmittleNr DW_EintragsNr
# Mimic: DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr
# Mimic: DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0_$l_DBskript $LogDatei
print(f"Jobkennung     : {JobKennung}")
print(f"DB-Skript      : {l_DBskript}")

# Step 10: Setup error traps / try-finally blocks
try:
    print(f"Job-Nr    : {DW_EintragsNr}")
    print(f"Logdatei  : {LogDatei}")
    
    # Step 11: Execute SQL script via runner
    # # REVIEW-STRUCT: starteSQLSkript is an external runner.
    # In BigQuery, this must be replaced with executing the SQL query via BigQuery Client.
    # For representation, we use a placeholder or subprocess run.
    # starteSQLSkript(DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr)
    print(f"Executing SQL script {l_DBskript} on BigQuery platform...")
    # e.g., bigquery_client.query(open(l_DBskript).read())
    
    # Step 12: Set OK status on success
    # DWMSG_SetzeStatusOK(DW_EintragsNr)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")

except Exception as e:
    # Trap handler: DWMSG_Fehlerbehandlung
    print(f"Error during script execution: {e}", file=sys.stderr)
    if p_Verbose != 0:
        # If verbose, display log file content
        try:
            with open(LogDatei, "r") as f:
                print(f.read())
        except Exception:
            pass
    sys.exit(1)
```

### Execution Order
The legacy orchestration order must be preserved in the target environment (e.g., in the Cloud Composer DAG). The execution flow sequence maps as follows:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Legacy UC4 Orchestration) ➔ Target: Cloud Composer DAG (`dags/dw_dwh_abtn_smart_kubi_dag.py`)
2. **d_abtn_x_smart_kubi.sql** (Legacy SQL DB transformation script) ➔ Target: BigQuery SQL/Dataform SQLX pipelines (to be migrated in its own design pass)
3. **r_sqlscript** (KornShell wrapper) ➔ Target: Python execution runner (`kubi/r_sqlscript.py`)
4. **.dw_init** (Environment initialization) ➔ Sourced dynamically or mapped to Composer environment configuration
5. **f_alis_msgerr.ksh** (Error messaging utility library) ➔ Target: Imported Python logging/metadata utilities
6. **h_alis_sqlplus.ksh** (SQL execution helper script) ➔ Target: Python BigQuery client connector logic

---

### Schedule & Variables — Must Be Retained
The schedule parameters and dynamically evaluated run variables from UC4 must be retained. They should be resolved at runtime using Airflow logical date macros/functions and passed to the target Python wrapper task:
* **DWH_JOB_KENNUNG** (Value: `'ABTN_SMART_KUBI'`): Passed as a static task-level config parameter.
* **cdate** (Value: `'SYS_DATE("YYYYMMDD")'`): Target Mapping: Derived using standard logical execution date parameters: `{{ dag_run.logical_date.strftime('%Y%m%d') }}`.
* **cmonth** (Value: `'SUBSTR(&cdate,1,6)'`): Target Mapping: Derived using: `{{ dag_run.logical_date.strftime('%Y%m') }}`.
* **cday** (Value: `'SUBSTR(&cdate,7,2)'`): Target Mapping: Derived using: `{{ dag_run.logical_date.strftime('%d') }}`.
* **first** (Value: `'01'`): Target Mapping: Retained as a static helper string.
* **MONATSID** (Calculated over multiple steps: YYYYMM ➔ YYYYMM01 ➔ subtract 1 day ➔ first 6 characters of previous month date): Target Mapping: Resolves to the previous month's YYYYMM ID. This should be calculated at DAG runtime in Composer using a python/jinja expression equivalent to:
  `{{ (dag_run.logical_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m') }}`

---

### Lineage
* **Upstream Producers**: 
  * Sourced configuration script: `FILE:.dw_init` (defines local paths and system defaults)
  * Sourced utility script: `FILE:f_alis_msgerr.ksh` (provides legacy messaging/status registration)
  * Sourced utility script: `FILE:h_alis_sqlplus.ksh` (provides legacy shell database script launching)
* **Downstream Consumers**:
  * Direct invocation target: `d_abtn_x_smart_kubi.sql` (passed via the `-f` flag during orchestration execution)

---

### Cross-File Dependencies
* **.dw_init**: Used to initialize standard paths and system environments before executing database wrappers.
* **f_alis_msgerr.ksh**: Provides shared operational support routines (`DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`).
* **h_alis_sqlplus.ksh**: Provides `starteSQLSkript` which serves as the execution layer launcher for individual SQL scripts via SQL*Plus.

---

### Target File Plan
* **Relative Target Path**: `kubi/r_sqlscript.py`
  * **Language**: Python
  * **Source File**: `local/home/gurunathan_t/kubi/r_sqlscript`

---

### Environment-Specific Values
* **DW_DIR_ROOT**: `GLOBAL` (The deployment root directory path. In BigQuery Python execution, this will be sourced from runtime environmental variables or mapped to static container mounts).
* **HOME**: `GLOBAL` (User home path. Sourced from the system's standard environment variables).
* **DW_EintragsNr**: `JOB-SPECIFIC` (Task sequence run ID. Dynamically resolved at execution time by the database metadata tracking system).
* **LogDatei**: `JOB-SPECIFIC` (Physical path to the operational output logs. Resolved dynamically at execution time and directed to Cloud Logging or a designated Google Cloud Storage logging bucket).
* **JobKennung / p_Job**: `JOB-SPECIFIC` (Unique operational identifier for tracking. Passed in via CLI arguments/task definitions).
* **p_sqlscript / l_DBskript**: `JOB-SPECIFIC` (File path of the execution target SQL script. Resolved via command line flags or dynamic configuration lookups).
* **p_sqlpar**: `JOB-SPECIFIC` (Optional dynamic parameters meant for standard SQL substitution. Provided at runtime).

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `kubi/r_sqlscript.py` | Migrated from KornShell to a modern Python runner. Preserves CLI parameter processing (`getopts`), search directory path routing, log output redirects, and standard interface orchestration while executing SQL assets on BigQuery via the BigQuery Client API. |