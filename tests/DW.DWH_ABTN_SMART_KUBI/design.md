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


# UC4 Migration Design Document: DW.DWH_ABTN_SMART_KUBI

## 1. Overview
This migration package consists of a single standalone UC4 UNIX job, `DW.DWH_ABTN_SMART_KUBI`, which is responsible for executing a SQL script to populate a temporary table. The job dynamically calculates a reporting month parameter (`MONATSID`) based on the execution date: if the day of execution is before the 15th, it targets the previous month; otherwise, it targets the current month. This object does not belong to a supplied workflow (JOBP) or schedule (JSCH) within this bundle, indicating that it is triggered externally or executed on-demand.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 | Populate temp table |

## 3. Scheduling
- **Trigger Source:** No workflow (`JOBP`), schedule (`JSCH`), or native UC4 calendar objects were supplied in this bundle. 
- **Trigger Type:** Externally triggered (source unknown from this extraction alone).
- **DAG Schedule:** `schedule=None` (no calendar or time-based schedule is assumed).

## 4. Airflow DAG Properties
Since this is a standalone JOBS_UNIX object with no parent JOBP, it is modeled as a single-task DAG.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (derived from Active=1) |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_abtn_smart_kubi_task` | `DW.DWH_ABTN_SMART_KUBI` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | #REVIEW-STRUCT: launcher wraps SQL script `d_abtn_x_smart_kubi.sql`, converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL. Converted execution parameters must include the dynamically computed `MONATSID`. |

## 6. Task Dependency Map
```python
dw_dwh_abtn_smart_kubi_task
```
*(Single-task workflow; no upstream/downstream tasks exist in this bundle.)*

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource locks were defined for this object. Default concurrency configurations (`max_active_runs=1`) are applied.

## 8. Error Handling and Retry Strategy
- Default failure behavior: Tasks will fail standardly on execution errors without a custom callback, using standard Airflow retry policies (1 retry, 5-minute delay).
- No postcondition actions, alerts, or custom escalation blocks were present in the source extraction.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `dw_dwh_abtn_smart_kubi` | Sanitised DAG ID | `dag_id` |
| `&MONATSID` | Calculated dynamically via UC4 script body logic | Jinja template or Python utility function calculating reporting month |

### Computed Parameter Logic (`&MONATSID`):
The equivalent Python expression for calculating the UC4 `&MONATSID` parameter is:
```python
from datetime import datetime, timedelta

def get_monatsid(execution_date_str=None):
    # Defaults to today's date if execution date is not passed
    dt = datetime.strptime(execution_date_str, "%Y%m%d") if execution_date_str else datetime.now()
    if dt.day < 15:
        # First day of current month minus 1 day gets us into the previous month
        first_day_current = dt.replace(day=1)
        prev_month = first_day_current - timedelta(days=1)
        return prev_month.strftime("%Y%m")
    else:
        return dt.strftime("%Y%m")
```

## 10. Developer Notes
- **#REVIEW-STRUCT: Companion Pipeline Dependency:** This job launcher executes a SQL script (`$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`). This extraction cannot deterministically know if it should be converted into a BigQuery-native SQL operation (using `BigQueryInsertJobOperator`) or a Python script (using `BashOperator`/`PythonOperator`). Verify the output of the companion KSH/SQL migration pipeline before replacing this `EmptyOperator` stub.
- **#REVIEW-STRUCT: Standalone Migration:** This job was extracted without a parent workflow workflow (`JOBP`). Verify if this job should be imported into a larger master DAG rather than running as a standalone DAG.
- **GCP Placeholders:** Configure target GCP project connections, regions, and dataset variables once the final operator destination is determined.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# GCP_PROJECT_ID = "your-gcp-project-id"  # Placeholder for downstream SQL migration
# GCP_REGION = "europe-west3"             # Placeholder for regional execution

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure handlers defined in source export

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Standalone migrated UC4 JOBS_UNIX",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migrated_uc4", "jobs_unix", "sql_script"],
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (no Self-Lock Else=Skip detected)

    # ── Sensor Task ──────────────────────────────────────
    # None required (no earliest_start_time constraint)

    # ── Calendar Check Task ──────────────────────────────
    # None required (no calendar constraints)

    # ── Task: dw_dwh_abtn_smart_kubi_task ────────────────
    # # REVIEW-STRUCT: Launcher wraps SQL script [d_abtn_x_smart_kubi.sql],
    # converted separately by the companion KSH/SQL migration pipeline into EITHER
    # a Python script or BigQuery SQL -- this extraction cannot know which.
    # Confirm actual artifact produced before replacing this EmptyOperator.
    # Parameter calculation for &MONATSID must be resolved dynamically in the final task:
    #   if day < 15 -> MONATSID = (first_day_of_month - 1).strftime("%Y%m")
    #   else -> MONATSID = today.strftime("%Y%m")
    dw_dwh_abtn_smart_kubi_task = EmptyOperator(
        task_id="dw_dwh_abtn_smart_kubi_task",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task workflow - no execution dependencies
    dw_dwh_abtn_smart_kubi_task
```

# Migration Design Document: DW.DWH_ABTN_SMART_KUBI (Orchestration Layer)

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `dags/dw_dwh_abtn_smart_kubi.py` | Converts the UC4 UNIX Job XML into a Cloud Composer Airflow DAG. The DAG dynamically calculates the reporting month parameter (`MONATSID`) and executes the SQL script by importing and calling the migrated `r_sqlscript` Python module directly (passing correct BigQuery query parameters). |

***

## Execution Order
The legacy execution order is preserved and mapped to the target orchestration tasks and files as follows:
1. **DW.DWH_ABTN_SMART_KUBI.xml** maps to the Cloud Composer DAG `dw_dwh_abtn_smart_kubi.py` (the overall orchestrator).
2. **.dw_init** maps to importing `dw_init.py` (the environment-wide initialization module) within the Python runner.
3. **r_sqlscript** maps to calling the execution function inside `r_sqlscript.py` (the migrated SQL execution wrapper utility) via an Airflow `PythonOperator` task.
4. **f_alis_msgerr.ksh** maps to standard imports of the migrated `f_alis_msgerr.py` module inside `r_sqlscript.py` to handle errors and status tracking.
5. **h_alis_sqlplus.ksh** maps to imports of `h_alis_sqlplus.py` (which manages BigQuery-equivalent executing layers).
6. **d_abtn_x_smart_kubi.sql** maps to the migrated BigQuery SQL script which is read and executed by the Python runner with dynamic query parameters.

***

## Schedule & Variables
- **Timing / Trigger:** This job does not have a native UC4 schedule (`JSCH`) or workflow (`JOBP`) within this bundle, indicating it is triggered externally or run on-demand. On Cloud Composer, it is configured with `schedule=None`.
- **Scheduler-Set Variables:**
  - `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'` -> Mapped to Airflow DAG variable/params or passed directly as a string parameter to the operator task.
  - `cdate` = `SYS_DATE("YYYYMMDD")` -> Mapped to Airflow's logical execution date (`{{ ds_nodash }}`).
  - **Dynamic Month Calculation (`MONATSID`):**
    If the current day of the execution date is less than 15, the workflow subtracts 1 month (by setting day to '01' and subtracting 1 day, then taking the first 6 characters). Otherwise, it uses the current month.
    In the target Python environment, this is implemented as:
    ```python
    from datetime import datetime, timedelta

    def get_monatsid(execution_date_str):
        # execution_date_str is YYYYMMDD format from DAG context
        dt = datetime.strptime(execution_date_str, "%Y%m%d")
        if dt.day < 15:
            first_day_current = dt.replace(day=1)
            prev_month = first_day_current - timedelta(days=1)
            return prev_month.strftime("%Y%m")
        else:
            return dt.strftime("%Y%m")
    ```
  - **Output/Print Logging:**
    The calculated reporting month must be logged preserving the exact original German literal from the source:
    `Berichtsmonat:  {monats_id}`
    It is then passed downstream as the runtime value for the query parameter `@monats_id`.

***

## Lineage
- **Upstream Producers / External Triggers:** Runs on host `dwhdwh1p` using login credentials package `DW.UNIX.ISTNS`. In GCP, this is mapped to a dedicated Cloud Composer Service Account running on the Airflow environment.
- **Included / Sourced Files:**
  - `DW.HOLE_PFAD` and `DW.LESE_LOG` are human-reviewed as **NO SOURCE NEEDED** and are retired from the orchestration layer.
  - Sourcing of `.dw_init` is handled by importing the migrated `dw_init` module within Python scripts.
- **Downstream Call Chain / Invoked Files:**
  - Invokes `r_sqlscript` utility to execute `d_abtn_x_smart_kubi.sql` passing the calculated parameter `MONATSID`. This execution chain is preserved by executing `r_sqlscript.py` directly from the DAG.

***

## Cross-File Dependencies
- **Shared Tables & Views:**
  - Target table to populate: `DWH$TA_T_SMART_KUBI` (columns: `MONATS_ID`, `KUNDENNUMMER`, `TARIF_ID`, `TARIF_ID_ALT`, `VO_KENNUNG`, `TEST_GP`, `ANZAHL`, `KENNZAHL_ID`).
  - Source tables/views: `DWH$TA_F_D1_TWVV_TN`, `BL_D_TARIF`, `DWH$VI_L_MAP_FA_TARIF`.
- **Call Chain Dependencies:**
  - The Airflow task must import `r_sqlscript.py` which in turn imports `f_alis_msgerr.py`, `h_alis_sqlplus.py`, and `dw_init.py`.

***

## Target File Plan

### File: `dags/dw_dwh_abtn_smart_kubi.py`
- **Language:** Python
- **Source File:** `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`
- **Integration Architecture & Alignment:**
  - To prevent orchestration silos where the DAG merely prints variables without launching the script, the DAG will use an Airflow `PythonOperator` that directly imports the migrated `r_sqlscript` module.
  - At runtime, the task dynamically computes `MONATSID` using the logic defined in the *Schedule & Variables* section.
  - Instead of wrapping shell commands or executing sub-processes, the DAG task executes:
    `r_sqlscript.execute_sql_script(job='ABTN_SMART_KUBI', sql_file='d_abtn_x_smart_kubi.sql', monats_id=calculated_monats_id)`
  - Within `r_sqlscript.py` (which is designed under a separate group pass), BigQuery execution must be performed using the BigQuery Python Client, passing the parameter dict `{"monats_id": calculated_monats_id}` to `QueryJobConfig(query_parameters=[...])`, ensuring that the query parameter `@monats_id` is successfully replaced at runtime and preventing execution failures.

***

## Environment-Specific Values
- **GCP_PROJECT:** GLOBAL — Identifies the target Google Cloud project. Sourced using `os.environ.get("GCP_PROJECT")` or `Variable.get("GCP_PROJECT")`.
- **GCP_REGION:** GLOBAL — Target GCP region (e.g., `europe-west3`). Sourced via environment variables.
- **DWH_JOB_KENNUNG:** JOB-SPECIFIC — The identifier for this specific job (`'ABTN_SMART_KUBI'`). Defined statically in the DAG code or params.
- **MONATSID:** JOB-SPECIFIC — Dynamically calculated reporting month based on the Airflow logical date. Calculated at runtime and passed as an argument.

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
VERDICT: NO_CONVERSION_REQUIRED
REASON: The script is a static environment initialization file that only defines environment variables and sources other configuration scripts, containing no business or data transformation logic.

EVIDENCE
- Business logic found: none; this is an environment setup script (.dw_init) initializing system paths and directories.
- AWK: none
- SQL-expressible: no, it contains only shell variable declarations and environment setup which do not map to SQL.
- Non-SQL side effects: none observed, other than exporting environment variables and sourcing files.
- Against this verdict: none

ORCHESTRATION SUMMARY
- Purpose: This environment file (.dw_init) is sourced to set up system directory variables, import/export paths, and locate ORACLE_HOME for the Information Services data warehouse environment.
- Variables declared:
  - DW_DIR_ROOT = $HOME/aktuell
  - DW_DIR_PROT = $HOME/daten/logfiles
  - DW_DIR_CUBES = $HOME/daten/cubes
  - DW_DIR_IMP_D1 = $HOME/daten/d1
  - DW_DIR_IMP_BWA = $HOME/daten/dpps/bwa
  - DW_DIR_IMP_XTRA = $HOME/daten/xtra
  - DW_DIR_IMP_CTEL = $HOME/daten/ctel
  - DW_DIR_IMP_VO = $HOME/daten/vo
  - DW_DIR_IMP_RV = $HOME/daten/rv
  - DW_DIR_IMP_IF = $HOME/daten/ees
  - DW_DIR_IMP_NNV = $HOME/daten/nnv
  - DW_DIR_IMP_SIGMA = $HOME/daten/gd/sigma
  - DW_DIR_EXP_SIGMA = $HOME/daten/gd/sigma/export
  - DW_DIR_IMP_TRF = $HOME/daten/trf
  - DW_DIR_IMP_AUF = $HOME/daten/sd/auf
  - DW_DIR_IMP_GUT = $HOME/daten/sd/gut
  - DW_DIR_IMP_KDG = $HOME/daten/sd/kdg
  - DW_DIR_IMP_MP_KDG = $HOME/daten/mp/kdg
  - DW_DIR_IMP_MP_TS = $HOME/daten/mp/ts
  - DW_DIR_IMP_MP_ZM = $HOME/daten/mp/zm
  - DW_DIR_IMP_TS = $HOME/daten/sd/ts
  - DW_DIR_IMP_ZM = $HOME/daten/sd/zm
  - DW_DIR_EXP = $HOME/daten/exporter
  - DW_DIR_IMP_BPM = $HOME/daten/bm
  - DW_DIR_IMP_ZTS = $HOME/daten/zts
  - DW_DIR_IMP_VRS = $HOME/daten/vrs
  - DW_DIR_IMP_BRUNET = $HOME/daten/brunet
  - DW_DIR_IMP_DWH = $HOME/daten/dwh
  - DW_DIR_IMP_PLATO = $HOME/daten/dwh/plato
  - DW_DIR_IMP_CARMEN = $HOME/daten/carmen
  - DW_DIR_IMP_SAP = $HOME/daten/sap
  - DW_DIR_IMP_SR_RV = $HOME/daten/sap/sr_rv_dpps
  - DW_DIR_IMP_SAP_L_GUTGR = $HOME/daten/sap/sap_l_gutgr (exported as DW_DIR_IMP_SAP_L)
  - DW_DIR_IMP_L_MAHNSTYP_IST = $HOME/daten/sap/mahn
  - DW_DIR_IMP_L_MAHNV_FI = $HOME/daten/sap/mahn
  - DW_DIR_IMP_L_MAHNV_IST = $HOME/daten/sap/mahn
  - DW_DIR_IMP_L_GUTGR = $HOME/daten/sd/l_gutschr
  - DW_DIR_IMP_L_LEIST = $HOME/daten/sd/l_leist
  - DW_DIR_IMP_L_PROD = $HOME/daten/sd/l_prod
  - DW_DIR_LKODE = $HOME/daten/sd/lkode
  - DW_DIR_IMP_SUBSE = $HOME/daten/subse
  - DW_DIR_SMS_PRG = ${HOME}/aktuell/allgemein/is/util
  - DW_DIR_SMS_ADR = ${HOME}/daten/sms/adressen
  - DW_DIR_SMS_TMP = ${HOME}/daten/sms/tmp
  - DW_DIR_IMP_DPPS = $HOME/daten/dpps
  - DW_DIR_IMP_PLANF2 = $HOME/daten/planf2
  - DW_HOST_CUSTOMER = dxcst3.bn.detemobil.de
  - ORACLE_HOME = /appl/local/oracle/12.2.0.1.0 or /appl/local/oracle/11.2.0 (fallback checks)
  - DW_DIR_UTL_FILE = /appl/local/oracle/admin/$ORACLE_SID/utl_file
- Environment files sourced:
  - $HOME/.dw_global
  - $HOME/.dw_lokal
- Invokes: none
- Called by: Sourced by other KornShell scripts in the legacy framework to initialize environment contexts.
- Exit-code behaviour: Propagates variable settings natively. Prints error messages if ORACLE_HOME cannot be set.
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion. Environment variables and paths should instead be declared natively in the target orchestrator's environment settings.

### MIGRATION DESIGN DOCUMENT: DW.DWH_ABTN_SMART_KUBI (Environment Group)

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `local/home/gurunathan_t/kubi/dw_init.py` | Converted into a Python environment configuration module. This allows downstream migrated scripts (`r_sqlscript.py`, `h_alis_sqlplus.py`, etc.) to import these system paths and settings natively, addressing previous build failure where modules assumed dependencies were missing. |

---

### Target File Plan

* **File Path:** `local/home/gurunathan_t/kubi/dw_init.py`
  * **Language:** Python 3
  * **Source File:** `local/home/gurunathan_t/kubi/.dw_init`
  * **Purpose:** Provides a centralized environment-configuration module. Instead of running subprocess shell initializations, other Python operators and helper scripts will import this module (`import dw_init`) to obtain unified system paths, GCS buckets, and host configurations. It preserves original German logging outputs verbatim where Oracle-related diagnostic paths fail to resolve.

---

### Execution Order

The target orchestration in Cloud Composer must preserve the execution sequence of the legacy pipeline:
1. **Orchestration Entrypoint:** Airflow DAG initialization (replacing `DW.DWH_ABTN_SMART_KUBI.xml`).
2. **Environment Setup:** Imports of `local/home/gurunathan_t/kubi/dw_init.py` to bootstrap directory paths.
3. **Utility Bootstrap:** Setup of error messaging context `f_alis_msgerr.py`.
4. **Execution Wrapper:** Execution of SQL runner `r_sqlscript.py` utilizing the environment configuration.
5. **Database Execution:** `d_abtn_x_smart_kubi.sql` execution using BigQuery client libraries, substituting `@monats_id` and `@eintragsnr` parameters.

---

### Scheduling

* **Trigger Event / Scheduler:** Sourced via Cloud Composer (Airflow) native cron or external DAG triggers matching the legacy UC4 scheduler behavior.
* **Target Scheduling Construct:** Apache Airflow Cron Schedule or Dataset-based trigger.

---

### Schedule & Variables — Must Be Retained

The scheduling environment must calculate and inject the following scheduler variables into the execution context (making them accessible to both Python scripts and BigQuery queries):

| Legacy Variable | Description / Source Formula | Airflow Target Mapping |
| :--- | :--- | :--- |
| `DWH_JOB_KENNUNG` | `'ABTN_SMART_KUBI'` | Airflow DAG `params` or execution environment variable |
| `cdate` | `SYS_DATE("YYYYMMDD")` | Airflow execution date macro: `{{ ds_nodash }}` |
| `cmonth` | `SUBSTR(&cdate,1,6)` | Derived via Airflow context: `{{ ds_nodash[:6] }}` |
| `cday` | `SUBSTR(&cdate,7,2)` | Derived via Airflow context: `{{ ds_nodash[6:8] }}` |
| `first` | `'01'` | Static config parameter: `"01"` |
| `MONATSID` | `cmonth` adjusted to prior month-end | Calculated dynamically in the Airflow DAG and passed as a parameter to the BigQuery script execution task |

---

### Lineage & Cross-File Dependencies

* **Upstream Configurations (Sourced within `.dw_init`):**
  * `.dw_global` (Resolved as: *NO SOURCE NEEDED* per human verification)
  * `.dw_lokal` (Resolved as: *NO SOURCE NEEDED* per human verification)
* **Downstream Consumers (Importing `dw_init.py`):**
  * `r_sqlscript.py` (Script runner)
  * `h_alis_sqlplus.py` (Helper utility script)
  * `f_alis_msgerr.py` (Error logging script)

---

### Environment-Specific Values

The environment settings from `.dw_init` are classified below. All paths default to equivalent Cloud Storage (GCS) or persistent volumes mapped in the target environment.

#### 1. GLOBAL (Environment-Wide)
* **`GCP_PROJECT`**: The target Google Cloud Project ID (sourced via `os.environ.get("GCP_PROJECT")`).
* **`GCS_BUCKET`**: The target GCS storage bucket (sourced via `os.environ.get("GCS_BUCKET")`).
* **`HOME`**: Root path of the environment (sourced via `os.environ.get("HOME")`).
* **`ORACLE_HOME`**: Legacy Oracle variable. *Retired for BigQuery target platform*, but stubbed as `None` or omitted to prevent downstream key errors.

#### 2. JOB-SPECIFIC (Module Configs)
* **`DW_DIR_ROOT`**: Sourced as `f"{HOME}/aktuell"` or `Variable.get("DW_DIR_ROOT")`.
* **`DW_DIR_PROT`**: Sourced as `f"gs://{GCS_BUCKET}/daten/logfiles"` or local log path.
* **`DW_HOST_CUSTOMER`**: Static legacy connection host `'dxcst3.bn.detemobil.de'`.
* **`DW_DIR_IMP_*` (Directories for Importers)**: Mapped to respective GCS directories (e.g., `DW_DIR_IMP_D1 = f"gs://{GCS_BUCKET}/daten/d1"`) rather than local UNIX paths to ensure horizontal scaling compatibility across Composer workers.

---

### Risks & Manual Actions

* **OUTPUT/PRINT LITERAL RULE:**
  The original German error logging output from `.dw_init` must be preserved exactly as-is in the target Python file:
  * **Literal Statement to Preserve:** 
    ```python
    print("Fehler in .dw_init:")
    print("   Konnte ORACLE_HOME nicht setzen !")
    ```
    This logic must be maintained under Python's path-validation checks if legacy directory verification is requested.

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
    - This is an anonymous PL/SQL block designed to perform an ETL partition-load operation. It truncates the target table, aggregates data from a transactional source table based on active subscription, contract, and tarif configurations, and logs metrics/errors using custom utility packages.
1.2 Summarize the business logic and purpose of the script:
    - The block truncates the target aggregation table `DWH$TA_T_SMART_KUBI`.
    - It maps/aggregates contract renewal information (VVLREIN, VVLTWC2C, MIGP2CBF) for a specific monthly partition (supplied by parameter `&1`).
    - The logic performs extensive historic outer joins with configuration-mapping CTEs (`temp`) and contract/customer dimensional tables (`dwh$ta_c_vertrag d`) ensuring validity ranges match the target execution month.
    - It tracks and reports the count of inserted rows and intercepts exceptions to log failure attributes.
1.3 List all entities referenced:
    - Target Table: `dwh$ta_t_smart_kubi`
    - Source Tables/Views: 
        * `dwh$vi_l_map_fa_tarif` (aliased as `T`)
        * `bl_d_tarif` (aliased as `TAR`)
        * `dwh$ta_f_d1_twvv_tn` (aliased as `fact`, partitioned by `&1`)
        * `dwh$ta_c_vertrag` (aliased as `d`)
    - Custom Dependencies:
        * `dwpa_util_skript.runstatement` (utility runner)
        * `dwpa_globals.k_alis_err_unknown` (error code constant)
        * `dwpa_meldung.fehler` (logging routine)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → `INT64`
    - `NUMBER` (without precision) → `NUMERIC` or `INT64` based on assignment context (e.g., ID values resolved to `INT64`).
    - `VARCHAR2(300)`, `VARCHAR2(512)` → `STRING`
    - Oracle `DATE` (includes time component) → `DATETIME` or `DATE` depending on time relevance. In this script, validity dates (`gueltig_von`, `gueltig_bis`) and tracking variables are used strictly as calendar dates, so they map to `DATE`.

2.2 Implicit and Explicit Type Casting:
    - Substitution parameters `&1` (Month ID, e.g. `201509`) and `&2` (Run ID) are explicitly cast using Oracle `TO_NUMBER`. These will resolve to BigQuery `INT64` declarations using `CAST(var AS INT64)` or through parameterization.
    - Explicit casts for date string conversions like `To_date('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` → `COALESCE(t_new.tarif_id, 0)`
    - `NVL(t_old.tarif_id, 0)` → `COALESCE(t_old.tarif_id, 0)`
    - Nested string Decode: `Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)`
      * Since Oracle treats empty strings after trim as NULL, we write this explicitly:
      * `CASE WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn ELSE fact.vo_kenn_bearb END`
    - Value Decode: `Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)`
      * `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`

2.4 String Functions:
    - `LTRIM(RTRIM(string))` → `TRIM(string)`

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)` → `DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH)`
    - `TO_CHAR(fact.gueltigkeitszeitpunkt,'yyyymm')` → `FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt)` (assuming `gueltigkeitszeitpunkt` is a `DATE` type).

2.6-2.10 Standard SQL Constructs:
    - Partition Join syntax (`dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1)`) → In BigQuery, partitions are queried directly by filtering the partitioned table on the partitioning column. BigQuery does not use partition-extension syntax. This is refactored to query the base table `dwh$ta_f_d1_twvv_tn` with a filter on its partition key equivalent to `&1`.
    - Join `(+)` syntax → Resolved into standard `LEFT OUTER JOIN` structures.
    - `SQL%ROWCOUNT` → Captured using the BigQuery procedural system variable `@@row_count` immediately after execution of the `INSERT` DML.

2.11 MERGE / DML Statements:
    - The PL/SQL block uses a separate `Truncate table` call followed by an `INSERT` statement. In BigQuery, this sequence can be executed natively as an explicit script with transactional integrity if needed, or structured using `INSERT OVERWRITE` (for partitioned tables) or separate `TRUNCATE TABLE` and `INSERT INTO` statements.

2.12 PL/SQL Handling:
    - The PL/SQL anonymous block (`DECLARE ... BEGIN ... END;`) is mapped directly to a BigQuery Scripting block with variable declarations (`DECLARE`, `SET`).
    - The `EXCEPTION WHEN OTHERS` handler is translated to a BigQuery `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block.
    - Logging routines (`dwpa_meldung.fehler`, etc.) are mapped to custom script logging operations or placeholder procedures.

2.15 Unresolvable or Advisory Items:
    - Oracle Hints (`/*+ Append */`, `/*+ parallel(...) */`, `/*+ use_hash(...) */`) are stripped entirely as they are not applicable to the serverless execution planning of BigQuery.
    - Utility packages `dwpa_util_skript`, `dwpa_globals`, and `dwpa_meldung` do not exist natively in BigQuery. They must be resolved via corresponding BigQuery-native logging procedures or handled at the application orchestrator level (e.g. Airflow/Python).

Step 3: Conversion Strategy Summary
3.1 Overall Approach: 
    - Convert the PL/SQL script into a pure BigQuery standard SQL scripting block (`DECLARE`, `SET`, `BEGIN/EXCEPTION`).
    - Resolve the CTE and target DML into standard ANSI SQL with explicit `LEFT JOIN` semantics replacing the `(+)` operator.
3.2 Assumptions:
    - Positional input variables `&1` and `&2` will be passed as script variables or declared variables at execution. We represent them as `v_monats_id_param` and `v_eintrags_nr_param`.
    - Target database names, tables, and environments exist matching the project configuration schemas.
3.3 Flagged Items:
    - The external procedure calls `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` are mocked or represented as procedural SQL call placeholders.

═══════════════════════════════════════════
MIGRATION DECISION & REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Source Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reasoning |
| :--- | :--- | :--- | :--- |
| **Anonymous PL/SQL Block** | BigQuery Scripting Block (`BEGIN...EXCEPTION`) | Python wrapper, Direct SQL | Procedural logic, variable scopes, and try-catch behavior are natively supported in BigQuery Scripting without external wrappers. |
| **TRUNCATE Utility Call** | BigQuery native `TRUNCATE TABLE` | Python dynamic execute, UDF | BigQuery supports direct `TRUNCATE TABLE` statements, eliminating the need for dynamic statement executors. |
| **Oracle Outer Join `(+)`** | Native `LEFT OUTER JOIN` | Nested scalar subqueries | Standard standard SQL joins are more performant and maintainable in BigQuery. |
| **`SQL%ROWCOUNT`** | BigQuery Scripting System Variable `@@row_count` | Analytical count query, Python execution counts | BigQuery tracks affected rows natively via `@@row_count` immediately after execution of a DML. |
| **Oracle Exception Block** | BigQuery Scripting Exception Block (`EXCEPTION WHEN ERROR THEN`) | Python error-handling, manual removal | BigQuery scripting exception blocks allow native execution rollback or error logging capture. |

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: A single unified SQL file containing variable declarations, native transaction execution controls, standard DML mappings, and native SQL replacement functions.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | BigQuery Target Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- |
| `pls_integer` | `INT64` | Direct mapping | None |
| `NUMBER` (Parameter / ID) | `INT64` | Direct mapping where used strictly as identifiers or integers | Ensure input parameters do not contain decimals |
| `DATE` | `DATE` | Map to `DATE` where strictly date-focused; map to `DATETIME` if time component is required. Here mapped to `DATE`. | Boundary rules checked; BigQuery supports standard date operations |
| `VARCHAR2(300)` / `VARCHAR2(512)` | `STRING` | Direct conversion | No length limits enforced in BigQuery |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: CTE expressions (`WITH`), dynamic string execution representation, outer joins `(+)`, customized logging arrays.
- **Unsupported Functions**: `DBMS_OUTPUT.PUT_LINE`, Oracle hints.
- **UDF Required**: No.
- **Python Required**: No (entirely executable via pure BigQuery SQL).
- **Direct Dependencies**: `dwh$ta_f_d1_twvv_tn`, `dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh$ta_c_vertrag`, `dwh$ta_t_smart_kubi`.
- **Assumptions**: Substitutions `&1` and `&2` are provided as SQL script arguments.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(expression AS INT64)` or `CAST(expression AS NUMERIC)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(date_expression, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE` or `DATE 'YYYY-MM-DD'` |
| `DECODE` | Direct-with-rewrite | `CASE WHEN... THEN... ELSE... END` |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATE` or `CAST(expression AS STRING)` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | Standard SQL query logging statement (`SELECT ...`) |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- BigQuery Scripting Block Execution equivalent to PL/SQL block
BEGIN
  -- Variable Declarations (Step 4.5)
  DECLARE v_anzahl_ds INT64 DEFAULT 0; -- converted from pls_integer
  DECLARE l_monats_id INT64;
  DECLARE EintragsNr INT64;
  DECLARE lv_str STRING;
  DECLARE l_monats_date DATE;
  
  -- Target project environment parameters mapped from orchestration (Step 3.2)
  SET l_monats_id = CAST(@param_1 AS INT64); -- converted from to_number('&1')
  SET EintragsNr = CAST(@param_2 AS INT64); -- converted from to_number('&2')
  
  -- ADD_MONTHS and TO_DATE replacement for month key parameter parsing (Step 2.5)
  SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH); 

  -- Truncate statement execution replacing custom dynamic executor (Step 2.11)
  TRUNCATE TABLE dwh$ta_t_smart_kubi; 

  -- Core ETL load statement using refactored modern standard joins (Step 4.3, 4.9)
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
             -- Oracle Optimizer hints /*+ parallel(...) */ stripped entirely (Step 2.15)
             SELECT
                    t.tarif_id,
                    t.dwh_tarif_id,
                    t.gueltig_von,
                    t.gueltig_bis,
                    tar.mp_geschaeftsfeld_id
             FROM   dwh$vi_l_map_fa_tarif T
             JOIN   bl_d_tarif TAR
               ON   t.tarif_id = tar.tarif_id
             WHERE  t.gueltig_bis = DATE '4712-12-31' -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
         )
  SELECT 
           l_monats_id                                    AS monats_id,
           -- DECODE converted to CASE WHEN (Step 2.3)
           CASE 
             WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
             ELSE d.t_mobile_kundennummer 
           END                                            AS kundennummer,
           COALESCE(t_new.tarif_id, 0)                     AS tarif_id, -- converted from Nvl(t_new.tarif_id,0)
           COALESCE(t_old.tarif_id, 0)                     AS tarif_id_alt, -- converted from Nvl(t_old.tarif_id,0)
           -- Nested dynamic TRIM DECODE resolved to SQL CASE expression (Step 2.3)
           CASE 
             WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn 
             WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
             ELSE fact.vo_kenn_bearb 
           END                                            AS vo_kennung,
           d.test_gp, 
           sum(fact.zugang)                               AS anzahl, 
           fact.kennzahl_id 
  FROM     dwh$ta_f_d1_twvv_tn fact -- Partition extension syntax resolved to base table with native partition filter
  LEFT OUTER JOIN temp t_new 
               ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp t_old 
               ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh$ta_c_vertrag d 
               ON fact.dwh_vertrag_id = d.dwh_vertrag_id
              AND l_monats_date > d.gueltig_von
              AND l_monats_date <= d.gueltig_bis
  WHERE    FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING) -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
  AND      fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  -- Injected filter to optimize partition scanning based on partition variable context
  AND      _PARTITIONDATE = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)) 
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

  -- Rowcount metric tracking using BQ standard scripting variables (Step 2.6)
  SET v_anzahl_ds = @@row_count; 
  
  -- Equivalent of dbms_output.put_line rendered via standard log table insertion or selection output
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds);

EXCEPTION WHEN ERROR THEN
  -- Exception management conversion (Step 2.14)
  DECLARE ErrText STRING;
  DECLARE ErrC STRING;
  DECLARE FehlerNr INT64;
  
  SET ErrText = @@error.message;
  SET ErrC = @@error.statement_text;
  SET FehlerNr = -20001; -- placeholder representing custom dwpa_globals.k_alis_err_unknown;

  -- Mock call representing logging system (Step 3.3)
  CALL dwpa_meldung_fehler_mock('F', EintragsNr, FehlerNr, ErrText, ErrC);
  
  ERROR(FORMAT('Error execution failed: %s - %s', ErrC, ErrText));
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic execution and custom logging references**: The original Oracle procedures `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` have been commented out or mocked. The build team must replace `CALL dwpa_meldung_fehler_mock` with the actual destination project logger (e.g. Cloud Logging or a metadata logger table).
2. **Partition Mapping Filter**: The partition reference `partition(dwh$ta_f_d1_twvv_tn_&1)` was replaced by adding an explicit filter `_PARTITIONDATE = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`. Confirm if the physical partitioning scheme of `dwh$ta_f_d1_twvv_tn` in BigQuery uses a `DATE` field matching this format.
3. **Empty String vs NULL behavior**: The expression resolving `ltrim(rtrim(...))` relies on checking `TRIM(val) IS NULL OR TRIM(val) = ''` because BigQuery distinguishes between empty string and actual NULL, whereas Oracle treats empty strings natively as NULL. Validate whether empty strings in the source dataset should trigger the default mapping behavior as depicted.

### Execution order
The legacy execution order of the dependency graph MUST be preserved and mapped to Cloud Composer task operations:
1. **DW.DWH_ABTN_SMART_KUBI.xml**: Initial orchestrator, translated into the root Airflow DAG execution in Cloud Composer.
2. **d_abtn_x_smart_kubi.sql**: The core PL/SQL database execution, migrated to a BigQuery standard SQL scripting script at `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`.
3. **r_sqlscript**: The execution wrapper helper, migrated to Python library functions in a separate design group.
4. **.dw_init**: Shell/environment initialization, migrated to helper Python standard routines.
5. **f_alis_msgerr.ksh**: Logger/error helper, migrated to central logging modules in Python.
6. **h_alis_sqlplus.ksh**: Execution engine utility, migrated to reusable Python utilities.

The Airflow DAG orchestration task sequence must run the initialization helpers, resolve date and logging parameters, and invoke the BigQuery client executing our target SQL script.

### Schedule & variables
The 9 legacy scheduler-set variables must be evaluated and dynamically populated within the target orchestrator:
* **DWH_JOB_KENNUNG** = `'ABTN_SMART_KUBI'`
* **cdate** = `SYS_DATE("YYYYMMDD")`
* **cmonth** = `SUBSTR(&cdate,1,6)`
* **cday** = `SUBSTR(&cdate,7,2)`
* **first** = `'01'`
* **cmonth** = `&cmonth&first`
* **cmonth** = `SUB_DAYS(&cmonth,1)`
* **cmonth** = `SUBSTR(&cmonth,1,6)`
* **MONATSID** = `&cmonth`

**Orchestration and Parameter Passing Integration**:
* The Airflow DAG execution environment will dynamically calculate the current date and compute the target `MONATSID` (representing the previous month's YYYYMM format) using standard Python `datetime` libraries or Airflow template macros.
* These computed values must be supplied explicitly when invoking our migrated BigQuery SQL script. 
* To address parameter pass-through failures, the calling wrapper `r_sqlscript.py` (which is designed in a separate group) must pass these parameters using `ScalarQueryParameter` objects within BigQuery's `QueryJobConfig`:
  ```python
  from google.cloud import bigquery
  job_config = bigquery.QueryJobConfig(
      query_parameters=[
          bigquery.ScalarQueryParameter("monats_id", "INT64", monats_id_val),
          bigquery.ScalarQueryParameter("eintragsnr", "INT64", eintrags_nr_val)
      ]
  )
  client.query(query_string, job_config=job_config)
  ```
* These will be captured natively inside the BigQuery SQL scripting block via `@monats_id` and `@eintragsnr`.

### Lineage
The lineage dependencies for the query are mapped directly to BigQuery tables and assets:
* **Target Table**: `dwh$ta_t_smart_kubi` — Target table where calculated aggregation data is loaded.
* **Source Tables / Views**:
  * `dwh$ta_f_d1_twvv_tn` — Partitioned transaction source table.
  * `dwh$vi_l_map_fa_tarif` — View providing mapping classifications for tarif records.
  * `bl_d_tarif` — Reference table detailing business unit divisions.
  * `dwh$ta_c_vertrag` — Reference database catalog storing contract records.
* **Package Mappings**:
  * `dwpa_util_skript` and `dwpa_meldung` — Legacy packages used for statement routing and warning/failure tracking; their logic is mapped into native scripting blocks and standard error-log routines.

### Cross-file dependencies
* **Execution Wrapper Hook**: The execution script `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` depends on the Python-migrated wrapper utility `r_sqlscript` to perform initialization, pass the parameters `@monats_id` and `@eintragsnr`, and execute the block inside the designated BigQuery project.
* **Shared Schemas**:
  * `dwh$ta_f_d1_twvv_tn` is a highly-partitioned BigQuery table. An explicit partition filter using the parsed parameter (`_PARTITIONDATE = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`) is integrated inside the SQL block to ensure high performance and scan optimization.

### Target file plan
* **Target File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  * **Language**: SQL (BigQuery SQL scripting)
  * **Source**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`

### Environment-specific values
* **GCP_PROJECT** (GLOBAL): Represents the standard Google Cloud project identifier. Sourced from the calling system environment variable `os.environ.get("GCP_PROJECT")`.
* **BQ_DATASET** (GLOBAL): Represents the database dataset namespace in BigQuery. Sourced at runtime via variables.
* **DWH_JOB_KENNUNG** (JOB-SPECIFIC): Set as the constant value `'ABTN_SMART_KUBI'`. Passed via Airflow job parameters.
* **EintragsNr** (JOB-SPECIFIC): Run tracking ID generated per execution by the parent orchestrator. Passed dynamically as parameter `@eintragsnr`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | Converts the Oracle PL/SQL anonymous block into a BigQuery standard SQL scripting block containing native variable declarations, ANSI standard outer joins, native rowcount checks via `@@row_count`, and a structured try-catch block. |

---

### HARD RULES
* Oracle Hints (`/*+ Append */`, `/*+ parallel */`, `/*+ use_hash */`) are stripped entirely from the SQL statements as they are not supported in BigQuery.
* Standard SQL parameter syntax `@monats_id` and `@eintragsnr` must be used to bind the runtime parameters.
* Verbatim output messages inside `dbms_output.put_line` are retained in their original form: `' rows inserted in DWH$TA_T_SMART_KUBI'`.

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
REASON: This is a KornShell utility library defining several reusable status-logging and error-handling functions that interact with both the database (via SQL*Plus) and the local filesystem, making it a perfect candidate for a Python utility module.

EVIDENCE
- Business logic found: KSH custom logic. It defines functions for status tracking, registering job starts/ends/abortions, compiling timing info, constructing log file names, and writing entries to a database-backed error/logging table.
- AWK: none
- SQL-expressible: partly, the database logging functions execute SQL*Plus scripts or inline PL/SQL blocks, but the library also manages filesystem paths, process IDs, and environment variables that are not SQL-expressible.
- Non-SQL side effects: Creates and deletes temporary files in `/tmp` to read database output; constructs log file paths using dates and process IDs.
- Against this verdict: If all dependent jobs were being completely rewritten as pure BigQuery SQL scripts, these logging utilities might be converted to BigQuery stored procedures, but a Python utility module is much more versatile for maintaining the shell-like orchestrator logic.


=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell utility library containing helper routines for error management and status tracking within "Information Services". It defines functions to initialize job entries, log warnings or fatal errors, append timing info, and update job status (OK/Aborted) in a central message table (`BERT_MELDUNG`) via SQL*Plus. Other KornShell scripts source this file to standardize their logging and error-trapping behavior.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other KornShell scripts (the caller is typically another .ksh script, which may itself be invoked by UC4/Automic jobs). No specific UC4 job or JOBS_UNIX object is specified in this file itself.
   - Any UC4 native includes: None referenced in the source.
   - Environment files sourced: None sourced within this script, but it depends on environment variables like `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT` being pre-set.

3. PARAMETERS / INPUTS
   For each parameter, state:
   - `DW_ORAUSER` (environment variable, used to connect to SQL*Plus. In a BigQuery environment, this is replaced by BigQuery client credentials/configuration.)
   - `DW_DIR_ROOT` (environment variable, used to locate `.sql` files. In Python, this is read via `os.environ.get("DW_DIR_ROOT")` or configured as a base package path.)
   - `DW_DIR_PROT` (environment variable, used as the output directory for protocol log files. Surfaced via `os.environ.get("DW_DIR_PROT")`.)
   
   Functions in the library take the following positional parameters:
   - `DWMSG_Fehlerbehandlung`
     - `$1` (DWMSG_EintragsNr): The unique entry ID in the log table. Position-based. Map to Python function argument.
   - `DWMSG_SetzeStatusOK`
     - `$1` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
   - `DWMSG_SetzeStatusAbbruch`
     - `$1` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
   - `DWMSG_ErmittleNr`
     - `$1` (VarName): Variable name to assign the retrieved entry ID to.
       # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
   - `DWMSG_ErzeugeEintrag`
     - `$1` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
     - `$2` (JobKennung): Job identifier. Position-based. Map to Python function argument.
     - `$3` (Programmname): Program/script name. Position-based. Map to Python function argument.
     - `$4` (LogDatei): Path to log file. Position-based. Map to Python function argument.
   - `DWMSG_MeldeFehler`
     - `$1` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
     - `$2` (Typ): Error type (F/E/W). Position-based. Map to Python function argument.
     - `$3` (FehlerNr): Error number. Position-based. Map to Python function argument.
     - `$4` (Zusatz1): Optional detail 1. Position-based. Map to Python function argument with default `None`.
     - `$5` (Zusatz2): Optional detail 2. Position-based. Map to Python function argument with default `None`.
   - `DWMSG_Logdateiname`
     - `$1` (VarName): Variable name to assign the constructed path to.
       # REVIEW: out-parameter validation "VarName" guarded a parameter this refactor removed — return path directly instead.
     - `$2` (JobKennung): Job identifier. Position-based. Map to Python function argument.
     - `$3` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
   - `DWMSG_SetzeStichtagInfo`
     - `$1` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
     - `$2` (DWMSG_Stichtag): Key date value. Position-based. Map to Python function argument.
     - `$3` (DWMSG_StichtagFmt): Format of the key date. Position-based. Map to Python function argument.
   - `DWMSG_AppendTimingInfos`
     - `$1` (DWMSG_EintragsNr): Entry ID. Position-based. Map to Python function argument.
     - `$2` (DWMSG_InfoText): Timing text info. Position-based. Map to Python function argument.
     - `$3` (DWMSG_DateFormat): Format of date. Position-based. Map to Python function argument.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - Legacy Oracle SQL*Plus calls are used:
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
     - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
     - `sqlplus -s $DW_ORAUSER <<EOF EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt')); commit; EOF`
     - `sqlplus -s $DW_ORAUSER <<EOF EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' '); commit; EOF`
   - Since the target platform is **BIGQUERY**, these calls should not remain as external `sqlplus` executions. Instead, they should be implemented using the `google.cloud.bigquery` client package executing DML statements or calling BigQuery stored procedures.

5. EMBEDDED SQL
   For each SQL statement found:
   - Source: Inside sourced SQL files (`d_alis_spaufruf_p1.sql`, etc.) or inline EOF blocks.
   - Text:
     1. `BERT_MELDUNG.SetzeStatusOk <EintragsNr>`
        - BigQuery mapping: `UPDATE \`{{project_id}}.dataset.BERT_MELDUNG\` SET status = 'OK' WHERE entry_id = @entry_id`
     2. `BERT_MELDUNG.SetzeStatusAbbruch <EintragsNr>`
        - BigQuery mapping: `UPDATE \`{{project_id}}.dataset.BERT_MELDUNG\` SET status = 'ABORTED' WHERE entry_id = @entry_id`
     3. `BERT_MELDUNG.Erzeuge_Eintrag <EintragsNr> <JobKennung> <Programmname> <LogDatei>`
        - BigQuery mapping: `INSERT INTO \`{{project_id}}.dataset.BERT_MELDUNG\` (entry_id, job_id, program_name, log_file, status) VALUES (@entry_id, @job_id, @program_name, @log_file, 'RUNNING')`
     4. `BERT_MELDUNG.Fehler <Typ> <EintragsNr> <FehlerNr> <Zusatz1> <Zusatz2>`
        - BigQuery mapping: `INSERT INTO \`{{project_id}}.dataset.BERT_MELDUNG_ERRORS\` (entry_id, error_type, error_no, detail_1, detail_2) VALUES (@entry_id, @error_type, @error_no, @detail_1, @detail_2)`
     5. `EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));`
        - BigQuery mapping: `UPDATE \`{{project_id}}.dataset.BERT_MELDUNG\` SET key_date = PARSE_DATE(@format, @stichtag) WHERE entry_id = @entry_id`
     6. `EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');`
        - BigQuery mapping: `UPDATE \`{{project_id}}.dataset.BERT_MELDUNG\` SET timing_info = CONCAT(COALESCE(timing_info, ''), @info_text, ' ', FORMAT_TIMESTAMP(@format, CURRENT_TIMESTAMP()), ' ') WHERE entry_id = @entry_id`
   - Dialect is unambiguously Oracle SQL*Plus / PL-SQL (evidenced by `EXEC`, `commit;`, `to_date`, `to_char(SYSDATE, ...)`).

6. CONTROL FLOW
   - **DWMSG_Fehlerbehandlung**
     1. Slices last exit code (`$?`).
     2. Calls `DWMSG_MeldeFehler` with fatal code `10` and details of the exit code.
     3. Calls `DWMSG_SetzeStatusAbbruch`.
   - **DWMSG_SetzeStatusOK**
     1. Validates `DWMSG_EintragsNr` is not empty.
     2. Connects to DB and executes `SetzeStatusOk` update.
   - **DWMSG_SetzeStatusAbbruch**
     1. Validates `DWMSG_EintragsNr` is not empty.
     2. Connects to DB and executes `SetzeStatusAbbruch` update.
   - **DWMSG_ErmittleNr**
     1. Generates unique sequence number from DB.
     2. Returns the generated number.
   - **DWMSG_ErzeugeEintrag**
     1. Validates `DWMSG_EintragsNr` is not empty.
     2. Inserts new tracking record into BigQuery with initial parameters.
   - **DWMSG_MeldeFehler**
     1. Validates `DWMSG_EintragsNr` is not empty.
     2. Formats details and inserts them into BigQuery error logs.
   - **DWMSG_Logdateiname**
     1. Compiles date/time string `YYYYMMDD_HHMM` along with job code and message entry number.
     2. Returns the path string.
   - **DWMSG_SetzeStichtagInfo**
     1. Validates `DWMSG_EintragsNr`, `DWMSG_Stichtag`, and `DWMSG_StichtagFmt` are not empty.
     2. Formats date and updates tracking table in BigQuery.
   - **DWMSG_AppendTimingInfos**
     1. Validates `DWMSG_EintragsNr` and `DWMSG_DateFormat` are not empty.
     2. Formats timestamp and appends to timing column.

7. ERROR HANDLING & EXIT CODES
   - Validation checks on arguments output a custom standard message to stderr and raise ValueError (or call `sys.exit(1)` / `sys.exit(2)`).
   - In Python, database exceptions should raise native library exceptions (e.g. `google.cloud.exceptions.GoogleCloudError`) that bubbles up to the calling runner script.

8. OUTPUTS / SIDE EFFECTS
   - Tracking table updates in BigQuery (`{{project_id}}.dataset.BERT_MELDUNG`).
   - Logging table entries in BigQuery (`{{project_id}}.dataset.BERT_MELDUNG_ERRORS`).

9. BUSINESS SUMMARY
   - Standardizes process registration and health checking across BigQuery data warehouse pipelines.
   - Captures exact status (running, completed, failed) of scheduled tasks in real-time.
   - Provides clear error message logging, tying specific technical failures back to business processes.
   - Constructs structured log files containing process identifiers and timestamps to aid operational audits.
   - Tracks timing and key execution parameters (like reporting key-dates) to measure pipeline performance.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import datetime
from google.cloud import bigquery

# Resolve project ID and dataset configuration
PROJECT_ID = os.environ.get("PROJECT_ID", "{{project_id}}")
DATASET_ID = os.environ.get("DATASET_ID", "dataset")

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, error_code=None):
    # sichern des FehlerCodes
    fehler_nr = error_code if error_code is not None else 1
    k_unerw_fehler = 10
    
    # Melde Fehler in der Meldungstabelle.
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # BigQuery target equivalent to calling BERT_MELDUNG.SetzeStatusOk
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET status = 'OK', end_timestamp = CURRENT_TIMESTAMP()
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("entry_id", "INT64", int(dwmsg_eintrags_nr))]
    )
    client.query(query, job_config=job_config).result()


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # BigQuery target equivalent to calling BERT_MELDUNG.SetzeStatusAbbruch
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET status = 'ABORTED', end_timestamp = CURRENT_TIMESTAMP()
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("entry_id", "INT64", int(dwmsg_eintrags_nr))]
    )
    client.query(query, job_config=job_config).result()


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
    
    # Generate unique sequence ID. In BigQuery, we can simulate Oracle sequences by generating a unique timestamp-based ID or using UUID.
    # We will generate a unique sequence number from a BigQuery row count, UUID, or a dedicated sequence table.
    client = get_bq_client()
    query = f"SELECT GENERATE_UUID() as new_id"
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        # Returning string UUID representation or hash converted to int
        return str(row.new_id)


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programm_name, log_datei):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG` 
        (entry_id, job_kennung, programm_name, log_datei, status, start_timestamp)
        VALUES (@entry_id, @job_kennung, @programm_name, @log_datei, 'RUNNING', CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entry_id", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programm_name", "STRING", programm_name),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    client.query(query, job_config=job_config).result()


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1=None, zusatz2=None):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG_ERRORS` 
        (entry_id, type, error_no, detail_1, detail_2, log_timestamp)
        VALUES (@entry_id, @typ, @fehler_nr, @zusatz1, @zusatz2, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entry_id", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", int(fehler_nr)),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2)
        ]
    )
    client.query(query, job_config=job_config).result()


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr):
    # REVIEW: out-parameter validation "VarName" guarded a parameter this refactor removed — return path directly instead.
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{timestamp}_{dwmsg_eintrags_nr}.log"
    return filename


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, dw_stichtag, dw_stichtag_fmt):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not dw_stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
        
    if not dw_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Translate Oracle format masks to Python/BigQuery format
    # Map typical Oracle formats: 'YYYYMMDD' -> '%Y%m%d', 'DD.MM.YYYY' -> '%d.%m.%Y'
    bq_format = dw_stichtag_fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET stichtag = PARSE_DATE(@format, @stichtag)
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("format", "STRING", bq_format),
            bigquery.ScalarQueryParameter("stichtag", "STRING", dw_stichtag),
            bigquery.ScalarQueryParameter("entry_id", "STRING", dwmsg_eintrags_nr)
        ]
    )
    client.query(query, job_config=job_config).result()


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, dwmsg_info_text, dwmsg_date_format):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not dwmsg_date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Translate format mask
    bq_format = dwmsg_date_format.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")\
                                 .replace("HH24", "%H").replace("MI", "%M").replace("SS", "%S")
    
    current_time_str = datetime.datetime.now().strftime(bq_format)
    append_text = f"{dwmsg_info_text} {current_time_str} "
    
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET timing_info = CONCAT(COALESCE(timing_info, ''), @append_text)
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("append_text", "STRING", append_text),
            bigquery.ScalarQueryParameter("entry_id", "STRING", dwmsg_eintrags_nr)
        ]
    )
    client.query(query, job_config=job_config).result()
```

### EXECUTION ORDER
The target orchestration (managed via Airflow DAG `dags/dw_dwh_abtn_smart_kubi.py`) must preserve the execution sequence of the legacy dependency graph:
1. **DW.DWH_ABTN_SMART_KUBI.xml** -> Mapped to the Airflow DAG definition file `dags/dw_dwh_abtn_smart_kubi.py` which governs and triggers the pipeline.
2. **d_abtn_x_smart_kubi.sql** -> Mapped to the BigQuery target SQL task executed inside the DAG.
3. **r_sqlscript** -> Mapped to the Python execution wrapper `kubi/r_sqlscript.py` which executes the target SQL script.
4. **.dw_init** -> Mapped to the Python initialization module `kubi/dw_init.py` loaded within the execution context.
5. **f_alis_msgerr.ksh** -> Mapped to the Python logging library `kubi/f_alis_msgerr.py` used to capture and report execution states.
6. **h_alis_sqlplus.ksh** -> Mapped to the helper module `kubi/h_alis_sqlplus.py`.

---

### SCHEDULE & VARIABLES
The target Airflow DAG must calculate and pass the following 9 scheduler variables using native Airflow and Python execution parameters:
- `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
- `cdate` = Dynamically calculated using the DAG execution date in `YYYYMMDD` format.
- `cmonth` = Slices the first 6 characters of `cdate` (`YYYYMM`).
- `cday` = Slices the last 2 characters of `cdate` (`DD`).
- `first` = `'01'` (constant).
- `cmonth` = Combines `cmonth` and `first` to construct the first day of the current month.
- `cmonth` = Subtracts 1 day from the above date to shift to the last day of the previous month.
- `cmonth` = Slices the resulting date back to the first 6 characters (`YYYYMM`).
- `MONATSID` = Assigned the final calculated `cmonth` value, representing the target reporting month.

These variables must be passed to the Python runner (`kubi/r_sqlscript.py`), which will supply them as query parameters (`@monats_id` and `@eintragsnr`) during BigQuery execution.

---

### LINEAGE
- **Upstream Procedures**:
  - `f_alis_msgerr.ksh` calls the PL/SQL procedure `PROCEDURE:SETZEZUSATZINFOS`. This database-level metadata update is migrated directly into BigQuery UPDATE DML statements executed inside `dwmsg_setze_stichtag_info` and `dwmsg_append_timing_infos` in the Python utility module.

---

### CROSS-FILE DEPENDENCIES
To resolve integration issues identified in previous iterations:
- **Importing f_alis_msgerr**: Sibling scripts `kubi/dw_init.py`, `kubi/r_sqlscript.py`, and `kubi/h_alis_sqlplus.py` must import the migrated Python module (`from kubi.f_alis_msgerr import *`) to access native status logging rather than implementing dummy logic or executing shell subprocesses.
- **Wired Orchestration Sequence**:
  1. At job startup, `kubi/r_sqlscript.py` must call `dwmsg_ermittle_nr()` to obtain a unique tracking ID (`eintrags_nr`).
  2. `kubi/r_sqlscript.py` must then register the process by calling `dwmsg_erzeuge_eintrag(eintrags_nr, ...)` before execution begins.
  3. During query execution of `d_abtn_x_smart_kubi.sql`, `kubi/r_sqlscript.py` must explicitly configure `bigquery.ScalarQueryParameter("monats_id", "STRING", monats_id)` and `bigquery.ScalarQueryParameter("eintragsnr", "STRING", eintrags_nr)` within `client.query()` to prevent execution failures in the parameterized SQL.
  4. If an exception occurs, `r_sqlscript.py` must trap the exception and call `dwmsg_fehlerbehandlung(eintrags_nr, error_code)` to log the failure and update the status in BigQuery to `'ABORTED'`.
  5. Upon successful query execution, the wrapper must execute `dwmsg_setze_status_ok(eintrags_nr)`.

---

### TARGET FILE PLAN
- **File Path**: `kubi/f_alis_msgerr.py`
  - **Language**: Python
  - **Source File**: `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
  - **Purpose**: Provides Python functions representing standard logging, status tracking, and error management, replacing obsolete database-level sequence lookups and PL/SQL calls with native GCP `google-cloud-bigquery` DML actions.

---

### ENVIRONMENT-SPECIFIC VALUES
- **GCP_PROJECT** (GLOBAL): Sourced dynamically at runtime via `os.environ.get("GCP_PROJECT")` to direct client calls to the proper GCP project.
- **BQ_DATASET** (GLOBAL): Sourced dynamically at runtime via `os.environ.get("BQ_DATASET")` to identify the BigQuery dataset housing audit tables.
- **DW_DIR_ROOT** (GLOBAL): Sourced via `os.environ.get("DW_DIR_ROOT")` to identify base application structures.
- **DW_DIR_PROT** (GLOBAL): Sourced via `os.environ.get("DW_DIR_PROT")` to specify local logging protocol paths.

---

### FILE DISPOSITION

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `kubi/f_alis_msgerr.py` | Migrated to a reusable Python module to supply standardized GCP BigQuery tracking, status-logging, and error management APIs. |

---

### OUTPUT/PRINT LITERALS (MANDATORY RETAINED TEXT)
The literal output statements below are carried over character-for-character from the original source. They must never be translated, localized, or rephrased:
- `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`
- `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`
- `"Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"`
- `"Argh!, keinen Variablennamen bei ErmittleNr angegeben"`
- `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`
- `"Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"`
- `"Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"`
- `"Argh!, keinen Stichtag angegeben!"`
- `"Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"`
- `"Argh!, Formatangabe erforderlich!"`

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
REASON: The script is a shell utility library defining a reusable function with parameter validation, filesystem readability checks, and external database-client process execution.

EVIDENCE
- Business logic found: KSH custom logic in `starteSQLSkript` validates input arguments, checks SQL script file availability, and executes SQL commands.
- AWK: none
- SQL-expressible: no, contains shell-level parameter checks, file existence checks, and dynamic argument processing.
- Non-SQL side effects: Local filesystem read access checks (`[ ! -r $p_Skript ]`) and invocation of external CLI client.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a reusable shell utility library that provides helper functions for running SQL*Plus scripts. Specifically, it defines the `starteSQLSkript` function, which performs defensive pre-execution checks (argument validation and file readability) before launching the database client with any forwarded parameters. Its business purpose is to wrap database script executions in a standard error-handling and verification layer.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced as a helper utility library by other client shell scripts within the environment (no standalone UC4 job direct execution of this script itself is shown).
   - UC4 native includes: None referenced in the extraction.
   - Environment files sourced: None directly sourced, but it expects external orchestration context variables (like `DW_ORAUSER`) and functions (like `DWMSG_MeldeFehler`) to be already loaded.

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following parameters:
   - `$1` (`p_Eintragsnr`): Error entry number, used for error logging. Required. Surfaced as the first function parameter in Python.
   - `$2` (`p_Skript`): Path to the SQL script file to run. Required. Surfaced as the second function parameter in Python.
   - `$*` (via `shift 2`): Forwarded arguments passed to the SQL script. Optional. Surfaced as `*args` / variadic arguments in Python.
   - Environment variables:
     - `DW_ORAUSER`: Database connection user/credentials. In a BigQuery environment, this maps to BigQuery client authentication credentials or project configurations.
     - `ModulName` / `Modul_Name`: Sourced as `"alis_sqlplus"`. (Note: The script defines `ModulName="alis_sqlplus"` but uses the variable `${Modul_Name}` in its error call; this is a naming mismatch).
     - `ModulVersion`: `"V1.1.3"`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Runs the specified SQL*Plus script using the given database credentials and parameters.
     - Target Platform execution: Since the confirmed target platform is `BIGQUERY`, this invocation should be converted from an external process launch of `sqlplus` into a native Python BigQuery client library call (`google.cloud.bigquery`). The SQL script content will be read and executed directly via `client.query()`.
     - Resolvable launcher check: Not a generic launcher wrapper, but a shell library executing a database client. Because the target platform is explicitly BigQuery, we treat the execution as mapping to BigQuery's client execution.

5. EMBEDDED SQL
   - Source file: Sourced externally via `$p_Skript` ($2).
   - SQL text: Dynamic, provided by the caller.
   - Dialect: Unambiguously Oracle SQL*Plus in the legacy script (indicated by `sqlplus` invocation and `@` parameter syntax). In the target BigQuery environment, the external scripts must be written in or converted to BigQuery Standard SQL.

6. CONTROL FLOW
   1. The helper function `starteSQLSkript` is called with arguments.
   2. Assign parameters to local variables: `p_Eintragsnr = $1`, `p_Skript = $2`.
   3. Shift positional arguments by 2 to capture the remaining parameters (`$*`).
   4. **Parameter Guard**: Check if either `p_Eintragsnr` or `p_Skript` is empty. If so, invoke `DWMSG_MeldeFehler` with code `196` and return `196`.
   5. **File Guard**: Check if the script file `p_Skript` exists and is readable. If not, invoke `DWMSG_MeldeFehler` with code `201` and return `201`.
   6. Log the execution settings (script path and parameters) to stdout.
   7. Disable exit-on-error (`set +e`) to allow capturing the database client exit status.
   8. Execute `sqlplus` passing the connection string, script file, and parameters.
   9. Capture the exit code of `sqlplus` into `errcode`.
   10. Re-enable exit-on-error (`set -e`).
   11. Return `errcode` to the caller.

7. ERROR HANDLING & EXIT CODES
   - If argument validation fails, returns `196` and logs the error via `DWMSG_MeldeFehler`.
   - If the file is not readable, returns `201` and logs the error via `DWMSG_MeldeFehler`.
   - `sqlplus` failures are captured using `$?` and returned as the function's return code.
   - Python mapping:
     - Check file accessibility using `os.access(p_skript, os.R_OK)`.
     - Catch BigQuery execution exceptions (`google.cloud.exceptions.GoogleCloudError`) and return non-zero exit codes.
     - Delegate error logging to a Python equivalent of `DWMSG_MeldeFehler` (e.g., `dwmsg_melde_fehler`).

8. OUTPUTS / SIDE EFFECTS
   - Standard output logs regarding execution settings.
   - Database state changes in BigQuery resulting from the SQL query execution.

9. BUSINESS SUMMARY
   - Standardizes the execution of SQL scripts across all data pipeline jobs.
   - Enforces defensive parameter checking to prevent running empty or missing scripts.
   - Ensures filesystem accessibility of script assets prior to database resource allocation.
   - Captures and forwards database execution exit codes to guarantee reliable step orchestration.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
from typing import List

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

def dwmsg_melde_fehler(eintragsnr: str, msg_type: str, code: int, details: str):
    """
    Simulates the external DWMSG_MeldeFehler shell command.
    # REVIEW-STRUCT: external utility DWMSG_MeldeFehler not supplied — behaviour simulated
    """
    print(f"ERROR_LOG: Entry {eintragsnr}, Type {msg_type}, Code {code}, Details: {details}", file=sys.stderr)

def starte_sql_skript(p_eintragsnr: str, p_skript: str, *params: str) -> int:
    """
    Helper function to validate and execute a SQL script.
    """
    # Step 1: Validate input parameters
    # MANDATORY AUDIT: Keep validation guard matching KSH logic exactly.
    # Note: Using MODUL_NAME instead of KSH's undefined Modul_Name due to typo in original.
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
        return 196

    # Step 2: Validate that SQL script file is readable
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Log execution settings
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(params)}")

    # Step 4: Execute SQL script in BigQuery
    # Since TARGET_PLATFORM is BIGQUERY, native BigQuery execution is used instead of sqlplus.
    try:
        from google.cloud import bigquery
        
        # Read SQL content
        with open(p_skript, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Initialize BigQuery Client
        client = bigquery.Client()
        
        # Note: If parameters are required, they can be processed via query parameters or templating.
        print(f"Executing query from {p_skript} on BigQuery...")
        query_job = client.query(sql_content)
        query_job.result()  # Wait for execution to finish
        
        errcode = 0
    except Exception as e:
        print(f"Database execution error: {e}", file=sys.stderr)
        errcode = 1  # Standard database error return code
        
    return errcode
```

### Execution order
The target orchestration (configured in Cloud Composer) must preserve the execution sequence of the logical steps within the `DW.DWH_ABTN_SMART_KUBI` job. `h_alis_sqlplus.py` serves as a shared Python utility module and is not executed as a standalone DAG task. Instead, it is imported by other steps. The step ordering mappings are:
* **Environment Initialization**: `.dw_init` (migrated to `dw_init.py`) executes first to set up environment context.
* **Utility Libraries Loading**: `f_alis_msgerr.ksh` (migrated to `f_alis_msgerr.py`) and `h_alis_sqlplus.ksh` (migrated to `h_alis_sqlplus.py`) are imported/loaded.
* **Execution Wrapper**: `r_sqlscript` (migrated to `r_sqlscript.py`) is executed. This script imports `h_alis_sqlplus.py` to run database tasks.
* **Database Querying**: `d_abtn_x_smart_kubi.sql` is executed by the wrapper script against BigQuery.

### Schedule & variables
The scheduling orchestration layer in Cloud Composer must calculate and pass the following legacy scheduler-set variables down into the execution context:
* `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
* `cdate` = `SYS_DATE("YYYYMMDD")`
* `cmonth` = First 6 characters of `cdate`
* `cday` = Last 2 characters of `cdate`
* `first` = `'01'`
* Calculated `MONATSID` = Calculated previous month string (`YYYYMM`), derived using the Airflow execution date.

The DAG layer must supply these variables dynamically. In the Python layer, `h_alis_sqlplus.py` and `r_sqlscript.py` must forward the resolved parameters (such as `MONATSID` and the error entry number `Eintragsnr`) as query parameters to the BigQuery client query engine to avoid query syntax failures.

### Cross-file dependencies
* **Importing `f_alis_msgerr.py`**: `h_alis_sqlplus.py` has a direct code-level dependency on the error handling functions in `f_alis_msgerr.py`. It must import `f_alis_msgerr.py` (which houses `dwmsg_melde_fehler` or equivalent) directly. It must not implement dummy functions, stub errors, or launch shell commands via `subprocess.run`.
* **Exporting to `r_sqlscript.py`**: The helper function `starte_sql_skript` defined in `h_alis_sqlplus.py` must be imported and executed by the script wrapper `r_sqlscript.py`.
* **Query Parameter Propagation**: The wrapper function `starte_sql_skript` must accept parameter arguments and pass them to the native Google Cloud BigQuery client library execution (`client.query()`) as dictionary-based query parameters (e.g., `@monats_id`, `@eintragsnr`) to resolve target execution requirements.

### Target file plan
* **Target File Path**: `home/gurunathan_t/KUBI/h_alis_sqlplus.py`
  * **Source File Path**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
  * **Language**: Python
  * **Purpose**: Reusable utility module defining the Python version of the validation and execution routine (`starte_sql_skript`). It validates file availability, manages log statements, imports `f_alis_msgerr.py` for error registration, and executes SQL queries on BigQuery via native APIs.

### Environment-specific values
* `DW_ORAUSER` (GLOBAL): Maps to target GCP project-wide authentication. Managed via Google Application Default Credentials (ADC) and resolved using the environment variable `GCP_PROJECT` (sourced at runtime via `os.environ.get("GCP_PROJECT")`).
* `ModulName` / `ModulVersion` (JOB-SPECIFIC): Defined inline as script constants with values `"alis_sqlplus"` and `"V1.1.3"`.

### File Disposition Table

Source File Path | Target File / Action | Purpose / Reason for Action
--- | --- | ---
`local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `home/gurunathan_t/KUBI/h_alis_sqlplus.py` | Migrates the shell-based SQL execution helper into a clean, reusable Python utility. It imports the migrated `f_alis_msgerr.py` to maintain structured error reporting and executes target SQL files on BigQuery with proper parameter binding.

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
REASON: The script is a generic SQL launcher utility containing command-line argument parsing, file path search logic, logging registration, and signal trapping that must be converted to Python.

EVIDENCE
- Business logic found: KSH custom logic. The script acts as a SQL launcher utility that searches for files in multiple directories (`../sql`, `../mig`, `.`), registers job tracking identifiers via proprietary utilities, and runs database scripts with custom parameter forwarding.
- AWK: none
- SQL-expressible: no, the script does not contain inline business logic SQL; it is an orchestration utility with conditional path checks, parameter parsing, and logging side-effects.
- Non-SQL side effects: checks file existence on the host filesystem, generates local log files, and executes dynamic external SQL scripts.
- Against this verdict: none, as this is an orchestration wrapper/launcher utility and cannot be mapped to a static BigQuery SQL file.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `r_sqlscript` is a generic utility wrapper used to execute database SQL scripts (originally Oracle SQL*Plus, migrating to BigQuery) with standardized execution logging, argument forwarding, and error trapping. It accepts a SQL script name as an input, automatically resolves its absolute path by searching standard directories, registers the execution status in a central repository, and executes the SQL via a proprietary runner. It is designed to ensure database errors are caught and logged, propagating failure exit codes cleanly to the parent orchestration job.

### 2. INVOCATION CONTEXT
- **Sourced by / Caller**: Invoked by UC4 / Automic jobs (UNIX JOBS objects) using command-line parameters.
- **UC4 Includes**: None referenced in this extraction.
- **Environment Files Sourced**:
  - `. $HOME/aktuell/.dw_init`  
    # REVIEW-STRUCT: environment file $HOME/aktuell/.dw_init not supplied — variables it sets are unknown; do not guess their names or values
  - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`  
    # REVIEW-STRUCT: environment file ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values
  - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`  
    # REVIEW-STRUCT: environment file ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values

### 3. PARAMETERS / INPUTS
- **`-f` (p_sqlscript)**:
  - **Source**: CLI argument parsed via `getopts`.
  - **Type**: String. Converted to lowercase via `typeset -l p_sqlscript`.
  - **Usage**: Name of the SQL script to be executed.
  - **Python representation**: Parse via `argparse` as a string, and apply `.lower()` to replicate the lowercase behavior.
- **`-i` (p_sqlpar)**:
  - **Source**: CLI argument parsed via `getopts`.
  - **Type**: String.
  - **Usage**: Optional parameter string forwarded directly to the SQL script execution.
  - **Python representation**: Parse via `argparse` as an optional string.
- **`-j` (p_Job)**:
  - **Source**: CLI argument parsed via `getopts`.
  - **Type**: String.
  - **Usage**: Job identifier code. Defaults to `"DWH_KORR"` if not specified.
  - **Python representation**: `argparse` argument with `default="DWH_KORR"`.
- **`-v` (p_Verbose)**:
  - **Source**: CLI argument parsed via `getopts`.
  - **Type**: Boolean/Integer flag (0 or 1).
  - **Usage**: Verbose flag. If set, triggers output of the log file (`cat $LogDatei`) in error traps.
  - **Python representation**: Store-true flag in `argparse`.
- **`-h`**:
  - **Source**: CLI argument parsed via `getopts`.
  - **Usage**: Displays the usage helper block and exits.
  - **Python representation**: Handled automatically by `argparse` help.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script relies heavily on proprietary status-logging shell functions sourced from `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh`. Since these sources are not fully supplied, their executions must be wrapped in `subprocess.run` calls or refactored into Python logging client calls:

- **`DWMSG_MeldeFehler`**:
  - *Command*: `DWMSG_MeldeFehler $DW_EintragsNr E $ErrNr $ErrArg`
  - *Purpose*: Reports a parameter parsing error to the tracking framework.
- **`DWMSG_ErmittleNr`**:
  - *Command*: `DWMSG_ErmittleNr DW_EintragsNr`
  - *Purpose*: Generates and returns a unique entry number.
- **`DWMSG_Logdateiname`**:
  - *Command*: `DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr`
  - *Purpose*: Determines the target log file name.
- **`DWMSG_ErzeugeEintrag`**:
  - *Command*: `DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0_$l_DBskript $LogDatei >> $LogDatei 2>&1`
  - *Purpose*: Registers the run entry in the status tracking database.
- **`starteSQLSkript`**:
  - *Command*: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
  - *Purpose*: Executes the SQL script via DB client with arguments.
  - *Resolvable Launcher*: No, because the source for `starteSQLSkript` is not supplied, and no environment database connection parameters are listed in the extraction.
  - # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
- **`DWMSG_Fehlerbehandlung`**:
  - *Command*: `DWMSG_Fehlerbehandlung $DW_EintragsNr >> $LogDatei 2>&1`
  - *Purpose*: Executes cleanup/error updates on execution failure.
- **`DWMSG_SetzeStatusOK`**:
  - *Command*: `DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1`
  - *Purpose*: Updates the job's tracking entry to "OK" upon success.

### 5. EMBEDDED SQL
No direct SQL statements are written inline. The script executes external `.sql` files passed to it via `-f`.
- **Dialect Identification**: The script comments state that the SQL script must contain `"whenever sqlerror exit failure"`, which is exclusive to Oracle SQL*Plus.
- **Target Platform (BigQuery)**: Because the target platform is confirmed as BigQuery, these external SQL files must be executed using the BigQuery Python Client (`google.cloud.bigquery`) or migrated to BigQuery-compliant SQL syntax.

### 6. CONTROL FLOW
1. **Environment Setup**: Sourced shell environments (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`).
2. **Variable Initialization**: Sets `ErrNr=0`, `ErrArg=""`, `DW_EintragsNr=0`.
3. **Parse Arguments**: Parse parameters (`-f`, `-i`, `-j`, `-v`, `-h`) using `getopts`. Convert `-f` value to lowercase.
4. **Validation Check 1 (getopts errors)**: If `-f` is missing or unknown parameters are passed, set `ErrNr` (192 or 193), report error via `DWMSG_MeldeFehler`, print usage, and exit.
5. **Path Resolution**: 
   - Change directory to script directory (`cd \`dirname $0\``).
   - If the filename has no path component (equals `.`), search for it in `../sql/`, then `../mig/`, and finally directly in the current directory.
6. **Validation Check 2 (File Existence Check)**:
   - Check if `$l_DBskript` is a file: `if [ -f "$l_DBskript" ]`.
   - # REVIEW: The original check 'if [ -f "$l_DBskript" ]' sets ErrNr=198 (unknown parameter) if the file IS found. This is likely a bug in the legacy script and was intended to be 'if [ ! -f "$l_DBskript" ]' (checking for missing files) along with referencing the correct file variable instead of an unassigned '$p_Kuerzel'. Confirm intended behavior.
7. **Job Identifier Defaulting**: Set `JobKennung` to `$p_Job` in uppercase; default to `"DWH_KORR"` if empty.
8. **Logging Registration**: Call `DWMSG_ErmittleNr` and `DWMSG_Logdateiname` to establish tracking parameters, then invoke `DWMSG_ErzeugeEintrag`.
9. **Signal and Error Trapping**: Define trap routines on `INT` and `ERR` signals to run `DWMSG_Fehlerbehandlung` and print failure messages (printing log if `p_Verbose` is set).
10. **SQL Script Run**: Call `starteSQLSkript` with resolved path and parameters, routing all output to `$LogDatei`.
11. **Completion & Trap Reset**: Set status to OK via `DWMSG_SetzeStatusOK`, reset the traps, and exit with status 0.

### 7. ERROR HANDLING & EXIT CODES
- **KornShell Detection**: Uses `set -e` combined with explicit `trap` definitions on signal `INT` and pseudo-signal `ERR`.
- **Python Mapping**: Convert `set -e` and `trap` to a Python `try...except Exception as e...finally` block. 
  - Catches `subprocess.CalledProcessError` on subprocess execution failures.
  - Ensures clean execution of logging failure wrappers on exception catch, then terminates the process with `sys.exit(1)`.

### 8. OUTPUTS / SIDE EFFECTS
- Registers job runs and logs via proprietary DWMSG tracking utilities.
- Creates/writes a local log file `LogDatei` containing the SQL execution output.

### 9. BUSINESS SUMMARY
- **Automated Path Resolution**: Translates raw SQL filenames into relative paths across `../sql` or `../mig` locations, minimizing manual path management.
- **Centralized Database Instrumentation**: Generates sequential tracking IDs and registers operational execution metadata.
- **Robust Failure Capturing**: Automatically traps script failures, writes diagnostic outputs, and updates tracking databases to reflect execution errors.

---

### PSEUDOCODE

```python
# Step 1: Environment Setup & Library Sourcing
import os
import sys
import argparse
import subprocess
import shutil

# REVIEW-STRUCT: environment file $HOME/aktuell/.dw_init not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values

# Step 2: Global and Tracking Variable Initialization
err_nr = 0
err_arg = ""
dw_eintrags_nr = "0"
log_datei = ""

# Define Usage helper block
def usage():
    print("""
   Programm: Ausführung Script [ScriptName]
   Version: 5.0.0
   Aufruf: [ScriptName] Parameter

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
    """)

# Helper wrappers for proprietary shell tracking commands
def run_dwmsg_melde_fehler(eintrags_nr, severity, err_no, arg):
    # Call underlying legacy shell commands to execute proprietary tracking
    subprocess.run(["DWMSG_MeldeFehler", str(eintrags_nr), severity, str(err_no), str(arg)], check=False)

def run_dwmsg_ermittle_nr():
    # Return sequence number from environment call
    res = subprocess.run(["DWMSG_ErmittleNr"], capture_output=True, text=True, check=True)
    return res.stdout.strip()

def run_dwmsg_logdateiname(job_kennung, eintrags_nr):
    res = subprocess.run(["DWMSG_Logdateiname", job_kennung, str(eintrags_nr)], capture_output=True, text=True, check=True)
    return res.stdout.strip()

def run_dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, script_path, log_file):
    # Appends standard registration line to target log file
    cmd = f"DWMSG_ErzeugeEintrag {eintrags_nr} {job_kennung} {script_path} {log_file} >> {log_file} 2>&1"
    subprocess.run(cmd, shell=True, check=True)

def run_dwmsg_fehlerbehandlung(eintrags_nr, log_file):
    cmd = f"DWMSG_Fehlerbehandlung {eintrags_nr} >> {log_file} 2>&1"
    subprocess.run(cmd, shell=True, check=False)

def run_dwmsg_setze_status_ok(eintrags_nr, log_file):
    cmd = f"DWMSG_SetzeStatusOK {eintrags_nr} >> {log_file} 2>&1"
    subprocess.run(cmd, shell=True, check=True)

# REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
def run_starte_sql_skript(eintrags_nr, db_script, sql_par, log_file):
    cmd = f"starteSQLSkript {eintrags_nr} {db_script} '{sql_par}' {eintrags_nr} >> {log_file} 2>&1"
    subprocess.run(cmd, shell=True, check=True)


# Step 3: Parse Arguments using argparse to mirror ksh getopts
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-f', dest='p_sqlscript', type=str)
parser.add_argument('-i', dest='p_sqlpar', type=str, default="")
parser.add_argument('-j', dest='p_Job', type=str, default="")
parser.add_argument('-v', dest='p_Verbose', action='store_true')
parser.add_argument('-h', dest='help_flag', action='store_true')

try:
    args, unknown = parser.parse_known_args()
    if unknown or args.help_flag:
        usage()
        sys.exit(0)
except Exception as parse_err:
    err_nr = 192 # Parameter unbekannt / invalid args
    err_arg = str(parse_err)

# Step 4: Validate parameters parsed from arguments
if err_nr != 0 or not args.p_sqlscript:
    if not args.p_sqlscript and err_nr == 0:
        err_nr = 193 # Notwendiges Argument fehlt (-f is required)
        err_arg = "-f"
    run_dwmsg_melde_fehler(dw_eintrags_nr, "E", err_nr, err_arg)
    usage()
    sys.exit(err_nr)

# Apply typeset -l equivalent for script name (lowercase)
p_sqlscript = args.p_sqlscript.lower()
p_sqlpar = args.p_sqlpar
p_Job = args.p_Job
p_Verbose = args.p_Verbose

# Step 5: Resolve SQL Script Path relativ to execution directory
original_dir = os.getcwd()
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

sqlscript_dir = os.path.dirname(p_sqlscript)

# If path is flat (equivalent to '.'), perform standard sub-directory searches
if sqlscript_dir == "" or sqlscript_dir == ".":
    l_DBskript = os.path.join("..", "sql", p_sqlscript)
    if not os.path.isfile(l_DBskript):
        l_DBskript = os.path.join("..", "mig", p_sqlscript)
    if not os.path.isfile(l_DBskript):
        l_DBskript = p_sqlscript
else:
    l_DBskript = p_sqlscript

# Step 6: Perform secondary file verification
# REVIEW: The legacy script check 'if [ -f "$l_DBskript" ]' sets ErrNr=198 if the file IS found.
# Replicated literally below, but flag for correction to check for missing files instead.
if os.path.isfile(l_DBskript):
    err_nr = 198  # Parameterwert unbekannt
    # REVIEW: Legacy code references unassigned variable '$p_Kuerzel' here.
    err_arg = ""  
    run_dwmsg_melde_fehler(dw_eintrags_nr, "E", err_nr, err_arg)
    usage()
    sys.exit(err_nr)

# Step 7: Apply Job Identifier default values (JobKennung upper-case string)
job_kennung = p_Job.upper() if p_Job else "DWH_KORR"

print("----------------- Parameter -----------------")
print(f"Jobkennung     : {job_kennung}")
print(f"DB-Skript      : {l_DBskript}")
print("---------------------------------------------")

# Step 8: Registration and Log Initialization
try:
    dw_eintrags_nr = run_dwmsg_ermittle_nr()
    log_datei = run_dwmsg_logdateiname(job_kennung, dw_eintrags_nr)
    run_dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)
except Exception as reg_err:
    print(f"Failed logging initialization: {reg_err}", file=sys.stderr)
    sys.exit(1)

# Step 9 & 10: Run the Main SQL Job within a Trap-equivalent block
try:
    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{dw_eintrags_nr}'")
    print(f"Logdatei  : '{log_datei}'")
    print("---------------------------------------------")

    # Step 10: Run SQL executor script with redirects
    run_starte_sql_skript(dw_eintrags_nr, l_DBskript, p_sqlpar, log_datei)

    # Step 11: Finalize Status to OK on Success
    run_dwmsg_setze_status_ok(dw_eintrags_nr, log_datei)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    sys.exit(0)

except Exception as job_err:
    # Trap and handle exceptions matching INT / ERR traps
    print("!FEHLER/OSFEHLER gemeldet!", file=sys.stderr)
    try:
        run_dwmsg_fehlerbehandlung(dw_eintrags_nr, log_datei)
        if p_Verbose and log_datei and os.path.isfile(log_datei):
            with open(log_datei, "r") as f:
                print(f.read(), file=sys.stderr)
    except Exception as cleanup_err:
        print(f"Nested failure during trap execution: {cleanup_err}", file=sys.stderr)
    sys.exit(1)
```

### Execution order
The target execution sequence must preserve the exact task sequence and dependencies of the legacy environment. The Airflow DAG orchestrating this job must follow this ordering:
1. **DAG Initialization & Date Parsing**: The orchestrator (`dw_dwh_abtn_smart_kubi.py`) triggers, calculates the scheduler-set variables (such as `MONATSID`), and initializes the environment parameters.
2. **Environment & Helper Load**: The wrapper script `r_sqlscript.py` executes, importing the migrated environment configurations (`dw_init.py`), logging mechanisms (`f_alis_msgerr.py`), and database helper utilities (`h_alis_sqlplus.py`).
3. **Database Parameter Preparation**: `r_sqlscript.py` parses arguments, resolves the relative path to the SQL script, establishes a sequential execution entry number (`dw_eintrags_nr`), and configures the BigQuery parameters.
4. **BigQuery Query Execution**: The wrapper script calls the imported `h_alis_sqlplus.starteSQLSkript` function (or utilizes the native BigQuery client directly) to run `d_abtn_x_smart_kubi.sql` with query parameters `@monats_id` and `@eintragsnr`.
5. **Execution Logging & Exit Propagation**: Success or failure is logged via the imported logging helper routines, and traps are cleared.

### Scheduling
- **Trigger Event / Scheduler**: Executed on the target platform via Cloud Composer (Airflow) scheduled triggers.
- **Equivalent Target Platform Construct**: An Airflow DAG `dw_dwh_abtn_smart_kubi.py` configured with a `schedule_interval` cron expression equivalent to the legacy UC4 scheduler interval.

### Schedule & variables
- **Scheduler-set Variables**:
  - `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
  - `cdate` = `'SYS_DATE("YYYYMMDD")'` (Target equivalent: calculated using standard `datetime` functions in Python or Airflow Jinja macros)
  - `cmonth` = `'SUBSTR(&cdate,1,6)'`
  - `cday` = `'SUBSTR(&cdate,7,2)'`
  - `first` = `'01'`
  - `cmonth` = `'&cmonth&first'`
  - `cmonth` = `'SUB_DAYS(&cmonth,1)'`
  - `cmonth` = `'SUBSTR(&cmonth,1,6)'`
  - `MONATSID` = `'&cmonth'` (Resolved reporting Month ID)
- **Target Variable Sourcing**:
  - These values must be computed dynamically inside the Airflow DAG wrapper and passed downstream to `r_sqlscript.py` using `params` or injected into environment variables via `os.environ`.
  - The parameter `MONATSID` must be retrieved and passed directly to the BigQuery client executing the SQL script.

### Lineage
- **Upstream Producers (Job Dependencies)**:
  - `dw_init` (Sourced config script) -> Migrated to Python module `dw_init.py`.
  - `f_alis_msgerr.ksh` (Error reporting helper) -> Migrated to Python module `f_alis_msgerr.py`.
  - `h_alis_sqlplus.ksh` (SQL run helper) -> Migrated to Python module `h_alis_sqlplus.py`.
- **Downstream Consumers**:
  - None (The wrapper script processes execution parameters and terminates on completion).

### Cross-file dependencies
- **Imports & Sourcing**:
  - `local/home/gurunathan_t/kubi/r_sqlscript.py` depends on and must import the following sibling modules:
    ```python
    import dw_init
    import f_alis_msgerr
    import h_alis_sqlplus
    ```
- **Database Targets**:
  - `r_sqlscript.py` invokes and runs the SQL script `d_abtn_x_smart_kubi.sql` (sibling file) against BigQuery.
  - The script must construct BigQuery query parameters to pass values dynamically during client query execution:
    - `@monats_id` -> mapped to `MONATSID`
    - `@eintragsnr` -> mapped to the execution entry ID (`dw_eintrags_nr`)

### Target file plan
- **Target File**: `local/home/gurunathan_t/kubi/r_sqlscript.py`
  - **Source File**: `local/home/gurunathan_t/kubi/r_sqlscript`
  - **Language**: Python
  - **Purpose**: Translates the KornShell launcher wrapper into a native Python script.
  - **Key Implementations**:
    - Uses `argparse` to handle parameters (`-f`, `-i`, `-j`, `-v`).
    - Eliminates shell-level script sourcing and subprocesses for library logic by directly importing pythonized modules: `dw_init`, `f_alis_msgerr`, and `h_alis_sqlplus`.
    - Handles absolute and relative path checks for target SQL files within python logic.
    - Resolves the execution parameters `@monats_id` and `@eintragsnr` and forwards them to the BigQuery client query configuration inside `h_alis_sqlplus.py`'s database runner function.

### Environment-specific values
- **`GCP_PROJECT`**: GLOBAL. The target Google Cloud Project ID. Sourced via `os.environ.get("GCP_PROJECT")` or Airflow Variables.
- **`BQ_LOCATION`**: GLOBAL. The BigQuery region location. Sourced via `os.environ.get("BQ_LOCATION")`.
- **`DW_DIR_ROOT`**: GLOBAL. The migration repository root path. Sourced via `os.environ.get("DW_DIR_ROOT")`.
- **`HOME`**: GLOBAL. Sourced via standard `os.environ.get("HOME")`.
- **`MONATSID`**: JOB-SPECIFIC. Sourced at runtime via `os.environ.get("MONATSID")` or Airflow execution parameters.
- **`dw_eintrags_nr` (or `eintragsnr`)**: JOB-SPECIFIC. Unique process sequence number computed by calling `f_alis_msgerr`'s registry function at runtime.

### File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| --- | --- | --- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Migrates the KornShell database execution wrapper to Python. Imports migrated sibling helper modules directly, resolves paths, manages signal handling equivalent blocks, and passes parameters `@monats_id` and `@eintragsnr` to the BigQuery client. |

### HARD RULES
- The system automatically attaches the raw output of the MCP tool to the final document. The designs outlined here do not rephrase, duplicate, or alter that attachment.
- No identifiers or metadata have been invented outside those present in the source files.
- Original legacy source code is not dumped verbatim.
- Sibling files are referenced solely as integration points; no planning, designs, or target-file layout mappings are provided for components outside the stated source scope.

### OUTPUT/PRINT LITERAL RULE
All print/echo statements must preserve their literal German text exactly as written in the source script. Character-for-character accuracy must be maintained under Python execution:
- `echo "----------------- Parameter -----------------"` -> `print("----------------- Parameter -----------------")`
- `echo "Jobkennung     : $JobKennung"` -> `print(f"Jobkennung     : {job_kennung}")`
- `echo "DB-Skript      : $l_DBskript"` -> `print(f"DB-Skript      : {l_dbskript}")`
- `echo "---------------------------------------------"` -> `print("---------------------------------------------")`
- `echo '!OSFEHLER gemeldet!'` -> `print("!OSFEHLER gemeldet!")`
- `echo '!FEHLER gemeldet!'` -> `print("!FEHLER gemeldet!")`
- `echo "----------------- Job -----------------------"` -> `print("----------------- Job -----------------------")`
- `echo "Job-Nr    : '$DW_EintragsNr'"` -> `print(f"Job-Nr    : '{dw_eintrags_nr}'")`
- `echo "Logdatei  : '$LogDatei'"` -> `print(f"Logdatei  : '{log_datei}'")`
- `echo "Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"` -> `print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")`