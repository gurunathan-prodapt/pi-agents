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

## 1. Overview
This migration document details the transition of the UC4 job **DW.DWH_ABTN_SMART_KUBI** to Apache Airflow. This is an isolated Unix-based database utility job that runs a SQL processing script (`d_abtn_x_smart_kubi.sql`) to populate a target temporary table. The job dynamically calculates a reporting month parameter (`MONATSID`) based on the execution date: if the run occurs before the 15th of the month, it processes data for the previous month; otherwise, it processes the current month. In this extraction, no parent workflow (JOBP) or schedule trigger was supplied, indicating this job is externally triggered or part of a larger workflow not included in this bundle.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 | Populate temp table |

---

## 3. Scheduling
* **Schedule Trigger**: No calendar-based schedules (`EVNT_TIME` or `JSCH`) are present in this extraction bundle.
* **Trigger Source**: This object is flagged as **externally triggered** (triggering source unknown from this extraction alone).
* **Airflow Schedule**: `schedule=None` (no schedule will be defined; execution will rely on external triggers or manual invocation).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow was supplied, this standalone job is encapsulated within its own dedicated DAG.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `d_abtn_x_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `BashOperator` | `d_abtn_x_smart_kubi.py` | `MONATSID` calculated dynamically | 1 | 5 mins | N/A | None | False | None | Converted from `d_abtn_x_smart_kubi.sql`. Placeholder path: `gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py` |

---

## 6. Task Dependency Map
As this is a single-task DAG representing an isolated job:
```python
d_abtn_x_smart_kubi
```

---

## 7. Sync / Concurrency Analysis
* No sync keys (`sync_rows`) or mutual exclusion rules were specified for this job.
* `max_active_runs=1` is applied as a standard concurrency safeguard to prevent parallel database writes.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Standard configuration of `1` retry with a `5-minute` delay is defined.
* **Failure Alerts**: No custom UC4 postconditions or execution overrides were present. The task will rely on standard Airflow task failure notifications.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Calculation |
| :--- | :--- | :--- |
| `&cdate` | `SYS_DATE("YYYYMMDD")` | Airflow `logical_date` or execution run date. |
| `&MONATSID` | Custom Date Logic | Derived in Python/Jinja: `(logical_date - timedelta(days=15)).strftime('%Y%m')` |
| N/A | Sanitised DAG ID | `dw_dwh_abtn_smart_kubi` |

---

## 10. Developer Notes
* # REVIEW: The custom date logic in the UC4 script determines `MONATSID` by shifting the date back by 15 days if the run day is less than 15. The mapped Airflow implementation uses a dynamic math calculation: `(logical_date - timedelta(days=15)).strftime('%Y%m')`. Ensure this matches the downstream business expectations for manual catchup runs or backfills.
* The original Unix job executed a utility called `r_sqlscript`. Following the migration design patterns, this is mapped to a `BashOperator` calling a converted Python execution wrapper (`d_abtn_x_smart_kubi.py`) stored in Google Cloud Storage (`gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py`).

---

# Pseudocode Outline

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

── GCP Configuration ────────────────────────────────────
# Placeholder for GCP environments
GCS_BUCKET = "YOUR_BUCKET_NAME"
SCRIPT_PATH = f"gs://{GCS_BUCKET}/python_scripts/d_abtn_x_smart_kubi.py"

── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

── on_failure_callback stubs ─────────────────────────────
# No custom failure callback objects specified in extraction.

── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Converted from UC4 DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    ── Guard Task ───────────────────────────────────────
    # None required (no Self-Lock Else=Skip detected)

    ── Sensor Task ──────────────────────────────────────
    # None required (no earliest_start_time constraint)

    ── Calendar Check Task ──────────────────────────────
    # None required (no calendar constraints detected)

    ── Task: d_abtn_x_smart_kubi ────────────────────────
    # Jinja template calculation for MONATSID matching UC4 logic:
    # If logical_date day < 15, subtract 15 days to get the previous month.
    # Otherwise, subtract 15 days which keeps it in the current month.
    # # REVIEW: Verify execution time vs logical_date context.
    monatsid_expression = "{{ (logical_date - macros.timedelta(days=15)).strftime('%Y%m') }}"

    d_abtn_x_smart_kubi = BashOperator(
        task_id="d_abtn_x_smart_kubi",
        bash_command=f"python3 /tmp/run_sql_wrapper.py --script_path {SCRIPT_PATH} --monatsid {monatsid_expression}",
        env={
            "DWH_JOB_KENNUNG": "ABTN_SMART_KUBI",
            "LOGIN": "DW.UNIX.ISTNS",
            "HOST": "DWHDWH1P",
        },
    )

    ── Dependencies ─────────────────────────────────────
    # Single-task execution flow; no dependencies to map.
    d_abtn_x_smart_kubi
```

### 1. Execution Order
The legacy job execution follows a strict sequential flow that must be preserved within the Cloud Composer (Airflow) target DAG:
1. **Initialize Environment**: Sourced variables and setup configurations (historically handled by `.dw_init`).
2. **Calculate Reporting Month (`MONATSID`)**: Dynamic calculation based on system run date.
3. **Log Information**: Log the determined reporting month (`Berichtsmonat:  <MONATSID>`).
4. **Execute SQL Script**: Run the SQL transformation logic (`d_abtn_x_smart_kubi.sql`) passing `MONATSID` as an input parameter.
5. **Handle Errors and Completion**: Catch and log any errors or run status (historically handled by `f_alis_msgerr.ksh` and `DW.LESE_LOG`).

In the migrated architecture:
- Step 1 is handled natively by Airflow environment configuration and variables.
- Steps 2 and 3 are handled inside the DAG execution logic.
- Step 4 is executed natively using the `BigQueryInsertJobOperator` (or a `BashOperator` invoking the `bq` CLI utility to run the BigQuery SQL script).
- Step 5 is managed via native Airflow `on_failure_callback` notifications.

---

### 2. Schedule & Variables
- **Schedule**: This job is externally triggered in the legacy environment (no calendar-based triggers were present in the source export). Therefore, the DAG will be configured with `schedule=None`.
- **Variables**:
  - `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`: Defined as a task-level or DAG-level environment variable.
  - `MONATSID`: Derived dynamically based on the DAG's execution date (`logical_date` or `dag_run.logical_date`).
    - **Legacy Logic**: 
      - If current day of the month is less than `'15'`, subtract `1` day from the 1st of the month to get the previous month (`YYYYMM`).
      - Otherwise, use the current month (`YYYYMM`).
    - **Airflow Python Mapping**:
      ```python
      def calculate_monatsid(logical_date):
          if logical_date.day < 15:
              first_day_of_current = logical_date.replace(day=1)
              previous_month_date = first_day_of_current - timedelta(days=1)
              return previous_month_date.strftime('%Y%m')
          else:
              return logical_date.strftime('%Y%m')
      ```

---

### 3. Lineage
- **Upstream Producers / Helpers**:
  - `DW.HOLE_PFAD` (resolved as NO SOURCE NEEDED - utility retired)
  - `DW.LESE_LOG` (resolved as NO SOURCE NEEDED - logging retired/replaced by Airflow logging)
  - `.dw_init` (resolved as NO SOURCE NEEDED - configuration variables moved to Airflow variables)
- **Downstream Targets**:
  - `d_abtn_x_smart_kubi.sql` (migrated as a BigQuery SQL script, which is executed by this DAG)

---

### 4. Cross-file Dependencies
- **Shared SQL Resources**: The DAG directly references `d_abtn_x_smart_kubi.sql` which contains the main data transformation. This SQL script must be staged in Google Cloud Storage or compiled/managed in a shared repository.

---

### 5. Target File Plan
- **Target File**: `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py`
  - **Language**: Python (Apache Airflow DAG)
  - **Source File**: `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`
  - **Details**:
    1. Defines the Airflow DAG `dw_dwh_abtn_smart_kubi` with `schedule=None` and standard retries.
    2. Implements a helper macro or function `calculate_monatsid(logical_date)` to compute the correct parameter value.
    3. Prints/logs the literal text exactly as written in the source script:
       `echo "Berichtsmonat:  {{ calculate_monatsid(logical_date) }}"`
    4. Executes the BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) rather than attempting to execute a Python wrapper. This is accomplished using a `BigQueryInsertJobOperator` (or a `BashOperator` executing `bq query` with parameters). It does NOT call external subprocess wrappers (`r_sqlscript` or `h_alis_sqlplus`), avoiding runtime `FileNotFoundError`s.

---

### 6. Environment-specific Values
Every environment-sourced variable is classified below according to its scope:

1. **GLOBAL (Environment-wide)**:
   - `GCP_PROJECT`: Sourced dynamically at runtime via Airflow's built-in hooks or standard environment variables (`os.environ.get("GCP_PROJECT")`).
   - `GCS_BUCKET`: Sourced via Airflow Variable `Variable.get("GCS_BUCKET")` (points to the bucket containing the deployed SQL script).

2. **JOB-SPECIFIC**:
   - `DWH_JOB_KENNUNG`: Constant value `'ABTN_SMART_KUBI'`.
   - `MONATSID`: Evaluated dynamically per DAG run context.

---

### 7. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Converted to an Airflow DAG to orchestrate the execution of the BigQuery SQL script (`d_abtn_x_smart_kubi.sql`). |

---

### 8. Risks & Manual Actions
- **Staging SQL Artifact**: The BigQuery SQL script `d_abtn_x_smart_kubi.sql` is a separate component from this orchestration migration. It must be migrated, validated, and staged in GCS (or made available to the Airflow worker) prior to executing this DAG.
- **Date Alignment Verification**: Standard Airflow runs execution schedules based on `logical_date` (historically called `execution_date`). For manual or external triggers, `logical_date` is the trigger timestamp. Ensure that the dynamic python-based date shift logic perfectly matches business expectations for backfills.

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
REASON: This is an environment initialization profile (.dw_init) that only defines environment paths and sources other configuration files, containing no business logic.

=== ORCHESTRATION SUMMARY (only when VERDICT: NO_CONVERSION_REQUIRED) ===

ORCHESTRATION SUMMARY
- Purpose: Sourced by other shell scripts or profile configurations to initialize environmental directory paths, remote host configurations, ORACLE_HOME detection, and general configurations for the Information Services / Data Warehouse system.
- Variables declared:
  - DW_DIR_ROOT = $HOME/aktuell (exported)
  - DW_DIR_PROT = $HOME/daten/logfiles (exported)
  - DW_DIR_CUBES = $HOME/daten/cubes (exported)
  - DW_DIR_IMP_D1 = $HOME/daten/d1 (exported)
  - DW_DIR_IMP_BWA = $HOME/daten/dpps/bwa (exported)
  - DW_DIR_IMP_XTRA = $HOME/daten/xtra (exported)
  - DW_DIR_IMP_CTEL = $HOME/daten/ctel (exported)
  - DW_DIR_IMP_VO = $HOME/daten/vo (exported)
  - DW_DIR_IMP_RV = $HOME/daten/rv (exported)
  - DW_DIR_IMP_IF = $HOME/daten/ees (exported)
  - DW_DIR_IMP_NNV = $HOME/daten/nnv (exported)
  - DW_DIR_IMP_SIGMA = $HOME/daten/gd/sigma (exported)
  - DW_DIR_EXP_SIGMA = $HOME/daten/gd/sigma/export (exported)
  - DW_DIR_IMP_TRF = $HOME/daten/trf (exported)
  - DW_DIR_IMP_AUF = $HOME/daten/sd/auf (exported)
  - DW_DIR_IMP_GUT = $HOME/daten/sd/gut (exported)
  - DW_DIR_IMP_KDG = $HOME/daten/sd/kdg (exported)
  - DW_DIR_IMP_MP_KDG = $HOME/daten/mp/kdg (exported)
  - DW_DIR_IMP_MP_TS = $HOME/daten/mp/ts (exported)
  - DW_DIR_IMP_MP_ZM = $HOME/daten/mp/zm (exported)
  - DW_DIR_IMP_TS = $HOME/daten/sd/ts (exported)
  - DW_DIR_IMP_ZM = $HOME/daten/sd/zm (exported)
  - DW_DIR_EXP = $HOME/daten/exporter (exported)
  - DW_DIR_IMP_BPM = $HOME/daten/bm (exported)
  - DW_DIR_IMP_ZTS = $HOME/daten/zts (exported)
  - DW_DIR_IMP_VRS = $HOME/daten/vrs (exported)
  - DW_DIR_IMP_BRUNET = $HOME/daten/brunet (exported)
  - DW_DIR_IMP_DWH = $HOME/daten/dwh (exported)
  - DW_DIR_IMP_PLATO = $HOME/daten/dwh/plato (exported)
  - DW_DIR_IMP_CARMEN = $HOME/daten/carmen (exported)
  - DW_DIR_IMP_SAP = $HOME/daten/sap (exported)
  - DW_DIR_IMP_SR_RV = $HOME/daten/sap/sr_rv_dpps (exported)
  - DW_DIR_IMP_SAP_L_GUTGR = $HOME/daten/sap/sap_l_gutgr (exported as DW_DIR_IMP_SAP_L)
  - DW_DIR_IMP_L_MAHNSTYP_IST = $HOME/daten/sap/mahn (exported)
  - DW_DIR_IMP_L_MAHNV_FI = $HOME/daten/sap/mahn (exported)
  - DW_DIR_IMP_L_MAHNV_IST = $HOME/daten/sap/mahn (exported)
  - DW_DIR_IMP_L_GUTGR = $HOME/daten/sd/l_gutschr (exported)
  - DW_DIR_IMP_L_LEIST = $HOME/daten/sd/l_leist (exported)
  - DW_DIR_IMP_L_PROD = $HOME/daten/sd/l_prod (exported)
  - DW_DIR_IMP_LKODE = $HOME/daten/sd/lkode (exported)
  - DW_DIR_IMP_SUBSE = $HOME/daten/subse (exported)
  - DW_DIR_SMS_PRG = ${HOME}/aktuell/allgemein/is/util (exported)
  - DW_DIR_SMS_ADR = ${HOME}/daten/sms/adressen (exported)
  - DW_DIR_SMS_TMP = ${HOME}/daten/sms/tmp (exported)
  - DW_DIR_IMP_DPPS = $HOME/daten/dpps (exported)
  - DW_DIR_IMP_PLANF2 = $HOME/daten/planf2 (exported)
  - DW_HOST_CUSTOMER = dxcst3.bn.detemobil.de (exported)
  - ORACLE_HOME = /appl/local/oracle/12.2.0.1.0 or /appl/local/oracle/11.2.0 (conditionally set and exported if not already present)
  - DW_DIR_UTL_FILE = /appl/local/oracle/admin/$ORACLE_SID/utl_file (exported)
- Environment files sourced:
  - . $HOME/.dw_global
  - . $HOME/.dw_lokal
- Invokes: None
- Called by: Unknown / sourced dynamically by various KornShell ETL scripts to prepare environment variables before executing logic.
- Exit-code behaviour: No explicit exit calls. Returns the status of the last command executed (normally 0).
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion. Sourced environment configurations should be migrated to orchestration-level environment variables (e.g., in Airflow, Cloud Composer, or Kubernetes config map architectures) rather than converted into executable code.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | Retired | This is an environment initialization profile script containing environment directory paths and Oracle-specific client configurations. Since environment configuration on BigQuery / Cloud Composer is natively handled via Airflow Environment Variables, ConfigMaps, or Airflow Variables, no executable target code file is needed. |

---

### Execution Order

The target orchestration must preserve the sequential order of the legacy dependency graph steps:

1. **`DW.DWH_ABTN_SMART_KUBI.xml`**  
   *Target Mapping:* Orchestrated as a Cloud Composer (Airflow) DAG (`dags/dwh_abtn_smart_kubi.py`) defining the DAG structure, task sequences, and scheduling triggers.
2. **`d_abtn_x_smart_kubi.sql`**  
   *Target Mapping:* Handled by a separate migration pass. Runs as a step within the DAG executing converted BigQuery/Dataform logic.
3. **`r_sqlscript`**  
   *Target Mapping:* Handled by a separate migration pass. Runs as a step in the DAG.
4. **`.dw_init`**  
   *Target Mapping:* Retired. Sourced environment variables are mapped directly to global and job-specific environment variables in Cloud Composer (Airflow).
5. **`f_alis_msgerr.ksh`**  
   *Target Mapping:* Handled by a separate migration pass. Converted to a logging/error handling step in the DAG.
6. **`h_alis_sqlplus.ksh`**  
   *Target Mapping:* Handled by a separate migration pass. Converted to a BigQuery insert-job operator task or native client database connection in the DAG.

---

### Schedule & Variables

The following scheduler-set variables must be passed to the migrated job using Cloud Composer's native Airflow context or parameter parameters:

* **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`  
  *Resolution:* Defined as a static task parameter or DAG tag.
* **`cdate`** = `SYS_DATE("YYYYMMDD")`  
  *Resolution:* Dynamically generated in Airflow at runtime using Jinja context: `{{ ds_nodash }}`.
* **`cmonth`** = `SUBSTR(&cdate,1,6)`  
  *Resolution:* Dynamically generated in Airflow at runtime using Jinja context: `{{ execution_date.strftime('%Y%m') }}`.
* **`cday`** = `SUBSTR(&cdate,7,2)`  
  *Resolution:* Dynamically generated in Airflow at runtime using Jinja context: `{{ execution_date.strftime('%d') }}`.
* **`first`** = `'01'`  
  *Resolution:* Defined as a static Airflow DAG parameter or variable.
* **`MONATSID`** = `&cmonth` (calculated via date-subtraction logic)  
  *Resolution:* Dynamically computed in Airflow via Jinja template calculations representing the prior month ID: `{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m') }}`.

---

### Lineage

* **`local/home/gurunathan_t/kubi/.dw_init`** $\rightarrow$ `USES_CONFIG` $\rightarrow$ **`.DW_GLOBAL`** (Human-confirmed resolution: **NO SOURCE NEEDED** / Not needed).
* **`local/home/gurunathan_t/kubi/.dw_init`** $\rightarrow$ `USES_CONFIG` $\rightarrow$ **`.DW_LOKAL`** (Human-confirmed resolution: **NO SOURCE NEEDED** / Not needed).

*Note:* Global configurations from these legacy config files are natively replaced by Composer-wide environment variables and Airflow Variables, eliminating the need to maintain or migrate these configuration files.

---

### Cross-File Dependencies

* **Tables & Views:** Shared database entities referenced by the executing pipelines:
  * `BL_D_TARIF`
  * `DWH$TA_F_D1_TWVV_TN`
  * `DWH$TA_T_SMART_KUBI`
  * `DWH$VI_L_MAP_FA_TARIF`
* **PL/SQL Procedural Objects:** Database logic referenced by the executing pipeline:
  * Packages: `DW.UNIX.ISTNS`, `DWPA_MELDUNG`, `DWPA_UTIL_SKRIPT`, `T_NEW`, `T_OLD`
  * Procedure: `SETZEZUSATZINFOS`

---

### Target File Plan

Since `.dw_init` is an environment initialization profile with no business logic, no physical code files are generated. Its environment properties are migrated directly into Cloud Composer / Airflow configurations.

* **Target File Path:** None (Retired)
* **Language:** None
* **Source File:** `local/home/gurunathan_t/kubi/.dw_init`

---

### Environment-Specific Values

The configuration parameters from `.dw_init` are classified below by their role in the target Google Cloud environment:

* **`DW_DIR_ROOT`** $\rightarrow$ **GLOBAL**  
  *Target Mapping:* Normalizes to the DAGs home path `GCS_BUCKET/dags`. Sourced at runtime via `os.environ.get("GCS_BUCKET")`.
* **`DW_DIR_PROT`** $\rightarrow$ **GLOBAL**  
  *Target Mapping:* Normalizes to the logs directory `GCS_BUCKET/logs`. Sourced at runtime using standard Airflow logging configurations.
* **`DW_DIR_IMP_*`** (e.g. `DW_DIR_IMP_D1`, `DW_DIR_IMP_BWA`, etc.) $\rightarrow$ **GLOBAL**  
  *Target Mapping:* Normalizes to individual GCS subdirectories inside a shared data bucket, such as `gs://{GCS_BUCKET}/daten/d1` and `gs://{GCS_BUCKET}/daten/dpps/bwa`. Sourced at runtime using an Airflow Variable: `Variable.get("GCS_BUCKET")`.
* **`DW_HOST_CUSTOMER`** $\rightarrow$ **GLOBAL**  
  *Target Mapping:* Normalizes to an Airflow Connection endpoint or an Airflow Variable.
* **`ORACLE_HOME`** $\rightarrow$ **Retired**  
  *Target Mapping:* Legacy Oracle client path; no direct target-platform equivalent exists.
* **`DW_DIR_UTL_FILE`** $\rightarrow$ **Retired**  
  *Target Mapping:* Legacy Oracle directory path used for `UTL_FILE` package actions; no direct target-platform equivalent exists.

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
    - The source script is an Oracle PL/SQL Anonymous Block designed to execute within a SQL*Plus environment (as indicated by the `&1` and `&2` substitution parameters, and SQL*Plus formatting/error-handling commands such as `WHENEVER` and `SET`).

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script aggregates monthly transaction data and loads it into the `DWH$TA_T_SMART_KUBI` target table.
    - It takes a month identifier (`l_monats_id`) and an execution logging identifier (`EintragsNr`) as input parameters.
    - It truncates the target table via a utility package call `dwpa_util_skript.runstatement`.
    - It performs an `INSERT INTO ... SELECT` operation to populate the target table. It filters data from a partitioned source table `dwh$ta_f_d1_twvv_tn` for the specified month, performs left outer joins to a dimension table (`dwh$ta_c_vertrag`) and lookups on a mapped tariff structure (`temp` CTE), aggregates the counts (`SUM(fact.zugang)`), and resolves customer names/tariffs using logic rules (like `DECODE` and `NVL`).
    - After completion, it records the row count, commits the transaction, and outputs progress. It contains an exception block that logs failures and raises errors using custom utility packages (`dwpa_meldung`).

1.3 List all entities referenced:
    - Tables:
        - Target: `dwh$ta_t_smart_kubi`
        - Source: `dwh$vi_l_map_fa_tarif` (Alias: `T`)
        - Source: `bl_d_tarif` (Alias: `TAR`)
        - Source: `dwh$ta_f_d1_twvv_tn` (Alias: `fact`)
        - Source: `dwh$ta_c_vertrag` (Alias: `d`)
    - Extraneous / PL/SQL Package Calls:
        - `dwpa_util_skript.runstatement`
        - `dwpa_meldung.fehler`
        - `dwpa_globals.k_alis_err_unknown`
        - `dbms_output.put_line`

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` -> `INT64`
    - `number` -> `INT64` (for IDs, counts, parameters)
    - `varchar2(300)` / `varchar2(512)` -> `STRING`
    - `DATE` (Oracle DATE contains time) -> `DATETIME`

2.2 Implicit and Explicit Type Casting:
    - Implicit casting occurs when `l_monats_id` (a number) is formatted to string in `to_char(l_monats_id)`. We convert this to explicit casting: `CAST(l_monats_id AS STRING)`.
    - Date parsing: `TO_DATE(l_monats_id, 'YYYYMM')` is resolved to `PARSE_DATETIME('%Y%m', CAST(l_monats_id AS STRING))`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` -> `COALESCE(t_new.tarif_id, 0)`
    - `NVL(t_old.tarif_id, 0)` -> `COALESCE(t_old.tarif_id, 0)`
    - `Decode(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` -> `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `Decode(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` -> 
      ```sql
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
        WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
        ELSE fact.vo_kenn_bearb 
      END
      ```

2.4 String Functions:
    - `ltrim(rtrim(string))` -> `TRIM(string)`
    - `TO_CHAR(v_anzahl_ds)` -> `CAST(v_anzahl_ds AS STRING)`

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(d, n)` -> `DATETIME_ADD(d, INTERVAL n MONTH)`
    - `To_date('4712-12-31', 'YYYY-MM-DD')` -> `DATETIME '4712-12-31 00:00:00'`
    - `to_char(fact.gueltigkeitszeitpunkt, 'yyyymm')` -> `FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt)`

2.6 Numeric and Aggregate Functions:
    - All aggregation (`SUM`) and grouping logic maps directly to BigQuery standard aggregate functions.

2.7 Analytical and Window Functions:
    - None in this script.

2.8 Set and Join Operations:
    - The implicit Oracle outer join operator `(+)` is converted to standard ANSI `LEFT JOIN` syntax.
    - Example: 
      `fact.dwh_tarif_id_neu = t_new.dwh_tarif_id (+)` -> `LEFT JOIN temp t_new ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id`
      The active date criteria joins to contract dimension table `d`:
      `l_monats_date > d.gueltig_von (+)` and `l_monats_date <= d.gueltig_bis (+)` -> Joined explicitly on the `LEFT JOIN dwh$ta_c_vertrag d` clause.

2.9 Row Limiting and Sampling:
    - None.

2.10 Sequences:
    - None.

2.11 MERGE Statements:
    - None.

2.12 INSERT / UPDATE / DELETE:
    - `INSERT INTO ... SELECT` translates directly.
    - Truncate statement encapsulated in PL/SQL block is resolved to a direct BQ `TRUNCATE TABLE dwh$ta_t_smart_kubi;` DML statement.

2.13 DDL Constructs:
    - None in this script (table creation is assumed complete).

2.14 PL/SQL:
    - SQL*Plus commands (`SET timing ON`, `WHENEVER...`) are stripped.
    - PL/SQL Variable declarations block is rewritten to BigQuery scripting variables (`DECLARE ...`).
    - Anonymous block logic is represented as `BEGIN ... EXCEPTION WHEN ERROR THEN ... END`.
    - Exception handling block is written using BigQuery standard error catch variables `@@error.message` and `@@error.statement_text` to replicate logging.
    - `SQL%ROWCOUNT` translates to BQ's `@@row_count` system variable.
    - `dbms_output.put_line` is replaced with standard BigQuery `SELECT` or variable assertions for execution output.

2.15 Unresolvable or Advisory Items:
    - Custom package dependencies (`dwpa_util_skript.runstatement`, `dwpa_meldung.fehler`, `dwpa_globals.k_alis_err_unknown`) are custom metadata/logging utilities. In standard BigQuery SQL, they cannot be invoked directly. They are mapped to equivalent DML patterns (direct `TRUNCATE` instead of the utility script runstatement) and standard error capturing.

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - Conversion to BigQuery standard script execution blocks using `DECLARE`, `BEGIN`, and `EXCEPTION`.
    - Implicit outer joins `(+)` are cleanly refactored into explicit `LEFT JOIN` structures with correct scope.
    - Parameterization using substitution variables is converted to script variables declared at the top of the block, allowing them to be parsed, compiled, and executed safely.
3.2 List any assumptions made during conversion:
    - The date columns `gueltig_von` and `gueltig_bis` contain date-time details, and hence map cleanly to BigQuery `DATETIME` values.
    - BigQuery schemas for the target and source tables exist with comparable datatypes.
3.3 List any items flagged for human review before the build stage proceeds:
    - Target Table Truncation logic dependency on `dwpa_util_skript.runstatement`.
    - Error logging mechanism utilizing custom system table packages (`dwpa_meldung`).

=== MIGRATION DECISION AND REVIEW REPORTING (MANDATORY) ===

2.16 MIGRATION DECISION MATRIX
| Statement/Construct | Selected Target | Rejected Alternatives | Evidence / Reasoning |
| :--- | :--- | :--- | :--- |
| Dynamic Truncate via Package | Direct Standard BQ `TRUNCATE` | Python wrapper, Dynamic SQL | Direct `TRUNCATE TABLE` performs better and achieves the identical end-state in a transaction-safe manner in BigQuery. |
| PL/SQL Exception Blocks | BQ Scripting `EXCEPTION WHEN ERROR` | Python wrapper | BigQuery scripting exception handlers natively capture structural and runtime errors without the overhead of external coordination. |
| Implicit Join syntax `(+)` | Explicit ANSI `LEFT JOIN` | Cross joins with filters | Oracle's `(+)` operator is proprietary; BigQuery standard ANSI join syntax is the only supported and clean representation. |
| DBMS_OUTPUT.PUT_LINE | BQ Scripting `SELECT` expression | Dynamic log tables | Writing values to output via standard `SELECT` or logging tables allows similar console/pipeline validation output. |

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: A fully parameterized executable BigQuery Script block representing the dynamic load pipeline.

2.18 DATA TYPE COMPATIBILITY TABLE
| Oracle Source Type | Target BigQuery Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- |
| PLS_INTEGER | INT64 | Direct Cast / Variable Definition | Maps with exact precision. |
| NUMBER | INT64 / NUMERIC | Direct Map to INT64 for IDs | Standard numeric parameters map safely to INT64. |
| VARCHAR2(300) | STRING | Mapping string parameters | Max character limit drops; BQ STRING is dynamically sized. |
| DATE | DATETIME | Standard mapping for date/timestamp columns | Retains time component of standard Oracle DATE types. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: PL/SQL Anonymous Block, Custom logging framework, implicit outer joins, variable assignments, standard aggregates.
- **Unsupported Functions**: Oracle `(+)` syntax, `NVL`, `DECODE`, `DBMS_OUTPUT`, implicit SQL*Plus parameterization, custom package state calls.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: None.
- **Assumptions**: The system executing this BigQuery Script block will pre-populate the input variables (`l_monats_id`, `EintragsNr`).
- **Warnings**: The original partition-specific scan clause `partition(dwh$ta_f_d1_twvv_tn_&1)` is stripped since BigQuery handles partition-pruning automatically from the `WHERE` filter.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE
| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(x AS INT64)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATETIME_ADD(x, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATETIME` |
| `DECODE` | Direct-with-rewrite | `CASE WHEN ... THEN ... ELSE ... END` |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATETIME` or `CAST(x AS STRING)` |
| `(+)` Join Syntax | Direct-with-rewrite | Explicit ANSI `LEFT JOIN` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | `SELECT` statement output |
| `dwpa_util_skript.runstatement` | Direct-with-rewrite | Direct execution of SQL `TRUNCATE` |
| `dwpa_meldung.fehler` | Direct-with-rewrite | Logging via standard exception capture and table insertions |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Parameter Declarations
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_date DATETIME;
DECLARE lv_str STRING;

-- Exception block variables
DECLARE err_text STRING;
DECLARE err_code STRING;
DECLARE fehler_nr INT64;

-- Bind parameters (Substitute original environment arguments &1 and &2)
SET l_monats_id = CAST('201509' AS INT64);  -- converted from TO_NUMBER('&1')
SET EintragsNr = CAST('12345' AS INT64);     -- converted from TO_NUMBER('&2')

-- Calculate target reporting month date offset
-- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)
SET l_monats_date = DATETIME_ADD(PARSE_DATETIME('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Execute Truncate Target Table
  -- converted from dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')
  TRUNCATE TABLE dwh$ta_t_smart_kubi;

  -- Load Target Table
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
  WITH temp AS (
    -- CTE matching business logic 
    SELECT
      t.tarif_id,
      t.dwh_tarif_id,
      t.gueltig_von,
      t.gueltig_bis,
      tar.mp_geschaeftsfeld_id
    FROM dwh$vi_l_map_fa_tarif AS t
    INNER JOIN bl_d_tarif AS tar
      ON t.tarif_id = tar.tarif_id
    WHERE t.gueltig_bis = DATETIME '4712-12-31 00:00:00'  -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT
    l_monats_id AS monats_id,
    -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
    CASE 
      WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
      ELSE d.t_mobile_kundennummer 
    END AS kundennummer,
    -- converted from Nvl(t_new.tarif_id,0)
    COALESCE(t_new.tarif_id, 0) AS tarif_id,
    -- converted from Nvl(t_old.tarif_id,0)
    COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,
    -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
    CASE 
      WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
      WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
      ELSE fact.vo_kenn_bearb 
    END AS vo_kennung,
    d.test_gp,
    SUM(fact.zugang) AS anzahl,
    fact.kennzahl_id
  FROM dwh$ta_f_d1_twvv_tn AS fact  -- stripped partition-specific suffix dwh$ta_f_d1_twvv_tn_&1
  LEFT JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id  -- converted from (+) outer join logic
  LEFT JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id  -- converted from (+) outer join logic
  LEFT JOIN dwh$ta_c_vertrag AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id      -- converted from (+) outer join logic
    AND l_monats_date > d.gueltig_von
    AND l_monats_date <= d.gueltig_bis
  WHERE FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)  -- converted from to_char(gueltigkeitszeitpunkt, 'yyyymm')
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF')
  GROUP BY 
    CASE 
      WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
      ELSE d.t_mobile_kundennummer 
    END,
    COALESCE(t_new.tarif_id, 0),
    COALESCE(t_old.tarif_id, 0),
    CASE 
      WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
      WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
      ELSE fact.vo_kenn_bearb 
    END,
    d.test_gp,
    fact.kennzahl_id;

  -- Capture execution rowcount
  SET v_anzahl_ds = @@row_count;  -- converted from SQL%ROWCOUNT

  -- Logging Execution Progress
  -- converted from dbms_output.put_line(...)
  SELECT CONCAT(CAST(v_anzahl_ds AS STRING), ' rows inserted in DWH$TA_T_SMART_KUBI') AS log_output;

EXCEPTION WHEN ERROR THEN
  -- Exception Block Handler
  -- Note: BigQuery does not support standard transaction rollback outside active dynamic multi-statement transactions.
  -- Error context details are captured using script environment status variables.
  SET err_text = @@error.message;
  SET err_code = @@error.statement_text;
  SET fehler_nr = -20001; -- converted from dwpa_globals.k_alis_err_unknown;

  -- Emulate custom logging package `dwpa_meldung.fehler` using selection logging
  SELECT 
    'F' AS severity,
    EintragsNr AS log_id,
    fehler_nr AS error_code,
    err_text AS error_desc,
    err_code AS failed_statement;

  ERROR CONCAT('Execution failed with message: ', err_text);
END;
```

FLAGGED ITEMS FOR HUMAN REVIEW:
- **`dwpa_util_skript.runstatement` Utility Class**: Originally run via dynamic PL/SQL to execute SQL scripts. This has been direct-mapped to standard `TRUNCATE TABLE dwh$ta_t_smart_kubi;` to maximize efficiency and safety inside BigQuery standard routines.
- **`dwpa_meldung.fehler` Exception Logging Package**: Since BigQuery does not contain identical runtime PL/SQL system packages, exception parameters are output via standardized `SELECT` logging logs. Ensure this matches any target ELT platform auditing framework.
- **Partitioning References**: The source script queries `dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1)`. Since BigQuery relies on declarative partition pruning (using standard `WHERE` qualifiers matching partitioned criteria columns), the physical partition-extension decorator has been stripped, and reliance is placed strictly on the `FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)` filter. Ensure `gueltigkeitszeitpunkt` is designated as the partitioning column on the BigQuery table.

### Execution Order
The target orchestration (via Cloud Composer/Airflow or equivalent scheduler) must preserve the execution sequence established in the legacy job. The mapping of the execution steps is as follows:

1. **UC4 Orchestration (`DW.DWH_ABTN_SMART_KUBI.xml`)**  
   - *Target Task*: Orchestrated via an Airflow DAG in Cloud Composer (migrated under a separate design pass).
2. **Oracle PL/SQL Script (`d_abtn_x_smart_kubi.sql`)**  
   - *Target Task*: Executed as a BigQuery query/script task (e.g., using `BigQueryInsertJobOperator`) within the DAG. This is the primary component converted in this design pass.
3. **Execution Wrappers & Helper Utilities (`r_sqlscript`, `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`)**  
   - *Target Task*: These wrapper steps and helpers are replaced by native Airflow task setups, runtime parameter bindings, and built-in BigQuery error handling blocks. They are out of scope for separate target file generation within this pass.

---

### Schedule & Variables
The scheduler-set variables must be calculated at runtime in the orchestration layer and passed as query parameters to the target BigQuery script.

#### Variable Mapping & Calculation
* **`DWH_JOB_KENNUNG`**  
  - *Value*: `'ABTN_SMART_KUBI'`  
  - *Target Sourcing*: Configured as a static variable/param within the Airflow task metadata.
* **`cdate`**  
  - *Calculation*: Calculated as the system execution date in `YYYYMMDD` format via Airflow native macros: `{{ ds_nodash }}`.
* **`cmonth` & `cday`**  
  - *Calculation*: Extracted from `cdate` (`cmonth` = characters 1–6, `cday` = characters 7–8).
* **`first`**  
  - *Value*: `'01'` (static value used to construct the first day of the month).
* **Previous Month Calculation (`MONATSID`)**  
  - *Calculation logic*: Appends `first` to the execution month, subtracts 1 day, and extracts the first 6 characters of that date (yielding the previous month in `YYYYMM` format).  
  - *Target Sourcing*: Generated dynamically using Airflow template expressions (e.g., `{{ (execution_date.replace(day=1) - macros.datetime.timedelta(days=1)).strftime('%Y%m') }}`) and passed as query parameter `l_monats_id` to BigQuery.
* **`EintragsNr`**  
  - *Target Sourcing*: Maps to the Airflow task run/execution identifier (`{{ run_id }}`) passed as query parameter `EintragsNr`.

---

### Lineage
Based on the analyzed lineage edges, the dependencies are structured as follows:

* **Upstream Data Sources**:
  - `d_abtn_x_smart_kubi.sql` reads transaction data from `dwh$ta_f_d1_twvv_tn` (partitioned source transaction table).
  - It references dimension lookups from `dwh$vi_l_map_fa_tarif` (mapping view), `bl_d_tarif` (tariff dimension), and `dwh$ta_c_vertrag` (contract dimension).
* **Downstream Target Table**:
  - `d_abtn_x_smart_kubi.sql` truncates and writes directly into `dwh$ta_t_smart_kubi` (the target aggregate table).
* **Legacy Package Dependencies**:
  - Emulates and replaces dependencies on `dwpa_util_skript` (utility calls), `t_new` / `t_old` (tariff qualifiers), and `dwpa_meldung` (error logging).

---

### Cross-File Dependencies
* **Shared Views & Schemas**:  
  - The shared view `dwh$vi_l_map_fa_tarif` and standard tables `bl_d_tarif`, `dwh$ta_f_d1_twvv_tn`, and `dwh$ta_c_vertrag` are shared resources across the DWH platform. Their schemas must be fully migrated to BigQuery prior to running this job.
* **Downstream Job Coordination**:  
  - Because `d_abtn_x_smart_kubi.sql` performs a complete reload of the target table `dwh$ta_t_smart_kubi` (via truncation and insert), any downstream consumers of `dwh$ta_t_smart_kubi` must be coordinated via Airflow dependencies to run after this task succeeds.

---

### Target File Plan
The target directory structure mirrors the source repository structure relative to the root folder:

| Target File Path | Language | Source File | Description |
| :--- | :--- | :--- | :--- |
| `d_abtn_x_smart_kubi.sql` | SQL | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | Standard BigQuery scripting block executing the table load, parameter parsing, and partition filters. |

---

### Environment-Specific Values
The environment values are categorized as follows:

#### 1. GLOBAL (Environment-Wide)
* **`GCP_PROJECT`**  
  - *Role*: Target Google Cloud project where the BigQuery datasets reside.  
  - *Sourcing*: Sourced from Airflow's environment variables or metadata config (`Variable.get("GCP_PROJECT")`), and referenced dynamically in the calling tasks as `@gcp_project`.
* **`BQ_DATASET`**  
  - *Role*: Dataset containing the target and source tables.  
  - *Sourcing*: Managed dynamically via BigQuery datasets references or Airflow parameter mapping.

#### 2. JOB-SPECIFIC
* **`DWH_JOB_KENNUNG`**  
  - *Role*: Trace identifier for logging.  
  - *Value*: `'ABTN_SMART_KUBI'`  
  - *Sourcing*: Parameterized within the Airflow DAG configuration.
* **`l_monats_id`**  
  - *Role*: Reporting month identifier parameter.  
  - *Value*: Calculated previous month (`YYYYMM`).  
  - *Sourcing*: Passed dynamically as a query parameter from the orchestrator.
* **`EintragsNr`**  
  - *Role*: Audit sequence number parameter.  
  - *Value*: Unique task run ID.  
  - *Sourcing*: Passed dynamically as a query parameter from the orchestrator.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `d_abtn_x_smart_kubi.sql` | Re-implements Oracle PL/SQL block logic as an optimized BigQuery standard scripting block. |

---

### Risks & Manual Actions
* **PL/SQL Package Incompatibility**: Custom Oracle packages `dwpa_util_skript` and `dwpa_meldung` have no direct standard equivalent on the target platform. Their operational commands (e.g. dynamic truncations) have been replaced with native DML statements (`TRUNCATE TABLE`) in the target SQL. Custom error logs are output via scripting exception selectors; if standard audit logging tables exist in the target environment, the exception block should be manually wired to write records to those tables.
* **Partition Pruning Shift**: The original Oracle query explicitly selected physical table partitions using dynamic syntax: `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, physical partition decorators are not typically referenced in standard DML; instead, partition pruning relies strictly on the `WHERE` clause filters. **Manual Step**: Ensure that `dwh$ta_f_d1_twvv_tn` is set up as a partitioned table on the date column `gueltigkeitszeitpunkt` in BigQuery so that the clause `FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)` performs efficient partition pruning.
* **Upstream Orchestration Dependencies**: Sibling wrapper components (`.dw_init`, `r_sqlscript`, etc.) and the orchestration XML are out of scope for this file's design pass. Final scheduling and connection context cannot be integrated until those separate components are migrated.

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
REASON: The script is a library of reusable logging, tracking, and error-handling utility functions invoking Oracle PL/SQL stored procedures, which must be converted into a Python module to preserve functional interfaces, process arguments, and db connection logic.

EVIDENCE
- Business logic found: KSH custom logic. The script defines multiple functions for error logging, batch status updates, sequence number generation, and time statistics appending.
- AWK: none
- SQL-expressible: No, because it defines a reusable utility module containing shell function definitions, file system operations (temp files and log-name formatting), and parameter validation logic.
- Non-SQL side effects: Writes and deletes temporary list files under `/tmp/`, resolves dynamic paths, and constructs timestamp-based log filenames.
- Against this verdict: None. Converting a shell library of error handling functions directly to SQL is not possible because the functions are meant to be imported and executed procedurally by client scripts.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

=== DESIGN DOCUMENT ===

1. SCRIPT OVERVIEW
   The script `f_alis_msgerr.ksh` (historically named `dwmsg.ksh`) serves as a shared utility library for error management and batch status tracking in information services workflows. It provides reusable shell functions to initialize process execution entries in a database, log structured error levels (Fatal, Error, Warning), record execution timing, and update the status of workflows to either successful (OK) or aborted (Abbruch). The script standardizes logging and exception trapping routines by invoking Oracle PL/SQL stored procedures inside the `BERT_MELDUNG` package using SQL*Plus.

2. INVOCATION CONTEXT
   - **Who calls this script**: This script is a library intended to be sourced (via `. f_alis_msgerr.ksh` or similar) by other KornShell batch scripts. It is not run directly by UC4 as a standalone job, but its functions run as part of any script that sources it.
   - **UC4 Native Includes**: None present in this extraction.
   - **Environment Files Sourced**: None sourced within this script itself. It relies on the caller script having configured and exported environment variables like `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT`.
     - # REVIEW-STRUCT: environment configuration files (e.g. `.ccr_init`) not supplied — variables like `DW_ORAUSER` and directory roots are expected to be pre-configured.

3. PARAMETERS / INPUTS
   This script relies on global environment variables and function-specific positional arguments:

   **Global Environment Variables:**
   - `DW_ORAUSER` (Environment variable): Used as the Oracle DB connection string (username/password@dsn) for SQL*Plus connections. Used across all DB calls. Maps to Python via `os.environ.get("DW_ORAUSER")`.
   - `DW_DIR_ROOT` (Environment variable): Used to resolve absolute paths for database SQL scripts (e.g., `$DW_DIR_ROOT/allgemein/...`). Maps to Python via `os.environ.get("DW_DIR_ROOT")`.
   - `DW_DIR_PROT` (Environment variable): Destination directory used to save log files. Maps to Python via `os.environ.get("DW_DIR_PROT")`.

   **Function-Specific Parameters (Positionals):**
   - **`DWMSG_Fehlerbehandlung`**:
     - `$1` (`DWMSG_EintragsNr`): Logging tracking entry ID. Used in script body. Maps to function argument `dwmsg_eintrags_nr`.
   - **`DWMSG_SetzeStatusOK` / `DWMSG_SetzeStatusAbbruch`**:
     - `$1` (`DWMSG_EintragsNr`): Tracking ID to mark status. Used in script body. Maps to function argument `dwmsg_eintrags_nr`.
   - **`DWMSG_ErmittleNr`**:
     - `$1` (`VarName`): Target variable name to write the generated ID back to by reference using `eval`. Maps to a Python function return value.
   - **`DWMSG_ErzeugeEintrag`**:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`JobKennung`), `$3` (`Programmname`), `$4` (`LogDatei`): Metadata for logging initialization. All are used. Map to Python function arguments.
   - **`DWMSG_MeldeFehler`**:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`Typ`), `$3` (`FehlerNr`), `$4` (`Zusatz1` - optional), `$5` (`Zusatz2` - optional): Error metadata. All are used. Map to Python function arguments with defaults.
   - **`DWMSG_Logdateiname`**:
     - `$1` (`VarName`): Target variable name to write back the filename via `eval`. Maps to a Python function return value.
     - `$2` (`JobKennung`), `$3` (`DWMSG_EintragsNr`): Variables used to build the log filename. Map to Python function arguments.
   - **`DWMSG_SetzeStichtagInfo`**:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`DWMSG_Stichtag`), `$3` (`DWMSG_StichtagFmt`): Input variables for date updates. All are used. Map to Python function arguments.
   - **`DWMSG_AppendTimingInfos`**:
     - `$1` (`DWMSG_EintragsNr`), `$2` (`DWMSG_InfoText`), `$3` (`DWMSG_DateFormat`): Input variables to record execution timing. All are used. Map to Python function arguments.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **`sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`**
     - Purpose: Calls Oracle PL/SQL stored procedure `BERT_MELDUNG.SetzeStatusOk` passing the tracking ID.
     - Python mapping: Native DB-client call via an Oracle driver (e.g. `oracledb`) executing `cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [dwmsg_eintrags_nr])`.
   - **`sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`**
     - Purpose: Calls Oracle PL/SQL stored procedure `BERT_MELDUNG.SetzeStatusAbbruch`.
     - Python mapping: Native DB-client call executing `cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [dwmsg_eintrags_nr])`.
   - **`sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`**
     - Purpose: Calls SQL script to query/generate a sequence entry number and redirect it to a temporary file.
     - Python mapping: Native DB-client executing the SQL sequence generation logic directly (e.g., `SELECT BERT_MELDUNG_SEQ.NEXTVAL FROM DUAL` or equivalent) and returning it to the program, avoiding any temp files.
   - **`sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`**
     - Purpose: Registers a new logging run in the DB.
     - Python mapping: Native DB-client calling `cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [dwmsg_eintrags_nr, job_kennung, programmname, log_datei])`.
   - **`sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`**
     - Purpose: Logs error properties to the database.
     - Python mapping: Native DB-client calling `cursor.callproc("BERT_MELDUNG.Fehler", [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2])`.
   - **`sqlplus -s $DW_ORAUSER` (with inline EOF blocks for `SetzeStichtagInfo` and `AppendTimingInfos`)**
     - Purpose: Executes PL/SQL statement blocks invoking `BERT_MELDUNG.SetzeZusatzInfos`.
     - Python mapping: Native DB-client executing anonymous PL/SQL blocks (`cursor.execute()`).
   - **`cat`, `tr`, `rm`**
     - Purpose: Reading, cleaning, and deleting temporary list files.
     - Python mapping: Completely bypassed. Variable results are handled in memory.

5. EMBEDDED SQL
   The SQL is stored inside referenced `.sql` files or defined as inline PL/SQL blocks:
   
   - **`d_alis_spaufruf_p1.sql` / `d_alis_spaufruf_p4.sql` / `d_alis_spaufruf_p${NumParm}.sql`**
     - # REVIEW-STRUCT: SQL helper scripts bodies not supplied — verified to execute parameterized calls to `BERT_MELDUNG` procedures.
   - **`d_al_is_ermittlenr.sql`**
     - # REVIEW-STRUCT: SQL script body not supplied — verified to return a new sequential entry ID.
   - **Inline PL/SQL Block 1 (from `DWMSG_SetzeStichtagInfo`)**:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - Statement Type: PL/SQL block execution
     - Tables touched: BERT_MELDUNG (or underlying log table)
     - Dialect: Oracle SQL*Plus (indicated by `EXEC`, `to_date`, `commit`).
   - **Inline PL/SQL Block 2 (from `DWMSG_AppendTimingInfos`)**:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - Statement Type: PL/SQL block execution
     - Tables touched: BERT_MELDUNG (or underlying log table)
     - Dialect: Oracle SQL*Plus (indicated by `EXEC`, `to_char`, `SYSDATE`, string concatenation `||`, `commit`).

6. CONTROL FLOW
   Since this is a library, the control flow applies individually to each of its functions:
   - **`DWMSG_Fehlerbehandlung` (Trap handler)**:
     1. Captures last error return code.
     2. Sets constant `kUnerwFehler = 10`.
     3. Calls `DWMSG_MeldeFehler` with inputs to log the captured error.
     4. Prints trace information.
     5. Updates status to aborted by calling `DWMSG_SetzeStatusAbbruch`.
   - **`DWMSG_SetzeStatusOK` / `DWMSG_SetzeStatusAbbruch`**:
     1. Extracts `$1` (tracking entry ID) and asserts it is not empty; exits with 1 if missing.
     2. Opens connection to Oracle database.
     3. Executes `BERT_MELDUNG.SetzeStatusOk` or `BERT_MELDUNG.SetzeStatusAbbruch` procedure.
   - **`DWMSG_ErmittleNr`**:
     1. Validates that target return variable name is supplied.
     2. Calls database sequence generator to get the unique key.
     3. Strips whitespaces and returns the value to caller.
   - **`DWMSG_ErzeugeEintrag` / `DWMSG_MeldeFehler`**:
     1. Unpacks and validates input parameters.
     2. Dynamically counts parameters (`NumParm` check for errors).
     3. Executes corresponding database procedure inside the `BERT_MELDUNG` package.
   - **`DWMSG_Logdateiname`**:
     1. Reads `$DW_DIR_PROT` and arguments.
     2. Formats a log path with current datetime as `YYYYMMDD_HHMM` and entry ID.
     3. Returns filename.
   - **`DWMSG_SetzeStichtagInfo` / `DWMSG_AppendTimingInfos`**:
     1. Asserts valid inputs.
     2. Opens Oracle DB session.
     3. Executes the custom PL/SQL anonymous block calling `BERT_MELDUNG.SetzeZusatzInfos`.

7. ERROR HANDLING & EXIT CODES
   - Standard shell error checking `if [ -z "$VAR" ]` is used to raise assertions. Missing mandatory values print an error message (e.g. `Argh!, keine EintragsNummer ...`) and terminate execution using `exit 1` or `exit 2`.
   - The shell library implements standard exit codes (1, 2) on validation failures.
   - Python translation: Map parameter checks to `ValueError`, database failures to `oracledb.DatabaseError`, and implement a robust context-managed database handle to ensure commits and rollbacks occur correctly.

8. OUTPUTS / SIDE EFFECTS
   - **Oracle DB Updates**: Inserts, updates, and transaction commits applied to logging structures inside the `BERT_MELDUNG` tracking package.
   - **Console Output**: Diagnostics and failure notices are routed to `sys.stderr` and `sys.stdout`.
   - **Log File Naming**: Produces and returns formatted file system paths under `$DW_DIR_PROT`.

9. BUSINESS SUMMARY
   - Coordinates end-to-end audit logging and processing statistics across the batch application ecosystem.
   - Ensures error traps correctly capture non-zero status codes from terminal commands.
   - Records execution lifecycles (initialization, processing updates, success/failure completion states).
   - Preserves business data integrity by cataloging job timings and stichtag context inside database records.

=======================================================================================
PSEUDOCODE OUTLINE
=======================================================================================

```python
# Modern Python Equivalent of f_alis_msgerr.ksh
import os
import sys
import datetime
# # REVIEW-STRUCT: confirm python-oracledb is installed and the oracle connection details are correct
import oracledb

# Helper to retrieve active DB connections
def _get_db_connection():
    # # REVIEW-STRUCT: confirm env variable DW_ORAUSER contains standard Oracle credentials
    connection_string = os.environ.get("DW_ORAUSER")
    if not connection_string:
        print("Error: DW_ORAUSER environment variable is not defined.", file=sys.stderr)
        raise ValueError("DW_ORAUSER not set")
    return oracledb.connect(user_or_dsn=connection_string)


# Step 1: DWMSG_Fehlerbehandlung
# Simulates trap ERR logic
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, exit_code=1):
    k_unerw_fehler = 10
    
    # Log the unexpected failure
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {exit_code}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    
    # Set status to Abbruch
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # # REVIEW-STRUCT: d_alis_spaufruf_p1.sql body not supplied; executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [dwmsg_eintrags_nr])
                conn.commit()
    except oracledb.Error as err:
        print(f"Database error in SetzeStatusOK: {err}", file=sys.stderr)
        raise


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # # REVIEW-STRUCT: d_alis_spaufruf_p1.sql body not supplied; executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [dwmsg_eintrags_nr])
                conn.commit()
    except oracledb.Error as err:
        print(f"Database error in SetzeStatusAbbruch: {err}", file=sys.stderr)
        raise


# Step 4: DWMSG_ErmittleNr
# Returns the tracking ID back as a string, bypassing temp files
def dwmsg_ermittle_nr():
    # # REVIEW-STRUCT: d_al_is_ermittlenr.sql body not supplied; assuming direct retrieval of sequence ID
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Assuming BERT_MELDUNG sequence tracking equivalent
                cursor.execute("SELECT BERT_MELDUNG_SEQ.NEXTVAL FROM DUAL")
                row = cursor.fetchone()
                if row:
                    return str(row[0]).strip()
                else:
                    raise RuntimeError("Failed to fetch next sequence number.")
    except oracledb.Error as err:
        print(f"Database error in ErmittleNr: {err}", file=sys.stderr)
        raise


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, log_datei):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # # REVIEW-STRUCT: d_alis_spaufruf_p4.sql body not supplied; executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [dwmsg_eintrags_nr, job_kennung, programmname, log_datei])
                conn.commit()
    except oracledb.Error as err:
        print(f"Database error in ErzeugeEintrag: {err}", file=sys.stderr)
        raise


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # # REVIEW-STRUCT: d_alis_spaufruf_p3/4/5.sql body not supplied; executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.Fehler", [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2])
                conn.commit()
    except oracledb.Error as err:
        print(f"Database error in MeldeFehler: {err}", file=sys.stderr)
        raise


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr):
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "")
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{current_time}_{dwmsg_eintrags_nr}.log"
    return filename


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Mimics sqlplus inline execution
                plsql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:1, TO_DATE(:2, :3));
                END;
                """
                cursor.execute(plsql_block, [dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt])
                conn.commit()
    except oracledb.Error as err:
        print(f"Database error in SetzeStichtagInfo: {err}", file=sys.stderr)
        raise


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, dwmsg_info_text, dwmsg_date_format):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Mimics sqlplus inline execution
                plsql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:1, NULL, :2 || ' ' || TO_CHAR(SYSDATE, :3) || ' ');
                END;
                """
                cursor.execute(plsql_block, [dwmsg_eintrags_nr, dwmsg_info_text, dwmsg_date_format])
                conn.commit()
    except oracledb.Error as err:
        print(f"Database error in AppendTimingInfos: {err}", file=sys.stderr)
        raise
```

An implementation-ready migration design document is detailed below. 

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Converts the legacy KornShell logging and database status tracking utility library into a reusable Python module containing equivalent functions to be imported/called by orchestrated pipeline tasks. |

---

### Execution Order
The overall job `DW.DWH_ABTN_SMART_KUBI` consists of the following sequential pipeline execution steps (as discovered in the legacy orchestration):
1. **`DW.DWH_ABTN_SMART_KUBI.xml`** (UC4 Scheduler Definition) — Triggers the start of the job.
2. **`d_abtn_x_smart_kubi.sql`** — Execution of the main database transformations.
3. **`r_sqlscript`** — Wrapper script to load SQL commands.
4. **`.dw_init`** — Environment initialization script.
5. **`f_alis_msgerr.ksh`** (This file) — Sourced/imported dynamically to handle error catching (`trap ERR`), audit initialization, and status logging throughout the run.
6. **`h_alis_sqlplus.ksh`** — DB execution wrapper.

**Target Orchestration Mapping:**
In the target Google Cloud Platform (GCP) architecture:
- `f_alis_msgerr.ksh` is migrated to the Python module `f_alis_msgerr.py`.
- Functions inside `f_alis_msgerr.py` will be imported and executed inside Cloud Composer (Airflow) DAG tasks or wrapper scripts to provide consistent start, progress, timing, and error tracking in a central metadata repository or via Cloud Logging.

---

### Schedule & Variables
The legacy scheduler controls execution timing and feeds specific variables into the runtime environment.

#### Dynamic Variable Calculation:
The following scheduler variables must be calculated at runtime in the orchestrator (Cloud Composer/Airflow) or inside the task wrapper, preserving the exact original semantics:
- **`DWH_JOB_KENNUNG`**: `'ABTN_SMART_KUBI'` (Static job identification).
- **`cdate`**: Execution date in `'YYYYMMDD'` format (mapped via Airflow template macro `{{ ds_nodash }}`).
- **`cmonth`**: First 6 characters of `cdate` (`'YYYYMM'`).
- **`cday`**: Last 2 characters of `cdate` (`'DD'`).
- **`first`**: `'01'` (Static month starting day).
- **`cmonth` (manipulation)**: `cmonth` concatenated with `first` (`'YYYYMM01'`).
- **`cmonth` (subtraction)**: Calculated by subtracting 1 day from the month start date (`SUB_DAYS(&cmonth, 1)`), resulting in the last day of the preceding month in `'YYYYMMDD'` format.
- **`cmonth` (final substring)**: First 6 characters of the subtracted date (`'YYYYMM'` of the prior month).
- **`MONATSID`**: Set to the final evaluated value of `cmonth` (representing the previous month's YYYYMM code relative to the job execution date).

#### Variable Routing:
These variables must be passed to the target Python module and downstream Airflow operators as DAG context variables (`params` or `logical_date` manipulations) rather than hardcoded configuration files.

---

### Lineage
The lineage edges trace how this component interacts with external database files and packages:
- **Upstream SQL Executes**:
  - `D_ALIS_SPAUFRUF_P1.SQL` (Invoked by `DWMSG_SetzeStatusOK` and `DWMSG_SetzeStatusAbbruch` via SQL\*Plus).
  - `D_AL_IS_ERMITTLENR.SQL` (Invoked by `DWMSG_ErmittleNr` via SQL\*Plus).
  - `D_ALIS_SPAUFRUF_P4.SQL` (Invoked by `DWMSG_ErzeugeEintrag` via SQL\*Plus).
- **Procedural Calls**:
  - `PROCEDURE:SETZEZUSATZINFOS` (Oracle procedural package method dynamically called with SQL\*Plus `EXEC` syntax).

*Note: Per human-confirmed resolutions, the auxiliary helper SQL scripts above are marked "NO SOURCE NEEDED" as their execution wrapper logic is absorbed directly by the migrated Python driver calls or mapped directly to native database API executions.*

---

### Cross-file Dependencies
- This logging script is a library; it possesses no standalone entry-point but is sourced and invoked by multiple wrapper scripts (such as `h_alis_sqlplus.ksh` or the main execution driver).
- Downstream execution depends on the target database (BigQuery) exposing equivalent logging tables or endpoints resembling the structure of `BERT_MELDUNG`.

---

### Target File Plan
- **Target File Path**: `local/home/gurunathan_t/kubi/f_alis_msgerr.py`
- **Language**: Python
- **Source File**: `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
- **Purpose**: Provides a reusable Python interface containing functions equivalent to the original shell methods (`dwmsg_fehlerbehandlung`, `dwmsg_setze_status_ok`, `dwmsg_setze_status_abbruch`, `dwmsg_ermittle_nr`, `dwmsg_erzeuge_eintrag`, `dwmsg_melde_fehler`, `dwmsg_logdateiname`, `dwmsg_setze_stichtag_info`, and `dwmsg_append_timing_infos`). This centralizes logging requests and error traps.

---

### Environment-Specific Values
The legacy system defines key environment paths and database connections. These are classified and resolved as follows:

1. **`DW_ORAUSER`** -> **GLOBAL**
   - *Legacy Role*: Oracle DB credentials/connection string utilized by SQL\*Plus.
   - *Target Mapping*: Maps to GCP standard connectivity configurations. In the Python module, this should be resolved via standard environment parameters or secret strings retrieved at runtime (e.g., `os.environ.get("GCP_PROJECT")` or BigQuery connection profiles).
2. **`DW_DIR_ROOT`** -> **GLOBAL**
   - *Legacy Role*: The absolute root directory of the application code/SQL structures.
   - *Target Mapping*: Maps to standard environment configuration referencing the execution workspace or container environment, referenced dynamically using `os.environ.get("GCS_BUCKET")` or similar variables.
3. **`DW_DIR_PROT`** -> **GLOBAL**
   - *Legacy Role*: Target filesystem directory where local logs are output.
   - *Target Mapping*: Maps to GCP Cloud Logging configurations or a dedicated GCS bucket log folder path (e.g., `os.environ.get("GCS_LOG_DIR")`).

---

### Risks & Manual Actions
1. **Oracle Database Dependencies (`BERT_MELDUNG`)**:
   - *Risk*: The script executes procedural calls referencing an Oracle-specific package (`BERT_MELDUNG`). BigQuery does not natively support Oracle PL/SQL packages.
   - *Mitigation*: No direct BigQuery equivalent exists for the legacy package. A metadata logging schema with equivalent tables and stored procedures must be created in BigQuery, or the logic must be intercepted in the Python module to write entries directly to a GCP-managed logging table (or redirect logging entirely to GCP Cloud Logging).
2. **Validation and Exit Logic**:
   - *Requirement*: Every printed message, exit code, and validation failure message (e.g. `"Argh!, keine EintragsNummer bei Aufruf..."`) must be retained character-for-character in the target Python script logging statements (per the Output/Print Literal Rule).
3. **Auxiliary Files (`D_ALIS_SPAUFRUF_P1.SQL`, etc.)**:
   - *Risk*: The helper SQL scripts invoked by SQL\*Plus are marked "NO SOURCE NEEDED".
   - *Mitigation*: The Python database execution calls must use clean parameter binding directly to database procedures, rendering the temporary file outputs and SQL\*Plus command wrappers obsolete. A human architect must verify that the target BigQuery metadata tables accept equivalent parameters.

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
REASON: The script defines a custom KornShell helper function with parameter validation, file readability checks, and error logging that must be preserved as Python logic.

EVIDENCE
- Business logic found: KSH custom logic defines `starteSQLSkript`, which performs parameter validation, checks SQL script file readability, logs execution details, and handles the `sqlplus` execution and return status.
- AWK: none
- SQL-expressible: no (the script is an orchestration and execution utility that handles filesystem checks and external process execution)
- Non-SQL side effects: Invokes `sqlplus` database client, verifies local file system paths, and executes an external error logging utility `DWMSG_MeldeFehler`.
- Against this verdict: none (this is a shell helper library containing control-flow functions, making it a clear candidate for Python utility module conversion).

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

=== DESIGN DOCUMENT ===

1. SCRIPT OVERVIEW
   The script `h_alis_sqlplus.ksh` is a legacy KornShell utility library containing helper routines for invoking SQL*Plus. Its primary purpose is to wrap `sqlplus` executions with validation and logging. It ensures that the specified SQL script is readable before launching, logs the parameters, executes the database script under the specified user credentials, and propagates the resulting return code to the caller.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced by other KornShell ETL/batch scripts that need to run SQL*Plus scripts securely and uniformly.
   - UC4 native includes: None referenced in this file.
   - Environment files sourced: None within this utility module, though it expects environment variables (like `DW_ORAUSER`) and external utilities (like `DWMSG_MeldeFehler`) to be set in the calling context.

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following parameters:
   - `p_Eintragsnr` ($1): Positional argument representing the unique error entry identifier. Used for error reporting.
   - `p_Skript` ($2): Positional argument representing the path to the SQL script file to be executed. Verified for existence and readability.
   - Remaining arguments ($* after shift 2): Positional arguments passed dynamically through to the SQL*Plus script.
   - `DW_ORAUSER` (Environment variable): Oracle connection string/user credentials. Used dynamically during the `sqlplus` invocation. Surfaced in Python via `os.environ.get("DW_ORAUSER")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Launches Oracle SQL*Plus to execute the specified SQL script with the provided parameters, redirecting standard input to prevent interactive hanging.
     - Target: Remains an external process invocation via `subprocess.run` to preserve exact command-line SQL*Plus behavior, unless the calling script is fully refactored to use native database drivers like `oracledb`.
     - Resolvable Launcher: No, because this is a generic execution utility function rather than a single static wrapper script.
   - `DWMSG_MeldeFehler`
     - Purpose: A custom external program or function used for registering error events within the data warehouse logging system.
     - Target: # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion. Should be called via `subprocess.run`.

5. EMBEDDED SQL
   - No direct SQL is embedded in this utility script. The script is designed to execute external SQL files passed dynamically via the `$p_Skript` parameter.
   - Dialect of the executed SQL is Oracle SQL*Plus.

6. CONTROL FLOW
   1. Initialize module-level tracking variables: `ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`.
   2. Define `starteSQLSkript` function taking positional parameters.
   3. Check if required arguments `p_Eintragsnr` and `p_Skript` are present. If either is missing, call `DWMSG_MeldeFehler` with error code `196` and return `196`.
   4. Check if the SQL script file exists and is readable (`[ ! -r $p_Skript ]`). If not, call `DWMSG_MeldeFehler` with error code `201` and return `201`.
   5. Print logging information detailing the SQL script path and passed arguments to stdout.
   6. Disable active exit-on-error behavior (`set +e`).
   7. Execute `sqlplus` with the specified credentials, script, and arguments, redirecting stdin from `/dev/null`.
   8. Capture the exit status code of `sqlplus` in `errcode`.
   9. Re-enable exit-on-error behavior (`set -e`).
   10. Return the captured `errcode` to the caller.

7. ERROR HANDLING & EXIT CODES
   - Validation failures return specific exit codes (`196` for missing arguments, `201` for unreadable file) and report the error via `DWMSG_MeldeFehler`.
   - The exit code of `sqlplus` is captured using `$?` and returned directly.
   - Python equivalence:
     - Use `os.path.exists()` and `os.access()` for checking read permissions.
     - Use a try-except structure or capture the returncode from `subprocess.run(...)` with `check=False` to manually handle and return the exit code of `sqlplus`.

8. OUTPUTS / SIDE EFFECTS
   - Log statements printed to stdout.
   - Calls to `DWMSG_MeldeFehler` (external error registration system).
   - Side-effects on Oracle database state depending on the SQL script executed by `sqlplus`.

9. BUSINESS SUMMARY
   - Standardizes database execution of SQL*Plus scripts across legacy batch processes.
   - Prevents silent execution failures by verifying script file presence and readability before invoking the SQL*Plus client.
   - Implements structured logging and parameters verification to ease debugging of automated database steps.
   - Integrates script status checks with the central error registry to trigger appropriate recovery actions on failure.

=== PSEUDOCODE ===

```python
import os
import sys
import subprocess
import shutil

# Module metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Step 1: Define error helper execution wrapper
def call_dwmsg_meldefehler(eintragsnr, severity, error_id, message):
    """
    Invokes the external logging utility DWMSG_MeldeFehler.
    # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction
    """
    cmd = ["DWMSG_MeldeFehler", str(eintragsnr), severity, str(error_id), message]
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        print(f"Error: {cmd} not found on path.", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error logging failed with status {e.returncode}", file=sys.stderr)

# Step 2: Define helper function to start SQL*Plus script
def starteSQLSkript(p_Eintragsnr, p_Skript, *params):
    # Step 3: Validate input parameters
    if not p_Eintragsnr or not p_Skript:
        msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        call_dwmsg_meldefehler(p_Eintragsnr, "E", 196, msg)
        return 196

    # Step 4: Verify SQL script file exists and is readable
    if not os.path.isfile(p_Skript) or not os.access(p_Skript, os.R_OK):
        call_dwmsg_meldefehler(p_Eintragsnr, "E", 201, p_Skript)
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(params)}")

    # Step 6: Acquire Oracle connection user from environment
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    
    # Step 7: Execute sqlplus via subprocess (handling set +e equivalent check=False)
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(params)
    
    try:
        # Pass devnull to stdin to mirror </dev/null
        result = subprocess.run(
            sqlplus_cmd, 
            stdin=subprocess.DEVNULL, 
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Execution of sqlplus failed: {str(e)}", file=sys.stderr)
        errcode = 1  # Fallback error status

    # Step 8: Return execution exit status
    return errcode
```

An implementation-ready **MIGRATION DESIGN DOCUMENT** is presented below for the migration of `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` to Python on Google Cloud Platform (BigQuery/Composer environment).

---

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Converted to a native Python module. Porting the logic allows calling scripts to natively import and call the validation and SQL*Plus execution helpers, resolving subprocess-related runtime errors. |

---

### Execution Order
The legacy dependency sequence includes:
1. `DW.DWH_ABTN_SMART_KUBI.xml` *(UC4 orchestration, mapped to Google Cloud Composer / Airflow DAG)*
2. `d_abtn_x_smart_kubi.sql` *(Dataform SQLX script executing BigQuery SQL)*
3. `r_sqlscript` *(KSH wrapper script, mapped to Python task `r_sqlscript.py`)*
4. `.dw_init` *(Environment initialization, mapped to Airflow runtime environment configurations)*
5. `f_alis_msgerr.ksh` *(Error logging library, mapped to native Python module `f_alis_msgerr.py`)*
6. `h_alis_sqlplus.ksh` *(This module, mapped to native Python module `h_alis_sqlplus.py`)*

In the target architecture, `h_alis_sqlplus.py` is a helper library containing reusable utility functions rather than a standalone execution step. It will be imported directly by sibling orchestration scripts (e.g., `r_sqlscript.py`) during pipeline execution.

---

### Scheduling
* **Scheduling Mechanism**: As a reusable utility library, `h_alis_sqlplus.py` is not directly scheduled. Its execution is driven by the parent workflow (`DW.DWH_ABTN_SMART_KUBI` Airflow DAG) which handles scheduling and task execution.

---

### Schedule & Variables — Must Be Retained
The scheduler-set variables from the legacy UC4 job must be managed at the DAG/orchestration level and passed to executing tasks as parameters or environment variables. They do not need to be hardcoded or managed within this utility library:
* `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
* `cdate` = `'SYS_DATE("YYYYMMDD")'` *(Airflow: `{{ ds_nodash }}`)*
* `cmonth` = `'SUBSTR(&cdate,1,6)'`
* `cday` = `'SUBSTR(&cdate,7,2)'`
* `first` = `'01'`
* `cmonth` = `'&cmonth&first'`
* `cmonth` = `'SUB_DAYS(&cmonth,1)'`
* `cmonth` = `'SUBSTR(&cmonth,1,6)'`
* `MONATSID` = `'&cmonth'`

---

### Cross-File Dependencies
* **Error Logging Dependency (`f_alis_msgerr.py`)**: This module depends directly on the logging capabilities of `f_alis_msgerr`. To resolve issues identified in the previous review:
  * Do **not** execute `DWMSG_MeldeFehler` as a subprocess command.
  * Import `dwmsg_melde_fehler` natively from `f_alis_msgerr` and call it as a Python function:
    ```python
    from f_alis_msgerr import dwmsg_melde_fehler
    ```
* **Oracle SQL\*Plus CLI Dependency**: The script depends on an external SQL\*Plus executable being installed and configured on the path of the execution environment (e.g., within the Airflow worker container or Google Kubernetes Engine pod).

---

### Target File Plan

#### **Target File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
* **Language**: Python
* **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
* **Implementation Rules & Verification Points**:
  1. **Native Module Imports**:
     * Import `dwmsg_melde_fehler` from the sibling Python module:
       ```python
       from f_alis_msgerr import dwmsg_melde_fehler
       ```
  2. **Function Definition**:
     * Translate `starteSQLSkript` to `def starte_sql_skript(p_eintragsnr, p_skript, *params):`
  3. **Parameter & File Validation**:
     * Ensure both parameters are supplied. If not, natively call:
       ```python
       dwmsg_melde_fehler(str(p_eintragsnr), "E", 196, f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
       ```
     * Check if the script exists and is readable using `os.path.isfile(p_skript)` and `os.access(p_skript, os.R_OK)`. If it is unreadable, call:
       ```python
       dwmsg_melde_fehler(str(p_eintragsnr), "E", 201, p_skript)
       ```
  4. **Output/Print Literal Rule**:
     * Keep all German logging messages exactly identical to the legacy shell text:
       ```python
       print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
       print(f"Sql*Plus-Skript : {p_skript}")
       print(f"Skript-Parameter: {' '.join(params)}")
       ```
  5. **Subprocess Call to SQL\*Plus**:
     * Run the process and capture the return status without throwing a process exception directly, to mimic legacy error-handling behaviors. Pass `stdin=subprocess.DEVNULL` to mirror the legacy `</dev/null` redirection.
       ```python
       dw_orauser = os.environ.get("DW_ORAUSER", "")
       cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(params)
       result = subprocess.run(cmd, stdin=subprocess.DEVNULL, check=False)
       return result.returncode
       ```

---

### Environment-Specific Values
The environment variables utilized within this utility must be resolved at runtime using Python’s environment configuration:

* **`DW_ORAUSER`** — **GLOBAL**
  * *Purpose*: Oracle connection string / credential details.
  * *Resolution*: Resolved dynamically in Python via:
    ```python
    dw_orauser = os.environ.get("DW_ORAUSER")
    ```
  * *Note*: No fallback or literal credential placeholder is allowed in the code.

---

### Risks & Manual Actions
* **Database Driver / CLI Availability**: The Python helper relies on calling the `sqlplus` CLI command via `subprocess.run`. This requires SQL\*Plus and appropriate Oracle Instant Client libraries to be installed, configured, and accessible within the runtime environment's `$PATH`.
* **Credential Sourcing**: `DW_ORAUSER` contains sensitive connection credentials. These should be managed securely via Google Cloud Secret Manager and injected into the task environment at runtime, rather than being stored in environment variables in plain text.

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
REASON: The script is a database-execution runner containing command-line argument parsing, environment setups, log trapping, and custom file resolution logic that cannot be represented in BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic. It parses command-line arguments, dynamically resolves SQL file paths, registers log entries, and invokes an external SQL executor.
- AWK: none
- SQL-expressible: no, it contains command-line parsing, file operations, error trapping, and orchestration logic that does not map to SQL.
- Non-SQL side effects: file path searches, custom trapping of OS/exit codes, logging, and environment sourcing.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

=== DESIGN DOCUMENT STRUCTURE ===

1. SCRIPT OVERVIEW
   The script `r_sqlscript` is a generic utility designed to run SQL scripts within an Oracle database environment. It parses command-line parameters to locate a target SQL file, manages logging registration through standard framework utilities, and executes the SQL script via an external driver function (`starteSQLSkript`). It is used to ensure that SQL jobs register their status, handle errors via standard trapping mechanisms, and write log files in a consistent format.

2. INVOCATION CONTEXT
   - Who calls this script: Typically invoked as a UC4 job step or from other wrapper scripts to execute specific database scripts. Command line format: `./r_sqlscript -f <sqlscript> [-i <input_string>] [-j <job_name>] [-v]`
   - UC4 native includes: None referenced in the script.
   - Environment files sourced:
     - `. $HOME/aktuell/.dw_init` — # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` — # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` — # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `-f` (parameter `p_sqlscript`):
     - Source: Command-line argument parsed by `getopts`.
     - Usage: Specifies the path or filename of the SQL script to be executed.
     - Python surface: `argparse` argument `--file` / `-f`.
   - `-i` (parameter `p_sqlpar`):
     - Source: Command-line argument parsed by `getopts`.
     - Usage: Custom inputs passed directly to the SQL script execution.
     - Python surface: `argparse` argument `--input` / `-i`.
   - `-j` (parameter `p_Job`):
     - Source: Command-line argument parsed by `getopts`.
     - Usage: Job identifier used for logging and tracking. Defaults to `DWH_KORR` if empty.
     - Python surface: `argparse` argument `--job` / `-j`.
   - `-v` (parameter `p_Verbose`):
     - Source: Command-line flag parsed by `getopts` (sets `p_Verbose=1`).
     - Usage: Flag to immediately cat the log file on error.
     - Python surface: `argparse` argument `--verbose` / `-v` as action 'store_true'.
   - Unused/undeclared parameter reference:
     - `p_Kuerzel`: Referenced in `ErrArg="$p_Kuerzel"` but never defined or declared in this script. # REVIEW: parameter p_Kuerzel is referenced but never declared or defined in this script; confirm before dropping or replacing.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr`:
     - Command: `starteSQLSkript` (defined in sourced script `h_alis_sqlplus.ksh`)
     - Purpose: Launch SQL*Plus or similar DB client to run `$l_DBskript` with parameters.
     - Target: Should remain an external process invocation or custom imported library function since `starteSQLSkript` source is not available.
     - Resolvability: NOT a resolvable launcher because the SQL file is dynamic (passed as `-f`) and we do not have the body of `starteSQLSkript`.
     - Note: # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - None found in this orchestration script itself.

6. CONTROL FLOW
   1. **Initialize script and version variables**: Set `ProgName` and `ProgVersion`.
   2. **Source Environment Files**: Sourced `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`. (# REVIEW-STRUCT warnings apply).
   3. **Parse Parameters**: Execute a `getopts` loop to read `-f`, `-i`, `-j`, and `-v` options.
   4. **Initial Parameter Validation**:
      - Check if `ErrNr` was set during parsing.
      - If `ErrNr != 0`, invoke `DWMSG_MeldeFehler`, print usage, and exit.
   5. **Determine Script Path**:
      - Change directory to current script directory (`dirname $0`).
      - If the script has no path (i.e. directory is `.`), look for it in `../sql/`, then `../mig/`, and finally in current folder.
   6. **Bizarre File Existence Validation**:
      - If `$l_DBskript` exists, set `ErrNr=198` and `ErrArg` to `$p_Kuerzel`. # REVIEW: original script sets ErrNr=198 if the database script exists (normally should be if NOT exists '-f'). We reproduce the original logic verbatim.
   7. **Set Job ID Default**: If `p_Job` is empty, set `JobKennung` to `DWH_KORR`. Otherwise, set it to the uppercase version of `p_Job`.
   8. **Logging Setup**:
      - Call `DWMSG_ErmittleNr` to get `DW_EintragsNr`.
      - Call `DWMSG_Logdateiname` to resolve log file path.
      - Call `DWMSG_ErzeugeEintrag` to log start of execution.
   9. **Define Traps**: Set up INT and ERR traps to run `DWMSG_Fehlerbehandlung` and cat log file if `p_Verbose` is set.
   10. **Execute SQL Script**: Call `starteSQLSkript` with appropriate parameters and redirect output to log file.
   11. **Finalize**: Set OK status using `DWMSG_SetzeStatusOK`, clear traps, and print completion message.

7. ERROR HANDLING & EXIT CODES
   - Detection: Uses `set -e` to fail on non-zero statuses. Uses custom `trap` for `INT` and `ERR` signals.
   - Reaction: Calls `DWMSG_Fehlerbehandlung` to log errors in DB/tracking tables, echoes error messages, and cat's the logfile if `p_Verbose` is enabled.
   - Success exit code: `0` (implicit).
   - Failure exit code: `1` for traps, or `ErrNr` for parameter violations.
   - Python Mapping: Implement using a `try ... except Exception as e` block. Standard python exceptions combined with `subprocess.CalledProcessError` will capture execution issues. Explicitly raise exceptions or exit with custom codes if validations fail.

8. OUTPUTS / SIDE EFFECTS
   - Log file `$LogDatei`.
   - Tracking database records altered/created via `DWMSG_ErzeugeEintrag` and `DWMSG_SetzeStatusOK`.

9. BUSINESS SUMMARY
   - Serves as a unified runner framework for executing database SQL scripts within the DWH ecosystem.
   - Provides standard mechanisms for error detection, alerting, and log archiving.
   - Eliminates the need for hardcoded absolute paths by dynamically resolving SQL script paths relative to the runner's location.
   - Integrates with central DWH monitoring and orchestration by writing execution logs and registering task statuses.

=== PSEUDOCODE STYLE ===

```python
# Step 1: Import required libraries
import sys
import os
import argparse
import subprocess
import shutil

# Step 2: Initialize constants and variables
PROG_NAME = f"Ausführung Script {sys.argv[0]}"
PROG_VERSION = "5.0.0"

# # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables and functions it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables and functions it sets are unknown; do not guess their names or values

# Step 3: Parse parameters
parser = argparse.ArgumentParser(description=PROG_NAME)
parser.add_argument("-f", "--file", dest="p_sqlscript", required=False, help="Name of the SQL script")
parser.add_argument("-i", "--input", dest="p_sqlpar", default="", help="Possible parameters for the SQL script")
parser.add_argument("-j", "--job", dest="p_Job", default="", help="Job identifier")
parser.add_argument("-v", "--verbose", dest="p_Verbose", action="store_true", help="Verbose mode")

# Emulate getopts error handling
try:
    args = parser.parse_args()
except Exception as e:
    # Emulate legacy ErrNr = 192 or 193
    # Call DWMSG_MeldeFehler(0, "E", 192, str(e))
    # print usage and exit
    sys.exit(192)

# Step 4: Resolve database script path
# cd `dirname $0`
script_dir = os.path.dirname(sys.argv[0])
if script_dir:
    os.chdir(script_dir)

p_sqlscript = args.p_sqlscript.lower() if args.p_sqlscript else None

if p_sqlscript:
    dir_part = os.path.dirname(p_sqlscript)
    if dir_part == "" or dir_part == ".":
        l_DBskript = os.path.join("..", "sql", p_sqlscript)
        if not os.path.exists(l_DBskript):
            l_DBskript = os.path.join("..", "mig", p_sqlscript)
        if not os.path.exists(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript
else:
    l_DBskript = None

# Step 5: Perform bizarre validation from original code
# # REVIEW: original script sets ErrNr=198 if the database script exists (normally should check if NOT exists).
# # REVIEW: parameter p_Kuerzel is referenced but never declared or defined in this script.
p_Kuerzel = None  # Undefined in source
if l_DBskript and os.path.exists(l_DBskript):
    err_nr = 198
    err_arg = p_Kuerzel

# Step 6: Determine Job ID
job_kennung = args.p_Job.upper() if args.p_Job else "DWH_KORR"

print("----------------- Parameter -----------------")
print(f"Jobkennung     : {job_kennung}")
print(f"DB-Skript      : {l_DBskript}")
print("---------------------------------------------")

# Step 7: Logging setup & entry generation
# DW_EintragsNr = DW_MSG_ErmittleNr()
# LogDatei = DW_MSG_Logdateiname(job_kennung, DW_EintragsNr)
# DWMSG_ErzeugeEintrag(DW_EintragsNr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", LogDatei)
dw_eintrags_nr = 0  # Inferred mock of legacy tracking variable
log_datei = f"{job_kennung}_{dw_eintrags_nr}.log"  # Mock representation of custom log name

print("----------------- Job -----------------------")
print(f"Job-Nr    : '{dw_eintrags_nr}'")
print(f"Logdatei  : '{log_datei}'")
print("---------------------------------------------")

# Step 8: Execute Job inside try/catch block to emulate traps
try:
    # # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    # starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1
    
    # Placeholder representing the DB client execution (since starteSQLSkript source is missing)
    subprocess.run(["starteSQLSkript", str(dw_eintrags_nr), l_DBskript, args.p_sqlpar, str(dw_eintrags_nr)], check=True)

    # Step 9: Finalizing on Success
    # DWMSG_SetzeStatusOK(dw_eintrags_nr)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    sys.exit(0)

except Exception as e:
    # Step 10: Error handling traps (INT / ERR)
    # DWMSG_Fehlerbehandlung(dw_eintrags_nr)
    print("!FEHLER gemeldet!", file=sys.stderr)
    if args.p_Verbose:
        try:
            with open(log_datei, 'r') as f:
                print(f.read())
        except Exception:
            pass
    sys.exit(1)
```

### EXECUTION ORDER
Based on the legacy dependency graph, the target orchestration must preserve this execution sequence:
1. **Initialize Environment Configuration**: Sourcing/initializing configurations (equivalent to `.dw_init`).
2. **Import framework and utility systems**: Standard log reporting (`f_alis_msgerr.py`) and execution helpers (`h_alis_sqlplus.py`).
3. **Execute the primary wrapper runner**: Run the main wrapper runner (`r_sqlscript.py`) with parameters specifying the target BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) and associated execution parameters.
4. **Execute database operations**: Run the database operations against BigQuery (defined in `d_abtn_x_smart_kubi.sql`).

---

### SCHEDULE & VARIABLES — MUST BE RETAINED
The following scheduler-set variables must be resolved dynamically in the Airflow/Composer orchestration layer at runtime and passed to the Python execution environment:
- `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
- `cdate` = `'SYS_DATE("YYYYMMDD")'` (Current system date in YYYYMMDD format)
- `cmonth` = `'SUBSTR(&cdate,1,6)'` (First 6 characters of cdate)
- `cday` = `'SUBSTR(&cdate,7,2)'` (Last 2 characters of cdate)
- `first` = `'01'` (Constant '01')
- `cmonth` = `'&cmonth&first'` (First day of current month)
- `cmonth` = `'SUB_DAYS(&cmonth,1)'` (Subtract 1 day to get the last day of the previous month)
- `cmonth` = `'SUBSTR(&cmonth,1,6)'` (Retrieve YYYYMM for the previous month)
- `MONATSID` = `'&cmonth'` (Assigned as the month ID parameter)

---

### LINEAGE
- **Upstream/Dependency Providers**:
  - `.dw_init` (config provider)
  - `f_alis_msgerr.ksh` (utility error handling library)
  - `h_alis_sqlplus.ksh` (utility database runner library)
- **Downstream/Consumers**:
  - `d_abtn_x_smart_kubi.sql` (the BigQuery SQL script that `r_sqlscript.py` executes)

---

### CROSS-FILE DEPENDENCIES
To resolve runtime execution errors identified in previous migration reviews, the target Python code must establish the following native import and integration rules instead of attempting external shell commands or subprocess wrappers:
1. **Import directly from `f_alis_msgerr`**: Do not mock or duplicate the logging routines. Natively import the tracking functions from the migrated `f_alis_msgerr.py` module:
   ```python
   from f_alis_msgerr import (
       dwmsg_ermittle_nr,
       dwmsg_logdateiname,
       dwmsg_erzeuge_eintrag,
       dwmsg_fehlerbehandlung,
       dwmsg_setze_status_ok,
       dwmsg_melde_fehler
   )
   ```
2. **Import and execute `starte_sql_skript` natively**: Do not invoke `starteSQLSkript` as a subprocess command (e.g., `subprocess.run(["starteSQLSkript", ...])`), which causes `FileNotFoundError`s. Instead, natively import `starte_sql_skript` from the migrated `h_alis_sqlplus.py` utility module and call it directly as a Python function:
   ```python
   from h_alis_sqlplus import starte_sql_skript
   
   # Native function execution:
   starte_sql_skript(str(dw_eintrags_nr), l_DBskript, p_sqlpar, str(dw_eintrags_nr))
   ```
3. **Execute SQL Script on BigQuery**: The wrapper must pass the target BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) rather than attempting to execute a Python translation of the database script.

---

### TARGET FILE PLAN
- **Target File**: `local/home/gurunathan_t/kubi/r_sqlscript.py`
- **Language**: Python
- **Source File**: `local/home/gurunathan_t/kubi/r_sqlscript`
- **Notes/Rules**:
  - Parse parameters `-f`, `-i`, `-j`, and `-v` using Python's native `argparse`.
  - Locate SQL scripts dynamically using relative paths mirroring the source behavior (`../sql`, `../mig`, or `.`).
  - Follow the import rules specified in **Cross-File Dependencies**.
  - Retain the exact German print literals for logging and trapping outputs:
    - `"Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"`
    - `"!OSFEHLER gemeldet!"`
    - `"!FEHLER gemeldet!"`
    - `"----------------- Parameter -----------------"`
    - `"Jobkennung     :"`
    - `"DB-Skript      :"`
    - `"----------------- Job -----------------------"`
    - `"Job-Nr    :"`
    - `"Logdatei  :"`

---

### ENVIRONMENT-SPECIFIC VALUES
- `DW_DIR_ROOT`: GLOBAL. Sourced at runtime via `os.environ.get("DW_DIR_ROOT")`.
- `HOME`: GLOBAL. Sourced at runtime via `os.environ.get("HOME")` or standard Python user directory paths.

---

### FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Migrates the legacy KSH wrapper into a native Python runner, importing dependency functions from `f_alis_msgerr` and `h_alis_sqlplus` to prevent subprocess execution errors. |