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
The workflow consists of a single standalone UNIX job `DW.DWH_ABTN_SMART_KUBI` designed to populate a temporary database table by executing a SQL script (`d_abtn_x_smart_kubi.sql`). Before executing the script, it calculates a reporting month identifier (`MONATSID`) based on the execution day of the month: if the current day is before the 15th, it targets the previous month; otherwise, it targets the current month. This process is currently externally triggered since there is no native calendar schedule or parent workflow defined within the provided extraction.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 | Populate temp table |

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` (time event) or calendar scheduler object is present in this extraction bundle. 
* **Trigger Source**: This workflow is triggered externally; the source cannot be determined from this extraction alone.
* **Airflow Implementation**: The Airflow DAG will be configured with `schedule=None`.

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `calculate_monatsid` | N/A (Helper) | `PythonOperator` | N/A | Calculated dynamically based on logical execution date | 0 | N/A | None | None | False | None | Custom logic porting the UC4 script-based date calculation. Passes result via XCom. |
| `dwh_abtn_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `BashOperator` | `gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py` | `--monatsid {{ task_instance.xcom_pull(task_ids='calculate_monatsid') }}` | 1 | 5 min | None | None | False | None | Downloads target Python script from GCS and executes it with the computed `MONATSID`. |

## 6. Task Dependency Map
```python
calculate_monatsid >> dwh_abtn_smart_kubi
```

## 7. Sync / Concurrency Analysis
* No UC4 sync/lock mechanisms were present on this object.
* `max_active_runs=1` is sufficient to prevent concurrent race conditions on the target temporary tables.

## 8. Error Handling and Retry Strategy
* Standard tasks will default to Airflow's native retry behavior (1 retry with a 5-minute delay).
* There are no complex postconditions or custom failure callback objects requested in the original extraction.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Implementation |
| :--- | :--- | :--- |
| `&cdate` | `SYS_DATE("YYYYMMDD")` | Handled via Python standard library execution date parsing |
| `&MONATSID` | UC4 date-subtraction logic | Passed dynamically via `calculate_monatsid` XCom output |
| `job_arg` | `'ABTN_SMART_KUBI'` | Passed as hardcoded arg or config mapping inside the converted Python script |
| `sql_path` | `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql` | `gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py` |

## 10. Developer Notes
* **GCP Placeholders**: Set the appropriate GCS bucket name (`gs://YOUR_BUCKET_NAME`) in the Airflow environment configuration or variables.
* **SQL execution path**: The native UC4 UNIX job called a SQL script using a wrapper utility `r_sqlscript`. The migration target converts this SQL script to run via a converted Python script (`d_abtn_x_smart_kubi.py`) executing inside a standard Cloud SQL / BigQuery wrapper environment.
* **Date logic**: The date logic relies on the logical execution time of the Airflow run, ensuring idempotency.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW: Define target GCS bucket containing transformed Python scripts
GCS_BUCKET = "YOUR_BUCKET_NAME"
SCRIPT_PATH = f"gs://{GCS_BUCKET}/python_scripts/d_abtn_x_smart_kubi.py"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Date Calculation Helper Function ─────────────────────
def get_monatsid(logical_date, **context):
    """
    Translates the original UC4 logic:
    :set &cdate = SYS_DATE("YYYYMMDD")
    :if &cday < '15'
    :  subtract 1 month
    :set &MONATSID = &cmonth
    """
    execution_date = logical_date
    day = execution_date.day
    
    if day < 15:
        # Subtract one month by finding the last day of the previous month
        first_day_current_month = execution_date.replace(day=1)
        last_day_prev_month = first_day_current_month - timedelta(days=1)
        monatsid = last_day_prev_month.strftime("%Y%m")
    else:
        monatsid = execution_date.strftime("%Y%m")
        
    print(f"Calculated MONATSID: {monatsid}")
    return monatsid

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Converted from UC4",
    schedule=None,  # No schedule defined in extraction, triggered externally
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # ── Task: calculate_monatsid ─────────────────────────
    calculate_monatsid_task = PythonOperator(
        task_id="calculate_monatsid",
        python_callable=get_monatsid,
        op_kwargs={"logical_date": "{{ logical_date }}"},
    )

    # ── Task: dwh_abtn_smart_kubi ────────────────────────
    dwh_abtn_smart_kubi_task = BashOperator(
        task_id="dwh_abtn_smart_kubi",
        bash_command=(
            f"gsutil cp {SCRIPT_PATH} /tmp/d_abtn_x_smart_kubi.py && "
            "python3 /tmp/d_abtn_x_smart_kubi.py "
            "--job_arg ABTN_SMART_KUBI "
            "--monatsid {{ task_instance.xcom_pull(task_ids='calculate_monatsid') }}"
        ),
    )

    # ── Dependencies ─────────────────────────────────────────
    calculate_monatsid_task >> dwh_abtn_smart_kubi_task
```

### Job dependencies
- **Upstream Jobs**: 
  - There are no direct upstream parent jobs listed in the legacy scheduling or dependency metadata. This job is executed as a standalone workflow triggered externally.
- **Downstream Jobs**: 
  - There are no downstream jobs directly triggered by this workflow in the provided context.

### Execution order
The target Airflow DAG orchestration must strictly preserve the execution order of the legacy steps:
1. **DAG Initialization & Environment Setup**: Replaces step 4 (`.dw_init`) and step 1 (`DW.DWH_ABTN_SMART_KUBI.xml`). Sets up Airflow-level configurations, project configurations, and logging.
2. **Date Variable Calculation**: Native Python step that computes the `MONATSID` date parameter dynamically, mirroring the UC4 date logic.
3. **Execution of the Wrapper Script**: Replaces step 3 (`r_sqlscript`) and step 2 (`d_abtn_x_smart_kubi.sql`). The DAG invokes the migrated Python wrapper script `r_sqlscript.py` passing `-j ABTN_SMART_KUBI`, `-f d_abtn_x_smart_kubi.sql`, and `-i <computed_monatsid>`.
4. **Error Handling & Status Tracking**: Replaces steps 5 and 6 (`f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh`). Uses native Airflow task state transitions and logging to record run states instead of invoking old shell wrappers.

### Scheduling
- **Trigger Source**: The legacy job is executed without a calendar schedule or time event in UC4.
- **Airflow Implementation**: The Airflow DAG will be configured with `schedule=None` (manual or external trigger only).

### Schedule & variables
The DAG must dynamically compute and map the following scheduler-set variables:
- **`DWH_JOB_KENNUNG`**: Hardcoded to `'ABTN_SMART_KUBI'`. Passed directly to the wrapper command as `-j ABTN_SMART_KUBI`.
- **`MONATSID`**: Dynamically computed inside the DAG via a Python task using the DAG's logical execution date (`logical_date`):
  - Let `execution_date` be the reference date.
  - If `execution_date.day < 15`:
    - `MONATSID` = The year and month (`YYYYMM`) of the last day of the *previous* month.
  - Else:
    - `MONATSID` = The year and month (`YYYYMM`) of the *current* month.
  - The calculated value is logged and passed to `r_sqlscript.py` as `-i <monatsid>`.

### Lineage
- **Upstream / Included Scripts**:
  - `DW.HOLE_PFAD` and `DW.LESE_LOG`: Confirmed as **NO SOURCE NEEDED (Retired)** by human review. The legacy directory path helper and log reader are retired in favor of native Airflow execution configurations and Cloud Logging.
  - `.dw_init`: Retired. Environment initialization is handled via standard Airflow Variables and environment configurations.
- **Invoked Components**:
  - `r_sqlscript`: Invoked by the UC4 script. Maps to the migrated python utility `r_sqlscript.py` (handled in a separate design pass).
  - `d_abtn_x_smart_kubi.sql`: Invoked via `r_sqlscript`. Maps to the migrated BigQuery SQL script (handled in a separate design pass).
- **Target Host / Login**:
  - Legacy environment host is `|DWHDWH1P|HOST` using login `DW.UNIX.ISTNS`. In GCP, this is mapped to Google Cloud Composer running under a designated GCP service account.

### Cross-file dependencies
- **Execution Wrapper**: The DAG is structurally dependent on the migrated `r_sqlscript.py` utility script, which must be packaged and available in the target environment (e.g., imported via a shared Python utilities library in the Composer environment).
- **SQL Execution Target**: The DAG is dependent on the presence of the migrated BigQuery SQL script `d_abtn_x_smart_kubi.sql` in the directory path where `r_sqlscript.py` looks for SQL files.

### Target file plan
- **Target File Path**: `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py`
  - **Language**: Python (Airflow DAG)
  - **Source File**: `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`
  - **Description**: Apache Airflow DAG representing the orchestration logic. It performs the dynamic date subtraction logic in Python to compute `MONATSID`, prints a log line matching the exact legacy German syntax (`"Berichtsmonat:  <MONATSID>"`), and invokes the wrapper script `r_sqlscript.py` with arguments `-j ABTN_SMART_KUBI -f d_abtn_x_smart_kubi.sql -i <monatsid>`. It avoids bypassing the wrapper or running the SQL directly through an invented python translation, as dictated by architectural guidelines.

### Environment-specific values
1. **GLOBAL (Environment-wide)**:
   - **`GCP_PROJECT`**: The target Google Cloud Project. Sourced at runtime via `os.environ.get("GCP_PROJECT")`.
   - **`GCS_BUCKET`**: The Composer bucket storing pipeline code. Sourced at runtime via `Variable.get("GCS_BUCKET")`.
   - **`GCP_CONN_ID`**: The Airflow connection ID used for BigQuery. Sourced at runtime via `Variable.get("GCP_CONN_ID")`.
   - **`R_SQLSCRIPT_PATH`**: The system path or package name of the migrated `r_sqlscript.py` wrapper. Sourced at runtime via `Variable.get("R_SQLSCRIPT_PATH")` or directly imported via standard Python pathing (`from common_utils import r_sqlscript`).
2. **JOB-SPECIFIC**:
   - **`job_arg` (`-j`)**: `'ABTN_SMART_KUBI'` (The job code parameter). Inline literal value.
   - **`sql_path` (`-f`)**: `'d_abtn_x_smart_kubi.sql'` (The name of the target SQL script). Inline literal value.

---

### File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Converted from UC4 job XML to an Airflow DAG. Calculates the `MONATSID` variable dynamically and executes the migrated `r_sqlscript.py` wrapper script with arguments `-j ABTN_SMART_KUBI -f d_abtn_x_smart_kubi.sql -i <monatsid>`. |

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
VERDICT: PYTHON
REASON: The script contains conditional logic to determine ORACLE_HOME and sources other external environment files, requiring programmatic environment management in Python.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

=== DESIGN DOCUMENT ===

1. SCRIPT OVERVIEW
   This script (`.dw_init`) acts as the central environment initialization configuration for the "Information Services" data warehouse system. It defines directory structures for root, protocols/logs, data cubes, and numerous system-specific import/export interfaces (such as SAP, Siemens, SMS, etc.). It dynamically detects and sets the `ORACLE_HOME` variable depending on the paths available on the filesystem, and imports additional global and local configuration scripts.

2. INVOCATION CONTEXT
   - Sourced interactively or by other KornShell scripts (e.g., during job initialization or cron execution) to set up the execution environment.
   - UC4 Job context: Not directly specified, but typically sourced as part of the initial shell startup environment or script pre-execution phase.
   - UC4 includes: None.
   - Environment files sourced:
     - `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
     - `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   This script does not accept positional command-line parameters. It relies on and manipulates the following environment variables:
   - `HOME` (Source: OS Environment) — Used as the base path for almost all directory definitions. Used: Yes.
   - `ORACLE_HOME` (Source: OS Environment / Fallback detection) — Used to locate Oracle binaries. If already set, it is preserved. If unset, the script attempts to discover it. Used: Yes.
   - `ORACLE_SID` (Source: OS Environment) — Used to construct the Oracle directory path `DW_DIR_UTL_FILE`. Used: Yes.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   No external databases or compiled binaries are invoked directly. The script performs:
   - Directory checks (`[ -d /path ]`) to locate Oracle installations.
   - Sourcing of `.dw_global` and `.dw_lokal`.

5. EMBEDDED SQL
   There is no embedded SQL or database interaction in this initialization script.

6. CONTROL FLOW
   1. Define and export base directories using the current user's `$HOME` directory.
   2. Define and export interfaces-specific input/output directories (`DW_DIR_IMP_*` and `DW_DIR_EXP_*`).
   3. Check if `ORACLE_HOME` is already set. If not, proceed to check directory pathways:
      - If `/appl/local/oracle/12.2.0.1.0` exists, set `ORACLE_HOME` to this path.
      - Else, if `/appl/local/oracle/11.2.0` exists, set `ORACLE_HOME` to this path.
      - Else, print an error message indicating `ORACLE_HOME` could not be set.
   4. Export `ORACLE_HOME`.
   5. Source `$HOME/.dw_global`.
   6. Source `$HOME/.dw_lokal`.
   7. Define and export `DW_DIR_UTL_FILE` path using the resolved or existing `$ORACLE_SID` value.

7. ERROR HANDLING & EXIT CODES
   - If directory detection for `ORACLE_HOME` fails, the script outputs a warning message to standard output but does not exit or raise a non-zero termination code.
   - In Python, this behavior should be mapped to logging a warning/error message or throwing a non-fatal `UserWarning`/`RuntimeError` depending on downstream dependency strictness.

8. OUTPUTS / SIDE EFFECTS
   - Side effects: Populates and exports a large array of directory and configuration-related environment variables.

9. BUSINESS SUMMARY
   - Coordinates paths for the complete Information Services filesystem environment.
   - Automatically handles Oracle environment resolution for both Oracle 11g and Oracle 12c directory profiles.
   - Integrates environment variables with user-specific and system-wide overrides via `.dw_global` and `.dw_lokal`.

=== PSEUDOCODE ===

```python
# Step 1: Import necessary modules
import os
import sys

def initialize_environment():
    # Step 2: Establish home directory base
    home = os.environ.get("HOME", "")
    if not home:
        print("Warning: HOME environment variable is not set.", file=sys.stderr)

    # Step 3: Define directory configurations
    os.environ["DW_DIR_ROOT"] = os.path.join(home, "aktuell")
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
    os.environ["DW_DIR_IMP_SAP_L"] = os.path.join(home, "daten/sap/sap_l_gutgr")
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

    os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

    # Step 4: Resolve ORACLE_HOME if empty
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:")
            print("   Konnte ORACLE_HOME nicht setzen !")

    # Step 5: Sourcing of external configs (Simulated)
    # # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
    # # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

    # Step 6: Define and export DW_DIR_UTL_FILE
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
```

### Execution order
The target orchestration (via Cloud Composer / Apache Airflow) must preserve the execution order from the legacy dependency graph:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Airflow DAG orchestrator definition)
2. **d_abtn_x_smart_kubi.sql** (Dataform SQLX or BigQuery SQL translation task)
3. **r_sqlscript** (Orchestration wrapper task calling SQL execution)
4. **.dw_init** -> `local/home/gurunathan_t/kubi/dw_init.py` (Environment variable and directory mapping initialization)
5. **f_alis_msgerr.ksh** (Error trapping and execution logging utility)
6. **h_alis_sqlplus.ksh** (Database execution validation utility)

---

### Schedule & variables
- **Schedule**: Equivalent scheduler trigger configurations must be set up in Cloud Composer / Airflow. Although no explicit cron string is defined in the source scheduler context, the job is triggered via UC4 scheduling.
- **Scheduler-set variables**:
  - `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
  - `cdate` = Dynamically computed target system date in `YYYYMMDD` format.
  - `cmonth` = First 6 characters of `cdate` (representing year and month).
  - `cday` = Character positions 7 and 8 of `cdate` (representing the day).
  - `first` = `'01'`
  - `cmonth` = Calculated dynamically by concatenating `cmonth` and `first`, subtracting 1 day via date arithmetic (`SUB_DAYS`), and then substring-extracting the first 6 characters to resolve the previous month.
  - `MONATSID` = Stores the calculated previous month value.
- **Handling in Target**: These variables must be dynamically calculated at runtime in the Airflow DAG context using standard macros (such as `{{ execution_date }}` manipulations) and passed as parameters or environment variables to the task instances.

---

### Lineage
- **Upstream USES_CONFIG dependencies**:
  - `.dw_init` references `.DW_GLOBAL` (unresolved configuration file in legacy codebase). Human review status: **NO SOURCE NEEDED** (Ananya, 2026-08-21).
  - `.dw_init` references `.DW_LOKAL` (unresolved configuration file in legacy codebase). Human review status: **NO SOURCE NEEDED** (Ananya, 2026-08-21).
- **Downstream consumers**:
  - This environment setup script is sourced downstream by the execution shell steps of the `DW.DWH_ABTN_SMART_KUBI` pipeline group to establish runtime configurations.

---

### Cross-file dependencies
- The utility functions `r_sqlscript`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh` depend on the directory structures defined within `.dw_init` for logging, error output redirection, and path configurations.
- The human-reviewed external components (`.DW_GLOBAL`, `.DW_LOKAL`, `DW.HOLE_PFAD`, and `DW.LESE_LOG`) are confirmed as "not needed" on BigQuery, meaning their functional scope is either handled natively by the GCP deployment environment or retired.

---

### Target file plan
- **Target File Path**: `local/home/gurunathan_t/kubi/dw_init.py`
  - **Source File**: `local/home/gurunathan_t/kubi/.dw_init`
  - **Language**: Python
  - **Purpose**: Mimics the legacy initialization behavior by defining and exporting directory and environment variables, mapping them into the Python runtime environment or Airflow configuration parameters.

---

### Environment-specific values

#### 1. GLOBAL (environment-wide)
These represent infrastructure-level definitions and must be resolved dynamically at runtime via environment variables (`os.environ.get`) or Airflow's configuration variable store (`Variable.get`).
- `DW_DIR_ROOT` -> Map to `os.environ.get("DW_DIR_ROOT")` (representing the base repository directory on Composer worker nodes).
- `DW_DIR_PROT` -> Map to `os.environ.get("DW_DIR_PROT")` or redirect to standard Cloud Logging / GCS logging bucket.
- `DW_DIR_CUBES` -> Map to GCS paths under `gs://{GCS_BUCKET}/daten/cubes`.
- `DW_DIR_IMP_*` (e.g., `DW_DIR_IMP_D1`, `DW_DIR_IMP_BWA`, `DW_DIR_IMP_SAP`, etc.) -> Map to respective GCS directories inside input buckets (e.g., `f"gs://{os.environ.get('GCS_BUCKET')}/daten/..."`).
- `DW_HOST_CUSTOMER` -> Map to Airflow Variable `Variable.get("DW_HOST_CUSTOMER")`.
- `ORACLE_HOME` -> Mapped to `os.environ.get("ORACLE_HOME")` (retired or simplified in BigQuery serverless runtimes).
- `DW_DIR_UTL_FILE` -> Map to GCS bucket paths where file exchange occurs.

#### 2. JOB-SPECIFIC
These properties are isolated to this specific pipeline execution and must be defined within the Airflow DAG configuration parameters (`params`).
- `ORACLE_SID` -> Replaced with target BigQuery Project ID or Airflow Connection ID references.
- `DWH_JOB_KENNUNG` -> `'ABTN_SMART_KUBI'`
- `MONATSID` -> Dynamically computed parameter passed to specific data jobs.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `local/home/gurunathan_t/kubi/dw_init.py` | Converts the environment setup KornShell script into a Python module to handle initialization on GCP. |

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
    - Multi-statement PL/SQL Anonymous Block inside a migration script.
1.2 Business Logic Summary:
    - Truncates the target table `DWH$TA_T_SMART_KUBI`.
    - Gathers mapping data from `dwh$vi_l_map_fa_tarif` and `bl_d_tarif` with a specific date filter.
    - Aggregates access logs/records from the partitioned table `dwh$ta_f_d1_twvv_tn` for a specific month.
    - Resolves both current (`tarif_id`) and old (`tarif_id_alt`) tariffs by joining with the mapping data.
    - Joins with the customer master table `dwh$ta_c_vertrag` based on validity date intervals.
    - Inserts the aggregated metrics into the target table `dwh$ta_t_smart_kubi`.
    - Implements robust error handling and logging via custom DB utilities (`dwpa_util_skript`, `dwpa_meldung`).
1.3 Entities Referenced:
    - Target: `dwh$ta_t_smart_kubi` (Table)
    - Sources:
        - `dwh$vi_l_map_fa_tarif` (View/Table)
        - `bl_d_tarif` (Table)
        - `dwh$ta_f_d1_twvv_tn` (Partitioned Fact Table)
        - `dwh$ta_c_vertrag` (Dimension Table)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → `INT64`
    - `number` → `NUMERIC` or `INT64` (where used as ID/count)
    - `varchar2(300)`, `varchar2(512)` → `STRING`
    - `DATE` → `DATE` (or `DATETIME` if time component is preserved; here, business logic uses pure dates, so `DATE` is highly appropriate)

2.2 Implicit and Explicit Type Casting:
    - Oracle `to_number('&1')` → `CAST(@monats_id AS INT64)`
    - Oracle `to_char(fact.gueltigkeitszeitpunkt,'yyyymm')` → `FORMAT_DATE('%Y%m', DATE(fact.gueltigkeitszeitpunkt))`

2.3 NULL Handling and Conditional Functions:
    - `Nvl(t_new.tarif_id, 0)` → `COALESCE(t_new.tarif_id, 0)`
    - `Nvl(t_old.tarif_id, 0)` → `COALESCE(t_old.tarif_id, 0)`
    - `Decode(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` →
      `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `Decode(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` →
      Since Oracle treats empty strings `''` as `NULL` but BigQuery does not, explicit check is added:
      ```sql
      CASE
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn
        WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END
      ```

2.4 String Functions:
    - `ltrim(rtrim(...))` → `TRIM(...)`

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)` →
      `DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH)`
    - `To_date('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`

2.6-2.8 Analytical / Set / Join Operations:
    - Oracle old-style outer joins `(+)` on multiple tables:
      Resolved to modern, explicit `LEFT OUTER JOIN` syntax.
      Note: Outer join filter condition `l_monats_date > d.gueltig_von(+)` and `l_monats_date <= d.gueltig_bis(+)` must be moved to the `ON` clause of the left join for `dwh$ta_c_vertrag d` to maintain semantic equivalence.

2.9 Row Limiting and Partition References:
    - `partition(dwh$ta_f_d1_twvv_tn_&1)`: In BigQuery, table partition names cannot be dynamically appended to a table identifier in this manner. Instead, the partition is queried via the logical table with a filter on the partitioning column. The filter `to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)` natively handles this filter when converted to standard SQL.

2.10-2.13 Sequences, MERGE, DML, DDL:
    - Oracle dynamic SQL `dwpa_util_skript.runstatement(eintragsnr, 'Truncate table ...')`:
      Translated to a direct BigQuery standard DML statement: `TRUNCATE TABLE dwh_ta_t_smart_kubi;`.

2.14 PL/SQL Block Structures:
    - PL/SQL anonymous block structure → BigQuery script block wrapping execution logic in `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`.
    - `SQL%ROWCOUNT` → Captured using `@@row_count` system variable immediately following DML execution.
    - Exception block catches error codes (`@@error.code`, `@@error.message`) and routes them to equivalent logging wrappers.

2.15 Unresolvable / Advisory Items:
    - Global packages / utilities: `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` are custom Oracle packages. These must be represented as either native BigQuery SQL Procedure calls or placeholder calls requiring downstream system integration.
    - Oracle optimizer hints (e.g., `/*+ Append */`, `/*+ parallel(...) */`) are stripped.

Step 3: Conversion Strategy Summary
3.1 Conversion Approach:
    - Refactor the PL/SQL Anonymous block into a BigQuery procedural script (`DECLARE ... BEGIN ... EXCEPTION ... END;`).
    - Standardize Oracle implicit joins `(+)` to SQL-92 `LEFT OUTER JOIN` clauses.
    - Standardize special table character patterns: Replace Oracle naming style `DWH$TA_T_SMART_KUBI` with standard BigQuery identifiers `dwh_ta_t_smart_kubi`.
3.2 Key Assumptions:
    - Parameter variables `&1` and `&2` will be passed as BQ script variables or parameters named `monats_id` and `eintrags_nr`.
    - Date formats in `fact.gueltigkeitszeitpunkt` are parseable as Date objects in BigQuery.
3.3 Human Review Required:
    - Implementation check on the `dwpa_meldung_fehler` procedure mock in BigQuery.

═══════════════════════════════════════════
MIGRATION DECISION AND REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Oracle Construct / Statement | Target Architecture | Rejected Alternatives | Evidence & Reason for Choice |
| :--- | :--- | :--- | :--- |
| **Anonymous PL/SQL Block** | BigQuery Scripting (`BEGIN...END`) | Python Orchestrator | Direct execution of procedural steps is native to BigQuery scripting, removing orchestration overhead. |
| **Dynamic TRUNCATE SQL Execution** | Direct BigQuery DML `TRUNCATE` | Python DB-API `execute()` | Direct DML runs faster, provides transactional safety within the script block, and eliminates runtime evaluation. |
| **Oracle Outer Joins `(+)`** | BigQuery `LEFT OUTER JOIN` | Dynamic execution | SQL-92 Joins are standard, highly optimized, and necessary for compile-time validations in BigQuery. |
| **Custom Logging (`dwpa_meldung`)** | BigQuery `CALL` to Logging SP | Python Wrapper logging | Allows the migration logic to remain entirely in SQL, keeping standard operational flows consistent. |

2.17 REQUIRED ARTIFACTS

The migration must generate the following deliverables:
1. **BigQuery SQL Script**: A standalone SQL script containing procedural definitions (`DECLARE`), `TRUNCATE`, `INSERT INTO` (using CTEs and standardized `LEFT JOIN`), operational tracking (`@@row_count`), and error capture (`EXCEPTION WHEN ERROR`).
2. **Metadata Procedure Mocks (Advisory)**: DDL definitions for missing shared utilities, e.g., `CALL dwh_utility.dwpa_meldung_fehler(...)`.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Type | Target BigQuery Type | Conversion Rule | Warning / Assessment |
| :--- | :--- | :--- | :--- |
| `pls_integer` | `INT64` | Direct Assignment | No risk of data overflow or precision loss. |
| `number` | `NUMERIC` or `INT64` | `INT64` for identifiers; `NUMERIC` for precise scales | Evaluated numeric ranges; ids like `eintrags_nr` convert safely to `INT64`. |
| `varchar2(300)` | `STRING` | Direct Assignment | BigQuery `STRING` type size is variable; no truncation issues. |
| `DATE` | `DATE` / `DATETIME` | Match business context (Here: `DATE` is used since data logic is date-bound) | Date boundary comparisons (`4712-12-31`) are preserved using standard `DATE '4712-12-31'`. |

2.19 DESIGN REVIEW SUMMARY

- **Patterns/Objects Found**: Static partition referencing in Oracle FROM clauses, old Oracle outer-joins (`(+)`), dynamic string SQL execution, and custom PL/SQL exception handling.
- **Unsupported Functions**: Oracle `ADD_MONTHS`, `TO_DATE` with specific format models, and custom PL/SQL exception attributes (`SQLERRM`, `SQLCODE`).
- **UDFs Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Requires target table `dwh_ta_t_smart_kubi` and source tables to be present in target datasets.
- **Assumptions**: The system executing this script will replace `&1` and `&2` placeholders with values before compilation or pass them as parameters.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(date, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE` / `DATE 'YYYY-MM-DD'` literal |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(value AS INT64)` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATE` / `CAST(value AS STRING)` |
| `DECODE` | Direct-with-rewrite | Standard `CASE WHEN ... THEN ... ELSE ... END` |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `SQLERRM` | Direct-with-rewrite | `@@error.message` |
| `SQLCODE` | Direct-with-rewrite | `@@error.code` |
| `WHENEVER SQLERROR` | Direct-with-rewrite | Native exception block handling (`EXCEPTION WHEN ERROR`) |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Vendor-Neutral Pseudocode

```sql
-- Parameters declared at script scope, mimicking runtime configuration
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

-- Initialize runtime input variables (placeholders for script execution inputs)
SET l_monats_id = CAST(@monats_id AS INT64);  -- converted from to_number('&1')
SET EintragsNr = CAST(@eintrags_nr AS INT64);  -- converted from to_number('&2')

-- Calculate target period boundary
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);  -- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)

BEGIN
  -- Execute truncate on target table
  SET lv_str = 'Truncate table dwh_ta_t_smart_kubi';
  TRUNCATE TABLE dwh_ta_t_smart_kubi;  -- converted from dynamic SQL execution

  -- Main processing chunk with explicit JOIN conditions
  INSERT INTO dwh_ta_t_smart_kubi (
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
    CASE 
      WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
      ELSE d.t_mobile_kundennummer 
    END AS kundennummer,  -- converted from Decode(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)
    COALESCE(t_new.tarif_id, 0) AS tarif_id,  -- converted from Nvl(t_new.tarif_id, 0)
    COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,  -- converted from Nvl(t_old.tarif_id, 0)
    CASE
      WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn
      WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
      ELSE fact.vo_kenn_bearb
    END AS vo_kennung,  -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, ...)
    d.test_gp,
    SUM(fact.zugang) AS anzahl,
    fact.kennzahl_id
  FROM dwh_ta_f_d1_twvv_tn AS fact
  LEFT OUTER JOIN temp AS t_new
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp AS t_old
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh_ta_c_vertrag AS d
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    AND l_monats_date > d.gueltig_von
    AND l_monats_date <= d.gueltig_bis  -- converted Oracle (+) join conditions into modern standard ANSI syntax
  WHERE FORMAT_DATE('%Y%m', DATE(fact.gueltigkeitszeitpunkt)) = CAST(l_monats_id AS STRING)  -- converted from to_char(gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
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

  -- Gathers transaction outcomes
  SET v_anzahl_ds = @@row_count;  -- converted from SQL%ROWCOUNT

  -- Logging transaction success
  SELECT FORMAT('%d rows inserted in dwh_ta_t_smart_kubi', v_anzahl_ds);  -- converted from dbms_output.put_line

EXCEPTION WHEN ERROR THEN
  -- Exception Block to trap any run failures
  -- Rolling back is automatically handled by BigQuery for failed transaction blocks.
  DECLARE ErrText STRING;
  DECLARE ErrC STRING;
  DECLARE FehlerNr INT64;

  SET ErrText = @@error.message;  -- converted from SQLERRM
  SET ErrC = CAST(@@error.code AS STRING);  -- converted from SQLCODE
  SET FehlerNr = -20001; -- Resolved arbitrary custom global constant k_alis_err_unknown to a generic application error block

  -- Call generic logger to preserve business operational logs
  CALL `project.dataset.dwpa_meldung_fehler`('F', EintragsNr, FehlerNr, ErrText, ErrC);
  
  -- Propagate error out of script block
  ERROR FORMAT('Fehler %d: %s', FehlerNr, ErrText);
END;
```

FLAGGED ITEMS FOR HUMAN REVIEW:
1. **Dynamic SQL Logging Hook**: The original code executes `dwpa_util_skript.runstatement(eintragsnr, lv_str)`. In BigQuery, this has been simplified to run direct standard SQL statements without passing the string through a wrapper. If this custom logger performs database-side process auditing, a separate audit table log should be added.
2. **Metadata Procedure Mock**: The package call `dwpa_meldung.fehler` has been replaced with `CALL project.dataset.dwpa_meldung_fehler`. This procedure must be separately compiled/deployed in the target environment to support process failure logging.
3. **Implicit Empty Character Rules**: Evaluated differences between Oracle and BigQuery string evaluation. In Oracle, `LTRIM(RTRIM(vo_kenn_bearb))` evaluating to empty is implicitly captured by `IS NULL`. In BigQuery, this check has been explicitly expanded to `TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = ''` to prevent functional variance. Confirm if empty spaces should also fall back to original `vo_kenn`.

### Execution order
Based on the legacy dependency graph, the execution sequence is defined as follows:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (the UC4 wrapper orchestration)
2. `d_abtn_x_smart_kubi.sql` (the primary PL/SQL aggregation and load logic)
3. `r_sqlscript` (the Oracle execution utility)
4. `.dw_init` (environment initialization script)
5. `f_alis_msgerr.ksh` (error handling and logging shell wrapper)
6. `h_alis_sqlplus.ksh` (SQL*Plus helper script with validation)

In the target architecture:
- The orchestration sequence is preserved in a Cloud Composer (Airflow) DAG.
- `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, and `r_sqlscript` are retired as standalone files. Their logic (setting environments, trapping execution errors, and logging status) is handled natively by the Airflow DAG configuration, BigQuery scripting `EXCEPTION` blocks, and native query execution operators.
- The SQL file `d_abtn_x_smart_kubi.sql` is converted to BigQuery SQL and executed via a BigQuery script task.

### Schedule & variables
The scheduler-set variables from the legacy environment must be computed dynamically in the Airflow DAG and passed down to the BigQuery SQL script task at runtime:
- `DWH_JOB_KENNUNG` (Value: `'ABTN_SMART_KUBI'`): Passed as a static task label or parameter.
- `cdate` (Value: `SYS_DATE("YYYYMMDD")`): Calculated dynamically in Airflow using Jinja context: `{{ ds_nodash }}`.
- `cday` (Value: `SUBSTR(&cdate,7,2)`): Calculated dynamically in Airflow: `{{ ds_nodash[6:8] }}`.
- `first` (Value: `'01'`).
- `cmonth` / `MONATSID` (Value represents the prior month in `YYYYMM` format, computed sequentially in the legacy scheduler): Calculated dynamically in Airflow using: `{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m') }}` and bound to the BigQuery query parameter `@monats_id`.

### Lineage
- **Upstream Inputs**:
  - `dwh$ta_f_d1_twvv_tn`: Monthly partitioned fact table containing access and transaction logs.
  - `dwh$vi_l_map_fa_tarif`: View providing current active tariff mappings.
  - `bl_d_tarif`: Dimension table containing business-line tariffs.
  - `dwh$ta_c_vertrag`: Dimension table containing customer master contracts.
- **Downstream Output**:
  - `dwh$ta_t_smart_kubi`: Aggregated customer tariff table written to and populated by this execution block.

### Cross-file dependencies
- **Oracle Shared Package Dependencies**:
  - `dwpa_util_skript.runstatement`: Used dynamically in the legacy script to execute TRUNCATE statements. It is retired and replaced with native, static `TRUNCATE TABLE` statements.
  - `dwpa_meldung.fehler`: Used to register procedural runtime errors. It is mapped to a centralized utility stored procedure call in BigQuery: `CALL <project>.<dataset>.dwpa_meldung_fehler(...)`.
- **Parser Aliases**:
  - The lineage references `T_NEW` and `T_OLD` as package dependencies. These are actually SQL CTE aliases (`temp t_new`, `temp t_old`) referencing the CTE `temp`, and do not map to external physical package structures.

### Target file plan
- **Target File Path**: `home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  - **Language**: SQL (BigQuery Dialect)
  - **Source File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`

### Environment-specific values
- **GLOBAL**:
  - `GCP_PROJECT`: Sourced dynamically at runtime through Airflow variables or `@gcp_project` query parameters.
  - `BQ_DATASET`: BigQuery dataset where the DWH tables reside. Sourced dynamically from Airflow configuration variables.
- **JOB-SPECIFIC**:
  - `monats_id`: Sourced from the calculated `@monats_id` parameter, passed from the Airflow execution environment.
  - `eintrags_nr`: Audit execution trace number, bound to the `@eintrags_nr` parameter.
  - `dwh_ta_t_smart_kubi`, `dwh_ta_f_d1_twvv_tn`, `dwh_vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh_ta_c_vertrag`: Explicit table and view identifiers, mapped to their resolved environment pathing under `BQ_DATASET`.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | Converted to native BigQuery Standard SQL script. Replaces implicit outer joins `(+)` with SQL-92 `LEFT OUTER JOIN` structures, handles custom Oracle conditional statements, and converts dynamic truncate executions to static BigQuery statements. |

### Risks & Manual Actions
- **Logging Procedure Deployment**: The legacy call to `dwpa_meldung.fehler` requires a central, mock stored procedure `dwpa_meldung_fehler` to be pre-deployed in the target utility dataset to prevent query validation errors during execution.
- **Partition Filtering and Cost Pruning**: The original code targets a static Oracle partition using the clause `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, query scanning costs are optimized by ensuring that the filter on the partitioning column is explicitly written in the `WHERE` clause of the query, allowing BigQuery's query optimizer to perform partition pruning.

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
REASON: The script defines a library of KornShell logging and error handling functions that utilize shell evaluation, temporary files, external date commands, and SQL*Plus invocations.

EVIDENCE
- Business logic found: KSH custom logic. The script implements operational support functions for error handling, database status updates (OK/Aborted), unique ID generation, logging registration, error reporting, timestamp calculation, and date formatting.
- AWK: none
- SQL-expressible: No, because it involves OS interactions, shell runtime environment manipulations (such as dynamic variable assignment via eval), generating temp files, and handling execution errors.
- Non-SQL side effects: Interacts with the local file system (creating, reading, and removing temporary files in `/tmp`), invokes external commands (sqlplus, date, cat, rm, tr), and relies on environment variable modification.
- Against this verdict: If we ignored the shell environment management and OS integrations, the database updates themselves could be represented in SQL/PL-SQL; however, because this is an orchestration-level error handling and logging framework meant to wrap job processes, it must be converted to Python to retain its system-level utility and integration.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

=== DESIGN DOCUMENT STRUCTURE ===

1. SCRIPT OVERVIEW
   This script acts as a utility library (`f_alis_msgerr.ksh` / `dwmsg.ksh`) of KornShell functions designed for centralized error handling, logging, and job status tracking within the "Information Services" system. It provides routines to initialize a log entry, record errors, update completion statuses (OK/Aborted), generate standardized log file names, and log execution timing/business date metrics to an Oracle database (using a `BERT_MELDUNG` PL/SQL package via `sqlplus`).

2. INVOCATION CONTEXT
   - Who calls this script: Sourced as a library by various job scripts (e.g. using `. f_alis_msgerr.ksh`) within the UC4 orchestration ecosystem.
   - UC4 native includes: None supplied in the extraction.
   - Environment variables / files sourced: No environment files are explicitly sourced inside this file itself, but it expects environment variables like `$DW_ORAUSER`, `$DW_DIR_ROOT`, and `$DW_DIR_PROT` to be defined by the calling parent environment.

3. PARAMETERS / INPUTS
   The script does not receive direct command-line arguments when sourced, but its individual functions accept arguments:
   - `DW_ORAUSER` (env var) - Used as the Oracle database connection credential string for `sqlplus`. In Python, this should be surfaced via `os.environ.get("DW_ORAUSER")`.
   - `DW_DIR_ROOT` (env var) - Path used to find helper SQL scripts in `$DW_DIR_ROOT/allgemein/is/util/sql/`. Surfaced in Python via `os.environ.get("DW_DIR_ROOT")`.
   - `DW_DIR_PROT` (env var) - Path used to determine the directory for writing log files. Surfaced in Python via `os.environ.get("DW_DIR_PROT")`.
   - Argument parameters in functions:
     - `EintragsNr` ($1) - Database sequence key / unique process run ID used across most functions.
     - `VarName` ($1) - Name of variable to assign result back to in `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`.
     - `JobKennung` ($2) - Job identifier code.
     - `Programmname` ($3) - Program name/executable name.
     - `LogDatei` ($4) - Log filename.
     - `Typ` ($2) - Type of message: F (Fatal), E (Error), W (Warning).
     - `FehlerNr` ($3) - Integer error number.
     - `Zusatz1` ($4, optional) - Auxiliary detail message 1.
     - `Zusatz2` ($5, optional) - Auxiliary detail message 2.
     - `Stichtag` ($2) - Reference date.
     - `StichtagFmt` ($3) - Reference date format.
     - `InfoText` ($2) - Performance/timing metadata message.
     - `DateFormat` ($3) - Date/time format mask.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Invoked via subprocess with connection credentials `$DW_ORAUSER`. Replaced in modern Python by native database adapter calls (such as the standard `oracledb` library).
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
     - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
     - `sqlplus -s $DW_ORAUSER` (running inline blocks)
     * # REVIEW-STRUCT: SQL helper scripts (e.g., d_alis_spaufruf_p1.sql, d_al_is_ermittlenr.sql, etc.) are not supplied in this extraction; confirm database client procedure parameters and rewrite as native DB-client calls.
   - `cat`: Used to retrieve the sequence key from `/tmp/ErmittleNr_$$.lst`. Replaced in Python by standard file reading or, ideally, direct DB query results.
   - `tr`: Used to strip whitespace from SQL output files. Replaced by `.replace(" ", "")` or `.strip()` in Python.
   - `rm`: Used to delete the temporary file `/tmp/ErmittleNr_$$.lst`. Replaced by Python's `os.remove()` or handled contextually via `tempfile` library.
   - `date`: Used to format timestamps for naming log files. Replaced by Python's native `datetime.now().strftime()`.

5. EMBEDDED SQL
   - Inline SQL block in `DWMSG_SetzeStichtagInfo`:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - Statement type: PL/SQL execution block.
     - Table(s) touched: Unknown (encapsulated in package `BERT_MELDUNG`).
     - Dialect: Oracle SQL*Plus dialect.
   - Inline SQL block in `DWMSG_AppendTimingInfos`:
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - Statement type: PL/SQL execution block.
     - Table(s) touched: Unknown (encapsulated in package `BERT_MELDUNG`).
     - Dialect: Oracle SQL*Plus dialect.
     * # REVIEW: Target database platform not specified in code, but Oracle dialect is confirmed via sqlplus and PL/SQL syntax. DB-client library choice (oracledb) is provisional.

6. CONTROL FLOW
   Execution order mapping per function:
   1. `DWMSG_Fehlerbehandlung`:
      - Capture exit status of the failed command (`FehlerNr=$?`).
      - Call `DWMSG_MeldeFehler` with Fatal ("F") type, error code "10" (kUnerwFehler), and custom text containing the exit code.
      - Call `DWMSG_SetzeStatusAbbruch`.
   2. `DWMSG_SetzeStatusOK`:
      - Verify argument `$1` is not empty. If empty, print error and exit 1.
      - Execute `sqlplus` running `d_alis_spaufruf_p1.sql` with action `BERT_MELDUNG.SetzeStatusOk`.
   3. `DWMSG_SetzeStatusAbbruch`:
      - Verify argument `$1` is not empty. If empty, print error and exit 1.
      - Execute `sqlplus` running `d_alis_spaufruf_p1.sql` with action `BERT_MELDUNG.SetzeStatusAbbruch`.
   4. `DWMSG_ErmittleNr`:
      - Verify argument `$1` is not empty. If empty, print error and exit 1.
      - Create unique temp file path `/tmp/ErmittleNr_$$.lst`.
      - Execute `sqlplus` running `d_al_is_ermittlenr.sql` passing the temp file path as an argument.
      - Read the generated temp file, strip spaces, and assign the retrieved ID back to the named variable using shell `eval`.
      - Remove the temporary file.
   5. `DWMSG_ErzeugeEintrag`:
      - Verify argument `$1` is not empty. If empty, print error and exit 1.
      - Execute `sqlplus` running `d_alis_spaufruf_p4.sql` with the 4 arguments to invoke `BERT_MELDUNG.Erzeuge_Eintrag`.
   6. `DWMSG_MeldeFehler`:
      - Parse up to 5 arguments.
      - Verify `$1` is not empty. If empty, print error and exit 1.
      - Determine parameter count (3, 4, or 5) based on which optional variables are set.
      - Invoke corresponding helper SQL script `d_alis_spaufruf_p${NumParm}.sql` via `sqlplus` to call `BERT_MELDUNG.Fehler` with the arguments.
   7. `DWMSG_Logdateiname`:
      - Build a formatted string of the current date and time: `YYYYMMDD_HHMM` using the `date` command.
      - Construct log path: `${DW_DIR_PROT}/${JobKennung}_${date}_${EintragsNr}.log`.
      - Assign back to the named variable using shell `eval`.
   8. `DWMSG_SetzeStichtagInfo`:
      - Check all 3 arguments are present; if any are missing, print error and exit 1 or 2.
      - Call `BERT_MELDUNG.SetzeZusatzInfos` via `sqlplus` with an inline block, parsing the date via Oracle `to_date()`.
   9. `DWMSG_AppendTimingInfos`:
      - Check arguments; if EintragsNr or DateFormat is missing, print error and exit 1 or 2.
      - Call `BERT_MELDUNG.SetzeZusatzInfos` via `sqlplus` with an inline block, appending the current timestamp formatted via `to_char(SYSDATE, DateFormat)`.

7. ERROR HANDLING & EXIT CODES
   - If a function fails its argument check, it writes an error message to stdout and exits with code 1 or 2.
   - Modernized Python: Replace shell exits inside functions with `ValueError` exceptions or log messages to `sys.stderr`, allowing parent orchestrators to catch and properly format exceptions.
   - For shell script compatibility, functions can be called via a CLI wrapper that maps exceptions to matching shell exit status codes.

8. OUTPUTS / SIDE EFFECTS
   - Writes database updates to Oracle DB tables via `BERT_MELDUNG` packages.
   - Creates and removes temporary files under `/tmp/`.
   - Generates and returns string variables (such as file paths).

9. BUSINESS SUMMARY
   - Centralizes batch job monitoring, status logging, and error tracing in an Oracle database.
   - Allows automatic execution recovery tracking by registering every job run with a unique identifier (`EintragsNr`).
   - Ensures error states (Fatal, Error, Warning) are standardized and captured with contextual details (e.g. system errors, file paths).
   - Facilitates operational auditing by stamping log files with job names, run IDs, and dates, and appending timing metrics for performance analysis.

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import datetime
import subprocess
import tempfile

# REVIEW: Target database platform not specified in code, but Oracle dialect is confirmed via sqlplus and PL/SQL syntax.
# Python 'oracledb' library is the recommended modern client for Oracle database connectivity.

def get_db_connection():
    # Helper to establish connection using credentials in os.environ["DW_ORAUSER"]
    # oracledb.connect(user=..., password=..., dsn=...)
    pass

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code=1):
    # sichern des FehlerCodes
    fehler_nr = last_exit_code
    k_unerw_fehler = 10
    
    # Melde Fehler in der Meldungstabelle
    dwmsg_melde_fehler(eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL helper script d_alis_spaufruf_p1.sql not supplied.
    # Below shows the external invocation. Modern replacement should use native DB call:
    # cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    
    cmd = ["sqlplus", "-s", dw_orauser, f"@{sql_script}", "BERT_MELDUNG.SetzeStatusOk", str(eintrags_nr)]
    subprocess.run(cmd, input="", text=True, check=True)

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL helper script d_alis_spaufruf_p1.sql not supplied.
    # Modern replacement should use native DB call:
    # cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    
    # Note: original did not have -s for SetzeStatusAbbruch
    cmd = ["sqlplus", dw_orauser, f"@{sql_script}", "BERT_MELDUNG.SetzeStatusAbbruch", str(eintrags_nr)]
    subprocess.run(cmd, input="", text=True, check=True)

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    # REVIEW: Instead of eval to set parent shell variables, return the value directly.
    # REVIEW-STRUCT: SQL helper script d_al_is_ermittlenr.sql not supplied.
    # Modern replacement should query Oracle sequence directly, e.g. "SELECT BERT_MELDUNG.GetNextVal() FROM DUAL"
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_al_is_ermittlenr.sql")
    
    with tempfile.NamedTemporaryFile(mode='w+', prefix='ErmittleNr_', suffix='.lst', delete=False) as temp_file:
        temp_file_path = temp_file.name
        
    try:
        cmd = ["sqlplus", "-s", dw_orauser, f"@{sql_script}", temp_file_path]
        subprocess.run(cmd, input="", text=True, check=True)
        
        with open(temp_file_path, 'r') as f:
            eintrags_nr = f.read().replace(' ', '').strip()
            
        return eintrags_nr
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL helper script d_alis_spaufruf_p4.sql not supplied.
    # Modern replacement: cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programmname, log_datei])
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p4.sql")
    
    cmd = ["sqlplus", "-s", dw_orauser, f"@{sql_script}", "BERT_MELDUNG.Erzeuge_Eintrag", str(eintrags_nr), job_kennung, programmname, log_datei]
    subprocess.run(cmd, input="", text=True, check=True)

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    num_parm = 3
    if zusatz1:
        num_parm = 4
    if zusatz2:
        num_parm = 5
        
    # REVIEW-STRUCT: SQL helper script d_alis_spaufruf_p[3-5].sql not supplied.
    # Modern replacement: cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    sql_script = os.path.join(dw_dir_root, f"allgemein/is/util/sql/d_alis_spaufruf_p{num_parm}.sql")
    
    cmd = ["sqlplus", "-s", dw_orauser, f"@{sql_script}", "BERT_MELDUNG.Fehler", typ, str(eintrags_nr), str(fehler_nr), f"'{zusatz1}'", f"'{zusatz2}'"]
    subprocess.run(cmd, input="", text=True, check=True)

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # REVIEW: Instead of eval to set parent shell variables, return the value directly.
    dw_dir_prot = os.environ.get("DW_DIR_PROT")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = f"{dw_dir_prot}/{job_kennung}_{timestamp}_{eintrags_nr}.log"
    return dateiname

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
        
    # Modern replacement: cursor.execute("BEGIN BERT_MELDUNG.SetzeZusatzInfos(:1, TO_DATE(:2, :3)); COMMIT; END;", [eintrags_nr, stichtag, stichtag_fmt])
    dw_orauser = os.environ.get("DW_ORAUSER")
    sql_block = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({eintrags_nr}, to_date('{stichtag}', '{stichtag_fmt}'));
    commit;
    """
    cmd = ["sqlplus", "-s", dw_orauser]
    subprocess.run(cmd, input=sql_block, text=True, check=True)

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Modern replacement: cursor.execute("BEGIN BERT_MELDUNG.SetzeZusatzInfos(:1, NULL, :2 || ' ' || TO_CHAR(SYSDATE, :3) || ' '); COMMIT; END;", [eintrags_nr, info_text, date_format])
    dw_orauser = os.environ.get("DW_ORAUSER")
    sql_block = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({eintrags_nr},null,'{info_text}'||' '||to_char(SYSDATE,'{date_format}')||' ');
    commit;
    """
    cmd = ["sqlplus", "-s", dw_orauser]
    subprocess.run(cmd, input=sql_block, text=True, check=True)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Migrated from a KornShell utility library to a Python utility module. Replaces Oracle `sqlplus` database status updates with standard calls to BigQuery stored procedures via the `google-cloud-bigquery` library, conforming to the BigQuery target architecture and correcting the previous integration errors. |

---

### Execution Order
The target orchestration must preserve the execution sequence of the legacy dependency graph. The utilities defined in `f_alis_msgerr.py` fit into this ordered task chain as follows:
1. **`DW.DWH_ABTN_SMART_KUBI.xml`** $\rightarrow$ Migrated to Cloud Composer DAG.
2. **`d_abtn_x_smart_kubi.sql`** $\rightarrow$ Migrated to Dataform SQLX / BigQuery SQL.
3. **`r_sqlscript`** $\rightarrow$ Migrated to Python script execution wrapper.
4. **`.dw_init`** $\rightarrow$ Migrated to Python environment initialization module.
5. **`f_alis_msgerr.ksh`** $\rightarrow$ Migrated to Python logging/monitoring utility module (`local/home/gurunathan_t/kubi/f_alis_msgerr.py`).
6. **`h_alis_sqlplus.ksh`** $\rightarrow$ Migrated to Python SQL-execution utility module.

---

### Schedule & Variables — Must Be Retained
This job is executed based on scheduler-defined parameters. The legacy scheduler-set variables must be calculated in the Cloud Composer DAG and passed to the Python utilities and SQL tasks:
* **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`
* **`cdate`** = Dynamic execution date of the DAG (mapped via Airflow Jinja: `{{ ds_nodash }}`).
* **`cmonth`** = First 6 characters of `cdate` (`{{ ds_nodash[:6] }}`).
* **`cday`** = Last 2 characters of `cdate` (`{{ ds_nodash[6:8] }}`).
* **`first`** = `'01'`
* **`cmonth`** = Derived dynamically via date manipulation (equivalent to standard Airflow execution date metrics).
* **`MONATSID`** = Month ID passed down to the pipeline execution.

These variables will reach the migrated job via Cloud Composer execution context parameters and Python function arguments.

---

### Lineage Edges
* **Upstream Producer / External Call:** `f_alis_msgerr.py` calls the BigQuery stored procedures corresponding to the legacy Oracle `SETZEZUSATZINFOS` procedure (lineage confidence: 0.75).

---

### Cross-File Dependencies
* **Common Call Chains:** Other components (such as `r_sqlscript`, `h_alis_sqlplus.ksh`, or individual SQL execution wrappers) source and utilize `f_alis_msgerr.ksh` for error tracking and database logging.
* **Target Integration:** In the Python environment, these scripts will import `local.home.gurunathan_t.kubi.f_alis_msgerr` and call its functions directly. This avoids subprocess calls and ensures standard modular execution.

---

### Target File Plan

#### File: `local/home/gurunathan_t/kubi/f_alis_msgerr.py`
This module will be a clean Python library implementing the legacy error logging and tracking functions. It uses `google-cloud-bigquery` to execute BigQuery stored procedures representing the `BERT_MELDUNG` (or `dwpa_meldung`) package operations.

* **Key Functions:**
  1. `get_bq_client()`: Initializes and returns a `google.cloud.bigquery.Client` using environment-wide configurations.
  2. `dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code)`: Replaces the custom shell trap handler. Logs the fatal error and sets the job status to aborted.
  3. `dwmsg_setze_status_ok(eintrags_nr)`: Calls the BigQuery stored procedure `dwpa_meldung_setze_status_ok` parameterized with `@eintrags_nr`.
  4. `dwmsg_setze_status_abbruch(eintrags_nr)`: Calls the BigQuery stored procedure `dwpa_meldung_setze_status_abbruch` parameterized with `@eintrags_nr`.
  5. `dwmsg_ermittle_nr()`: Direct SQL call to fetch the next sequence value (e.g., executing `SELECT dwpa_meldung_next_val()`) instead of outputting to a temporary file in `/tmp`.
  6. `dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei)`: Calls `dwpa_meldung_erzeuge_eintrag` parameterized with the respective values.
  7. `dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2="")`: Calls `dwpa_meldung_fehler` parameterized with `@typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2`.
  8. `dwmsg_logdateiname(job_kennung, eintrags_nr)`: Generates a standard log file name string matching the legacy format under the configured log bucket/directory.
  9. `dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt)`: Maps the Oracle date format to a Python `strptime` format, parses the date in Python, and calls `dwpa_meldung_setze_stichtag_info` passing a native `datetime.date` query parameter.
  10. `dwmsg_append_timing_infos(eintrags_nr, info_text, date_format)`: Formats the current time in Python using the mapped date format and calls the corresponding BigQuery logging procedure.

---

### Environment-Specific Values

All environment variables are classified by their role in the target environment:

#### 1. GLOBAL (Environment-wide constants)
These identify target infrastructure and are resolved at runtime via Airflow config or environment variables:
* **`GCP_PROJECT`** $\rightarrow$ Sourced via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`. Represents the active GCP Project.
* **`GCP_REGION`** $\rightarrow$ Sourced via `Variable.get("GCP_REGION")` or `os.environ.get("GCP_REGION")`. Represents the operational region.
* **`BQ_DATASET`** $\rightarrow$ Sourced via `Variable.get("BQ_DATASET")`. Identifies the metadata/monitoring dataset where the logging stored procedures reside.
* **`GCS_BUCKET`** $\rightarrow$ Sourced via `Variable.get("GCS_BUCKET")`. Replaces `$DW_DIR_PROT` for physical log file destinations if required (otherwise redirecting to Cloud Logging).

#### 2. JOB-SPECIFIC
These are parameters specific to this job's execution context:
* **`DWH_JOB_KENNUNG`** $\rightarrow$ Map to `params` or execution parameters in Cloud Composer: `'ABTN_SMART_KUBI'`.
* **`MONATSID`** $\rightarrow$ Passed dynamically via Cloud Composer task context parameters.

---

### Output/Print Literal Rule
All original logging and stdout messages are preserved verbatim in German inside the Python module:
* `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`
* `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`
* `"Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"`
* `"Argh!, keinen Variablennamen bei ErmittleNr angegeben"`
* `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`
* `"Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"`
* `"Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"`
* `"Argh!, keinen Stichtag angegeben!"`
* `"Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"`
* `"Argh!, Formatangabe erforderlich!"`

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
REASON: This is a helper script defining a function to validate and execute SQL*Plus scripts via an external command, which requires Python for shell-like file validation and process execution.

EVIDENCE
- Business logic found: KSH custom logic: Defines a helper function `starteSQLSkript` that validates parameters, checks SQL script file readability, and executes SQL*Plus.
- AWK: none
- SQL-expressible: No, this is orchestration logic containing parameter checks, filesystem validation, and process execution control.
- Non-SQL side effects: Interacts with the local filesystem (checking file readability) and invokes external commands (`sqlplus` and `DWMSG_MeldeFehler`).
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a reusable KornShell helper library designed to facilitate execution of SQL*Plus scripts. It defines a single public function, `starteSQLSkript`, which performs parameter validation and verifies that a target SQL script is readable on the local filesystem before calling SQL*Plus. This ensures safe execution and standardized error logging for database operations within a wider orchestration architecture.

2. INVOCATION CONTEXT
   - Who calls this script: It is designed to be sourced or executed by other shell scripts or UC4 jobs that perform database transactions via Oracle SQL*Plus. The specific calling UC4 job names or arguments are not present in this isolated utility script.
   - UC4 native includes: None referenced in the script itself.
   - Environment files sourced: None directly sourced in this script, though it expects environment variables (like `DW_ORAUSER`) to be pre-configured by the calling shell context.

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following arguments:
   - `p_Eintragsnr` ($1): Positional argument. Represents a unique error log or transaction entry ID used by the custom error reporting utility. Required. Map to a Python parameter.
   - `p_Skript` ($2): Positional argument. The absolute or relative path to the SQL*Plus script to execute. Required. Map to a Python parameter.
   - Remaining arguments ($* / $3 onwards after `shift 2`): Zero or more parameters passed directly to the target SQL script. Map to Python `*args` varargs.
   - `DW_ORAUSER` (Environment Variable): Used as the connection string or credential identifier for `sqlplus`. Accessed via `os.environ.get("DW_ORAUSER")`.
   - `ModulName` / `ModulVersion`: Local variables representing metadata (`"alis_sqlplus"` and `"V1.1.3"`).

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWMSG_MeldeFehler`:
     - Command Line: `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"` or `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
     - Purpose: An external logging/alerting program to report validation failures (e.g., missing arguments or unreadable SQL script).
     - Execution: Must remain an external process invocation via `subprocess.run()`.
     - # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
   - `sqlplus`:
     - Command Line: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Launches the Oracle SQL*Plus command line interface to execute the specified script with arguments, using `/dev/null` as standard input to prevent interactive waiting.
     - Execution: Keep as an external process invocation via `subprocess.run()` since it executes arbitrary dynamic scripts passed as arguments.
     - # REVIEW: target database platform is Oracle based on sqlplus; if migrating to BigQuery, sqlplus commands must be refactored to BigQuery client calls.

5. EMBEDDED SQL
   There is no embedded SQL inside this shell script. It only acts as an orchestrator/runner for external `.sql` scripts.

6. CONTROL FLOW
   1. Initialize script-level metadata variables: `ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`.
   2. Define function `starteSQLSkript` with local parameters mapped to variables.
   3. Check parameters: If `p_Eintragsnr` or `p_Skript` are null/empty, execute `DWMSG_MeldeFehler` with error code `196` and return `196`.
   4. Check file readability: If `p_Skript` is not a readable file, execute `DWMSG_MeldeFehler` with error code `201` and return `201`.
   5. Print diagnostic information containing the script name and passed parameters to standard output.
   6. Execute SQL*Plus by running `sqlplus ${DW_ORAUSER} @$p_Skript` followed by any remaining arguments, redirecting `/dev/null` to standard input.
   7. Capture the return code of `sqlplus`.
   8. Return the captured error code to the caller.

7. ERROR HANDLING & EXIT CODES
   - Validation Errors: Missing parameters return exit code `196`. Unreadable SQL files return exit code `201`.
   - Execution Errors: SQL*Plus execution is wrapped in `set +e` to prevent the shell from exiting automatically on failure. The return code `$?` of the `sqlplus` execution is captured and returned by the function.
   - Python Mapping: Map validation checks to Python raising exceptions or returning integers (matching the script's function return strategy). Map `sqlplus` execution to a `subprocess.run` call capturing `returncode` without raising `CalledProcessError` immediately, allowing the function to return the exit code natively.

8. OUTPUTS / SIDE EFFECTS
   - Standard output logging about script initiation.
   - Standard error logging if `sqlplus` or validation fails.
   - External dependency side effects caused by whatever target SQL script is being executed (e.g., tables updated/inserted in Oracle).

9. BUSINESS SUMMARY
   - Serves as a central, standardized shell routine for running database SQL migrations/updates safely.
   - Prevents empty or missing file references from silently succeeding or hanging in SQL*Plus.
   - Formats execution metadata to standard output to improve operations visibility and auditability.
   - Ensures any failures within database transactions are propagated correctly back to the master job calling this helper.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess

# Step 1: Initialize module-level metadata
modul_name = "alis_sqlplus"
modul_version = "V1.1.3"

# Step 2: Define starteSQLSkript helper function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Validates and executes an external SQL*Plus script.
    
    :param p_eintragsnr: Fehlereintragsnummer (error log ID)
    :param p_skript: Path to the SQL script to be run
    :param args: Dynamic arguments to pass to the SQL script
    :return: Exit status of the validation or SQL*Plus execution
    """
    # Step 3: Validate mandatory arguments are not null or empty
    if not p_eintragsnr or not p_skript:
        # # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction
        subprocess.run(
            ["DWMSG_MeldeFehler", p_eintragsnr, "E", "196", f"{modul_name} {modul_version} starteSQLSkript"],
            check=False
        )
        return 196

    # Step 4: Validate that the target script file exists and is readable
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        # # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction
        subprocess.run(
            ["DWMSG_MeldeFehler", p_eintragsnr, "E", "201", p_skript],
            check=False
        )
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Retrieve Oracle credentials/identifier from environment
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 7: Construct and invoke the SQL*Plus command
    # Emulates SQL*Plus command execution and redirects stdin from devnull
    # # REVIEW: target database platform is Oracle based on sqlplus; if migrating to BigQuery, refactor to BigQuery client calls.
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
    try:
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            check=False  # Emulates 'set +e' logic to prevent immediate Python script crash
        )
        errcode = result.returncode
    except Exception as e:
        print(f"System Error running SQL*Plus: {e}", file=sys.stderr)
        errcode = -1

    # Step 8: Return result status
    return errcode
```

### Execution order
The 6 execution steps from the legacy dependency graph map as follows in the BigQuery and Airflow target environment:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 Job) — Migrates to an Airflow DAG in Cloud Composer.
2. `d_abtn_x_smart_kubi.sql` (Core DB operations) — Converts to a BigQuery SQL script executed via `h_alis_sqlplus.py`.
3. `r_sqlscript` (Shell execution wrapper) — Migrates to a Python execution coordinator.
4. `.dw_init` (Environment setup) — Migrates to DAG-level environment variable configuration.
5. `f_alis_msgerr.ksh` (Error utility) — Migrates to `f_alis_msgerr.py` to handle logging and status updates.
6. `h_alis_sqlplus.ksh` (SQL helper - THIS FILE) — Migrates to `h_alis_sqlplus.py` to validate and execute the target BigQuery SQL scripts.

### Schedule & variables
The scheduler-set variables must be calculated at the Airflow DAG orchestration level (using Airflow task parameters or macros) and passed to the execution of `h_alis_sqlplus.py`:
- `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
- `cdate` = current date in `YYYYMMDD` format (computed using Airflow macro `{{ ds_nodash }}`)
- `cmonth` = first 6 characters of `cdate` (`YYYYMM`)
- `cday` = characters 7-8 of `cdate` (`DD`)
- `first` = `'01'`
- `cmonth` updated to `&cmonth&first` (`YYYYMM01`)
- `cmonth` updated by subtracting 1 day (resulting in the last day of the previous month)
- `cmonth` updated to the first 6 characters of the subtracted date (`YYYYMM` of the previous month)
- `MONATSID` = calculated `cmonth` representing the previous month ID in `YYYYMM` format.

### Lineage
- **Upstream / Downstream Lineage**: None found for these files. This is a helper utility library that does not read or write data tables directly, but rather wraps the orchestration and execution of other SQL scripts.

### Cross-file dependencies
- This module has a critical runtime dependency on `f_alis_msgerr.py` (migrated from `f_alis_msgerr.ksh`). It must directly import `f_alis_msgerr` to call the `DWMSG_MeldeFehler` logic.
- Sibling orchestration wrapper scripts (such as `r_sqlscript`) import and call this file's `starteSQLSkript` function.

### Target file plan
- **Target File Path**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
- **Language**: Python
- **Purpose**: BigQuery-aligned SQL execution utility replacing the legacy Oracle SQL*Plus script runner.
- **Architectural Specifications (aligned with Reviewer Feedback)**:
  - **Direct Module Import**: Do not call `DWMSG_MeldeFehler` via `subprocess.run`. Instead, import the migrated module and invoke the python function directly:
    ```python
    import f_alis_msgerr
    # Call directly:
    # f_alis_msgerr.DWMSG_MeldeFehler(p_eintragsnr, "E", "196", f"{modul_name} {modul_version} starteSQLSkript")
    ```
  - **BigQuery Client Execution**: Do not execute Oracle `sqlplus` via shell subprocesses. Instead, use the `google-cloud-bigquery` Python library to read the contents of the target BigQuery SQL script and execute it as a BQ query job:
    ```python
    from google.cloud.bigquery import Client
    # Execute query:
    # client = Client()
    # query_job = client.query(sql_content)
    # query_job.result()
    ```
  - **Preserved Print Literals**: In compliance with strict print literal translation rules, all diagnostic outputs must preserve the exact German phrasing and structure from the source code:
    - `print("Rufe SQL*PLUS auf mit folgenden Einstellungen")`
    - `print(f"Sql*Plus-Skript : {p_Skript}")`
    - `print(f"Skript-Parameter: {' '.join(args)}")`

### Environment-specific values
- `GCP_PROJECT` (GLOBAL) — Sourced at runtime via `os.environ.get("GCP_PROJECT")` or the Composer environment's default configuration.
- `BQ_LOCATION` (GLOBAL) — Sourced via `os.environ.get("BQ_LOCATION")` to define the dataset/query execution region.
- `DW_ORAUSER` (Retired / Legacy) — Oracle-specific username variable. Retired since connection to BigQuery is authenticated natively via Application Default Credentials (ADC) or the service account.

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Migrates to a Python module providing a `starteSQLSkript` function that executes SQL scripts using the native BigQuery client library and reports execution errors via direct python import of `f_alis_msgerr.py`. |

### Risks & Manual Actions
- **EXTERNAL DEPENDENCY - NOT IN SOURCE FILES**: The file `f_alis_msgerr.ksh` (which is imported as `f_alis_msgerr.py`) is outside the scope of this invocation's SOURCE FILES. It must be migrated in its own design pass to prevent runtime `ImportError` exceptions.
- **SQL SCRIPT PARAMETER SUBSTITUTION**: The legacy script passed positional parameters (`$*`) directly to SQL*Plus. Because BigQuery SQL scripts do not accept command-line positional parameters, the Python helper must implement a strategy (such as dictionary-based placeholder replacement in the read SQL string or configuring `query_parameters` in `google.cloud.bigquery.QueryJobConfig`) to safely substitute runtime parameters prior to executing the query.

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
REASON: The script is a dynamic SQL launcher containing command-line argument parsing, file path resolution logic, and complex error trapping/logging that cannot be expressed in BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic. It implements complex command-line option parsing, custom file path searching across multiple relative directories, signals/trap handling, and dynamically invokes SQL scripts via custom database utility functions.
- AWK: none
- SQL-expressible: no. While it executes SQL scripts, this script's primary role is orchestration wrapper logic, path checking, signal capturing, and logging configuration which cannot be expressed inside BigQuery SQL.
- Non-SQL side effects: file-existence checking, directory navigation, system signal traps, logging redirected to local files, and launching of external commands via helper scripts.
- Against this verdict: none. This is an orchestration utility wrapper script and must be translated into an equivalent Python utility structure to preserve its parameters, directory search logic, and error-handling framework.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script (`r_sqlscript`) is a utility wrapper designed to locate and run an Oracle SQL script within the DWH environment. It parses parameters to identify the SQL script file, resolves the script's physical location across multiple default subdirectories (`../sql`, `../mig`), establishes customized logging, and handles error signaling (`trap INT ERR`). The execution is mediated by a sourced helper script (`h_alis_sqlplus.ksh`), which executes SQL*Plus with appropriate environment parameters.

### 2. INVOCATION CONTEXT
- **Who calls this script:** Typically invoked by UC4/Automic jobs or other parent shell scripts using complete path qualification or from the directory in which it resides.
- **UC4 native includes:** None referenced in the extracted code.
- **Environment files sourced:**
  - `. $HOME/aktuell/.dw_init`
    # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
  - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables it sets are unknown; do not guess their names or values
  - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables it sets are unknown; do not guess their names or values

### 3. PARAMETERS / INPUTS
- **`-f` (`p_sqlscript`):** (Input argument, parsed via `getopts`) Name of the SQL script to be executed. Automatically cast to lowercase in legacy shell via `typeset -l p_sqlscript`. Actually used in path resolution and logging. Handled in Python via `argparse`.
- **`-i` (`p_sqlpar`):** (Input argument, parsed via `getopts`) Positional parameters passed through to the target SQL script. Handled in Python via `argparse`.
- **`-j` (`p_Job`):** (Input argument, parsed via `getopts`) Job identifier used for logging and tracking. Defaults to `"DWH_KORR"`. Automatically cast to uppercase in legacy via `typeset -u JobKennung`. Handled in Python via `argparse`.
- **`-v` (`p_Verbose`):** (Input argument, parsed via `getopts`) Verbose flag. If set, outputs the log file on error or interruption. Handled in Python via `argparse` as a boolean flag.
- **`p_Kuerzel`:** Referenced in error block `ErrArg="$p_Kuerzel"` but never declared or initialized in this script.
  # REVIEW: Parameter p_Kuerzel is referenced but not declared. Confirm if it is initialized inside .dw_init or if it is a legacy bug.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **`starteSQLSkript` (shell function from `h_alis_sqlplus.ksh`):**
  - **Verbatim Call:** `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
  - **Purpose:** Executes the SQL script via SQL*Plus, routing output to the log file.
  - **Mapping:** Remains an external process invocation via `subprocess` because its definition is inside an unsupplied environment file. It does not qualify as a RESOLVABLE LAUNCHER since database connection variables are not declared locally.
  - **Inference:** # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

### 5. EMBEDDED SQL
- None. (The script executes an external SQL file passed dynamically via parameters).

### 6. CONTROL FLOW
1. **Initialize Environments:** Source `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`.
2. **Setup Defaults:** Define `DW_EintragsNr=0` and export it.
3. **Parse Arguments:** Evaluate inputs using `getopts` for options `-f`, `-i`, `-j`, `-v`, and `-h`.
4. **Input Validation:**
   - Detect unknown parameters (`?`) or missing arguments (`:`).
   - If error (`ErrNr != 0`), report error via `DWMSG_MeldeFehler` and exit.
5. **Path Resolution:**
   - Change directory to the current script's parent directory (`dirname $0`).
   - If the database script name (`p_sqlscript`) does not contain a directory separator:
     - Check if `../sql/{p_sqlscript}` exists.
     - If not, check if `../mig/{p_sqlscript}` exists.
     - If not, fall back to `{p_sqlscript}` in the local directory.
   - If the script name *does* contain a path, use it directly as `l_DBskript`.
6. **File Existence Validation (Legacy Bug check):**
   - Check if `$l_DBskript` exists.
     # REVIEW: The legacy script sets ErrNr=198 ("Parameter value unknown") if the resolved database script file DOES exist. This appears to be a logical inversion bug in legacy code (it should likely trigger an error if the file does NOT exist). Verify intended behavior before converting.
7. **Set Job Identifier:** Convert `p_Job` or default `DWH_KORR` to uppercase.
8. **Logging Registry:**
   - Generate tracking number via `DWMSG_ErmittleNr`.
   - Resolve log filename via `DWMSG_Logdateiname`.
   - Register job execution via `DWMSG_ErzeugeEintrag`.
9. **Register Traps:** Configure error trapping for unexpected exits or signals (INT, ERR) to invoke error handler `DWMSG_Fehlerbehandlung` and dump log contents if `p_Verbose` is enabled.
10. **Execute Core Function:** Run `starteSQLSkript` with resolved paths, redirected to the log file.
11. **Post-Execution Cleanup:** Mark status as OK using `DWMSG_SetzeStatusOK`, reset signal handlers, and print completion statement.

### 7. ERROR HANDLING & EXIT CODES
- **Legacy mechanism:** `set -e` forces abort on unhandled errors. `trap` intercepts INT and ERR signals.
- **Parsing errors:**
  - Code `192`: Parameter unknown
  - Code `193`: Required argument missing
  - Code `198`: File path/parameter value validation error (Legacy logic dependent).
- **Python Mapping:** Standardize parameter extraction via `argparse`. Trap program-wide exceptions (`subprocess.CalledProcessError`, `FileNotFoundError`) inside a global try/except block. Emulate signals using Python's `signal` module or structured `try-finally` cleanup blocks.

### 8. OUTPUTS / SIDE EFFECTS
- **Log Files:** Writes tracking records and detailed command output directly into the generated `LogDatei` path.
- **Database Effects:** SQL actions inside the executed script modify tables (unspecified).

### 9. BUSINESS SUMMARY
- Standardizes execution wrappers for database scripts across DWH systems.
- Automatically resolves paths to separate operational directories (`sql`, `mig`).
- Generates registered tracking entries (`DW_EintragsNr`) in logging tables for monitoring.
- Provides consistent error handling and logs outputs to dedicated runtime file locations.

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
import os
import sys
import argparse
import subprocess
import traceback

# Step 1: Environment Sourcing (Placeholder representations)
# # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied
# # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied

# Placeholder definitions for functions defined in sourced files:
def DWMSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
    # Emulates legacy DWMSG_MeldeFehler shell command
    pass

def DWMSG_ErmittleNr():
    # Emulates DWMSG_ErmittleNr returning a tracking number
    return "12345"

def DWMSG_Logdateiname(job_kennung, eintrags_nr):
    # Emulates DWMSG_Logdateiname
    return f"/tmp/{job_kennung}_{eintrags_nr}.log"

def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, script_ident, log_file):
    # Registers entry and appends setup metadata to log_file
    pass

def DWMSG_Fehlerbehandlung(eintrags_nr):
    # Handles error logging registration
    pass

def DWMSG_SetzeStatusOK(eintrags_nr):
    # Marks job execution as OK
    pass

def starteSQLSkript(eintrags_nr, db_script, sql_par, tracking_nr, log_file):
    # # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction
    # Emulates the execution of the actual SQL command via subprocess
    cmd = ["starteSQLSkript", str(eintrags_nr), db_script, sql_par, str(tracking_nr)]
    with open(log_file, "a") as f:
        subprocess.run(cmd, check=True, stdout=f, stderr=subprocess.STDOUT)

def main():
    ProgName = f"Ausführung Script {sys.argv[0]}"
    ProgVersion = "5.0.0"
    
    DW_EintragsNr = 0
    
    # Step 2: Parse command-line parameters
    parser = argparse.ArgumentParser(description=ProgName, add_help=False)
    parser.add_argument("-f", dest="p_sqlscript", required=False)
    parser.add_argument("-i", dest="p_sqlpar", default="")
    parser.add_argument("-j", dest="p_Job", default="DWH_KORR")
    parser.add_argument("-v", dest="p_Verbose", action="store_true")
    parser.add_argument("-h", dest="p_Help", action="store_true")

    args, unknown = parser.parse_known_args()

    if args.p_Help:
        print(f"Programm: {ProgName}\nVersion: {ProgVersion}\n...")
        sys.exit(0)

    # Step 3: Argument Validation
    if unknown:
        # Legacy ErrNr 192 (unknown param)
        DWMSG_MeldeFehler(DW_EintragsNr, "E", 192, str(unknown))
        sys.exit(192)

    if not args.p_sqlscript:
        # Legacy ErrNr 193 (missing required argument -f)
        DWMSG_MeldeFehler(DW_EintragsNr, "E", 193, "-f")
        sys.exit(193)

    # Dynamic casting matching ksh 'typeset -l' / 'typeset -u'
    p_sqlscript = args.p_sqlscript.lower()
    p_sqlpar = args.p_sqlpar
    JobKennung = args.p_Job.upper()

    # Step 4: Directory navigation and path search
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(script_dir)

    l_DBskript = p_sqlscript
    if os.path.dirname(p_sqlscript) in ("", "."):
        # Relational path searching
        paths_to_test = [
            os.path.join("..", "sql", p_sqlscript),
            os.path.join("..", "mig", p_sqlscript),
            p_sqlscript
        ]
        for path in paths_to_test:
            if os.path.isfile(path):
                l_DBskript = path
                break

    # Step 5: File Validation Logic
    # # REVIEW: This conditional mirrors legacy 'if [ -f "$l_DBskript" ]' trigger. 
    # Verify why it triggers an "unknown parameter/value" error if the file IS found.
    if os.path.isfile(l_DBskript):
        p_Kuerzel = "" # # REVIEW: p_Kuerzel is referenced but not declared in legacy ksh script
        DWMSG_MeldeFehler(DW_EintragsNr, "E", 198, p_Kuerzel)
        sys.exit(198)

    # Step 6: Logging and Tracking configuration
    DW_EintragsNr = DWMSG_ErmittleNr()
    LogDatei = DWMSG_Logdateiname(JobKennung, DW_EintragsNr)
    
    # Establish logging entry
    DWMSG_ErzeugeEintrag(DW_EintragsNr, JobKennung, f"{sys.argv[0]}_{l_DBskript}", LogDatei)

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{DW_EintragsNr}'")
    print(f"Logdatei  : '{LogDatei}'")
    print("---------------------------------------------")

    # Step 7: Trap execution block (Try-Except equivalent)
    try:
        # Step 8: Invoke core database job script
        starteSQLSkript(DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr, LogDatei)
        
        # Step 9: Post-execution handling and OK notification
        DWMSG_SetzeStatusOK(DW_EintragsNr)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except Exception as e:
        # Emulating 'trap INT ERR' actions
        DWMSG_Fehlerbehandlung(DW_EintragsNr)
        
        if args.p_Verbose:
            # Emulates cat $LogDatei on error if verbose enabled
            if os.path.isfile(LogDatei):
                with open(LogDatei, 'r') as log_file:
                    print(log_file.read())
                    
        print("!FEHLER gemeldet!", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Port the KSH SQL wrapper utility into Python. It will handle argument parsing, dynamic SQL path resolution, logging setup, and orchestrate SQL executions by directly importing helper modules rather than executing subprocesses. |

---

### Key Architecture Notes & Reviewer Feedback Alignment

To fully resolve the architectural alignment issues and avoid runtime failures highlighted in the reviewer feedback, the migrated Python code must strictly follow these rules:
1. **No External Subprocess Calls for Migrated Helpers:** Do **not** use `subprocess.run`, `os.system`, or any shell execution mechanism to invoke functions from sibling utility scripts like `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh`. 
2. **Direct Module Imports:** The legacy helper scripts are migrated to their own Python modules. `r_sqlscript.py` must import these modules and call their functions directly:
   * Import the error logging mechanism: `import f_alis_msgerr` and invoke `f_alis_msgerr.DWMSG_MeldeFehler(...)` directly.
   * Import the SQL execution wrapper: `import h_alis_sqlplus` and invoke `h_alis_sqlplus.starteSQLSkript(...)` directly.
3. **No Legacy Database Drivers in Wrappers:** Ensure that the underlying database execution utilizes the designated target architecture (BigQuery python client/Dataform API calls) rather than legacy Oracle `sqlplus` or `oracledb` drivers.

---

### Execution Order
The legacy dependency graph defines a 6-step sequence. The target orchestration (Cloud Composer DAG) must preserve this order of operations:
1. **Trigger / Initialization:** `DW.DWH_ABTN_SMART_KUBI.xml` (Legacy UC4 workflow trigger).
2. **SQL Preparation:** `d_abtn_x_smart_kubi.sql` (Aggregates and loads contract/tariff/fact data into target tables).
3. **Wrapper Execution (This Component):** `r_sqlscript` (Executes the preparation script using parameters).
4. **Environment Initialization:** `.dw_init` (Sourced dynamically during step execution).
5. **Error Logging Registration:** `f_alis_msgerr.ksh` (Invoked by the wrapper for error handling).
6. **SQL Engine Client Execution:** `h_alis_sqlplus.ksh` (Invoked by the wrapper to run the actual SQL queries).

*Note: Sibling files (1, 2, 4, 5, 6) belong to separate design passes or groups. This pass defines how `r_sqlscript.py` interfaces within this order.*

---

### Schedule & Variables

The following scheduler-set variables must be passed into the migrated job. In Cloud Composer, these can be mapped as DAG parameters (`params`) or dynamic variables:

#### Retained Variables
* **`DWH_JOB_KENNUNG`**: String value `'ABTN_SMART_KUBI'`
* **`cdate`**: Dynamic date calculation representing `'SYS_DATE("YYYYMMDD")'`. (In Airflow, this maps to `{{ ds_nodash }}`).
* **`cmonth`**: Derived as `SUBSTR(&cdate,1,6)`.
* **`cday`**: Derived as `SUBSTR(&cdate,7,2)`.
* **`first`**: Constant value `'01'`.
* **`MONATSID`**: Derived value calculated by taking the first day of the current month, subtracting one day, and extracting the first 6 characters (e.g. if `cdate` is `20260821`, `MONATSID` is resolved to `202607`). 

These variables must be passed to `r_sqlscript.py` as command-line arguments (e.g. `-i "MONATSID=202607"`) or injected directly via environment variables.

---

### Lineage
The legacy lineage graph details the following relationships involving our source wrapper:
* **`r_sqlscript` —[INVOKES]→ `f_alis_msgerr.ksh`**: In the target environment, this dependency must be resolved by having `r_sqlscript.py` perform a standard Python import: `import f_alis_msgerr`.
* **`r_sqlscript` —[INVOKES]→ `h_alis_sqlplus.ksh`**: In the target environment, this dependency must be resolved by having `r_sqlscript.py` perform a standard Python import: `import h_alis_sqlplus`.
* **`r_sqlscript` —[USES_CONFIG]→ `.dw_init`**: Sourced variables must be fetched from environment variables or a shared configuration file instead of dynamically sourcing shell scripts.

---

### Cross-File Dependencies
* **Helper Modules:** `r_sqlscript.py` relies directly on the existence of `f_alis_msgerr.py` and `h_alis_sqlplus.py`. The build and packaging stage must ensure these modules reside in the Python search path (e.g., in the `plugins` or `dags/dependencies` folder of Cloud Composer).
* **Target Schema:** The underlying SQL scripts executed by this wrapper target `DWH$TA_T_SMART_KUBI`, joining with `BL_D_TARIF`, `DWH$TA_F_D1_TWVV_TN`, and the view `DWH$VI_L_MAP_FA_TARIF`.

---

### Target File Plan

| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript.py` | Python | `local/home/gurunathan_t/kubi/r_sqlscript` | Receives parameters via `argparse`, resolves paths for target BigQuery SQL scripts, sets up status logging, and triggers execution. |

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
These values identify the infrastructure and must be sourced dynamically at runtime.
* **`DW_DIR_ROOT`**: Sourced via `os.environ.get("DW_DIR_ROOT")` or `Variable.get("DW_DIR_ROOT")`. Defines the root directory of the source/target folder tree.
* **`HOME`**: Sourced via `os.environ.get("HOME")`.
* **`GCP_PROJECT`**: Sourced via `os.environ.get("GCP_PROJECT")` or `Variable.get("GCP_PROJECT")`.
* **`GCS_BUCKET`**: Sourced via `Variable.get("GCS_BUCKET")`. (Used to hold temporary execution files or log outputs).

#### 2. JOB-SPECIFIC
These values are particular to this execution unit and are populated directly or via command-line arguments.
* **`p_Job` / `JobKennung`**: The unique job identifier. Set to `"DWH_KORR"` as a default, or overridden via the `-j` parameter.
* **`p_sqlscript` / `l_DBskript`**: The path/name of the target database execution script, supplied via the `-f` parameter.
* **`p_sqlpar`**: Positional or named parameters passed downstream to the SQL execution engine, supplied via the `-i` parameter.

---

### Risks & Manual Actions
* **Verification of File Finding Logic:** The legacy script contains a condition `if [ -f "$l_DBskript" ]` which raises an `ErrNr=198` ("Parameter value unknown") if the file **does** exist. This appears to be an inverted logic bug in the original shell code (which likely meant to check if the file did *not* exist). A human reviewer must verify the intended behavior of this validation check during deployment.
* **Logging Integration:** Local file-based log trapping (`trap INT ERR` redirected to a local text file) must be integrated with BigQuery / Cloud Logging to ensure that logs from python and BigQuery execution are centralized and discoverable.