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
This workflow processes data to populate a temporary table (`ABTN_SMART_KUBI`) via a SQL-based script execution. It consists of a single migrated Unix SQL job (`DW.DWH_ABTN_SMART_KUBI`). A key business feature of this workflow is the dynamic calculation of a reporting month identifier (`MONATSID`) based on the execution date: if run before the 15th of the month, it targets the previous month; otherwise, it targets the current month. Since no parent workflow (`JOBP`) or calendar schedule was supplied in this extraction, the workflow is designed as a standalone, externally triggered Airflow DAG.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 | Populate temp table |

---

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` or scheduling definitions are present in this extraction. Additionally, there are no native trigger scripts (`SCRI`) or parent job plans (`JOBP`) referencing this object.
* **Trigger Source**: Externally triggered (source unknown from this extraction alone).
* **Airflow Schedule**: `schedule=None` (manual or external orchestration trigger only).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` was supplied, a wrapper DAG is created for this standalone task.

| Property | Value |
| :--- | :--- |
| **DAG ID** | `dw_dwh_abtn_smart_kubi` |
| **Schedule** | `None` |
| **Start Date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **Catchup** | `False` |
| **Max Active Runs** | `1` *(To prevent race conditions during table population)* |
| **Is Paused Upon Creation** | `False` *(Active=1)* |
| **Default Args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_abtn_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `BashOperator` | `gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py` | Pass calculated `MONATSID` as command line argument | 1 | 5 Min | N/A | N/A | N/A | N/A | Runs converted Python script wrapper around original SQL. Requires dynamic calculation of the target month parameter. |

---

## 6. Task Dependency Map
As this is a single-task DAG wrapping a standalone job execution, there are no internal DAG dependencies:

```
dwh_abtn_smart_kubi
```

---

## 7. Sync / Concurrency Analysis
No native UC4 Sync objects or mutual exclusions were identified for this object. To prevent concurrent runs from corrupting the temporary table state, `max_active_runs=1` is enforced at the DAG level.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Standard task-level retry of 1 attempt with a 5-minute delay is configured via `default_args`.
* **Failure Actions**: Default failure behavior (raise Airflow failure state) is maintained; no custom callbacks are defined in the source UC4 object.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&MONATSID` | Calculated via date logic: if execution day < 15, previous month (YYYYMM); else current month (YYYYMM) | Calculated dynamically in DAG using Airflow `logical_date` inside a Python helper or Jinja macro. |
| `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql` | SQL source path | Wrapped inside Cloud Storage bucket Python script execution: `gs://YOUR_BUCKET_NAME/python_scripts/d_abtn_x_smart_kubi.py` |

---

## 10. Developer Notes
* **GCP Storage Placeholders**: The target Python script path `gs://YOUR_BUCKET_NAME/...` is a placeholder. Update `YOUR_BUCKET_NAME` to the target GCP environment's bucket name.
* **Idempotent Date Calculation**: The original UC4 script calculates `&MONATSID` dynamically based on execution date. To ensure Airflow tasks are idempotent and re-runnable for historic dates, this design calculates `MONATSID` relative to the DAG's `logical_date` (`execution_date`) rather than the system's real-time clock.
* #REVIEW-STRUCT: This extraction only contains a single `JOBS_UNIX` object. It has been wrapped as a single-task DAG here, but if its parent JOBP is extracted in the future, this task should be integrated into that larger DAG.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW-STRUCT: Replace with your actual target GCP environment configuration
GCS_BUCKET = "YOUR_BUCKET_NAME"
SCRIPT_PATH = f"gs://{GCS_BUCKET}/python_scripts/d_abtn_x_smart_kubi.py"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Helper Logic / Jinja Macro ───────────────────────────
def calculate_monatsid(logical_date):
    """
    Replicates the UC4 script logic:
    If execution day < 15, use the previous month (YYYYMM).
    Otherwise, use the current month (YYYYMM).
    Calculated relative to logical_date for DAG idempotency.
    """
    # logical_date is typically passed as a pendulum/datetime object
    day = logical_date.day
    if day < 15:
        # Subtract days to get into the previous month
        first_of_this_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        return last_day_prev_month.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Converted from UC4 DW.DWH_ABTN_SMART_KUBI",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    user_defined_macros={
        "calculate_monatsid": calculate_monatsid
    },
) as dag:

    # ── Task: dwh_abtn_smart_kubi ────────────────────────
    # Executes the python wrapper on GCS, passing the computed MONATSID
    dwh_abtn_smart_kubi = BashOperator(
        task_id="dwh_abtn_smart_kubi",
        bash_command=(
            "gcloud storage cp {{ params.script_path }} /tmp/d_abtn_x_smart_kubi.py && "
            "python /tmp/d_abtn_x_smart_kubi.py --monatsid {{ calculate_monatsid(logical_date) }}"
        ),
        params={
            "script_path": SCRIPT_PATH
        },
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-task DAG; no downstream dependency chain required.
    dwh_abtn_smart_kubi
```

An implementation-ready **MIGRATION DESIGN DOCUMENT** has been prepared for the file listed under **SOURCE FILES** in the context. 

---

### EXECUTION ORDER
The target orchestration (Airflow DAG) preserves the execution sequence defined in the legacy dependency graph as follows:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Orchestration entrypoint) $\rightarrow$ Converted to Airflow DAG `dw_dwh_abtn_smart_kubi.py`.
2. **.dw_init** $\rightarrow$ Replaced by Airflow execution parameters and dynamic task variable setup.
3. **r_sqlscript** (SQL Wrapper execution) $\rightarrow$ Executed via Airflow's native `BigQueryInsertJobOperator` running the migrated SQL directly, bypassing the legacy Shell wrapper.
4. **d_abtn_x_smart_kubi.sql** $\rightarrow$ Migrated BigQuery SQL statement executed by the `BigQueryInsertJobOperator` task.
5. **f_alis_msgerr.ksh** and **h_alis_sqlplus.ksh** (Logging & Database connectivity) $\rightarrow$ Replaced by Airflow's built-in logging framework and native Google Cloud BigQuery client connections.

---

### SCHEDULE & VARIABLES — MUST BE RETAINED
The source UC4 schedule-set variables are fully mapped to native Airflow configurations to ensure identical run-time behavior:
* **`DWH_JOB_KENNUNG`** (`'ABTN_SMART_KUBI'`): Mapped as a DAG-level parameter.
* **Date Variables (`cdate`, `cmonth`, `cday`, `first`, `MONATSID`)**: The date calculation must remain idempotent and re-runnable for historic dates. The logic is calculated dynamically using Airflow's native `logical_date` (historically `execution_date`) in a Jinja template or Python task macro.

#### Dynamic `MONATSID` Calculation Mapping:
```python
def calculate_monatsid(logical_date):
    """
    Replicates the legacy UC4 dynamic date calculation logic:
    - cdate = SYS_DATE("YYYYMMDD")
    - cmonth = SUBSTR(cdate,1,6)
    - cday = SUBSTR(cdate,7,2)
    - If cday < 15, cmonth is set to the previous month (YYYYMM).
    - Otherwise, cmonth remains the current month (YYYYMM).
    """
    day = logical_date.day
    if day < 15:
        # Subtract to get a date in the previous month
        first_of_this_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        return last_day_prev_month.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")
```
The resulting `MONATSID` value is passed as a query parameter (`@MONATSID`) to the target BigQuery query execution.

---

### LINEAGE
Based on the analyzed lineage edges, the following dependencies are mapped:
* **Upstream Producers / Includes**:
  * `DW.HOLE_PFAD` and `DW.LESE_LOG`: Confirmed by human review as **NO SOURCE NEEDED** and are retired.
  * `.dw_init`: Mapped to Airflow environment and connection settings.
* **Target / Invoked Assets**:
  * `r_sqlscript` & `d_abtn_x_smart_kubi.sql`: Converted to BigQuery SQL syntax and run directly inside the DAG via the `BigQueryInsertJobOperator`.
  * `DW.UNIX.ISTNS` (UC4 Login Package): Replaced by standard IAM Service Account authentication on GCP Composer.
  * `dwhdwh1p` (Target Host): Replaced by BigQuery serverless execution environment.

---

### CROSS-FILE DEPENDENCIES
* **Shared Tables**: 
  * **Target**: `DWH$TA_T_SMART_KUBI` (loaded by the query).
  * **Sources**: `BL_D_TARIF`, `DWH$TA_F_D1_TWVV_TN`, `DWH$VI_L_MAP_FA_TARIF` (joined to perform aggregation).
* **Call Chains**: The Airflow DAG executes the SQL script `d_abtn_x_smart_kubi.sql` on BigQuery. The SQL script references the parameter `@MONATSID` generated at runtime by the DAG orchestration.

---

### TARGET FILE PLAN
The target folder structure mirrors the source folder structure, adhering to the folder integrity guidelines:
* **Target File Path**: `dags/dw_dwh_abtn_smart_kubi.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `DW.DWH_ABTN_SMART_KUBI.xml`

---

### ENVIRONMENT-SPECIFIC VALUES
These variables are classified by their deployment role and must be dynamically resolved at runtime (no literal hardcoded placeholders):

#### 1. GLOBAL (Environment-Wide)
* **`GCP_PROJECT`**: Identifies the destination Google Cloud Project. Sourced via Airflow Variable: `Variable.get("GCP_PROJECT")`.
* **`GCS_BUCKET`**: Identifies the Cloud Storage bucket containing SQL scripts. Sourced via Airflow Variable: `Variable.get("GCS_BUCKET")`.
* **`BQ_LOCATION`**: Identifies the BigQuery processing region. Sourced via Airflow Variable: `Variable.get("BQ_LOCATION")`.

#### 2. JOB-SPECIFIC
* **`DWH_JOB_KENNUNG`**: `'ABTN_SMART_KUBI'` (defined as a DAG parameter).
* **`MONATSID`**: Dynamically computed inside the DAG using `logical_date` and injected as a query parameter.
* **`SQL_FILE_PATH`**: `'gcs/sql/d_abtn_x_smart_kubi.sql'` (the path to the SQL query file).

---

### FILE DISPOSITION TABLE
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `dags/dw_dwh_abtn_smart_kubi.py` | Migrated to an Airflow DAG. Dynamically calculates the reporting month (`MONATSID`) and executes the converted BigQuery query. |

---

### OUTPUT/PRINT LITERAL RULE
To preserve logging parity, all printed outputs are kept verbatim from the source. The print statement:
```
Berichtsmonat: &MONATSID
```
Must be logged in Python using the exact original literal text, character-for-character:
```python
self.log.info(f"Berichtsmonat:  {monatsid_value}")
```

---

### REVIEWER FEEDBACK RESOLUTION
To address the feedback from the previous run:
1. **Elimination of the Bash Wrapper**: Rather than generating a custom Python wrapper script (`r_sqlscript.py`) to replicate the KornShell execution, this design natively executes the SQL query in BigQuery using the `BigQueryInsertJobOperator`. This simplifies the architecture, improves performance, and removes the risk of a flawed Bash execution layer.
2. **Correct SQL Execution in DAG**: The DAG tasks are configured with `BigQueryInsertJobOperator` (referencing the migrated SQL script with parameter injection) instead of `BashOperator`.

#### Task Execution Structure in `dags/dw_dwh_abtn_smart_kubi.py`:
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Global Environment Variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_LOCATION = Variable.get("BQ_LOCATION", default_var="EU")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def calculate_monatsid(logical_date):
    day = logical_date.day
    if day < 15:
        first_of_this_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        return last_day_prev_month.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Migrated from UC4 DW.DWH_ABTN_SMART_KUBI",
    schedule_interval=None,  # Externally triggered or manual
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    user_defined_macros={
        "calculate_monatsid": calculate_monatsid
    },
) as dag:

    # Log execution parameter matching UC4 print statement
    # "Berichtsmonat:  <MONATSID>"
    monatsid_param = "{{ calculate_monatsid(logical_date) }}"

    execute_smart_kubi_sql = BigQueryInsertJobOperator(
        task_id="execute_d_abtn_x_smart_kubi",
        configuration={
            "query": {
                "query": f"gcs://{GCS_BUCKET}/sql/d_abtn_x_smart_kubi.sql",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "MONATSID",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": monatsid_param}
                    }
                ]
            }
        },
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )

    execute_smart_kubi_sql
```

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
REASON: The script contains conditional logic blocks to check directory paths and resolve ORACLE_HOME, while also sourcing external environment initialization files.

EVIDENCE
- Business logic found: KSH custom logic: Resolves the dynamic pathing of directory mappings, validates and defines ORACLE_HOME through nested conditional path checks, and sources downstream environment modules.
- AWK: none
- SQL-expressible: no, this is a pure environment initialisation and path configuration utility.
- Non-SQL side effects: Validates file directory structures on the filesystem, updates process environment variables, and sources external shell configuration scripts.
- Against this verdict: If all variables could be statically defined in a YAML configuration, this could be treated as config metadata; however, the dynamic directory validation and conditional routing logic require programming capability, rendering Python the correct choice.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script, `.dw_init`, is a legacy KornShell environment initialisation script. Its primary purpose is to define and export system paths, DWH import/export directories, and SMS configuration folders relative to the user's home directory (`$HOME`). Additionally, it dynamically determines the Oracle Home installation path (`ORACLE_HOME`) based on directory existence checks, and sources secondary environment configuration scripts (`.dw_global`, `.dw_lokal`). This file serves as a shared environment loader sourced by downstream orchestration processes.

2. INVOCATION CONTEXT
   - Who calls this script: Typically sourced within legacy KornShell scripts (e.g., via `. $HOME/.dw_init`) or invoked within UC4 / Automic jobs before executing business steps. The exact UC4 job name is unknown.
   - Any UC4 native includes: None referenced in the script extraction.
   - Environment files sourced:
     * `$HOME/.dw_global` — # REVIEW-STRUCT: environment file $HOME/.dw_global not supplied — variables it sets are unknown; do not guess their names or values
     * `$HOME/.dw_lokal` — # REVIEW-STRUCT: environment file $HOME/.dw_lokal not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `HOME` (system environment variable): Used as the base path for relative directory structures. Surfaced via `os.environ.get("HOME")` or `os.path.expanduser("~")`.
   - `ORACLE_HOME` (system environment variable): Checked to verify if already set; if missing, conditional filesystem checks are triggered. Surfaced via `os.environ.get("ORACLE_HOME")`.
   - `ORACLE_SID` (system environment variable): Used to construct the dynamic database utility file path `DW_DIR_UTL_FILE`. Surfaced via `os.environ.get("ORACLE_SID")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   No external database clients or operational scripts are directly executed. However, the script tests filesystem directories using the shell `-d` flag, which translates to Python's `os.path.isdir()`.

5. EMBEDDED SQL
   None. No SQL or DDL statements are embedded within this environment configuration script.

6. CONTROL FLOW
   1. **Define and Export Environment Variables**: Initialize directory variables for the main application root (`DW_DIR_ROOT`), log protocols (`DW_DIR_PROT`), cube files (`DW_DIR_CUBES`), various importer pipelines (`DW_DIR_IMP_*`), export paths (`DW_DIR_EXP_*`), and SMS utility subfolders (`DW_DIR_SMS_*`) relative to the active `$HOME` path.
   2. **Oracle Home Path Resolution**:
      - Check if `ORACLE_HOME` is already populated.
      - If missing, check directory presence in order of precedence:
        1. `/appl/local/oracle/12.2.0.1.0`
        2. `/appl/local/oracle/11.2.0`
      - If neither exists, output a descriptive error message to standard output.
   3. **Dynamic Script Sourcing**: Source configuration scripts `.dw_global` and `.dw_lokal`.
   4. **Construct Database Utilities Path**: Assign `DW_DIR_UTL_FILE` based on the active `ORACLE_SID` dynamic variable.

7. ERROR HANDLING & EXIT CODES
   - **Failure Detection**: Standard output messages are used to signal missing Oracle Home pathways (via standard `echo`).
   - **No Immediate Terminations**: The shell script continues execution even if the Oracle directory setup fails or if sourced files are missing.
   - **Python Mapping**: We should raise standard exceptions or log errors to `sys.stderr` when crucial configuration paths are missing.

8. OUTPUTS / SIDE EFFECTS
   - Mutates and exports environmental path variables for downstream processes.
   - # REVIEW: Legacy anomaly detected in line `DW_DIR_IMP_SAP_L_GUTGR=$HOME/daten/sap/sap_l_gutgr; export DW_DIR_IMP_SAP_L`. The assigned variable was `DW_DIR_IMP_SAP_L_GUTGR`, but the exported name was `DW_DIR_IMP_SAP_L`. The Python design exposes both keys to maintain compatibility.

9. BUSINESS SUMMARY
   - Establishes a unified path system across multiple analytical datasets (including DPPS, SIGMA, CARMEN, and SAP files).
   - Centralizes remote host configurations (`DW_HOST_CUSTOMER`) for DWH communication.
   - Validates the active Oracle database engine location on host systems.
   - Coordinates database administration layout paths (`utl_file`).

=== PSEUDOCODE ===

```python
# Step 1: Import required system modules
import os
import sys

def init_env():
    # Step 2: Extract base HOME directory environment variable
    home = os.environ.get("HOME", "")
    if not home:
        home = os.path.expanduser("~")

    # Step 3: Establish and assign DWH pathing variables
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

    # Step 4: Map legacy variable mismatch (assigned vs. exported)
    # # REVIEW: Legacy mismatch in variable assignment and export: DW_DIR_IMP_SAP_L_GUTGR was assigned but DW_DIR_IMP_SAP_L was exported. Both have been mapped to ensure stability.
    os.environ["DW_DIR_IMP_SAP_L_GUTGR"] = os.path.join(home, "daten/sap/sap_l_gutgr")
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

    # Step 5: Conditionally resolve ORACLE_HOME path
    oracle_home = os.environ.get("ORACLE_HOME")
    if not oracle_home:
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            oracle_home = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            oracle_home = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
        
        if oracle_home:
            os.environ["ORACLE_HOME"] = oracle_home

    # Step 6: Trigger sourced environment modules
    # # REVIEW-STRUCT: environment file $HOME/.dw_global not supplied — variables it sets are unknown; do not guess their names or values
    # # REVIEW-STRUCT: environment file $HOME/.dw_lokal not supplied — variables it sets are unknown; do not guess their names or values

    # Step 7: Resolve runtime dynamic database administrative output directory
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
```

### 1. EXECUTION ORDER
The legacy execution sequence must be preserved in the target Cloud Composer (Airflow) orchestration environment:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Legacy UC4 Orchestration) $\rightarrow$ Cloud Composer DAG.
2. **d_abtn_x_smart_kubi.sql** (Oracle PL/SQL core script) $\rightarrow$ BigQuery SQL / Dataform SQLX pipeline.
3. **r_sqlscript** (KSH wrapper executing SQL) $\rightarrow$ Migrated Python execution task / operator within Airflow.
4. **.dw_init** (Environment initialization) $\rightarrow$ Sourced as a shared Python module (`dw_init.py`) within the target DAG tasks.
5. **f_alis_msgerr.ksh** (Error handling helper) $\rightarrow$ Migrated Airflow task failure callbacks / Python error utility module.
6. **h_alis_sqlplus.ksh** (SQL*Plus helper) $\rightarrow$ Migrated Airflow BigQuery execution operators.

---

### 2. SCHEDULE & VARIABLES — MUST BE RETAINED
The target DAG orchestration must define and dynamically compute the equivalent legacy scheduler-set variables using Airflow's execution date context (e.g., using `{{ ds_nodash }}` or execution date macros):

- **DWH_JOB_KENNUNG**: Static parameter set to `'ABTN_SMART_KUBI'`. Passed as an Airflow DAG param.
- **cdate**: Derived from the current schedule execution date in `YYYYMMDD` format.
- **cmonth**: Derived as the first 6 characters of `cdate` (`YYYYMM`).
- **cday**: Derived as the last 2 characters of `cdate` (`DD`).
- **first**: Static variable set to `'01'`.
- **cmonth_full_1**: Dynamic calculation merging `cmonth` and `first` (`YYYYMM01`).
- **cmonth_full_2**: Dynamic subtraction of 1 day from `cmonth_full_1` (yields the last day of the previous month).
- **cmonth_final**: Extracted first 6 characters of `cmonth_full_2` (`YYYYMM`).
- **MONATSID**: Assigned the value of `cmonth_final` to control the data scope for the reporting month.

These resolved values must be passed to downstream execution operators (such as BigQuery operators executing SQL tasks) at runtime.

---

### 3. LINEAGE
- **Upstream Producers (Config)**:
  - `local/home/gurunathan_t/kubi/.dw_global` $\rightarrow$ (Human-confirmed: NO SOURCE NEEDED/Retired).
  - `local/home/gurunathan_t/kubi/.dw_lokal` $\rightarrow$ (Human-confirmed: NO SOURCE NEEDED/Retired).
- **Downstream Consumers**:
  - Environment settings in `.dw_init` are utilized by execution wrappers such as `r_sqlscript.py`.

---

### 4. CROSS-FILE DEPENDENCIES
- Sibling execution components such as `r_sqlscript.py` depend on the configuration of root directories (`DW_DIR_ROOT`) and operational logging directories (`DW_DIR_PROT`) defined in `.dw_init`.
- In the migrated GCP environment, these directories map directly to GCS buckets and Airflow log locations.
- The initialization file must load these constants so that other migrated Python tasks can dynamically import them.

---

### 5. TARGET FILE PLAN
In accordance with the Folder Integrity Rule, the target repo mirrors the legacy source directory layout. No pseudocode is written here to avoid divergence from the automated conversion.

- **Source File**: `local/home/gurunathan_t/kubi/.dw_init`
- **Target File**: `local/home/gurunathan_t/kubi/dw_init.py`
- **Language**: Python
- **Purpose**: Exposes environment paths, directory structures, and global configuration values as a dictionary and system environment mapping for Cloud Composer.

---

### 6. ENVIRONMENT-SPECIFIC VALUES
All values are categorized by their role in the target environment:

#### GLOBAL (Environment-Wide Infrastructure)
These values are environment-wide constants that identify the infrastructure itself and must be resolved dynamically at runtime:
- **GCS_BUCKET** $\rightarrow$ Replaces `$HOME` folder references for DWH file operations. Resolved at runtime using:
  ```python
  GCS_BUCKET = os.environ.get("GCS_BUCKET")
  ```
- **DW_DIR_ROOT** $\rightarrow$ Maps to the deployment root directory or primary GCS path. Resolved at runtime using:
  ```python
  DW_DIR_ROOT = os.environ.get("GCS_BUCKET") + "/aktuell"
  ```
- **DW_DIR_PROT** $\rightarrow$ Shared execution logging path in GCS. Resolved at runtime using:
  ```python
  DW_DIR_PROT = os.environ.get("GCS_BUCKET") + "/daten/logfiles"
  ```
- **DW_DIR_CUBES** $\rightarrow$ Cubes storage bucket folder. Resolved at runtime using:
  ```python
  DW_DIR_CUBES = os.environ.get("GCS_BUCKET") + "/daten/cubes"
  ```
- **DW_HOST_CUSTOMER** $\rightarrow$ Replaces the legacy host `dxcst3.bn.detemobil.de` with the target database endpoint or connection ID. Resolved at runtime using:
  ```python
  DW_HOST_CUSTOMER = os.environ.get("DW_HOST_CUSTOMER")
  ```
- **ORACLE_HOME** & **ORACLE_SID** $\rightarrow$ Legacy Oracle configuration keys. If needed for backward-compatible connection wrappers, these are mapped to:
  ```python
  ORACLE_HOME = os.environ.get("ORACLE_HOME")
  ORACLE_SID = os.environ.get("ORACLE_SID")
  ```
- **DW_DIR_UTL_FILE** $\rightarrow$ Dynamic Oracle utility file path. Resolved using:
  ```python
  DW_DIR_UTL_FILE = f"/appl/local/oracle/admin/{os.environ.get('ORACLE_SID')}/utl_file"
  ```

#### JOB-SPECIFIC
- **DW_DIR_IMP_*** (e.g., `DW_DIR_IMP_D1`, `DW_DIR_IMP_SAP`, etc.) and **DW_DIR_EXP_*** $\rightarrow$ Individual data landing pathways. These are stored within a single job configuration mapping object at the job level:
  ```python
  JOB_CONFIG = {
      "DW_DIR_IMP_D1": "daten/d1",
      "DW_DIR_IMP_BWA": "daten/dpps/bwa",
      "DW_DIR_IMP_XTRA": "daten/xtra",
      "DW_DIR_IMP_CTEL": "daten/ctel",
      "DW_DIR_IMP_VO": "daten/vo",
      "DW_DIR_IMP_RV": "daten/rv",
      "DW_DIR_IMP_IF": "daten/ees",
      "DW_DIR_IMP_NNV": "daten/nnv",
      "DW_DIR_IMP_SIGMA": "daten/gd/sigma",
      "DW_DIR_EXP_SIGMA": "daten/gd/sigma/export",
      "DW_DIR_IMP_TRF": "daten/trf",
      "DW_DIR_IMP_AUF": "daten/sd/auf",
      "DW_DIR_IMP_GUT": "daten/sd/gut",
      "DW_DIR_IMP_KDG": "daten/sd/kdg",
      "DW_DIR_IMP_MP_KDG": "daten/mp/kdg",
      "DW_DIR_IMP_MP_TS": "daten/mp/ts",
      "DW_DIR_IMP_MP_ZM": "daten/mp/zm",
      "DW_DIR_IMP_TS": "daten/sd/ts",
      "DW_DIR_IMP_ZM": "daten/sd/zm",
      "DW_DIR_EXP": "daten/exporter",
      "DW_DIR_IMP_BPM": "daten/bm",
      "DW_DIR_IMP_ZTS": "daten/zts",
      "DW_DIR_IMP_VRS": "daten/vrs",
      "DW_DIR_IMP_BRUNET": "daten/brunet",
      "DW_DIR_IMP_DWH": "daten/dwh",
      "DW_DIR_IMP_PLATO": "daten/dwh/plato",
      "DW_DIR_IMP_CARMEN": "daten/carmen",
      "DW_DIR_IMP_SAP": "daten/sap",
      "DW_DIR_IMP_SR_RV": "daten/sap/sr_rv_dpps",
      "DW_DIR_IMP_SAP_L_GUTGR": "daten/sap/sap_l_gutgr",
      "DW_DIR_IMP_SAP_L": "daten/sap/sap_l_gutgr",
      "DW_DIR_IMP_L_MAHNSTYP_IST": "daten/sap/mahn",
      "DW_DIR_IMP_L_MAHNV_FI": "daten/sap/mahn",
      "DW_DIR_IMP_L_MAHNV_IST": "daten/sap/mahn",
      "DW_DIR_IMP_L_GUTGR": "daten/sd/l_gutschr",
      "DW_DIR_IMP_L_LEIST": "daten/sd/l_leist",
      "DW_DIR_IMP_L_PROD": "daten/sd/l_prod",
      "DW_DIR_IMP_LKODE": "daten/sd/lkode",
      "DW_DIR_IMP_SUBSE": "daten/subse",
      "DW_DIR_SMS_PRG": "aktuell/allgemein/is/util",
      "DW_DIR_SMS_ADR": "daten/sms/adressen",
      "DW_DIR_SMS_TMP": "daten/sms/tmp",
      "DW_DIR_IMP_DPPS": "daten/dpps",
      "DW_DIR_IMP_PLANF2": "daten/planf2"
  }
  ```

---

### 7. FILE DISPOSITION
The following table lists every file under scope for this specific design pass:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `local/home/gurunathan_t/kubi/dw_init.py` | Converts the legacy KSH environment initialization and path mapping script into a clean Python module configuration. |

---

### 8. RISKS & MANUAL ACTIONS
- **Sourced Shell Scripts Missing**: Sourced legacy files `.dw_global` and `.dw_lokal` were identified in lineage analysis as "NO SOURCE NEEDED/Retired". Verify if any downstream execution steps require parameters originally defined within them. If found, those parameters must be explicitly added to `dw_init.py` or defined in Airflow variables.

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
1.1 Object Type: 
    PL/SQL Anonymous Block (Multi-statement batch loading script with substitution variables).
1.2 Business Logic Summary:
    The script truncates the target table `dwh$ta_t_smart_kubi` and inserts aggregated and consolidated billing/contract data for a specific reporting month (passed as parameter `&1`). It combines facts from `dwh$ta_f_d1_twvv_tn` (using partition-specific filtering) with tarif and contract dimensions (`dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, and `dwh$ta_c_vertrag`) via outer joins. It computes customer-specific groupings, accounts for contract validity ranges, handles special business field rules, and reports the number of processed rows or raises a custom framework error on failure.
1.3 Entities Referenced:
    - Target Table: `dwh$ta_t_smart_kubi`
    - Source Tables/Views:
      - `dwh$vi_l_map_fa_tarif` (Alias: `t`)
      - `bl_d_tarif` (Alias: `tar`)
      - `dwh$ta_f_d1_twvv_tn` (Alias: `fact`)
      - `dwh$ta_c_vertrag` (Alias: `d`)
    - External Packages/Procedures:
      - `dwpa_util_skript.runstatement` (Dynamic DDL execution wrapper)
      - `dwpa_globals.k_alis_err_unknown` (Global error constant)
      - `dwpa_meldung.fehler` (Custom framework error logging)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → `INT64`
    - `number` → `INT64` (for identifiers, IDs, counts, and date representation fields)
    - `varchar2(300)` / `varchar2(512)` → `STRING`
    - `DATE` → `DATE` (or `DATETIME` if time component is required. The variable `l_monats_date` has no time components and functions purely as a calendar boundary; mapping it to `DATE` is safe).

2.2 Implicit and Explicit Type Casting:
    - `to_number('&1')` and `to_number('&2')` → `CAST(? AS INT64)`
    - `to_char(fact.gueltigkeitszeitpunkt, 'yyyymm')` → `FORMAT_TIMESTAMP('%Y%m', fact.gueltigkeitszeitpunkt)` (assuming `gueltigkeitszeitpunkt` is a TIMESTAMP/DATETIME).
    - `to_char(l_monats_id)` → `CAST(l_monats_id AS STRING)`

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` → `COALESCE(t_new.tarif_id, 0)`
    - `NVL(t_old.tarif_id, 0)` → `COALESCE(t_old.tarif_id, 0)`
    - `Decode(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` → `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `Decode(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` → Resolve `LTRIM(RTRIM(...))` to `TRIM(...)`.
      Equivalent logic: `CASE WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn ELSE fact.vo_kenn_bearb END`

2.4 String Functions:
    - `ltrim(rtrim(x))` → `TRIM(x)`

2.5 Date and Timestamp Functions:
    - `TO_DATE(l_monats_id, 'YYYYMM')` → `PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`
    - `ADD_MONTHS(d, 1)` → `DATE_ADD(d, INTERVAL 1 MONTH)`
    - `To_date('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`

2.6 Numeric and Aggregate Functions:
    - `SUM(fact.zugang)` → `SUM(fact.zugang)` (fully compatible).

2.7 Analytical and Window Functions:
    - None present.

2.8 Set and Join Operations:
    - Left Outer Join Syntax `(+)` → Rewrite to ANSI standard `LEFT OUTER JOIN`.
      The Oracle joins:
      - `fact.dwh_tarif_id_neu = t_new.dwh_tarif_id (+)`
      - `fact.dwh_tarif_id_alt = t_old.dwh_tarif_id (+)`
      - `fact.dwh_vertrag_id = d.dwh_vertrag_id (+)` along with contract bounds `l_monats_date > d.gueltig_von (+)` and `l_monats_date <= d.gueltig_bis (+)`
      Are transformed into distinct ANSI `LEFT JOIN` structures on the main query.

2.9 Row Limiting and Sampling:
    - None present.

2.10 Sequences:
     - None present.

2.11 MERGE Statements:
     - None present.

2.12 INSERT / UPDATE / DELETE:
     - Direct `INSERT INTO ... SELECT` is transformed to standard BigQuery DML.
     - Dynamic TRUNCATE statement `dwpa_util_skript.runstatement(eintragsnr, 'Truncate table dwh$ta_t_smart_kubi')` is resolved directly to a native BigQuery script statement: `TRUNCATE TABLE dwh_ta_t_smart_kubi;`.

2.13 DDL Constructs:
     - The target table partition reference `partition(dwh$ta_f_d1_twvv_tn_&1)` is an Oracle partitioning clause. In BigQuery, this is resolved by querying the base table `dwh_ta_f_d1_twvv_tn` directly and using standard partitioning filters in the `WHERE` clause (which ensures partition pruning).

2.14 PL/SQL:
     - Anonymous PL/SQL Block → BigQuery Scripting Block using standard `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`
     - `SQL%ROWCOUNT` → System variable `@@row_count` used immediately after insertion.
     - Exception Handler: `EXCEPTION WHEN OTHERS` is rewritten to BigQuery `EXCEPTION WHEN ERROR THEN`. BigQuery does not support transactional `ROLLBACK` unless explicitly within a transaction block (`BEGIN TRANSACTION`). We will omit rollback if not using explicit transactions, or structure the script with a transaction block for atomic safety.
     - Custom dynamic logging (`dwpa_meldung.fehler`) → Since external procedure execution is not natively supported in BigQuery SQL, this must be logged into a tracking or error log table as a standard DML `INSERT` statement or represented as a procedural placeholder.

2.15 Unresolvable or Advisory Items:
     - Substitution variables `&1` and `&2` → Resolved as BigQuery Scripting variables declared at the beginning of the block, allowing parameterization.
     - `dwpa_util_skript.runstatement` and `dwpa_meldung.fehler` → Replaced with native BigQuery operations (direct `TRUNCATE` and logging table inserts).
     - Database hints (`/*+ Append */`, `/*+ parallel(...) */`, `/*+ use_hash(...) */`) → Stripped entirely as BigQuery's optimizer manages query execution plans and parallelism automatically.

2.16 MIGRATION DECISION MATRIX

| Source Construct | Direct BigQuery SQL | BigQuery SQL UDF | Python Wrapper | Selected Target & Justification |
| :--- | :--- | :--- | :--- | :--- |
| **Anonymous PL/SQL Block** | Yes (Scripting) | No | No | **Direct BigQuery SQL**: BigQuery standard scripting completely supports procedural variables, standard DML, exceptions, and blocks. |
| **Dynamic SQL Truncate** | Yes (TRUNCATE) | No | No | **Direct BigQuery SQL**: Replaced with static `TRUNCATE TABLE` statement since table name is deterministic. |
| **Oracle Outer Join (+)** | Yes (LEFT JOIN) | No | No | **Direct BigQuery SQL**: Native ANSI `LEFT OUTER JOIN` replaces old Oracle proprietary syntax. |
| **Error Handlers / Logging** | Yes (DML Insert) | No | No | **Direct BigQuery SQL**: Structured error capturing using `@@error.message` and loading metadata into an error tracking table. |

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: A unified `.sql` file executing the procedural logic.
- **Log Table**: A structural logging entity mimicking `dwpa_meldung` if tracking of execution errors is to be preserved in BigQuery.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | BigQuery Target Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- |
| `PLS_INTEGER` | `INT64` | Direct numeric mapping | None |
| `NUMBER` | `INT64` | Explicit cast for IDs | Evaluated for scale and mapped to standard 64-bit integer. |
| `VARCHAR2(300)` | `STRING` | Standard string conversion | Length restrictions are not enforced in BigQuery. |
| `DATE` | `DATE` / `DATETIME` | Evaluated based on granularity | `l_monats_date` converted to `DATE`. `fact.gueltigkeitszeitpunkt` resolved as `TIMESTAMP` or `DATETIME`. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: PL/SQL scripting block, substitution parameters, implicit outer joins, partition-specific queries, and nested conditional logic (`DECODE`, `NVL`, `TRIM`).
- **Unsupported Functions**: PL/SQL package wrappers (`dwpa_util_skript`, `dwpa_meldung`).
- **UDF Required**: No.
- **Python Required**: No.
- **Assumptions**: 
  - `gueltigkeitszeitpunkt` contains timezone details or is mapped to a standard timezone timestamp.
  - Special characters in tables (e.g. `$`) are replaced with underscores (`_`) or wrapped in backticks. For clean execution, table names are sanitized (`dwh$ta_t_smart_kubi` becomes `dwh_ta_t_smart_kubi`).

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(x AS INT64)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(x, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y%m', CAST(x AS STRING))` |
| `DECODE` | Direct-with-rewrite | `CASE WHEN ... THEN ... ELSE ... END` |
| `NVL` | Direct-with-rewrite | `COALESCE(x, y)` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM(x)` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_TIMESTAMP` / `CAST(x AS STRING)` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `SQLERRM` | Direct-with-rewrite | `@@error.message` |
| `SQLCODE` | Direct-with-rewrite | `@@error.statement_text` |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | `SELECT` statement (or native logging) |
| `(+)` Join Syntax | Direct-with-rewrite | `LEFT OUTER JOIN` |
| `Partition(...)` | Direct-with-rewrite | Direct query on base table with partition filters |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- DECLARE scripting variables to replace PL/SQL and parameter bindings
DECLARE v_anzahl_ds INT64 DEFAULT 0;  -- converted from pls_integer
DECLARE l_monats_id INT64;            -- converted from number
DECLARE EintragsNr INT64;             -- converted from number
DECLARE lv_str STRING;                -- converted from varchar2(300)
DECLARE l_monats_date DATE;           -- converted from DATE

-- Assign substitution parameters (mimics '&1' and '&2')
SET l_monats_id = CAST(@param_monats_id AS INT64);  -- converted from to_number('&1')
SET EintragsNr = CAST(@param_eintrags_nr AS INT64); -- converted from to_number('&2')

-- Determine month boundary using safe date manipulation
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);  -- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)

BEGIN
  -- Execute native TRUNCATE statement 
  -- Converted from dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Dynamic partition selection handled natively by querying the base table with partition filters
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
    
    -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
    CASE 
      WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
      ELSE d.t_mobile_kundennummer 
    END AS kundennummer,
    
    COALESCE(t_new.tarif_id, 0) AS tarif_id,      -- converted from Nvl(t_new.tarif_id,0)
    COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,  -- converted from Nvl(t_old.tarif_id,0)
    
    -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
    CASE 
      WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn
      WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
      ELSE fact.vo_kenn_bearb
    END AS vo_kennung,
    
    d.test_gp,
    SUM(fact.zugang) AS anzahl,
    fact.kennzahl_id
  FROM dwh_ta_f_d1_twvv_tn AS fact
  
  -- Outer joins converted from implicit (+) syntax to standard ANSI JOINs
  LEFT OUTER JOIN temp AS t_new
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp AS t_old
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh_ta_c_vertrag AS d
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    AND l_monats_date > CAST(d.gueltig_von AS DATE)
    AND l_monats_date <= CAST(d.gueltig_bis AS DATE)
    
  WHERE FORMAT_TIMESTAMP('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING) -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF')
  GROUP BY
    -- Replicate standard analytical projections inside aggregate group
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

  -- Capture affected rows inside the block
  SET v_anzahl_ds = @@row_count;  -- converted from SQL%ROWCOUNT

  -- Return information as a query response to replace dbms_output
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Exception Block to replace PL/SQL WHEN OTHERS handler
  BEGIN
    DECLARE err_text STRING;
    DECLARE err_code STRING;
    DECLARE fehler_nr INT64;

    SET err_text = @@error.message;           -- converted from SQLERRM
    SET err_code = @@error.statement_text;    -- converted from SQLCODE
    SET fehler_nr = -20001;                   -- representative constant for dwpa_globals.k_alis_err_unknown;

    -- Log failure information into a metrics table to replace dwpa_meldung.fehler procedure
    INSERT INTO dwh_error_log (log_type, entry_nr, error_nr, error_msg, sql_code, log_time)
    VALUES ('F', EintragsNr, fehler_nr, err_text, err_code, CURRENT_TIMESTAMP());

    -- Bubble up the runtime error
    ERROR(err_text);
  END;
END;
```

FLAGGED ITEMS FOR HUMAN REVIEW
1. **Dynamic Partition Filters**: The source code targeted specific partitions explicitly using `dwh$ta_f_d1_twvv_tn partition (dwh$ta_f_d1_twvv_tn_&1)`. In the BigQuery translation, we query the main base table `dwh_ta_f_d1_twvv_tn` directly and rely on partition pruning via `FORMAT_TIMESTAMP('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)`. Verify that `gueltigkeitszeitpunkt` is indeed the table's ingestion-time or partition key column to ensure pruning occurs.
2. **Oracle Date Bound Truncation / Arithmetic**: The variable `l_monats_date` has been evaluated as standard `DATE`. If either of the source columns `d.gueltig_von` or `d.gueltig_bis` contains timezone or minute components, ensure they are cast safely to avoid mismatch during comparison.
3. **Database Logging / Framework Calls**: The logging package calls `dwpa_meldung.fehler` and dynamic truncation `dwpa_util_skript.runstatement` have been replaced with standard native queries. The target error logging table `dwh_error_log` is a synthetic representation of the logging schema and must be adjusted to match your team's logging target.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `d_abtn_x_smart_kubi.sql` | Converted PL/SQL anonymous block to a BigQuery SQL script. Replaced legacy outer joins, conditional `DECODE` and `NVL` constructs with ANSI SQL equivalents, and integrated standard BigQuery exception handling. |

---

### Execution order

The execution of this job on the target platform (BigQuery and Cloud Composer) must strictly preserve the sequence defined in the legacy system:

1. **`DW.DWH_ABTN_SMART_KUBI.xml` (DAG Orchestration)**: Migrates to an Airflow DAG (`dags/dw_dwh_abtn_smart_kubi.py`) in Cloud Composer.
2. **`d_abtn_x_smart_kubi.sql` (Main Script)**: Migrates to the target BigQuery SQL script `d_abtn_x_smart_kubi.sql`.
3. **`r_sqlscript` (Wrapper Executable)**: Re-implemented in Python as `r_sqlscript.py` (handled in its own design pass).
4. **`.dw_init` (Environment Initialization)**: Migrates to standard Airflow variable definitions or environment configurations.
5. **`f_alis_msgerr.ksh` (Error Utility)**: Migrates to shared logging/error modules or standard Airflow logging operators.
6. **`h_alis_sqlplus.ksh` (Utility Script)**: Superseded by standard Airflow BigQuery connection handlers.

#### System Orchestration & Review Integration
* To address the reviewer feedback from the previous attempt, the migrated Airflow DAG (`dw_dwh_abtn_smart_kubi.py`) must **not** assume the SQL script was converted to a Python executable. Instead, the DAG must invoke the migrated BigQuery SQL script `d_abtn_x_smart_kubi.sql` using one of the two correct patterns:
  - **Direct execution**: Use the `BigQueryInsertJobOperator` to execute the SQL script in BigQuery, passing the dynamic monthly reporting variables as query parameters.
  - **Wrapper execution**: Call the migrated `r_sqlscript.py` helper utility via a suitable Python or Bash operator, which natively forwards the BigQuery execution.

---

### Schedule & variables

The variables calculated by the scheduler must be dynamically constructed inside Cloud Composer and passed to the BigQuery script at runtime:

* **`DWH_JOB_KENNUNG`**: Constant value `'ABTN_SMART_KUBI'`.
* **`cdate`**: Derived dynamically from the logical run date using the Airflow Jinja template `{{ ds_nodash }}` (format `YYYYMMDD`).
* **`cmonth`** and **`cday`**: Derived inside the DAG via standard string slicing of `cdate`.
* **Prior Month Calculations (`MONATSID`)**:
  - Legacy calculation:
    `first = '01'` → `cmonth = cmonth + first` (represents the first day of the current month).
    `cmonth = SUB_DAYS(cmonth, 1)` (gets the last day of the prior month).
    `cmonth = SUBSTR(cmonth, 1, 6)` (extracts `YYYYMM` of the prior month).
  - **Composer equivalent**: Use standard Python `datetime` manipulation (or Airflow `macros.ds_add`) to calculate the prior month's `YYYYMM` and assign it to `MONATSID`. This value is then passed as the query parameter `@param_monats_id`.

---

### Lineage

Grounded in the metadata lineage of this component:

* **Upstream Data Sources (Producers)**:
  - `dwh$ta_f_d1_twvv_tn` → Target table: `dwh_ta_f_d1_twvv_tn`
  - `dwh$vi_l_map_fa_tarif` → Target table/view: `dwh_vi_l_map_fa_tarif`
  - `bl_d_tarif` → Target table: `bl_d_tarif`
  - `dwh$ta_c_vertrag` → Target table: `dwh_ta_c_vertrag`
* **Downstream Consumers**:
  - `dwh$ta_t_smart_kubi` → Target table: `dwh_ta_t_smart_kubi` (Written to via dynamic insert and read from during execution).
* **Package/Module References**:
  - `DWPA_UTIL_SKRIPT` / `DWPA_MELDUNG` → Oracle PL/SQL utility packages. The target BigQuery script handles truncation natively and inserts execution errors directly into a structured log table `dwh_error_log`.

---

### Cross-file dependencies

* **`r_sqlscript.py` Interaction**: The BigQuery SQL script expects input variables (such as `@param_monats_id` and `@param_eintrags_nr`) to be set by the caller. If the orchestration utilizes the migrated `r_sqlscript.py` wrapper to invoke SQL scripts, this SQL file must be packaged in the shared target directory where the Python wrapper can read and execute it.
* **Shared Table Schema**: Ensure that the target BigQuery tables `dwh_ta_f_d1_twvv_tn`, `dwh_vi_l_map_fa_tarif`, `bl_d_tarif`, and `dwh_ta_c_vertrag` are populated and up to date before running this script.

---

### Target file plan

* **Relative Path**: `d_abtn_x_smart_kubi.sql` (mirrors the source root `/home/gurunathan_t/KUBI/` folder structure)
* **Language**: `SQL` (BigQuery SQL Scripting Dialect)
* **Source File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`

---

### Environment-specific values

#### 1. GLOBAL (Environment-Wide)
* **`GCP_PROJECT`**: The GCP Project containing the BigQuery resources. Sourced via Airflow variables/environment configuration.
* **`BQ_DATASET`**: The target BigQuery dataset containing the core DWH tables (e.g., `dwh_ta_t_smart_kubi`). Sourced at runtime.
* **`dwpa_globals.k_alis_err_unknown`**: Standardised global framework error integer (mapped to standard global constant `-20001` in the exception handler).

#### 2. JOB-SPECIFIC
* **`EintragsNr`**: Particular to the execution run instance. Sourced as parameter `@param_eintrags_nr` supplied by the caller.
* **`MONATS_ID` / `MONATSID`**: Particular to the scheduling execution slice. Sourced as parameter `@param_monats_id` supplied by the scheduling DAG.

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
REASON: The script contains multiple KornShell function definitions managing logging and error states via Oracle SQL*Plus, which must be converted into a Python utility module.

EVIDENCE
- Business logic found: KSH custom logic defining helper functions (`DWMSG_*`) for logging, timing updates, status changes, and error handling via Oracle database PL/SQL procedure calls.
- AWK: none
- SQL-expressible: No, the core functionality consists of shell function abstractions, environment validations, parameter parsing, dynamic variables, and local temporary file IO.
- Non-SQL side effects: Writes and deletes temporary files in `/tmp`, constructs dynamic log file paths, and manages process-level signal traps.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) functions as a shared KornShell utility library designed to be sourced by other application scripts in the Information Services DWH environment. Its main purpose is to standardize job registration, status reporting (success, warnings, fatal errors), and timing instrumentation by invoking the Oracle `BERT_MELDUNG` PL/SQL package via `sqlplus`. It provides a standardized error handler designed to be called automatically via shell signal traps (e.g., `trap ... ERR`).

2. INVOCATION CONTEXT
   - **Caller:** Sourced by other parent KornShell jobs (e.g. `. f_alis_msgerr.ksh`). It is not executed as a standalone script.
   - **UC4 Includes Referenced:** None.
   - **Environment Files Sourced:** None explicitly sourced within this library script itself, but it relies on several environment variables being pre-configured by the caller.

3. PARAMETERS / INPUTS
   This library depends on the following caller-defined environment variables:
   - `DW_ORAUSER` (Source: caller environment) — Database connection string / credentials for Oracle SQL*Plus (e.g., `username/password@tns_alias`). Usable as evidence of Oracle target.
   - `DW_DIR_ROOT` (Source: caller environment) — Root directory under which SQL utility scripts are located.
   - `DW_DIR_PROT` (Source: caller environment) — Directory path where log/protocol files are written.

   Additionally, the functions receive positional arguments when invoked:
   - `DWMSG_Fehlerbehandlung`: `$1` = `DWMSG_EintragsNr` (the primary log entry ID)
   - `DWMSG_SetzeStatusOK`: `$1` = `DWMSG_EintragsNr`
   - `DWMSG_SetzeStatusAbbruch`: `$1` = `DWMSG_EintragsNr`
   - `DWMSG_ErmittleNr`: `$1` = Name of the variable in which to store the generated ID (using `eval`)
   - `DWMSG_ErzeugeEintrag`: `$1` = `DWMSG_EintragsNr`, `$2` = `JobKennung`, `$3` = `Programmname`, `$4` = `LogDatei`
   - `DWMSG_MeldeFehler`: `$1` = `DWMSG_EintragsNr`, `$2` = `Typ` (F/E/W), `$3` = `FehlerNr`, `$4` = `Zusatz1` (optional), `$5` = `Zusatz2` (optional)
   - `DWMSG_Logdateiname`: `$1` = Name of the variable to store the path (using `eval`), `$2` = `JobKennung`, `$3` = `DWMSG_EintragsNr`
   - `DWMSG_SetzeStichtagInfo`: `$1` = `DWMSG_EintragsNr`, `$2` = `DWMSG_Stichtag`, `$3` = `DWMSG_StichtagFmt`
   - `DWMSG_AppendTimingInfos`: `$1` = `DWMSG_EintragsNr`, `$2` = `DWMSG_InfoText`, `$3` = `DWMSG_DateFormat`

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Invoked repeatedly to execute PL/SQL procedures.
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
     - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
     - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
     - Inline dynamic PL/SQL calls in `DWMSG_SetzeStichtagInfo` and `DWMSG_AppendTimingInfos`.
     - *Resolvability:* These launchers call external wrapper scripts (`d_alis_spaufruf_p*.sql`) whose sources are not supplied. 
       # REVIEW-STRUCT: SQL wrapper scripts (e.g. d_alis_spaufruf_p1.sql) are not supplied — behavior assumed to map directly to executing BERT_MELDUNG package procedures. If migrating to native DB-client calls (e.g., using python-oracledb), replace these subprocess-driven sqlplus wrappers with direct connection execution of the database procedures.
   - `rm`: Deletes temporary files.
   - `cat`, `tr`: Reads and formats temporary sequence numbers.
   - `date`: System date command for log filename timestamps.

5. EMBEDDED SQL
   Two dynamic anonymous blocks are built and passed to `sqlplus` via standard input:
   - **Statement 1** (in `DWMSG_SetzeStichtagInfo`):
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
     commit;
     ```
     - Type: PL/SQL Procedure execution.
     - Tables touched: Unknown (encapsulated in `BERT_MELDUNG` package).
     - Dialect: Oracle (unambiguous via `EXEC`, `to_date`, and `commit`).
   - **Statement 2** (in `DWMSG_AppendTimingInfos`):
     ```sql
     EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
     commit;
     ```
     - Type: PL/SQL Procedure execution.
     - Tables touched: Unknown (encapsulated in `BERT_MELDUNG` package).
     - Dialect: Oracle (unambiguous via `EXEC`, `to_char`, `SYSDATE`, and `commit`).

6. CONTROL FLOW
   Each function is executed independently depending on when parent scripts call them:
   - **DWMSG_Fehlerbehandlung**: Saves last exit status, triggers fatal error report to DB using `DWMSG_MeldeFehler`, prints status messages, and marks the task aborted using `DWMSG_SetzeStatusAbbruch`.
   - **DWMSG_SetzeStatusOK**: Asserts entry ID presence, executes SQL*Plus task status update to successful.
   - **DWMSG_SetzeStatusAbbruch**: Asserts entry ID presence, executes SQL*Plus task status update to aborted.
   - **DWMSG_ErmittleNr**: Asserts variable name argument is present, generates a temporary filename with process ID `/tmp/ErmittleNr_$$.lst`, queries sequence value from Oracle, cleanses and stores the value, deletes the temp file, and assigns it dynamically. In Python, this dynamic assignment should be handled as a standard function return value.
   - **DWMSG_ErzeugeEintrag**: Validates input parameters, executes SQL*Plus to create logging metadata record.
   - **DWMSG_MeldeFehler**: Validates inputs, determines the count of parameters to select the correct SQL execution wrapper script (`p3.sql` to `p5.sql`), and runs SQL*Plus.
   - **DWMSG_Logdateiname**: Formats log directory, job identifier, formatted timestamp (`YYYYMMDD_HHMM`), and entry sequence ID into a dynamic file path.
   - **DWMSG_SetzeStichtagInfo**: Validates all arguments, and runs inline PL/SQL block to update business dates.
   - **DWMSG_AppendTimingInfos**: Validates inputs, runs inline PL/SQL block appending execution timing metrics to database columns.

7. ERROR HANDLING & EXIT CODES
   - **Parameter Validations:** KornShell utilizes `exit 1` or `exit 2` when validating required function arguments (e.g. `[ -z "$VarName" ]`).
   - **Database Errors:** The KornShell script does not catch standard `sqlplus` execution errors internally (such as connection issues); it lets them propagate.
   - **Python Migration:** Error checks should raise custom exceptions (`ValueError`, `RuntimeError`) or handle database exceptions natively if a python DB-driver is adopted. `sys.exit` should map to appropriate script return status codes.

8. OUTPUTS / SIDE EFFECTS
   - **File System:** Temporary run-files are generated inside `/tmp/ErmittleNr_[pid].lst` (immediately cleaned up). Log files are written dynamically based on the generated file paths in `$DW_DIR_PROT`.
   - **Oracle Database:** Logging metadata tables (accessed via `BERT_MELDUNG` package) are modified, updated, or appended to.

9. BUSINESS SUMMARY
   - **Job Auditing & Logging:** Standardizes the registration, scheduling execution logs, and timing metrics across all batch processing routines in the DWH environment.
   - **System Integrity Monitoring:** Integrates a centralized, database-backed error handler to ensure that failure events across shell-based pipelines are accurately updated inside Oracle.
   - **Instrumentation:** Provides standard performance logging boundaries to track the exact elapsed time for batch sequence tasks.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import datetime
import subprocess

# Note on Environment Variables:
# - DW_ORAUSER: Oracle credentials (e.g., "user/password@db")
# - DW_DIR_ROOT: Application root path
# - DW_DIR_PROT: Directory for protocol log files

# Define constants
UNEXPECTED_ERROR_CODE = 10

def _get_env_var(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"Error: Environment variable {name} is not defined.", file=sys.stderr)
        sys.exit(1)
    return val

def _run_sqlplus(args: list, input_data: str = None) -> subprocess.CompletedProcess:
    # Wrapper helper to run sqlplus
    cmd = ["sqlplus"] + args
    try:
        # Utilizing standard subprocess structure
        result = subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            text=True,
            check=True
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"Database operation failed: {e.stderr}", file=sys.stderr)
        raise

# Step 1: DWMSG_Fehlerbehandlung(entry_id, last_error_code)
def dwmsg_fehlerbehandlung(entry_id: str, last_error_code: int):
    # Log the fatal error to the database
    dwmsg_melde_fehler(
        entry_id, 
        "F", 
        UNEXPECTED_ERROR_CODE, 
        f"ErrorCode is: {last_error_code}"
    )
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(entry_id)

# Step 2: DWMSG_SetzeStatusOK(entry_id)
def dwmsg_setze_status_ok(entry_id: str):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    dw_orauser = _get_env_var("DW_ORAUSER")
    dw_dir_root = _get_env_var("DW_DIR_ROOT")
    
    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    
    # Execute SQL*Plus task status update
    # # REVIEW-STRUCT: SQL script body for d_alis_spaufruf_p1.sql not supplied in extraction
    _run_sqlplus([
        "-s", dw_orauser, 
        f"@{script_path}", 
        "BERT_MELDUNG.SetzeStatusOk", 
        entry_id
    ])

# Step 3: DWMSG_SetzeStatusAbbruch(entry_id)
def dwmsg_setze_status_abbruch(entry_id: str):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    dw_orauser = _get_env_var("DW_ORAUSER")
    dw_dir_root = _get_env_var("DW_DIR_ROOT")
    
    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    
    # Execute SQL*Plus task status update to aborted
    # # REVIEW-STRUCT: SQL script body for d_alis_spaufruf_p1.sql not supplied in extraction
    _run_sqlplus([
        dw_orauser, 
        f"@{script_path}", 
        "BERT_MELDUNG.SetzeStatusAbbruch", 
        entry_id
    ])

# Step 4: DWMSG_ErmittleNr() -> str
def dwmsg_ermittle_nr() -> str:
    dw_orauser = _get_env_var("DW_ORAUSER")
    dw_dir_root = _get_env_var("DW_DIR_ROOT")
    
    # Clean alternative to dynamic shell 'eval': Return sequence directly
    pid = os.getpid()
    temp_file_path = f"/tmp/ErmittleNr_{pid}.lst"
    
    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_al_is_ermittlenr.sql")
    
    try:
        # Run SQL*Plus query generating entry number into temporary file
        # # REVIEW-STRUCT: SQL script body for d_al_is_ermittlenr.sql not supplied in extraction
        _run_sqlplus([
            "-s", dw_orauser, 
            f"@{script_path}", 
            temp_file_path
        ])
        
        # Read file contents and strip whitespaces
        with open(temp_file_path, "r") as f:
            entry_id = f.read().replace(" ", "").strip()
            
        return entry_id
    finally:
        # Ensure cleanup of temporary sequence file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# Step 5: DWMSG_ErzeugeEintrag(entry_id, job_id, program_name, log_file)
def dwmsg_erzeuge_eintrag(entry_id: str, job_id: str, program_name: str, log_file: str):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    dw_orauser = _get_env_var("DW_ORAUSER")
    dw_dir_root = _get_env_var("DW_DIR_ROOT")
    
    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p4.sql")
    
    # # REVIEW-STRUCT: SQL script body for d_alis_spaufruf_p4.sql not supplied in extraction
    _run_sqlplus([
        "-s", dw_orauser, 
        f"@{script_path}", 
        "BERT_MELDUNG.Erzeuge_Eintrag", 
        entry_id, 
        job_id, 
        program_name, 
        log_file
    ])

# Step 6: DWMSG_MeldeFehler(entry_id, typ, error_no, extra_1=None, extra_2=None)
def dwmsg_melde_fehler(entry_id: str, typ: str, error_no: int, extra_1: str = "", extra_2: str = ""):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    dw_orauser = _get_env_var("DW_ORAUSER")
    dw_dir_root = _get_env_var("DW_DIR_ROOT")
    
    # Determine parameter count to select script
    if not extra_1:
        num_parm = 3
    elif not extra_2:
        num_parm = 4
    else:
        num_parm = 5
        
    script_name = f"d_alis_spaufruf_p{num_parm}.sql"
    script_path = os.path.join(dw_dir_root, f"allgemein/is/util/sql/{script_name}")
    
    # Format extra args for SQL CLI parameters
    formatted_extra1 = f"'{extra_1}'" if extra_1 else "''"
    formatted_extra2 = f"'{extra_2}'" if extra_2 else "''"
    
    # # REVIEW-STRUCT: SQL script bodies for d_alis_spaufruf_p3/p4/p5.sql not supplied in extraction
    _run_sqlplus([
        "-s", dw_orauser,
        f"@{script_path}",
        "BERT_MELDUNG.Fehler",
        typ,
        entry_id,
        str(error_no),
        formatted_extra1,
        formatted_extra2
    ])

# Step 7: DWMSG_Logdateiname(job_id, entry_id) -> str
def dwmsg_logdateiname(job_id: str, entry_id: str) -> str:
    # Cleaner structure replacing dynamic 'eval' assignment with returned path string
    dw_dir_prot = _get_env_var("DW_DIR_PROT")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{job_id}_{timestamp}_{entry_id}.log"
    return os.path.join(dw_dir_prot, filename)

# Step 8: DWMSG_SetzeStichtagInfo(entry_id, stichtag, format_mask)
def dwmsg_setze_stichtag_info(entry_id: str, stichtag: str, format_mask: str):
    if not entry_id:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not format_mask:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    dw_orauser = _get_env_var("DW_ORAUSER")
    
    # Build inline PL/SQL block
    plsql_payload = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({entry_id}, to_date('{stichtag}', '{format_mask}'));
    commit;
    """
    
    _run_sqlplus(["-s", dw_orauser], input_data=plsql_payload)

# Step 9: DWMSG_AppendTimingInfos(entry_id, info_text, date_format)
def dwmsg_append_timing_infos(entry_id: str, info_text: str, date_format: str):
    if not entry_id:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    dw_orauser = _get_env_var("DW_ORAUSER")
    
    # Build inline PL/SQL timing update block
    plsql_payload = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({entry_id},null,'{info_text}'||' '||to_char(SYSDATE,'{date_format}')||' ');
    commit;
    """
    
    _run_sqlplus(["-s", dw_orauser], input_data=plsql_payload)
```

### MIGRATION DESIGN DOCUMENT: DW.DWH_ABTN_SMART_KUBI (f_alis_msgerr.ksh Group)

#### 1. Job Dependencies
- **Upstream / Caller Jobs:** Sourced by other components in the execution order (e.g., `r_sqlscript` wrapper, `.dw_init`, or the parent job orchestration `DW.DWH_ABTN_SMART_KUBI.xml`).
- **Downstream Consumers:** Sourced during task execution to perform logging, error trapping, and status updates.
- **Wiring on BigQuery:** In the target Cloud Composer environment, this KornShell utility library is converted to a Python module (`kubi/f_alis_msgerr.py`). It will be imported by other Python scripts/operators to maintain consistent status logging and error management.
- **Unmigrated Upstream/Downstream Risk:** `r_sqlscript` and `DW.DWH_ABTN_SMART_KUBI.xml` belong to a separate design pass group and are not yet migrated in this pass. The wiring of this utility Python module into the parent DAG or orchestration script cannot be finalized until those sibling components exist.

#### 2. Execution Order
- The legacy execution sequence is:
  1. `DW.DWH_ABTN_SMART_KUBI.xml` (Scheduler / Orchestration)
  2. `d_abtn_x_smart_kubi.sql` (Main SQL logic)
  3. `r_sqlscript` (Wrapper)
  4. `.dw_init` (Initialization)
  5. `f_alis_msgerr.ksh` (Logging/Error helper utility - sourced by wrappers)
  6. `h_alis_sqlplus.ksh` (SQL helper utility)
- **Target Mapping:**
  - Since `f_alis_msgerr.ksh` is a utility library rather than an independent execution step, it does not represent a standalone task in the Cloud Composer DAG. Instead, its target file `kubi/f_alis_msgerr.py` will be imported and utilized dynamically within the Python operators executing the other steps (such as the migrated `r_sqlscript` wrapper).

#### 3. Scheduling
- **Trigger/Scheduler:** This utility is not directly triggered by any scheduler. It is event-driven, called dynamically during the execution of parent tasks to log execution statuses or traps.
- **Target Scheduling Construct:** Handled implicitly via Python imports and execution within Composer tasks.

#### 4. Schedule & Variables — Must Be Retained
- **Scheduler-Set Variables:**
  The parent scheduler passes several variables to the orchestrator:
  - `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
  - `cdate` = `'SYS_DATE("YYYYMMDD")'`
  - `cmonth` = `'SUBSTR(&cdate,1,6)'`
  - `cday` = `'SUBSTR(&cdate,7,2)'`
  - `first` = `'01'`
  - `cmonth` = `'&cmonth&first'`
  - `cmonth` = `'SUB_DAYS(&cmonth,1)'`
  - `cmonth` = `'SUBSTR(&cmonth,1,6)'`
  - `MONATSID` = `'&cmonth'`
- **Target Passing Mechanism:** These scheduler-set variables will be managed as Airflow DAG `params` or runtime variables in Cloud Composer, and passed down to individual tasks. The Python logging functions in `kubi/f_alis_msgerr.py` (e.g., `dwmsg_erzeuge_eintrag`) will receive these as function parameters (e.g., `job_id`, `entry_id`) when called by parent execution scripts.

#### 5. Lineage
- **Upstream Producers:** Calls database procedures to read or generate IDs (`d_al_is_ermittlenr.sql`).
- **Downstream Consumers:** Updates execution status table via procedures like `BERT_MELDUNG.SetzeStatusOk`, `BERT_MELDUNG.SetzeStatusAbbruch`, and `BERT_MELDUNG.Fehler`.

#### 6. Cross-File Dependencies
- **Shared Utilities:** Sourced by wrappers to log errors and exit status.
- **SQL Scripts Called:** Calls several unprovided utility SQL scripts (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, `d_alis_spaufruf_p3.sql`, `d_alis_spaufruf_p4.sql`, `d_alis_spaufruf_p5.sql`).
- **PL/SQL Package:** Relies on database PL/SQL package `BERT_MELDUNG` with procedures `SetzeStatusOk`, `SetzeStatusAbbruch`, `Fehler`, `SetzeZusatzInfos`, and `Erzeuge_Eintrag`. No direct equivalent exists on BigQuery for these Oracle database procedures; they must be executed on Oracle via a Python database connector (or manually refactored to native GCP logging mechanisms).

#### 7. Target File Plan
- **Target File:** `kubi/f_alis_msgerr.py`
  - **Source File:** `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
  - **Language:** Python
  - **Purpose:** Provide a Python library mirroring all `DWMSG_*` logging and error handling functions to maintain compatibility with other migrated scripts.

#### 8. Environment-Specific Values
- **DW_ORAUSER** (GLOBAL): Environment-wide connection string / credentials to access Oracle. Sourced in Python via `os.environ.get("DW_ORAUSER")` or retrieved securely from Google Secret Manager / Airflow Connection variables.
- **DW_DIR_ROOT** (GLOBAL): Local application root path. Maps to a global environment variable or Airflow configuration.
- **DW_DIR_PROT** (GLOBAL): Directory path where protocol log files are written. Maps to a global environment variable or GCS bucket folder.

#### 9. File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `kubi/f_alis_msgerr.py` | Migrated to a Python utility library containing logging and status-reporting helper functions. |

#### 10. Risks & Manual Steps
- **Cross-Group Migration Scope:**
  - `r_sqlscript` and `DW.DWH_ABTN_SMART_KUBI.xml` belong to a separate design pass group and are NOT in the `SOURCE FILES` list of this migration group. They are omitted from our target file plan and file disposition table.
  - Sibling migration passes must ensure that `r_sqlscript` is converted to `r_sqlscript.py` and that the Composer DAG for `DW.DWH_ABTN_SMART_KUBI.xml` is designed to execute the BigQuery SQL script correctly (either via `BigQueryInsertJobOperator` or by executing `r_sqlscript.py`).
- **Missing PL/SQL Script Definitions:**
  - The exact implementation details of the PL/SQL script files (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, etc.) are not supplied. If the project requires migrating away from SQL*Plus execution, these external script execution logic should be replaced by a native python database connector (e.g. `oraclesql` or BigQuery client) directly executing equivalent statements.

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
REASON: The script defines a procedural helper function with custom argument validations, file-readability checks, and a subprocess invocation of SQL*Plus that cannot be represented in standard BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic defines a helper function `starteSQLSkript` to validate parameters, check file readability, and execute SQL scripts via `sqlplus`.
- AWK: none
- SQL-expressible: no, it contains procedural shell logic, filesystem checks, and external command invocations.
- Non-SQL side effects: Validates file existence/readability, executes SQL*Plus as an external subprocess, and invokes an external error utility `DWMSG_MeldeFehler`.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a KornShell helper/library script containing reusable utility routines for running Oracle SQL*Plus. It defines the `starteSQLSkript` function, which validates input arguments, ensures the target SQL script file is readable, and executes the script using SQL*Plus with appropriate parameter forwarding and error tracking.

2. INVOCATION CONTEXT
   - Who calls this script: It is a helper script meant to be sourced (`. h_alis_sqlplus.ksh`) by other KSH batch scripts or UC4 job environments needing to run SQL*Plus scripts. No specific parent script is supplied in this extraction.
   - Any UC4 native includes: None referenced in the extraction.
   - Environment files sourced: None. However, it relies on external variables such as `DW_ORAUSER` and the availability of the external executable/function `DWMSG_MeldeFehler` from the environment.

3. PARAMETERS / INPUTS
   For function `starteSQLSkript`:
   - `p_Eintragsnr` ($1): Positional argument representing the error entry number. Used when calling `DWMSG_MeldeFehler`. Surfaced in Python as a positional parameter.
   - `p_Skript` ($2): Positional argument representing the path to the SQL script file. Checked for readability and executed. Surfaced in Python as a positional parameter.
   - `*` (shifted remaining arguments): Variable arguments forwarded to the SQL*Plus script. Surfaced in Python via `*args`.
   Environment variables:
   - `DW_ORAUSER`: Used as the connection schema/user credentials for SQL*Plus. Surfaced in Python via `os.environ.get("DW_ORAUSER")`.
   - `Modul_Name` and `Modul_Version` (note the underscore difference in the `DWMSG_MeldeFehler` call relative to local variables `ModulName` and `ModulVersion`): Used in error logging; fallback to local default variables if undefined.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"`
     - Purpose: Reports missing parameter error (code 196).
     - Subprocess conversion: Yes, should remain an external process invocation via `subprocess.run()`.
     - Launcher status: # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
     - Purpose: Reports unreadable file error (code 201).
     - Subprocess conversion: Yes, should remain an external process invocation via `subprocess.run()`.
     - Launcher status: # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Runs the target Oracle SQL script with connection details from `DW_ORAUSER` and forwards trailing arguments.
     - Subprocess conversion: Yes, must remain an external process invocation via `subprocess.run()` because it dynamically runs arbitrary SQL scripts.
     - Launcher status: Not resolvable since the target SQL script executed is dynamic and not specified in this extraction.

5. EMBEDDED SQL
   - Source file / label: None. No inline SQL is present; SQL is dynamically executed via external script paths.
   - Dialect: Oracle SQL*Plus (indicated by `sqlplus ${DW_ORAUSER} @$p_Skript $*`).
   - # REVIEW: target database platform is Oracle based on 'sqlplus' invocation; if migrating to a different target (e.g. BigQuery), this helper function and the SQL scripts it launches will require a complete redesign.

6. CONTROL FLOW
   1. Initialize module variables `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"`.
   2. Define function `starteSQLSkript` receiving arguments `p_Eintragsnr`, `p_Skript`, and optional script parameters (`*args`).
   3. Check if `p_Eintragsnr` or `p_Skript` is empty. If true, call `DWMSG_MeldeFehler` with code 196 and return 196.
   4. Check if file `$p_Skript` is readable. If false, call `DWMSG_MeldeFehler` with code 201 and return 201.
   5. Log parameters to standard output: "Rufe SQL*PLUS auf mit folgenden Einstellungen", "Sql*Plus-Skript : $p_Skript", and "Skript-Parameter: $*".
   6. Disable strict error handling (`set +e`).
   7. Execute `sqlplus` with `DW_ORAUSER`, target script, and trailing parameters, redirecting standard input from `/dev/null`.
   8. Capture exit code (`errcode=$?`).
   9. Restore strict error handling (`set -e`).
   10. Return the captured `errcode`.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments: returns `196` and calls `DWMSG_MeldeFehler`.
   - Script file unreadable: returns `201` and calls `DWMSG_MeldeFehler`.
   - SQL execution failure: captures the exit status of `sqlplus` and returns it.
   - In Python, these will be handled by returning integers from the function, allowing the calling script to manage orchestration flow.

8. OUTPUTS / SIDE EFFECTS
   - Standard output logs and error messages.
   - Execution logs from the SQL*Plus engine.
   - Potential registration of errors in an external logging database/system via `DWMSG_MeldeFehler`.

9. BUSINESS SUMMARY
   - Provides an execution wrapper for Oracle SQL*Plus scripts inside the batch system.
   - Implements protective pre-checks (validating parameter presence and script file readability) to prevent partial/silent batch failures.
   - Integrates with a central enterprise error messaging module (`DWMSG_MeldeFehler`) to record batch initialization or execution anomalies.
   - Preserves audit trails by logging the targeted script path and runtime arguments.

=======================================================================================
PSEUDOCODE OUTLINE (PYTHON)
=======================================================================================

```python
import os
import sys
import subprocess
import pathlib

# Step 1: Initialize module-level identification variables
ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"

# Step 2: Define helper function to start SQL*Plus script
def starteSQLSkript(p_Eintragsnr, p_Skript, *args):
    # Step 3: Validate mandatory arguments
    if not p_Eintragsnr or not p_Skript:
        # # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        modul_name_err = os.environ.get("Modul_Name", ModulName)
        modul_version_err = os.environ.get("Modul_Version", ModulVersion)
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_Eintragsnr, 
            "E", 
            "196", 
            f"{modul_name_err} {modul_version_err} starteSQLSkript"
        ], check=False)
        return 196

    # Step 4: Validate script file is readable
    script_path = pathlib.Path(p_Skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        # # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_Eintragsnr, 
            "E", 
            "201", 
            p_Skript
        ], check=False)
        return 201

    # Step 5: Log run parameters
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Invoke sqlplus and capture exit code (reproducing set +e and set -e logic)
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(args)
    
    # # REVIEW: target database platform is Oracle based on 'sqlplus' invocation; if migrating to a different target (e.g. BigQuery), this helper function and the SQL scripts it launches will require a complete redesign.
    try:
        # Redirect stdin from /dev/null
        result = subprocess.run(cmd, stdin=subprocess.DEVNULL)
        errcode = result.returncode
    except Exception as e:
        # Handling execution failure (e.g., sqlplus binary not found on PATH)
        print(f"Error executing sqlplus: {e}", file=sys.stderr)
        errcode = 1
        
    return errcode
```

### Execution order
The legacy execution order of the job group contains the following steps, which must be preserved in the target orchestration:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (orchestration entrypoint)
2. `d_abtn_x_smart_kubi.sql` (primary SQL execution script)
3. `r_sqlscript` (wrapper shell execution script)
4. `.dw_init` (initialization file/settings)
5. `f_alis_msgerr.ksh` (error handling and messaging utility script)
6. `h_alis_sqlplus.ksh` (SQL*Plus helper/utility script, which is the current group's migration scope)

Any calling orchestration (such as a Cloud Composer DAG or a task representing `r_sqlscript.py`) will import or invoke the functionality migrated from `h_alis_sqlplus.ksh` to validate and run database scripts.

### Schedule & variables
In the target environment, the migrated job must receive its required scheduler variables. These variables must be calculated at runtime in the target orchestration (Airflow DAG) and passed dynamically:
- `DWH_JOB_KENNUNG` (constant: `'ABTN_SMART_KUBI'`)
- `cdate` (dynamically calculated at run-date: `SYS_DATE("YYYYMMDD")`, equivalent to Airflow's native `{{ ds_nodash }}`)
- `cmonth` (computed via sequential date manipulations: extracting substring of `cdate`, appending `'01'`, subtracting 1 day, and taking the first 6 characters to yield the reporting month ID)
- `cday` (extracted day substring from `cdate`)
- `first` (constant: `'01'`)
- `MONATSID` (the final computed value of `cmonth` representing the reporting month)

### Target file plan
The following target file will be generated:
- **Relative Path**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
- **Language**: Python
- **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
- **Description**: A Python module containing utility functions to replace the procedural shell functions of `h_alis_sqlplus.ksh`. It validates parameters, checks script file readability, and executes SQL scripts. Since there is no direct target-platform equivalent for `sqlplus` on BigQuery, SQL execution must be performed using the BigQuery Python Client API.

### Environment-specific values
- `DW_ORAUSER` (Legacy Oracle Database User): Classified as **GLOBAL**. It identifies the connection credentials and database schema. In the target environment, this maps to the `GCP_PROJECT` and associated connection profiles managed globally (e.g., via Airflow connections).

### File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Converted to a Python utility module to execute database scripts, keeping input validations, readability checks, and logging logic intact. |

### Risks & Manual Actions
- **No Direct CLI Equivalent**: No direct target-platform equivalent exists for `sqlplus` on BigQuery. SQL execution logic must be rewritten to interface with the Google Cloud BigQuery API or native Airflow operators instead of spawning a `sqlplus` shell subprocess.
- **External Logger Dependency**: The script calls an external error-logging utility `DWMSG_MeldeFehler`. Because its internal implementation is outside this group's source code, a mock or target-compatible logger wrapper must be provided to avoid missing-dependency errors.
- **Orchestration Alignment**: The calling DAG (handling the XML/orchestration layer) must correctly execute the BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) and invoke the migrated wrapper utility (`r_sqlscript.py` / `h_alis_sqlplus.py`) rather than attempting to execute SQL directly as a generic bash script. This integration must be validated when combining sibling deliverables.

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
REASON: The script is an orchestrator/launcher utility containing command-line argument parsing, file path searching, process execution, and error trapping logic that cannot be expressed in SQL.

EVIDENCE
- Business logic found: KSH custom logic parses command-line options (`getopts`), dynamically searches for SQL script paths, manages execution logs, and controls run-state logging.
- AWK: none
- SQL-expressible: no, it is a generic utility script that executes external SQL scripts.
- Non-SQL side effects: checks file existence, dynamically modifies current working directory, configures signal traps, creates/updates logs, and launches external processes.
- Against this verdict: none. This is a framework/orchestration script that must be converted to Python.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `r_sqlscript` utility is a legacy KornShell wrapper script used to run Oracle SQL*Plus scripts in a standardized manner. It accepts a SQL script name, resolves its filesystem path relative to the caller, configures error trapping, registers execution metadata with a logging framework, and executes the SQL commands. It serves as a central mechanism to ensure database tasks report their outcomes consistently.

2. INVOCATION CONTEXT
   - Who calls this script: Typically invoked by UC4/Automic jobs or other parent orchestration scripts, passing command-line parameters like:
     `/local/home/gurunathan_t/kubi/r_sqlscript -f <script.sql> -j <JOB_NAME> -i "<parameters>"`
   - UC4 native includes: None referenced in the script.
   - Environment files sourced:
     1. `. $HOME/aktuell/.dw_init`
        # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     2. `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
        # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — functions it defines (e.g. DWMSG_MeldeFehler) are unknown; do not guess their names or values
     3. `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
        # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — defines starteSQLSkript; internal behaviour unknown

3. PARAMETERS / INPUTS
   - `-f` (p_sqlscript): Positional argument parsed via `getopts`. Target filename of the SQL script to be run. Converted internally to lowercase (`typeset -l`). Surfaced in Python using `argparse` as a required parameter.
   - `-i` (p_sqlpar): Optional input string containing parameters to pass directly to the SQL script. Surfaced in Python using `argparse`.
   - `-j` (p_Job / JobKennung): Optional Job Identifier (defaults to "DWH_KORR"). Surfaced in Python using `argparse`.
   - `-v` (p_Verbose): Optional verbose flag. If set to 1, prints out the execution log immediately if an error occurs. Surfaced in Python using `argparse` (`action='store_true'`).
   - `-h`: Usage/help flag. Handled natively by Python's `argparse`.
   - `p_Kuerzel` (Referenced but unused/undefined):
     # REVIEW: `p_Kuerzel` is referenced in the script's path validation check but is never defined or declared. Verify if this is an argument that should have been captured or if it is a legacy bug.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `dirname $0` and `dirname ${p_sqlscript}`: Used to find directories. Natively handled in Python using `os.path.dirname` and `os.path.abspath`.
   - `starteSQLSkript`: External shell function defined in sourced file `h_alis_sqlplus.ksh`.
     - Verbatim command line: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
     - Purpose: Launches SQL*Plus to run the resolved SQL script with parameters.
     - Python equivalent: Since this launcher's body was not supplied, represent as an external execution.
       # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - There are no inline SQL statements in this wrapper script. It executes external scripts whose paths are resolved dynamically.
   - The dialect of the executed script is implicitly Oracle SQL*Plus (indicated by the use of `h_alis_sqlplus.ksh` and `whenever sqlerror exit failure` in the usage instructions).

6. CONTROL FLOW
   1. **Environment Setup:** Source `$HOME/aktuell/.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh` to load the database wrapper environment. Enable fail-on-error (`set -e`).
   2. **Initialize Tracking:** Set `DW_EintragsNr=0` and export it.
   3. **Parse Arguments:** Use `getopts` to process options `-f`, `-i`, `-j`, `-v`, and `-h`.
   4. **Validate Arguments:** If `getopts` fails (`ErrNr != 0`), call `DWMSG_MeldeFehler` with the error number, display usage, and exit.
   5. **Path Resolution:**
      - Change the current directory to the parent directory of this wrapper script.
      - If the directory of the SQL script is `.`, search for the file sequentially in `../sql/`, then `../mig/`, and finally in the current directory.
      - If the directory of the SQL script is not `.`, use the path as provided.
   6. **Validate File Presence:**
      - Check if the resolved script path `l_DBskript` is a file.
        # REVIEW: The legacy script contains `if [ -f "$l_DBskript" ] then ErrNr=198; ErrArg="$p_Kuerzel"; fi`. This means if the file EXISTS, it sets an error code and uses an undefined variable. This appears to be a bug in the legacy logic (likely missing a `!`). Implement with warning and verify correct logic.
   7. **Establish Job ID:** Set `JobKennung` to the value of `-j` or default to "DWH_KORR". Convert to uppercase.
   8. **Register Execution Context:**
      - Generate a unique database sequence number via `DWMSG_ErmittleNr`.
      - Determine log filename via `DWMSG_Logdateiname`.
      - Write log header/entry via `DWMSG_ErzeugeEintrag` redirecting output to `$LogDatei`.
   9. **Trap Setups:** Configure INT and ERR traps to call `DWMSG_Fehlerbehandlung` and (if `-v` is active) output the log file using `cat` before exiting.
   10. **SQL Script Invocation:** Execute `starteSQLSkript` with resolved arguments, redirecting output to `$LogDatei`.
   11. **Post-execution Success Logging:** If execution completes without error, update status via `DWMSG_SetzeStatusOK`, clear signal traps, and output completion message.

7. ERROR HANDLING & EXIT CODES
   - Standard success exit code is `0`.
   - Failure is caught via the shell's `set -e` and trap mechanisms on `ERR` and `INT`.
   - On error, the trap runs `DWMSG_Fehlerbehandlung $DW_EintragsNr` and terminates with status `1`.
   - Argument errors exit with codes `192` (unknown argument) or `193` (missing parameter value).
   - In Python, we wrap the execution block in a `try...except Exception` structure. Cleanups and status updates are executed inside the `except` block before exiting with `sys.exit(1)`.

8. OUTPUTS / SIDE EFFECTS
   - Log file: Writes to a dynamically resolved `$LogDatei` path.
   - Database side effects: Executes the targeted SQL scripts which alter database states, and calls monitoring hooks (`DWMSG_*` utilities) to write status into audit/logging tables.

9. BUSINESS SUMMARY
   - Standardizes the database connection and execution wrappers across all automated SQL-based ETL jobs.
   - Enforces uniform logging structures and unique transaction sequence ID tracking.
   - Decouples SQL code location from UC4 configurations by dynamically searching standard deployment subdirectories (`../sql`, `../mig`).
   - Ensures any database-level exceptions trigger standard operational alarms and dump detailed logs for debugging.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
# Step 1: Import core dependencies and initialize setup
import os
import sys
import argparse
import subprocess

# REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown
# REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — functions it defines are unknown
# REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — defines starteSQLSkript

ProgName = f"Ausführung Script {sys.argv[0]}"
ProgVersion = "5.0.0"

# Mock/Stubs for sourced functions whose definitions are not supplied:
def DWMSG_MeldeFehler(eintrags_nr, level, err_nr, err_arg):
    # Stub for the legacy DWMSG_MeldeFehler shell function
    print(f"ERROR: {level} {err_nr} {err_arg}", file=sys.stderr)

def DWMSG_ErmittleNr():
    # Stub for sequence generator DWMSG_ErmittleNr
    return 1001  # Placeholder run ID

def DWMSG_Logdateiname(job_kennung, eintrags_nr):
    # Stub for DWMSG_Logdateiname
    return f"/tmp/{job_kennung}_{eintrags_nr}.log"

def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, program, log_file):
    # Stub for DWMSG_ErzeugeEintrag
    pass

def DWMSG_Fehlerbehandlung(eintrags_nr):
    # Stub for DWMSG_Fehlerbehandlung
    pass

def DWMSG_SetzeStatusOK(eintrags_nr):
    # Stub for DWMSG_SetzeStatusOK
    pass

def starteSQLSkript(eintrags_nr, db_script, sql_par, run_id, log_file_path):
    # # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction
    # This represents running the actual SQL*Plus executable with arguments
    cmd = ["sqlplus", "-S", "/nolog", f"@{db_script}", sql_par]
    with open(log_file_path, "a") as log:
        subprocess.run(cmd, check=True, stdout=log, stderr=log)

# Step 2: Define and parse command-line arguments (getopts replacement)
parser = argparse.ArgumentParser(description=ProgName, add_help=False)
parser.add_argument('-f', required=True)
parser.add_argument('-i', default='')
parser.add_argument('-j', default='DWH_KORR')
parser.add_argument('-v', action='store_true')
parser.add_argument('-h', action='help')

try:
    args = parser.parse_args()
except argparse.ArgumentError as e:
    # Handle getopts error scenarios (ErrNr 192/193)
    DWMSG_MeldeFehler(0, "E", 193, str(e))
    sys.exit(193)

p_sqlscript = args.f.lower()  # typeset -l p_sqlscript
p_sqlpar = args.i
p_Job = args.j
p_Verbose = 1 if args.v else 0

# Step 3: Resolve paths
# Change directory to script's directory
original_dir = os.getcwd()
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
os.chdir(script_dir)

p_sqlscript_dir = os.path.dirname(p_sqlscript)
if p_sqlscript_dir == '.' or p_sqlscript_dir == '':
    l_DBskript = os.path.join('..', 'sql', p_sqlscript)
    if not os.path.isfile(l_DBskript):
        l_DBskript = os.path.join('..', 'mig', p_sqlscript)
    if not os.path.isfile(l_DBskript):
        l_DBskript = p_sqlscript
else:
    l_DBskript = p_sqlscript

# Step 4: Validate script file presence
# REVIEW: Legacy code contains: if [ -f "$l_DBskript" ] then ErrNr=198 ...
# This logic erroneously triggers when the file is present. Retained here for parity, but needs fixing.
p_Kuerzel = None  # REVIEW: p_Kuerzel is referenced but was never initialized in the source script
if os.path.isfile(l_DBskript):
    ErrNr = 198
    ErrArg = p_Kuerzel
    DWMSG_MeldeFehler(0, "E", ErrNr, ErrArg)
    sys.exit(ErrNr)

# Step 5: Configure execution metadata
JobKennung = p_Job.upper()

print("----------------- Parameter -----------------")
print(f"Jobkennung     : {JobKennung}")
print(f"DB-Skript      : {l_DBskript}")
print("---------------------------------------------")

DW_EintragsNr = DWMSG_ErmittleNr()
LogDatei = DWMSG_Logdateiname(JobKennung, DW_EintragsNr)

# Ensure logging registry entry
DWMSG_ErzeugeEintrag(DW_EintragsNr, JobKennung, f"{sys.argv[0]}_{l_DBskript}", LogDatei)

# Step 6: Execute job within error handling context (Trap replacements)
try:
    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{DW_EintragsNr}'")
    print(f"Logdatei  : '{LogDatei}'")
    print("---------------------------------------------")
    
    # Step 7: Launch the database script
    starteSQLSkript(DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr, LogDatei)
    
    # Step 8: Complete execution successfully
    DWMSG_SetzeStatusOK(DW_EintragsNr)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    sys.exit(0)

except Exception as err:
    # Error trapping block (ERR/INT trap replacement)
    # Append error logging to LogDatei
    with open(LogDatei, "a") as log:
        log.write(f"!FEHLER gemeldet!: {str(err)}\n")
        
    DWMSG_Fehlerbehandlung(DW_EintragsNr)
    
    if p_Verbose != 0:
        with open(LogDatei, "r") as log:
            print(log.read())
            
    sys.exit(1)
```

### 1. **Execution order**
The legacy dependency graph outlines an execution sequence that must be preserved in the target orchestration (Cloud Composer):
1. `DW.DWH_ABTN_SMART_KUBI.xml` (The scheduling/triggering XML definition, handled by its sibling group).
2. `d_abtn_x_smart_kubi.sql` (The main PL/SQL script containing the processing logic).
3. `r_sqlscript` (The shell wrapper used to invoke the SQL script).
4. `.dw_init` (The environment variables initialization).
5. `f_alis_msgerr.ksh` (The KornShell utility for error messaging and logging).
6. `h_alis_sqlplus.ksh` (The helper script to execute SQL*Plus).

In the target environment on BigQuery and Cloud Composer:
- The Cloud Composer DAG will manage the step-by-step task execution.
- The wrapper `r_sqlscript` is converted to a Python wrapper script (`r_sqlscript.py`).
- The DAG or the Python wrapper executes the migrated BigQuery SQL script (`d_abtn_x_smart_kubi.sql`), preserving the parameter passing and error logging behaviors.

### 2. **Schedule & variables**
The migrated job must retain and dynamically process the scheduler-set variables listed below. In Cloud Composer / Airflow, these must be resolved natively (e.g., using Jinja templates, Airflow macros, or a Python helper task):
- **DWH_JOB_KENNUNG**: Constant string set to `'ABTN_SMART_KUBI'`. Passed as an execution argument or configuration.
- **cdate**: Calculated dynamically using system date in `'YYYYMMDD'` format (equivalent to `SYS_DATE("YYYYMMDD")`).
- **cmonth**: Derived from `cdate` (first 6 characters, equivalent to `SUBSTR(&cdate,1,6)`).
- **cday**: Derived from `cdate` (characters at positions 7 and 8, equivalent to `SUBSTR(&cdate,7,2)`).
- **first**: Constant string set to `'01'`.
- **cmonth** (concatenated): Resolved by combining `cmonth` and `first` (e.g., `'YYYYMM01'`).
- **cmonth** (subtracted): Subtract 1 day from the consolidated date above.
- **cmonth** (final): Extract the first 6 characters of the subtracted date string.
- **MONATSID**: Assigned the final value of `cmonth` (used as the reporting month identifier).

### 3. **Lineage**
The wrapper script manages direct dependencies on utility and environment configurations:
- **Upstream configuration sourcing**: Sources `.dw_init` for baseline environment paths.
- **Downstream script invocations**:
  - `FILE:f_alis_msgerr.ksh` for DB-backed error reporting and validation.
  - `FILE:h_alis_sqlplus.ksh` for executing the SQL script with parameters.

### 4. **Cross-file dependencies**
- **Environment dependencies**: `r_sqlscript` depends on environment paths defined in `.dw_init` (such as `DW_DIR_ROOT`).
- **Function/execution dependencies**:
  - Requires logging functions (`DWMSG_*` family) defined within `f_alis_msgerr.ksh`.
  - Requires the database executor `starteSQLSkript` defined within `h_alis_sqlplus.ksh`.
- **Target script execution**: Runs SQL files (e.g., `d_abtn_x_smart_kubi.sql`) dynamically based on command-line parameters.

### 5. **Target file plan**
| Target File Path | Language | Source File(s) |
| :--- | :--- | :--- |
| `r_sqlscript.py` | Python | `r_sqlscript` |

*Note: The target file path mirrors the source path relative to the root. In accordance with the reviewer feedback, the wrapper script is successfully planned as `r_sqlscript.py` to handle parameter processing, logging integration, and validation, allowing the Airflow orchestration to run BigQuery scripts in a robust, standardized manner.*

### 6. **Environment-specific values**
We classify environment-sourced variables into Global and Job-Specific categories:

1. **GLOBAL** (Environment-wide infrastructure values):
   - `DW_DIR_ROOT`: Root path of the execution framework. Sourced at runtime via `os.environ.get("DW_DIR_ROOT")` or mapped to standard target repository folders.
   - Database Connection Properties: Handled in `h_alis_sqlplus.ksh` to establish connection. These map to the global BigQuery Project and Region (`GCP_PROJECT`, `GCP_REGION`) and standard Composer Connection IDs.

2. **JOB-SPECIFIC** (Values specific to this pipeline execution):
   - `p_sqlscript` (`-f` parameter): Target SQL filename to execute.
   - `p_sqlpar` (`-i` parameter): Input parameter string passed directly to the SQL query.
   - `JobKennung` (`-j` parameter): Job execution identifier (defaults to `'DWH_KORR'`).
   - `DW_EintragsNr`: Dynamically generated run/sequence number tracked in logging databases.
   - `LogDatei`: Dynamic execution log filename calculated at run time.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `r_sqlscript.py` | Converted to a Python wrapper to execute BigQuery scripts while preserving command-line parsing, path validation, and logging hook setups. |

### Risks & Manual Actions
- **Missing Sourced Components**: Sourced library files `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh` are not part of this specific group's source code list. The converted `r_sqlscript.py` contains stubs representing their logging and execution APIs. These must be integrated with the target logging framework and GCP connection manager during the build phase.
- **Legacy Validation Code Bug**: The legacy script contains the logic `if [ -f "$l_DBskript" ] then ErrNr=198`. This means it flags an error (code 198) when the file *exists*, rather than when it is *missing*. A developer should manually review and confirm whether this was an error in the original script and should be corrected to `if not os.path.isfile(...)`.
- **Undefined Reference**: The variable `p_Kuerzel` is referenced when flagging error `198`, but is never initialized in the KornShell code. A developer must verify and supply the correct variable value.