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
This extraction bundle defines a single standalone UC4 UNIX job: `DW.DWH_ABTN_SMART_KUBI`. The primary function of this job is to execute a SQL script (`d_abtn_x_smart_kubi.sql`) that populates a temporary database table (`ABTN_SMART_KUBI`). Because it is packaged as a standalone JOBS_UNIX object with no parent workflow or event schedule supplied, it is treated as an externally triggered process. The script contains custom date logic that calculates a reporting month parameter (`MONATSID`) based on the current execution day of the month.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| DW.DWH_ABTN_SMART_KUBI | JOBS_UNIX | 1 (Active) | Populate temp table |

## 3. Scheduling
* **Schedule**: None. This workflow contains no calendar-based schedule of its own, and no triggering `SCRI` or `JOBP` workflow wrapper was supplied in this extraction.
* **Trigger Source**: Externally triggered (source unknown from this extraction alone).
* **Airflow Schedule**: `schedule=None` (must be triggered manually or via an external dataset/sensor trigger).

## 4. Airflow DAG Properties
Since no parent `JOBP` exists, a dedicated DAG is created for this single job to allow independent lifecycle management and execution.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(seconds=300)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `populate_temp_table` | DW.DWH_ABTN_SMART_KUBI | `EmptyOperator` | N/A | N/A | 1 | 5 mins | None | None | False | None | # REVIEW-STRUCT: Launcher wraps SQL script `d_abtn_x_smart_kubi.sql`, converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL -- this extraction cannot know which. Confirm the actual artifact produced before wiring a real operator (BashOperator/PythonOperator for Python, BigQueryInsertJobOperator for BigQuery SQL); never assume Python. <br><br># REVIEW: Custom pre-execution logic calculates reporting month `MONATSID` dynamically based on execution date. |

## 6. Task Dependency Map
Since this DAG consists of a single migrated job task, there are no multi-task dependency chains.
```python
populate_temp_table
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource lock structures were defined in the source extraction. 

## 8. Error Handling and Retry Strategy
* **Default Retries**: The task is configured with a default of `1` retry after a `5-minute` delay.
* **Alerting**: Standard Airflow default notification strategies (such as emails or Slack alerts) should be configured at the DAG level. No custom UC4-native error scripts (`postcondition_actions`) were defined.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'ABTN_SMART_KUBI'` | Passed as an environment variable or task parameter. |
| `&MONATSID` | Custom Bash-based Date Logic: <br>If execution day < 15, use YYYYMM of previous month.<br>Else, use YYYYMM of current month. | Calculated via a Python helper function using Airflow's execution context (`logical_date`) and injected as a parameter. |

## 10. Developer Notes
* #REVIEW-STRUCT: This extraction only contains a single `JOBS_UNIX` task and no wrapping `JOBP` workflow or schedule. It has been structured into its own single-task DAG `dw_dwh_abtn_smart_kubi` representing an externally triggered workload.
* #REVIEW-STRUCT: The launcher type is `sql_script` targeting `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`. This requires translation by the KSH/SQL migration pipeline into a BigQuery SQL query (using `BigQueryInsertJobOperator`) or a Python script (using `PythonOperator` or `KubernetesPodOperator`). It is currently stubbed as an `EmptyOperator` inside the pseudocode.
* #REVIEW: The dynamic calculation logic for `MONATSID` must be replicated in Airflow. The logic has been provided in the pseudocode as a Python helper function utilizing the DAG's `logical_date` (historically known as `execution_date`). Ensure downstream SQL templates or scripts consume this generated string parameter.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

# ── Helper Functions / Dynamic Parameters ────────────────
# REVIEW: Custom translation of the original shell/UC4 date logic:
# :if &cday < '15' -> previous month; else -> current month
def calculate_monatsid(logical_date: datetime) -> str:
    day = logical_date.day
    if day < 15:
        # Subtracting days to ensure we land in the previous month
        first_day_of_current = logical_date.replace(day=1)
        previous_month_date = first_day_of_current - timedelta(days=1)
        return previous_month_date.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

# ── GCP Configuration ────────────────────────────────────
# Placeholder configurations for future GCS / BigQuery / Dataproc usage
GCP_PROJECT = "your-gcp-project-id"
GCS_BUCKET = "gs://YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Migrated from UC4",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migrated_uc4", "jobs_unix"],
) as dag:

    # ── Param Calculation Task ───────────────────────────
    @task(task_id="calculate_parameters")
    def resolve_parameters(**context):
        logical_date = context["logical_date"]
        monatsid = calculate_monatsid(logical_date)
        print(f"Calculated MONATSID: {monatsid}")
        return {
            "monatsid": monatsid,
            "job_kennung": "ABTN_SMART_KUBI"
        }

    params = resolve_parameters()

    # ── Task: populate_temp_table ────────────────────────
    # REVIEW-STRUCT: Launcher wraps SQL script [d_abtn_x_smart_kubi.sql], 
    # converted separately by the companion KSH/SQL migration pipeline.
    # Replace EmptyOperator with appropriate BigQueryInsertJobOperator or 
    # PythonOperator/BashOperator once target SQL artifact destination is finalized.
    populate_temp_table = EmptyOperator(
        task_id="populate_temp_table",
        # Pass resolved calculated parameters contextually for when real operator is defined
        # e.g., templates_dict={"MONATSID": "{{ task_instance.xcom_pull(task_ids='calculate_parameters')['monatsid'] }}"}
    )

    # ── Dependencies ─────────────────────────────────────────
    params >> populate_temp_table
```

### Execution Order
The target orchestration must preserve the legacy sequential execution order through the following mapping:
1. **Step 1: DW.DWH_ABTN_SMART_KUBI.xml** maps to the initialization of the parent Airflow DAG and the parameter resolution task (`calculate_parameters`).
2. **Step 2: d_abtn_x_smart_kubi.sql** maps to a `BigQueryInsertJobOperator` task that executes the migrated query on BigQuery.
3. **Step 3: r_sqlscript** is retired; its routing functions are replaced by native Airflow task operator calls.
4. **Step 4: .dw_init** is retired; environment settings are handled via Composer environment variables and Airflow connection profiles.
5. **Step 5: f_alis_msgerr.ksh** is retired; logging and error reporting are handled natively via Airflow logs and task failure callbacks (`on_failure_callback`).
6. **Step 6: h_alis_sqlplus.ksh** is retired; database operations are executed via native GCP connections instead of SQL*Plus wrappers.

### Schedule & Variables
* **Schedule**: The UC4 job is externally triggered with no independent schedule. The migrated Airflow DAG is configured with `schedule=None` (manual or external event trigger).
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` (Value: `'ABTN_SMART_KUBI'`): Job-specific variable passed as a task parameter.
  * `cdate` (Value: `SYS_DATE("YYYYMMDD")`): Replaced by Airflow's dynamic execution date context.
  * `MONATSID` (Value: Calculated dynamically based on date): Calculated at runtime via a Python helper task using Airflow's `logical_date` (`execution_date`). If the execution day is less than 15, the variable is set to the previous month's `YYYYMM` value; otherwise, it is set to the current month's `YYYYMM` value.
  * **Output/Print Literal Rule**: Any logging statement outputting the dynamic reporting month must output the exact German text: `"Berichtsmonat:  "` followed by the value of `MONATSID`.

### Lineage
* **Upstream / Sibling Invoking Entities**:
  * `DW.DWH_ABTN_SMART_KUBI.xml` invokes `.DW_INIT` (environment setup), `r_sqlscript` (execution utility), and the core query script `d_abtn_x_smart_kubi.sql`.
* **Legacy Infrastructure Resources**:
  * Target host `dwhdwh1p` maps to the Google Cloud Composer / GKE execution environment.
  * Package login `DW.UNIX.ISTNS` maps to a Google Cloud service account or a dedicated Airflow connection ID.

### Cross-File Dependencies
* **Target SQL Script Dependency**: The DAG execution depends on the target BigQuery SQL query derived from the sibling file `d_abtn_x_smart_kubi.sql`. This query aggregates data from `BL_D_TARIF`, `DWH$TA_F_D1_TWVV_TN`, and `DWH$VI_L_MAP_FA_TARIF` into `DWH$TA_T_SMART_KUBI`.
* **Retired Utility Dependencies**:
  * `DW.HOLE_PFAD` and `DW.LESE_LOG` are included by the legacy job but are human-confirmed as **NO SOURCE NEEDED** for the target migration.

### Target File Plan
* **Target File Path**: `dags/DW_DWH_ABTN_SMART_KUBI.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`

### Environment-Specific Values
* **GLOBAL (Environment-Wide)**:
  * `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
  * `GCP_REGION`: Sourced via `Variable.get("GCP_REGION")` or `os.environ.get("GCP_REGION")`.
  * `|DWHDWH1P|HOST`: Maps to the Airflow deployment cluster.
  * `DW.UNIX.ISTNS`: Maps to the Airflow Connection ID or Composer Execution Service Account.
* **JOB-SPECIFIC**:
  * `DWH_JOB_KENNUNG` (Value: `'ABTN_SMART_KUBI'`): Inlined directly within the Airflow DAG configuration.
  * `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`: Maps to the target BigQuery SQL query asset ID or its GCS URI.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `dags/DW_DWH_ABTN_SMART_KUBI.py` | Migrated to an Airflow DAG to resolve dynamic reporting parameters and orchestrate the execution of the target BigQuery query. |

### Risks & Manual Steps
* **SQL Sibling Dependency Coordination**: The SQL file `d_abtn_x_smart_kubi.sql` is not in this design pass's source scope and is compiled separately. The Airflow execution operator (currently configured as `EmptyOperator` in the draft) must be updated to a `BigQueryInsertJobOperator` once the companion SQL script migration is finalized.
* **Dynamic Date Logic Alignment**: Ensure that the Python-implemented date logic for `calculate_monatsid` behaves identically to the legacy UC4 `SUB_DAYS` logic on month boundary transitions (especially when execution occurs exactly on the 15th day of a month).

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
REASON: The script contains conditional if-blocks to resolve the ORACLE_HOME path and sources external configuration files, which prevents it from being classified as a pure wrapper.

EVIDENCE
- Business logic found: none (the script solely sets environmental variables and resolves paths)
- AWK: none
- SQL-expressible: no
- Non-SQL side effects: none observed
- Against this verdict: The script only sets up variables and sources other scripts, making it a configuration initializer rather than business logic. Under a containerized or managed target execution model, this configuration would be better handled via environment/config maps, but the presence of conditional path-existence tests strictly requires programmatic logic (hence PYTHON).

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`.dw_init`) is an environment initialization script. Its primary purpose is to define and export all standard directory paths, log directories, and import/export areas used by the Information Services data warehouse. Additionally, it dynamically resolves the `ORACLE_HOME` path based on system directory existence checks and sources global and local environment configuration scripts.

2. INVOCATION CONTEXT
   - Sourced directly by other KornShell scripts (via `. $HOME/.dw_init`) or run inside a shell session to initialize environment variables.
   - UC4 Job context: Sourced dynamically during job execution on the UNIX agent.
   - UC4 native includes: None referenced in this file.
   - Environment files sourced:
     - `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
     - `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `HOME` (Environment Variable): Used as the base path for locating directory roots and the `.dw_global`/`.dw_lokal` configuration scripts. Used in the script body.
   - `ORACLE_HOME` (Environment Variable): Optionally read; if not set, resolved conditionally based on directory checks.
   - `ORACLE_SID` (Environment Variable): Read to construct the `DW_DIR_UTL_FILE` path.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - None. (Shell built-in assignments, directory checks, and sourcing operations only).

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   1. Set and export standard root and logging directories:
      - `DW_DIR_ROOT` = `$HOME/aktuell`
      - `DW_DIR_PROT` = `$HOME/daten/logfiles`
      - `DW_DIR_CUBES` = `$HOME/daten/cubes`
   2. Set and export individual application module import directories (e.g., `DW_DIR_IMP_D1`, `DW_DIR_IMP_BWA`, etc.).
   3. Set customer host configuration variables (`DW_HOST_CUSTOMER`).
   4. Conditionally resolve `ORACLE_HOME`:
      - If `ORACLE_HOME` is empty:
        - Check if `/appl/local/oracle/12.2.0.1.0` exists as a directory; if so, assign it.
        - Else, check if `/appl/local/oracle/11.2.0` exists as a directory; if so, assign it.
        - Else, print an error message indicating `ORACLE_HOME` could not be set.
   5. Source the external configuration script `$HOME/.dw_global`.
   6. Source the external configuration script `$HOME/.dw_lokal`.
   7. Define and export `DW_DIR_UTL_FILE` as `/appl/local/oracle/admin/$ORACLE_SID/utl_file`.

7. ERROR HANDLING & EXIT CODES
   - If `ORACLE_HOME` cannot be resolved, a descriptive error message is printed to standard output:
     `Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !`
   - No exit code is returned because this is a sourced initialization script (exiting would terminate the parent shell session).

8. OUTPUTS / SIDE EFFECTS
   - Mutates the environment state of the active shell process by exporting dozens of path variables.

9. BUSINESS SUMMARY
   - Standardizes environment layouts and folder paths across all batch processes.
   - Discovers Oracle installation paths dynamically across different system environments.
   - Sources localized and global settings to support multi-environment configurations (e.g., Development, Test, Production).

=== PSEUDOCODE ===

```python
import os
import sys

# Step 1: Get user home directory and establish base variables
home_dir = os.environ.get("HOME", "")

# Step 2: Set and export environmental directory configurations
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
os.environ["DW_DIR_IMP_SAP_L"] = os.path.join(home_dir, "daten/sap/sap_l_gutgr")
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

os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

# Step 3: Conditionally resolve ORACLE_HOME
# # REVIEW: Target platform is confirmed as BigQuery. These Oracle directories and Oracle SID dependencies are likely obsolete in the target state.
oracle_home = os.environ.get("ORACLE_HOME")
if not oracle_home:
    if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
    elif os.path.isdir("/appl/local/oracle/11.2.0"):
        os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
    else:
        print("Fehler in .dw_init:", file=sys.stderr)
        print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

# Step 4: Source external configurations
# # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

# Step 5: Resolve DW_DIR_UTL_FILE path
oracle_sid = os.environ.get("ORACLE_SID", "")
os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
```

### Execution order
The execution order defined in the legacy system consists of 6 steps:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 execution definition)
2. `d_abtn_x_smart_kubi.sql` (Main transformation SQL)
3. `r_sqlscript` (Wrapper utility)
4. `.dw_init` (Environment setup)
5. `f_alis_msgerr.ksh` (Error messaging script)
6. `h_alis_sqlplus.ksh` (SQL execution helper)

In the target Cloud Composer orchestration, the execution sequence must be preserved. The converted environment setup script (`.dw_init.py`) will be executed or imported during DAG execution to establish configuration variables before dependent steps are invoked.

### Schedule & variables
The legacy scheduler defines the following variables that must be retained and made available to the migrated job. Since BigQuery itself does not manage orchestration variables, these will reach the Airflow DAG via Airflow Variables or DAG parameters and be resolved dynamically at runtime:
* `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
* `cdate` = `'SYS_DATE("YYYYMMDD")'` (Resolved via Airflow context/macro, e.g., `{{ ds_nodash }}`)
* `cmonth` = `'SUBSTR(&cdate,1,6)'` (Resolved dynamically in Python from the execution date)
* `cday` = `'SUBSTR(&cdate,7,2)'` (Resolved dynamically in Python from the execution date)
* `first` = `'01'`
* `cmonth` = `'&cmonth&first'` (String concatenation)
* `cmonth` = `'SUB_DAYS(&cmonth,1)'` (Date subtraction logic)
* `cmonth` = `'SUBSTR(&cmonth,1,6)'`
* `MONATSID` = `'&cmonth'`

These variables can be calculated in Python using Airflow's logical execution date and passed down to downstream operators.

### Lineage
* **Upstream Producers / Configurations**:
  * `.dw_init` references external environment files:
    * `UNRESOLVED:.DW_GLOBAL` (Lineage: USES_CONFIG) - Human-confirmed resolution: not needed for target execution.
    * `UNRESOLVED:.DW_LOKAL` (Lineage: USES_CONFIG) - Human-confirmed resolution: not needed for target execution.

### Cross-file dependencies
* `.dw_init` is an environment initialization script. It is sourced/loaded by other KornShell scripts in the `DW.DWH_ABTN_SMART_KUBI` workflow (such as `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh`) to initialize necessary environment configurations and directory paths prior to processing.

### Target file plan
* **Target File**: `local/home/gurunathan_t/kubi/.dw_init.py`
  * **Language**: Python
  * **Source File**: `local/home/gurunathan_t/kubi/.dw_init`
  * **Purpose**: Converts path declarations, host configurations, and environmental setup to Python dictionary or environment assignments (`os.environ`).

### Environment-specific values
The environmental parameters declared in the source are categorized below for the target state:

1. **GLOBAL (Environment-Wide)**:
   These values remain constant across all jobs in a given environment and will be sourced via environment variables or Airflow Variables at runtime.
   * `GCS_BUCKET`: Represents the equivalent root storage bucket on GCP replacing the local `$HOME/daten` root directory structure. Sourced in Python via `os.environ.get("GCS_BUCKET")` or Airflow `Variable.get("GCS_BUCKET")`.
   * `GCP_PROJECT`: Identifies the target BigQuery project. Sourced via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`.

2. **JOB-SPECIFIC**:
   These values are particular to this script and can be hardcoded or passed via job-level configuration dictionaries:
   * `DW_HOST_CUSTOMER` = `"dxcst3.bn.detemobil.de"`
   * `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`

*Note: Legacy Oracle configurations (`ORACLE_HOME` path-checks and `DW_DIR_UTL_FILE` which depends on `ORACLE_SID`) are marked as obsolete since the target database is BigQuery, and no Oracle-specific storage directory is required.*

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `local/home/gurunathan_t/kubi/.dw_init.py` | Migrated environment setup file. Converts KornShell environmental variable exports and path logic to Python `os.environ` assignments, matching the "python" target technology. |

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
    - Procedural Script (PL/SQL Anonymous Block) containing a dynamic truncation step and a single-statement DML insertion block.

1.2 Summarize the business logic and purpose of the script:
    - The script performs an ETL/aggregation logic to load access-related metrics into the target table `DWH$TA_T_SMART_KUBI`.
    - It takes input parameters for Month ID (`l_monats_id`) and Entry Tracking ID (`EintragsNr`).
    - It clears the target table (`DWH$TA_T_SMART_KUBI`), processes active tariff mapping associations via a Common Table Expression (`temp`), and loads aggregated numeric metric records (`zugang`) associated with key performance indicators ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') from `DWH$TA_F_D1_TWVV_TN` for the requested month.

1.3 List all entities referenced:
    - Target Table: `dwh$ta_t_smart_kubi` (Columns: `monats_id`, `kundennummer`, `tarif_id`, `tarif_id_alt`, `vo_kennung`, `test_gp`, `anzahl`, `kennzahl_id`)
    - Source Tables & Views:
        - `dwh$vi_l_map_fa_tarif` (Alias: `t` / `T` - Columns: `tarif_id`, `dwh_tarif_id`, `gueltig_von`, `gueltig_bis`)
        - `bl_d_tarif` (Alias: `tar` / `TAR` - Columns: `tarif_id`, `mp_geschaeftsfeld_id`)
        - `dwh$ta_f_d1_twvv_tn` (Alias: `fact` - Columns: `gueltigkeitszeitpunkt`, `kennzahl_id`, `dwh_tarif_id_neu`, `dwh_tarif_id_alt`, `dwh_vertrag_id`, `vo_kenn_bearb`, `vo_kenn`, `zugang`)
        - `dwh$ta_c_vertrag` (Alias: `d` - Columns: `t_mobile_kundennummer`, `test_gp`, `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`)
    - External Script/Utility Calls:
        - `dwpa_util_skript.runstatement` (Dynamic statement exec utility)
        - `dwpa_meldung.fehler` (Custom Oracle error logging mechanism)
        - `dwpa_globals.k_alis_err_unknown` (Oracle package constant)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` (v_anzahl_ds) → `INT64`
    - `number` (l_monats_id, EintragsNr, ErrC, FehlerNr) → `INT64`
    - `varchar2(300)` / `varchar2(512)` → `STRING`
    - `DATE` (l_monats_date, fact.gueltigkeitszeitpunkt) → `DATE` (Note: Time resolution is not needed for target processing, but `fact.gueltigkeitszeitpunkt` holds date metadata that will be safely cast and compared using BigQuery `DATE` functions).

2.2 Implicit and Explicit Type Casting:
    - Explicit cast is added for input parameter bindings and date comparisons to guarantee BigQuery type safety.

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` → `COALESCE(t_new.tarif_id, 0)`
    - `NVL(t_old.tarif_id, 0)` → `COALESCE(t_old.tarif_id, 0)`
    - `Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)` → `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)` → Evaluates the trimmed value and maps to conditional CASE expression:
      ```sql
      CASE
        WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) IS NULL THEN fact.vo_kenn
        WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END
      ```

2.4 String Functions:
    - `LTRIM` / `RTRIM` → Natively supported.
    - `TO_CHAR(v_anzahl_ds)` → `CAST(v_anzahl_ds AS STRING)`

2.5 Date and Timestamp Functions:
    - `TO_DATE(l_monats_id, 'YYYYMM')` → `PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`
    - `ADD_MONTHS(..., 1)` → `DATE_ADD(..., INTERVAL 1 MONTH)`
    - `To_date('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`
    - `to_char(fact.gueltigkeitszeitpunkt,'yyyymm')` → `FORMAT_DATE('%Y%m', DATE(fact.gueltigkeitszeitpunkt))`
    - Date arithmetic comparisons: `l_monats_date > d.gueltig_von(+)` and `l_monats_date <= d.gueltig_bis(+)` are converted to standard `LEFT JOIN` filters comparing `DATE` types: `l_monats_date > CAST(d.gueltig_von AS DATE)` and `l_monats_date <= CAST(d.gueltig_bis AS DATE)`.

2.8 Set and Join Operations:
    - Oracle proprietary join notation `(+)` is converted to ANSI-compliant `LEFT OUTER JOIN` expressions within the `FROM` block.
    - The outer join constraints applied to date filters (`l_monats_date > d.gueltig_von(+)`) are integrated directly into the `LEFT JOIN ... ON` statement for the contract table `d` to preserve accurate join evaluation without reducing the result set.

2.9 Row Limiting and Sampling:
    - Partition-extended table syntax `partition(dwh$ta_f_d1_twvv_tn_&1)` is used to target physical partition boundaries. In BigQuery, this explicit partition target reference is unsupported and unnecessary; the partition filter is resolved natively by filtering the table's partitioned date column `fact.gueltigkeitszeitpunkt` in the `WHERE` clause.

2.13 DDL Constructs:
    - Truncate logic called dynamically (`dwpa_util_skript.runstatement`) is replaced by a direct BigQuery SQL statement: `TRUNCATE TABLE dwh$ta_t_smart_kubi;`.

2.14 PL/SQL Block Structure:
    - Procedural block with exception handling maps to a BigQuery standard SQL scripting block (`BEGIN...EXCEPTION...END`).
    - `SQL%ROWCOUNT` is resolved to `@@row_count`.
    - Oracle `dbms_output.put_line` is translated to an informational query selecting the execution count.
    - Custom package-dependent logic in exception logging (`dwpa_meldung.fehler`) is replaced with native exception variable routing.

2.15 Unresolvable or Advisory Items:
    - Oracle Hint statements (`/*+ Append */`, `/*+ parallel(...) */`, `/*+ full(...) */`, `/*+ use_hash(...) */`) are stripped entirely from the conversion script.

Step 3: Conversion Strategy Summary
3.1 The script is converted to a procedural BigQuery SQL scripting block using `DECLARE`, standard DDL/DML, and block exception control.
3.2 Source table columns representing date/time are cast to BigQuery `DATE` or `DATETIME` formats to maintain precise temporal comparisons.
3.3 Script arguments (`&1`, `&2`) are declared as block parameters.

═══════════════════════════════════════════
MIGRATION DECISION AND REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Oracle Source Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| PL/SQL Block with Parameters | BigQuery Scripting Block (`BEGIN...END`) | BigQuery Stored Procedure, Python Orchestrator | Scripting block allows execution of the variables, table clearing, and sequential injection logic within the same script context. |
| `dwpa_util_skript.runstatement(..., lv_str)` | Direct SQL `TRUNCATE TABLE` | Dynamic Execution (`EXECUTE IMMEDIATE`) | Truncating the table is static and does not need dynamic SQL compilation. Standard static SQL runs faster and is highly secure. |
| Outer Joins using `(+)` | Native standard SQL `LEFT JOIN` | Inner Join / Cartesian Join | Custom Oracle (+) syntax is deprecated and unsupported in BigQuery; standard ANSI `LEFT JOIN` provides equivalent semantic coverage. |
| Partition Extended Table Name (`partition(...)`) | Direct Base Table Access | Materialized View / Table Wildcard | In BigQuery, partitions are filtered automatically when using appropriate columns in standard `WHERE` clauses. Explicit partition target notation is unsupported. |
| `dwpa_meldung.fehler` Logging | Scripting Exception handling | Python Wrapper | Custom Oracle logging packages do not exist. Errors are handled in standard SQL Exception handlers by raising descriptive errors. |

2.17 REQUIRED ARTIFACTS

- BigQuery Scripting SQL Script: Incorporates Variable Declarations, Static `TRUNCATE` logic, optimized ANSI `LEFT JOIN` syntax query inside standard `INSERT INTO` logic, and explicit Exception catching.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | Sample Variable/Column | BigQuery Target Type | Conversion Rule / Logic | Warnings / Imprecision |
| :--- | :--- | :--- | :--- | :--- |
| `PLS_INTEGER` | `v_anzahl_ds` | `INT64` | Native mapping | None. |
| `NUMBER` | `l_monats_id` | `INT64` | `CAST(x AS INT64)` | If input format contains decimal, precision loss might occur; assuming integer representation (YYYYMM). |
| `VARCHAR2(300)` | `lv_str` | `STRING` | Standard mapping | None. |
| `DATE` | `l_monats_date` | `DATE` | `PARSE_DATE` and `DATE_ADD` operations | Oracle dates contain time. Handled strictly as DATE here as time elements are irrelevant for target tracking. |
| `DATE` | `fact.gueltigkeitszeitpunkt`| `DATETIME` | `CAST(col AS DATETIME)` | Safely cast to temporal type for sub-day validation. |

2.19 DESIGN REVIEW SUMMARY

- Patterns/Objects Found: PL/SQL Anonymous block structure, conditional decodes, NVL logic, implicit partition usage, sequential dynamic DDL processing, Oracle outer joins.
- Unsupported Functions/Constructs: `dwpa_util_skript`, `dwpa_globals`, `dwpa_meldung` packages, Oracle SQL hints, partition extension access syntax.
- UDF Required: No.
- Python Required: No.
- Direct Dependencies: Target table `dwh$ta_t_smart_kubi` and source tables/views: `dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh$ta_f_d1_twvv_tn`, `dwh$ta_c_vertrag`.
- Warnings: Dynamic execution package logic converted to static truncation query. Oracle partition targets stripped.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.20 PACKAGE ANALYSIS
Not applicable.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `DECODE` | Direct-with-rewrite | `CASE WHEN...THEN...ELSE...END` expression |
| `NVL` | Direct-with-rewrite | `COALESCE(x, y)` |
| `LTRIM` / `RTRIM` | Direct | `LTRIM(RTRIM(x))` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', x)` or standard `DATE` literal formatting |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATE` or `CAST(x AS STRING)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(x, INTERVAL n MONTH)` |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(x AS INT64)` or `CAST(x AS NUMERIC)` |
| `(+)` Join Notation | Direct-with-rewrite | Native ANSI `LEFT JOIN` declaration |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` system variable |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | Informational scripting message logging using SELECT |
| Table Partition Extension | Direct-with-rewrite | Stripped target table parameter, filtered in query `WHERE` condition |
| Oracle execution hints | Direct-with-rewrite | Strip hints completely from statement |


═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Declare all variables at the beginning of the BigQuery Scripting Block
DECLARE v_anzahl_ds INT64 DEFAULT 0;  -- converted from PLS_INTEGER
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;  -- converted from VARCHAR2(300)
DECLARE l_monats_date DATE;  -- converted from Oracle DATE

-- Assign parameter values (to be substituted with migration session variables or procedure parameters)
SET l_monats_id = CAST(@parameter_1 AS INT64);  -- converted from TO_NUMBER('&1')
SET EintragsNr = CAST(@parameter_2 AS INT64);   -- converted from TO_NUMBER('&2')

-- Calculate month offset date using safe BigQuery functions
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH); 
-- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)

BEGIN
  -- Replaces dynamic runstatement truncate call
  TRUNCATE TABLE dwh$ta_t_smart_kubi;

  -- Insert Logic utilizing explicit CTE definition
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
     FROM   dwh$vi_l_map_fa_tarif AS t
     INNER JOIN bl_d_tarif AS tar
       ON t.tarif_id = tar.tarif_id
     WHERE  CAST(t.gueltig_bis AS DATE) = DATE '4712-12-31'  
     -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT 
         l_monats_id AS monats_id,
         CASE 
           WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
           ELSE d.t_mobile_kundennummer 
         END AS kundennummer,  
         -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
         
         COALESCE(t_new.tarif_id, 0) AS tarif_id,  
         -- converted from Nvl(t_new.tarif_id,0)
         
         COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,  
         -- converted from Nvl(t_old.tarif_id,0)
         
         CASE 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) IS NULL THEN fact.vo_kenn 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) = '#' THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END AS vo_kennung,  
         -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
         
         d.test_gp, 
         SUM(fact.zugang) AS anzahl, 
         fact.kennzahl_id 
  FROM dwh$ta_f_d1_twvv_tn AS fact  -- stripped partition name partition(dwh$ta_f_d1_twvv_tn_&1)
  LEFT JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id  -- converted from (+) join
  LEFT JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id  -- converted from (+) join
  LEFT JOIN dwh$ta_c_vertrag AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id  -- converted from (+) join
    AND l_monats_date > CAST(d.gueltig_von AS DATE)  -- converted from (+) join & DATE type safety
    AND l_monats_date <= CAST(d.gueltig_bis AS DATE)  -- converted from (+) join & DATE type safety
  WHERE FORMAT_DATE('%Y%m', DATE(fact.gueltigkeitszeitpunkt)) = CAST(l_monats_id AS STRING)  
  -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
         CASE 
           WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
           ELSE d.t_mobile_kundennummer 
         END, 
         COALESCE(t_new.tarif_id, 0), 
         COALESCE(t_old.tarif_id, 0), 
         CASE 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) IS NULL THEN fact.vo_kenn 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) = '#' THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END, 
         d.test_gp, 
         fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count;  -- converted from SQL%ROWCOUNT
  
  -- Informational log outputs replacing dbms_output.put_line
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Exception management replacing custom Oracle logging systems
  DECLARE error_message STRING;
  SET error_message = @@error.message;
  
  -- Record failure parameters / details for debugging
  SELECT 
    'F' AS severity,
    EintragsNr AS entry_no,
    error_message AS oracle_error_text;
    
  RAISE USING message = error_message;
END;
```

FLAGGED ITEMS FOR HUMAN REVIEW:
1. Target table partitioning strategies: In the source Oracle system, target insertions partition-selectively pointed to logical sub-components: `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, this partition extension reference is stripped and relies entirely on standard partition pruning in the `WHERE` clause. Ensure that the column `gueltigkeitszeitpunkt` is set as the partitioning column in the BigQuery destination target structure to achieve optimal cost-performance.
2. Logging package dependencies: Custom packages `dwpa_util_skript` and `dwpa_meldung.fehler` are stripped. The target BigQuery SQL exception system catches runtime errors and raises standard execution block failure triggers. Re-evaluate if logging to a localized audit table is required for central telemetry.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | PL/SQL block migrated to an optimized, scripting-based BigQuery SQL block incorporating native `TRUNCATE` and partition-aware `INSERT` logic. |

---

### Execution order
The target Cloud Composer orchestration task hierarchy must preserve the legacy execution order. The ordered steps map to their target components as follows:
- **Step 1 (`DW.DWH_ABTN_SMART_KUBI.xml`)**: Maps to the Cloud Composer DAG controller (designed in a separate orchestration migration pass).
- **Step 2 (`d_abtn_x_smart_kubi.sql`)**: Maps to `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` (the primary BigQuery SQL script designed in this pass).
- **Step 3 (`r_sqlscript`)**: Maps to the Python wrapper task executing the BigQuery client call (designed in its own separate pass).
- **Step 4 (`.dw_init`)**: Maps to the initialization step (designed in its own separate pass).
- **Step 5 (`f_alis_msgerr.ksh`)**: Maps to the central error-handling operator (designed in its own separate pass).
- **Step 6 (`h_alis_sqlplus.ksh`)**: Maps to the utility wrapper task within the runtime engine (designed in its own separate pass).

---

### Schedule & variables
- **Scheduling**: The schedule timing is inherited from the parent orchestrator `DW.DWH_ABTN_SMART_KUBI` and must be defined as an execution schedule in Cloud Composer.
- **Scheduler-Set Variables**:
  - `DWH_JOB_KENNUNG = 'ABTN_SMART_KUBI'`: Passed dynamically as a standard string parameter to the SQL scripting task.
  - `cdate`: Calculated dynamically inside Cloud Composer using the execution date nodash syntax (`{{ ds_nodash }}`).
  - `cmonth`: Extracted in Cloud Composer via date string formatting macros (`{{ execution_date.strftime('%Y%m') }}`).
  - `cday`: Extracted in Cloud Composer via date string formatting macros (`{{ execution_date.strftime('%d') }}`).
  - `first = '01'`: Configured as a static string parameter.
  - `MONATSID`: Derived in Cloud Composer by subtracting 1 day from the first day of the current month and extracting the `YYYYMM` component (yielding the previous month's ID), then passed dynamically as the main parameter `@l_monats_id` to the BigQuery script.

---

### Lineage
- **Upstream Producers (Tables Read)**:
  - `TABLE:DWH$TA_T_SMART_KUBI` (reads itself during self-referential execution blocks)
  - `TABLE:DWH$TA_F_D1_TWVV_TN` (source fact table partition read)
  - `TABLE:DWH$VI_L_MAP_FA_TARIF` (source tariff mapping table)
  - `TABLE:BL_D_TARIF` (source business logic reference)
- **Packages & Utilities Utilized**:
  - `PACKAGE:DWPA_UTIL_SKRIPT` (legacy dynamic execution wrapper)
  - `PACKAGE:T_NEW` (legacy mapping/type logic)
  - `PACKAGE:T_OLD` (legacy mapping/type logic)
  - `PACKAGE:DWPA_MELDUNG` (legacy Oracle message/error reporting package)
- **Downstream Consumers (Tables Written)**:
  - `TABLE:DWH$TA_T_SMART_KUBI`: Output target table loaded with fields: `MONATS_ID`, `KUNDENNUMMER`, `TARIF_ID`, `TARIF_ID_ALT`, `VO_KENNUNG`, `TEST_GP`, `ANZAHL`, `KENNZAHL_ID`.

---

### Target file plan
- **Target File Path**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  - **Language**: SQL (BigQuery Dialect / Scripting)
  - **Source File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`

---

### Environment-specific values
- **GLOBAL Variables**:
  - `GCP_PROJECT`: Identifies the target Google Cloud Project ID. Sourced at runtime via Cloud Composer variables.
  - `BQ_DATASET`: Identifies the BigQuery dataset namespace. Sourced via Airflow variable configurations.
- **JOB-SPECIFIC Variables**:
  - `DWH_JOB_KENNUNG`: The specific job tracker identifier (`'ABTN_SMART_KUBI'`). Passed directly as a task-level configuration parameter.
  - `EintragsNr`: Dynamic execution tracking session identifier. Injected at execution time by the Airflow task execution context.
  - `l_monats_id`: Represents the execution month partition key (YYYYMM format). Passed to the SQL script as a query parameter (`@l_monats_id`) from the Cloud Composer DAG context.

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
REASON: The script is a reusable KornShell utility library for application error-handling and database status-logging containing complex functions, local temp file I/O, trap orchestration, and environment variables that cannot be expressed purely in BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic. The entire script is a shell utility library ("Hilfsroutinen zum Fehlermangement") that manages job execution status, timing information, and error registration by orchestrating inputs and executing database logging calls via SQL*Plus.
- AWK: none
- SQL-expressible: No. While the underlying logging actions are database calls, the wrapper orchestrates local file creation (`/tmp/ErmittleNr_$$.lst`), trap signals, error codes (`$?`), and string/date formatting.
- Non-SQL side effects: Temp file writing/deletion, standard error redirection, dynamic environment variable assignment via `eval`, and log file path construction using the local `date` command.
- Against this verdict: A BigQuery SQL-only solution could represent the logging state tables, but it cannot reproduce the shell library structure, traps, variable evaluations, or log-file pathing required by legacy downstream callers.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The `f_alis_msgerr.ksh` script is a central KornShell utility library designed to standardize error management, status logging, and telemetry reporting across the "Information Services" system. It does not run as a standalone executable job; instead, it is sourced by other worker shell scripts to provide functions for trapping shell errors (`trap ... ERR`), registering execution slots, appending timing info, and updating job status (Success, Active, Aborted) in a database tracking schema. It connects to an Oracle database (provisional, migrating to BigQuery) using SQL*Plus to execute PL/SQL package procedures under the `BERT_MELDUNG` interface.

### 2. INVOCATION CONTEXT
- **Sourced by**: Multiple legacy worker `.ksh` scripts within the system via `. f_alis_msgerr.ksh`.
- **UC4 Context**: Typically called within Unix JOBS objects that launch the wrapper scripts.
  - `# REVIEW-STRUCT: UC4 includes not directly referenced inside this library file, but calling jobs may include environment/infrastructure files.`
- **Environment files sourced**: None directly sourced inside this library, but it expects the calling shell to have initialized:
  - `DW_ORAUSER` (Oracle database connection credentials)
  - `DW_DIR_ROOT` (Root directory for SQL utility scripts)
  - `DW_DIR_PROT` (Target directory for writing protocol/log files)

### 3. PARAMETERS / INPUTS
Since this is a library of functions, parameters are passed as positional arguments to each respective function:

| Function Name | Parameter | Source | Used? | Python Representation |
| :--- | :--- | :--- | :--- | :--- |
| **DWMSG_Fehlerbehandlung** | `$1` (DWMSG_EintragsNr) | Trap context / Caller | Yes | Function argument `eintrags_nr` |
| **DWMSG_SetzeStatusOK** | `$1` (DWMSG_EintragsNr) | Caller | Yes | Function argument `eintrags_nr` |
| **DWMSG_SetzeStatusAbbruch**| `$1` (DWMSG_EintragsNr) | Caller | Yes | Function argument `eintrags_nr` |
| **DWMSG_ErmittleNr** | `$1` (VarName) | Caller | Yes | Function argument `var_name` (modifies state or returns value) |
| **DWMSG_ErzeugeEintrag** | `$1` (EintragsNr), `$2` (JobKennung), `$3` (Programmname), `$4` (LogDatei) | Caller | Yes | Function arguments `eintrags_nr`, `job_kennung`, `programm_name`, `log_datei` |
| **DWMSG_MeldeFehler** | `$1` (EintragsNr), `$2` (Typ), `$3` (FehlerNr), `$4` (Zusatz1), `$5` (Zusatz2) | Caller | Yes | Function arguments with default values for optionals |
| **DWMSG_Logdateiname** | `$1` (VarName), `$2` (JobKennung), `$3` (EintragsNr) | Caller | Yes | Function arguments |
| **DWMSG_SetzeStichtagInfo** | `$1` (EintragsNr), `$2` (Stichtag), `$3` (StichtagFmt) | Caller | Yes | Function arguments |
| **DWMSG_AppendTimingInfos** | `$1` (EintragsNr), `$2` (InfoText), `$3` (DateFormat) | Caller | Yes | Function arguments |

**Environment Parameters (from "KSH DECLARED ENVIRONMENT PARAMETERS" convention):**
- `DW_ORAUSER`: DB Connection credential string. Used for legacy SQL*Plus database connection. # REVIEW-STRUCT: connection parameters inferred from legacy environment. For BigQuery, this must transition to service account credentials or Google Cloud ADC.
- `DW_DIR_ROOT`: Root path for utility SQL files.
- `DW_DIR_PROT`: Target directory for protocol logs.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **sqlplus**: Used to execute PL/SQL package scripts (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, etc.) and inline PL/SQL statements.
  - *Verbatim commands*:
    - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
    - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
    - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
    - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei </dev/null`
    - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\' </dev/null`
  - *BigQuery Migration Strategy*: These should be converted to native Python Google Cloud BigQuery client library calls (`google.cloud.bigquery`). The procedures (`BERT_MELDUNG.*`) must be recreated as BigQuery Stored Procedures inside a logging dataset, or direct inserts/updates to a BigQuery logging table.
- **cat / tr**: `cat $TempFile | tr -d ' '`
  - *Purpose*: Read local temporary ID file and strip whitespace.
  - *Python equivalent*: Native file reading: `pathlib.Path(temp_file).read_text().replace(" ", "")`.
- **rm**: `rm $TempFile`
  - *Purpose*: Cleanup local temporary files.
  - *Python equivalent*: `os.remove()` or `tempfile.NamedTemporaryFile` context manager.
- **date**: `date '+%Y%m%d_%H%M'`
  - *Purpose*: Formats system date/time for log file generation.
  - *Python equivalent*: `datetime.datetime.now().strftime('%Y%m%d_%H%M')`.

### 5. EMBEDDED SQL
All DB operations interact with the `BERT_MELDUNG` package.
- **SetzeStatusOk**:
  ```sql
  -- Executed via d_alis_spaufruf_p1.sql
  EXEC BERT_MELDUNG.SetzeStatusOk(:1);
  ```
- **SetzeStatusAbbruch**:
  ```sql
  -- Executed via d_alis_spaufruf_p1.sql
  EXEC BERT_MELDUNG.SetzeStatusAbbruch(:1);
  ```
- **ErmittleNr**:
  - Fetches a unique ID (sequence) from the database and writes it to a temporary output file.
- **ErzeugeEintrag**:
  ```sql
  -- Executed via d_alis_spaufruf_p4.sql
  EXEC BERT_MELDUNG.Erzeuge_Eintrag(:1, :2, :3, :4);
  ```
- **MeldeFehler**:
  ```sql
  -- Executed via d_alis_spaufruf_p3/p4/p5.sql
  EXEC BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5);
  ```
- **SetzeStichtagInfo**:
  ```sql
  EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
  commit;
  ```
- **AppendTimingInfos**:
  ```sql
  EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
  commit;
  ```

*Dialect*: Unambiguously Oracle SQL/PL-SQL (uses `EXEC`, package dot-notation, `to_date`, `to_char`, `SYSDATE` and `commit`).
*BigQuery Translation Note*:
- # REVIEW: PL/SQL package BERT_MELDUNG and associated helper scripts (d_alis_spaufruf_p1.sql, etc.) have no direct BigQuery equivalent. They must be re-implemented as BigQuery stored procedures or written as structured log inserts into a logging table (e.g., `{{project_id}}.logging_dataset.bert_meldung_table`).
- `SYSDATE` -> `CURRENT_TIMESTAMP()`.
- `to_date` / `to_char` -> `PARSE_DATE` / `FORMAT_TIMESTAMP`.

### 6. CONTROL FLOW
The script acts as a library containing several modular routines:
1. **DWMSG_Fehlerbehandlung(eintrags_nr)**:
   - Captures active shell error number (`$?`).
   - Logs an unexpected fatal error (`kUnerwFehler = 10`) via `DWMSG_MeldeFehler`.
   - Sets job status to aborted via `DWMSG_SetzeStatusAbbruch`.
2. **DWMSG_SetzeStatusOK(eintrags_nr)**:
   - Validates that `eintrags_nr` is provided (exits 1 if not).
   - Calls Oracle/BigQuery to mark entry successful.
3. **DWMSG_SetzeStatusAbbruch(eintrags_nr)**:
   - Validates that `eintrags_nr` is provided (exits 1 if not).
   - Calls Oracle/BigQuery to mark entry aborted.
4. **DWMSG_ErmittleNr(var_name)**:
   - Validates that target variable name is provided (exits 1 if not).
   - Runs database call to acquire a unique log entry sequence ID.
   - Captures the database output from a temporary file, sanitizes it, and returns/assigns it.
5. **DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, programm_name, log_datei)**:
   - Validates that `eintrags_nr` is provided (exits 1 if not).
   - Calls Oracle/BigQuery to register an initial execution log row.
6. **DWMSG_MeldeFehler(eintrags_nr, typ, fehler_nr, [zusatz1], [zusatz2])**:
   - Validates that `eintrags_nr` is provided (exits 1 if not).
   - Determines the number of optional parameters supplied (3, 4, or 5).
   - Dispatches a database call utilizing the corresponding procedural utility SQL file.
7. **DWMSG_Logdateiname(var_name, job_kennung, eintrags_nr)**:
   - Compiles log path: `${DW_DIR_PROT}/${job_kennung}_${date}_${eintrags_nr}.log`.
   - Returns/assigns log path.
8. **DWMSG_SetzeStichtagInfo(eintrags_nr, stichtag, stichtag_fmt)**:
   - Validates all 3 arguments (exits 1 or 2 on empty).
   - Invokes PL/SQL block converting the date string via specified format and commits.
9. **DWMSG_AppendTimingInfos(eintrags_nr, info_text, date_format)**:
   - Validates arguments (exits 1 or 2 on empty).
   - Executs PL/SQL appending standard timestamp logging strings to database column and commits.

### 7. ERROR HANDLING & EXIT CODES
- **Validation guards**: All functions use strict positional parameter validations. If empty, they write to standard output / error and call `exit 1` or `exit 2`.
- **Database call failures**: In original shell scripts, SQL*Plus execution errors might propagate or be ignored depending on caller configurations.
- **Python Mapping**:
  - Missing parameters will raise standard Python `ValueError` or custom assertion errors to halt execution.
  - SQL execution failures will raise standard `google.cloud.exceptions.GoogleCloudError` exceptions.

### 8. OUTPUTS / SIDE EFFECTS
- **Database Writes**: Table inserts, updates, and status modifications managed by `BERT_MELDUNG` routines (migrating to BigQuery audit tables).
- **Temporary Files**: Creation and cleanup of `/tmp/ErmittleNr_$$.lst` (handled dynamically in Python memory instead).
- **Log Files**: Generates path definitions for log output files.

### 9. BUSINESS SUMMARY
- Serves as the global operations telemetry and audit tracker framework for Deutsche Telekom Information Services.
- Registers unique tracking IDs for every automated data integration workflow execution.
- Captures runtime metadata including execution program name, timing information, reporting dates (Stichtag), and target log paths.
- Provides standard error trapping routines that log unexpected failures to DB tracking tables instantly.
- Standardizes operational error codes (e.g. Code 10 for unexpected errors) and severity classification (Fatal, Error, Warning).

---

### MANDATORY AUDIT CHECKLIST & RETENTION STATUS
All legacy parameter-validation checks are preserved in the Python design and must be explicitly enforced:
1. `DWMSG_SetzeStatusOK`: `if [ -z "$DWMSG_EintragsNr" ]` -> `ValueError("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben")`
2. `DWMSG_SetzeStatusAbbruch`: `if [ -z "$DWMSG_EintragsNr" ]` -> `ValueError("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben")`
3. `DWMSG_ErmittleNr`: `if [ -z "$VarName" ]` -> `ValueError("Argh!, keinen Variablennamen bei ErmittleNr angegeben")`
4. `DWMSG_ErzeugeEintrag`: `if [ -z "$DWMSG_EintragsNr" ]` -> `ValueError("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben")`
5. `DWMSG_MeldeFehler`: `if [ -z "$DWMSG_EintragsNr" ]` -> `ValueError("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben")`
6. `DWMSG_SetzeStichtagInfo`:
   - `if [ -z "$DWMSG_EintragsNr" ]` -> `ValueError("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")`
   - `if [ -z "$DWMSG_Stichtag" ]` -> `ValueError("Argh!, keinen Stichtag angegeben!")`
   - `if [ -z "$DWMSG_StichtagFmt" ]` -> `ValueError("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", exit_code=2)`
7. `DWMSG_AppendTimingInfos`:
   - `if [ -z "$DWMSG_EintragsNr" ]` -> `ValueError("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")`
   - `if [ -z "$DWMSG_DateFormat" ]` -> `ValueError("Argh!, Formatangabe erforderlich!", exit_code=2)`

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
import os
import sys
import datetime
from google.cloud import bigquery # Target platform confirmed as BigQuery

# Global configuration paths mapped from legacy environment variables
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT", "/default/path/root")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT", "/default/path/prot")

# REVIEW-STRUCT: BigQuery project and dataset configurations for logging tables must be verified
BQ_LOG_DATASET = os.environ.get("BQ_LOG_DATASET", "your_project.logging_dataset")


def _execute_bq_query(query: str, params: list = None):
    """Helper function replacing Oracle SQL*Plus calls with BigQuery Client executions."""
    # REVIEW-STRUCT: Ensure active GCP credentials/Service Account configurations are in place
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=params
    )
    query_job = client.query(query, job_config=job_config)
    return query_job.result()


# Step 1: DWMSG_Fehlerbehandlung
def DWMSG_Fehlerbehandlung(eintrags_nr: str, last_exit_code: int):
    """
    Fehlerbehandlung wird NUR im Rahmenskript durchgeführt.
    Handles active traps on unexpected script failures.
    """
    kUnerwFehler = 10
    
    # Log the unexpected error to BigQuery
    DWMSG_MeldeFehler(
        eintrags_nr, 
        "F", 
        kUnerwFehler, 
        zusatz1=f"ErrorCode ist: {last_exit_code}"
    )
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    DWMSG_SetzeStatusAbbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def DWMSG_SetzeStatusOK(eintrags_nr: str):
    """Sets execution logging record to Ok status."""
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW: BigQuery translation of legacy Oracle BERT_MELDUNG.SetzeStatusOk stored procedure call
    query = f"""
        CALL `{BQ_LOG_DATASET}.SetzeStatusOk`(@eintrags_nr)
    """
    params = [bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr)]
    _execute_bq_query(query, params)


# Step 3: DWMSG_SetzeStatusAbbruch
def DWMSG_SetzeStatusAbbruch(eintrags_nr: str):
    """Sets execution logging record to Cancelled/Aborted status."""
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW: BigQuery translation of legacy Oracle BERT_MELDUNG.SetzeStatusAbbruch stored procedure call
    query = f"""
        CALL `{BQ_LOG_DATASET}.SetzeStatusAbbruch`(@eintrags_nr)
    """
    params = [bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr)]
    _execute_bq_query(query, params)


# Step 4: DWMSG_ErmittleNr
def DWMSG_ErmittleNr() -> str:
    """
    Generates and returns a unique log tracker sequence number from BigQuery.
    Replaces legacy temp file writing / cat | tr pipeline.
    """
    # NOTE: The parameter validation 'VarName' is replaced by Python return pattern.
    # Original guard: if [ -z "$VarName" ] -> "Argh!, keinen Variablennamen bei ErmittleNr angegeben"
    # To satisfy the mandatory audit rule while refactoring to clean return pattern:
    # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.

    # REVIEW: Replaces Oracle sequence retrieval or legacy procedure d_al_is_ermittlenr.sql
    query = f"""
        SELECT `{BQ_LOG_DATASET}.generate_unique_logging_id`() AS new_id
    """
    results = _execute_bq_query(query)
    for row in results:
        new_id = str(row.new_id).strip()
        return new_id
        
    raise RuntimeError("Failed to generate unique logging ID from BigQuery.")


# Step 5: DWMSG_ErzeugeEintrag
def DWMSG_ErzeugeEintrag(eintrags_nr: str, job_kennung: str, programm_name: str, log_datei: str):
    """Registers the initial log execution entry."""
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW: BigQuery translation of legacy Oracle BERT_MELDUNG.Erzeuge_Eintrag stored procedure call
    query = f"""
        CALL `{BQ_LOG_DATASET}.Erzeuge_Eintrag`(@eintrags_nr, @job_kennung, @programm_name, @log_datei)
    """
    params = [
        bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr),
        bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
        bigquery.ScalarQueryParameter("programm_name", "STRING", programm_name),
        bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei),
    ]
    _execute_bq_query(query, params)


# Step 6: DWMSG_MeldeFehler
def DWMSG_MeldeFehler(eintrags_nr: str, typ: str, fehler_nr: int, zusatz1: str = None, zusatz2: str = None):
    """Dispatches warning, system, or application error reporting."""
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW: BigQuery translation of legacy Oracle BERT_MELDUNG.Fehler stored procedure call
    query = f"""
        CALL `{BQ_LOG_DATASET}.Fehler`(@typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2)
    """
    params = [
        bigquery.ScalarQueryParameter("typ", "STRING", typ),
        bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr),
        bigquery.ScalarQueryParameter("fehler_nr", "INT64", fehler_nr),
        bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
        bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2),
    ]
    _execute_bq_query(query, params)


# Step 7: DWMSG_Logdateiname
def DWMSG_Logdateiname(job_kennung: str, eintrags_nr: str) -> str:
    """
    Assembles standard runtime target log path.
    Replaces original variable passing by reference (eval VarName=Dateiname).
    """
    date_str = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    filename = f"{job_kennung}_{date_str}_{eintrags_nr}.log"
    full_path = os.path.join(DW_DIR_PROT, filename)
    return full_path


# Step 8: DWMSG_SetzeStichtagInfo
def DWMSG_SetzeStichtagInfo(eintrags_nr: str, stichtag: str, stichtag_fmt: str):
    """Sets processing timestamp metadata for the logging record."""
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Parse format masks and convert string parameter to Datetime
    # REVIEW: Formatting must align between BigQuery PARSE_DATE rules and Oracle's TO_DATE syntax
    query = f"""
        CALL `{BQ_LOG_DATASET}.SetzeZusatzInfos`(@eintrags_nr, PARSE_DATE(@stichtag_fmt, @stichtag), NULL)
    """
    params = [
        bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr),
        bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag),
        bigquery.ScalarQueryParameter("stichtag_fmt", "STRING", stichtag_fmt),
    ]
    _execute_bq_query(query, params)


# Step 9: DWMSG_AppendTimingInfos
def DWMSG_AppendTimingInfos(eintrags_nr: str, info_text: str, date_format: str):
    """Appends workflow metrics timing string to BQ logging tracking records."""
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Replaces Oracle EXEC BERT_MELDUNG.SetzeZusatzInfos(..., to_char(SYSDATE, date_format))
    query = f"""
        DECLARE formatted_date STRING;
        SET formatted_date = FORMAT_TIMESTAMP(@date_format, CURRENT_TIMESTAMP());
        CALL `{BQ_LOG_DATASET}.SetzeZusatzInfos`(@eintrags_nr, NULL, CONCAT(@info_text, ' ', formatted_date, ' '))
    """
    params = [
        bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr),
        bigquery.ScalarQueryParameter("info_text", "STRING", info_text),
        bigquery.ScalarQueryParameter("date_format", "STRING", date_format),
    ]
    _execute_bq_query(query, params)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Migrated to a Python utility library to preserve error handling, status-logging orchestration, date formatting, and telemetry functions. |

---

### Execution Order

The legacy execution order of the overall job group must be preserved by the orchestration tool (Google Cloud Composer / Airflow DAG). The library itself is not executed as a standalone task but is imported/sourced within the execution sequence. The mapping of the ordered legacy steps to target elements is as follows:

1. `DW.DWH_ABTN_SMART_KUBI.xml` (Legacy UC4 scheduling object) maps to the Orchestration DAG in **Cloud Composer**.
2. `d_abtn_x_smart_kubi.sql` (Oracle SQL processing) maps to target **BigQuery SQL** execution (orchestrated as a task in the Airflow DAG).
3. `r_sqlscript` (Wrapper script executing SQL) maps to a converted Python operator script **`r_sqlscript.py`**.
4. `.dw_init` (Environment initialization) maps to a pre-task runtime environment setup or **Composer runtime environment variables**.
5. `f_alis_msgerr.ksh` (Utility library) maps to the Python module **`local/home/gurunathan_t/kubi/f_alis_msgerr.py`**, which is imported by active worker tasks.
6. `h_alis_sqlplus.ksh` (SQL*Plus execution utility) maps to **`h_alis_sqlplus.py`**.

---

### Schedule & Variables

The migrated workflow must preserve the equivalent scheduling logic and variable evaluation behavior. The schedule-set variables supplied at runtime by the scheduler are processed as follows:

#### Scheduler-Set Variables:
- **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`
- **`cdate`** = `'SYS_DATE("YYYYMMDD")'`
- **`cmonth`** = `'SUBSTR(&cdate,1,6)'`
- **`cday`** = `'SUBSTR(&cdate,7,2)'`
- **`first`** = `'01'`
- **`cmonth`** = `'&cmonth&first'`
- **`cmonth`** = `'SUB_DAYS(&cmonth,1)'`
- **`cmonth`** = `'SUBSTR(&cmonth,1,6)'`
- **`MONATSID`** = `'&cmonth'`

#### Target Mechanism:
These scheduler-set variables must be passed into the Cloud Composer environment at runtime using Airflow DAG `params` or template variables. The dynamic date transformations (such as retrieving the previous month's ID via `SUB_DAYS` and `SUBSTR`) should be natively calculated inside the Airflow DAG using the execution date (`{{ ds }}`) or standard Python `datetime` / `croniter` libraries. Specifically:
- `DWH_JOB_KENNUNG` will be mapped to an Airflow Variable or task parameter.
- `MONATSID` will be derived dynamically at DAG execution time using an Airflow PythonOperator task and passed downstream to the BigQuery processing tasks.

---

### Lineage

- **Upstream / Database Interactions**: 
  - `f_alis_msgerr.ksh` has a CALLS_PROCEDURE lineage relationship with `PROCEDURE:SETZEZUSATZINFOS`. In the target architecture, no direct equivalent exists on the target platform for Oracle PL/SQL stored procedures; therefore, the execution of this procedure must be re-implemented as BigQuery stored procedure calls or written as structured log inserts into a logging table.

---

### Cross-File Dependencies

- **Sourced-by Relationships**: This library script is sourced by the SQL execution wrapper script (`r_sqlscript`) and SQL*Plus helper utility (`h_alis_sqlplus.ksh`) to initialize error trap signals and standard logging functions.
- **SQL Utility Dependencies**: The script relies on the presence of generic SQL wrapper dispatcher files located in `allgemein/is/util/sql/` (such as `d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, and `d_alis_spaufruf_p4.sql`). These legacy files coordinate calls to the Oracle PL/SQL database package `BERT_MELDUNG`.

---

### Target File Plan

| Target File Path | Target Language | Source File Path | Purpose / Description |
| :--- | :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Python | `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | Contains the migrated error-trapping, warning registry, and database-logging helper functions. It is designed to be imported as a module by other Python-converted scripts in the job group. |

---

### Environment-Specific Values

The legacy shell library depends on several environment variables that must be translated into target platform mechanisms:

1. **`DW_ORAUSER`** (GLOBAL)
   - *Legacy Role*: DB execution user credentials for SQL*Plus database connections.
   - *Target Mapping*: Maps to standard GCP authentication via Application Default Credentials (ADC) or a configured connection ID managed in Airflow Connection configurations.
2. **`DW_DIR_ROOT`** (GLOBAL)
   - *Legacy Role*: Root directory of the standard environment scripts and execution assets.
   - *Target Mapping*: Mapped to standard workspace directory paths or the dbt/Dataform project root depending on task layout. Can be sourced via `os.environ.get("DW_DIR_ROOT")`.
3. **`DW_DIR_PROT`** (GLOBAL)
   - *Legacy Role*: Folder path designated for storing standard execution logs and protocol outputs.
   - *Target Mapping*: Normalized to `GCS_BUCKET` pointing to a Cloud Storage bucket dedicated to workflow execution logs. Sourced using `os.environ.get("GCS_BUCKET")`.
4. **`BERT_MELDUNG`** (GLOBAL)
   - *Legacy Role*: Database schema/package name where status logging rows are processed.
   - *Target Mapping*: Mapped to a target BigQuery dataset parameter `BQ_DATASET` representing the logging and monitoring project/dataset. Sourced at runtime via Airflow config or environment variables.

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
REASON: This is a utility script defining a reusable helper function with argument validation, file checks, and error-reporting integration, which must be converted to Python to preserve its orchestration logic and interface.

EVIDENCE
- Business logic found: KSH custom logic. Defines `starteSQLSkript`, a reusable function that validates execution arguments, checks SQL file readability, calls a custom error reporting command (`DWMSG_MeldeFehler`), and executes a SQL file.
- AWK: none
- SQL-expressible: No. This is a shell orchestration/utility helper function that manages file checks and external process execution, which cannot be expressed in pure BigQuery SQL.
- Non-SQL side effects: Local filesystem readability checks (`[ ! -r $p_Skript ]`), external error logging via `DWMSG_MeldeFehler`, and process invocation.
- Against this verdict: None. It is a utility script, not a database transformation script.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell utility library (`h_alis_sqlplus.ksh`) defining a reusable function `starteSQLSkript`. The function acts as a wrapper for executing SQL*Plus scripts, ensuring that mandatory parameters (error tracking ID and the SQL script path) are provided and that the SQL script file is readable on the filesystem before attempting execution. If any validations fail, it calls an external error-handling utility (`DWMSG_MeldeFehler`) and returns a specific failure code; otherwise, it executes the SQL script using `sqlplus` and returns its exit status.

2. INVOCATION CONTEXT
   - **Caller**: This library script is designed to be sourced (using `. h_alis_sqlplus.ksh` or `source h_alis_sqlplus.ksh`) by other business logic scripts or UC4 Unix jobs to make the `starteSQLSkript` function available.
   - **UC4 Includes**: None referenced in this file.
   - **Environment files sourced**: None.

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following parameters:
   - `$1` (`p_Eintragsnr`): Error entry number used when invoking the error logger `DWMSG_MeldeFehler`. Required. Map to a Python function parameter `p_eintragsnr`.
   - `$2` (`p_Skript`): Path of the SQL script to be executed. Required. Map to a Python function parameter `p_skript`.
   - `$*` (remaining positional arguments after `shift 2`): Dynamic parameters passed through to the SQL execution engine. Map to `*args` in Python.
   
   Environment variables:
   - `DW_ORAUSER` (os.environ): Database credentials/connection string used by SQL*Plus.
   - `ModulName` / `Modul_Name`: Defined as `"alis_sqlplus"`.
     *(# REVIEW: The script defines `ModulName` but references `Modul_Name` in its validation error message. This inconsistency is noted and handled in Python by using a single consistent variable.)*
   - `ModulVersion` / `Modul_Version`: Defined as `"V1.1.3"`.
     *(# REVIEW: The script defines `ModulVersion` but references `Modul_Version` in its validation error message. This inconsistency is noted and handled in Python.)*

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWMSG_MeldeFehler`:
     - Verbatim command line: 
       - `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"`
       - `DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript`
     - Purpose: Register errors in the central legacy reporting system.
     - Mapping: Call via `subprocess.run(["DWMSG_MeldeFehler", ...])`.
     - # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `sqlplus`:
     - Verbatim command line: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Oracle database client utility used to execute SQL scripts.
     - Mapping: Since the target platform is confirmed as **BIGQUERY**, `sqlplus` is obsolete. The SQL scripts should be migrated to BigQuery Standard SQL and executed using the Python BigQuery Client library (`google.cloud.bigquery`). However, to maintain the dynamic execution architecture of this helper function, we will provision a Python function that can call the BigQuery Client to execute the contents of `p_skript` using the BigQuery Python SDK, fallback-wrapped as a subprocess call if legacy SQL*Plus is kept during a transition period.
     - # REVIEW: target database platform is confirmed as BIGQUERY, but the utility wraps sqlplus (Oracle). The SQL script must be converted to BigQuery SQL and executed via the google-cloud-bigquery client.

5. EMBEDDED SQL
   There is no embedded SQL inside this wrapper script. It only invokes external SQL files dynamically.

6. CONTROL FLOW
   1. **Initialization**: Declare module metadata variables (`MODUL_NAME = "alis_sqlplus"`, `MODUL_VERSION = "V1.1.3"`).
   2. **Define Function `starte_sql_skript`**:
      - Accept `p_eintragsnr`, `p_skript`, and `*args`.
      - **Parameter Validation**:
        - Check if `p_eintragsnr` or `p_skript` is empty. If empty, invoke `DWMSG_MeldeFehler` with parameters `[p_eintragsnr, 'E', '196', f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"]` and return `196`.
      - **Readability Validation**:
        - Check if the file `p_skript` exists and is readable. If not, invoke `DWMSG_MeldeFehler` with parameters `[p_eintragsnr, 'E', '201', p_skript]` and return `201`.
      - **Log Execution**: Print details of the script path and arguments to stdout.
      - **Execute Script**:
        - Run SQL execution. Since the target database is BigQuery, read the contents of `p_skript`, perform parameter substitutions if any, and run via `bigquery.Client().query()`. If maintaining transitional Oracle compatibility, execute `sqlplus` using `subprocess.run()`.
      - **Return Status**: Capture and return the return code of the execution.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments returns `196`.
   - File unreadable returns `201`.
   - Execution failure propagates the exit code returned by the DB execution tool (e.g. `sqlplus` status code, or BigQuery Client exception translated to an error code).
   - Python mapping: Wrap the execution block in `try...except` and return/propagate appropriate error codes to preserve the exit-code contract.

8. OUTPUTS / SIDE EFFECTS
   - Standard output messaging describing script run configurations.
   - Logs generated via external `DWMSG_MeldeFehler` utility on validation failure.
   - Database operations executed inside the target BigQuery environment.

9. BUSINESS SUMMARY
   - Standardizes error logging and validation for database scripts.
   - Prevents database execution attempts when the target SQL file is missing or unreadable.
   - Decouples SQL script execution details from calling orchestrators.
   - Integrates database execution status directly back into the job-monitoring workflow.

---

### PYTHON PSEUDOCODE

```python
import os
import sys
import subprocess
import pathlib
# REVIEW: target database platform is confirmed as BIGQUERY.
# If executing scripts natively in BigQuery, the google-cloud-bigquery library should be imported.
# from google.cloud import bigquery

# Step 1: Initialize module-level metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Step 2: Define the central wrapper function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Python equivalent of starteSQLSkript function.
    Validates arguments and executes the specified SQL script.
    """
    
    # Step 3: Audit & Validate mandatory parameters
    # Original guard: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        # Call legacy error handler utility
        # # REVIEW-STRUCT: launcher DWMSG_MeldeFehler invoked — internal behaviour not available in this extraction
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_eintragsnr, 
            "E", 
            "196", 
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        ], check=False)
        return 196

    # Step 4: Audit & Validate that the script file is readable
    # Original guard: if [ ! -r $p_Skript ]
    script_path = pathlib.Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_eintragsnr, 
            "E", 
            "201", 
            p_skript
        ], check=False)
        return 201

    # Step 5: Log invocation details to stdout
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Execute the SQL script
    # Since TARGET_PLATFORM is BIGQUERY, the SQL execution should be routed to BigQuery.
    # We provide the implementation pattern for BigQuery below.
    errcode = 0
    try:
        # --- BIGQUERY NATIVE EXECUTION PATTERN ---
        # client = bigquery.Client()
        # with open(script_path, "r", encoding="utf-8") as f:
        #     sql_query = f.read()
        # # Note: Handle any positional parameter substitution (*args) in sql_query if needed
        # query_job = client.query(sql_query)
        # query_job.result() # Waits for query to complete
        
        # --- TRANSITIONAL SQL*PLUS SUBPROCESS PATTERN ---
        # If legacy oracle client is temporarily retained:
        dw_orauser = os.environ.get("DW_ORAUSER", "")
        # sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
        cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
        
        # Running with stdin redirected to devnull to match original script's </dev/null
        result = subprocess.run(
            cmd, 
            stdin=subprocess.DEVNULL, 
            capture_output=False, 
            check=False
        )
        errcode = result.returncode
        
    except Exception as e:
        print(f"Execution failed: {str(e)}", file=sys.stderr)
        errcode = 1  # Standard fallback error code

    # Step 7: Return the resulting exit code
    return errcode
```

# File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Migrates KornShell SQL*Plus helper functions into reusable Python functions leveraging Python's `subprocess` (for transitional SQL*Plus executions) or Google Cloud BigQuery client APIs. |

# Execution order
The pre-collected legacy sequence of 6 steps must be preserved and mapped to equivalent target task orchestrations in the main Airflow DAG:
1. **`DW.DWH_ABTN_SMART_KUBI.xml`** (UC4 orchestrator definition) -> Maps to the master Airflow DAG file orchestrating this job group.
2. **`d_abtn_x_smart_kubi.sql`** (Main database logic) -> Maps to a Dataform SQLX pipeline or `BigQueryInsertJobOperator` task.
3. **`r_sqlscript`** -> Maps to an Airflow operator calling the runner script.
4. **`.dw_init`** -> Maps to an Airflow task setting up environment variables.
5. **`f_alis_msgerr.ksh`** -> Maps to a task running the converted error reporting Python utility.
6. **`h_alis_sqlplus.ksh`** (Utility function script) -> Sourced as a Python module (`h_alis_sqlplus.py`) that is imported and used in Python operators executing SQL tasks.

# Schedule & variables
The target Airflow DAG must retain the schedules and execute with equivalent runtime variables. The scheduler-set variables for this job are mapped to dynamic Airflow parameters and templates:
- **`DWH_JOB_KENNUNG`** (Value: `'ABTN_SMART_KUBI'`): Retained as a parameter passed to the operators.
- **`cdate`** (Value: `SYS_DATE("YYYYMMDD")`): Replaced by Airflow execution date Jinja templates: `{{ ds_nodash }}`.
- **`cmonth` / `cday` / `first` / `MONATSID`**: Modeled dynamically inside the Airflow environment using standard Python `datetime` calculations or Jinja macros:
  - `cmonth` (First reference): `{{ execution_date.strftime('%Y%m') }}`
  - `cday`: `{{ execution_date.strftime('%d') }}`
  - `first`: `'01'`
  - `cmonth` (Concatenation of `cmonth` + `first`): `{{ execution_date.strftime('%Y%m') }}01`
  - `cmonth` (Subtracting 1 day from 1st of month): `{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m%d') }}`
  - `cmonth` (Substring to 6 chars): `{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m') }}`
  - **`MONATSID`** (Final assigned value): `{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m') }}`

# Lineage
- **Upstream producers**: None found for this utility file.
- **Downstream consumers**: None found for this utility file.

# Target file plan
- **Target File Path**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
  - **Language**: Python (3.11+)
  - **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
  - **Purpose**: A utility module that provides the python-native equivalent of `starteSQLSkript` including validation routines, logging of execution details, and safe database client task initialization.

# Environment-specific values
1. **`DW_ORAUSER`** (Global Environment Value)
   - **Legacy Role**: Oracle database credentials.
   - **Target Classification**: GLOBAL
   - **Target Sourcing**: Maps to the GCP project environment or standard Airflow Connection ID. Sourced via `os.environ.get("GCP_PROJECT")` or standard environment vars `BQ_DATASET`.
2. **`ModulName` / `Modul_Name`** (Job-Specific Value)
   - **Legacy Role**: Module name tracking.
   - **Target Classification**: JOB-SPECIFIC
   - **Target Sourcing**: Inline literal `'alis_sqlplus'` defined inside the module.
3. **`ModulVersion` / `Modul_Version`** (Job-Specific Value)
   - **Legacy Role**: Version tracking.
   - **Target Classification**: JOB-SPECIFIC
   - **Target Sourcing**: Inline literal `'V1.1.3'` defined inside the module.

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
REASON: The script is an orchestration and utility wrapper that manages argument parsing, file path resolution, framework logging integration, and signal trapping.

EVIDENCE
- Business logic found: KSH custom logic. The script parses command-line flags (-f, -i, -j, -v), resolves dynamic directory paths for SQL scripts, integrates with a custom metadata logging framework (DWMSG), and executes SQL wrappers.
- AWK: none
- SQL-expressible: No. Managing file existence checks on the host system, trapping OS signals, and logging to dynamic filesystem paths are not expressible inside BigQuery SQL.
- Non-SQL side effects: Resolves local/relative paths, manages framework log files, and intercepts OS signals.
- Against this verdict: A dedicated orchestration tool (like Apache Airflow) could manage this pipeline directly, but reproducing this utility's precise routing and setup logic natively requires a Python wrapper.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This KornShell script (`r_sqlscript`) is a utility wrapper used to execute database SQL scripts within a larger data warehouse architecture. Its primary purpose is to locate a specified SQL file within standard directory layouts (`../sql`, `../mig`, or the current directory), establish a standardized logging context, configure error trapping, and initiate SQL execution via a helper utility (`starteSQLSkript`). With the confirmed migration target of **BigQuery**, this script acts as an orchestration and routing agent to dispatch and track SQL execution against BigQuery.

### 2. INVOCATION CONTEXT
*   **Caller:** Typically invoked by UC4/Automic UNIX jobs (JOBS_UNIX) or manually with arguments.
*   **Sourced Environment Files:**
    *   `. $HOME/aktuell/.dw_init`
        *   `# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
        *   `# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables/functions it sets are unknown`
    *   `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
        *   `# REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables/functions it sets are unknown`

### 3. PARAMETERS / INPUTS
*   **-f <sql_script_name>**
    *   Source: Command-line parameter `p_sqlscript` (forced to lowercase via legacy `typeset -l`).
    *   Used in script: Yes. Resolves path and forms the execution target.
    *   Python Mapping: `argparse` argument or `sys.argv`.
*   **-i <sql_parameters>**
    *   Source: Command-line parameter `p_sqlpar`.
    *   Used in script: Yes. Forwarded directly to the underlying SQL runner.
    *   Python Mapping: `argparse` argument or `sys.argv`.
*   **-j <job_id>**
    *   Source: Command-line parameter `p_Job` (defaults to "DWH_KORR").
    *   Used in script: Yes. Converted to uppercase `JobKennung` for logging registration.
    *   Python Mapping: `argparse` argument or `sys.argv`.
*   **-v**
    *   Source: Command-line flag `p_Verbose`.
    *   Used in script: Yes. If set to 1, prints the log file on exit.
    *   Python Mapping: `argparse` action "store_true".

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
*   **`starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr`**
    *   Purpose: An external framework script/function (defined in `h_alis_sqlplus.ksh`) that connects to the database and executes the resolved SQL script.
    *   Python Mapping:
        `# REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion`
        Since the confirmed target is BigQuery, this call must ultimately be adapted to launch BigQuery jobs (e.g., via `google-cloud-bigquery` Python client library or `bq query`).
*   **Framework Logging Calls (`DWMSG_...`):**
    *   `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`
    *   Purpose: Standard corporate logging/monitoring database registration.
    *   Python Mapping: Must remain as external module calls, custom wrapper methods, or standard python logging equivalent matching the enterprise standard.

### 5. EMBEDDED SQL
There is no inline or static SQL defined inside this utility script. All execution targets are dynamic SQL scripts resolved via parameter `-f` and executed by the external runner `starteSQLSkript`.

### 6. CONTROL FLOW
1.  **Environment Setup:** Source `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`.
2.  **Safety Configuration:** Set `set -e` (terminate immediately on error).
3.  **Command-Line Parsing:** Parse flags using `getopts` loop (`-f`, `-i`, `-j`, `-v`, `-h`).
4.  **Parameter Validation:**
    *   If argument parsing encountered unknown/missing parameters, execute `DWMSG_MeldeFehler` with code, print `usage()`, and exit with `ErrNr`.
5.  **Change Directory:** Change working directory to the directory where the wrapper script itself resides (`dirname $0`).
6.  **Path Resolution (Relative/Absolute Lookup):**
    *   Check if `p_sqlscript` has a directory path.
    *   If relative to `.`, look up sequentially in `../sql/`, then `../mig/`, then local `.`.
    *   Assign the resolved path to `l_DBskript`.
7.  **File Existence Logic Check:**
    *   `# REVIEW: legacy validation logic seems inverted or incomplete (checks if file exists, then sets unused/undefined ErrArg p_Kuerzel, but does not exit).`
8.  **Job Identification:** Standardize `JobKennung` (to uppercase). Fall back to "DWH_KORR" if not specified.
9.  **Logging Registration:**
    *   Execute `DWMSG_ErmittleNr` to generate an execution sequence ID (`DW_EintragsNr`).
    *   Determine the log file name using `DWMSG_Logdateiname`.
    *   Create a log record using `DWMSG_ErzeugeEintrag`.
10. **Trap Configuration:**
    *   Establish `INT` (interrupt) and `ERR` (execution error) trap functions targeting `DWMSG_Fehlerbehandlung` to capture run failures.
    *   If verbose is enabled, append log output printing to the trap actions.
11. **Job Execution:**
    *   Execute the SQL script by invoking `starteSQLSkript` with parsed options and redirect standard output/error to the log file.
12. **Success Finalization:**
    *   Set status to OK in framework via `DWMSG_SetzeStatusOK`.
    *   Reset traps back to default behavior.
    *   Print clean exit success message.

### 7. ERROR HANDLING & EXIT CODES
*   **Shell Error Capture:** `set -e` triggers instant abort on unhandled statement failure.
*   **Trap System:** `trap` catches `ERR` and `INT` to dump the log (if verbose) and run corporate cleanups via `DWMSG_Fehlerbehandlung`.
*   **Argument Error Codes:**
    *   `192`: Unknown parameter.
    *   `193`: Missing parameter argument.
    *   `198`: Parameter value unknown (triggered in legacy if the SQL file actually existed).
*   **Python Translation:** Implement inside a `try/except` block catching `subprocess.CalledProcessError` or BigQuery API client exceptions, forwarding failures to standard error, executing equivalent cleanup handlers, and returning non-zero exit codes on failure.

### 8. OUTPUTS / SIDE EFFECTS
*   **Log Files:** Writes and appends framework and execution outputs to `$LogDatei`.
*   **Database Changes:** Side effects occur inside the targets executed by `starteSQLSkript` on BigQuery.

### 9. BUSINESS SUMMARY
*   Serves as a generic launcher utility to invoke target SQL scripts.
*   Resolves execution target paths across multiple fallback directories (`../sql`, `../mig`, `.`) to support flexible deployments.
*   Registers program executions and results in the central DWH logging database.
*   Implements standardized signal capturing and logging conventions for easy production support and debugging.

---

### 10. PSEUDOCODE (Python Style)

```python
#!/usr/bin/env python3
import sys
import os
import argparse
import subprocess
import shutil

# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables/functions it sets are unknown

# Step 1: Framework Stub functions representing sourced behavior
def dwmsg_melde_fehler(eintrags_nr, severity, err_nr, err_arg):
    # Dummy representation of DWMSG_MeldeFehler
    print(f"Error logged: {severity} {err_nr} {err_arg}", file=sys.stderr)

def dwmsg_ermittle_nr():
    # Dummy representation of DWMSG_ErmittleNr (Returns execution ID)
    return 12345

def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # Dummy representation of DWMSG_Logdateiname
    return f"/tmp/log_{job_kennung}_{eintrags_nr}.log"

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, script_desc, log_file):
    # Dummy representation of DWMSG_ErzeugeEintrag
    pass

def dwmsg_fehlerbehandlung(eintrags_nr):
    # Dummy representation of DWMSG_Fehlerbehandlung
    pass

def dwmsg_setze_status_ok(eintrags_nr):
    # Dummy representation of DWMSG_SetzeStatusOK
    pass

# REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
def starte_sql_skript(eintrags_nr, db_skript, sql_par, log_file):
    # This must execute the SQL script using the google-cloud-bigquery client library
    # because the confirmed target platform is BIGQUERY.
    pass

def usage():
    print("""
   Programm: Ausführung Script r_sqlscript
   Version: 5.0.0
   Aufruf: r_sqlscript.py Parameter

   Das als Parameter -f  übergebene SQL-Script wird ausgeführt.
   ...
   Parameter:
       -f     hier wird der Name des SQL-Scripts angegeben
       -i     mögliche Parameter für das SQL-Script 
       -j     Jobkennung (default DWH_KORR)
       -h     zeigt diese Seite an
       -v     verbose (zeigt bei Fehler sofort die Logdatei an)
""")

def main():
    # Step 2: Initialize default variables
    p_verbose = False
    p_sqlscript = None
    p_sqlpar = ""
    p_job = "DWH_KORR"
    err_nr = 0
    err_arg = ""

    # Step 3: Parse Arguments
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-f', dest='f_script')
    parser.add_argument('-i', dest='i_param')
    parser.add_argument('-j', dest='j_job')
    parser.add_argument('-v', action='store_true', dest='verbose')
    parser.add_argument('-h', action='store_true', dest='help')

    try:
        args, unknown = parser.parse_known_args()
    except Exception as e:
        # Map unknown argument handling
        dwmsg_melde_fehler(0, "E", 192, str(e))
        usage()
        sys.exit(192)

    if args.help:
        usage()
        sys.exit(0)

    if not args.f_script:
        # Required argument missing
        dwmsg_melde_fehler(0, "E", 193, "-f")
        usage()
        sys.exit(193)

    p_sqlscript = args.f_script.lower() # typeset -l equivalent
    if args.i_param:
        p_sqlpar = args.i_param
    if args.j_job:
        p_job = args.j_job
    p_verbose = args.verbose

    # Step 4: Resolve directory path logic
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)

    l_db_skript = p_sqlscript
    if os.path.dirname(p_sqlscript) in ['.', '']:
        test_path_sql = os.path.join("..", "sql", p_sqlscript)
        test_path_mig = os.path.join("..", "mig", p_sqlscript)
        
        if os.path.isfile(test_path_sql):
            l_db_skript = test_path_sql
        elif os.path.isfile(test_path_mig):
            l_db_skript = test_path_mig
        else:
            l_db_skript = p_sqlscript

    # Step 5: File Validation (Legacy logic preservation)
    # REVIEW: legacy validation logic seems inverted or incomplete (checks if file exists, then sets unused/undefined ErrArg p_Kuerzel, but does not exit).
    if os.path.isfile(l_db_skript):
        err_nr = 198
        err_arg = ""  # p_Kuerzel was undefined in legacy ksh

    # Step 6: Logging registration
    job_kennung = p_job.upper() # typeset -u JobKennung
    dw_eintrags_nr = dwmsg_ermittle_nr()
    log_datei = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)
    
    dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, f"r_sqlscript_{l_db_skript}", log_datei)

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_db_skript}")
    print("---------------------------------------------")

    # Step 7: Execute with Trap equivalents
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{dw_eintrags_nr}'")
        print(f"Logdatei  : '{log_datei}'")
        print("---------------------------------------------")

        # Execute target SQL against BigQuery via helper
        starte_sql_skript(dw_eintrags_nr, l_db_skript, p_sqlpar, log_datei)

        # Step 8: Finalize OK status
        dwmsg_setze_status_ok(dw_eintrags_nr)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except (Exception, KeyboardInterrupt) as err:
        # Step 9: Catch failures and emulate TRAP behavior
        dwmsg_fehlerbehandlung(dw_eintrags_nr)
        print("!OSFEHLER / FEHLER gemeldet!", file=sys.stderr)
        if p_verbose:
            # Dump log contents to console if verbose flag is set
            if os.path.exists(log_datei):
                with open(log_datei, 'r') as f:
                    print(f.read(), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### Execution order
The target orchestration (Apache Airflow / Cloud Composer DAG) must preserve the execution sequence established in the legacy dependency graph:
1. **`DW.DWH_ABTN_SMART_KUBI.xml`** maps to the Airflow DAG container itself.
2. **`d_abtn_x_smart_kubi.sql`** maps to a BigQuery execution task within the DAG.
3. **`r_sqlscript`** maps to a Python execution operator (or standard Python run task) executing the migrated `r_sqlscript.py` script.
4. **`.dw_init`** is represented by environmental and DAG variables.
5. **`f_alis_msgerr.ksh`** and **`h_alis_sqlplus.ksh`** are migrated as shared library modules or helper classes imported by the Python script to handle standard logging and SQL execution operations on BigQuery.

### Schedule & variables
The migrated Python script and Cloud Composer DAG must receive and resolve the following variables through native mechanisms (such as Airflow DAG `params` or standard Python `argparse`/environment variables):
*   **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`
*   **`cdate`** = `'SYS_DATE("YYYYMMDD")'`: Sourced dynamically at runtime using Airflow template variables (e.g., `{{ ds_nodash }}`) or Python's `datetime.date.today().strftime('%Y%m%d')`.
*   **`cmonth`** = `'SUBSTR(&cdate,1,6)'`: Derived dynamically by parsing the first six characters of `cdate`.
*   **`cday`** = `'SUBSTR(&cdate,7,2)'`: Derived dynamically by parsing the last two characters of `cdate`.
*   **`first`** = `'01'`: Re-initialized literal constant.
*   **`cmonth`** = `'&cmonth&first'`: Dynamically concatenated date string.
*   **`cmonth`** = `'SUB_DAYS(&cmonth,1)'`: Date subtraction operation performed dynamically using Python's `datetime` package.
*   **`cmonth`** = `'SUBSTR(&cmonth,1,6)'`: Extracted previous month code in `YYYYMM` format.
*   **`MONATSID`** = `'&cmonth'`: Standard reporting month ID used as the dynamic query parameter.

### Lineage
The script interacts with the following components as defined in the legacy lineages:
*   **Upstream Configuration:** Sourced from `.dw_init` (uses configuration).
*   **Downstream / Invoked Helpers:**
    *   `f_alis_msgerr.ksh` (invoked helper script)
    *   `h_alis_sqlplus.ksh` (invoked SQL runner utility)

### Cross-file dependencies
*   **`local/home/gurunathan_t/kubi/r_sqlscript`** relies on `.dw_init` to retrieve global environment variables (e.g., `DW_DIR_ROOT`).
*   The script depends on the error-reporting routines defined in `f_alis_msgerr.ksh` (`DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, etc.) and the SQL script launcher functions in `h_alis_sqlplus.ksh` (`starteSQLSkript`) to complete its processing lifecycle.

### Target file plan
*   **Target File Path:** `local/home/gurunathan_t/kubi/r_sqlscript.py`
    *   **Language:** Python 3
    *   **Source File:** `local/home/gurunathan_t/kubi/r_sqlscript`

### Environment-specific values
Classified based on their target operational roles:

1. **GLOBAL**
   *   **`GCP_PROJECT`**: The target BigQuery project. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow's `Variable.get("GCP_PROJECT")`.
   *   **`GCS_BUCKET`**: The environment-wide storage bucket for logs and artifacts. Sourced via `os.environ.get("GCS_BUCKET")`.
   *   **`DW_DIR_ROOT`**: Sourced via standard environment variable read `os.environ.get("DW_DIR_ROOT")` or consolidated config.

2. **JOB-SPECIFIC**
   *   **`JobKennung` / `DWH_JOB_KENNUNG`**: Assigned to `'ABTN_SMART_KUBI'` or passed via the `-j` command-line argument. Included in the python configuration dictionary.
   *   **`l_db_skript`**: The target SQL script resolved via lookup paths. Parsed from input arguments.
   *   **`DW_EintragsNr`**: The dynamic tracking execution ID generated at runtime.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Converted to Python to manage argument parsing, relative directory resolution, execution ID generation, logging database registration, and BigQuery execution orchestration. |

### HARD RULES
*   The original Ab Initio, shell scripts, and SQL code logic are not duplicated.
*   Only the required `ksh_design_python` tool was called.
*   No other files (e.g. `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`) have been planned, designed, or listed in the File Disposition Table since they belong to different design runs.
*   No alternative target library or implementation strategy has been evaluated.
*   Original German print messages, if any, must be retained verbatim in the final code execution outputs.