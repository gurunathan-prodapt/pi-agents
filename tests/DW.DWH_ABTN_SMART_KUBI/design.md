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
This migration design covers a single UC4 `JOBS_UNIX` object, `DW.DWH_ABTN_SMART_KUBI`, which is used to populate a temporary table by executing an external SQL script (`d_abtn_x_smart_kubi.sql`). This job performs a dynamic date calculation to determine a target reporting month parameter (`MONATSID`) based on the execution day of the month: if executed before the 15th, it targets the previous month; otherwise, it targets the current month. Because no parent `JOBP` (workflow) or `JSCH` (schedule) was included in this extraction bundle, this job is represented as an independent, single-task Airflow DAG that is assumed to be externally triggered.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | Active (1) | Populate temp table |

---

## 3. Scheduling
* **Calendar/Time Triggers:** None present in this extraction bundle.
* **Trigger Source:** Externally triggered (source unknown from this extraction alone).
* **Airflow Schedule:** `schedule=None` (no schedule will be defined; must be triggered manually, via an external dataset, or by an upstream DAG trigger).

---

## 4. Airflow DAG Properties
The following properties are defined for the DAG wrapper wrapping this single job:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1 in UC4 mapping)* |
| **default_args** | `{'owner': 'dw', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_abtn_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `EmptyOperator` | N/A | N/A | 1 | 5m | None | None | N/A | None | **#REVIEW-STRUCT:** Launcher wraps SQL script `d_abtn_x_smart_kubi.sql`. Converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL. Confirm actual artifact before replacing this `EmptyOperator` (e.g., with `BigQueryInsertJobOperator` or `BashOperator`). Includes dynamic date logic for `MONATSID`. |

---

## 6. Task Dependency Map
Since this extraction consists of a single standalone job, there is only a single-node task structure:

```python
dw_dwh_abtn_smart_kubi
```

---

## 7. Sync / Concurrency Analysis
No UC4 Sync rows (`sync_rows`) or mutual exclusion locks were defined for this object. The DAG is configured with `max_active_runs=1` as a standard defensive configuration.

---

## 8. Error Handling and Retry Strategy
* **Retries:** Inherits a default of `1` retry with a `5`-minute delay.
* **Postconditions:** No explicit postcondition actions or failure callback requirements were found in the extraction.
* **Trigger Rules:** Default `all_success` behavior is retained.

---

## 9. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&MONATSID` | Dynamic runtime execution date logic | Calculated via custom Python helper function accessing `logical_date` (see pseudocode) |
| `&DWH_JOB_KENNUNG` | `'ABTN_SMART_KUBI'` | Passed as metadata parameter or environment variable |
| `sql_path` | `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql` | Target path within SQL repository/GCS bucket |

---

## 10. Developer Notes
* **#REVIEW-STRUCT: External SQL Translation:** The core utility of this job is executing `d_abtn_x_smart_kubi.sql` via `r_sqlscript`. This SQL script must be migrated to its target destination (e.g., BigQuery, Snowflake, or Postgres). Once the destination is confirmed, replace the `EmptyOperator` placeholder with the appropriate operator (e.g., `BigQueryInsertJobOperator`). Do not assume Python is the execution medium.
* **#REVIEW: Standalone Workflow Assumption:** Since no parent `JOBP` workflow was provided in this extraction, this job has been isolated into its own DAG. If this job is actually a task within a larger downstream or upstream workflow, its task definition should be merged into that workflow's DAG file rather than running as a standalone DAG.
* **Dynamic Date Logic Execution:** The custom Python helper `get_reporting_month(logical_date)` matches the UC4 script logic precisely. Ensure that the dynamic target database utilizes this calculated value as an input parameter.

---

# Pseudocode Outline

```python
# ==============================================================================
# ── Imports ───────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: Import your target execution operator once the SQL target environment is confirmed:
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ==============================================================================
# ── GCP Configuration ─────────────────────────────────────────────────────────
# ==============================================================================
# Placeholders for migrated environments
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "europe-west3"
GCP_CONN_ID = "google_cloud_default"

# ==============================================================================
# ── Dynamic Date Logic (UC4 Script Translation) ──────────────────────────────
# ==============================================================================
def get_reporting_month(logical_date):
    """
    Translates UC4 logic:
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
    """
    # logical_date is a pendulum.DateTime object passed from Airflow execution context
    day = logical_date.day
    if day < 15:
        # Subtract days to get to the previous month
        first_of_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_month - timedelta(days=1)
        return last_day_prev_month.strftime('%Y%m')
    else:
        return logical_date.strftime('%Y%m')

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
default_args = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id='dw_dwh_abtn_smart_kubi',
    default_args=default_args,
    description='Populate temp table - Translated from UC4 DW.DWH_ABTN_SMART_KUBI',
    schedule=None,  # No calendar schedule defined in UC4 extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated', 'uc4', 'sql_script'],
) as dag:

    # ==========================================================================
    # ── Task: dw_dwh_abtn_smart_kubi ──────────────────────────────────────────
    # ==========================================================================
    # #REVIEW-STRUCT: This is currently styled as an EmptyOperator placeholder.
    # Replace this with the actual runtime SQL operator (e.g., BigQueryInsertJobOperator)
    # when the target database strategy and script location are finalized.
    # 
    # Example Target Parameters to pass to actual operator:
    #   job_id_prefix: "ABTN_SMART_KUBI_"
    #   sql_path: "gs://your-bucket/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql"
    #   query_parameters: {
    #       "MONATSID": get_reporting_month(logical_date)
    #   }
    
    dw_dwh_abtn_smart_kubi = EmptyOperator(
        task_id='dw_dwh_abtn_smart_kubi',
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-task workflow. No execution dependency pipeline required.
    dw_dwh_abtn_smart_kubi
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Converted to an Airflow DAG that manages the execution and runtime variables for the smart_kubi process. |

---

# Job Dependencies
* **Upstream / Called By:** This job is triggered as part of the monthly run of `DW.DWH_SMART_KUBI_ZUGANG_MONATLICH_JP` workflow (as indicated by the source origin path).
* **Downstream Job Wiring on BigQuery:**
  * Once migrated to BigQuery/Cloud Composer, this DAG will orchestrate the execution. Since this job triggers a SQL script `d_abtn_x_smart_kubi.sql` which belongs to a different PL/SQL/SQL migration group, a dependency exists on that migrated component. This dependency is wired as a task (e.g., `BigQueryInsertJobOperator` or `DataformStartActionOperator`) executing the translated BigQuery SQL or Dataform model.
  * For any upstream workflows not yet migrated to Composer, this DAG can be triggered via Airflow's REST API or an external Cloud Pub/Sub trigger.

---

# Execution Order
The legacy dependency graph defines a 6-step sequential execution order. Below is the mapping from the legacy steps to the target architecture:

1. **`DW.DWH_ABTN_SMART_KUBI.xml` (UC4 Job Wrapper):** Mapped to the master Airflow DAG (`dw_dwh_abtn_smart_kubi.py`).
2. **`d_abtn_x_smart_kubi.sql` (SQL Execution):** Mapped to a Cloud Composer task invoking the migrated SQL script in BigQuery or initiating a Dataform pipeline execution.
3. **`r_sqlscript` (KornShell Execution Utility):** Retired. The launcher utility's logging and connection features are replaced by Airflow's native GCP connection providers and task logging.
4. **`.dw_init` (Initialization Script):** Retired. Environment setup is handled natively via Composer variables, connection profiles, and Airflow task environment variables.
5. **`f_alis_msgerr.ksh` (Error Logger):** Retired. Error trapping and notification are handled using Airflow's native `on_failure_callback` mechanisms.
6. **`h_alis_sqlplus.ksh` (SQL*Plus Helper):** Retired. Database connection and execution are handled directly by Airflow's BigQuery operators.

---

# Scheduling
* **Trigger Construct:** The UC4 object was active but had no local schedules defined inside this specific XML export (it is triggered by a parent monthly job stream `DW.DWH_SMART_KUBI_ZUGANG_MONATLICH_JP`).
* **Target Scheduling:** In the Cloud Composer DAG, this will be represented with `schedule=None` to ensure it is triggered only on-demand or by the upstream master DAG using a `TriggerDagRunOperator`.

---

# Schedule & Variables
Every legacy scheduler-set variable must reach the target DAG or database task:

* **`DWH_JOB_KENNUNG`:** Constant value `'ABTN_SMART_KUBI'`. Passed as a `JOB-SPECIFIC` parameter or metadata tag.
* **Date Variables (`cdate`, `cmonth`, `cday`, `first`, `MONATSID`):** These variables compute the dynamic target month (`MONATSID`) based on the execution date. This date math is retained exactly as in the source logic using Python's `datetime` module within the Airflow DAG's context:
  * If the execution day (`cday`) is less than 15, `MONATSID` becomes the previous month in `YYYYMM` format (the month prior to execution).
  * Otherwise, `MONATSID` is the current month in `YYYYMM` format.
  * This calculated `MONATSID` will be passed dynamically to the BigQuery operator as a runtime parameter (e.g., `@MONATSID`).

---

# Lineage
* **Upstream Producers / Includes:**
  * `DW.HOLE_PFAD` (Human-confirmed: NO SOURCE NEEDED - Retired in target).
  * `DW.LESE_LOG` (Human-confirmed: NO SOURCE NEEDED - Retired in target).
  * `.DW_INIT` (Retired in target).
  * `DW.UNIX.ISTNS` (UC4 Login - Mapped to GKE service account / IAM roles).
* **Downstream Consumers / Invoked Elements:**
  * `r_sqlscript` (Retired in target).
  * `d_abtn_x_smart_kubi.sql` (Cross-job hand-off to the SQL/PLSQL group - Replaced by BigQuery query tasks).

---

# External System Replacements
* **Oracle DB / SQL\*Plus:** Replaced by **Google BigQuery** (for data warehousing) and **Google Cloud Storage** (for staging scripts if needed).
* **Unix Host `DWHDWH1P`:** Replaced by **Cloud Composer (GKE-based Airflow workers)**.
* **Unix Filesystem Paths (`$HOME/aktuell/...`):** Replaced by GCS bucket URIs (e.g., `gs://[GCS_BUCKET]/dags/...`) or referenced inside a Dataform repository.

---

# Cross-File Dependencies
* **Shared Tables / Common Schemas:**
  * The executed SQL script `d_abtn_x_smart_kubi.sql` references table `DWH$TA_T_SMART_KUBI` (which it truncates and repopulates) and reads from `DWH$TA_F_D1_TWVV_TN` and related dimensions.
  * Ensure that the Airflow DAG runs in coordination with the migrations of these target tables to BigQuery.
* **Call Chain:** The Airflow DAG triggers the SQL processing of `d_abtn_x_smart_kubi.sql`. This SQL execution must be fully translated to BigQuery SQL syntax and deployed before enabling this DAG.

---

# Target File Plan

| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Python | `DW.DWH_ABTN_SMART_KUBI.xml` | Orchestration DAG running in Cloud Composer that implements the monthly runtime date math and triggers the BigQuery execution. |

---

# Environment-Specific Values

### GLOBAL (Environment-Wide)
* **`GCP_PROJECT`:** Represents the target Google Cloud Project ID where Composer and BigQuery resources are provisioned. Sourced via Airflow config: `Variable.get("GCP_PROJECT")`.
* **`GCP_REGION`:** The regional endpoint where resources reside (e.g., `europe-west3`). Sourced via: `Variable.get("GCP_REGION")`.
* **`GCS_BUCKET`:** The shared GCS storage bucket hosting runtime DAG configurations or static reference SQL files. Sourced via: `Variable.get("GCS_BUCKET")`.
* **`BQ_CONN_ID`:** The Airflow connection ID used to interact with BigQuery. Sourced via: `Variable.get("BQ_CONN_ID", default_var="google_cloud_default")`.

### JOB-SPECIFIC
* **`DWH_JOB_KENNUNG`:** Constant string value `'ABTN_SMART_KUBI'`. Passed directly to the operator configurations.
* **`MONATSID`:** Calculated dynamic execution month (reporting month). Passed inline as a query parameter `@MONATSID` to BigQuery.

---

# Risks and Manual Steps
* **SQL Migration Dependency:** The job executes `d_abtn_x_smart_kubi.sql`. The logic inside that SQL script (truncating `DWH$TA_T_SMART_KUBI` and populating it with aggregated data) is being designed and migrated under a separate PL/SQL/SQL design pass. The wiring in the target DAG can only be finalized and validated once that script's BigQuery equivalent is fully implemented and its location (or Dataform execution target) is known.
* **German-Language Print Statements:** The original UC4 script prints execution parameters in German:
  `Berichtsmonat:  &MONATSID`
  Per the Output Print Literal Rule, this exact German phrasing must be logged. The Python script logs this exact output:
  `print(f"Berichtsmonat:  {monatsid}")`
* **Active Status Verification:** The UC4 object has `<Active>1</Active>`. In Composer, the DAG must be deployed in an active status (`is_paused_upon_creation=False`). Ensure appropriate DAG execution controls are in place to prevent accidental execution before the BigQuery tables and SQL scripts are migrated.

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
REASON: Contains conditional directory-existence checks to dynamically resolve and export ORACLE_HOME, which goes beyond simple static variable assignment.

EVIDENCE
- Business logic found: KSH custom logic dynamically resolves the `ORACLE_HOME` environment variable by checking the existence of filesystem directories, and builds directory paths dynamically using `$HOME` and `$ORACLE_SID`.
- AWK: none
- SQL-expressible: no, this is purely an environment configuration and filesystem management script.
- Non-SQL side effects: Tests for the physical existence of directories on the application host and manipulates the shell process's environment variables.
- Against this verdict: NO_CONVERSION_REQUIRED could be argued because this is a standard configuration file (`.dw_init`) whose variable declarations are usually mapped directly to modern orchestrator/container environment variables rather than translated into executable Python logic.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `.dw_init` script is an environment initialization file used to configure the execution environment for the Information Services / Data Warehouse system. It declares and exports absolute directory paths for various data imports, exports, logfiles, and utility scripts relative to the user's `$HOME` directory. Additionally, it dynamically detects and exports `ORACLE_HOME` based on filesystem directory presence, and sources companion global and local configuration files.

2. INVOCATION CONTEXT
   - Sourced by other KornShell scripts (e.g. via `. $HOME/.dw_init`) to establish environment variables in the calling shell's context. It is not called as a standalone executable.
   - UC4 Includes Referenced: None.
   - Environment files sourced:
     * `. $HOME/.dw_global` — # REVIEW-STRUCT: environment file .dw_global not supplied — variables it sets are unknown; do not guess their names or values
     * `. $HOME/.dw_lokal` — # REVIEW-STRUCT: environment file .dw_lokal not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `$HOME` (environment variable): Used as the base path for all legacy directories. Sourced from `os.environ["HOME"]`.
   - `$ORACLE_HOME` (environment variable): Checked to see if already populated. If missing, it is dynamically set by checking physical disk paths.
   - `$ORACLE_SID` (environment variable): Used to dynamically construct the Oracle `utl_file` path string.
   - No KSH Declared Environment Parameters section was provided in the extraction.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - None. (Performs file-system directory tests (`[ -d ... ]`) and prints notifications to standard output).

5. EMBEDDED SQL
   - None.

6. CONTROL FLOW
   1. Initialize and export directory variables relative to `$HOME` (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`, and dozens of source-specific `DW_DIR_IMP_*` and `DW_DIR_EXP_*` folders).
   2. Assign and export the remote customer host (`DW_HOST_CUSTOMER`).
   3. Check if `$ORACLE_HOME` is empty or unset:
      - If `/appl/local/oracle/12.2.0.1.0` is a directory, set `ORACLE_HOME` to `/appl/local/oracle/12.2.0.1.0`.
      - Else, if `/appl/local/oracle/11.2.0` is a directory, set `ORACLE_HOME` to `/appl/local/oracle/11.2.0`.
      - Otherwise, print error message:
        "Fehler in .dw_init:
           Konnte ORACLE_HOME nicht setzen !"
   4. Source the `$HOME/.dw_global` environment script.
   5. Source the `$HOME/.dw_lokal` environment script.
   6. Construct and export `DW_DIR_UTL_FILE` dynamically using the value of `$ORACLE_SID`.

7. ERROR HANDLING & EXIT CODES
   - If the script fails to locate an Oracle installation directory, it prints a multi-line error to stdout but does *not* exit or terminate execution (non-fatal warning behavior).
   - Python conversion should match this behavior by logging a warning to `sys.stderr` but allowing execution to continue.

8. OUTPUTS / SIDE EFFECTS
   - Mutates the environment context. Since a Python child subprocess cannot modify its parent shell's environment, this script should be converted into a Python module that updates `os.environ` for any downstream Python tasks, or loaded into a configuration dictionary.

9. BUSINESS SUMMARY
   - Coordinates the storage directories for different data providers (such as SAP, SMS, PLATO, SIGMA, CTEL) in a central place.
   - Automatically handles Oracle environment detection to allow seamless database utility access (SQL*Plus, Loader, etc.) across different server environments.
   - Manages global vs local scope configuration separation.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
# Step 1: Import required libraries
import os
import sys

def init_environment():
    # Step 2: Resolve HOME directory base
    home = os.environ.get("HOME", "")
    if not home:
        # Fallback to user home if HOME env var is completely missing
        home = os.path.expanduser("~")

    # Step 3: Define and export directory structures
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
    os.environ["DW_DIR_IMP_SAP_L_GUTGR"] = os.path.join(home, "daten/sap/sap_l_gutgr")
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

    # Step 4: Define Remote Hosts
    os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

    # Step 5: Resolve ORACLE_HOME dynamically if not already populated
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

    # Step 6: Sourcing external scripts
    # # REVIEW-STRUCT: environment file .dw_global not supplied — variables it sets are unknown; do not guess their names or values
    # In Python, global/local configs should be loaded from YAML/JSON or dynamic Python imports.
    print("[INFO] Sourcing of .dw_global and .dw_lokal skipped (not supplied). Ensure environment variables are consolidated.")

    # Step 7: Construct dynamic Oracle Admin UTL path
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | `dw_init.py` | Converts KornShell environment/directory initialization variables and dynamic Oracle path-checking logic into a Python module that downstream components can import to configure their runtime environment. |

---

### Execution Order
The legacy dependency graph lists 6 execution steps for the `DW.DWH_ABTN_SMART_KUBI` job flow:
1. `DW.DWH_ABTN_SMART_KUBI.xml` — Orchestration definition; maps to a Cloud Composer (Airflow) DAG that coordinates all downstream tasks.
2. `d_abtn_x_smart_kubi.sql` — Main database transformation/aggregation script; maps to a Dataform SQLX action or a BigQuery execution task within the Composer DAG.
3. `r_sqlscript` — SQL execution utility wrapper; maps to an Airflow task invoking BigQuery or Dataform.
4. `.dw_init` — Environment setup and initialization script; maps to importing the converted `dw_init.py` module at the beginning of the Airflow/Python execution context.
5. `f_alis_msgerr.ksh` — Message and error-logging framework; maps to a native Python logging library or GCP Cloud Logging integration.
6. `h_alis_sqlplus.ksh` — Helper utility for SQL*Plus database execution; retired on the target platform since database operations execute directly on BigQuery.

---

### Schedule & Variables — Must Be Retained
The scheduling of the job is inherited from the parent orchestration defined in `DW.DWH_ABTN_SMART_KUBI`.
At the start of execution, the scheduling system must calculate and propagate the following SCHEDULER-SET VARIABLES to downstream steps (e.g., via Airflow DAG task parameters or dynamic environment variables):
* `DWH_JOB_KENNUNG` (Literal `'ABTN_SMART_KUBI'`): Map as a JOB-SPECIFIC configuration constant.
* `cdate` (Legacy dynamic function `SYS_DATE("YYYYMMDD")`): Generated dynamically at runtime using the Airflow logical date, equivalent to the Jinja template variable `{{ ds_nodash }}`.
* `cmonth` (Legacy extraction `SUBSTR(&cdate,1,6)`): Derived dynamically from `cdate` by capturing the first 6 characters (YYYYMM). Map to `ds_nodash[:6]`.
* `cday` (Legacy extraction `SUBSTR(&cdate,7,2)`): Derived dynamically from `cdate` by capturing characters 7 and 8 (DD). Map to `ds_nodash[6:8]`.
* `first` (Literal `'01'`): Map as a JOB-SPECIFIC constant.
* `cmonth` (Legacy expression `&cmonth&first`): Constructs a date representation representing the first day of the current execution month (YYYYMM01).
* `cmonth` (Legacy subtract function `SUB_DAYS(&cmonth,1)`): Subtracts 1 day from the first of the month to yield the last day of the previous month. Equivalent in Python: `(datetime.strptime(cmonth, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")`.
* `cmonth` (Legacy extraction `SUBSTR(&cmonth,1,6)`): Truncates the previous month's date back to its year-month representation (YYYYMM).
* `MONATSID` (Legacy reference `&cmonth`): The resulting calculated month-identifier passed as an execution variable to the database queries (such as `d_abtn_x_smart_kubi.sql`).

---

### Lineage Edges
* **Upstream Configurations (USES_CONFIG):**
  * `.dw_init` sources `.DW_GLOBAL` (unresolved configuration script). *Note: This has a human-confirmed resolution of "NO SOURCE NEEDED (not needed)".*
  * `.dw_init` sources `.DW_LOKAL` (unresolved configuration script). *Note: This has a human-confirmed resolution of "NO SOURCE NEEDED (not needed)".*
* **Downstream Consumers:**
  * Variables and configurations exported by `.dw_init` are consumed by downstream shell utilities and database wrappers (e.g., `r_sqlscript`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`) that reside in sibling directories and belong to separate migration groups.

---

### External System Replacements
* **Oracle System Variables:** `ORACLE_HOME` and Oracle admin directories (`DW_DIR_UTL_FILE`) are legacy database-specific constructs and are retired on the native BigQuery platform.
* **Remote Customer Host:** `DW_HOST_CUSTOMER` (`dxcst3.bn.detemobil.de`) must be migrated to a Cloud Composer connection, GCP Secret Manager secret, or Airflow Variable if downstream tasks require SFTP/remote system data transfer.

---

### Cross-File Dependencies
* **Sourced Scripts:** Sourcing of the `.dw_global` and `.dw_lokal` files. While the legacy shell script sources these at runtime via `. $HOME/.dw_global`, the target environment should load environment variables or parameters through Cloud Composer configurations or a global environment YAML.

---

### Target File Plan
* **File Path:** `dw_init.py` (mirrored folder structure relative to the root directory)
* **Language:** Python
* **Source File:** `local/home/gurunathan_t/kubi/.dw_init`
* **Purpose:** Sets and exports path variables, folder locations, and host configurations as environment variables in Python (`os.environ`) to dynamically initialize the workspace context for downstream Python operators in Airflow.

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
* `GCS_BUCKET` (Canonical GCP environment variable, sourced via `os.environ.get("GCS_BUCKET")` or Airflow `Variable.get("GCS_BUCKET")`): Replaces the base environment directory `$HOME/daten/` for the DWH platform.
* `DW_HOST_CUSTOMER` ("dxcst3.bn.detemobil.de"): Environment host identifier, sourced via GCP Secret Manager or Airflow Connection configurations.
* **Legacy Directories** (`DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, and all import/export directories `DW_DIR_IMP_*` / `DW_DIR_EXP_*`): These define directory structures across the entire DWH workspace. In the target cloud infrastructure, these paths correspond to prefixes on Cloud Storage and should be mapped to global cloud constants referencing the central bucket (e.g., `DW_DIR_PROT` becomes `gs://{GCS_BUCKET}/daten/logfiles`).

#### 2. JOB-SPECIFIC
* `ORACLE_SID`: Sourced as a specific job parameter or Airflow DAG param if still needed for database context resolution, otherwise retired.

---

### Risks and Manual Steps
* **B4 REDESIGN / STORAGE MIGRATION:** Local server filesystem directories (e.g., `DW_DIR_IMP_D1`, `DW_DIR_IMP_SAP`, etc.) must be fully mapped to Cloud Storage bucket paths. A manual migration task is required to verify that files transferred from external vendors are routed directly to these corresponding Cloud Storage locations.
* **B4 REDESIGN / INFRASTRUCTURE RETIREMENT:** Directory-checking and path-validation logic for local Oracle installations (`/appl/local/oracle/...`) is hardware-specific and should be completely retired, as analytics workloads are moved to BigQuery.
* **UNRESOLVED CONFIGURATIONS:** `.dw_global` and `.dw_lokal` are listed as configuration dependencies. Although human-reviewed as "NO SOURCE NEEDED", any environment-wide parameters, credentials, or global settings originally set inside these scripts must be carefully audited and migrated to GCP Secret Manager or Airflow Variables.

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
1.1 Oracle SQL Object Type:
    - PL/SQL Anonymous Block containing dynamic DDL execution and a multi-table aggregate DML `INSERT INTO ... SELECT` query.

1.2 Business Logic Summary:
    - This script performs a monthly aggregation and load job into the target data warehouse table `DWH$TA_T_SMART_KUBI`.
    - It takes two input parameters: `&1` (monthly partition ID in `YYYYMM` format) and `&2` (entry log number).
    - It first truncates the target table `DWH$TA_T_SMART_KUBI` via a custom helper package.
    - It then runs a multi-table join aggregation, filtering on the specified partition month from the fact table `DWH$TA_F_D1_TWVV_TN` (using dynamic partition mapping).
    - The fact data is outer-joined with tariff mappings (using a CTE of active tariffs) and active contract dimensions (`DWH$TA_C_VERTRAG`).
    - The aggregated rows are inserted into the target table, the row count is tracked, and any errors are logged via a custom package execution in an exception block.

1.3 Entities Referenced:
    - `dwh$ta_t_smart_kubi` (Target table)
    - `dwh$vi_l_map_fa_tarif` (Source view, aliased as `t`)
    - `bl_d_tarif` (Source dimension table, aliased as `tar`)
    - `dwh$ta_f_d1_twvv_tn` (Source fact table, aliased as `fact`, partitioned dynamically)
    - `dwh$ta_c_vertrag` (Source dimension table, aliased as `d`)
    - `dwpa_util_skript.runstatement` (External Oracle package for dynamic execution)
    - `dwpa_meldung.fehler` (External Oracle package for error logging)
    - `dwpa_globals.k_alis_err_unknown` (External package global variable)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → converted to `INT64`
    - `NUMBER` → converted to `INT64` (for IDs/counts) or `NUMERIC`
    - `VARCHAR2` → converted to `STRING`
    - `DATE` (includes time component) → mapped to `DATETIME` to fully preserve hours/minutes/seconds.
    - Special Note: Oracle table names containing special character `$` (e.g., `dwh$ta_t_smart_kubi`) are invalid in BigQuery. They must be resolved by replacing `$` with `_` (e.g., `dwh_ta_t_smart_kubi`).

2.2 Implicit and Explicit Type Casting:
    - `to_number('&1')` → converted to explicit `CAST(? AS INT64)`
    - `To_date('4712-12-31', 'YYYY-MM-DD')` → converted to explicit `CAST('4712-12-31' AS DATETIME)`

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` → converted to `COALESCE(t_new.tarif_id, 0)`
    - `NVL(t_old.tarif_id, 0)` → converted to `COALESCE(t_old.tarif_id, 0)`
    - `Decode(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` → converted to:
      `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `Decode(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` → String trimming and dynamic replacement logic is resolved in BigQuery using a explicit CASE-WHEN statement:
      ```sql
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' OR TRIM(fact.vo_kenn_bearb) = '#' 
          THEN fact.vo_kenn 
        ELSE fact.vo_kenn_bearb 
      END
      ```

2.4 String Functions:
    - `ltrim(rtrim(val))` → converted to `TRIM(val)`
    - `to_char(val)` → converted to `CAST(val AS STRING)`

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)` → converted to BigQuery equivalent:
      `DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH)`
    - `to_char(fact.gueltigkeitszeitpunkt,'yyyymm')` → converted to BigQuery equivalent:
      `FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt)`

2.8 Set and Join Operations:
    - Partition-extended table syntax `dwh$ta_f_d1_twvv_tn partition(...)` is not supported in BigQuery. The partition reference must be stripped, querying the base table directly. Partition pruning is automatically handled by the WHERE clause filter on `gueltigkeitszeitpunkt`.
    - Oracle outer join syntax `(+)` → converted to standard ANSI `LEFT OUTER JOIN` syntax.

2.9 Row Limiting and Tracking:
    - `SQL%ROWCOUNT` → converted to BigQuery system scripting variable `@@row_count`.

2.13 DDL Constructs:
    - Table name containing `$` character replaced by `_` for BigQuery naming compatibility.
    - Oracle `/*+ Append */` and parallel/hash join hints are stripped.

2.14 PL/SQL Block Structures:
    - PL/SQL anonymous block structure (`DECLARE ... BEGIN ... EXCEPTION ... END`) is converted to BigQuery Scripting block syntax with native variables and error handling (`BEGIN ... EXCEPTION WHEN ERROR THEN ... END`).

2.15 Unresolvable or Advisory Items:
    - Oracle DB-specific utilities `dwpa_util_skript.runstatement`, `dwpa_globals`, and `dwpa_meldung.fehler` are native schema packages and cannot be automatically executed. In BigQuery, dynamic table truncation is replaced with direct SQL `TRUNCATE TABLE`, and error logging is converted to standard script error diagnostics.

Step 3: Conversion Strategy Summary
3.1 Structure: Convert the PL/SQL script into a single BigQuery standard multi-statement scripting block.
3.2 Assumptions:
    - It is assumed that parameter substitution inputs (`&1` and `&2`) are supplied as scripting variables or query parameters in the target orchestration environment.
    - It is assumed that the source table naming convention using `$` is mapped to `_` in the target BigQuery dataset.
3.3 Flagged Items:
    - Package-level error logger calls are replaced with generic error capture (`@@error.message`). These require manual mapping if a centralized audit log schema exists in BigQuery.

═══════════════════════════════════════════
MIGRATION DECISION & REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Oracle Source Element | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| PL/SQL Script Block | BigQuery Multi-Statement Scripting | Python wrapper | BQ native scripting supports variables, transaction-like rollback states, loops, and exception handling without external runtime dependencies. |
| `dwpa_util_skript.runstatement` | Direct BigQuery `TRUNCATE TABLE` | Python dynamic executing | The string input only issues static truncate logic. Replacing it with static DDL in BQ SQL reduces pipeline execution complexity. |
| Table Partition Extension `partition(...)` | Direct ANSI table reference with `WHERE` filter | BigQuery Table Partition Decorator | BQ Table Partition Decorators (e.g. `table$201509`) are legacy; standard WHERE filters on a partitioned column are the optimal, modern practice. |
| `dwpa_meldung.fehler` | Standard BQ Scripting Error Handler | Python error handler | Direct script exception handling (`EXCEPTION WHEN ERROR THEN`) can capture execution metadata natively. |

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: A standard `.sql` script containing variable declarations, initialization, table truncation, table insertion with nested CTEs, and exception handlers.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Type | BigQuery Type | Conversion Rule / Expression | Warning / Note |
| :--- | :--- | :--- | :--- |
| `pls_integer` | `INT64` | `DECLARE variable INT64;` | Standard precision mapping. |
| `NUMBER` (for IDs) | `INT64` | `CAST(value AS INT64)` | If numbers contain decimals, they must map to `NUMERIC`. |
| `VARCHAR2` | `STRING` | `CAST(value AS STRING)` | Safe conversion; variable length constraints are dropped. |
| `DATE` | `DATETIME` | `CAST(value AS DATETIME)` | Mapped to `DATETIME` to preserve time components without timezone conversion. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns Found**: Anonymous PL/SQL, Dynamic DDL call, dynamic partition queries, legacy outer-join operators (`(+)`).
- **Unsupported Functions**: Oracle hints, dynamic utility packages, package-level error routines.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: `dwh_ta_t_smart_kubi`, `dwh_vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh_ta_f_d1_twvv_tn`, `dwh_ta_c_vertrag`.
- **Warnings**: Centralized exception logging and audit details will need custom mapping to BigQuery logging tables.

OVERALL MIGRATION STRATEGY: Python Wrapper Required

*(Note: Chosen "Python Wrapper Required" in strict compliance with the decision rules because custom package components `dwpa_meldung.fehler` and `dwpa_util_skript` cannot be resolved natively within pure SQL engine limits and require dynamic orchestrator handling or manual intervention).*

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(value AS INT64)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', value)` or `PARSE_DATETIME` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATETIME` or `CAST(value AS STRING)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(value, INTERVAL x MONTH)` |
| `DECODE` | Direct-with-rewrite | `CASE WHEN` expression |
| `NVL` | Direct-with-rewrite | `COALESCE(value, replacement)` |
| `LTRIM / RTRIM` | Direct-with-rewrite | `TRIM(value)` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` system variable |
| `dwpa_util_skript.runstatement` | Unsupported | None — manual intervention (replaced by static `TRUNCATE TABLE`) |
| `dwpa_meldung.fehler` | Unsupported | None — manual intervention (captured via `@@error.message`) |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | `SELECT` statement displaying final logged rowcount |

&nbsp;
 
═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════
 
Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Declare scripting variables for tracking execution and storing parameters
DECLARE v_anzahl_ds INT64 DEFAULT 0; -- converted from Oracle pls_integer
DECLARE l_monats_id INT64;           -- input parameter 1 (e.g., 201509)
DECLARE EintragsNr INT64;            -- input parameter 2
DECLARE lv_str STRING;               -- converted from VARCHAR2(300)
DECLARE l_monats_date DATE;          -- converted from Oracle DATE type

-- Emulated Parameter Initialization
SET l_monats_id = CAST(@parameter_1 AS INT64); -- converted from TO_NUMBER('&1')
SET EintragsNr = CAST(@parameter_2 AS INT64);  -- converted from TO_NUMBER('&2')

-- Calculate start of next month based on l_monats_id (format: YYYYMM)
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH); -- converted from ADD_MONTHS(TO_DATE(...), 1)

BEGIN
  -- Truncate target table
  -- converted from dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')
  -- resolved name from dwh$ta_t_smart_kubi to dwh_ta_t_smart_kubi
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Standardized set-based aggregate insert block
  -- Oracle execution hints (Append, parallel, use_hash) stripped
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
  WITH temp AS 
  ( 
       -- Anchor CTE mapping valid tariff definitions
       SELECT
              t.tarif_id,
              t.dwh_tarif_id,
              t.gueltig_von,
              t.gueltig_bis,
              tar.mp_geschaeftsfeld_id
       FROM   dwh_vi_l_map_fa_tarif AS t
       INNER JOIN bl_d_tarif AS tar 
          ON t.tarif_id = tar.tarif_id
       WHERE  t.gueltig_bis = CAST('4712-12-31' AS DATETIME) -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT 
         l_monats_id AS monats_id,
         
         -- converted from DECODE(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)
         CASE 
           WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
           ELSE d.t_mobile_kundennummer 
         END AS kundennummer,
         
         COALESCE(t_new.tarif_id, 0) AS tarif_id, -- converted from NVL(t_new.tarif_id, 0)
         COALESCE(t_old.tarif_id, 0) AS tarif_id_alt, -- converted from NVL(t_old.tarif_id, 0)
         
         -- Converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)
         -- Handled both NULL and empty string outcomes from BigQuery TRIM
         CASE 
           WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' OR TRIM(fact.vo_kenn_bearb) = '#' 
             THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END AS vo_kennung,
         
         d.test_gp, 
         SUM(fact.zugang) AS anzahl, 
         fact.kennzahl_id 
  FROM     dwh_ta_f_d1_twvv_tn AS fact -- Partition clause stripped; base table accessed directly
  LEFT OUTER JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh_ta_c_vertrag AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    -- Type-safe comparison using l_monats_date casted to DATETIME
    AND CAST(l_monats_date AS DATETIME) > d.gueltig_von 
    AND CAST(l_monats_date AS DATETIME) <= d.gueltig_bis 
  WHERE    FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING) -- converted from TO_CHAR
  AND      fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
         -- Group By clauses matching SELECT-level transformation expressions 
         CASE 
           WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
           ELSE d.t_mobile_kundennummer 
         END, 
         COALESCE(t_new.tarif_id, 0), 
         COALESCE(t_old.tarif_id, 0), 
         CASE 
           WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' OR TRIM(fact.vo_kenn_bearb) = '#' 
             THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END, 
         d.test_gp, 
         fact.kennzahl_id;

  -- Store processed row count
  SET v_anzahl_ds = @@row_count; -- converted from SQL%ROWCOUNT

  -- Output results log (converted from DBMS_OUTPUT.PUT_LINE)
  SELECT FORMAT('%d rows inserted in DWH_TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Exception handling mapping PL/SQL Exception Block
  -- Standard rollback is automatic for failed transactions inside multi-statement queries in BigQuery
  DECLARE ErrText STRING;
  DECLARE ErrC STRING;
  
  SET ErrText = @@error.message;
  SET ErrC = @@error.status;
  
  -- Log error structure to output stream
  SELECT FORMAT('ERROR: Code %s, Message: %s during run of entry log %d', ErrC, ErrText, EintragsNr) AS error_log;
  
  -- Re-throw exception back to processing engine
  RAISE USING message = ErrText;
END;
```

&nbsp;
 
═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic Partition Reference replacement**: The original script pointed to partition reference `partition(dwh$ta_f_d1_twvv_tn_&1)`. In the converted code, we dropped the partition-extension syntax and used a native where clause filter (`FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)`). Verify that `gueltigkeitszeitpunkt` is the actual partitioned column of target table `dwh_ta_f_d1_twvv_tn` in BigQuery to guarantee partition pruning.
2. **Standardizing Special Table Names**: The table name patterns with `$` (e.g., `DWH$TA_T_SMART_KUBI`) were translated to `_` characters (e.g., `dwh_ta_t_smart_kubi`). This must match the actual deployment scheme in the BigQuery destination dataset.
3. **External Package Logic**: The external call to `dwpa_meldung.fehler` inside the exception handler was replaced by a native BigQuery scripting `SELECT` statement and a `RAISE` directive. If logging must write to a central database table, this logic must be converted to a specific `INSERT INTO logging_table` statement.

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | Converted to a native BigQuery scripting statement containing table truncation and insert-select logic. |

# Additional Context the MCP Could Not See

### Execution Order
The target orchestration (via Cloud Composer/Airflow DAG) must preserve the logical flow of the legacy pipeline:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Legacy UC4 orchestration) maps to the Cloud Composer DAG itself.
2. **.dw_init** (Environment initialization) maps to Airflow configuration variables, runtime environment configs, and the BigQuery connection profile.
3. **r_sqlscript / h_alis_sqlplus.ksh** (SQL Execution wrappers) are retired in favor of native Airflow execution operators (`BigQueryInsertJobOperator`).
4. **d_abtn_x_smart_kubi.sql** (The SQL block converted here) executes inside the BigQuery operator.
5. **f_alis_msgerr.ksh** (Error trapping helper) is retired. Standard scripting block `EXCEPTION` handling and Airflow task status alert callbacks (e.g., `on_failure_callback`) replace it.

### Schedule & Variables
The legacy scheduler-set variables must be calculated natively within the Cloud Composer/Airflow scheduler and passed dynamically into the BigQuery task:
* **cdate** = `SYS_DATE("YYYYMMDD")`
* **cmonth** / **cday** / **first** / **SUB_DAYS** / **MONATSID** logic: This logic shifts the execution date to the previous month in `YYYYMM` format.
* **Target Mapping**:
  * Pass `MONATS_ID` (Parameter `1` / `&1`) dynamically using Airflow macros:
    `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}`
  * Pass `EintragsNr` (Parameter `2` / `&2`) dynamically using Airflow's run context:
    `{{ run_id }}` or `{{ task_instance.try_number }}`

### Lineage
* **Upstream Producers (Inputs)**:
  * `TABLE:DWH$TA_F_D1_TWVV_TN` (Fact table)
  * `VIEW:DWH$VI_L_MAP_FA_TARIF` (Tariff Mapping View)
  * `TABLE:BL_D_TARIF` (Tariff Dimension)
  * `TABLE:DWH$TA_C_VERTRAG` (Contract Dimension)
* **Downstream Consumers (Outputs)**:
  * `TABLE:DWH$TA_T_SMART_KUBI` (Target table)

### Cross-File Dependencies
* **Shared Tables & Views**: The core fact table `DWH$TA_F_D1_TWVV_TN` and active contract tables are referenced across other jobs in the `DW.DWH_ABTN_SMART` prefix.
* **Orchestration Bindings**: The execution depends on parameters initialized during the DAG startup phase (formerly `.dw_init`).

### Target File Plan
* **Target Path**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  * **Language**: BigQuery SQL (Multi-Statement Scripting)
  * **Source**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`

### Environment-Specific Values
* **GLOBAL**:
  * `GCP_PROJECT`: Sourced in Python/Airflow using `Variable.get("GCP_PROJECT")` and injected into queries via parameter injection.
  * `BQ_DATASET`: Sourced via `Variable.get("BQ_DATASET")` to identify the correct target dataset.
* **JOB-SPECIFIC**:
  * `MONATS_ID`: Mapped to query parameter `@monats_id` and resolved via Airflow's macro context.
  * `EintragsNr`: Mapped to query parameter `@eintrags_nr` and resolved via Airflow runtime metrics.

### Risks & Manual Steps
* **Oracle Package Truncate Call**: The original call to `dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')` is replaced in the converted script with a direct BigQuery `TRUNCATE TABLE` DDL statement.
* **Oracle Exception Logging**: The exception block calls `dwpa_meldung.fehler` to write error details into an Oracle-specific system table. This logic has been converted to standard scripting error diagnostics. If a centralized auditing/logging framework exists in the target architecture, a manual step is required to map `@@error.message` and `@@error.status` to a BQ audit table insert.
* **Fact Table Partition Access**: The original code accesses a partition explicitly: `dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, partition extension syntax is not supported. The target file queries the base table directly and relies on BigQuery's automatic partition pruning via the `WHERE FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)` filter. Human review must verify that `gueltigkeitszeitpunkt` is the actual partitioned column of `dwh_ta_f_d1_twvv_tn` in BigQuery to prevent full table scans.

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
REASON: The script is a reusable shell library defining multiple logging, timing, and error-handling functions that interact with Oracle via SQL*Plus and perform temp-file manipulation.

EVIDENCE
- Business logic found: KSH custom logic. It defines functions for logging, timing, and error-handling, utilizing local state, file generation, and multiple calls to Oracle via SQL*Plus.
- AWK: none
- SQL-expressible: no (the script is a procedural, stateful environment and library module designed to orchestrate logging rather than executing simple data transformations)
- Non-SQL side effects: file generation/removal in `/tmp/` and dynamic KSH evaluation (`eval`) of parent-shell scope variables.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script is a KornShell function library (`f_alis_msgerr.ksh`, also referenced as `dwmsg.ksh`) used for error management, state tracking, and logging in the "Information Services" system. It provides generic error handling, status tracking (OK/Aborted), and log file generation by wrapping Oracle database operations. Sourced by wrapper execution scripts, it enforces standardized logging conventions and interacts with an Oracle package named `BERT_MELDUNG`.

2. INVOCATION CONTEXT
   - Who calls this script: It is designed as a reusable function library and is sourced (`. f_alis_msgerr.ksh`) by parent KornShell execution scripts running in the environment. It is not directly invoked as a standalone UC4 job.
   - UC4 native includes: None referenced inside this file.
   - Environment variables expected:
     - `DW_ORAUSER`: Contains the database connection string or credentials used for SQL*Plus invocation.
     - `DW_DIR_ROOT`: Sourced base directory path used to resolve SQL script templates.
     - `DW_DIR_PROT`: Sourced target directory path for logs/protocols.

3. PARAMETERS / INPUTS
   The library functions read parameters passed directly to them at invocation. Environment parameters are also fetched directly from the parent context.

   Environment Parameters:
   - `DW_ORAUSER` (Source: Parent environment, DB-connection-style parameter, used directly in SQL*Plus connection)
   - `DW_DIR_ROOT` (Source: Parent environment, generic directory path, used to find companion SQL scripts)
   - `DW_DIR_PROT` (Source: Parent environment, generic directory path, used to build log file names)

   Function Positional Parameters:
   - `DWMSG_Fehlerbehandlung`: `$1` (Entry ID in logging table, used to register errors)
   - `DWMSG_SetzeStatusOK`: `$1` (Entry ID in logging table, used to mark successful completion)
   - `DWMSG_SetzeStatusAbbruch`: `$1` (Entry ID in logging table, used to mark execution failure)
   - `DWMSG_ErmittleNr`: `$1` (KSH variable name to assign generated registration ID via `eval`) -> *Refactored in Python to return value.*
   - `DWMSG_ErzeugeEintrag`: `$1` (Entry ID), `$2` (Job identification), `$3` (Program name), `$4` (Log file path)
   - `DWMSG_MeldeFehler`: `$1` (Entry ID), `$2` (Error Type: F/E/W), `$3` (Error Code), `$4` (Optional Details 1), `$5` (Optional Details 2)
   - `DWMSG_Logdateiname`: `$1` (KSH variable name to assign constructed log path via `eval`), `$2` (Job identification), `$3` (Entry ID) -> *Refactored in Python to return value.*
   - `DWMSG_SetzeStichtagInfo`: `$1` (Entry ID), `$2` (Key Date / Stichtag), `$3` (Date format string)
   - `DWMSG_AppendTimingInfos`: `$1` (Entry ID), `$2` (Info message), `$3` (Timestamp format string)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s $DW_ORAUSER` and `sqlplus $DW_ORAUSER`
     - Purpose: Connects to Oracle database to execute stored procedures or PL/SQL blocks using script files (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, `d_alis_spaufruf_p4.sql`) or inline code blocks.
     - Target: Should become a native Python DB-client call utilizing python-oracledb (native Oracle driver). Spawning external subprocesses for database calls is inefficient and raises security concerns.
     - Resolvable Launcher: No, these are direct SQL*Plus client executable calls.

5. EMBEDDED SQL
   The script does not contain inline tabular SELECT statements, but rather runs procedure executions via Oracle SQL*Plus.
   - Target package: `BERT_MELDUNG` (specifically procedures `SetzeStatusOk`, `SetzeStatusAbbruch`, `Erzeuge_Eintrag`, `Fehler`, and `SetzeZusatzInfos`).
   - Referenced SQL Templates (Paths resolved using `$DW_DIR_ROOT`):
     - `allgemein/is/util/sql/d_alis_spaufruf_p1.sql` (Execution of single-parameter procedure)
     - `allgemein/is/util/sql/d_al_is_ermittlenr.sql` (Fetches next registration sequence number into a file)
     - `allgemein/is/util/sql/d_alis_spaufruf_p4.sql` (Execution of 4-parameter registration procedure)
     - `allgemein/is/util/sql/d_alis_spaufruf_p3.sql` to `d_alis_spaufruf_p5.sql` (Dynamic utility scripts used inside `DWMSG_MeldeFehler` based on argument counts)
   - Dialect is unambiguously Oracle SQL / PL/SQL, indicated by package-procedure invocations, SQL*Plus execution blocks (`EXEC...`), `to_date()`, `to_char()`, `commit;`, and SQL*Plus command-line arguments.

6. CONTROL FLOW
   - Sourcing: Library file defines functions inside execution context.
   - **`DWMSG_Fehlerbehandlung`**: Captures previous execution exit status (`$?`). Informs DB of fatal error via `DWMSG_MeldeFehler` (passing code 10 and status message). Sets job status to aborted via `DWMSG_SetzeStatusAbbruch`.
   - **`DWMSG_SetzeStatusOK`**: Validates entry ID argument. Runs `BERT_MELDUNG.SetzeStatusOk` via SQL*Plus.
   - **`DWMSG_SetzeStatusAbbruch`**: Validates entry ID argument. Runs `BERT_MELDUNG.SetzeStatusAbbruch` via SQL*Plus.
   - **`DWMSG_ErmittleNr`**: Validates destination variable name. Runs sequence generator SQL script, saving result to `/tmp/ErmittleNr_$$.lst`. Reads file, strips whitespace, assigns to target variable via shell `eval`, and removes temp file.
   - **`DWMSG_ErzeugeEintrag`**: Validates entry ID. Runs `BERT_MELDUNG.Erzeuge_Eintrag` with supplied job details.
   - **`DWMSG_MeldeFehler`**: Validates entry ID. Selects wrapper SQL script based on optional arguments provided. Executes `BERT_MELDUNG.Fehler` procedure.
   - **`DWMSG_Logdateiname`**: Constructs log file path based on directory path, job name, current system timestamp, and the entry ID. Returns via shell `eval`.
   - **`DWMSG_SetzeStichtagInfo`**: Validates inputs. Executes PL/SQL block invoking `BERT_MELDUNG.SetzeZusatzInfos` utilizing Oracle `to_date` conversion.
   - **`DWMSG_AppendTimingInfos`**: Validates inputs. Appends execution date/timestamp tracking information to DB logging record.

7. ERROR HANDLING & EXIT CODES
   - Validations: Missing required positional parameters results in direct shell stdout output followed by `exit 1` or `exit 2`.
   - Database Errors: SQL*Plus executions are launched in silent mode (`-s`) but lack explicit transaction success checks in the shell (no `WHENEVER SQLERROR EXIT`).
   - Python translation: Validation errors should raise custom exceptions or use `sys.exit` when acting within execution scripts. Native database connections should utilize try-except blocks handling `oracledb.Error` exceptions.

8. OUTPUTS / SIDE EFFECTS
   - Writes registration, timing, and error logs directly to the database via `BERT_MELDUNG` package.
   - Deploys temporary lists `/tmp/ErmittleNr_[PID].lst` (cleaned up immediately).
   - Generates string paths for operational log/protocol files.

9. BUSINESS SUMMARY
   - Serves as the central logging and orchestration handler for the "Information Services" warehouse environment.
   - Guarantees standard tracking of batch processing steps by writing job state updates to Oracle tables.
   - Registers error exceptions, capturing standard operational error codes alongside specific runtime logs.
   - Keeps execution timing metrics and key processing date limits (Stichtag) for operational auditing.

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
import os
import sys
import datetime
import tempfile
import oracledb  # Standard Oracle driver

# Global/Environment configuration
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")

# Helper function to get a DB connection
def get_db_connection():
    # REVIEW: Target database connection established from legacy DW_ORAUSER.
    # Ensure correct credentials and TNS setup in target environment.
    return oracledb.connect(dsn=DW_ORAUSER)

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code):
    # Capture error status and report to tracking system
    k_unerw_fehler = 10
    msg = f"ErrorCode ist: {last_exit_code}"
    
    print("Ich bin im Fehlerhandler, fehler der DB melden...", file=sys.stderr)
    dwmsg_melde_fehler(eintrags_nr, "F", k_unerw_fehler, msg)
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus", file=sys.stderr)
    dwmsg_setze_status_abbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Execute database status update
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Replaces call to d_alis_spaufruf_p1.sql with direct procedure invocation
            cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
            conn.commit()


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Execute database status update
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
            conn.commit()


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    # MANDATORY AUDIT STEP CHECKLIST:
    # # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
    
    # Query database to get unique generated registration ID
    # Replaces execution of d_al_is_ermittlenr.sql writing to temporary files
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Assuming sequencing query logic from d_al_is_ermittlenr.sql
            # In Python, we bypass the need for '/tmp/ErmittleNr_$$' file generation.
            cursor.execute("SELECT BERT_MELDUNG_SEQ.NEXTVAL FROM DUAL")  # REVIEW: Verify actual generation logic in d_al_is_ermittlenr.sql
            row = cursor.fetchone()
            if row:
                return str(row[0]).strip()
            else:
                raise RuntimeError("Failed to fetch next tracking sequence number")


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Register the job entry details
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programmname, log_datei])
            conn.commit()


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Log incident with optional details parameters
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Calls procedure BERT_MELDUNG.Fehler
            # Resolves dynamic signature cleanly in Python
            cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
            conn.commit()


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # Builds standard protocol log path and returns it directly
    # Replacing original shell 'eval' architecture with return
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    log_name = f"{job_kennung}_{current_time}_{eintrags_nr}.log"
    return os.path.join(DW_DIR_PROT, log_name)


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    # Guard check for missing operational inputs
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Update execution metadata using direct PL/SQL bindings
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Map parameters safely to avoid SQL injection
            # Oracle to_date handling is replaced by converting python datetime or passing strings
            plsql_block = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:e_nr, TO_DATE(:stich, :fmt));
            END;
            """
            cursor.execute(plsql_block, e_nr=eintrags_nr, stich=stichtag, fmt=stichtag_fmt)
            conn.commit()


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    # Guard check for missing operational inputs
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Log progress status and timing metrics to Oracle DB
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Construct statement with native current database timestamp formatted via parameters
            plsql_block = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:e_nr, NULL, :info || ' ' || TO_CHAR(SYSDATE, :fmt) || ' ');
            END;
            """
            cursor.execute(plsql_block, e_nr=eintrags_nr, info=info_text, fmt=date_format)
            conn.commit()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Converts the KornShell logging, timing, and error-management helper library into a Python utility module. |

### Execution Order
The legacy dependency graph shows the following sequence, which must be preserved in the target Cloud Composer (Airflow DAG) or orchestration workflow:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (Orchestration entry)
2. `d_abtn_x_smart_kubi.sql` (Main PL/SQL transformations)
3. `r_sqlscript` (Shell utility calling PL/SQL)
4. `.dw_init` (Initialization environment shell script)
5. `f_alis_msgerr.ksh` (This logging/error-management utility library, imported/sourced before execution)
6. `h_alis_sqlplus.ksh` (SQL*Plus helper execution utility)

In the target Python environment, `f_alis_msgerr.py` will be imported as a library module at the start of any job task that requires standardized logging and state-tracking.

### Schedule & Variables — Must Be Retained
This job is triggered by the orchestration framework (`DW.DWH_ABTN_SMART_KUBI`). 
The following scheduler variables must be computed dynamically or passed via Composer DAG params:
* `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'` (Passed as job config or DAG parameter)
* `cdate` = `SYS_DATE("YYYYMMDD")` (Evaluated dynamically in Airflow using `{{ ds_nodash }}`)
* `cmonth` = First 6 characters of `cdate` (`SUBSTR(&cdate,1,6)`)
* `cday` = Last 2 characters of `cdate` (`SUBSTR(&cdate,7,2)`)
* `first` = `'01'`
* `MONATSID` = Computed as `SUBSTR(SUB_DAYS(cmonth + '01', 1), 1, 6)` to represent the prior month's ID.

These variables will be passed into the job-specific execution environment via a single job-level configuration dictionary or Airflow task parameters.

### Lineage
* **Downstream Consumers (via Lineage Edges):**
  * `f_alis_msgerr.ksh` invokes `PROCEDURE:SETZEZUSATZINFOS` (Oracle procedural execution).

### External System Replacements
* **Oracle Database (SQL\*Plus):** The shell script utilizes standard SQL\*Plus command-line clients to interface with an Oracle database instance.
  * *Target Replacement:* This connection is replaced in Python using the native `oracledb` client library to execute package procedures directly. If the metadata tracking tables (under `BERT_MELDUNG`) are migrated to Google BigQuery, these database procedures should be rewritten as BigQuery stored procedures and accessed using the Python BigQuery Client API.

### Cross-File Dependencies
* This library script is sourced by other utility scripts (`r_sqlscript`, `h_alis_sqlplus.ksh`) and parent execution wrappers.
* It relies on SQL templates located in `$DW_DIR_ROOT/allgemein/is/util/sql/` (e.g., `d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, `d_alis_spaufruf_p4.sql`), which define how SQL\*Plus passes positional arguments to Oracle package procedures.

### Target File Plan
* **Target File:** `local/home/gurunathan_t/kubi/f_alis_msgerr.py`
  * *Language:* Python
  * *Source File:* `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
  * *Purpose:* Implements the `DWMSG` functions (`dwmsg_fehlerbehandlung`, `dwmsg_setze_status_ok`, `dwmsg_setze_status_abbruch`, `dwmsg_ermittle_nr`, `dwmsg_erzeuge_eintrag`, `dwmsg_melde_fehler`, `dwmsg_logdateiname`, `dwmsg_setze_stichtag_info`, `dwmsg_append_timing_infos`) as a clean, reusable Python library module.

### Environment-Specific Values
The environment variables from the original shell script must be classified and sourced as follows:

1. **GLOBAL (Environment-Wide):**
   * `DW_ORAUSER`: The Oracle database connection string/credentials. Sourced via `os.environ.get("DW_ORAUSER")` or stored as an Airflow Connection ID.
   * `DW_DIR_ROOT`: Sourced via `os.environ.get("DW_DIR_ROOT")` representing the base path of the code repository.
   * `DW_DIR_PROT`: Sourced via `os.environ.get("DW_DIR_PROT")` representing the centralized logs and protocols storage folder.

### Risks and Manual Steps
* **Sourcing and Evaluation Mechanism (`eval`):** The legacy shell script utilizes dynamic evaluation (`eval "$VarName=..."`) to return values back to the calling shell scope. In Python, this dynamic behavior is replaced with standard function return values, meaning all calling/wrapper scripts must be refactored to handle these return values instead of expecting modified environment variables.
* **Output / Print Literal Rule:** Original log/print statements in German (e.g. `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`, `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`) must be retained verbatim in the converted Python print statements to ensure operational log consistency. Do not translate or modify them.
* **Missing SQL Files:** The logic of `d_al_is_ermittlenr.sql` (sequence generator) and `d_alis_spaufruf_p*.sql` is external. The conversion assumes standard direct stored procedure calls (`BERT_MELDUNG.SetzeStatusOk`, etc.). If the target database is migrated to BigQuery, these database procedures, tables, and sequences under `BERT_MELDUNG` must be manually recreated in BigQuery.

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
REASON: The script defines a custom utility function with parameter validation, file system checks, and external sqlplus process invocation.

EVIDENCE
- Business logic found: KSH custom logic defines a reusable utility function (`starteSQLSkript`) that validates input parameters, verifies file system readability of a target SQL script, and executes it via Oracle SQL*Plus.
- AWK: none
- SQL-expressible: no (the script is a wrapper/orchestration utility executing dynamic files via an external database CLI client, not a set of database transformations).
- Non-SQL side effects: Invocation of external SQL*Plus process, file readability checks, and custom external error reporting command (`DWMSG_MeldeFehler`).
- Against this verdict: none (it is a procedural shell script and cannot be executed as raw SQL).

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `h_alis_sqlplus.ksh` is a helper library containing KornShell utility functions for launching Oracle SQL*Plus scripts. Specifically, it exports a function `starteSQLSkript` that ensures prerequisites—such as argument completeness and script file accessibility—are met before invoking the `sqlplus` executable. It acts as an orchestration layer to safely propagate execution codes and log execution statistics in legacy environments.

2. INVOCATION CONTEXT
   - Who calls this script: It is sourced (via `. h_alis_sqlplus.ksh` or similar) by other legacy Bourne/Korn shell jobs running within UC4/Automic to import the `starteSQLSkript` routine.
   - UC4 includes referenced: None.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   The module variables declared at the top-level are:
   - `ModulName`: Set to `"alis_sqlplus"` (used for identification).
   - `ModulVersion`: Set to `"V1.1.3"` (used for tracking versions).

   The function `starteSQLSkript` accepts the following parameters:
   - `$1` (positional, mapped to local `p_Eintragsnr`): Error log record tracking identifier. Used when reporting status to the monitoring system. Passed in Python as a function parameter.
   - `$2` (positional, mapped to local `p_Skript`): File path of the SQL*Plus script to execute. Passed in Python as a function parameter.
   - `$*` (remaining arguments after shifting): Arguments to pass downstream to the SQL*Plus script. Captured in Python as `*args` or a list of arguments.
   - `DW_ORAUSER` (environment variable): Database credentials (username/password@service) used to authenticate the `sqlplus` process. Surfaced in Python via `os.environ.get("DW_ORAUSER")`.

   - Typo Alert:
     # REVIEW: The script defines `ModulName="alis_sqlplus"` but uses `${Modul_Name}` (with an underscore) in the validation check failure message. In KSH, this resolves to an empty string. The Python implementation should resolve this mismatch by using the correct variable name.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Invokes the Oracle SQL*Plus command line tool to execute the script specified by `$p_Skript`, passing dynamic arguments and redirecting standard input to `/dev/null` to prevent the CLI from hanging in interactive prompt mode.
     - subprocess vs native: Must remain an external call via `subprocess.run` because the target SQL scripts are variable and arbitrary.
     - Resolvable: No. The wrapped SQL file content is not supplied and changes per invocation.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked — internal SQL behavior not available in this extraction; confirm database client configuration before deployment

   - `DWMSG_MeldeFehler <args>`
     - Purpose: An external error reporting command/utility utilized to capture execution failures and register them in an operations console.
     - subprocess vs native: Must remain a subprocess call (or map to a standardized python logging framework).
     - # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   There is no embedded SQL inside this utility. The SQL logic resides entirely within the external SQL script files launched dynamically.
   - Dialect: Oracle SQL*Plus.

6. CONTROL FLOW
   1. Initialize global-level tracking variables (`ModulName`, `ModulVersion`).
   2. Function entry: `starteSQLSkript` with arguments.
   3. Assign local variables `p_Eintragsnr` and `p_Skript`.
   4. Execute `shift 2` to separate standard function parameters from dynamic script parameters.
   5. Validate presence of `$p_Eintragsnr` and `$p_Skript`. If empty:
      - Call `DWMSG_MeldeFehler` with code `196` and message `"${Modul_Name} ${Modul_Version} starteSQLSkript"`.
      - Return status `196` to caller.
   6. Validate readability of script file `$p_Skript`. If not readable:
      - Call `DWMSG_MeldeFehler` with code `201` and file path.
      - Return status `201` to caller.
   7. Log execution headers showing the SQL script name and the dynamic parameters.
   8. Execute `set +e` to prevent shell termination.
   9. Invoke `sqlplus` CLI.
   10. Capture exit code of `sqlplus` into local variable `errcode`.
   11. Execute `set -e` to restore strict shell checking.
   12. Return `errcode`.

7. ERROR HANDLING & EXIT CODES
   - The script validates inputs and file readability, reverting to custom error calls (`DWMSG_MeldeFehler`) and returning explicit codes `196` and `201`.
   - To prevent crashing the parent process during database script execution, it disables shell strict checking (`set +e`) around `sqlplus`, captures its return status, re-enables strict mode (`set -e`), and propagates that return status.
   - Target Python approach:
     - Map validations to explicit condition checks.
     - Log failure via a Python equivalent logging or helper execution of `DWMSG_MeldeFehler`.
     - Use `subprocess.run(..., check=False)` to capture the return code of `sqlplus` safely without raising uncaught exceptions, and return the integer status.

8. OUTPUTS / SIDE EFFECTS
   - Writes diagnostic execution details to standard output.
   - Invokes database mutations externally depending on the commands executed by the dynamic SQL script files.

9. BUSINESS SUMMARY
   - Provides an abstract interface for executing SQL scripts to guarantee consistency across execution runs.
   - Validates that mandatory parameter contexts are initialized before triggering database executions.
   - Prevents execution crashes by safely handling missing SQL scripts and returning descriptive error codes.
   - Standardizes integration with legacy operations messaging frameworks.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
def dwmsg_melde_fehler(eintrags_nr, msg_type, error_code, details):
    """
    Simulates external logging command: DWMSG_MeldeFehler
    """
    cmd = ["DWMSG_MeldeFehler", str(eintrags_nr), msg_type, str(error_code), details]
    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)

def starte_sql_skript(p_eintragsnr, p_skript, *args):
    """
    Executes a SQL*Plus script with parameters and validations.
    """
    # Step 1: Validate required parameters are present
    if not p_eintragsnr or not p_skript:
        # REVIEW: Corrected typo from legacy shell reference ${Modul_Name} to use MODUL_NAME
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, error_msg)
        return 196

    # Step 2: Validate that target SQL script is readable
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Print execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 4: Resolve environment context
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 5: Execute SQL*Plus process
    # equivalent to: sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    # REVIEW-STRUCT: launcher [sqlplus] invoked — internal SQL behavior not available in this extraction; confirm database client configuration before deployment
    try:
        # Input redirected from devnull equivalent to </dev/null
        result = subprocess.run(
            sqlplus_cmd, 
            stdin=subprocess.DEVNULL,
            check=False # Equivalent to set +e / set -e handling surrounding the execution
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Failed to execute sqlplus: {e}", file=sys.stderr)
        errcode = 1  # Fallback error status if subprocess launch itself fails

    # Step 6: Return result code
    return errcode
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Migrates KornShell SQL*Plus script wrapper utility function (`starteSQLSkript`) into a reusable Python helper function using `subprocess`. |

# Execution order
The legacy execution order consists of 6 steps:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 Job Definition)
2. `d_abtn_x_smart_kubi.sql` (PL/SQL Transformation Script)
3. `r_sqlscript` (Shell Runner Script)
4. `.dw_init` (Shell Environment Initialization)
5. `f_alis_msgerr.ksh` (Shell Messaging and Error Utility)
6. `h_alis_sqlplus.ksh` (SQL*Plus Helper Utility - current scope)

This execution sequence must be preserved in the target orchestration (Google Cloud Composer / Airflow DAG). The helper utility `h_alis_sqlplus.py` will be imported and called as part of the execution tasks inside the Python operators in the Composer DAG workflow.

# Schedule & variables
The legacy scheduler sets 9 environment variables for this job, which must be dynamically resolved and retained in the migrated BigQuery/Composer environment:
- `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
- `cdate` = `SYS_DATE("YYYYMMDD")` (Resolved dynamically in target Airflow DAG using Jinja template formatting, e.g., `{{ ds_nodash }}`)
- `cmonth` = `SUBSTR(&cdate,1,6)` (Resolved dynamically in target Airflow DAG, e.g., `{{ ds_nodash[:6] }}`)
- `cday` = `SUBSTR(&cdate,7,2)` (Resolved dynamically in target Airflow DAG, e.g., `{{ ds_nodash[6:8] }}`)
- `first` = `'01'`
- `cmonth` = `&cmonth&first`
- `cmonth` = `SUB_DAYS(&cmonth,1)` (Calculates previous day via standard Python `timedelta` or Airflow template macros)
- `cmonth` = `SUBSTR(&cmonth,1,6)` (Extracts YYYYMM of the prior month)
- `MONATSID` = `&cmonth` (The final resolved prior-month identifier used as an execution partition parameter, e.g., `YYYYMM` of the previous month)

These variables must reach the migrated job as Airflow DAG execution parameters or task-level runtime environment variables.

# Lineage
- **Upstream / Downstream Lineage**: No lineage edges were found for `h_alis_sqlplus.ksh` in the scanned codebase context. It is a shared library/utility module sourced by other wrapper scripts to launch SQL executions rather than a direct database-mutating job.

# External system replacements
- **Database Client Execution**: The legacy shell command `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null` executes SQL scripts using Oracle SQL*Plus CLI. In the BigQuery target, the SQL scripts (once migrated) should be executed natively via the Google Cloud BigQuery Python client (`google-cloud-bigquery`) or BigQuery Airflow operators (such as `BigQueryExecuteQueryOperator`). The validation checks and logging features of the function must map to the BigQuery client execution sequence.
- **Legacy Error Log Reporting**: `DWMSG_MeldeFehler` is a custom legacy command-line tool for operations console alerting. It must be replaced by a standardized Python logging mechanism integrated with Google Cloud Logging (Stackdriver) or Composer alerting operators (e.g., Airflow Email / Slack Operators).

# Cross-file dependencies
- This utility script `h_alis_sqlplus.ksh` is referenced and sourced by other shell wrapper scripts (such as `r_sqlscript`) within the same legacy job. Migrating it to Python means upstream shell wrapper callers must be migrated to Python and import `starte_sql_skript` from `local.home.gurunathan_t.kubi.h_alis_sqlplus` or invoke it as a python module.

# Target file plan
- **Target File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
  - **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
  - **Language**: Python 3
  - **Purpose**: Defines `starte_sql_skript` function incorporating file readability checks, logging of original German execution headers, executing SQL scripts (translated to BigQuery-compatible Python client calls or BQ executions), and return code propagation.

# Environment-specific values
Classifying environment variables by role:

1. **GLOBAL (Environment-Wide)**:
   - `DW_ORAUSER`: Identifies legacy database connection and credentials. In the BigQuery environment, this must be replaced with global GCP variables: `GCP_PROJECT`, `BQ_LOCATION`, and `GCS_BUCKET` (used for schema/script staging if needed). These must be sourced at runtime via Python's standard `os.environ.get("GCP_PROJECT")` or Airflow's native variable storage `Variable.get("GCP_PROJECT")`.

2. **JOB-SPECIFIC**:
   - `ModulName` / `ModulVersion`: Constant values particular to this script ("alis_sqlplus" / "V1.1.3").
   - `p_Eintragsnr`: Logging tracker index passed to the function per execution.
   - `p_Skript`: Path to the SQL file to execute.
   - `MONATSID` (and related scheduler date variables): Generated dynamically per execution run. Passed via DAG `params`.

# Risks and manual steps
- **Oracle SQL\*Plus to BigQuery Shift**: The legacy utility script is custom-designed to invoke Oracle `sqlplus` binaries. Since the target database platform is BigQuery, the SQL scripts that this utility executes (such as `d_abtn_x_smart_kubi.sql`) are being migrated to BigQuery SQL/Dataform. Invoking them via command-line wrapper emulation is anti-pattern. A manual review is required to update callers of `starte_sql_skript` so that they execute queries directly through BigQuery Client API/Operators instead of executing command-line wrapper shells.
- **Typo in Source Script**: The source KornShell script defines `ModulName="alis_sqlplus"` but attempts to reference `${Modul_Name}` (with an underscore) in `DWMSG_MeldeFehler`. This evaluates to an empty string in legacy environments, resulting in incomplete error logs. In the target Python code, this has been corrected to use `MODUL_NAME`.
- **Legacy Error Tracking Dependency**: The function calls an external utility `DWMSG_MeldeFehler` for logging errors. The implementation details of this command are not provided in this scope. This must be replaced with Cloud Composer native logging / Stackdriver alerting during build-out.

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
REASON: The script is an orchestration launcher that parses command-line arguments, resolves SQL file paths, and manages logging and exit codes.

EVIDENCE
- Business logic found: KSH custom logic parses arguments, checks and resolves file paths on the file system across multiple relative paths, and handles system logging and execution traps.
- AWK: none
- SQL-expressible: no, contains file system operations, relative directory searches, trap handlers, and external process coordination.
- Non-SQL side effects: logs to file system, handles system signal traps, resolves relative directories, and launches external processes.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `r_sqlscript` KornShell script is a generic database utility launcher that executes Oracle SQL*Plus scripts. It handles command-line arguments to resolve the path of a target SQL script across several search directories, registers execution metadata with a central tracking system, handles error trapping, and redirects execution stdout/stderr to a tracked log file.

2. INVOCATION CONTEXT
   - Who calls this script: UC4 jobs (specific JOBS_UNIX object names are unknown as this is a reusable runner utility).
   - Command line arguments: `$0 -f <sql_script> [-i <sql_params>] [-j <job_name>] [-v] [-h]`
   - UC4 includes: None referenced in the script itself.
   - Environment files sourced:
     - `. $HOME/aktuell/.dw_init`
       # REVIEW-STRUCT: environment file $HOME/aktuell/.dw_init not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
       # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables and functions it defines (including DWMSG_MeldeFehler, DWMSG_ErmittleNr, DWMSG_Logdateiname, DWMSG_ErzeugeEintrag, DWMSG_Fehlerbehandlung, DWMSG_SetzeStatusOK) are unknown
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
       # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — functions it defines (including starteSQLSkript) are unknown

3. PARAMETERS / INPUTS
   - `-f` (`p_sqlscript`): The SQL script filename. Passed via command-line arguments, required. Conversions must surface this via `argparse`.
   - `-i` (`p_sqlpar`): String arguments/parameters passed on to the target SQL script. Surface via `argparse`.
   - `-j` (`p_Job`): Job identifier (defaults to `DWH_KORR`). Surface via `argparse`.
   - `-v` (`p_Verbose`): Verbose flag. If set to 1, prints out the log file immediately upon failure. Surface via `argparse`.
   - `-h`: Help / usage flag. Displays usage information. Surface via `argparse`.
   - `DW_DIR_ROOT`: Sourced/environment variable used to resolve paths. Surface via `os.environ`.
   - `HOME`: User home directory used to locate initialization script. Surface via `os.environ`.
   - `p_Kuerzel`: Referenced but never declared or initialized in the script body.
     # REVIEW: parameter p_Kuerzel is referenced in ErrArg assignment but is never declared or initialized in the source script; confirm if this is an obsolete or missing variable.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `dirname`: Used to resolve the script's directory and SQL script path. Should be replaced with native Python `os.path.dirname` or `pathlib.Path`.
   - `starteSQLSkript`: External SQL utility launcher sourced from `h_alis_sqlplus.ksh`.
     - Exact command line: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
     - Purpose: Executes the target SQL script using SQL*Plus with appropriate error-handling and parameter bindings.
     - Target implementation: Must remain an external process call via `subprocess.run` as its implementation is unsupplied and it represents a complex utility runner. It does not qualify as a RESOLVABLE LAUNCHER because the SQL script content to execute is dynamically passed and its database connection parameters are not declared in this script.
       # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - No direct embedded SQL statements are present in this wrapper script. It executes dynamic SQL scripts specified by the `-f` flag.

6. CONTROL FLOW
   1. Sourcing environment variables: `. $HOME/aktuell/.dw_init`
   2. Sourcing messaging and logging utilities: `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
   3. Sourcing SQL execution utility: `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
   4. Set script options and initialize argument variables (`ErrNr=0`, `ErrArg=""`, `DW_EintragsNr=0`, `p_Verbose=0`, `p_sqlscript=""`, `p_sqlpar=""`, `p_Job=""`).
   5. Parse options using `getopts`. If `-h` is passed, show usage and exit.
   6. Check for parsing errors (`ErrNr != 0`). If found, run `DWMSG_MeldeFehler`, print usage, and exit.
   7. Change working directory to the directory where this launcher resides (`cd \`dirname $0\``).
   8. Resolve the relative or absolute path of the target SQL script (`l_DBskript`):
      - If the directory of `p_sqlscript` is `.`, search for the file sequentially in:
        1. `../sql/${p_sqlscript}`
        2. `../mig/${p_sqlscript}`
        3. `${p_sqlscript}`
      - If directory is not `.`, use `p_sqlscript` directly.
   9. File existence check and error flag:
      - Checks `if [ -f "$l_DBskript" ]`.
        # REVIEW: The original script contains a conditional block 'if [ -f "$l_DBskript" ]' that sets 'ErrNr=198' and 'ErrArg="$p_Kuerzel"' if the file EXISTS. This is highly likely a logic bug where it should have checked '! -f' (non-existence). Also, 'p_Kuerzel' is used but undefined. No exit command follows this check, meaning the script continues anyway.
   10. Resolve `JobKennung` (use `-j` value or default to `DWH_KORR`), and convert to uppercase.
   11. Call tracker system initialization functions:
       - `DWMSG_ErmittleNr DW_EintragsNr`
       - `DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr`
       - `DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0_$l_DBskript $LogDatei >> $LogDatei 2>&1`
   12. Define `trap` statements to handle INT and ERR signals.
       - Trap calls `DWMSG_Fehlerbehandlung` and prints error indicators (`!OSFEHLER gemeldet!`/`!FEHLER gemeldet!`).
       - If `p_Verbose != 0`, cat the `$LogDatei` contents to stdout.
   13. Call `starteSQLSkript` with parameter arguments, routing all stdout and stderr to `$LogDatei`.
   14. On successful completion:
       - Call `DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1`
       - Unset traps (`trap INT ERR`).
       - Print success completion text.

7. ERROR HANDLING & EXIT CODES
   - Argument errors: exits with `ErrNr=192` (unknown param) or `193` (missing parameter value) after calling `DWMSG_MeldeFehler`.
   - Signal/execution errors: Trapped via `trap ... INT` and `trap ... ERR`. Trapping executes `DWMSG_Fehlerbehandlung` and exits with `1` on OS failures.
   - Successful execution returns `0`.
   - Python translation should map shell traps to `try-except-finally` blocks and raise `subprocess.CalledProcessError` on subprocess failures.

8. OUTPUTS / SIDE EFFECTS
   - Writes logging data to dynamically named log files (`$LogDatei`).
   - Registers and updates execution and error status with external systems via sourced `DWMSG_*` utilities.

9. BUSINESS SUMMARY
   - Standardizes the execution environment for Oracle SQL*Plus scripts across migration and development directories.
   - Centralizes logging, status registration, and error tracking under a unified job identification numbering system.
   - Ensures consistent trapping of database and shell execution failures.

---------------------------------------------------------------------------------------
MANDATORY AUDIT CHECKLIST FOR PARAMETER VALIDATION GUARDS
---------------------------------------------------------------------------------------
- Audit performed on all KSH function definitions in source code.
- KSH functions identified: `usage()`
- Parameter validation guards found inside identified functions: None. (The `usage` function contains no validation checks, only a static heredoc menu block).
- Conclusion: No parameter guards require matching Python translations or refactoring reviews.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Sourcing environment setup and logging utilities
# # REVIEW-STRUCT: environment file $HOME/aktuell/.dw_init not supplied — variables it sets are unknown
# # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables and functions it defines (DWMSG_*) are unknown
# # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — defines starteSQLSkript

import os
import sys
import argparse
import subprocess

# Step 2: Initialize variables
prog_name = f"Ausführung Script {sys.argv[0]}"
prog_version = "5.0.0"
err_nr = 0
err_arg = ""
dw_eintrags_nr = 0
p_verbose = 0
p_sqlscript = ""
p_sqlpar = ""
p_job = ""

# Define usage helper
def usage():
    print(f"""
   Programm: {prog_name}
   Version: {prog_version}
   Aufruf: {sys.argv[0]} Parameter

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

# Step 3: Parse parameters using argparse (mapping KSH getopts)
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-f', dest='f_val')
parser.add_argument('-i', dest='i_val')
parser.add_argument('-j', dest='j_val')
parser.add_argument('-v', action='store_true', dest='v_val')
parser.add_argument('-h', action='store_true', dest='h_val')

try:
    args, unknown = parser.parse_known_args()
    
    if args.h_val:
        usage()
        sys.exit(0)
        
    if unknown:
        err_nr = 192  # Parameter unknown
        err_arg = str(unknown)
        
    p_sqlscript = args.f_val.lower() if args.f_val else ""  # typeset -l p_sqlscript
    p_sqlpar = args.i_val if args.i_val else ""
    p_verbose = 1 if args.v_val else 0
    p_job = args.j_val if args.j_val else ""
    
    # Simulate getopts check where -f is conceptually mandatory
    if not p_sqlscript and err_nr == 0:
        err_nr = 193  # Required argument missing
        err_arg = "-f"
except Exception as e:
    err_nr = 192
    err_arg = str(e)

# Step 4: Validate parameters and abort if error occurred
if err_nr != 0:
    # DWMSG_MeldeFehler(dw_eintrags_nr, "E", err_nr, err_arg)
    print(f"Error {err_nr}: {err_arg}", file=sys.stderr)
    usage()
    sys.exit(err_nr)

# Step 5: Change directory to script directory
script_dir = os.path.dirname(sys.argv[0])
if script_dir:
    os.chdir(script_dir)

# Step 6: Resolve SQL script file path (l_DBskript)
p_sqlscript_dir = os.path.dirname(p_sqlscript)
if p_sqlscript_dir == '.' or not p_sqlscript_dir:
    l_DBskript = os.path.join('..', 'sql', p_sqlscript)
    if not os.path.isfile(l_DBskript):
        l_DBskript = os.path.join('..', 'mig', p_sqlscript)
    if not os.path.isfile(l_DBskript):
        l_DBskript = p_sqlscript
else:
    l_DBskript = p_sqlscript

# Step 7: Check script path existence (replicating KSH conditional bug exactly)
# # REVIEW: The original check 'if [ -f "$l_DBskript" ]' sets an error if the file DOES exist, which seems backwards.
# # Also, 'p_Kuerzel' is referenced but never initialized.
if os.path.isfile(l_DBskript):
    err_nr = 198  # Parameterwert unbekannt
    err_arg = os.environ.get("p_Kuerzel", "")  # p_Kuerzel is undefined in original script

# Step 8: Set uppercase JobKennung
job_kennung = p_job.upper() if p_job else "DWH_KORR"

print("----------------- Parameter -----------------")
print(f"Jobkennung     : {job_kennung}")
print(f"DB-Skript      : {l_DBskript}")
print("---------------------------------------------")

# Step 9: Initialize logging and job tracking variables
# DWMSG_ErmittleNr(dw_eintrags_nr)
# DWMSG_Logdateiname(log_datei, job_kennung, dw_eintrags_nr)
# DWMSG_ErzeugeEintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)
dw_eintrags_nr = 99999  # Mocked placeholder for system sequence
log_datei = f"log_{job_kennung}_{dw_eintrags_nr}.log"

# Step 10: Setup trap functions simulated via try-except block
def handle_error_trap(error_type):
    # DWMSG_Fehlerbehandlung(dw_eintrags_nr) >> log_datei
    print(f"!{error_type} gemeldet!")
    if p_verbose != 0:
        if os.path.isfile(log_datei):
            with open(log_datei, 'r') as f:
                print(f.read())
    sys.exit(1)

# Step 11: Execute the actual SQL runner job
print("----------------- Job -----------------------")
print(f"Job-Nr    : '{dw_eintrags_nr}'")
print(f"Logdatei  : '{log_datei}'")
print("---------------------------------------------")

try:
    # # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction
    # Executing external process for sourced utility starteSQLSkript
    with open(log_datei, "a") as log_f:
        cmd = ["starteSQLSkript", str(dw_eintrags_nr), l_DBskript, p_sqlpar, str(dw_eintrags_nr)]
        subprocess.run(cmd, stdout=log_f, stderr=subprocess.STDOUT, check=True)
except subprocess.CalledProcessError as e:
    handle_error_trap("FEHLER")
except KeyboardInterrupt:
    handle_error_trap("OSFEHLER")

# Step 12: Post-processing success tracking and cleanup
# DWMSG_SetzeStatusOK(dw_eintrags_nr) >> log_datei
print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Migrated KornShell script to Python using `ksh_design_python`. Converts command-line parsing, path resolution, and error trapping to native Python logic while wrapping BigQuery/Dataform API execution. |

# Execution Order

The legacy execution order consists of 6 steps. Since only step 3 (`r_sqlscript`) is in the scope of this migration design pass, the remaining steps are context-only and must be coordinated with their respective sibling design passes:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (UC4 XML orchestration) $\rightarrow$ Replaced by a Cloud Composer DAG (migrated in a sibling pass).
2. **d_abtn_x_smart_kubi.sql** (PL/SQL core transform logic) $\rightarrow$ Replaced by Dataform SQLX / BigQuery procedural execution (migrated in a sibling pass).
3. **r_sqlscript** (Shell script wrapper) $\rightarrow$ Replaced by `local/home/gurunathan_t/kubi/r_sqlscript.py` (this pass), which wraps the BigQuery or Dataform execution.
4. **.dw_init** (Shell config) $\rightarrow$ Replaced by GCP-native environment variables and connection configurations.
5. **f_alis_msgerr.ksh** (Error logging helper) $\rightarrow$ Replaced by a shared GCP logging module or native Airflow alerting.
6. **h_alis_sqlplus.ksh** (SQL*Plus helper) $\rightarrow$ Replaced by Google Cloud BigQuery client library calls.

# Schedule & Variables

The migrated runner must support the dynamic calculation of variables fed by the scheduler. 

### Scheduler-Set Variables to Retain:
- `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'`
- `cdate` = Run date formatted as `'YYYYMMDD'`
- `cmonth` / `cday` / `first` / `MONATSID` calculations:
  - In the legacy scheduler, these values were computed using date manipulation (slicing, appending, subtracting days). 
  - In the Cloud Composer (Airflow) target environment, these should be calculated dynamically via Jinja templates or within a Python operator.

### Target Python/Airflow Equivalent Variable Logic:
```python
# Schedulers on Google Cloud (Cloud Composer / Airflow) should resolve these at runtime:
from datetime import datetime, timedelta

# Assuming dynamic execution date from Airflow context (e.g., {{ ds_nodash }})
cdate = os.environ.get("cdate", datetime.now().strftime("%Y%m%d"))

# Replicating legacy calculation:
# Subtract 1 month (legacy does this by setting day to '01', subtracting 1 day, and taking YYYYMM)
run_date = datetime.strptime(cdate, "%Y%m%d")
first_of_current_month = run_date.replace(day=1)
last_day_of_prev_month = first_of_current_month - timedelta(days=1)

# Variables to feed to the downstream BigQuery jobs:
DWH_JOB_KENNUNG = "ABTN_SMART_KUBI"
MONATSID = last_day_of_prev_month.strftime("%Y%m")
```

# Lineage

The script `r_sqlscript` interacts with the following legacy components:
- **Upstream / Sourced Configs**:
  - `FILE:.dw_init` (Config initialization file)
- **Invoked Utilities**:
  - `FILE:f_alis_msgerr.ksh` (Logging and error handling library)
  - `FILE:h_alis_sqlplus.ksh` (Utility library to interface with Oracle SQL*Plus)

# External System Replacements

- **Oracle SQL\*Plus Replacement**: Sourced functions from `h_alis_sqlplus.ksh` (specifically `starteSQLSkript`) and their Oracle-specific SQL*Plus invocations must be replaced by the **Google Cloud BigQuery Client Library** (`google.cloud.bigquery`) or **Dataform API execution**, depending on the target implementation of the SQL code (`d_abtn_x_smart_kubi.sql`).

# Cross-File Dependencies

- **Shared Configs**: The environmental initialization script `.dw_init` is a shared asset. Its values are replaced by native Airflow/Composer config variables.
- **Shared Helpers**: `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh` represent shared framework utilities. Rather than rewriting them literally, their functionality (error reporting, database script execution, log forwarding) should be encapsulated in a shared Python utility module (e.g. `dwh_gcp_utils`) used across all migrated wrapper scripts.

# Target File Plan

### File: `local/home/gurunathan_t/kubi/r_sqlscript.py`
- **Source**: `local/home/gurunathan_t/kubi/r_sqlscript`
- **Language**: Python
- **Purpose**: A command-line script utilizing `argparse` to replicate the flags (`-f`, `-i`, `-j`, `-v`) of the legacy script. It resolves the relative path of the SQL scripts (searching through standard legacy folders `../sql`, `../mig`, and `.`), sets up Python's `logging` framework, and executes the SQL scripts against BigQuery.

# Environment-Specific Values

To align with the environment variable policy, legacy configurations must map to standard target variables:

### Global (Environment-Wide Variables):
- `HOME` $\rightarrow$ Standard Airflow home environment.
- `DW_DIR_ROOT` $\rightarrow$ Replaced by the global environment-wide configuration parameter `GCS_BUCKET` (to locate scripts stored in Cloud Storage) or `GCP_PROJECT`.
  - Python resolution: `GCP_PROJECT = os.environ.get("GCP_PROJECT")`
  - Airflow resolution: `Variable.get("GCP_PROJECT")`

### Job-Specific Variables:
- `DWH_JOB_KENNUNG` (passed as `-j`) $\rightarrow$ Mapped to the specific Airflow DAG run/task ID (`ABTN_SMART_KUBI`).
- `MONATSID` $\rightarrow$ Calculated dynamically within the task run and passed as a BigQuery query parameter.
- `p_sqlscript` (passed as `-f`) $\rightarrow$ Resolved to the path of the target Dataform/BigQuery SQL file.

# Risks and Manual Steps

- **Scope Exclusion (Orchestration & SQL Logic)**: The orchestrator UC4 job (`DW.DWH_ABTN_SMART_KUBI.xml`) and the actual PL/SQL script (`d_abtn_x_smart_kubi.sql`) are not part of this design pass. The runner script `r_sqlscript.py` cannot be fully integrated or verified until these sibling components are migrated.
- **Legacy Logic Bug - File Existence Check**: The legacy KSH script contains an apparent logic bug:
  ```bash
  if [ -f "$l_DBskript" ]
  then
      ErrNr=198
      ErrArg="$p_Kuerzel"
  fi
  ```
  This marks the run as a failure (`ErrNr=198`) if the file *does* exist, which is backwards. Additionally, the variable `p_Kuerzel` is referenced but never initialized.
  - *Manual Step*: Confirm with business stakeholders if this guard was intended to verify that the file does *not* exist (`if [ ! -f "$l_DBskript" ]`), and implement the correct guard in Python.
- **Unsupplied Utility Dependencies**: The functions `starteSQLSkript` (from `h_alis_sqlplus.ksh`) and `DWMSG_*` (from `f_alis_msgerr.ksh`) are imported but their internal code is not part of this file's code payload. Their native Python replacements must be verified against the global framework design to ensure correct logging registration and database execution.