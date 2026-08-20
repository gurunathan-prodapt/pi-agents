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


# Migration Design Document: UC4 to Apache Airflow

---

## 1. Overview
This workflow represents a single native Unix job (`DW.DWH_ABTN_SMART_KUBI`) migrating to Apache Airflow. Its primary function is to execute an SQL script (`d_abtn_x_smart_kubi.sql`) that populates a temporary database table. The job dynamically computes a reporting month identifier (`MONATSID`) based on the execution date: if the execution day is before the 15th, it shifts the context back to the prior month. Because this job was extracted standalone without an enclosing UC4 Job Plan (JOBP) or Script (SCRI) trigger, it is classified as an externally triggered or on-demand pipeline.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 (Active) | Populate temp table |

---

## 3. Scheduling
* **Schedule:** `None`
* **Trigger Analysis:** No `EVNT_TIME` or `JSCH` schedule objects are present in this extraction. Furthermore, there are no parent `JOBP` workflows or activating `SCRI` scripts included in the bundle. Consequently, this pipeline is configured with `schedule=None` (manual or external trigger only).

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_abtn_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | N/A | None | #REVIEW-STRUCT: Launcher wraps SQL script [`d_abtn_x_smart_kubi.sql`], which must be converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL. Confirm the actual artifact produced before wiring a real operator (e.g., `BashOperator`/`PythonOperator` for Python, `BigQueryInsertJobOperator` for BigQuery SQL); never assume Python. Script contains dynamic date logic to calculate `MONATSID`. |

---

## 6. Task Dependency Map
As this migration contains only a single standalone job, the dependency map is trivial:

```python
dwh_abtn_smart_kubi
```

---

## 7. Sync / Concurrency Analysis
* No sync keys or resource locks are declared in this extraction. 
* Concurrency is managed at the DAG level using `max_active_runs=1` to prevent parallel overlapping executions.

---

## 8. Error Handling and Retry Strategy
* **Retries:** Inherits the default argument of `1` retry with a `5`-minute delay.
* **Failure Actions:** No post-conditions or failure actions are specified in the extraction.
* **Date Pre-conditions:** There are no `earliest_start_time` or complex calendar conditions. However, the execution date calculation must be carried over.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Calculation Logic |
| :--- | :--- | :--- |
| `&MONATSID` | Calculated dynamically from execution run date | Calculated in a Python helper using the execution date (`logical_date`):<br>`if logical_date.day < 15: calculate previous month`<br>`else: calculate current month (format: YYYYMM)` |
| `&DWH_JOB_KENNUNG` | `'ABTN_SMART_KUBI'` | Passed as metadata or environment variable if required by target script. |

---

## 10. Developer Notes
* **#REVIEW-STRUCT: SQL Script Migration Pipeline Integration:** The source job wraps SQL script execution (`$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`). This SQL must be handled by the external SQL migration team. Do not attempt to run it directly; instead, swap the `EmptyOperator` placeholder with the appropriate operator once the target target architecture (e.g., BigQuery, Postgres, Snowflake) and artifact type (SQL vs Python wrapper) are finalized.
* **Month Calculation Logic replication:** Ensure the target execution layer uses the calculated `MONATSID` parameter. The logic in Python should look like this:
  ```python
  def get_monatsid(logical_date):
      # If day of execution < 15, use previous month
      if logical_date.day < 15:
          first_of_month = logical_date.replace(day=1)
          previous_month = first_of_month - timedelta(days=1)
          return previous_month.strftime("%Y%m")
      return logical_date.strftime("%Y%m")
  ```

---

# Numbered Pseudocode Outline

```python
# 1. Imports
# -------------------------------------------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: Once the SQL migration strategy is finalized, import the appropriate operator:
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# or
# from airflow.operators.bash import BashOperator

# 2. GCP Configuration
# -------------------------------------------------------------
# # REVIEW-STRUCT: Placeholders to be filled once migration target is confirmed
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "us-central1"

# 3. Default Args
# -------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 4. on_failure_callback stubs
# -------------------------------------------------------------
# No standard UC4 global failure hooks were defined in the extraction.

# 5. DAG Definition
# -------------------------------------------------------------
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=DEFAULT_ARGS,
    description="Populate temp table - Migrated from DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migrated_uc4", "jobs_unix"],
) as dag:

    # 6. Guard Task (None required)
    # 7. Sensor Task (None required)
    # 8. Calendar Check Task (None required)

    # 9. Task: dwh_abtn_smart_kubi
    # ---------------------------------------------------------
    # # REVIEW-STRUCT: The UC4 script executes the SQL file:
    # # "$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql" 
    # # with parameter -i &MONATSID (calculated date variable).
    # # 
    # # Calculate &MONATSID in Python equivalent:
    # # monatsid = "{{ (dag_run.logical_date.replace(day=1) - macros.dateutil.relativedelta.relativedelta(days=1)).strftime('%Y%m') if dag_run.logical_date.day < 15 else dag_run.logical_date.strftime('%Y%m') }}"
    # #
    # # Currently implemented as EmptyOperator placeholder until companion pipeline 
    # # determines target execution type (e.g., BigQuery SQL vs wrapper Python).
    
    dwh_abtn_smart_kubi = EmptyOperator(
        task_id="dwh_abtn_smart_kubi",
        # Keep track of UC4 metadata in doc_md
        doc_md="""
        ### UC4 Source Metadata
        * **Source Name:** DW.DWH_ABTN_SMART_KUBI
        * **Original Host:** |DWHDWH1P|HOST
        * **Original Login:** DW.UNIX.ISTNS
        * **Original Script Target:** `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`
        """,
    )

    # 10. Dependencies
    # ---------------------------------------------------------
    # Single standalone task pipeline. No internal dependencies defined.
    dwh_abtn_smart_kubi
```

### Execution Order

The target orchestration must preserve the legacy execution order sequence. The following mapping details how each step from the legacy dependency graph is preserved or mapped in the target platform:

1. **`DW.DWH_ABTN_SMART_KUBI.xml`** $\rightarrow$ Handled by the target Apache Airflow DAG (`dags/kubi/dw_dwh_abtn_smart_kubi.py`) which acts as the main orchestrator.
2. **`d_abtn_x_smart_kubi.sql`** $\rightarrow$ Executed as a task in the target DAG via a BigQuery job operator (e.g., `BigQueryInsertJobOperator` or via Dataform compilation). This SQL logic is migrated separately in its own design pass.
3. **`r_sqlscript`** $\rightarrow$ This KornShell utility wrapper is retired. Its responsibilities (establishing database connections, setting session contexts, logging, and error tracing) are handled natively by Cloud Composer/Airflow operators and BigQuery native execution logging.
4. **`.dw_init`** $\rightarrow$ Retired. Environment initialization and variable definition are handled natively by Airflow environment variables, DAG `params`, or Airflow Variable lookups.
5. **`f_alis_msgerr.ksh`** $\rightarrow$ Retired. Error messaging and alert mechanisms are mapped to Airflow's native `on_failure_callback` notifications (e.g., email or Slack operators).
6. **`h_alis_sqlplus.ksh`** $\rightarrow$ Retired. Oracle SQL*Plus execution is replaced entirely by the BigQuery client libraries or native Airflow operators.

---

### Schedule & Variables — Must Be Retained

The timing rules and variable flows from the legacy scheduler must be strictly preserved.

#### Scheduler-Set Variables Mapping
* **`DWH_JOB_KENNUNG`** $= \text{'ABTN_SMART_KUBI'}$
  * **Airflow Target Implementation:** Passed as an Airflow DAG-level parameter (`params`) or injected as an environment variable in the execution context of the BigQuery/Python tasks.
* **Date Calculations (`cdate`, `cmonth`, `cday`, `first`, `MONATSID`)**
  * **Legacy Logic:**
    ```shell
    cdate = SYS_DATE("YYYYMMDD")
    cmonth = SUBSTR(cdate, 1, 6)
    cday = SUBSTR(cdate, 7, 2)
    if cday < '15':
        first = '01'
        cmonth = cmonth + first
        cmonth = SUB_DAYS(cmonth, 1)
        cmonth = SUBSTR(cmonth, 1, 6)
    MONATSID = cmonth
    ```
  * **Airflow Target Implementation:** Since SQL execution relies on `MONATSID`, this dynamic date calculation must be implemented within an Airflow Jinja template macro using the DAG's `logical_date` (execution date) to ensure idempotency. 
  * **Calculation Logic:**
    ```python
    # Calculated dynamically at run-time in the Airflow DAG
    def calculate_monatsid(logical_date):
        if logical_date.day < 15:
            # Shift back to the prior month
            first_of_current = logical_date.replace(day=1)
            previous_month = first_of_current - timedelta(days=1)
            return previous_month.strftime("%Y%m")
        return logical_date.strftime("%Y%m")
    ```

---

### Lineage

#### Upstream Producers
* **Host (`dwhdwh1p`)** $\rightarrow$ Replaced by the native GCP environment hosting Cloud Composer and BigQuery.
* **Credentials/Login (`DW.UNIX.ISTNS`)** $\rightarrow$ Legacy UNIX login credentials are replaced by GCP IAM Service Accounts assigned to Composer worker nodes.

#### Downstream Consumers
* **`d_abtn_x_smart_kubi.sql` (job: `DW.DWH_ABTN_SMART_KUBI`)** $\rightarrow$ Cross-job hand-off. The SQL script receives the calculated `MONATSID` as a parameter and performs aggregations on target BigQuery datasets.
* **`DW.HOLE_PFAD` and `DW.LESE_LOG`** $\rightarrow$ Identified as auxiliary utilities that do not contain core business transformation logic. They have been human-confirmed as **NO SOURCE NEEDED** and are retired in the target environment.

---

### Cross-File Dependencies

The SQL script `d_abtn_x_smart_kubi.sql` invoked by this job interacts with the following database resources:
* **Tables Read:**
  * `BL_D_TARIF` (Tariff details)
  * `DWH$TA_F_D1_TWVV_TN` (Fact contract table)
  * `DWH$VI_L_MAP_FA_TARIF` (Tariff mapping view)
* **Tables Written:**
  * `DWH$TA_T_SMART_KUBI` (Monats_ID, Kundennummer, Tarif_ID, Tarif_ID_Alt, VO_Kennung, Test_GP, Anzahl, Kennzahl_ID)

These dependencies must be synchronized if the tables are being modified by concurrent workflows. Ensure downstream pipelines consuming `DWH$TA_T_SMART_KUBI` wait for this Airflow DAG to succeed.

---

### Target File Plan

| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/kubi/dw_dwh_abtn_smart_kubi.py` | Python | `DW.DWH_ABTN_SMART_KUBI.xml` | Orchestrates the calculation of `MONATSID` and triggers the downstream BigQuery execution. |

---

### Environment-Specific Values

The environment values extracted from the source configurations are mapped below as global infrastructure variables or job-specific configurations:

| Legacy Source Value | Classification | Canonical Target Name | Resolution Mechanism |
| :--- | :--- | :--- | :--- |
| **GCP Project** | GLOBAL | `GCP_PROJECT` | Sourced via Airflow Variable `Variable.get("GCP_PROJECT")` or default environment configurations. |
| **GCP Region** | GLOBAL | `GCP_REGION` | Sourced via Airflow Variable `Variable.get("GCP_REGION")`. |
| **Target Dataset** | GLOBAL | `BQ_DATASET` | BigQuery destination dataset containing `DWH$TA_T_SMART_KUBI`, resolved via Airflow Variable. |
| **`DWH_JOB_KENNUNG`** | JOB-SPECIFIC | `dwh_job_kennung` | Hardcoded as a job-specific parameter metadata value `'ABTN_SMART_KUBI'` in the DAG definition. |

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `dags/kubi/dw_dwh_abtn_smart_kubi.py` | Migrated UC4 UNIX job xml structure into an Apache Airflow DAG to orchestrate the pipeline run. |

---

### Risks & Manual Actions

1. **UNMIGRATED UPSTREAM SQL DEPENDENCY:** The target Airflow DAG triggers the execution of `d_abtn_x_smart_kubi.sql` (mapped in a separate design group). The execution task in `dags/kubi/dw_dwh_abtn_smart_kubi.py` is currently defined with a placeholder (`EmptyOperator`). A developer must manually replace this placeholder with the appropriate `BigQueryInsertJobOperator` or Dataform compilation trigger once the SQL/Dataform migration pass is completed.
2. **BUSINESS CALENDAR VALIDATION:** The dynamic reporting month calculation (`MONATSID`) is a business-critical function. Verify that the Python timezone settings and run-time parameters replicate the legacy server’s timezone precisely, ensuring that executions close to midnight on the 15th do not trigger a discrepancy in the reporting month identifier.

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
REASON: The script is a pure environment initialization profile that only defines directory structures and configuration variables with no business logic.

EVIDENCE
- Business logic found: None. The script is an environment initialization profile (.dw_init) used to set directory paths, remote host names, and locate ORACLE_HOME.
- AWK: none
- SQL-expressible: No, this is purely system-level environment and path configuration.
- Non-SQL side effects: None.
- Against this verdict: The presence of a conditional 'if-else' block to dynamically find ORACLE_HOME might technically exceed simple variable assignments, but because it only sets environment variables, converting it to Python or SQL is not viable.

ORCHESTRATION SUMMARY
- Purpose: This script initializes the global environment variables, local directory paths, remote customer host, and Oracle environment paths for the Information Services data warehouse runtime.
- Variables declared:
  * DW_DIR_ROOT = $HOME/aktuell
  * DW_DIR_PROT = $HOME/daten/logfiles
  * DW_DIR_CUBES = $HOME/daten/cubes
  * DW_DIR_IMP_D1 = $HOME/daten/d1
  * DW_DIR_IMP_BWA = $HOME/daten/dpps/bwa
  * DW_DIR_IMP_XTRA = $HOME/daten/xtra
  * DW_DIR_IMP_CTEL = $HOME/daten/ctel
  * DW_DIR_IMP_VO = $HOME/daten/vo
  * DW_DIR_IMP_RV = $HOME/daten/rv
  * DW_DIR_IMP_IF = $HOME/daten/ees
  * DW_DIR_IMP_NNV = $HOME/daten/nnv
  * DW_DIR_IMP_SIGMA = $HOME/daten/gd/sigma
  * DW_DIR_EXP_SIGMA = $HOME/daten/gd/sigma/export
  * DW_DIR_IMP_TRF = $HOME/daten/trf
  * DW_DIR_IMP_AUF = $HOME/daten/sd/auf
  * DW_DIR_IMP_GUT = $HOME/daten/sd/gut
  * DW_DIR_IMP_KDG = $HOME/daten/sd/kdg
  * DW_DIR_IMP_MP_KDG = $HOME/daten/mp/kdg
  * DW_DIR_IMP_MP_TS = $HOME/daten/mp/ts
  * DW_DIR_IMP_MP_ZM = $HOME/daten/mp/zm
  * DW_DIR_IMP_TS = $HOME/daten/sd/ts
  * DW_DIR_IMP_ZM = $HOME/daten/sd/zm
  * DW_DIR_EXP = $HOME/daten/exporter
  * DW_DIR_IMP_BPM = $HOME/daten/bm
  * DW_DIR_IMP_ZTS = $HOME/daten/zts
  * DW_DIR_IMP_VRS = $HOME/daten/vrs
  * DW_DIR_IMP_BRUNET = $HOME/daten/brunet
  * DW_DIR_IMP_DWH = $HOME/daten/dwh
  * DW_DIR_IMP_PLATO = $HOME/daten/dwh/plato
  * DW_DIR_IMP_CARMEN = $HOME/daten/carmen
  * DW_DIR_IMP_SAP = $HOME/daten/sap
  * DW_DIR_IMP_SR_RV = $HOME/daten/sap/sr_rv_dpps
  * DW_DIR_IMP_SAP_L = $HOME/daten/sap/sap_l_gutgr (exported as DW_DIR_IMP_SAP_L)
  * DW_DIR_IMP_L_MAHNSTYP_IST = $HOME/daten/sap/mahn
  * DW_DIR_IMP_L_MAHNV_FI = $HOME/daten/sap/mahn
  * DW_DIR_IMP_L_MAHNV_IST = $HOME/daten/sap/mahn
  * DW_DIR_IMP_L_GUTGR = $HOME/daten/sd/l_gutschr
  * DW_DIR_IMP_L_LEIST = $HOME/daten/sd/l_leist
  * DW_DIR_IMP_L_PROD = $HOME/daten/sd/l_prod
  * DW_DIR_IMP_LKODE = $HOME/daten/sd/lkode
  * DW_DIR_IMP_SUBSE = $HOME/daten/subse
  * DW_DIR_SMS_PRG = ${HOME}/aktuell/allgemein/is/util
  * DW_DIR_SMS_ADR = ${HOME}/daten/sms/adressen
  * DW_DIR_SMS_TMP = ${HOME}/daten/sms/tmp
  * DW_DIR_IMP_DPPS = $HOME/daten/dpps
  * DW_DIR_IMP_PLANF2 = $HOME/daten/planf2
  * DW_HOST_CUSTOMER = dxcst3.bn.detemobil.de
  * ORACLE_HOME = /appl/local/oracle/12.2.0.1.0 or /appl/local/oracle/11.2.0 (dynamically selected if not already set)
  * DW_DIR_UTL_FILE = /appl/local/oracle/admin/$ORACLE_SID/utl_file
- Environment files sourced:
  * . $HOME/.dw_global
  * . $HOME/.dw_lokal
- Invokes:
  * . $HOME/.dw_global
  * . $HOME/.dw_lokal
- Called by: unknown (Sourced dynamically by executing ETL scripts)
- Exit-code behaviour: Does not exit; prints warning messages to stdout/stderr if ORACLE_HOME cannot be resolved.
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion.

### Execution order
Mapping of the 6-step legacy execution sequence to the target Cloud Composer (Airflow) DAG and BigQuery/Dataform architecture:
* **Step 1: DW.DWH_ABTN_SMART_KUBI.xml** (UC4 job orchestration) -> Mapped to the main Airflow DAG definition (`dags/dw_dwh_abtn_smart_kubi.py`) which schedules and sequences the tasks.
* **Step 2: d_abtn_x_smart_kubi.sql** (Main ETL/SQL logic loading `DWH$TA_T_SMART_KUBI`) -> Mapped to a Dataform SQLX workflow or a BigQuery execution task (`BigQueryInsertJobOperator`) within the Airflow DAG.
* **Step 3: r_sqlscript** (KSH wrapper script) -> Obsolete. The wrapping, execution control, and logging functions are handled natively by the Airflow task execution framework.
* **Step 4: .dw_init** (Environment initialization script) -> Mapped to a JSON configuration file (`.dw_init.json`) which defines GCS paths and runtime settings. These are loaded into Airflow Variables or DAG parameters at run-time, rather than running as an active task.
* **Step 5: f_alis_msgerr.ksh** (Error logging utility) -> Obsolete. Error trapping, warning generation, and alerting are handled natively by Airflow task status handlers (`on_failure_callback`) and Google Cloud Logging.
* **Step 6: h_alis_sqlplus.ksh** (SQL*Plus helper script) -> Obsolete. Database execution is handled natively using BigQuery's REST API/Airflow operators, removing the need for shell-based SQL execution clients.

### Schedule & variables
The timing and parameters set by the legacy scheduler (UC4) must be preserved using Airflow's native scheduling and macro functionality.
* **Schedule**: The equivalent trigger/schedule will be defined in the Airflow DAG's `schedule_interval` (mapped from UC4 schedule definitions).
* **Runtime Variables**:
  * `DWH_JOB_KENNUNG` (JOB-SPECIFIC): Set to `'ABTN_SMART_KUBI'` in the DAG configuration or `params`.
  * `cdate` (JOB-SPECIFIC): Captured dynamically in Airflow using the execution date macro `{{ ds_nodash }}` (format `YYYYMMDD`).
  * `cmonth` (JOB-SPECIFIC): Extracted via `{{ ds_nodash[:6] }}`.
  * `cday` (JOB-SPECIFIC): Extracted via `{{ ds_nodash[6:8] }}`.
  * `first` (JOB-SPECIFIC): Standardized as `'01'`.
  * `MONATSID` (JOB-SPECIFIC): Calculated dynamically to obtain the previous month's ID in `YYYYMM` format.
    * **Legacy Calculation**: Sets `cmonth` to the first day of the current month, subtracts 1 day to get the last day of the prior month, and extracts the first 6 characters (`YYYYMM`).
    * **Airflow Python Equivalent**:
      ```python
      execution_date = kwargs['execution_date']
      first_day_current_month = execution_date.replace(day=1)
      last_day_prior_month = first_day_current_month - datetime.timedelta(days=1)
      MONATSID = last_day_prior_month.strftime('%Y%m')
      ```

### Lineage
* **Upstream Configuration Sourcing**:
  * `.DW_GLOBAL`: USES_CONFIG dependency. Human-confirmed resolution: **NO SOURCE NEEDED** (global infrastructure settings). Its target equivalent is the global Airflow configuration environment.
  * `.DW_LOKAL`: USES_CONFIG dependency. Human-confirmed resolution: **NO SOURCE NEEDED** (local environment-wide directory overrides). Its target equivalent is the environment-level variable config.

### Cross-file dependencies
* **Environment-to-Script Coupling**:
  * Legacy: The `.dw_init` profile is sourced (`. $HOME/.dw_init`) by shell wrapper scripts (`r_sqlscript`, `h_alis_sqlplus.ksh`) prior to invoking Oracle SQL scripts.
  * Target: This manual sourcing is replaced by Airflow task orchestration, where global environment values and dataset names are injected into BigQuery or Dataform runs as query parameters or compile-time variables, eliminating the need for runtime shell-sourcing.

### Target file plan
The legacy environment configuration is migrated to a structured JSON file to initialize the Cloud Composer Airflow Variables.
* **File Path**: `.dw_init.json`
  * **Language**: JSON
  * **Source File**: `.dw_init`
  * **Purpose**: Maps all active legacy directory pointers to Cloud Storage (GCS) equivalents and stores connection metadata.

### Environment-specific values
Classification of all legacy-sourced parameters according to the target Google Cloud environment policy:
1. **GLOBAL (Environment-Wide)**:
   * `DW_HOST_CUSTOMER` -> Identifies the remote customer connection host (`dxcst3.bn.detemobil.de`). Mapped to Airflow Connections.
   * `GCS_BUCKET` -> Normalized canonical GCS bucket path mapping the legacy `$HOME` base directory structure:
     * `DW_DIR_ROOT` -> `gs://GCS_BUCKET/aktuell`
     * `DW_DIR_PROT` -> `gs://GCS_BUCKET/daten/logfiles`
     * `DW_DIR_CUBES` -> `gs://GCS_BUCKET/daten/cubes`
     * `DW_DIR_IMP_D1` -> `gs://GCS_BUCKET/daten/d1`
     * `DW_DIR_IMP_BWA` -> `gs://GCS_BUCKET/daten/dpps/bwa`
     * `DW_DIR_IMP_XTRA` -> `gs://GCS_BUCKET/daten/xtra`
     * `DW_DIR_IMP_CTEL` -> `gs://GCS_BUCKET/daten/ctel`
     * `DW_DIR_IMP_VO` -> `gs://GCS_BUCKET/daten/vo`
     * `DW_DIR_IMP_RV` -> `gs://GCS_BUCKET/daten/rv`
     * `DW_DIR_IMP_IF` -> `gs://GCS_BUCKET/daten/ees`
     * `DW_DIR_IMP_NNV` -> `gs://GCS_BUCKET/daten/nnv`
     * `DW_DIR_IMP_SIGMA` -> `gs://GCS_BUCKET/daten/gd/sigma`
     * `DW_DIR_EXP_SIGMA` -> `gs://GCS_BUCKET/daten/gd/sigma/export`
     * `DW_DIR_IMP_TRF` -> `gs://GCS_BUCKET/daten/trf`
     * `DW_DIR_IMP_AUF` -> `gs://GCS_BUCKET/daten/sd/auf`
     * `DW_DIR_IMP_GUT` -> `gs://GCS_BUCKET/daten/sd/gut`
     * `DW_DIR_IMP_KDG` -> `gs://GCS_BUCKET/daten/sd/kdg`
     * `DW_DIR_IMP_MP_KDG` -> `gs://GCS_BUCKET/daten/mp/kdg`
     * `DW_DIR_IMP_MP_TS` -> `gs://GCS_BUCKET/daten/mp/ts`
     * `DW_DIR_IMP_MP_ZM` -> `gs://GCS_BUCKET/daten/mp/zm`
     * `DW_DIR_IMP_TS` -> `gs://GCS_BUCKET/daten/sd/ts`
     * `DW_DIR_IMP_ZM` -> `gs://GCS_BUCKET/daten/sd/zm`
     * `DW_DIR_EXP` -> `gs://GCS_BUCKET/daten/exporter`
     * `DW_DIR_IMP_BPM` -> `gs://GCS_BUCKET/daten/bm`
     * `DW_DIR_IMP_ZTS` -> `gs://GCS_BUCKET/daten/zts`
     * `DW_DIR_IMP_VRS` -> `gs://GCS_BUCKET/daten/vrs`
     * `DW_DIR_IMP_BRUNET` -> `gs://GCS_BUCKET/daten/brunet`
     * `DW_DIR_IMP_DWH` -> `gs://GCS_BUCKET/daten/dwh`
     * `DW_DIR_IMP_PLATO` -> `gs://GCS_BUCKET/daten/dwh/plato`
     * `DW_DIR_IMP_CARMEN` -> `gs://GCS_BUCKET/daten/carmen`
     * `DW_DIR_IMP_SAP` -> `gs://GCS_BUCKET/daten/sap`
     * `DW_DIR_IMP_SR_RV` -> `gs://GCS_BUCKET/daten/sap/sr_rv_dpps`
     * `DW_DIR_IMP_SAP_L` -> `gs://GCS_BUCKET/daten/sap/sap_l_gutgr`
     * `DW_DIR_IMP_L_MAHNSTYP_IST` -> `gs://GCS_BUCKET/daten/sap/mahn`
     * `DW_DIR_IMP_L_MAHNV_FI` -> `gs://GCS_BUCKET/daten/sap/mahn`
     * `DW_DIR_IMP_L_MAHNV_IST` -> `gs://GCS_BUCKET/daten/sap/mahn`
     * `DW_DIR_IMP_L_GUTGR` -> `gs://GCS_BUCKET/daten/sd/l_gutschr`
     * `DW_DIR_IMP_L_LEIST` -> `gs://GCS_BUCKET/daten/sd/l_leist`
     * `DW_DIR_IMP_L_PROD` -> `gs://GCS_BUCKET/daten/sd/l_prod`
     * `DW_DIR_IMP_LKODE` -> `gs://GCS_BUCKET/daten/sd/lkode`
     * `DW_DIR_IMP_SUBSE` -> `gs://GCS_BUCKET/daten/subse`
     * `DW_DIR_SMS_PRG` -> `gs://GCS_BUCKET/aktuell/allgemein/is/util`
     * `DW_DIR_SMS_ADR` -> `gs://GCS_BUCKET/daten/sms/adressen`
     * `DW_DIR_SMS_TMP` -> `gs://GCS_BUCKET/daten/sms/tmp`
     * `DW_DIR_IMP_DPPS` -> `gs://GCS_BUCKET/daten/dpps`
     * `DW_DIR_IMP_PLANF2` -> `gs://GCS_BUCKET/daten/planf2`
     * Sourced at runtime inside Python/Airflow DAG via: `from airflow.models import Variable; GCS_BUCKET = Variable.get("GCS_BUCKET")`
   * `ORACLE_HOME` -> Obsolete legacy variable, no direct target equivalent.
   * `DW_DIR_UTL_FILE` -> Legacy database utility path (`/appl/local/oracle/admin/$ORACLE_SID/utl_file`). Obsolete on BigQuery.

2. **JOB-SPECIFIC**:
   * `DWH_JOB_KENNUNG` -> Defined as `'ABTN_SMART_KUBI'` inside DAG parameters or Airflow task context.
   * `MONATSID` -> Dynamically calculated at runtime using Airflow macro-derived Python values.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `.dw_init` | `.dw_init.json` | Environment profile defining legacy directory paths and settings, converted to Airflow Variables JSON configuration to initialize GCS paths and connection variables in the Cloud Composer environment. |

### Risks & Manual Actions
* `SOURCE: NOT FOUND — .DW_GLOBAL — no candidate` (Note: This is human-reviewed and confirmed as **NO SOURCE NEEDED**, as global infrastructure parameters are managed by the environment configuration).
* `SOURCE: NOT FOUND — .DW_LOKAL — no candidate` (Note: This is human-reviewed and confirmed as **NO SOURCE NEEDED**, as local parameter overrides are managed by environment-level Airflow Variables).
* **Legacy Directory Dependency Verification**: Sourced data directories (e.g., `gs://GCS_BUCKET/daten/d1`, etc.) must exist in the target GCS bucket if they are still written to or read from by other processes.

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
    - PL/SQL Anonymous block with variable declarations, dynamic SQL, transaction management, exception block, and metadata/error logging procedures.
1.2 Summarize the business logic and purpose:
    - This is an ETL Aggregation script that loads key performance metrics into the target table `DWH$TA_T_SMART_KUBI` for a specific billing month partition (passed via script parameter `&1`). 
    - It first truncates the target table.
    - It maps incoming contracts, old and new tariff IDs, and verification statuses from contract-history and fact tables using conditional mapping rules (`DECODE`, `NVL`, `LTRIM`, `RTRIM`).
    - It groups and aggregates transaction data, tracking rowcount results, and logging procedure metadata.
1.3 List all entities referenced:
    - `DWH$TA_T_SMART_KUBI` (Target table)
    - `DWH$VI_L_MAP_FA_TA_TARIF` (Source View/Table aliased as `T`)
    - `BL_D_TARIF` (Source Table aliased as `TAR`)
    - `DWH$TA_F_D1_TWVV_TN` (Source Fact table aliased as `fact`)
    - `DWH$TA_C_VERTRAG` (Source Contract Dimension table aliased as `d`)
    - `DWPA_UTIL_SKRIPT` (Oracle Utility Package used for dynamic SQL execution)
    - `DWPA_MELDUNG` (Oracle custom error logging Package)
    - `DWPA_GLOBALS` (Oracle package-level error constant provider)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `PLS_INTEGER` → Map to `INT64` in BigQuery.
    - `NUMBER` → Map to `INT64` (for identifiers and months) or `NUMERIC` / `FLOAT64` where precision or floating values are involved.
    - `VARCHAR2` → Map to `STRING` in BigQuery.
    - `DATE` → Map to `DATE` (or `DATETIME` if time component is critical). The fields in the query represent calendar dates (e.g., `'4712-12-31'`), so BigQuery `DATE` type is chosen.
    - Special Identifier Characters: The Oracle character `$` is invalid in BigQuery table names. It is resolved by replacing `$` with `_` (e.g., `DWH$TA_T_SMART_KUBI` → `dwh_ta_t_smart_kubi`).

2.2 Implicit and Explicit Type Casting:
    - Oracle variables and date functions implicitly cast numbers to dates. In BigQuery, these must be explicitly cast using `CAST()`, `PARSE_DATE()`, and `FORMAT_DATE()`.
    - `TO_NUMBER('&1')` → `CAST(p_monats_id AS INT64)`
    - `TO_DATE(l_monats_id, 'YYYYMM')` → `PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`

2.3 NULL Handling and Conditional Functions:
    - `NVL(x, y)` → `COALESCE(x, y)`
    - `DECODE(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` → Resolved to standard CASE expression:
      `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `DECODE(LTRIM(RTRIM(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` → Resolved to:
      `CASE WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn ELSE fact.vo_kenn_bearb END`

2.4 String Functions:
    - `LTRIM(RTRIM(x))` → Simplified to `TRIM(x)` in BigQuery.
    - `TO_CHAR(v_anzahl_ds)` → `CAST(v_anzahl_ds AS STRING)`.

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)` → Translated using BigQuery date addition:
      `DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH)`
    - `TO_DATE('4712-12-31', 'YYYY-MM-DD')` → Translated to `DATE '4712-12-31'`.
    - `TO_CHAR(fact.gueltigkeitszeitpunkt, 'yyyymm')` → Translated to `FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt)` (assuming `gueltigkeitszeitpunkt` is mapped to BQ `DATE`).

2.6-2.10:
    - Optimizer hints: `/*+ Append */`, `/*+ parallel(t,4) */`, etc., are completely stripped since BigQuery handles execution planning, indexing, and clustering automatically.

2.11 MERGE Statements:
    - Not present in the source.

2.12 INSERT / UPDATE / DELETE:
    - Oracle's dynamic truncate `dwpa_util_skript.runstatement` → Migrated to direct `TRUNCATE TABLE dwh_ta_t_smart_kubi`.
    - The insert utilizes standard BigQuery syntax: `INSERT INTO dwh_ta_t_smart_kubi (...) SELECT ...`

2.13 DDL Constructs:
    - Oracle table partitioned layout: `partition(dwh$ta_f_d1_twvv_tn_&1)` is dynamic partition-level scanning. In BigQuery, this is mapped directly to the base table `dwh_ta_f_d1_twvv_tn`, utilizing standard date/month-based column partitioning. Standard partition pruning will be triggered via the `WHERE` clause filter: `FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)`.

2.14 PL/SQL Scripting constructs:
    - PL/SQL block is refactored into a BigQuery standard procedural script using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` for transaction safety.
    - SQL%ROWCOUNT is mapped to the BigQuery system variable `@@row_count`.
    - Exception block executes a rollback and inserts tracking metrics into a logging table or raises the system error message via scripting exception handlers.

2.15 Unresolvable or Advisory Items:
    - Dynamic logging package `dwpa_meldung.fehler` cannot be executed natively inside BQ. It must be logged via standard table writes or handled by the orchestrator (e.g., Airflow or Python wrapper).

Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    - The PL/SQL block is converted to a BigQuery Standard SQL Procedural Block (`DECLARE`, `SET`, `BEGIN/END`, `BEGIN TRANSACTION`, `COMMIT/ROLLBACK TRANSACTION`, `EXCEPTION`).
3.2 Assumptions:
    - Variables `&1` (Month ID) and `&2` (Entry/Run ID) are declared as script parameters at the very top of the migration script.
    - All table names containing `$` are migrated to use underscore `_`.
3.3 Flagged Items:
    - Replacement of custom logging procedures (`dwpa_meldung.fehler`) with manual TODO block for human review.

═══════════════════════════════════════════
MIGRATION DECISION AND REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Source SQL Statement / Construct | Selected BigQuery Target | Rejected Alternatives | Evidence & Decision Justification |
| :--- | :--- | :--- | :--- |
| **Anonymous PL/SQL Block** | BigQuery SQL Procedural Scripting (`DECLARE`, `BEGIN`, `EXCEPTION`) | Python Wrapper | Script contains database-centric operations (Truncate and single Insert). BigQuery scripting is fully capable of handling local variable scopes, sequential execution, and exceptions. |
| **dwpa_util_skript.runstatement** | Native BigQuery standard `TRUNCATE TABLE` statement | BigQuery Dynamic Execution (`EXECUTE IMMEDIATE`) | The statement passed is deterministic and constant. Dynamic SQL is not required. |
| **Partition-targeted SELECT (`partition(...)`)** | Standard `SELECT` from base table with partitioning filters | Wildcard tables (`_TABLE_SUFFIX`) | Partition pruning on base table `dwh_ta_f_d1_twvv_tn` is highly efficient and avoids dynamic table construction issues. |
| **Custom Package Logging (`dwpa_meldung.fehler`)** | Standard SQL Logging Table insert / standard scripting exception raise | SQL UDF | Scripting does not allow UDFs to execute write operations. Direct INSERT or standard `ERROR` raising is preferred. |
| **Optimizer Hints** | Stripped entirely | BigQuery query options | BigQuery operates serverlessly and automatically parallelizes and optimizes scans. Hints are obsolete. |

2.17 REQUIRED ARTIFACTS

- **BigQuery SQL Script**: A single integrated multi-statement procedural file (`.sql`) containing:
  - Declarations for input variables (bind variables converted to `DECLARE`).
  - Safe transaction control framework (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `ROLLBACK TRANSACTION`).
  - Standard SQL DML statements (`TRUNCATE`, `INSERT INTO`).
  - Structured exception block.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | Target BigQuery Type | Conversion Rule / Logic | Risks / Warnings |
| :--- | :--- | :--- | :--- |
| `PLS_INTEGER` | `INT64` | Direct numeric conversion. | None. |
| `NUMBER` (for IDs) | `INT64` | Maps to 64-bit integer. | Verify that ID values do not contain decimals. |
| `VARCHAR2(300)` | `STRING` | Unicode character string. | No length constraints are enforced by BigQuery on the variable itself. |
| `DATE` | `DATE` | Standard Gregorian calendar date. | Oracle `DATE` stores time. Verify if source fields contain non-midnight values. We assume only date values are present based on the code filters. |

2.19 DESIGN REVIEW SUMMARY

- **Patterns/Objects Found**: Custom package dependencies (`dwpa_util_skript`, `dwpa_meldung`), Dynamic Partition reference, Oracle Outer Join syntax `(+)`.
- **Unsupported Functions**: Oracle dynamic utilities, explicit metadata tracking on rowcounts in transaction buffers.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Base tables `dwh_ta_f_d1_twvv_tn`, `dwh_ta_c_vertrag`, `bl_d_tarif`, `dwh_vi_l_map_fa_tarif`.
- **Warnings**: Ensure dataset or catalog prefix is added to tables in final environment.
- **Manual Intervention Items**: Integration of metadata metrics tracking to replace Oracle packages.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(expression AS INT64)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(date, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y%m', str)` or standard `DATE 'YYYY-MM-DD'` literals |
| `DECODE` | Direct-with-rewrite | Standard `CASE WHEN` logic |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `LTRIM` / `RTRIM` | Direct-with-rewrite | `TRIM` |
| `TO_CHAR` | Direct-with-rewrite | `CAST(expression AS STRING)` or `FORMAT_DATE` for dates |
| `(+)` Outer Join syntax | Direct-with-rewrite | Standard ANSI `LEFT OUTER JOIN` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` system variable |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Vendor-Neutral Pseudocode

```sql
-- Parameters passed down from orchestration to replace Oracle bind variables &1 and &2
DECLARE p_monats_id INT64;
DECLARE p_eintrags_nr INT64;

-- Convert input parameters (Oracle substitution variables)
SET p_monats_id = CAST('201509' AS INT64); -- converted from Oracle &1
SET p_eintrags_nr = CAST('123456' AS INT64); -- converted from Oracle &2

-- Declarations
DECLARE v_anzahl_ds INT64 DEFAULT 0; -- converted from PLS_INTEGER
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

SET l_monats_id = p_monats_id;
SET EintragsNr = p_eintrags_nr;

-- Converted ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1) with explicit type safety
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Wrap operations inside transaction blocks to replicate structural isolation
  BEGIN TRANSACTION;

  -- Dynamic Truncate mapped to standard, direct DML execution
  SET lv_str = 'Truncate table dwh_ta_t_smart_kubi'; 
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Primary INSERT statement
  -- Hints stripped entirely: /*+ Append */, /*+ parallel */, and /*+ use_hash */
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
      SELECT
          t.tarif_id,
          t.dwh_tarif_id,
          t.gueltig_von,
          t.gueltig_bis,
          tar.mp_geschaeftsfeld_id
      FROM dwh_vi_l_map_fa_tarif AS t
      INNER JOIN bl_d_tarif AS tar
         ON t.tarif_id = tar.tarif_id
      WHERE t.gueltig_bis = DATE '4712-12-31'  -- converted from TO_DATE('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT 
      l_monats_id AS monats_id,
      -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
      CASE 
          WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
          ELSE d.t_mobile_kundennummer 
      END AS kundennummer,
      COALESCE(t_new.tarif_id, 0) AS tarif_id,  -- converted from Nvl(t_new.tarif_id,0)
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
  FROM dwh_ta_f_d1_twvv_tn AS fact  -- partitioned mapping: explicit partition filter used in WHERE clause instead
  LEFT OUTER JOIN temp AS t_new  -- converted from fact.dwh_tarif_id_neu = t_new.dwh_tarif_id (+)
    ON fact.dwh_tarif_id_neu  = t_new.dwh_tarif_id 
  LEFT OUTER JOIN temp AS t_old  -- converted from fact.dwh_tarif_id_alt = t_old.dwh_tarif_id (+)
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id 
  LEFT OUTER JOIN dwh_ta_c_vertrag AS d  -- converted from Oracle (+) outer join properties combined with join predicates
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
   AND l_monats_date > d.gueltig_von
   AND l_monats_date <= d.gueltig_bis
  WHERE FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING) -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
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

  -- Print logging output
  SELECT FORMAT('%d rows inserted in dwh_ta_t_smart_kubi', v_anzahl_ds); -- converted from dbms_output.put_line

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  -- Logging and Exception Blocks
  DECLARE ErrText STRING;
  DECLARE ErrC STRING;
  SET ErrText = @@error.message;
  SET ErrC = @@error.code;
  
  -- TODO: Manual intervention needed to replace Oracle package-level logging.
  -- Insert metadata records to tracking tables or throw to standard orchestration logs:
  -- INSERT INTO control_table_log (eintrags_nr, error_code, error_msg, log_time) VALUES (EintragsNr, ErrC, ErrText, CURRENT_TIMESTAMP());
  
  ERROR(FORMAT('Error code: %s. Message: %s', ErrC, ErrText));
END;
```

### FLAGGED ITEMS FOR HUMAN REVIEW
1. **Oracle custom metadata packages**: `dwpa_util_skript` and `dwpa_meldung.fehler` are omitted. Standard SQL logs or target orchestration-level hooks must be written in BigQuery's surrounding pipeline to track running tasks and capture structural log outputs.
2. **Special Characters in Identifiers**: All occurrences of the character `$` in table names (e.g. `dwh$ta_t_smart_kubi`) have been converted to underscore `_` (e.g. `dwh_ta_t_smart_kubi`) to comply with standard BigQuery naming standards. Ensure table schema objects are deployed with these matching names.

### EXECUTION ORDER
The legacy dependency graph defines a 6-step sequence. For this design pass, we are responsible only for step 2 (`d_abtn_x_smart_kubi.sql`). The other steps belong to separate orchestration or wrapper groups (UC4, KSH) and are owned by sibling design passes.
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 orchestration, handled in a sibling design pass)
2. `d_abtn_x_smart_kubi.sql` -> Maps to target file `kubi/d_abtn_x_smart_kubi.sql` (BigQuery SQL script task inside Cloud Composer)
3. `r_sqlscript` (KSH wrapper, handled in a sibling design pass)
4. `.dw_init` (Initialization script, handled in a sibling design pass)
5. `f_alis_msgerr.ksh` (Logging/Error utility, handled in a sibling design pass)
6. `h_alis_sqlplus.ksh` (SQL Execution helper, handled in a sibling design pass)

---

### SCHEDULE & VARIABLES
The scheduler-set variables must be calculated dynamically within the target orchestrator (e.g., Cloud Composer/Airflow DAG) and passed to the BigQuery SQL script task at runtime as parameters.

- **Scheduler-Set Variables:**
  1. `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'` -> Standard identifier stored in Airflow environment configs or as a DAG runtime variable.
  2. `cdate` = `'SYS_DATE("YYYYMMDD")'` -> Calculated via Airflow execution context (e.g., `{{ ds_nodash }}`).
  3. `cmonth` = `'SUBSTR(&cdate,1,6)'` -> Parsed within the orchestrator using standard date/string operations.
  4. `cday` = `'SUBSTR(&cdate,7,2)'` -> Parsed within the orchestrator to extract the calendar day.
  5. `first` = `'01'` -> Constant value used to find the first day of the month.
  6. `cmonth` = `'&cmonth&first'` -> Concatenated in the orchestrator to construct the date string (e.g., `YYYYMM01`).
  7. `cmonth` = `'SUB_DAYS(&cmonth,1)'` -> Date arithmetic calculated in Python (subtracts 1 day to find the last day of the previous calendar month).
  8. `cmonth` = `'SUBSTR(&cmonth,1,6)'` -> Extracted in Python to yield the previous month's ID in `YYYYMM` format.
  9. `MONATSID` = `'&cmonth'` -> Final resolved parameter value, passed dynamically as parameter `p_monats_id` to the BigQuery SQL task.

- **Variables Mapping Table:**

| Scheduler Variable | Target Resolution Mechanism |
| :--- | :--- |
| `DWH_JOB_KENNUNG` | Airflow Variable: `Variable.get("DWH_JOB_KENNUNG", default_var="ABTN_SMART_KUBI")` |
| `MONATSID` | Calculated dynamically via Python datetime arithmetic and passed as SQL query parameter `p_monats_id` |
| `EintragsNr` | Generated run-tracking identifier passed as SQL query parameter `p_eintrags_nr` |

---

### LINEAGE
- **Upstream Producers (Read Tables/Views):**
  - `TABLE:BL_D_TARIF` (Dimension table containing core tariff references)
  - `TABLE:DWH$VI_L_MAP_FA_TARIF` (Tariff dimension mapping view/table)
  - `TABLE:DWH$TA_F_D1_TWVV_TN` (Source Fact table — specifically the partition corresponding to the reporting month)
  - `TABLE:DWH$TA_T_SMART_KUBI` (Target table is read back, potentially for structural verification checks)
- **Downstream Consumers (Write Tables):**
  - `TABLE:DWH$TA_T_SMART_KUBI` (Target aggregation table loaded by this script)
- **Lineage Parser Discrepancies:**
  - `PACKAGE:T_NEW` and `PACKAGE:T_OLD` are listed in legacy lineage edges. However, source SQL inspection reveals these are actually local subquery aliases (`temp t_new` and `temp t_old`) and not database-level Oracle packages.
  - `PACKAGE:DWPA_UTIL_SKRIPT` and `PACKAGE:DWPA_MELDUNG` represent legacy Oracle procedural packages. No direct equivalent exists on the target BigQuery SQL platform.

---

### CROSS-FILE DEPENDENCIES
- **Shared Tables & Common Schemas:**
  - `DWH$TA_F_D1_TWVV_TN`: Partitioned source fact table shared with other activation, migration, and contract analysis load pipelines.
  - `DWH$VI_L_MAP_FA_TARIF`: Reference mapping view utilized broadly across the DWH domain for customer-tariff tracking.
  - `BL_D_TARIF`: Shared master lookup table for Mp-Geschäftsfeld attributes.

---

### TARGET FILE PLAN
- **Target File Relative Path:** `kubi/d_abtn_x_smart_kubi.sql`
- **Language:** SQL (BigQuery Standard SQL Procedural Scripting)
- **Source File:** `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
- **Purpose:** Executes the main truncate-and-insert pipeline to aggregate and load `dwh_ta_t_smart_kubi` table.

---

### ENVIRONMENT-SPECIFIC VALUES
- **GLOBAL (Environment-Wide):**
  - `GCP_PROJECT`: Standard Google Cloud Project ID hosting the target datasets. Retrieved at runtime via the environment: `os.environ.get("GCP_PROJECT")`.
  - `BQ_DATASET`: Target BigQuery dataset containing the schema tables. Retrieved at runtime via orchestrator params.
  - `BQ_LOCATION`: The physical execution region of the BQ dataset.
- **JOB-SPECIFIC:**
  - `p_monats_id`: Reporting month parameter (`YYYYMM`), computed in Python and passed as a script parameter.
  - `p_eintrags_nr`: Running entry ID parameter, passed dynamically to track individual script executions.
  - Normalized Table Identifiers (normalized to lowercase with underscores to remove the legacy `$` character):
    - `dwh_ta_t_smart_kubi`
    - `dwh_vi_l_map_fa_tarif`
    - `dwh_ta_f_d1_twvv_tn`
    - `dwh_ta_c_vertrag`
    - `bl_d_tarif`

---

### FILE DISPOSITION TABLE

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `kubi/d_abtn_x_smart_kubi.sql` | Converts the Oracle PL/SQL anonymous block into a BigQuery standard SQL scripting task featuring safe explicit transactions, cast-based variable parameterization, ANSI left outer joins, and formatted standard outputs. |

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
REASON: This is a utility library of shell functions for error handling, metadata logging, and batch execution tracking that invokes external database routines and performs shell-level variable manipulation and file operations.

EVIDENCE
- Business logic found: KSH custom logic contains helper routines to format log filenames, handle traps/aborts, and perform tracking inserts/updates in the database via stored procedures.
- AWK: none
- SQL-expressible: Partly; database logging calls map to SQL, but shell-level orchestration (trap mechanisms, environment variable reads, dynamic file path assembly) requires Python.
- Non-SQL side effects: Resolves filesystem log paths, creates/deletes temporary files (`/tmp/ErmittleNr_$$.lst`), and handles exit codes.
- Against this verdict: One could implement the logging routines as individual BigQuery stored procedures, but since this is a library designed to be sourced by other shell jobs to control execution flow, it must be migrated to a Python module to remain callable by migrated Python jobs.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

=== DESIGN DOCUMENT STRUCTURE ===

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`, originally `dwmsg.ksh`) acts as a centralized KornShell utility library for error management and metadata logging within the Information Services project. It is sourced by execution scripts that set up a shell `trap` to handle command failures. When triggered, it logs status changes (OK, Aborted), records job execution metadata, and logs application or system errors to a central database repository (`BERT_MELDUNG` packages) using Oracle (to be migrated to BigQuery).

2. INVOCATION CONTEXT
   - Sourced directly (e.g., `. f_alis_msgerr.ksh`) by various parent batch processes/KSH scripts.
   - Parent scripts are invoked via UC4 jobs (UNIX JOBS objects) under different schedules.
   - Sourced environment files: None explicitly sourced inside this script; it expects environment variables like `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT` to be set in the parent execution environment.

3. PARAMETERS / INPUTS
   The utility functions process positional parameters as follows:
   - `DWMSG_Fehlerbehandlung`: 
     * `$1` (DWMSG_EintragsNr) - Central metadata entry ID (sourced from calling job, used in function). Surfaced in Python as function parameter `eintrags_nr`.
   - `DWMSG_SetzeStatusOK`:
     * `$1` (DWMSG_EintragsNr) - Metadata entry ID. Surfaced as function parameter `eintrags_nr`.
   - `DWMSG_SetzeStatusAbbruch`:
     * `$1` (DWMSG_EintragsNr) - Metadata entry ID. Surfaced as function parameter `eintrags_nr`.
   - `DWMSG_ErmittleNr`:
     * `$1` (VarName) - The name of the shell variable where the generated unique ID will be returned via dynamic evaluation (`eval`).
     * Refactoring Note: Out-parameter `VarName` is replaced by returning the value directly from the Python function.
   - `DWMSG_ErzeugeEintrag`:
     * `$1` (DWMSG_EintragsNr) - Metadata entry ID.
     * `$2` (JobKennung) - Identifier for the job.
     * `$3` (Programmname) - Program or script name.
     * `$4` (LogDatei) - Log file path.
     * All are surfaced as native Python function parameters.
   - `DWMSG_MeldeFehler`:
     * `$1` (DWMSG_EintragsNr) - Metadata entry ID.
     * `$2` (Typ) - Error severity type ('F', 'E', 'W').
     * `$3` (FehlerNr) - Application error number.
     * `$4` (Zusatz1) - Optional contextual info (e.g. filename).
     * `$5` (Zusatz2) - Optional secondary contextual info.
     * All are surfaced as native Python function parameters with defaults for optional ones.
   - `DWMSG_Logdateiname`:
     * `$1` (VarName) - Variable to assign the constructed path to via dynamic evaluation (`eval`).
     * `$2` (JobKennung) - Job identifier.
     * `$3` (DWMSG_EintragsNr) - Metadata entry ID.
     * Refactoring Note: Out-parameter `VarName` is replaced by returning the constructed path string directly in Python.
   - `DWMSG_SetzeStichtagInfo`:
     * `$1` (DWMSG_EintragsNr) - Metadata entry ID.
     * `$2` (DWMSG_Stichtag) - Reporting date string.
     * `$3` (DWMSG_StichtagFmt) - Format of the reporting date.
     * All are surfaced as native Python function parameters.
   - `DWMSG_AppendTimingInfos`:
     * `$1` (DWMSG_EintragsNr) - Metadata entry ID.
     * `$2` (DWMSG_InfoText) - Timing text.
     * `$3` (DWMSG_DateFormat) - Datetime formatting template.
     * All are surfaced as native Python function parameters.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus`: Invoked dynamically across functions to execute packaged Oracle stored procedures.
     * Exact command lines:
       1. `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr`
       2. `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr`
       3. `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile"`
       4. `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag $DWMSG_EintragsNr $JobKennung $Programmname $LogDatei`
       5. `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler $Typ $DWMSG_EintragsNr $FehlerNr \'$Zusatz1\' \'$Zusatz2\'`
       6. `sqlplus -s $DW_ORAUSER <<EOF ...` (Heredocs executing `BERT_MELDUNG.SetzeZusatzInfos`)
     * Target Platform Resolution: Since the target platform is confirmed as `BIGQUERY`, these `sqlplus` executions must not remain as external subprocess calls. Instead, they should become native BigQuery client (`google.cloud.bigquery`) calls, executing the corresponding BigQuery stored procedures (e.g., `CALL \`{{project_id}}.dataset.BERT_MELDUNG__SetzeStatusOk\`(...)`).

5. EMBEDDED SQL
   - Procedures called via SQL wrapper scripts:
     * Package: `BERT_MELDUNG`
     * Target Tables: Assumed to be central metadata/log tables managed by the `BERT_MELDUNG` routines (e.g., a table named `BERT_MELDUNG` or similar tracking system logs).
     * Statements / BigQuery equivalent calls:
       1. `BERT_MELDUNG.SetzeStatusOk(DWMSG_EintragsNr)` -> `CALL \`{{project_id}}.dataset.BERT_MELDUNG__SetzeStatusOk\`(eintrags_nr)`
       2. `BERT_MELDUNG.SetzeStatusAbbruch(DWMSG_EintragsNr)` -> `CALL \`{{project_id}}.dataset.BERT_MELDUNG__SetzeStatusAbbruch\`(eintrags_nr)`
       3. `BERT_MELDUNG.Erzeuge_Eintrag(DWMSG_EintragsNr, JobKennung, Programmname, LogDatei)` -> `CALL \`{{project_id}}.dataset.BERT_MELDUNG__Erzeuge_Eintrag\`(eintrags_nr, job_kennung, programmname, log_datei)`
       4. `BERT_MELDUNG.Fehler(Typ, DWMSG_EintragsNr, FehlerNr, Zusatz1, Zusatz2)` -> `CALL \`{{project_id}}.dataset.BERT_MELDUNG__Fehler\`(typ, eintrags_nr, fehler_nr, zusatz1, zusatz2)`
     * Stored Procedure Inline Block (Stichtag):
       ```sql
       EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
       ```
       * Type: Anonymous PL/SQL block / stored-procedure call.
       * BigQuery Translation:
         `CALL \`{{project_id}}.dataset.BERT_MELDUNG__SetzeZusatzInfos\`(eintrags_nr, PARSE_DATE(bq_format_string, stichtag), NULL)`
     * Stored Procedure Inline Block (Timing):
       ```sql
       EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
       ```
       * Type: Anonymous PL/SQL block / stored-procedure call.
       * BigQuery Translation:
         `CALL \`{{project_id}}.dataset.BERT_MELDUNG__SetzeZusatzInfos\`(eintrags_nr, NULL, CONCAT(info_text, ' ', FORMAT_DATETIME(bq_format_string, CURRENT_DATETIME()), ' '))`

6. CONTROL FLOW
   Each function constitutes a standalone control flow unit:
   - **DWMSG_Fehlerbehandlung**: Captures current `$FhlerNr` (`$?`), calls `DWMSG_MeldeFehler` with unexpected error code 10, and calls `DWMSG_SetzeStatusAbbruch`.
   - **DWMSG_SetzeStatusOK**: Verifies `$DWMSG_EintragsNr` is not empty (exits with code 1 if empty), then calls database procedure `SetzeStatusOk`.
   - **DWMSG_SetzeStatusAbbruch**: Verifies `$DWMSG_EintragsNr` is not empty (exits with code 1 if empty), then calls database procedure `SetzeStatusAbbruch`.
   - **DWMSG_ErmittleNr**: Verifies `$VarName` is not empty, queries Oracle for a unique number via `d_al_is_ermittlenr.sql` to a temporary output file, reads the file, cleans white spaces, removes the file, and assigns the value to the variable name dynamically.
   - **DWMSG_ErzeugeEintrag**: Verifies `$DWMSG_EintragsNr` is not empty, calls database procedure `Erzeuge_Eintrag`.
   - **DWMSG_MeldeFehler**: Verifies `$DWMSG_EintragsNr`, determines optional parameters, matches the dynamic wrapper script path, and runs database procedure `Fehler`.
   - **DWMSG_Logdateiname**: Assembles a date-stamped filename under directory `DW_DIR_PROT` and assigns it to `$VarName`.
   - **DWMSG_SetzeStichtagInfo**: Validates parameters, executes SQL inline block calling `SetzeZusatzInfos` with parsed date.
   - **DWMSG_AppendTimingInfos**: Validates parameters, executes SQL inline block calling `SetzeZusatzInfos` appending dynamic formatted timestamps.

7. ERROR HANDLING & EXIT CODES
   - Missing required positional arguments in utility calls prints an error string ("Argh!, ...") to stderr and immediately issues an `exit 1` or `exit 2`.
   - Database operations (PL/SQL execution) are managed by SQL*Plus. If the database execution fails, error codes must be captured and translated to Python exceptions.
   - Success exits are implicit or explicit status 0.
   - Translation to Python: Use `ValueError` or `RuntimeError` for parameter checking (or `sys.exit(code)` to maintain terminal execution termination behaviour if called by processes expecting exit codes). Database exceptions will raise `google.cloud.exceptions.GoogleCloudError`.

8. OUTPUTS / SIDE EFFECTS
   - Central database tracking tables (updated via BigQuery stored procedure calls).
   - Writes date-stamped log files to path configured in environment variable `DW_DIR_PROT`.
   - Emits error messages to stdout/stderr.

9. BUSINESS SUMMARY
   - Standardizes job lifecycle logging across the batch data warehousing architecture.
   - Inserts metadata checkpoints at start, completion, and failure of jobs.
   - Automatically traps shell failures, ensuring unexpected job crashes are immediately updated in the database logging layer and marked as "Aborted".
   - Appends performance metrics and timing diagnostics to log entries for operations auditing.

=== PSEUDOCODE STYLE ===

```python
# Module: dwmsg.py
# Re-usable utility library for BigQuery environment-based logging and execution tracking.

import os
import sys
import datetime
from google.cloud import bigquery

# Helper: Retrieve BigQuery Client
def get_bq_client():
    # # REVIEW: target database platform confirmed as BIGQUERY; ensure credentials / project are configured in environment
    return bigquery.Client()

# Helper: Translate Oracle Datetime Format to BigQuery format string
def translate_oracle_format(fmt: str) -> str:
    # Basic translation for common patterns. Extend as needed.
    mapping = {
        'YYYYMMDD_HH24MI': '%Y%m%d_%H%M',
        'YYYYMMDD': '%Y%m%d',
        'HH24:MI:SS': '%H:%M:%S',
        'DD.MM.YYYY HH24:MI:SS': '%d.%m.%Y %H:%M:%S'
    }
    return mapping.get(fmt, fmt)

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_error_code=None):
    """
    Error handling routine called when a shell trap catches a failure.
    Sets the entry to Aborted and logs an unexpected error code 10.
    """
    if last_error_code is None:
        last_error_code = 1 # Default fallback error code
    
    k_unerw_fehler = 10
    
    # Report standard unexpected failure
    dwmsg_melde_fehler(eintrags_nr, 'F', k_unerw_fehler, f"ErrorCode ist: {last_error_code}")
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    """Sets status of the job execution metadata entry to Success (Ok)."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    # Call BigQuery stored procedure equivalent
    # # REVIEW: project_id and dataset should be customized to your environment
    query = f"CALL `{{project_id}}.dataset.BERT_MELDUNG__SetzeStatusOk`({int(eintrags_nr)})"
    client.query(query).result()

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    """Sets status of the job execution metadata entry to Aborted (Abbruch)."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    # Call BigQuery stored procedure equivalent
    query = f"CALL `{{project_id}}.dataset.BERT_MELDUNG__SetzeStatusAbbruch`({int(eintrags_nr)})"
    client.query(query).result()

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    """
    Obtains a unique job entry ID from the sequence or ID generation logic.
    Returns the generated integer.
    """
    # # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
    
    client = get_bq_client()
    
    # # REVIEW: BigQuery does not use sequences. Implementing entry ID generation via UUID or sequence-holder table is required.
    # Below simulates fetching a unique integer or generating a hashed/integer sequence.
    # We query the BigQuery stored procedure or generation query directly instead of writing to temp file.
    query = "SELECT `{{project_id}}.dataset.generate_next_eintrags_nr`()"
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        eintrags_nr = row[0]
        return str(eintrags_nr).strip()
    
    raise RuntimeError("Could not retrieve a unique entry number from BigQuery.")

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    """Creates a tracking entry in the metadata logging structure."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG__Erzeuge_Eintrag`(
            @eintrags_nr, @job_kennung, @programmname, @log_datei
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programmname", "STRING", programmname),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    client.query(query, job_config=job_config).result()

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1=None, zusatz2=None):
    """Logs an error entry against a tracking job ID."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG__Fehler`(
            @typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", int(fehler_nr)),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1 if zusatz1 is not None else ""),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2 if zusatz2 is not None else "")
        ]
    )
    client.query(query, job_config=job_config).result()

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    """
    Assembles a standardized diagnostic log filename and returns it.
    """
    # Refactored: out-parameter assigned dynamically in KSH is now natively returned.
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{now_str}_{eintrags_nr}.log"
    return filename

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """Saves business reporting date (Stichtag) context in metadata record."""
    # Guard checks
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    client = get_bq_client()
    
    bq_fmt = translate_oracle_format(stichtag_fmt)
    
    # Perform parse and stored procedure call in BigQuery
    query = f"""
        CALL `{{project_id}}.dataset.BERT_MELDUNG__SetzeZusatzInfos`(
            @eintrags_nr, 
            PARSE_DATE(@bq_fmt, @stichtag),
            NULL
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("bq_fmt", "STRING", bq_fmt),
            bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag)
        ]
    )
    client.query(query, job_config=job_config).result()

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """Appends timestamps and profiling remarks to metadata execution record."""
    # Guard checks
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    client = get_bq_client()
    bq_fmt = translate_oracle_format(date_format)
    
    # Calculate formatted datetime in python, then execute call
    now_formatted = datetime.datetime.now().strftime(bq_fmt)
    timing_str = f"{info_text} {now_formatted} "
    
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG__SetzeZusatzInfos`(
            @eintrags_nr, 
            NULL,
            @timing_str
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("timing_str", "STRING", timing_str)
        ]
    )
    client.query(query, job_config=job_config).result()
```

### Execution Order
The execution sequence from the legacy system must be preserved in the target Cloud Composer (Airflow) DAG orchestration as follows:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Legacy UC4 Orchestration) $\rightarrow$ Maps to the target Airflow DAG definition that schedules and orchestrates the tasks.
2. **d_abtn_x_smart_kubi.sql** (Data Loading) $\rightarrow$ Maps to a Dataform execution task or a BigQuery execution task to aggregate and load data into the target table `DWH$TA_T_SMART_KUBI`.
3. **r_sqlscript** (Execution Wrapper) $\rightarrow$ Maps to an Airflow operator (e.g., PythonOperator) executing SQL scripts via BigQuery.
4. **.dw_init** (Environment Initialization) $\rightarrow$ Sourced or executed as a setup step within the Airflow task execution environment.
5. **f_alis_msgerr.ksh** (Error Handling & Logging Library) $\rightarrow$ Maps to the target Python module (`f_alis_msgerr.py`) containing logging and metadata tracking functions.
6. **h_alis_sqlplus.ksh** (Execution Helper) $\rightarrow$ Maps to custom Python helper functions for executing SQL queries on BigQuery.

---

### Schedule & Variables — Must Be Retained
The target Cloud Composer (Airflow) DAG must dynamically calculate and inject the equivalent schedule variables at runtime. These scheduler-set variables will be made available via Airflow DAG context or `params`:

* **DWH_JOB_KENNUNG** $\rightarrow$ Configured as a constant string `'ABTN_SMART_KUBI'`.
* **cdate** $\rightarrow$ Evaluated at runtime from the DAG execution date using Airflow macros:
  ```python
  cdate = "{{ dag_run.logical_date.in_timezone('Europe/Berlin').strftime('%Y%m%d') }}"
  ```
* **cmonth** (initial step) $\rightarrow$ Calculated as the first 6 characters of `cdate`:
  ```python
  cmonth_init = cdate[:6]
  ```
* **cday** $\rightarrow$ Calculated as characters 7 and 8 of `cdate`:
  ```python
  cday = cdate[6:8]
  ```
* **first** $\rightarrow$ Standard string constant `'01'`.
* **cmonth** (concatenated) $\rightarrow$ Concatenated as `cmonth_init + '01'`.
* **cmonth** (subtracted) $\rightarrow$ Derived by converting the concatenated date to a datetime object, subtracting 1 day, and formatting back to `YYYYMM`:
  ```python
  from datetime import datetime, timedelta
  temp_dt = datetime.strptime(cmonth_init + '01', '%Y%m%d')
  subtracted_dt = temp_dt - timedelta(days=1)
  cmonth_final = subtracted_dt.strftime('%Y%m')
  ```
* **MONATSID** $\rightarrow$ Equal to `cmonth_final` (calculated dynamically above).

---

### Lineage
* **Downstream Consumers**:
  * `PROCEDURE:SETZEZUSATZINFOS` $\rightarrow$ BigQuery Stored Procedure (originally Oracle PL/SQL stored procedure called within `BERT_MELDUNG` packages).

---

### Target File Plan
* **Target File Path**: `f_alis_msgerr.py`
  * **Language**: Python
  * **Source File**: `f_alis_msgerr.ksh`

---

### Environment-Specific Values
The environment variables from the source are classified below. They must be retrieved dynamically rather than hardcoded in the target code:

#### GLOBAL (Environment-Wide)
* **GCP_PROJECT** $\rightarrow$ Maps to the target Google Cloud project identifier. Source at runtime via `os.environ.get("GCP_PROJECT")` or Airflow variables.
* **BQ_DATASET** $\rightarrow$ Maps to the metadata/logging dataset containing the migrated execution tracking tables and stored procedures. Source at runtime via `os.environ.get("BQ_DATASET")`.
* **DW_DIR_ROOT** $\rightarrow$ The root directory of the application deployment on Cloud Composer. Source at runtime via `os.environ.get("DW_DIR_ROOT")`.
* **DW_DIR_PROT** $\rightarrow$ The execution/diagnostic logs storage directory or Cloud Storage bucket path. Source at runtime via `os.environ.get("DW_DIR_PROT")`.

#### JOB-SPECIFIC
* **DWH_JOB_KENNUNG** $\rightarrow$ `'ABTN_SMART_KUBI'`. Configured as a job-specific parameter.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | `f_alis_msgerr.py` | Migrates KSH logging and database-tracking utility library to an importable Python module, allowing migrated Airflow tasks to perform central execution logging and status auditing in BigQuery. |

---

### Hard Rules & Output/Print Literal Constraints
Any print, warning, error, or validation logging statements carried over from the original KSH source must preserve their literal German text exactly as written. Surrounding syntax must adapt to native Python logging or output streams without modifying the literal strings:

* `echo "Ich bin im Fehlerhandler, fehler der DB melden..."` $\rightarrow$ Must remain character-for-character: `"Ich bin im Fehlerhandler, fehler der DB melden..."`
* `echo "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"` $\rightarrow$ Must remain character-for-character: `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`
* `echo "Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"` $\rightarrow$ Must remain character-for-character: `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`
* `echo "Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"` $\rightarrow$ Must remain character-for-character: `"Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben"`
* `echo "Argh!, keinen Variablennamen bei ErmittleNr angegeben"` $\rightarrow$ Must remain character-for-character: `"Argh!, keinen Variablennamen bei ErmittleNr angegeben"`
* `echo "Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"` $\rightarrow$ Must remain character-for-character: `"Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben"`
* `echo "Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"` $\rightarrow$ Must remain character-for-character: `"Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben"`
* `echo "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"` $\rightarrow$ Must remain character-for-character: `"Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben"`
* `echo "Argh!, keinen Stichtag angegeben!"` $\rightarrow$ Must remain character-for-character: `"Argh!, keinen Stichtag angegeben!"`
* `echo "Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"` $\rightarrow$ Must remain character-for-character: `"Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!"`
* `echo "Argh!, Formatangabe erforderlich!"` $\rightarrow$ Must remain character-for-character: `"Argh!, Formatangabe erforderlich!"`

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
REASON: The script defines a reusable KornShell utility function with parameter validation, filesystem checks, and dynamic external database utility execution that must be converted to Python.

EVIDENCE
- Business logic found: KSH custom logic defining a reusable SQL*Plus wrapper function (`starteSQLSkript`) with input validation and file readability checks.
- AWK: none
- SQL-expressible: no, it contains filesystem checks and dynamic execution of parameterised SQL scripts.
- Non-SQL side effects: checks file existence/readability and launches SQL*Plus.
- Against this verdict: none, because it is a generic utility library wrapper rather than a single database query/load job, making Python the only logical target.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a KornShell utility library containing helper routines for invoking SQL*Plus. Its primary routine, `starteSQLSkript`, validates parameters, verifies that the target SQL script file is readable, and then safely executes it using SQL*Plus. In the modern GCP architecture with a confirmed target platform of BigQuery, this utility will serve as a foundational Python helper to execute converted BigQuery SQL files.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced or imported by other database orchestration scripts. Its internal function `starteSQLSkript` is called with a Fehlereintragsnummer (error entry number), a script path, and arbitrary SQL parameters.
   - UC4 native includes: None.
   - Environment files sourced: None.
   - External dependencies: Relies on `DWMSG_MeldeFehler` (an external error-reporting utility or function) and `sqlplus` (the Oracle client utility).
     - # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following parameters:
   - `$1` (`p_Eintragsnr`): Fehlereintragsnummer (Error Entry ID). Source: Function call argument. Used inside validation guards and passed to `DWMSG_MeldeFehler`. Surfaced in Python as a positional string argument.
   - `$2` (`p_Skript`): Path of the SQL script to be executed. Source: Function call argument. Used in file check and execution. Surfaced in Python as a positional string/Path argument.
   - `$*` (remaining arguments shifted via `shift 2`): Arbitrary parameter list passed down to the SQL script. Surfaced in Python as variable positional arguments (`*args`).
   - `DW_ORAUSER` (environment variable): Oracle connection string credential. In a BigQuery execution context, this credential is obsolete and should be replaced by BigQuery Client credentials (via service accounts or default credentials).

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     - Purpose: Invokes the Oracle SQL*Plus command-line interface with the credentials in `DW_ORAUSER`, passing the script path and its dynamic arguments, redirecting standard input from `/dev/null` to prevent interactive hangs.
     - Target Transformation: Because the target platform is confirmed as BigQuery, this must become a Python execution utilizing the `google.cloud.bigquery` client. The SQL files passed into `p_Skript` must be migrated to BigQuery-compatible Standard SQL. The parameter passing (`$*`) should map to BigQuery Query Parameters (using `bigquery.ScalarQueryParameter` etc.) or simple template rendering depending on how the SQL scripts are rewritten.

5. EMBEDDED SQL
   There is no embedded SQL inside this wrapper script. The wrapper dynamically executes external `.sql` files specified by the caller via the `p_Skript` parameter.

6. CONTROL FLOW
   1. Define module metadata variables: `ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`.
   2. Function `starteSQLSkript` lifecycle:
      - Assign `$1` to `p_Eintragsnr` and `$2` to `p_Skript`.
      - Shift positional parameters by 2 to capture the remaining arguments (`$*`).
      - Guard 1: Validate that `p_Eintragsnr` and `p_Skript` are not null. If either is missing, call `DWMSG_MeldeFehler` with parameters: `$p_Eintragsnr`, `E`, `196`, and `"${Modul_Name} ${Modul_Version} starteSQLSkript"`. Return exit code `196`.
      - Guard 2: Validate that the file `p_Skript` exists and is readable. If not, call `DWMSG_MeldeFehler` with parameters: `$p_Eintragsnr`, `E`, `201`, and `$p_Skript`. Return exit code `201`.
      - Print execution configuration info to stdout:
        - "Rufe SQL*PLUS auf mit folgenden Einstellungen"
        - "Sql*Plus-Skript : $p_Skript"
        - "Skript-Parameter: $*"
      - Temporarily disable exit-on-error (`set +e`) to allow capturing of the utility's return code.
      - Execute `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`.
      - Capture the exit status in `errcode`.
      - Re-enable exit-on-error (`set -e`).
      - Return `errcode`.

7. ERROR HANDLING & EXIT CODES
   - Missing required inputs: Calls `DWMSG_MeldeFehler` and returns code `196`.
   - File unreadable: Calls `DWMSG_MeldeFehler` and returns code `201`.
   - SQL*Plus execution failure: Propagates the exit code (`$?`) returned by `sqlplus`.
   - Python mapping:
     - Wrap BigQuery executions in `try/except` blocks (handling `google.cloud.exceptions.GoogleCloudError`).
     - Replicate error codes `196` and `201` explicitly when validations fail.
     - Call the Python equivalent of `DWMSG_MeldeFehler` (or a standardized logging/error management framework) on failure.

8. OUTPUTS / SIDE EFFECTS
   - Writes log info to standard output.
   - Standard output / Standard error of the executed SQL scripts.
   - DB modifications applied by the underlying SQL statements inside `p_Skript`.

9. BUSINESS SUMMARY
   - Standardizes the safe execution of SQL scripts.
   - Prevents silent failures or hangs by verifying SQL script readability prior to execution.
   - Formats and logs parameters passed to database scripts.
   - Tracks error details using a centralized message registration mechanism (`DWMSG_MeldeFehler`).
   - Integrates database-level exit code propagation into the shell orchestration layer.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
from pathlib import Path
from typing import List, Any
# Import BigQuery client library (target platform confirmed: BIGQUERY)
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Placeholder for external dependency DWMSG_MeldeFehler
# # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction
def dwmsg_melde_fehler(eintrags_nr: str, msg_type: str, code: int, msg_text: str) -> None:
    print(f"ERROR_LOG [{eintrags_nr}] Type: {msg_type}, Code: {code}, Message: {msg_text}", file=sys.stderr)

def starte_sql_skript(p_eintragsnr: str, p_skript: str, *p_params: Any) -> int:
    """
    Safely executes a SQL script file.
    
    Ported from KSH: starteSQLSkript()
    """
    # Step 1 & 2: Validate that required arguments are present
    # KSH Guard: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        # Replicates: DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"
        # Note: If p_eintragsnr was empty, we pass empty string
        module_info = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr or "", "E", 196, module_info)
        return 196

    # Step 3: Check if the SQL script is readable
    # KSH Guard: if [ ! -r $p_Skript ]
    script_path = Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        # Replicates: DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, str(script_path))
        return 201

    # Step 4: Log invocation settings
    # Replicates echo statements verbatim
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(map(str, p_params))}")

    # Step 5: Execute the SQL target
    # The original script invoked Oracle SQL*Plus. Because the target platform is confirmed 
    # as BIGQUERY, we utilize the Google Cloud BigQuery client to run the migrated SQL file.
    # Note: Any SQL script loaded here must be previously converted to BigQuery dialect.
    try:
        # Initialize the BigQuery client (uses default GCP credentials / service account)
        client = bigquery.Client()
        
        # Read the SQL query from the migrated script file
        with open(script_path, "r", encoding="utf-8") as sql_file:
            query_text = sql_file.read()

        # # REVIEW: Determine parameter parameterisation strategy (query parameters vs. templating).
        # Standard parameters pass-through implementation:
        # For this pseudocode, we will log/execute with parameter replacement if applicable, or as standard text.
        print(f"Executing Query in file '{p_skript}' via BigQuery Client...")
        
        # Run query job
        # (Assuming variables in SQL might be mapped through positional parameters or format placeholders)
        # For safety and generality, we query directly. If query parameters are needed, configure query_params.
        query_job = client.query(query_text)
        
        # Wait for the query to finish execution
        query_job.result()
        
        errcode = 0
    except GoogleCloudError as gcp_err:
        print(f"BigQuery execution failed: {gcp_err}", file=sys.stderr)
        # If execution fails, we return a non-zero exit status.
        errcode = gcp_err.code if hasattr(gcp_err, 'code') else 1
    except Exception as err:
        print(f"Execution failed: {err}", file=sys.stderr)
        errcode = 1

    # Step 6: Return exit status code
    return errcode
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Converted to a Python utility module maintaining validation logic and adapting execution to use Google Cloud BigQuery client library. |

***

### Execution order

The legacy orchestration sequence is structured as follows:
1. `DW.DWH_ABTN_SMART_KUBI.xml` (UC4 orchestration wrapper)
2. `d_abtn_x_smart_kubi.sql` (Main aggregation SQL executing on the database)
3. `r_sqlscript` (Shell execution wrapper)
4. `.dw_init` (Environment initialization)
5. `f_alis_msgerr.ksh` (Error tracking and registration library)
6. `h_alis_sqlplus.ksh` (SQL*Plus execution utility library)

**Orchestration Mapping to Cloud Composer:**
- The utility script `h_alis_sqlplus.ksh` (migrated to `h_alis_sqlplus.py`) is not executed as an independent standalone DAG task. Instead, it is a shared utility module imported and called by other Python tasks in the DAG (such as the task representing `r_sqlscript`) to safely load, validate, and execute BigQuery SQL queries (like `d_abtn_x_smart_kubi.sql`).

***

### Schedule & variables

The following legacy variables must be dynamically calculated and preserved during scheduling inside Cloud Composer (using Airflow context variables, macros, or python execution wrappers):

- **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'`
  - **Target Mapping:** Airflow task parameter or environment variable.
- **`cdate`** = `SYS_DATE("YYYYMMDD")`
  - **Target Mapping:** Computed dynamically using Airflow execution date/logical date, e.g. `{{ ds_nodash }}` or `logical_date.strftime('%Y%m%d')`.
- **`cmonth`** = `SUBSTR(&cdate,1,6)`
  - **Target Mapping:** Derived via slicing or string formatting: `logical_date.strftime('%Y%m')`.
- **`cday`** = `SUBSTR(&cdate,7,2)`
  - **Target Mapping:** Derived via slicing or string formatting: `logical_date.strftime('%d')`.
- **`first`** = `'01'`
  - **Target Mapping:** Constant string parameter.
- **`cmonth`** = `&cmonth&first` (Concatenation to get first day of month)
  - **Target Mapping:** Calculated dynamic string: `f"{cmonth}01"`.
- **`cmonth`** = `SUB_DAYS(&cmonth,1)` (Subtract one day to get the last day of the previous month)
  - **Target Mapping:** Computed using `timedelta(days=1)` subtraction in Python, or DAG execution macro.
- **`cmonth`** = `SUBSTR(&cmonth,1,6)` (Extract Year and Month of previous month)
  - **Target Mapping:** Formatted as Year-Month string.
- **`MONATSID`** = `&cmonth`
  - **Target Mapping:** Dynamic job run identifier passed as query parameter `@monats_id` to BigQuery.

***

### Target file plan

- **Target File Path:** `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
  - **Language:** Python
  - **Source File:** `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`
  - **Target Role:** A Python utility library that encapsulates parameter validation (re-raising exit codes `196` and `201` as appropriate), prints the configuration info in the exact German literal text of the source, and uses the `google.cloud.bigquery` library to load and execute SQL files against BigQuery.

***

### Environment-specific values

- **`GCP_PROJECT`** (GLOBAL):
  - **Description:** The target Google Cloud Project where BigQuery queries are executed.
  - **Target Sourcing:** Resolved at runtime using Python environment variable retrieval: `os.environ.get("GCP_PROJECT")` or Airflow variable configuration `Variable.get("GCP_PROJECT")`.
- **`DW_ORAUSER`** (RETIRED):
  - **Description:** Oracle user database credential string used for SQL*Plus connection.
  - **Target Sourcing:** Obsolete under BigQuery IAM-based authentication. This environment variable is retired. The Python script will instantiate standard Google Application Default Credentials (ADC) or run under the identity of the Cloud Composer worker's service account.

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
REASON: The script is a database orchestration utility that performs getopts argument parsing, relative filesystem path traversal, signal trapping, and dynamic database execution.

EVIDENCE
- Business logic found: KSH custom logic. The script processes command-line options (-f, -i, -j, -v), resolves the location of a SQL script across multiple directories, handles operational framework logging (DWMSG_* functions), and executes SQL via a launcher helper.
- AWK: none
- SQL-expressible: No, this is an orchestration wrapper and utility runner with dynamic file-path resolution and framework-integrated logging, which is not expressible as pure BigQuery SQL.
- Non-SQL side effects: Dynamic filesystem checks (`-f`), directory changes (`cd`), environment sourcing, custom exit code generation, and process trapping.
- Against this verdict: If all SQL files executed by this wrapper were known and static, they could be compiled into individual BigQuery SQL jobs, but as a generic utility wrapper, a Python execution script is required to preserve its reusable routing and orchestration logic.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `r_sqlscript` shell script acts as a standardized wrapper utility used to execute external SQL scripts within a batch execution pipeline. It accepts a SQL filename, searches for the file dynamically across a priority list of directories (`../sql`, `../mig`, or the current directory), establishes framework-compliant error trapping, and executes the SQL using a database launcher. Since the target database platform is confirmed as BigQuery, this utility will be migrated into a Python runner that uses the BigQuery client library to read and run the resolved SQL files.

2. INVOCATION CONTEXT
   - Who calls this script: Called by generic UC4/Automic UNIX jobs using JOBS_UNIX. Typical invocation pattern: `r_sqlscript -f <sql_script_name> [-i <sql_parameters>] [-j <job_name>] [-v]`
   - UC4 native includes: None referenced in the provided extraction.
   - Environment files sourced:
     - `. $HOME/aktuell/.dw_init`
       # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
       # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
       # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — defines starteSQLSkript; behaviour unknown

3. PARAMETERS / INPUTS
   - `p_sqlscript` (Option `-f` via getopts): The name or path of the SQL script to execute. Mandatory. Maps to `argparse` argument.
   - `p_sqlpar` (Option `-i` via getopts): Optional parameters/arguments passed directly to the SQL script. Maps to `argparse` argument.
   - `p_Verbose` (Option `-v` via getopts): Verbose flag (0 or 1). If 1, logs will be output to stderr/stdout on failure. Maps to `argparse` argument.
   - `p_Job` (Option `-j` via getopts): Specific job identifier used for logging and tracking. Defaults to `DWH_KORR` if omitted. Maps to `argparse` argument.
   - `DW_EintragsNr` (Global Environment Variable): Framework-specific entry/execution sequence number. Generated during execution and exported. Maps to an internal tracking state/variable.

   MANDATORY AUDIT STEP:
   No functions containing internal parameter-validation guards of the form `if [ -z "$X" ]` followed by an exit exist in the source code (the only defined function is `usage()`, which contains no logic guards). No review comments for omitted parameter checks are required.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `dirname`: Native directory parsing command. Will be replaced by Python's `os.path.dirname` or `pathlib.Path`.
   - `starteSQLSkript` (defined in sourced script `h_alis_sqlplus.ksh`):
     - Verbatim command: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
     - Purpose: Dynamically executes the SQL file using the framework's database runner.
     - Conversion: Since TARGET_PLATFORM is BIGQUERY, this launcher should be implemented as a native BigQuery Python client execution block (`google.cloud.bigquery.Client().query()`).
     - # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - No inline SQL statements are present in `r_sqlscript` itself. The SQL is loaded from the external path dynamically resolved at runtime.

6. CONTROL FLOW
   1. **Initialization & Sourcing**: Sours `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh` (represented in Python as importing equivalent wrapper modules or logging wrappers).
   2. **Parameter Parsing**: Parses arguments `-f`, `-i`, `-j`, `-v`, and `-h` using standard argument parsing (`argparse`).
   3. **Input Validation**:
      - If required parameters are missing or invalid options are supplied, registers the error via `DWMSG_MeldeFehler` and exits.
   4. **Path Resolution**:
      - Changes execution context to the script's directory.
      - If the directory of the target SQL file is `.`, searches sequentially for:
        1. `../sql/<sql_filename>`
        2. `../mig/<sql_filename>`
        3. `./<sql_filename>`
      - Otherwise, defaults to the literal input path.
   5. **File Existence Validation**:
      - Checks if the resolved path is a file. If it exists, the script executes:
        `ErrNr=198`
        `ErrArg="$p_Kuerzel"`
        # REVIEW: legacy script logic sets ErrNr=198 when the SQL script file EXISTS, and references undefined variable p_Kuerzel. Verify if this check is inverted or obsolete.
   6. **Job Identification**: Sets `JobKennung` to `p_Job` (uppercase) if provided, otherwise defaults to `DWH_KORR`.
   7. **Logging Setup**:
      - Calls framework function `DWMSG_ErmittleNr` to generate `DW_EintragsNr`.
      - Calls `DWMSG_Logdateiname` to define `LogDatei`.
      - Logs execution start using `DWMSG_ErzeugeEintrag` and redirects output to `LogDatei`.
   8. **Trap Setup**: Configures system traps to trigger `DWMSG_Fehlerbehandlung` upon receiving SIGINT (`INT`) or encountering shell runtime errors (`ERR`).
   9. **SQL Execution**: Invokes `starteSQLSkript` with resolved parameters and routes standard outputs to `LogDatei`.
   10. **Success Cleanup**: Sets status to OK via `DWMSG_SetzeStatusOK`, clears signal handlers, and exits with code 0.

7. ERROR HANDLING & EXIT CODES
   - KornShell detects errors using `set -e` and trap handlers on `ERR` and `INT`.
   - Native error framework outputs and registration are conducted via `DWMSG_MeldeFehler` and `DWMSG_Fehlerbehandlung`.
   - In Python, this must be structured using standard try/except blocks wrapping the entire execution. Failures in database queries (via `google.cloud.bigquery`) or missing files will raise standard Python exceptions (`FileNotFoundError`, `GoogleCloudError`), which will trigger equivalent logging calls and execute with non-zero exit codes.

8. OUTPUTS / SIDE EFFECTS
   - Writes all stdout/stderr logs into the dynamic `$LogDatei` path.
   - Performs database alterations as defined inside the dynamically executed SQL target scripts.

9. BUSINESS SUMMARY
   - Establishes a unified, safe runtime interface for database updates within the UC4 scheduler.
   - Provides decoupled, configuration-free execution by dynamically locating SQL files across relative directories (`../sql`, `../mig`).
   - Ensures rigorous enterprise operational tracking by integrating directly with log managers and monitoring tools (`DWMSG_*`).
   - Standardizes error recovery and failure transparency across different database interaction scopes.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Import modern equivalents of system and database libraries
import sys
import os
import argparse
import subprocess
from pathlib import Path
from google.cloud import bigquery

# REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — defines starteSQLSkript; behaviour unknown

# Mocking DWMSG framework logging methods that would map to external modules or enterprise API calls
def DW_MSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
    print(f"Error registered: {severity} {err_nr} {err_arg}", file=sys.stderr)

def DWMSG_ErmittleNr():
    # In legacy, this retrieves a unique run ID from a sequence or database
    return 12345

def DWMSG_Logdateiname(job_kennung, eintrags_nr):
    return f"/tmp/log_{job_kennung}_{eintrags_nr}.log"

def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, script_run, log_file):
    print(f"Log entry created: {job_kennung} - {script_run} -> {log_file}")

def DW_MSG_Fehlerbehandlung(eintrags_nr, log_file, verbose):
    print(f"Running error recovery for ID {eintrags_nr}", file=sys.stderr)
    if verbose == 1:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                print(f.read(), file=sys.stderr)

def DWMSG_SetzeStatusOK(eintrags_nr):
    print(f"Status set to OK for run {eintrags_nr}")

def main():
    # Step 2: Parse command-line parameters
    parser = argparse.ArgumentParser(description="Ausführung Script r_sqlscript equivalent")
    parser.add_argument("-f", dest="p_sqlscript", required=True, help="SQL-Script Name")
    parser.add_argument("-i", dest="p_sqlpar", default="", help="SQL Parameter")
    parser.add_argument("-j", dest="p_Job", default="DWH_KORR", help="Jobkennung")
    parser.add_argument("-v", dest="p_Verbose", action="store_true", help="Verbose")
    
    # Handle parsing errors
    try:
        args = parser.parse_args()
    except SystemExit:
        DW_MSG_MeldeFehler(0, "E", 192, "Invalid arguments")
        sys.exit(192)

    p_sqlscript = args.p_sqlscript
    p_sqlpar = args.p_sqlpar
    p_Job = args.p_Job
    p_Verbose = 1 if args.p_Verbose else 0

    # Step 3: Change directory to script's location and resolve SQL script path
    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir)

    sql_path = Path(p_sqlscript)
    l_DBskript = None

    if sql_path.parent == Path('.'):
        # Search priority: ../sql, then ../mig, then current directory
        opt1 = script_dir.parent / "sql" / p_sqlscript
        opt2 = script_dir.parent / "mig" / p_sqlscript
        opt3 = script_dir / p_sqlscript

        if opt1.is_file():
            l_DBskript = opt1
        elif opt2.is_file():
            l_DBskript = opt2
        else:
            l_DBskript = opt3
    else:
        l_DBskript = sql_path

    # Step 4: Replicate legacy file existence checks and edge behavior
    # REVIEW: legacy script logic sets ErrNr=198 when the SQL script file EXISTS, and references undefined variable p_Kuerzel. Verify if this check is inverted or obsolete.
    if l_DBskript.is_file():
        # Representing legacy behavior verbatim
        err_nr = 198
        p_Kuerzel = "" # Undefined in legacy script, initialized here to prevent runtime crash
        DW_MSG_MeldeFehler(0, "E", err_nr, p_Kuerzel)
        # In legacy, this block sets ErrNr but does not exit immediately, continuing execution.

    # Step 5: Format Job Kennung
    JobKennung = p_Job.upper()

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 6: Initialize Logging Framework Parameters
    DW_EintragsNr = DWMSG_ErmittleNr()
    LogDatei = DWMSG_Logdateiname(JobKennung, DW_EintragsNr)
    DWMSG_ErzeugeEintrag(DW_EintragsNr, JobKennung, f"r_sqlscript_{l_DBskript}", LogDatei)

    # Step 7: Core Job Block with exception handling (Python equivalent to trap INT ERR)
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{DW_EintragsNr}'")
        print(f"Logdatei  : '{LogDatei}'")
        print("---------------------------------------------")

        # Step 8: Execute SQL Script (Representing the legacy starteSQLSkript)
        # REVIEW-STRUCT: launcher starteSQLSkript is resolved to native google.cloud.bigquery client calls since target platform is BIGQUERY
        if not l_DBskript.exists():
            raise FileNotFoundError(f"SQL file not found: {l_DBskript}")
        
        with open(l_DBskript, 'r') as sql_file:
            query_text = sql_file.read()

        # Instantiate BigQuery Client
        client = bigquery.Client()
        
        # Format parameters if any are passed. Since the wrapper passes raw strings, 
        # actual deployment should configure QueryJobConfig parameterized values if needed.
        query_config = bigquery.QueryJobConfig()
        
        print(f"Executing Query: {l_DBskript} on BigQuery")
        query_job = client.query(query_text, job_config=query_config)
        query_job.result() # Wait for job completion. Will raise exception on SQL failure.

    except Exception as e:
        # Error handling path equivalent to trap_err / trap_os
        print(f"!OSFEHLER / !FEHLER gemeldet!: {str(e)}", file=sys.stderr)
        DW_MSG_Fehlerbehandlung(DW_EintragsNr, LogDatei, p_Verbose)
        sys.exit(1)

    # Step 9: Post-execution success procedures
    DWMSG_SetzeStatusOK(DW_EintragsNr)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Converts the KSH utility wrapper to a Python runner that dynamically resolves, parameters, and executes external SQL scripts against BigQuery, maintaining framework logging and signal trapping equivalents. |

---

### Execution Order
The target orchestration (e.g., Cloud Composer / Airflow) must preserve the 6-step execution sequence from the legacy dependency graph:
1. **DW.DWH_ABTN_SMART_KUBI.xml** -> Mapped to the parent Airflow DAG definition and scheduling.
2. **d_abtn_x_smart_kubi.sql** -> The target SQL query executed by the workflow runner.
3. **r_sqlscript** -> Mapped to the execution task invoking the migrated Python script (`local/home/gurunathan_t/kubi/r_sqlscript.py`).
4. **.dw_init** -> Sourced configurations mapped to Airflow Variables or environment variables.
5. **f_alis_msgerr.ksh** -> Core error-logging framework routines mapped to Python standard logging or custom hooks.
6. **h_alis_sqlplus.ksh** -> SQL*Plus launcher routines replaced natively by the Python Google Cloud BigQuery client library.

---

### Schedule & Variables
The variables generated by the scheduler (UC4) must be dynamically computed in the target environment (e.g., using Apache Airflow dynamic context / Jinja macros) and supplied as inputs or environment parameters to the Python job task:

*   **DWH_JOB_KENNUNG**: `'ABTN_SMART_KUBI'` (Static job identification string).
*   **cdate**: Dynamic date representation in `YYYYMMDD` format. Calculated in Airflow using `{{ ds_nodash }}` or current execution date context.
*   **cmonth**: Extracted from `cdate` (first 6 characters, e.g., `YYYYMM`).
*   **cday**: Extracted from `cdate` (last 2 characters, e.g., `DD`).
*   **first**: Static string value `'01'`.
*   **cmonth (Intermediate manipulation)**: Initialized as `YYYYMM` + `01`, then decremented by 1 day (subtracting 1 day using date manipulation logic to resolve the previous month's ending date).
*   **cmonth (Final value)**: Extracted from the decremented date (first 6 characters, representing the prior month in `YYYYMM` format).
*   **MONATSID**: Set to the resolved value of `cmonth` (prior month `YYYYMM` string).

These variables must be passed to the migrated script using Airflow's environment mappings or task execution `params`.

---

### Lineage
Based on legacy code structures, the script interfaces with the following dependencies:
*   **Upstream Sourced Framework Modules**:
    *   `FILE:f_alis_msgerr.ksh` (provides logging/monitoring error handlers: `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`).
    *   `FILE:h_alis_sqlplus.ksh` (provides launcher execution routines: `starteSQLSkript`).
    *   `FILE:.dw_init` (establishes standard base path and system configurations).

---

### Cross-File Dependencies
*   **Shared SQL execution schema**: The script requires access to external `.sql` scripts located in the runtime search paths (such as `../sql/` or `../mig/` relative to the script execution path).
*   **Shared framework tracking**: The legacy system uses operational status tracking via environment procedures. The migrated script maps these actions to global cloud-compatible monitoring (e.g., Cloud Logging or central metadata tracking).

---

### Target File Plan
*   **Target File Path**: `local/home/gurunathan_t/kubi/r_sqlscript.py`
    *   **Language**: Python
    *   **Source File**: `local/home/gurunathan_t/kubi/r_sqlscript`

---

### Environment-Specific Values

#### GLOBAL (Environment-Wide Configuration)
*   **GCP_PROJECT**: The target BigQuery Google Cloud Project ID.
    *   *Sourcing Method*: Sourced via environment variable `os.environ.get("GCP_PROJECT")` or Cloud Composer execution configs.
*   **DW_DIR_ROOT**: The legacy root path containing shared logging libraries.
    *   *Sourcing Method*: Map to repository root folder or local environment paths where utility scripts are packaged, retrieved via `os.environ.get("DW_DIR_ROOT")`.
*   **HOME**: Legacy home directory context.
    *   *Sourcing Method*: Standard OS variable `os.environ.get("HOME")` or local directory workspace structures.

#### JOB-SPECIFIC (Job Runtime Parameters)
*   **JobKennung**: The specific monitoring name for execution logs (defaults to `'DWH_KORR'` if not provided via parameter `-j`).
    *   *Sourcing Method*: Passed as an execution argument (`-j` / `--job`) and parsed via `argparse`.
*   **l_DBskript**: The dynamically resolved filepath of the target `.sql` query to execute.
    *   *Sourcing Method*: Resolved dynamically within the execution directory using relative path probing (`../sql`, `../mig`).
*   **p_sqlpar**: Argument string passed directly to the executed SQL script.
    *   *Sourcing Method*: Passed as an execution argument (`-i` / `--input`) and parsed via `argparse`.