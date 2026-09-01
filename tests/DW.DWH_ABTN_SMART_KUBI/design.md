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


# DESIGN DOCUMENT: DW.DWH_ABTN_SMART_KUBI Migration

## 1. Overview
This extraction bundle consists of a single Unix-based job, `DW.DWH_ABTN_SMART_KUBI`, which is responsible for populating a temporary table by executing a SQL script (`d_abtn_x_smart_kubi.sql`). The job dynamically calculates a reporting month parameter (`MONATSID`) based on the execution date: if the execution day is before the 15th, it uses the previous month; otherwise, it uses the current month. As no workflow/jobplan (JOBP) or schedule (EVNT_TIME/JSCH) was supplied in this bundle, this job is represented here as a standalone DAG that is assumed to be externally triggered.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 | Populate temp table |

## 3. Scheduling
- **Schedule**: `None`
- **Trigger Source**: Externally triggered (source unknown from this extraction alone; no matching SCRI or JOBP objects were provided in this bundle).

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
| `dwh_abtn_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `EmptyOperator` | N/A | N/A | 1 | 5m | None | None | False | None | # REVIEW-STRUCT: Launcher wraps SQL script `d_abtn_x_smart_kubi.sql`, converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL. Confirm actual artifact before wiring a real operator (e.g., BigQueryInsertJobOperator). |

## 6. Task Dependency Map
Since only a single job has been extracted, the task dependency map is trivial:
```
dwh_abtn_smart_kubi
```

## 7. Sync / Concurrency Analysis
No UC4 synchronization rules (`sync_rows`) or cross-DAG locks were found in this extraction.

## 8. Error Handling and Retry Strategy
No specific postcondition actions or complex retry blocks were found in the extraction. Default Airflow task retry parameters are applied.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&MONATSID` | Script-derived date calculation: If day < 15, use previous month (`YYYYMM`); else, use current month (`YYYYMM`). | Python macro/Jinja expression evaluated at runtime: `{{ (execution_date.in_timezone('Europe/Berlin').subtract(months=1) if execution_date.in_timezone('Europe/Berlin').day < 15 else execution_date.in_timezone('Europe/Berlin')).strftime('%Y%m') }}` |
| `&DWH_JOB_KENNUNG` | `'ABTN_SMART_KUBI'` | Passed as Airflow parameter or environment variable if required by the target artifact. |

## 10. Developer Notes
* **# REVIEW-STRUCT: SQL Translation Gap**: The job `DW.DWH_ABTN_SMART_KUBI` executes an external SQL script `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`. The conversion of this SQL code is handled outside this XML extraction by a KSH/SQL migration pipeline. The developer must replace the placeholder `EmptyOperator` with the corresponding target operator (e.g., `BigQueryInsertJobOperator` if migrating to GCP/BigQuery, or `BashOperator`/`PythonOperator` if the script remains a local file/Python equivalent).
* **# REVIEW-STRUCT: Workflow Context Gap**: This job was extracted as a standalone object without its parent `JOBP` workflow container. The DAG has been structured as a standalone wrapper; however, its eventual upstream trigger must be wired in once the wider orchestrating pipeline is defined.
* **Date Parsing Logic**: The UC4 date logic (`SYS_DATE`, `SUB_DAYS`) translates to standard Airflow context-aware date manipulation. A timezone-aware Jinja/Python expression is provided to replicate this dynamic logic.

---

## PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW-STRUCT: Configure GCP variables here if BigQuery is target
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_REGION = "YOUR_GCP_REGION"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=DEFAULT_ARGS,
    description="Populate temp table - Standalone Migration wrapper",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dwh_abtn_smart_kubi ────────────────────────
    # # REVIEW-STRUCT: The launcher wraps SQL script [d_abtn_x_smart_kubi.sql],
    # which is converted separately by the companion KSH/SQL migration pipeline.
    # Replace this EmptyOperator placeholder with the concrete operator when ready.
    #
    # Expected parameter:
    # MONATSID equivalent in Jinja:
    # {% set dt = execution_date.in_timezone('Europe/Berlin') %}
    # {% if dt.day < 15 %}
    #   {% set monatsid = (dt.replace(day=1) - timedelta(days=1)).strftime('%Y%m') %}
    # {% else %}
    #   {% set monatsid = dt.strftime('%Y%m') %}
    # {% endif %}
    
    dwh_abtn_smart_kubi = EmptyOperator(
        task_id="dwh_abtn_smart_kubi",
        # Keep empty until target architecture is finalized (BigQuery vs Python VM)
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-node DAG, no dependency statements required.
    pass
```

### Execution order
The target Airflow DAG (orchestration) must preserve the logical execution sequence of the legacy job. The ordered legacy steps are mapped to the target as follows:
- **Step 1: `DW.DWH_ABTN_SMART_KUBI.xml`** → Mapped to the overall Airflow DAG wrapper (`dags/dw_dwh_abtn_smart_kubi.py`).
- **Step 2: `d_abtn_x_smart_kubi.sql`** → Mapped to a native BigQuery execution task (e.g., `BigQueryInsertJobOperator`) within the DAG.
- **Step 3: `r_sqlscript`** → Retired. Replaced by native Airflow BigQuery execution operators.
- **Step 4: `.dw_init`** → Retired. Shared configurations are handled natively via Airflow environment variables or connections.
- **Step 5: `f_alis_msgerr.ksh`** → Retired. Replaced by Airflow’s built-in task failure callbacks and logging capabilities.
- **Step 6: `h_alis_sqlplus.ksh`** → Retired. Replaced by BigQuery native API calls.

### Schedule & variables
The timing and dynamic variables from the legacy scheduler must be retained. 
- **Scheduling**: The legacy job is scheduled to run monthly (as indicated by the monthly context and `MONATSID` calculation). In the Airflow DAG, this is represented with a schedule of `@monthly` (or an equivalent cron expression) while taking care of catching up via `catchup=False`.
- **Variables**:
  - `DWH_JOB_KENNUNG`: Inherited constant value of `'ABTN_SMART_KUBI'`. This will be passed to the execution task as a parameter or task tag.
  - `MONATSID`: Dynamically calculated reporting month based on the execution date. The legacy calculation logic is preserved verbatim via a Python/Jinja macro inside the DAG:
    - If the current execution day is before the 15th, it calculates the previous month in `YYYYMM` format.
    - Otherwise, it calculates the current month in `YYYYMM` format.
    - Output statement literal text is preserved exactly as: `"Berichtsmonat: "` (printed during execution).

### Lineage
The runtime lineage and relationships are structured as follows:
- **Upstream / Inclusions**:
  - `DW.HOLE_PFAD` (Included script) → Human-confirmed as **NO SOURCE NEEDED**; its logic is retired/omitted.
  - `DW.LESE_LOG` (Included script) → Human-confirmed as **NO SOURCE NEEDED**; its logic is retired/omitted.
  - `.dw_init` (Shell initialization) → Replaced by native Airflow configuration.
- **Downstream / Invoked Scripts**:
  - `d_abtn_x_smart_kubi.sql` (SQL invocation) → Handed off to the separate SQL migration pipeline. It is not part of this design pass.
- **Legacy Host**:
  - `dwhdwh1p` (Execution host) → Retired. Workloads are shifted to serverless BigQuery and Cloud Composer.
- **Legacy Package**:
  - `DW.UNIX.ISTNS` (Execution login/user context) → Mapped to the target execution Service Account on GCP.

### Cross-file dependencies
The orchestration DAG directly invokes the SQL logic defined in `d_abtn_x_smart_kubi.sql`. This SQL script manipulates the following tables/views, establishing a database-level dependency on:
- `BL_D_TARIF` (Source table)
- `DWH$TA_F_D1_TWVV_TN` (Source table)
- `DWH$VI_L_MAP_FA_TARIF` (Source view)
- `DWH$TA_T_SMART_KUBI` (Target table)

### Target file plan
The following target file will be generated:
- **Relative Path**: `dags/dw_dwh_abtn_smart_kubi.py`
- **Language**: Python (Airflow DAG)
- **Source File**: `DW.DWH_ABTN_SMART_KUBI.xml`
- **Purpose**: Defines the Airflow DAG structure, computes the runtime `MONATSID` variable, and triggers the BigQuery task that executes the compiled version of `d_abtn_x_smart_kubi.sql`.

### Environment-specific values
These values are classified by their target role and must be dynamically resolved at runtime rather than hardcoded:
1. **GLOBAL**
   - `GCP_PROJECT`: The target Google Cloud Project ID where the BigQuery tables reside. Sourced using `os.environ.get("GCP_PROJECT")` or `Variable.get("GCP_PROJECT")`.
   - `GCP_REGION`: The GCP location for data processing and Composer execution. Sourced via `os.environ.get("GCP_REGION")`.
   - `BQ_DATASET`: The destination BigQuery dataset name.
2. **JOB-SPECIFIC**
   - `gcp_conn_id`: Airflow connection identifier for GCP authentication, set to `'google_cloud_default'`.
   - `sql_file_path`: Path to the migrated SQL script in the target GCS bucket or DAG resources.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `dags/dw_dwh_abtn_smart_kubi.py` | Converted into a standard Airflow Python DAG. Retains the dynamic `MONATSID` calculation and triggers the main SQL execution task. |

### Risks & Manual steps
- **Separately Migrated SQL Logic**: The SQL logic contained within `d_abtn_x_smart_kubi.sql` is not in scope for this design pass. It is assumed to be migrated separately (via Dataform or a companion SQL pipeline). The developer must verify the final path and execution interface of this SQL asset and update the `BigQueryInsertJobOperator` task in the target DAG accordingly.
- **Reporting Date Logic Edge Cases**: The legacy scheduler computes the reporting month (`MONATSID`) based on the runtime execution date. Ensure that the Airflow `execution_date` or local timezone-aware `data_interval_end` is passed correctly to match the legacy behavior during catchups or manual backfills.

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
REASON: This is an environment initialization script that only defines directory path variables and sources configuration files.

EVIDENCE
- Business logic found: none — the script's sole function is setting up environment variables and directory paths for legacy tools.
- AWK: none
- SQL-expressible: no (only environment variable exports and directory checks)
- Non-SQL side effects: none observed
- Against this verdict: none

ORCHESTRATION SUMMARY
- Purpose: This environment profile script (.dw_init) sets up local and remote directory paths, locates the ORACLE_HOME directory, and sources global and local configuration profiles.
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
  - DW_DIR_IMP_SAP_L_GUTGR = $HOME/daten/sap/sap_l_gutgr
  - DW_DIR_IMP_L_MAHNSTYP_IST = $HOME/daten/sap/mahn
  - DW_DIR_IMP_L_MAHNV_FI = $HOME/daten/sap/mahn
  - DW_DIR_IMP_L_MAHNV_IST = $HOME/daten/sap/mahn
  - DW_DIR_IMP_L_GUTGR = $HOME/daten/sd/l_gutschr
  - DW_DIR_IMP_L_LEIST = $HOME/daten/sd/l_leist
  - DW_DIR_IMP_L_PROD = $HOME/daten/sd/l_prod
  - DW_DIR_IMP_LKODE = $HOME/daten/sd/lkode
  - DW_DIR_IMP_SUBSE = $HOME/daten/subse
  - DW_DIR_SMS_PRG = ${HOME}/aktuell/allgemein/is/util
  - DW_DIR_SMS_ADR = ${HOME}/daten/sms/adressen
  - DW_DIR_SMS_TMP = ${HOME}/daten/sms/tmp
  - DW_DIR_IMP_DPPS = $HOME/daten/dpps
  - DW_DIR_IMP_PLANF2 = $HOME/daten/planf2
  - DW_HOST_CUSTOMER = dxcst3.bn.detemobil.de
  - ORACLE_HOME = /appl/local/oracle/12.2.0.1.0 or /appl/local/oracle/11.2.0 (dynamically selected if unassigned)
  - DW_DIR_UTL_FILE = /appl/local/oracle/admin/$ORACLE_SID/utl_file
- Environment files sourced:
  - `. $HOME/.dw_global`
  - `. $HOME/.dw_lokal`
- Invokes: None. It only sources the environment files listed above.
- Called by: Sourced dynamically at startup by other KornShell scripts in the legacy environment to import required environment variables.
- Exit-code behaviour: Propagates the exit status of sourced scripts. It has no custom error traps or exit statements.
- Recommendation: Retain as-is. This script performs no business logic and requires no conversion. Sourced environment configurations of local directory structures should instead be mapped to orchestrator environment variables (e.g., Airflow variables or dbt profiles) if migrating to Google Cloud/BigQuery.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/.dw_init` | Retired | Legacy KornShell environment initialization script. It defines local directory variables, Oracle environment variables, and sources config files. Since the target environment is Google Cloud (BigQuery and Cloud Composer/Airflow), directory paths and Oracle variables are obsolete. Environment configurations must be managed natively via Airflow variables, connections, or Composer environment variables. |

---

### Execution Order
The legacy dependency graph lists a 6-step execution sequence. The retired environment initializer `.dw_init` was executed as Step 4 in the sequence. In the target orchestration, this step is handled implicitly through Airflow environment configurations and does not require an independent, sequential task:

1. **DW.DWH_ABTN_SMART_KUBI.xml** (UC4 XML Orchestration) $\rightarrow$ Migrated to Airflow DAG structure (handled in a separate orchestration migration pass).
2. **d_abtn_x_smart_kubi.sql** (Oracle PL/SQL Script) $\rightarrow$ Migrated to BigQuery / Dataform pipeline (handled in a separate SQL migration pass).
3. **r_sqlscript** (Helper Script) $\rightarrow$ Handled by the target orchestration logic or Python/Bash task in Airflow.
4. **.dw_init** (Environment Initializer) $\rightarrow$ **Retired** (Standalone task eliminated; legacy variables are resolved natively via Airflow environment variables or task parameters).
5. **f_alis_msgerr.ksh** (Error Helper) $\rightarrow$ Migrated to Airflow built-in error handling and alerting mechanisms (e.g., `on_failure_callback`).
6. **h_alis_sqlplus.ksh** (SQL Runner) $\rightarrow$ Migrated to the Airflow BigQueryOperator or equivalent client execution operator.

---

### Schedule & Variables
The legacy scheduler-set variables must be mapped to Airflow macros, Airflow Variables, or run parameters in the Airflow DAG wrapper that schedules this process:

* **cdate** (`SYS_DATE("YYYYMMDD")`): Map to Airflow template macro `{{ ds_nodash }}` representing the DAG execution date (format `YYYYMMDD`).
* **cday** (`SUBSTR(&cdate,7,2)`): Map to Airflow template macro `{{ dag_run.logical_date.strftime('%d') }}`.
* **cmonth** (`SUBSTR(&cdate,1,6)`): Map to Airflow template macro `{{ dag_run.logical_date.strftime('%Y%m') }}`.
* **first** (`'01'`): Standard Python/Airflow constant within the DAG parameters.
* **MONATSID** (`&cmonth`): Derived dynamically from the Airflow execution date macro and passed directly into downstream BigQuery tasks.
* **DWH_JOB_KENNUNG** (`'ABTN_SMART_KUBI'`): Retained as a static DAG-level parameter or Airflow `Variable` to identify the job context in logs.

---

### Lineage
The lineage edges of `.dw_init` indicate dependencies on external configurations:
* **Upstream Sourced Configurations:**
  * `.dw_init` $\rightarrow$ `UNRESOLVED:.DW_GLOBAL` (uses config)
  * `.dw_init` $\rightarrow$ `UNRESOLVED:.DW_LOKAL` (uses config)

*Note: Per human-confirmed resolutions, both `.DW_GLOBAL` and `.DW_LOKAL` have been marked as **NO SOURCE NEEDED** and are not required to be migrated as physical source scripts.*

---

### Cross-File Dependencies
* **Sourced Environment Variables:** Legacy scripts (such as the SQL runners and PL/SQL execution scripts) sourced `.dw_init` to obtain the directory variables (`DW_DIR_IMP_*`, `DW_DIR_PROT`) and `ORACLE_HOME`. These paths are replaced in the target cloud architecture with unified Cloud Storage (GCS) URI variables or direct BigQuery dataset references.

---

### Target File Plan
Because `.dw_init` has a disposition of **Retired**, no target code file is generated for it. Its variable-setting responsibilities are absorbed entirely into the Cloud Composer environment and the Airflow DAG configuration.

---

### Environment-Specific Values
The environment variables from `.dw_init` are classified below to guide their setup on Google Cloud:

#### 1. GLOBAL (Environment-Wide)
These values represent the infrastructure configuration and should be defined as environment variables in Cloud Composer (Airflow) or parameterized via Airflow Connections/Variables:
* **GCP_PROJECT**: The target Google Cloud Project ID. Sourced in Python using `os.environ.get("GCP_PROJECT")`.
* **GCS_BUCKET**: The Cloud Storage bucket representing the target root directory (replacing legacy local path root `$DW_DIR_ROOT`). Sourced using `os.environ.get("GCS_BUCKET")` or Airflow variables.
* **BQ_LOCATION**: The target BigQuery region (e.g., `EU` or `US`).

#### 2. JOB-SPECIFIC
These values are specific to the legacy execution environment and are either obsolete or mapped directly to task parameters/variables within the Airflow DAG:
* **DWH_JOB_KENNUNG**: Set to `'ABTN_SMART_KUBI'` and made available within the DAG context.
* **DW_HOST_CUSTOMER** (`dxcst3.bn.detemobil.de`): Legacy SFTP/Remote host. If remote file transfers are still required, this should be defined as an Airflow Connection ID (`customer_sftp_connection`).
* **ORACLE_HOME**: Obsolete. Not required for BigQuery operations.
* **DW_DIR_UTL_FILE**: Obsolete. Oracle UTL directory is replaced by GCS staging paths.

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
    - This is a PL/SQL anonymous block script containing transactional block parameters, variable declarations, a dynamic table truncation call, and a major multi-table INSERT INTO statement utilizing common table expressions (CTEs), ANSI-style mapping of legacy Oracle outer join operators (`(+)`), and exception handling with custom rollback logging.

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script aggregates monthly telecommunication service contracts and tariff changes. 
    - It truncates a target data warehouse table (`DWH$TA_T_SMART_KUBI`) and performs an incremental load based on a target month ID parameter.
    - It pulls data from a partitioned transaction fact table (`DWH$TA_F_D1_TWVV_TN`), resolves corresponding current and previous tariffs using a mapping CTE (`temp`), and filters active records based on validity start/end timestamps from a contract table (`DWH$TA_C_VERTRAG`).
    - After completion, it logs the row count of inserted rows or raises an application-specific error on failure.

1.3 List all entities referenced:
    - Tables/Views:
      * `DWH$TA_T_SMART_KUBI` (Target table)
      * `DWH$VI_L_MAP_FA_TARIF` (Aliased as `T` inside CTE)
      * `BL_D_TARIF` (Aliased as `TAR` inside CTE)
      * `DWH$TA_F_D1_TWVV_TN` (Aliased as `fact`, originally queried from a specific partition `dwh$ta_f_d1_twvv_tn_&1`)
      * `DWH$TA_C_VERTRAG` (Aliased as `d`)
    - External Objects / Package APIs:
      * `dwpa_util_skript.runstatement` (Utility to execute dynamic statements)
      * `dwpa_globals.k_alis_err_unknown` (Global variable for generic error code)
      * `dwpa_meldung.fehler` (Error reporting procedure)

---

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `pls_integer` → `INT64`
    - `NUMBER` (without precision) → `NUMERIC` (or `INT64` for identifier/count usage)
    - `VARCHAR2(300)`, `VARCHAR2(512)` → `STRING`
    - Oracle `DATE` (which contains time) → mapped to `DATE` where only date components are processed (e.g. `l_monats_date`), or `DATETIME` if timestamps are preserved. Here, we will map them to BQ `DATE` to maintain clean date boundary conditions.

2.2 Implicit and Explicit Type Casting:
    - Implicit type conversions like comparing numbers (monats_id) to strings inside format functions are explicitly resolved using `CAST(... AS STRING)`.
    - Oracle date comparison strings in CTEs are resolved using explicit BQ date constructors (`DATE '4712-12-31'`).

2.3 NULL Handling and Conditional Functions:
    - `NVL(t_new.tarif_id, 0)` → `COALESCE(t_new.tarif_id, 0)`
    - `NVL(t_old.tarif_id, 0)` → `COALESCE(t_old.tarif_id, 0)`
    - `DECODE(t_new.mp_geschaeftsfeld_id, 2, '-1', d.t_mobile_kundennummer)` → `CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END`
    - `DECODE(ltrim(rtrim(fact.vo_kenn_bearb)), NULL, fact.vo_kenn, '#', fact.vo_kenn, fact.vo_kenn_bearb)` → Mapped to:
      ```sql
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END
      ```

2.4 String Functions:
    - `LTRIM(RTRIM(x))` → `TRIM(x)`
    - `TO_CHAR(v_anzahl_ds)` → `CAST(v_anzahl_ds AS STRING)`
    - `TO_CHAR(fact.gueltigkeitszeitpunkt, 'yyyymm')` → `FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE))`

2.5 Date and Timestamp Functions:
    - `TO_DATE(l_monats_id, 'YYYYMM')` → `PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING))`
    - `ADD_MONTHS(date, 1)` → `DATE_ADD(date, INTERVAL 1 MONTH)`
    - `To_date('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`
    - Date arithmetic comparisons:
      * `l_monats_date > d.gueltig_von(+)` → Resolved as an explicit `LEFT OUTER JOIN` condition: `l_monats_date > CAST(d.gueltig_von AS DATE)`
      * `l_monats_date <= d.gueltig_bis(+)` → Resolved as an explicit `LEFT OUTER JOIN` condition: `l_monats_date <= CAST(d.gueltig_bis AS DATE)`

2.6 Numeric and Aggregate Functions:
    - `SUM(...)` → Fully supported standard aggregate.

2.7 Analytical and Window Functions:
    - None used in this block.

2.8 Set and Join Operations:
    - Implicit outer joins with `(+)` operator:
      * `fact.dwh_tarif_id_neu = t_new.dwh_tarif_id (+)` → `LEFT JOIN temp t_new ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id`
      * `fact.dwh_tarif_id_alt = t_old.dwh_tarif_id (+)` → `LEFT JOIN temp t_old ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id`
      * `fact.dwh_vertrag_id = d.dwh_vertrag_id (+)` → `LEFT JOIN dwh$ta_c_vertrag d ON fact.dwh_vertrag_id = d.dwh_vertrag_id` with additional boundary filters included in the `ON` clause to preserve the exact outer join semantics.

2.9 Row Limiting and Sampling:
    - None used.

2.10 Sequences:
     - None used.

2.11 MERGE Statements:
     - None used.

2.12 INSERT / UPDATE / DELETE:
     - `INSERT /*+ Append */ INTO dwh$ta_t_smart_kubi` → Standard `INSERT INTO dwh$ta_t_smart_kubi` in BigQuery (with hint removed).
     - Partition targeting (`partition(...)`): BigQuery does not use static partition references in DML statements. Instead, BQ uses standard table filters on partition columns. The partition filtering logic is already logically executed by `to_char(fact.gueltigkeitszeitpunkt, 'yyyymm') = to_char(l_monats_id)`.

2.13 DDL Constructs:
     - Dynamic `Truncate table` statement resolved to standard BQ SQL `TRUNCATE TABLE` statement.

2.14 PL/SQL:
     - The procedural anonymous block with variables, exception handling, and error logging is mapped to a BigQuery Scripting block using `DECLARE`, `BEGIN`, `EXCEPTION WHEN ERROR THEN`, `ROLLBACK TRANSACTION`, and system variables like `@@row_count`.

2.15 Unresolvable or Advisory Items:
     - `dwpa_util_skript.runstatement`, `dwpa_globals.k_alis_err_unknown`, and `dwpa_meldung.fehler` are custom enterprise DB utilities. These must be replaced with BigQuery-native statements or calls to migrated logging/telemetry procedures. We will represent them as mock procedure calls in the pseudocode.
     - Oracle optimization hints (`/*+ parallel(...) */`, `/*+ full(...) */`, `/*+ use_hash(...) */`) are stripped entirely.

---

Step 3: Conversion Strategy Summary
3.1 Overall Approach:
    - The PL/SQL block is converted to a BigQuery standard SQL scripting block (`DECLARE... BEGIN... EXCEPTION... END`).
    - The Oracle-specific `(+)` outer join markers are refactored into clean ANSI standard `LEFT OUTER JOIN` syntax.
    - All date conversion functions are converted to BigQuery equivalent format/parse functions with explicit type casting.
    - Row counting is captured natively using `@@row_count` after the DML operation.

3.2 Assumptions:
    - The variables `&1` and `&2` will be passed as script parameters (`@monats_id` and `@eintrags_nr`) during query execution or declared as script-level parameters.
    - The tables `dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh$ta_f_d1_twvv_tn`, and `dwh$ta_c_vertrag` exist in BigQuery under the same names (or relevant dataset paths).

3.3 Items Flagged for Human Review:
    - Custom enterprise package logging calls (`dwpa_meldung.fehler`).
    - Verifying partition column types on `dwh$ta_f_d1_twvv_tn` to ensure that querying via `FORMAT_DATE('%Y%m', ...)` triggers proper partition pruning.

---

2.16 MIGRATION DECISION MATRIX

| Statement / Oracle Construct | Target Option | Selected Target | Rejected Alternatives | Reason for Selection |
| :--- | :--- | :--- | :--- | :--- |
| Dynamic TRUNCATE call | Direct BigQuery SQL | `TRUNCATE TABLE` | Execute Immediate / Python wrapper | BigQuery natively supports `TRUNCATE TABLE` statements, eliminating the need for dynamic execution. |
| Outer Joins `(+)` | Direct BigQuery SQL | ANSI `LEFT OUTER JOIN` | Nested subqueries | ANSI join syntax is the most performant and standard-compliant layout for BigQuery. |
| PL/SQL Exception Handling | Direct BigQuery SQL | BQ Scripting `BEGIN...EXCEPTION` | Python wrapper | BigQuery Scripting supports robust, native transactional error handling blocks. |
| Oracle Logging/Helper Packages | Direct BigQuery SQL | Mock BQ CALL procedures | Python/UDFs | Custom logging should be ported to native BQ stored procedures (`CALL log_procedure()`). |

---

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: A single standard SQL scripting file containing variables, transaction controls, table operations, and exception handling blocks.

---

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | BigQuery Target Type | Conversion Rule / Expression | Warning / Note |
| :--- | :--- | :--- | :--- |
| `pls_integer` | `INT64` | Native mapping | None. |
| `NUMBER` (ID/Count) | `INT64` | `CAST(val AS INT64)` | Standard scaling; safe for integer identifiers. |
| `VARCHAR2` | `STRING` | Native mapping | No length limits apply in BigQuery. |
| `DATE` (time component) | `DATE` | `CAST(val AS DATE)` or `DATETIME` | Mapped to `DATE` where date-only calculations are verified. |

---

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: PL/SQL Anonymous block, implicit join operators (`(+)`), dynamic statements, database package call logging.
- **Unsupported Functions**: `ADD_MONTHS`, `TO_DATE` (with Oracle formats), `DECODE`, `NVL`, `SQL%ROWCOUNT`.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Target tables must reside in the target BigQuery environment.
- **Warnings**: Ensure partition columns are formatted correctly to support query pruning.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

---

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(value AS INT64)` |
| `ADD_MONTHS` | Direct-with-rewrite | `DATE_ADD(date, INTERVAL n MONTH)` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', value)` or `PARSE_DATE('%Y%m', value)` |
| `DECODE` | Direct-with-rewrite | `CASE WHEN condition THEN result ELSE alternative END` |
| `NVL` | Direct-with-rewrite | `COALESCE(value, default_value)` |
| `LTRIM / RTRIM` | Direct-with-rewrite | `TRIM(value)` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATE('%Y%m', date_val)` or `CAST(num AS STRING)` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | `SELECT ...` or logging tracking table insert |
| `(+)` Join Syntax | Direct-with-rewrite | Standard `LEFT OUTER JOIN` |
| `WHENEVER SQLERROR` | Direct-with-rewrite | Managed via orchestration tool or BQ script `EXCEPTION` blocks |

No package definitions were found in the source code; section 2.20 is omitted.

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Pseudocode representation of BigQuery SQL Scripting Block

DECLARE v_anzahl_ds INT64 DEFAULT 0; -- converted from pls_integer
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

-- Parse script run-time arguments
SET l_monats_id = CAST(@parameter_1 AS INT64); -- converted from to_number('&1')
SET EintragsNr = CAST(@parameter_2 AS INT64); -- converted from to_number('&2')

-- Process Month Boundary Date
-- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Start transactional execution
  BEGIN TRANSACTION;

  -- Truncate Target Table
  -- converted from dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')
  TRUNCATE TABLE dwh$ta_t_smart_kubi;

  -- Insert Logic
  -- Oracle optimization hints are stripped
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
      FROM dwh$vi_l_map_fa_tarif AS t
      INNER JOIN bl_d_tarif AS tar 
        ON t.tarif_id = tar.tarif_id
      WHERE CAST(t.gueltig_bis AS DATE) = DATE '4712-12-31' -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT 
      l_monats_id AS monats_id,
      -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
      CASE 
        WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
        ELSE d.t_mobile_kundennummer 
      END AS kundennummer,
      COALESCE(t_new.tarif_id, 0) AS tarif_id, -- converted from Nvl(t_new.tarif_id,0)
      COALESCE(t_old.tarif_id, 0) AS tarif_id_alt, -- converted from Nvl(t_old.tarif_id,0)
      -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END AS vo_kennung,
      d.test_gp, 
      SUM(fact.zugang) AS anzahl, 
      fact.kennzahl_id 
  FROM dwh$ta_f_d1_twvv_tn AS fact -- Partition filter is enforced globally via the WHERE clause
  -- converted from (+) outer joins to ANSI LEFT JOINs
  LEFT OUTER JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh$ta_c_vertrag AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    AND l_monats_date > CAST(d.gueltig_von AS DATE)
    AND l_monats_date <= CAST(d.gueltig_bis AS DATE)
  WHERE FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING) -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
      CASE 
        WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
        ELSE d.t_mobile_kundennummer 
      END, 
      COALESCE(t_new.tarif_id, 0), 
      COALESCE(t_old.tarif_id, 0), 
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END, 
      d.test_gp, 
      fact.kennzahl_id;

  -- Save Row Count
  SET v_anzahl_ds = @@row_count; -- converted from SQL%ROWCOUNT

  COMMIT TRANSACTION;

  -- Log process output
  -- converted from dbms_output.put_line
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS execution_log;

EXCEPTION WHEN ERROR THEN
  -- Exception Handler Block (converted from Oracle WHEN OTHERS block)
  ROLLBACK TRANSACTION;
  BEGIN
    DECLARE ErrText STRING;
    DECLARE ErrC INT64;
    DECLARE FehlerNr INT64;
    
    SET ErrText = @@error.message;
    SET ErrC = -1; -- Mock SQLCODE placeholder
    SET FehlerNr = -20001; -- Representing dwpa_globals.k_alis_err_unknown;

    -- Call custom enterprise logging routing
    -- converted from dwpa_meldung.fehler(...)
    CALL `project.dataset.dwpa_meldung_fehler`('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));
    
    -- Raise application-specific error
    ERROR(ErrText);
  END;
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic Statement Packaging (`dwpa_util_skript.runstatement`)**: Since dynamic execution is avoided by executing standard `TRUNCATE TABLE` directly, the dynamic SQL wrapper API should be deprecated during the migration process.
2. **Metadata & Custom Error Logging Packages**: The procedural exception block references `dwpa_globals` and `dwpa_meldung` objects. These must be created as user-defined functions (UDFs) or separate stored logging procedures in BigQuery before deploying this block.
3. **Implicit Partition Target**: The Oracle source referenced a distinct partition `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, this partition must be natively pruned. Ensure that the field `gueltigkeitszeitpunkt` is configured as the partition column in the physical BigQuery table schema.

### Execution order
The target orchestration (via Cloud Composer / Airflow) must preserve the execution order of the legacy dependency graph:
1. **Initialize Environment**: Set up run-time variables and context (equivalent to `.dw_init`).
2. **Execute Aggregation Logic**: Execute the BigQuery SQL script target file `kubi/d_abtn_x_smart_kubi.sql` (replacing the manual invocation sequence of `r_sqlscript`, `h_alis_sqlplus.ksh`, and `d_abtn_x_smart_kubi.sql`).
3. **Handle Errors/Logs**: If the SQL execution fails, trigger downstream failure processing and error reporting (replacing `f_alis_msgerr.ksh`).

### Schedule & variables
The scheduler-set variables must be dynamically calculated at run-time by the orchestration DAG (e.g., using Airflow macros and Jinja templating) and passed to the SQL script execution:
- `cdate` = `SYS_DATE("YYYYMMDD")` $\rightarrow$ Sourced via Airflow macro: `{{ ds_nodash }}`
- `cday` = `SUBSTR(&cdate,7,2)` $\rightarrow$ Sourced via Airflow Jinja: `{{ ds_nodash[6:8] }}`
- `cmonth` = `SUBSTR(&cdate,1,6)` $\rightarrow$ Sourced via Airflow Jinja: `{{ ds_nodash[0:6] }}`
- `first` = `'01'` $\rightarrow$ Static DAG parameter
- `MONATSID` = `&cmonth` (calculated via steps of subtracting 1 day from the first of the current month to get the prior month in `YYYYMM` format) $\rightarrow$ Sourced via Airflow Jinja: `{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y%m') }}`. This is passed directly as `@monats_id` to the BigQuery SQL Script.
- `DWH_JOB_KENNUNG` = `'ABTN_SMART_KUBI'` $\rightarrow$ Static DAG runtime parameter.

### Lineage
- **Upstream Producers (Read Tables)**:
  - `TABLE:DWH$TA_F_D1_TWVV_TN` (Source partitioned contract transaction fact table)
  - `TABLE:DWH$VI_L_MAP_FA_TARIF` (Tariff mapping view)
  - `TABLE:BL_D_TARIF` (Tariff dimension table)
  - `TABLE:DWH$TA_C_VERTRAG` (Contract status master table)
- **Downstream Consumers (Write Tables)**:
  - `TABLE:DWH$TA_T_SMART_KUBI` (Aggregated customer/tariff target table)

### Cross-file dependencies
- **Package Utilities**: The script depends on external database package APIs:
  - `PACKAGE:DWPA_UTIL_SKRIPT` (used for running the dynamic `Truncate table` statement) $\rightarrow$ Replaced natively by executing `TRUNCATE TABLE` directly in BigQuery SQL.
  - `PACKAGE:DWPA_MELDUNG` (used for tracking execution failure metadata) $\rightarrow$ Replaced by executing a standard `CALL` to a migrated logging stored procedure in BigQuery (`CALL bq_dataset.dwpa_meldung_fehler(...)`).

### Target file plan
- **Target File Path**: `kubi/d_abtn_x_smart_kubi.sql`
  - **Source File**: `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`
  - **Language**: SQL (BigQuery Standard SQL / Scripting)
  - **Purpose**: Direct translation of the PL/SQL database block. It truncates the target table, performs the CTE mapping query using ANSI left outer joins, commits the transaction, and maps procedural exception handling to BigQuery native blocks.

### Environment-specific values
Every environment-sourced value from the legacy scripts is classified below for appropriate target resolution:

1. **GLOBAL (Environment-wide)**:
   - `GCP_PROJECT`: Identifies the target Google Cloud project. Sourced at runtime from Airflow config via `Variable.get("GCP_PROJECT")`.
   - `BQ_DATASET`: The BigQuery dataset where the tables reside. Sourced at runtime via `Variable.get("BQ_DATASET")`.
   - **Resolution in SQL**: Referenced using standard query parameters or dynamic SQL injection (`@gcp_project.@bq_dataset.table_name`).

2. **JOB-SPECIFIC**:
   - `EintragsNr` (legacy parameter `&2`): Specific execution log tracking ID. Passed at runtime from Airflow context as `@eintrags_nr`.
   - `l_monats_id` (legacy parameter `&1`): The calculated target execution month ID. Passed at runtime as `@monats_id`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `kubi/d_abtn_x_smart_kubi.sql` | Fully converts the main PL/SQL logic, dynamic truncate statement, nested joins, and database logs into BigQuery Standard SQL scripting format. |

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
REASON: The script is a KornShell library containing utility functions for status logging, error trapping, and file name generation, which cannot be expressed in pure SQL and maps directly to a reusable Python module.

EVIDENCE
- Business logic found: KSH custom logic contains reusable utility functions for orchestrating ETL error logging, capturing system exit statuses, dynamic log file naming, and tracking database statuses.
- AWK: none
- SQL-expressible: No, because it defines shell functions, temporary files, dynamic system environment queries, dynamic shell evaluations (`eval`), and error-trap orchestration.
- Non-SQL side effects: Writes/deletes temporary files (`/tmp/ErmittleNr_$$.lst`), computes standard file paths, and implements shell trap mechanisms.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`f_alis_msgerr.ksh`) acts as a central KornShell library containing utility functions for job instrumentation, tracking, and error management within the "Information Services" environment. It provides APIs to initialize tracking entries, report warnings/errors, update job completion statuses, append date-bound execution telemetry, and construct structured log file paths. It is designed to be sourced by actual execution scripts which register these helpers in shell `trap` hooks to ensure system-level failures are logged automatically.

2. INVOCATION CONTEXT
   - Sourced or called by: Multiple ETL scripts across the environment. There is no direct execution context within a single UC4 Job; rather, it is sourced as a shell helper library.
   - UC4 Includes Referenced: None.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   The library functions accept several arguments, which should be mapped to Python parameters:
   - `DWMSG_Fehlerbehandlung`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
   - `DWMSG_SetzeStatusOK`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
   - `DWMSG_SetzeStatusAbbruch`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
   - `DWMSG_ErmittleNr`:
     - `$1` (`VarName`): Variable name used for out-parameter assignment. (Used in script: Yes -> Refactored to standard Python return value)
   - `DWMSG_ErzeugeEintrag`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
     - `$2` (`JobKennung`): ETL Job identifier. (Used in script: Yes -> Map to Python function argument)
     - `$3` (`Programmname`): Triggering script filename. (Used in script: Yes -> Map to Python function argument)
     - `$4` (`LogDatei`): Log file path. (Used in script: Yes -> Map to Python function argument)
   - `DWMSG_MeldeFehler`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
     - `$2` (`Typ`): Classification of the message (F/E/W). (Used in script: Yes -> Map to Python function argument)
     - `$3` (`FehlerNr`): Internal error/message code. (Used in script: Yes -> Map to Python function argument)
     - `$4` (`Zusatz1`): Optional contextual message argument 1. (Used in script: Yes -> Map to optional argument)
     - `$5` (`Zusatz2`): Optional contextual message argument 2. (Used in script: Yes -> Map to optional argument)
   - `DWMSG_Logdateiname`:
     - `$1` (`VarName`): Out-parameter for the dynamic log path string. (Used in script: Yes -> Refactored to standard Python return value)
     - `$2` (`JobKennung`): ETL Job identifier. (Used in script: Yes -> Map to Python function argument)
     - `$3` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
   - `DWMSG_SetzeStichtagInfo`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
     - `$2` (`DWMSG_Stichtag`): Target processing date. (Used in script: Yes -> Map to Python function argument)
     - `$3` (`DWMSG_StichtagFmt`): Format mask for processing date. (Used in script: Yes -> Map to Python function argument)
   - `DWMSG_AppendTimingInfos`:
     - `$1` (`DWMSG_EintragsNr`): Logging track entry ID. (Used in script: Yes -> Map to Python function argument)
     - `$2` (`DWMSG_InfoText`): Contextual text for runtime timing. (Used in script: Yes -> Map to Python function argument)
     - `$3` (`DWMSG_DateFormat`): Format mask for target system datetime string. (Used in script: Yes -> Map to Python function argument)

   Environment Variables used:
   - `DW_ORAUSER`: Database connection user string. Surface in Python as `os.environ.get("DW_ORAUSER")` or map to the modern GCP authentication context.
   - `DW_DIR_ROOT`: Root path of the environment installation. Surface in Python as `os.environ.get("DW_DIR_ROOT")`.
   - `DW_DIR_PROT`: Directory path where execution logs are stored. Surface in Python as `os.environ.get("DW_DIR_PROT")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusOk $DWMSG_EintragsNr </dev/null`
     - Purpose: Updates a track status to OK.
     - Target Recommendation: Native Python DB-Client (Google Cloud BigQuery library `google.cloud.bigquery`) executing the equivalent BigQuery SQL script or Stored Procedure.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p1.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

   - `sqlplus $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p1.sql BERT_MELDUNG.SetzeStatusAbbruch $DWMSG_EintragsNr </dev/null`
     - Purpose: Updates a track status to cancelled/aborted.
     - Target Recommendation: Native BigQuery python client statement.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p1.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_al_is_ermittlenr.sql "$TempFile" </dev/null`
     - Purpose: Extracts a unique sequence key and outputs it into a temporary text file.
     - Target Recommendation: Query the corresponding BigQuery sequence or unique ID generator function directly in Python, returning the value as a variable (eliminating file reads/writes).
     - # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_al_is_ermittlenr.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

   - `sqlplus -s $DW_ORAUSER @$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_spaufruf_p4.sql BERT_MELDUNG.Erzeuge_Eintrag ... </dev/null`
     - Purpose: Inserts a tracking entry into metadata database.
     - Target Recommendation: Call a native BigQuery stored procedure or construct a standard `INSERT` query.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p4.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

   - `sqlplus -s $DW_ORAUSER @$Dateipfad BERT_MELDUNG.Fehler ... </dev/null`
     - Purpose: Writes an error exception tracking entry.
     - Target Recommendation: Call native BigQuery procedures or SQL.
     - # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p[3-5].sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   There are two inline SQL blocks:

   1. SQL Block in `DWMSG_SetzeStichtagInfo`:
      ```sql
      EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
      commit;
      ```
      - Statement type: PL/SQL Procedure execution block.
      - Tables touched: Metadata/Tracking tables (internal to package `BERT_MELDUNG`).
      - Dialect: Oracle SQL*Plus.
      - Target BigQuery Migration:
        # REVIEW: PL/SQL block has no BigQuery equivalent — requires manual rewrite if the target platform is confirmed as BigQuery. Should translate to standard BigQuery scripting statement or Stored Procedure call:
        `CALL {{project_id}}.dataset.BERT_MELDUNG_SetzeZusatzInfos(dwmsg_eintrags_nr, PARSE_DATE(dwmsg_stichtag_fmt, dwmsg_stichtag))`

   2. SQL Block in `DWMSG_AppendTimingInfos`:
      ```sql
      EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
      commit;
      ```
      - Statement type: PL/SQL Procedure execution block.
      - Tables touched: Metadata/Tracking tables (internal to package `BERT_MELDUNG`).
      - Dialect: Oracle SQL*Plus.
      - Target BigQuery Migration:
        # REVIEW: PL/SQL block has no BigQuery equivalent — requires manual rewrite if the target platform is confirmed as BigQuery. BigQuery equivalent must translate string formatting and date operations to standard queries:
        `CALL {{project_id}}.dataset.BERT_MELDUNG_SetzeZusatzInfos(dwmsg_eintrags_nr, NULL, CONCAT(dwmsg_info_text, ' ', FORMAT_DATETIME(dwmsg_date_format, CURRENT_DATETIME()), ' '))`

6. CONTROL FLOW
   As a library script, execution is modular and split across specific functional definitions:

   - **`DWMSG_Fehlerbehandlung`**:
     1. Captures system exit code `$FehlerNr = $?`.
     2. Assigns tracking ID `DWMSG_EintragsNr = $1`.
     3. Calls internal `DWMSG_MeldeFehler` with code `10` (unexpected exception code).
     4. Sets execution status to Aborted by calling `DWMSG_SetzeStatusAbbruch`.

   - **`DWMSG_SetzeStatusOK`**:
     1. Validates that the entry ID `$1` is provided. If missing, prints standard error and exits with code 1.
     2. Calls database with package stored procedure `BERT_MELDUNG.SetzeStatusOk`.

   - **`DWMSG_SetzeStatusAbbruch`**:
     1. Validates that the entry ID `$1` is provided. If missing, prints standard error and exits with code 1.
     2. Calls database with package stored procedure `BERT_MELDUNG.SetzeStatusAbbruch`.

   - **`DWMSG_ErmittleNr`**:
     1. Validates that the return variable name `$1` is provided. If missing, prints standard error and exits with code 1.
     2. Sets temporary extraction path to `/tmp/ErmittleNr_$$.lst`.
     3. Executes SQL file `d_al_is_ermittlenr.sql` via database utility, routing outputs to the temp file.
     4. Reads the ID from the temp file, stripping empty whitespace characters.
     5. Deletes the temporary file.
     6. Returns value via dynamic shell variable definition (`eval`).

   - **`DWMSG_ErzeugeEintrag`**:
     1. Validates that the tracking ID `$1` is provided. If missing, prints standard error and exits with code 1.
     2. Calls database procedure `BERT_MELDUNG.Erzeuge_Eintrag` passing parameters.

   - **`DWMSG_MeldeFehler`**:
     1. Validates that the tracking ID `$1` is provided. If missing, prints standard error and exits with code 1.
     2. Quantifies parameters (3, 4, or 5 arguments) to dynamically build target parameter count for script selection (`d_alis_spaufruf_p*.sql`).
     3. Calls dynamic SQL routine to trigger procedure `BERT_MELDUNG.Fehler`.

   - **`DWMSG_Logdateiname`**:
     1. Extracts job details and entry status.
     2. Concatenates datetime mask `date '+%Y%m%d_%H%M'` and directory `DW_DIR_PROT` to compute target log filename.
     3. Writes name to dynamic shell out-parameter.

   - **`DWMSG_SetzeStichtagInfo`**:
     1. Validates tracking ID (exits 1 if missing).
     2. Validates target date parameter (exits 1 if missing).
     3. Validates date format (exits 2 if missing).
     4. Invokes SQL execution block on tracking table.

   - **`DWMSG_AppendTimingInfos`**:
     1. Validates tracking ID (exits 1 if missing).
     2. Validates format syntax string (exits 2 if missing).
     3. Formats current system timestamp and posts timings via SQL execution.

7. ERROR HANDLING & EXIT CODES
   - Library processes exit via direct `exit 1` or `exit 2` when positional parameter validations fail inside functions.
   - Database operations assume correct script execution without explicit exit-on-error statements; python migration will improve this by utilizing standard BigQuery Exception handling structures (`google.cloud.exceptions.GoogleCloudError`).
   - Standard shell error-trapping functions are simulated by Python `try ... except ... finally` architectures.

8. OUTPUTS / SIDE EFFECTS
   - Tracking table updates posted directly to the BigQuery tracking dataset.
   - Formatted execution log paths mapped to targeted log directories.
   - Slashes / spaces stripped in system values.

9. BUSINESS SUMMARY
   - Standardizes reporting pipelines, tracking overall data flow completion, durations, and health metrics.
   - Ensures any unforeseen program crash or shell pipeline error triggers a unified cleanup routine.
   - Enables auditing of SLA metrics by recording run timestamps.
   - Translates technical system error signals into high-level business warnings.

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
# Step 1: Set up imports, environments, and BigQuery client session
import os
import sys
import datetime
from google.cloud import bigquery

# Resolve environment variables
DW_DIR_PROT = os.environ.get("DW_DIR_PROT", "/tmp")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT", "/tmp")

# Initialize the GCP BigQuery client connection
bq_client = bigquery.Client()


# Step 2: Define unexpected error trap helper
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr: str, last_exit_code: int):
    """
    Simulates shell 'trap ... ERR' execution.
    """
    # Step 2.1: Define internal unexpected exception code
    k_unerw_fehler = 10
    
    # Step 2.2: Log internal error code to BigQuery
    dwmsg_melde_fehler(
        dwmsg_eintrags_nr, 
        "F", 
        k_unerw_fehler, 
        f"ErrorCode ist: {last_exit_code}"
    )
    
    # Step 2.3: Set aborted tracking status in BigQuery
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


# Step 3: Define OK status tracker function
def dwmsg_setze_status_ok(dwmsg_eintrags_nr: str):
    """
    Updates the log entry state to OK.
    """
    # Step 3.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 3.2: Perform BigQuery procedure execution
    # # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p1.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    query = "CALL `{{project_id}}.dataset.BERT_MELDUNG_SetzeStatusOk`(@eintrags_nr)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


# Step 4: Define ABORT status tracker function
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr: str):
    """
    Updates the log entry state to Aborted.
    """
    # Step 4.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 4.2: Perform BigQuery procedure execution
    # # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p1.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    query = "CALL `{{project_id}}.dataset.BERT_MELDUNG_SetzeStatusAbbruch`(@eintrags_nr)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


# Step 5: Define unique ID retriever function
def dwmsg_ermittle_nr() -> str:
    """
    Queries and returns a unique tracker run sequence number.
    """
    # # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
    
    # Step 5.1: Perform BigQuery sequence fetch
    # # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_al_is_ermittlenr.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    query = "SELECT `{{project_id}}.dataset.generate_tracking_nr`() AS eintrags_nr"
    query_job = bq_client.query(query)
    results = query_job.result()
    row = next(results)
    
    # Step 5.2: Strip and return sequence key
    return str(row["eintrags_nr"]).strip()


# Step 6: Define log entry constructor function
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr: str, job_kennung: str, programm_name: str, log_datei: str):
    """
    Inserts a newly generated job execution log entry.
    """
    # Step 6.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 6.2: Execute initialization call in BigQuery
    # # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p4.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG_Erzeuge_Eintrag`(@eintrags_nr, @job_kennung, @programm_name, @log_datei)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programm_name", "STRING", programm_name),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


# Step 7: Define exception reporter function
def dwmsg_melde_fehler(dwmsg_eintrags_nr: str, typ: str, fehler_nr: int, zusatz1: str = "", zusatz2: str = ""):
    """
    Records an error occurrence.
    """
    # Step 7.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 7.2: Perform logging execution call in BigQuery
    # # REVIEW-STRUCT: launcher [sqlplus] invoked with [d_alis_spaufruf_p_num.sql] — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG_Fehler`(@typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", fehler_nr),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


# Step 8: Define log directory file mapping function
def dwmsg_logdateiname(job_kennung: str, dwmsg_eintrags_nr: str) -> str:
    """
    Constructs a standardized execution log string path.
    """
    # Step 8.1: Query system runtime date
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    # Step 8.2: Build and return full file string path
    filename = f"{DW_DIR_PROT}/{job_kennung}_{now_str}_{dwmsg_eintrags_nr}.log"
    return filename


# Step 9: Define target business date logging function
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr: str, stichtag: str, stichtag_fmt: str):
    """
    Appends execution stichtag info using parsed datetime strings.
    """
    # Step 9.1: Audit validation checks
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Step 9.2: Call BigQuery target stored procedure
    # # REVIEW: PL/SQL block has no BigQuery equivalent — requires manual rewrite if the target platform is confirmed as BigQuery
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG_SetzeZusatzInfos`(@eintrags_nr, PARSE_DATE(@stichtag_fmt, @stichtag))
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag),
            bigquery.ScalarQueryParameter("stichtag_fmt", "STRING", stichtag_fmt)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


# Step 10: Define telemetry execution timers function
def dwmsg_append_timing_infos(dwmsg_eintrags_nr: str, info_text: str, date_format: str):
    """
    Saves timing/performance logs to tracking table.
    """
    # Step 10.1: Audit validation checks
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Step 10.2: Execute query mapping to post concatenated telemetry string values
    # # REVIEW: PL/SQL block has no BigQuery equivalent — requires manual rewrite if the target platform is confirmed as BigQuery
    query = """
        CALL `{{project_id}}.dataset.BERT_MELDUNG_SetzeZusatzInfos`(@eintrags_nr, NULL, CONCAT(@info_text, ' ', FORMAT_DATETIME(@date_format, CURRENT_DATETIME()), ' '))
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("info_text", "STRING", info_text),
            bigquery.ScalarQueryParameter("date_format", "STRING", date_format)
        ]
    )
    bq_client.query(query, job_config=job_config).result()
```

### Execution Order
The target orchestration (e.g., Cloud Composer DAG task ordering) must preserve this exact sequence of execution from the legacy dependency graph:
1. **DW.DWH_ABTN_SMART_KUBI.xml**: Trigger / orchestration entry point.
2. **d_abtn_x_smart_kubi.sql**: Runs the primary SMART_KUBI query or Dataform model.
3. **r_sqlscript**: Script execution wrapper.
4. **.dw_init**: Context initialization.
5. **f_alis_msgerr.ksh**: Reusable logging and error-management library sourced/invoked during execution.
6. **h_alis_sqlplus.ksh**: Execution helper for Oracle SQL*Plus database connection and query submission.

Since `f_alis_msgerr.ksh` is a helper library rather than a standalone sequential job, it will be migrated to `f_alis_msgerr.py` and imported by the Python tasks corresponding to the execution steps above, or invoked as helper methods during execution.

### Schedule & Variables
The scheduler-set variables must reach the migrated job at runtime. They are mapped from the scheduler context as follows:
- **`cdate`** (`SYS_DATE("YYYYMMDD")`): Sourced dynamically at runtime. In Cloud Composer (Airflow), this can be fetched using Airflow Jinja templates like `{{ ds_nodash }}` or via Python's `datetime` module.
- **`cday`** (`SUBSTR(&cdate,7,2)`): Substring extraction of `cdate` (days). Map to Airflow parameter or Python `datetime` extraction.
- **`cmonth`** (`SUBSTR(&cdate,1,6)`): Substring extraction of `cdate` (year and month). Map to Airflow parameter or Python `datetime` extraction.
- **`cmonth`** (`&cmonth&first`): Appends `first` (01) to `cmonth`. Map to Airflow parameter or Python datetime manipulation.
- **`cmonth`** (`SUB_DAYS(&cmonth,1)`): Subtracts 1 day from the constructed month-start date. Map to Airflow parameter or Python `datetime` manipulation.
- **`cmonth`** (`SUBSTR(&cmonth,1,6)`): Re-extracts year and month from the adjusted date. Map to Airflow parameter or Python `datetime` manipulation.
- **`DWH_JOB_KENNUNG`** (`'ABTN_SMART_KUBI'`): Job-specific string identifier. Map to Airflow task parameter.
- **`first`** (`'01'`): Constant value used for date arithmetic. Map to job-specific configuration.
- **`MONATSID`** (`&cmonth`): Resulting month identifier string. Map to job parameter.

These variables will reach the migrated Python job through an Airflow DAG context parameter dictionary, or as explicit Python function arguments.

### Lineage
The source library interacts with the following database dependencies:
- **`D_ALIS_SPAUFRUF_P1.SQL`**: Executed dynamically to update status (calls database procedure `BERT_MELDUNG.SetzeStatusOk` and `SetzeStatusAbbruch`). (Human-reviewed: NO SOURCE NEEDED).
- **`D_AL_IS_ERMITTLENR.SQL`**: Executed to query unique database tracking ID (calls sequence generator). (Human-reviewed: NO SOURCE NEEDED).
- **`D_ALIS_SPAUFRUF_P4.SQL`**: Executed to create tracking entry in metadata tables. (Human-reviewed: NO SOURCE NEEDED).
- **`PROCEDURE:SETZEZUSATZINFOS`**: DB procedure used for appending runtime telemetry/timing information.

### Cross-File Dependencies
The KSH helper library functions in `f_alis_msgerr.ksh` are dynamically loaded (sourced) by other shell scripts in this job group. Since the underlying SQL execution wrappers are confirmed as "NO SOURCE NEEDED" (human-reviewed as not needed), the migrated Python code in `f_alis_msgerr.py` should replace the execution of these shell SQL wrappers by directly calling native BigQuery stored procedures or writing back to metadata tracking tables using the Google Cloud BigQuery Python client API.

### Target File Plan
- **Target File**: `local/home/gurunathan_t/kubi/f_alis_msgerr.py`
  - **Language**: Python
  - **Source File**: `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh`
  - **Purpose**: Translates the KornShell error-logging, dynamic log file naming, sequence generation, and execution status-tracking utilities into a reusable Python module containing clean, structured functions.

### Environment-Specific Values
- **`GCP_PROJECT`** (GLOBAL)
  - *Legacy Source*: Contextually derived from `DW_ORAUSER` connection parameters.
  - *Sourcing*: `os.environ.get("GCP_PROJECT")` or standard BigQuery client authentication.
- **`BQ_DATASET`** (GLOBAL)
  - *Legacy Source*: Contextually derived from schema namespaces and `DW_ORAUSER` connection parameters.
  - *Sourcing*: `os.environ.get("BQ_DATASET")`.
- **`DW_DIR_ROOT`** (GLOBAL)
  - *Legacy Source*: `$DW_DIR_ROOT`
  - *Sourcing*: `os.environ.get("DW_DIR_ROOT")`.
- **`GCS_BUCKET`** (GLOBAL)
  - *Legacy Source*: `$DW_DIR_PROT` (Log directory)
  - *Sourcing*: `os.environ.get("GCS_BUCKET")` (replaces local disk paths with target cloud bucket paths for logs).

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.ksh` | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Migrated to a Python utility library to be imported by the job's main tasks, preserving logging, status tracking, and error handling functions. |

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
REASON: The script is a utility module that defines a reusable shell function (starteSQLSkript) with parameter validation, local file readability checks, and external command execution (sqlplus), which requires a Python implementation.

EVIDENCE
- Business logic found: KSH custom logic. The script does not contain tabular data transformations; instead, it defines a utility function to validate SQL script files and run them using SQL*Plus, checking return codes and calling a custom error-reporting tool.
- AWK: none
- SQL-expressible: no. The logic consists of filesystem accessibility checks (`[ ! -r $p_Skript ]`), shell parameter management, and calling external command-line executables.
- Non-SQL side effects: Local filesystem readability checks (`-r`), process invocation of `sqlplus` with stdin redirected from `/dev/null`, and execution of the custom `DWMSG_MeldeFehler` error logging tool.
- Against this verdict: One could argue for `NO_CONVERSION_REQUIRED` if this script is treated as a dead legacy utility library that will be completely replaced by modern cloud orchestration tools. However, because it contains custom error logging and parameter validation logic that must be preserved, converting it to a Python utility module is the correct path.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`h_alis_sqlplus.ksh`) is a reusable KornShell utility module containing helper routines for executing Oracle SQL*Plus scripts. Its primary function, `starteSQLSkript`, verifies that a specified SQL script file exists and is readable, prints execution details to the log, and runs the script using SQL*Plus with the credentials stored in the `DW_ORAUSER` environment variable. It also integrates with a custom enterprise error logging utility (`DWMSG_MeldeFehler`) to report missing parameters or unreadable files.

2. INVOCATION CONTEXT
   - Who calls this script: Sourced or executed by other parent KornShell scripts within the data warehouse orchestration flow (triggered via UC4/Automic jobs). No specific UC4 JOBS_UNIX object was provided in this extraction.
   - UC4 native includes: None referenced in the extraction.
   - Environment files sourced: None directly sourced inside this snippet.

3. PARAMETERS / INPUTS
   The function `starteSQLSkript` accepts the following positional parameters:
   - `$1` (typeset as `p_Eintragsnr`): Error entry number used for error reporting. Source: Passed by parent caller. Used in validation guards and when calling `DWMSG_MeldeFehler`.
   - `$2` (typeset as `p_Skript`): Path to the SQL script file to be executed. Source: Passed by parent caller. Used to verify file readability and as the target script for SQL*Plus.
   - `$*` (remaining arguments after `shift 2`): Any additional parameters required by the SQL script. Source: Passed by parent caller. Forwarded directly to SQL*Plus.
   
   Additionally, the following environment variables are referenced:
   - `DW_ORAUSER`: The Oracle database connection string / user details used to authenticate the SQL*Plus session.
     * Note: This is a DB-connection-style parameter. Since the confirmed target platform is BigQuery, this Oracle-specific variable will be superseded by Google Cloud authentication mechanisms (e.g., service account JSON keys or Application Default Credentials).

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWMSG_MeldeFehler`:
     * Exact command: `DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"` (and similar for error 201).
     * Purpose: Custom enterprise messaging and error logging tool to log failures in a centralized monitoring repository.
     * Conversion: Must remain an external process call via `subprocess` or be replaced by a standardized Python logging wrapper.
     * # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `sqlplus`:
     * Exact command: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
     * Purpose: Oracle command-line utility used to execute database scripts.
     * Conversion: Since the target platform is confirmed as `BIGQUERY`, direct execution via `sqlplus` is legacy. This should be rewritten to run the SQL file's contents using the Google Cloud BigQuery Python Client (`google.cloud.bigquery`). However, if a hybrid/migration phase requires running local Oracle SQL scripts, it can be kept as a `subprocess.run` execution of a SQL client.
     * Resolvable Launcher: Does not qualify as a Resolvable Launcher because the SQL script content itself is not embedded or supplied in this extraction, and `sqlplus` is a standard executable rather than a custom internal metadata-driven wrapper.

5. EMBEDDED SQL
   There is no embedded SQL inside this KornShell utility script. It only invokes external SQL script files via `sqlplus`.

6. CONTROL FLOW
   Execution steps of `starteSQLSkript` function:
   1. **Initialization**: Set local module metadata variables: `ModulName="alis_sqlplus"` and `ModulVersion="V1.1.3"`.
   2. **Parameter Capture**: Assign positional parameter `$1` to `p_Eintragsnr` and `$2` to `p_Skript`.
   3. **Shift Arguments**: Perform `shift 2` to remove the first two parameters, leaving only the SQL script arguments in `$*`.
   4. **Parameter Validation (Guard 1)**: Check if either `p_Eintragsnr` or `p_Skript` is empty. If empty:
      - Call `DWMSG_MeldeFehler` with error code `196` and description.
      - Return exit code `196`.
   5. **File Readability Validation (Guard 2)**: Check if the file path in `p_Skript` is readable using the `[ ! -r $p_Skript ]` test. If not readable:
      - Call `DWMSG_MeldeFehler` with error code `201`.
      - Return exit code `201`.
   6. **Logging**: Print informational messages containing the script name and the remaining forward-passed parameters (`$*`).
   7. **Disable Active Error Trapping**: Call `set +e` to prevent the shell from exiting immediately if `sqlplus` returns a non-zero exit code.
   8. **SQL Execution**: Execute `sqlplus` with database credentials, pointing to the script file, passing forward-passed parameters, and redirecting standard input from `/dev/null`.
   9. **Capture Exit Code**: Save the exit code of the SQL*Plus process in `errcode=$?`.
   10. **Re-enable Active Error Trapping**: Call `set -e`.
   11. **Return**: Propagate the captured `errcode` back to the caller.

7. ERROR HANDLING & EXIT CODES
   - The script detects failure by checking empty strings for required parameters, checking file read permissions (`-r`), and capturing the exit status (`$?`) of the `sqlplus` command.
   - On parameter validation failure, it calls `DWMSG_MeldeFehler` and returns `196`.
   - On file readability failure, it calls `DWMSG_MeldeFehler` and returns `201`.
   - The exit code of `sqlplus` is explicitly captured (with shell `set +e` safety scaffolding) and returned to the parent execution environment.
   - **Python Mapping**:
     * Implement file checks via `os.access(path, os.R_OK)`.
     * Use `try...except` blocks or capture `subprocess.CompletedProcess.returncode` without raising exceptions when executing the SQL engine, mirroring the `set +e` legacy mechanism.

8. OUTPUTS / SIDE EFFECTS
   - Logs output to stdout showing SQL*Plus settings and parameters.
   - Side effects: Database state changes resulting from the execution of the target SQL script (unspecified in this context).
   - Writes log entries through the external `DWMSG_MeldeFehler` program on failure.

9. BUSINESS SUMMARY
   - **Utility Wrapper**: Provides a standardized, robust, and safe wrapper function to execute database SQL scripts from shell orchestrations.
   - **Proactive Validation**: Prevents silent SQL*Plus failures by verifying that script files are physically accessible on the disk prior to execution.
   - **Standardized Error Reporting**: Ensures that missing parameters or missing scripts are cataloged consistently using the enterprise `DWMSG_MeldeFehler` log repository.
   - **Process Safety**: Prevents shell script aborts during SQL failures by capturing exit codes safely, allowing parent scripts to gracefully manage transaction rollbacks or retries.

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import subprocess
from pathlib import Path

# Module metadata constants
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction;
# confirm logging, error propagation, and credential handling before finalizing the conversion
def dwmsg_melde_fehler(eintragsnr: str, severity: str, error_code: int, message: str) -> None:
    try:
        cmd = ["DWMSG_MeldeFehler", str(eintragsnr), severity, str(error_code), message]
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)

def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Python equivalent of the starteSQLSkript shell function.
    Validates script existence and readability, then executes it.
    """
    # Step 1: Parameter Validation Guard
    # MANDATORY AUDIT: Checked for parameter validation guard
    if not p_eintragsnr or not p_skript:
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, error_msg)
        return 196

    # Step 2: File Accessibility Validation Guard
    skript_path = Path(p_skript)
    if not skript_path.is_file() or not os.access(skript_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, str(p_skript))
        return 201

    # Step 3: Informational Logging
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 4: Environment Credentials Fetching
    # # REVIEW: target database platform is confirmed as BIGQUERY. 
    # Standard SQLPlus is Oracle-specific. If migrating fully to BigQuery, 
    # this helper should utilize google.cloud.bigquery.Client to run SQL contents.
    # We preserve the legacy subprocess call structure below for backwards-compatibility.
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 5: Process Invocation with error-handling isolation (set +e equivalent)
    try:
        # Replicates: sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
        cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
        
        # stdin=subprocess.DEVNULL matches </dev/null redirection
        result = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=False)
        errcode = result.returncode
    except Exception as e:
        print(f"Exception encountered during sqlplus execution: {e}", file=sys.stderr)
        errcode = 1

    # Step 6: Return the exit status code (mimicking return $errcode)
    return errcode
```

### Execution Order
The target orchestration (e.g., Cloud Composer / Airflow DAG) must preserve the logical flow of the legacy execution order:
1. **`DW.DWH_ABTN_SMART_KUBI.xml`**: Orchestration and job definition configuration.
2. **`d_abtn_x_smart_kubi.sql`**: The primary SQL execution script containing the business transformation.
3. **`r_sqlscript`**: The runner/wrapper command structure.
4. **`.dw_init`**: Initialization environment settings.
5. **`f_alis_msgerr.ksh`**: Custom error-reporting routine.
6. **`h_alis_sqlplus.ksh`** (migrated to `h_alis_sqlplus.py`): The utility helper module executed to run the actual SQL files.

In the migrated target pipeline, `h_alis_sqlplus.py` should be imported or invoked by the Python operators within the Cloud Composer DAG to validate and execute the BigQuery SQL jobs.

---

### Schedule & Variables
The following scheduler-set variables must be retained and dynamically computed inside the target DAG/orchestrator, then passed down to the execution tasks:
* **`DWH_JOB_KENNUNG`** = `'ABTN_SMART_KUBI'` (Job identifier, constant).
* **`first`** = `'01'` (Constant).
* **`cdate`**: Current system execution date in `YYYYMMDD` format. In Airflow, this should be mapped to the DAG run execution date: `{{ ds_nodash }}`.
* **`cday`**: Derived from `cdate` as characters 7 and 8 (equivalent to `SUBSTR(&cdate,7,2)`).
* **`cmonth`**: Multi-step string manipulation representing the target processing month.
  * In legacy, this is computed sequentially as:
    1. Extract first 6 chars of `cdate`
    2. Append `first` ('01')
    3. Subtract 1 day (`SUB_DAYS(&cmonth,1)`)
    4. Extract first 6 chars again to get the previous month in `YYYYMM` format.
* **`MONATSID`**: Set to the computed `cmonth` value.

These variables must reach the migrated job using Airflow context params, environment variables, or as runtime parameters in the calling Python/BigQuery tasks.

---

### Target File Plan
* **Target File Path**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.py`
  * **Language**: Python 3
  * **Source File**: `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh`

---

### Environment-Specific Values
* **`DW_ORAUSER`** (GLOBAL): Legacy Oracle database connection/user details. On BigQuery, this is superseded by the environment-wide Google Cloud project and authentication context (e.g., GCP service account credentials configured via Composer or `GCP_PROJECT` env var).
* **`p_Eintragsnr`** (JOB-SPECIFIC): Function-level argument specifying the specific error log ID. This is managed locally in Python.
* **`p_Skript`** (JOB-SPECIFIC): Filepath pointing to the SQL file to be executed. This is managed as a localized input string parameter to the function.

---

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.ksh` | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Migrates legacy KornShell SQL*Plus execution helper containing file validation, logging, and error-catching wrapper logic into a reusable Python module. |

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
REASON: The script is a generic database utility launcher that performs command-line argument parsing, dynamic path resolution, and integrates with a custom metadata logging framework.

EVIDENCE
- Business logic found: KSH custom logic performs dynamic SQL file path resolution (checking priority folders `../sql`, `../mig`, and `.`), argument parsing via `getopts`, error logging registration via `DWMSG_*` utilities, and execution of a SQL script using a custom launcher.
- AWK: none
- SQL-expressible: No. The script is an orchestration utility meant to locate, set up, and launch external SQL files. Its control flow and filesystem interactions are not expressible as SQL.
- Non-SQL side effects: Directory path checks, writing to dynamic log files, environment signal trapping, and invoking external shell scripts/SQL procedures.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
This script (`r_sqlscript`) is a utility wrapper designed to locate, prepare, and execute dynamic SQL*Plus scripts within a data warehouse (DWH) context. It enforces structural logging, error trapping, and job registration through a suite of sourced `DWMSG` shell functions. Since the target database platform has been explicitly confirmed as **BigQuery**, the Python equivalent of this launcher should adapt its execution logic to run the resolved SQL scripts using the Google Cloud BigQuery client library, while preserving the wrapper's parameter-passing, error-tracking, and log-handling architecture.

### 2. INVOCATION CONTEXT
*   **Caller:** Typically invoked by UC4/Automic jobs (JOBS_UNIX) or other parent shell scripts.
*   **Command Line Syntax:** `r_sqlscript -f <sql_script_name> [-i <sql_parameters>] [-j <job_name>] [-v] [-h]`
*   **UC4 Includes:** None referenced directly in the script extraction.
*   **Environment Files Sourced:**
    *   `. $HOME/aktuell/.dw_init`
        *   # REVIEW-STRUCT: environment file $HOME/aktuell/.dw_init not supplied — variables it sets are unknown
    *   `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
        *   # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — logging and message handling functions (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`) are unknown
    *   `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
        *   # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — DB execution helper functions (e.g., `starteSQLSkript`) are unknown

### 3. PARAMETERS / INPUTS
*   `-f` (`p_sqlscript`): The filename of the SQL script to be executed. Required. Populated via `getopts`. Handled in Python via `argparse`.
*   `-i` (`p_sqlpar`): Optional arguments/parameters to pass to the SQL script. Populated via `getopts`. Handled in Python via `argparse`.
*   `-j` (`p_Job`): Optional job identifier used for log tracking (defaults to `DWH_KORR`). Populated via `getopts`. Handled in Python via `argparse`.
*   `-v`: Optional verbose flag (`p_Verbose` set to `1`). If active, displays the log file content on failure. Handled in Python via `argparse` as a boolean flag.
*   `-h`: Displays help/usage info.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
*   `starteSQLSkript`: External shell function loaded from `h_alis_sqlplus.ksh` used to run the resolved SQL script.
    *   **Verbatim invocation:** `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
    *   **Purpose:** Launches the SQL script passing metadata IDs and arguments.
    *   **Python Conversion Strategy:** 
        *   # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        *   Since the target platform is confirmed as **BigQuery**, instead of invoking an external Oracle SQL*Plus script, the Python launcher should read the resolved `.sql` file, parse any parameter substitutions from `p_sqlpar`, and execute the statements using the `google.cloud.bigquery.Client` library.

### 5. EMBEDDED SQL
No direct SQL statements are embedded in this wrapper script; it executes external SQL files passed dynamically via the `-f` flag.

### 6. CONTROL FLOW
1.  **Initialize Environment & Source Files:** Load `.dw_init`, `f_alis_msgerr.ksh`, and `h_alis_sqlplus.ksh`.
2.  **Initialize Variables:** Set default variables (`DW_EintragsNr=0`, `ErrNr=0`, `p_Verbose=0`).
3.  **Parse Arguments:** Loop through input flags (`-f`, `-i`, `-j`, `-v`, `-h`) using `getopts`. 
    *   Set `ErrNr=193` if a required parameter argument is missing.
    *   Set `ErrNr=192` if an unknown parameter is passed.
4.  **Error Check & Usage:** If `ErrNr != 0`, invoke logging via `DWMSG_MeldeFehler`, print usage info, and exit with `ErrNr`.
5.  **Change Working Directory:** Move execution context to the directory of the script (`cd $(dirname $0)`).
6.  **Path Resolution:** Locate the SQL script file `l_DBskript`:
    *   If `p_sqlscript` has no parent path (is in current directory `.`):
        *   Check if `../sql/{p_sqlscript}` exists. If so, use it.
        *   Else, check if `../mig/{p_sqlscript}` exists. If so, use it.
        *   Else, default to `{p_sqlscript}`.
    *   If `p_sqlscript` includes a relative/absolute path, use it directly.
7.  **Uncertain File Verification Step:**
    *   `UNCERTAIN`: The legacy code block:
        ```ksh
        if [  -f "$l_DBskript" ]
        then
            ErrNr=198 # Parameterwert unbekannt
            ErrArg="$p_Kuerzel"
        fi
        ```
        This check sets `ErrNr=198` if the resolved file *does* exist, which is logically counterintuitive (normally you error if it does *not* exist). Furthermore, `$p_Kuerzel` is unassigned. This is likely a legacy copy-paste bug. This check should be verified and corrected to ensure the script exists in Python.
8.  **Job Identifier Setup:** Convert `p_Job` (defaulting to `DWH_KORR`) to uppercase (`JobKennung`).
9.  **DWH Log Registration:**
    *   Call `DWMSG_ErmittleNr` to obtain a tracking ID (`DW_EintragsNr`).
    *   Call `DWMSG_Logdateiname` to generate the log path.
    *   Call `DWMSG_ErzeugeEintrag` to register the job run.
10. **Signal/Trap Registration:**
    *   Register traps for `INT` (interrupt) and `ERR` (errors) signals to invoke `DWMSG_Fehlerbehandlung` and dump the log file if verbose.
11. **Execution:** Call `starteSQLSkript` with tracking IDs and script parameters, redirecting all output to the generated log file.
12. **Success & Cleanup:** On success, set log status to OK via `DWMSG_SetzeStatusOK`, clear traps, and log completion message.

### 7. ERROR HANDLING & EXIT CODES
*   **Errors in Shell:** Handled via `set -e` and `trap`. If a command returns a non-zero status, the script exits and runs the trap function `DWMSG_Fehlerbehandlung`.
*   **Known Exit Codes:**
    *   `192`: Parameter unknown
    *   `193`: Necessary argument missing
    *   `198`: File path/abbreviation error
*   **Python Mapping:** Map shell traps to a try-except-finally block. If any step fails (e.g., file not found, BigQuery client error), raise an exception, write to the log, execute the equivalent of `DWMSG_Fehlerbehandlung`, and terminate with the corresponding exit code.

### 8. OUTPUTS / SIDE EFFECTS
*   **Standard Log File:** Output of the execution is captured in `$LogDatei`.
*   **Metadata DB Updates:** Job registration, tracking number allocation, and status updates via the `DWMSG` API.
*   **BigQuery Transformations:** Dynamic SQL statements executed against BigQuery tables.

### 9. BUSINESS SUMMARY
*   Provides a standardized, traceable utility launcher for executing SQL logic in the DWH.
*   Automatically manages directory path precedence (`../sql`, `../mig`, `.`) to simplify deployment across environments.
*   Ensures rigorous error logging and tracks execution status in a central job monitoring catalog.
*   Translates legacy SQL*Plus runs into BigQuery execution structures to enable cloud migration.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import sys
import os
import argparse
import subprocess

# # REVIEW-STRUCT: environment file $HOME/aktuell/.dw_init not supplied — variables it sets are unknown
# # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — functions like DWMSG_MeldeFehler are unknown
# # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — functions like starteSQLSkript are unknown

def usage():
    print("""
   Programm: Ausführung Script {sys.argv[0]}
   Version: 5.0.0
   Aufruf: {sys.argv[0]} Parameter

   Das als Parameter -f  übergebene SQL-Script wird ausgeführt.
   Es muß die Zeile "whenever sqlerror exit failure" enthalten,
   damit das Rahmenscript bei Fehlern abrricht.
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

# MANDATORY AUDIT: Checked for parameter-validation guards in functions.
# usage() has no parameters or validation guards.

def main():
    # Step 1: Initialize environment setup (equivalent of sourcing .dw_init etc.)
    # In practice, these environment initializations should load from os.environ or configs.
    dw_dir_root = os.environ.get("DW_DIR_ROOT", "")
    
    # Step 2: Initialize parameters & parse arguments
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-f', dest='p_sqlscript', required=False) # Marked required manually inside to handle exit codes precisely
    parser.add_argument('-i', dest='p_sqlpar', default="")
    parser.add_argument('-j', dest='p_Job', default="DWH_KORR")
    parser.add_argument('-v', dest='p_Verbose', action='store_true', default=False)
    parser.add_argument('-h', dest='p_Help', action='store_true', default=False)

    try:
        args, unknown = parser.parse_known_args()
    except Exception as e:
        # Step 3: Handle unknown arguments (ErrNr = 192)
        # In ksh, getopts sets ErrNr=192 for unknown parameters
        print(f"Error parsing arguments: {e}", file=sys.stderr)
        # Mocking DWMSG_MeldeFehler call
        # DWMSG_MeldeFehler(0, "E", 192, str(e))
        usage()
        sys.exit(192)

    if args.p_Help:
        usage()
        sys.exit(0)

    if not args.p_sqlscript:
        # Step 3b: Handle missing required parameter (ErrNr = 193)
        # DWMSG_MeldeFehler(0, "E", 193, "-f")
        usage()
        sys.exit(193)

    p_sqlscript = args.p_sqlscript.lower() # typeset -l p_sqlscript
    p_sqlpar = args.p_sqlpar
    p_Job = args.p_Job
    p_Verbose = args.p_Verbose

    # Step 4: Change directory to script's own path
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(script_dir)

    # Step 5: Resolve path of SQL script (l_DBskript)
    sql_dirname = os.path.dirname(p_sqlscript)
    l_DBskript = ""
    
    if sql_dirname == "" or sql_dirname == ".":
        test_path1 = os.path.join("..", "sql", p_sqlscript)
        test_path2 = os.path.join("..", "mig", p_sqlscript)
        
        if os.path.isfile(test_path1):
            l_DBskript = test_path1
        elif os.path.isfile(test_path2):
            l_DBskript = test_path2
        else:
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # Step 6: UNCERTAIN: Legacy file exists check (ErrNr = 198)
    # The original script does: if [ -f "$l_DBskript" ]; then ErrNr=198; ErrArg="$p_Kuerzel"; fi
    # This seems like an inverse logic bug. We preserve the structure but suggest validation:
    if os.path.isfile(l_DBskript):
        # # REVIEW: Legacy script threw Error 198 when the file was found. Confirm if this logic should be inverted (i.e. error when NOT found).
        p_Kuerzel = os.environ.get("p_Kuerzel", "")
        # DWMSG_MeldeFehler(0, "E", 198, p_Kuerzel)
        pass

    # Step 7: Format Job identifier
    job_kennung = p_Job.upper() # typeset -u JobKennung

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 8: Initialize logging and metadata identifiers
    # Mocking DWMSG metadata logging sequence:
    # dw_eintrags_nr = DWMSG_ErmittleNr()
    dw_eintrags_nr = "MOCK_DW_EINTRAGS_NR"
    # log_datei = DWMSG_Logdateiname(job_kennung, dw_eintrags_nr)
    log_datei = f"{job_kennung}_{dw_eintrags_nr}.log"

    # DWMSG_ErzeugeEintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)
    print(f"Logging to: {log_datei}")

    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{dw_eintrags_nr}'")
    print(f"Logdatei  : '{log_datei}'")
    print("---------------------------------------------")

    # Step 9: Execute Dynamic SQL Job with Error Handling
    try:
        # Open Log file to capture stdout/stderr
        with open(log_datei, "a") as log_file:
            log_file.write(f"Executing: {l_DBskript} with params {p_sqlpar}\n")
            
            # Step 10: Run the SQL execution wrapper
            # Since confirmed platform is BIGQUERY, the Python launcher should ideally execute SQL script via BQ client.
            # # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
            # Mocking starteSQLSkript behavior (e.g. running BQ client under the hood or calling external subprocess if not migrated):
            
            # Example Subprocess execution of the legacy launcher:
            # subprocess.run(["starteSQLSkript", dw_eintrags_nr, l_DBskript, p_sqlpar, dw_eintrags_nr], stdout=log_file, stderr=log_file, check=True)
            
            # BigQuery implementation alternative:
            # from google.cloud import bigquery
            # client = bigquery.Client()
            # with open(l_DBskript, 'r') as query_file:
            #     query_text = query_file.read()
            # # execute query_text substituting parameters from p_sqlpar...
            
            pass # Placeholder for actual run execution
            
        # Step 11: Register successful execution status
        # DWMSG_SetzeStatusOK(dw_eintrags_nr)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        
    except Exception as e:
        # Step 12: Exception Handling (Traps / DWMSG_Fehlerbehandlung equivalent)
        error_msg = f"Error during execution: {str(e)}"
        print(error_msg, file=sys.stderr)
        
        # Write to log
        try:
            with open(log_datei, "a") as log_file:
                log_file.write(f"!FEHLER gemeldet!\n{error_msg}\n")
        except:
            pass
            
        # DWMSG_Fehlerbehandlung(dw_eintrags_nr)
        if p_Verbose:
            # If verbose flag was set, dump the log file content directly to stderr
            try:
                with open(log_datei, "r") as log_file:
                    print(log_file.read(), file=sys.stderr)
            except Exception as read_err:
                print(f"Could not print log file: {read_err}", file=sys.stderr)
                
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### Execution Order
The legacy execution sequence consists of 6 steps. The target orchestration (such as a Cloud Composer / Airflow DAG) must preserve this order of operations:
1. **DW.DWH_ABTN_SMART_KUBI.xml** $\rightarrow$ Mapped to the main Airflow DAG schedule and orchestration layer.
2. **d_abtn_x_smart_kubi.sql** $\rightarrow$ Sourced SQL execution script containing database transformation logic (to be run via BigQuery).
3. **r_sqlscript** $\rightarrow$ The active KornShell wrapper script, mapped to the execution of `local/home/gurunathan_t/kubi/r_sqlscript.py`.
4. **.dw_init** $\rightarrow$ Shell initialization file, mapped to Airflow global/environment configuration loads.
5. **f_alis_msgerr.ksh** $\rightarrow$ Message and logging utility initialization.
6. **h_alis_sqlplus.ksh** $\rightarrow$ SQL*Plus execution helper loading.

---

### Schedule & Variables — Must Be Retained
The migrated job must receive and process the following scheduler-set variables. In the BigQuery/Cloud Composer target environment, these should be calculated dynamically using Airflow macros and passed directly to the execution task:
*   **cdate**: Sourced as `SYS_DATE("YYYYMMDD")`. In Airflow, this maps to `{{ ds_nodash }}`.
*   **cday**: Extracted substring from characters 7-8 of `cdate`. In Airflow, this maps to `{{ execution_date.strftime('%d') }}`.
*   **first**: Set as constant `'01'`.
*   **cmonth**: Extracted substring from characters 1-6 of `cdate` representing the previous month (by subtracting 1 day from the first of the month). In Airflow, this is computed as:
    `{{ (execution_date.replace(day=1) - macros.datetime.timedelta(days=1)).strftime('%Y%m') }}`
*   **DWH_JOB_KENNUNG**: Constant string `'ABTN_SMART_KUBI'`.
*   **MONATSID**: Evaluates to the resolved `cmonth` value (YYYYMM of the previous month).

---

### Lineage
The lineage edges represent structural dependencies on configuration files and legacy helper libraries:
*   `local/home/gurunathan_t/kubi/r_sqlscript` $\rightarrow$ Sources config/environment script `local/home/gurunathan_t/kubi/.dw_init`.
*   `local/home/gurunathan_t/kubi/r_sqlscript` $\rightarrow$ Sources logging library `FILE:f_alis_msgerr.ksh`.
*   `local/home/gurunathan_t/kubi/r_sqlscript` $\rightarrow$ Sources database utility execution script `FILE:h_alis_sqlplus.ksh`.

---

### Target File Plan
The target layout mirrors the original repository folder structure to maintain architectural integrity:
*   **Target File Path**: `local/home/gurunathan_t/kubi/r_sqlscript.py`
    *   **Language**: Python
    *   **Source File**: `local/home/gurunathan_t/kubi/r_sqlscript`

---

### Environment-Specific Values
*   **GLOBAL (Environment-Wide)**
    *   `DW_DIR_ROOT`: Sourced at runtime via `os.environ.get("DW_DIR_ROOT")`. Maps to the base directory of the DWH code repository.
    *   `HOME`: Sourced at runtime via `os.environ.get("HOME")` to reference current user context paths.
*   **JOB-SPECIFIC**
    *   `JobKennung` / `p_Job`: Passed as task/command argument `-j` (defaults to `"DWH_KORR"`).
    *   `DW_EintragsNr`: Generated execution ID tracked in monitoring schemas.
    *   `LogDatei`: Structured logs directory path dynamically resolved as `f"{JobKennung}_{DW_EintragsNr}.log"`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/r_sqlscript` | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Converted from KSH SQL wrapper to a Python utility running SQL files on BigQuery via the Google Cloud Client Library. |

---

### Risks & Manual Actions
*   **Potential Legacy Logic Error (Inverse File Check)**: The legacy script contains a block `if [ -f "$l_DBskript" ] then ErrNr=198`. This sets an error code if the SQL script file *does* exist, and `$p_Kuerzel` is unassigned. This is likely an inverse copy-paste bug from the legacy code. The Python implementation should correct this logic to verify that the file exists and throw an error only if the script is missing.
*   **Sourced Utility Dependencies**: The script relies on externalized wrapper frameworks (`f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh`). Because these are not inside the source file list for this pass, the Python wrapper should leverage standard Python logging and BigQuery library calls to handle the execution and error logging independently, or utilize migrated shared library equivalents.