=== OBJECT: DW.BERT_AUSD_V_TA_P_VERTRAG (JOBS_UNIX) ===
active=1
title=Update contract information regarding twin-bill
login=DW.UNIX.ISBERT
host=|DWHDWH1P|HOST
ert_seconds=382
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='AUSD_V_TA_P_VERTRAG'
. $HOME/.dw_init
&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
:inc DW.BERT_LESE_LOG
operational_notes=

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 Workload Migration Design Document: DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Overview
UNCERTAIN: This extraction bundle contains a single standalone UC4 Unix Job (`DW.BERT_AUSD_V_TA_P_VERTRAG`) without an accompanying parent Job Plan (`JOBP`) or triggering script (`SCRI`). Based on its metadata and script contents, this job updates contract information regarding twin-billing (twin-bill) within the DWH environment by executing a shell script (`r_ausd_v_ta_p_vertrag.ksh`). Because no orchestration context or schedule was provided, this job is represented as a single-task synthetic Airflow DAG, assumed to be externally triggered.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.BERT_AUSD_V_TA_P_VERTRAG` | JOBS_UNIX | 1 | Update contract information regarding twin-bill |

## 3. Scheduling
- **Trigger Source**: Externally triggered (source unknown from this extraction alone). No `EVNT_TIME` or schedule wrapper is present in this bundle.
- **Schedule**: `schedule=None` (No calendar or cron schedule can be assumed).

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_bert_ausd_v_ta_p_vertrag` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dw_bert_ausd_v_ta_p_vertrag_task` | `DW.BERT_AUSD_V_TA_P_VERTRAG` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | False | N/A | #REVIEW-STRUCT: launcher command `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` not recognised — confirm target operator/script manually. |

## 6. Task Dependency Map
```
[dw_bert_ausd_v_ta_p_vertrag_task]
```
*(Single-task workflow; no upstream/downstream task dependencies exist within this extraction)*

## 7. Sync / Concurrency Analysis
No `sync_rows` or cross-workflow locks were specified for this object. The DAG-level `max_active_runs=1` is sufficient to prevent concurrent execution of this single job.

## 8. Error Handling and Retry Strategy
- **Retries**: Configured with a default of 1 retry, with a 5-minute delay.
- **Triggers**: Execution defaults to standard `TriggerRule.ALL_SUCCESS`.
- No postcondition actions, execution alerts, or runtime window constraints were specified.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'AUSD_V_TA_P_VERTRAG'` | `Variable.get("dwh_job_kennung", default_var="AUSD_V_TA_P_VERTRAG")` or environment variable. |
| `&HOME` | Environment/System variable | `Variable.get("home_path")` or system-level env var. |

## 10. Developer Notes
- **UNCERTAIN: Standalone Object Wrapper**: Since no `JOBP` workflow was provided in the extraction, this single task has been wrapped in a dedicated synthetic DAG (`dw_bert_ausd_v_ta_p_vertrag`).
- **#REVIEW-STRUCT: Unrecognized Launcher Script**: The original command `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` is classified as `unrecognized`. If this shell script is migrated to run on a GKE executor, a standard VM runner, or as a container, the task operator must be updated (e.g., to `BashOperator`, `SSHOperator`, or `GKEStartPodOperator`) to execute this script in the target environment.
- **Environment Initialization**: The original script sources `. $HOME/.dw_init` and includes header configurations (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`). Ensure equivalent environment configuration is handled via Airflow variables, connection configurations, or container entrypoints.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# #REVIEW-STRUCT: If converting to Bash execution, import BashOperator:
# from airflow.operators.bash import BashOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP resources or GCS scripts are identified for this unrecognized launcher.
# If migrating script execution to Google Cloud (e.g., Dataproc or GKE),
# establish connection details here.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error handlers or notification hooks were specified in the source.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_bert_ausd_v_ta_p_vertrag',
    default_args=DEFAULT_ARGS,
    description='Update contract information regarding twin-bill',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task (none) ────────────────────────────────
    # ── Sensor Task (none) ───────────────────────────────
    # ── Calendar Check Task (none) ───────────────────────

    # ── Task: dw_bert_ausd_v_ta_p_vertrag_task ───────────
    # #REVIEW-STRUCT: The launcher type was unrecognized. An EmptyOperator is placed 
    # as a stub. Replace with BashOperator or SSHOperator once host infrastructure is defined.
    dw_bert_ausd_v_ta_p_vertrag_task = EmptyOperator(
        task_id='dw_bert_ausd_v_ta_p_vertrag_task',
        # Example BashOperator implementation for reference:
        # bash_command="source $HOME/.dw_init && $HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh",
        # env={'DWH_JOB_KENNUNG': 'AUSD_V_TA_P_VERTRAG'},
    )

    # ── Dependencies ─────────────────────────────────────────
    # No dependencies: Standalone task execution.
    dw_bert_ausd_v_ta_p_vertrag_task
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml` | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/dw_bert_ausd_v_ta_p_vertrag.py` | Converts the UC4 Job definition into a Cloud Composer Airflow DAG to manage orchestration of the contract information update. |

***

### Job Dependencies
* **Upstream Components (GCP Wiring)**:
  * `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711`: Already migrated and merged (PR: #844). BigQuery target dataset structures/fixes must exist prior to executing this workload.
  * `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`: Already migrated and merged (PR: #846). Converted logging and handling utilities are shared across execution modules.
  * `vobs/dw_source/istools/seu/template`: Already migrated and merged (PR: #845). Contains environment template files (e.g., `.dw_init` and `.dw_global`) that establish global settings.

### Execution Order
The execution order defined in the legacy dependency graph is preserved as follows:
1. **UC4 Job Wrapper (This Design Pass)**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml` maps to the Airflow DAG `dw_bert_ausd_v_ta_p_vertrag.py`.
2. **KSH Control Script**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` maps to a Python module or operator in its own separate design pass.
3. **KSH Core Alignment Script**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` maps to a Python module or operator in its own separate design pass.
4. **SQL Transformation Script**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` maps to a Dataform SQLX pipeline in its own separate design pass.

The Cloud Composer DAG defined here orchestrates this sequence by invoking tasks corresponding to steps 2, 3, and 4 in order.

### Scheduling
* **Trigger Source**: This job has no direct standalone schedule in the legacy environment. It is executed as an include/shared module within larger workflow plans.
* **Airflow Implementation**: The migrated Airflow DAG is defined with `schedule=None` (or `schedule_interval=None`), enabling it to be triggered dynamically via `TriggerDagRunOperator` from parent DAGs or through manual ad-hoc invocation.

### Schedule & Variables
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG = 'AUSD_V_TA_P_VERTRAG'`: Must be retained on BigQuery / Composer. This job-specific variable is passed to the execution environment as a DAG parameter or task environment variable (e.g., `params={'DWH_JOB_KENNUNG': 'AUSD_V_TA_P_VERTRAG'}`).

### Lineage
* **Upstream Inputs**:
  * Environment initialization file `vobs/dw_source/istools/seu/template/.dw_init` (invoked at startup).
  * Include files `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`. (Both are human-confirmed as "no source needed", representing legacy pathing and log parsing macros which are replaced by native Airflow logging and environment management).
* **Downstream Outputs**:
  * Executed target script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`.
  * Writes to entity `TABLE:CONTRACT` (updated downstream in the pipeline sequence).

### External System Replacements
* **Legacy Execution Host (`EXT:dwhdwh1p`)**: Sourced on UNIX isbert. Replaced by a Cloud Composer GKE executor pod running Python in the target Google Cloud Platform environment.
* **Oracle Database**: Replaced by BigQuery and Dataform.

### Cross-File Dependencies
* **Sourcing of Environment Configurations**: Sourcing `.dw_init` from template folders is replaced in Airflow by global configurations (Airflow Variables/Connections) and native task environment payloads.
* **Sibling Components**: This DAG acts as the parent controller for components `r_ausd_v_ta_p_vertrag.ksh`, `k_ausd_v_ta_p_vertrag.ksh`, and `d_ausd_v_ta_p_vertrag.sql`. These are distinct files living in separate legacy directories (`bin/` and `sql/` folders under `aufbereitung/`) and are owned and converted by separate migration passes.

### Target File Plan
* **Target File Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/dw_bert_ausd_v_ta_p_vertrag.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml`
  * **Purpose**: Orchestrate execution sequence of the contract alignment pipeline steps.

### Environment-Specific Values
* **GLOBAL**:
  * `&HOME` / `$HOME`: Root workspace directory. Sourced at runtime using `Variable.get("WORKSPACE_ROOT")` or fallback GKE container environment configurations.
* **JOB-SPECIFIC**:
  * `DWH_JOB_KENNUNG` (value: `'AUSD_V_TA_P_VERTRAG'`): Provided to task execution blocks via the Airflow operator `env` attribute or `params` dictionary.

### Risks and Manual Steps
* **Cross-Pass Wiring Dependency**: The Airflow DAG defined in this pass triggers sibling components (the converted Python versions of `r_ausd_v_ta_p_vertrag.ksh`, `k_ausd_v_ta_p_vertrag.ksh`, and `d_ausd_v_ta_p_vertrag.sql`) which are migrated under different design passes. Once those sibling passes are complete, the task operators must be updated to refer to their final target Python modules, Airflow task operators, or Dataform compile runs.
* **Legacy Environment Sourcing (`.dw_init`)**: Verify that environment configurations, settings, or aliases previously established within the legacy `.dw_init` and `.dw_global` scripts are mapped to equivalent Cloud Composer Airflow Variables or GKE environment settings to prevent missing runtime dependencies.

---

=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh ===
#!/bin/ksh

#
#
# Kontrollscript zu r_ausd_vertrag.ksh
# Autor:    Fabian Debus
# Erstellt: 20.12.2007
#
# Zweck:  Kontrollscript zu r_ausd_vertrag.ksh
#         a) aktive Jobs werden ignoriert
#         b) Aufruf SQL-Skript und Eintrag in die
#            Job-Tabelle
#         c) alte aktive Jobs werden einfach dekativiert
#
#####################
# Vorbereitende Massnahmen
#    Einlesen der Umgebung
. $HOME/.dw_init

# Fehlerkonzept einschalten
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh
# Hilfsskript fuer FOS-Jobverwaltung
#AL?? . ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_job.ksh   
# Hilfsskript fuer Datumscheck
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh   
# Hilfsskripte zum Parsen der Parameter
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh

# Lesen der Parameter
ParamList="j:f:" # Notation gemaess getopts(1)

# lese mit Hilfe getopts die Parameter
while getopts ":h$ParamList" param
do
    case $param in
        j)
            p_JobKennung=$OPTARG;;
        f)
            p_EintragsNr=$OPTARG;;
        h)
            print "Bitte ueber Rahmenscript aufrufen";
            exit;;
        :)
            ErrNr=193  # Notwendiges Argument fehlt
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unbekannt
            ErrArg="$OPTARG";;
    esac
done

# setze Tabellenname
v_TabName='ta_p_vertrag'

# Pruefe, ob notwendige Parameter gesetzt worden sind
# Abruchskontrolle ausschalten
    set +e

    ErrNr=0
    ErrArg=""

    pruefeParameterGesetzt Jobkennung p_JobKennung
    pruefeParameterGesetzt EintragsNr p_EintragsNr

    # Falls Fehler aufgetreten, abbrechen
    if [ ! $ErrNr -eq 0 ]
    then
	#Ausgabe gemaess Fehlerkonzept
	DWMSG_MeldeFehler 0 E $ErrNr "$ErrArg"
	echo "FEHLER: 0 E $ErrNr $ErrArg"
        print "Bitte ueber Rahmenscript aufrufen";
	#Austieg gemaess Nummernkreisen
	exit $ErrNr
    fi

# Abruchskontrolle einschalten
set -eu

# Routinen fuer SQL-Skript
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh

# SQL-Skript
Name_SQLskript="${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql"

# Temporares File fuer die Zahl der Records
tmpFile="$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp"

# *******************************************************

# DB-Script ausfuehren
# hierbei werden aktive Jobs ignoriert 
starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung 

print " ---------- ENDE Datenverarbeitung ----------"

# Hole Zahl der Bereitgestellten Records
eval "v_records=`cat $tmpFile`"





=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script uses getopts for parameter parsing, performs parameter validation, sources multiple external utility scripts, and reads from a temporary file.

EVIDENCE
- Business logic found: KSH custom logic parses and validates input arguments, coordinates the execution of an external SQL script via custom launchers, and extracts process details (record counts) from a temporary file.
- AWK: none
- SQL-expressible: No. While it orchestrates an SQL script execution, the orchestration itself involves dynamic parameter parsing, validation rules, custom error logging scripts, and file system I/O.
- Non-SQL side effects: Sourcing external shell utility scripts, reading from local temporary files, and using custom system exit codes.
- Against this verdict: BQSQL could be proposed if the SQL script itself was the only focus, but the wrapper contains necessary orchestration logic, validations, and file-based state checks that SQL cannot handle natively.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `k_ausd_v_ta_p_vertrag.ksh` is a control script for processing contract data (`ta_p_vertrag`). It initializes runtime parameters, validates command line arguments (Jobkennung and EintragsNr), sources utility scripts for error handling and database actions, and executes an external Oracle SQL*Plus script (`d_ausd_v_ta_p_vertrag.sql`). Following database execution, it extracts processed record count statistics from a generated temporary file.

2. INVOCATION CONTEXT
   - Who calls this script: Typically executed as a UC4/Automic Job (JOBS_UNIX) with parameters `-j <JobKennung>` and `-f <EintragsNr>`.
   - UC4 Native Includes: None referenced in the extraction.
   - Environment files sourced:
     - `. $HOME/.dw_init`
       # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
       # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
       # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
       # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
       # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `j` (positional parsed via `getopts` into `p_JobKennung`): Job Identifier. Required. Used in database execution setup. Surfaced in Python via `argparse` as `-j` / `--job-kennung`.
   - `f` (positional parsed via `getopts` into `p_EintragsNr`): Entry Number. Required. Used in database execution setup. Surfaced in Python via `argparse` as `-f` / `--eintrags-nr`.
   - Environment variables used:
     - `BERT_DIR_ROOT`: Base installation path for BERT module. Maps to `os.environ.get("BERT_DIR_ROOT")`.
     - `DW_DIR_UTL`: Directory path for utility/temporary files. Maps to `os.environ.get("DW_DIR_UTL")`.
     - `$$`: Sourced via shell for Process ID. Maps to `os.getpid()` in Python.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
     - Exact Command Line: `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
     - Purpose: Invokes SQL*Plus to execute the DB transformation script.
     - Target: Treated as an external subprocess invocation.
     - Resolvability: Not a resolvable launcher because the SQL script content is not supplied and database credentials/platform details are not declared.
     # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - `cat $tmpFile`
     - Exact Command Line: `cat $tmpFile` (evaluated via eval)
     - Purpose: Reads the contents of the temporary file containing record count statistics.
     - Target: Native Python file read.

5. EMBEDDED SQL
   - No embedded SQL is defined within this shell script. The SQL script is external: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`.

6. CONTROL FLOW
   1. **Environment Setup**: Sourced `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, and `h_alis_parameter.ksh`.
   2. **Argument Parsing**: Iterates through arguments via `getopts` matching `-j`, `-f`, and `-h`.
      - Maps option `j` to `p_JobKennung` and `f` to `p_EintragsNr`.
      - Maps option `h` to usage help print and exits immediately.
      - Sets `ErrNr=193` for missing arguments and `ErrNr=192` for unknown arguments.
   3. **Constant Variable Assignment**: Sets table name `v_TabName='ta_p_vertrag'`.
   4. **Parameter Validation**: 
      - Temporarily disables "exit on error" (`set +e`).
      - Validates if `p_JobKennung` and `p_EintragsNr` are set (using functions from `h_alis_parameter.ksh`).
      - If validation fails (`ErrNr != 0`), it logs the error using `DWMSG_MeldeFehler`, prints "Bitte ueber Rahmenscript aufrufen", and exits with the corresponding `ErrNr`.
   5. **Environment Configuration**: Re-enables strict exit-on-error and unset-variable checks (`set -eu`).
   6. **Source SQL Utilities**: Sourced `h_alis_sqlplus.ksh`.
   7. **Path Assembly**: Defines paths for `Name_SQLskript` and `tmpFile` (incorporating current Process ID).
   8. **DB Execution**: Invokes `starteSQLSkript` with `p_EintragsNr`, `Name_SQLskript`, and `p_JobKennung` parameters.
   9. **Console Logging**: Prints " ---------- ENDE Datenverarbeitung ----------".
   10. **File Processing**: Reads the record counts from `tmpFile` and assigns them to `v_records`.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments trigger exit codes `193` or `192` (via validation failure path).
   - If a step fails during SQL execution, the error propagates from `starteSQLSkript` due to `set -e`.
   - Python mapping: 
     - Argument validation issues should raise `sys.exit(193)` or `sys.exit(192)`.
     - Standard exceptions during database execution should trigger exit code 1 or propagate the failure status code from `subprocess.run` / `subprocess.CalledProcessError`.

8. OUTPUTS / SIDE EFFECTS
   - Writes log messages to standard output and standard error.
   - Cleans up / reads from a temporary statistics file named `/path/to/DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_[PID].tmp`.

9. BUSINESS SUMMARY
   - Acts as a structured controller ensuring data processing for contract entries (`ta_p_vertrag`) runs successfully.
   - Prevents concurrent executions or manual bypasses by requiring formal parameters via a framing script.
   - Triggers complex SQL database operations associated with specific contract processing sequences.
   - Records processed data volume metrics into runtime state configurations.

=== PSEUDOCODE STYLE ===

```python
import os
import sys
import argparse
import subprocess

# Step 1: Environment Setup
# # REVIEW-STRUCT: Sourcing environment parameters from .dw_init
# # REVIEW-STRUCT: Sourcing utility definitions from f_alis_msgerr.ksh, h_alis_date.ksh, h_alis_parameter.ksh, h_alis_sqlplus.ksh

def main():
    # Step 2: Command Line Argument Parsing
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-j', dest='p_JobKennung', default=None)
    parser.add_argument('-f', dest='p_EintragsNr', default=None)
    parser.add_argument('-h', action='store_true', dest='help_flag')
    
    # Emulate ksh getopts handling
    try:
        args, unknown = parser.parse_known_args()
    except Exception:
        # Parameter unknown
        # ErrNr = 192
        sys.exit(192)

    if args.help_flag:
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(0)

    # Step 3: Constant Variable Assignment
    v_TabName = 'ta_p_vertrag'
    
    # Step 4: Parameter Validation
    # Emulates "set +e" during verification
    err_nr = 0
    err_arg = ""

    if not args.p_JobKennung:
        err_nr = 193
        err_arg = "Jobkennung"
    elif not args.p_EintragsNr:
        err_nr = 193
        err_arg = "EintragsNr"

    if err_nr != 0:
        # Emulating DWMSG_MeldeFehler 0 E $ErrNr "$ErrArg"
        print(f"FEHLER: 0 E {err_nr} {err_arg}", file=sys.stderr)
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(err_nr)

    # Step 5: Environment Configuration (set -eu equivalent via standard Python exception flow)
    bert_dir_root = os.environ.get("BERT_DIR_ROOT")
    dw_dir_utl = os.environ.get("DW_DIR_UTL")
    pid = os.getpid()

    if not bert_dir_root or not dw_dir_utl:
        print("FEHLER: Required environment variables BERT_DIR_ROOT or DW_DIR_UTL are not set.", file=sys.stderr)
        sys.exit(1)

    # Step 6: Path Assembly
    name_sqlskript = os.path.join(bert_dir_root, "aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql")
    tmp_file = os.path.join(dw_dir_utl, f"bert_k_ausd_v_ta_p_vertrag_{pid}.tmp")

    # Step 7: DB Execution via StarteSQLSkript
    # # REVIEW-STRUCT: starteSQLSkript launcher logic not supplied. Subprocess fallback is implemented below.
    try:
        # Equivalent shell invocation: starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung
        subprocess.run([
            "starteSQLSkript", 
            args.p_EintragsNr, 
            name_sqlskript, 
            args.p_EintragsNr, 
            args.p_JobKennung
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"SQL Execution failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)

    # Step 8: Console Logging
    print(" ---------- ENDE Datenverarbeitung ----------")

    # Step 9: Result Parsing from Temporary File
    try:
        with open(tmp_file, "r") as f:
            v_records = f.read().strip()
    except IOError as e:
        # # REVIEW: File reading failure handling (e.g. if tmpFile was not created by starteSQLSkript)
        print(f"WARNUNG: Temporary file {tmp_file} could not be read: {e}", file=sys.stderr)
        v_records = ""

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.py` | Converts KSH orchestration, parameter parsing, validation, error tracking, and subprocess launching to a native Python script. |

---

### Job Dependencies
* **Upstream Dependencies**:
  * **Shared Files** — `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711` (specifically `d_ausd_v_ta_vertrag_tmp.sql`): Already migrated & merged (PR #844). On the target platform, any schema updates or temporary tables created by this step must exist before this job executes.
  * **Shared Files** — `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin` (specifically `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`): Already migrated & merged (PR #846). In the target Python code, these shared utility scripts must be referenced/imported as their migrated Python equivalents.
  * **Shared Files** — `vobs/dw_source/istools/seu/template` (specifically `.dw_global`, `.dw_init`): Already migrated & merged (PR #845). Global environment variables defined here must be loaded into the runtime environment.
* **Wiring on BigQuery / Cloud Composer**:
  * These upstream dependencies represent common shared libraries and prerequisite configurations. They must be packaged and made available in the Cloud Composer DAG's execution environment (e.g., packaged in a shared custom module or loaded as environment variables in the Airflow runtime).

---

### Execution Order
The legacy orchestration specifies the execution order as follows:
1. `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_V_TA_P_VERTRAG.xml` (UC4 Job Definition)
2. `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` (Wrapper/Framing Script)
3. `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` (Control/Verification Script — **Current Scope**)
4. `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` (Core SQL Script)

**Target Orchestration Mapping**:
* The parent wrapper script (`r_ausd_v_ta_p_vertrag.ksh`) maps to a Cloud Composer DAG or a calling task.
* The control script `k_ausd_v_ta_p_vertrag.ksh` converts to `k_ausd_v_ta_p_vertrag.py` (Current Scope).
* The execution of `d_ausd_v_ta_p_vertrag.sql` will be mapped to a BigQuery executing task (e.g., using `BigQueryInsertJobOperator` or a Dataform invocation inside the Composer workflow) triggered by `k_ausd_v_ta_p_vertrag.py`.

---

### Scheduling
* **Triggering Mechanism**: This job is NOT directly triggered by any standalone scheduler; it executes within parent scheduled jobs (e.g., as part of the overall `DW.BERT_P_VERTRAG_JP` pipeline).
* **Target Platform Mapping**: The migrated Python artifact (`k_ausd_v_ta_p_vertrag.py`) should remain a callable unit (either a Python task within a parent Airflow DAG or an importable helper module). It must not be assigned an independent standalone scheduler.

---

### Schedule & Variables — Must Be Retained
* **Inherited Scheduler Settings**: Executes as part of the larger job suite and inherits its runtime window.
* **Scheduler-set Variables**:
  * `DWH_JOB_KENNUNG` = `'AUSD_V_TA_P_VERTRAG'` (configured at the job level in UC4).
* **Target Implementation**: This variable must be supplied to the Python runtime at execution time. It can be passed via:
  * Airflow DAG `params` (e.g., `{{ params.dwh_job_kennung }}`)
  * Environment variables (`os.environ["DWH_JOB_KENNUNG"]`)
  * Command line parameter parsing (e.g., as part of the `argparse` logic in Python).

---

### Lineage
* **Upstream Producers (Inputs)**:
  * Environment/Config: `vobs/dw_source/istools/seu/template/.dw_init` (defines environmental directories and database execution configurations).
  * Utilities: Sourced helper scripts `f_alis_msgerr.ksh`, `h_alis_date.ksh`, and `h_alis_parameter.ksh`.
* **Downstream Consumers (Outputs)**:
  * Executes core script: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` to populate target tables.
  * Temporary File: Writes to and reads from `$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp` to fetch output record counts.

---

### External System Replacements
* **Oracle DB to BigQuery**: The Oracle SQL execution via SQL*Plus (`starteSQLSkript`) is replaced by running SQL statements natively in BigQuery. The connection credentials and execution driver will be handled natively by Cloud Composer using standard GCP Connection IDs or Google Cloud Client Libraries (BigQuery SDK).
* **Local POSIX Temp Files to GCS/Mem**: The temporary file `$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp` used to hold count metrics will be replaced by in-memory variable assignment (from the BigQuery job execution metadata/response) or written to a transient Google Cloud Storage (GCS) location if cross-job persistence is required.

---

### Cross-file Dependencies
* **Shared Variables and Constants**: Uses global environment configurations defined in `.dw_init`.
* **Call Chain**: The script is a control/orchestration step that is called by `r_ausd_v_ta_p_vertrag.ksh` and subsequently spawns `d_ausd_v_ta_p_vertrag.sql`.
* **Pre-migrated Helpers**: It relies on the common SQL launching logic from `h_alis_sqlplus.ksh` and custom parameter verification functions inside `h_alis_parameter.ksh`. These helpers are migrated separately under PRs #845 and #846, so their equivalents must be imported.

---

### Target File Plan
* **Target File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.py`
  * **Language**: Python
  * **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
  * **Description**: Replaces the shell-based parameter parser (`getopts`) and parameter validation with Python's standard `argparse` and native condition checks. It coordinates the trigger logic for BigQuery execution of `d_ausd_v_ta_p_vertrag.sql`.

---

### Environment-Specific Values

1. **GLOBAL (Environment-wide)**:
   * `DW_DIR_UTL` -> Maps to a shared Google Cloud Storage bucket path `GCS_BUCKET` or local directory managed in Composer environment variables (e.g., `/home/airflow/gcs/data/tmp/`).
   * Database/BigQuery project and region -> Normalizes to `GCP_PROJECT`, `BQ_DATASET`, and `BQ_LOCATION`.
   * Utility Sourcing Paths (`BERT_DIR_ROOT`) -> Normalizes to the location of Python modules in the target DAG environment, sourced using Python's `sys.path` or environment variable `BERT_DIR_ROOT`.

2. **JOB-SPECIFIC**:
   * `DWH_JOB_KENNUNG` -> Sourced via Airflow Task `params` or local environment, defaulting to `'AUSD_V_TA_P_VERTRAG'`.
   * `p_JobKennung` (passed via parameter `-j`) -> Sourced as a runtime command-line argument parsed via `argparse`.
   * `p_EintragsNr` (passed via parameter `-f`) -> Sourced as a runtime command-line argument parsed via `argparse`.
   * `v_TabName` -> Inline string value `'ta_p_vertrag'` used during validation and logging.
   * `Name_SQLskript` -> Path to the target BigQuery script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`).

---

### Risks and Manual Steps
* **Unmigrated/External Sibling Scripts**: Sibling files in the execution chain (such as the wrapper `r_ausd_v_ta_p_vertrag.ksh` and the core SQL `d_ausd_v_ta_p_vertrag.sql`) are outside this design's scope. Downstream orchestration must manually verify that these parts are fully wired together to prevent breaking the execution sequence.
* **State Recovery from Temp File**: The legacy script uses `tmpFile` to read the number of processed records (`v_records = cat tmpFile`). In BigQuery, this metadata should be retrieved directly from the query execution result (e.g., `query_job.num_dml_affected_rows` or `query_job.total_rows_processed`) rather than writing/reading local disk files, representing a structural B4 redesign item.
* **Literal Message Logging (German)**: To maintain compatibility with downstream log monitoring, all logged messages must be preserved in German:
  * `"Bitte ueber Rahmenscript aufrufen"`
  * `"FEHLER: 0 E {err_nr} {err_arg}"`
  * `" ---------- ENDE Datenverarbeitung ----------"`

---

=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh ===
#!/bin/ksh

# Zweck:
#    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_p_vertrag
#
# Erzeugt am: 20.12.2007
# Versions-Anmerkungen:
#    1.0.0;20.10.2007;Fabian Debus
#         
ProgName="Vertragsdatenabgleich"
ProgVersion="V1.0.0"

#####################################
# Funktion:
#    usage - Ausgabe der Programmbeschreibung
usage(){
cat <<EOF
    Programm: $ProgName
    Version:  $ProgVersion
    Aufruf:   $0 Parameter
    Parameter:
	-h     zeigt diese Seite an

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_p_vertrag.
EOF
}


##########################
# Vorbereitende Massnahmen
#    Einlesen der Umgebung
. $HOME/.dw_init


#    Fehlerkonzept einschalten
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh

set -eu

ErrNr=0
ErrArg=""

# Globale Fehlerbehandlung
ErrVal=0

DW_EintragsNr=0        

. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh

#####################
# Lesen der Parameter
ParamList="s:l:" # Notation gemaess getopts(1)

# lese mit Hilfe getopts die Parameter
while getopts ":h$ParamList" param
do
    case $param in
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

Name_Kernskript="${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh"


####################
# Fehlermeldekonzept
####################
typeset -u JobKennung="BERT_V_TA_P_VERTRAG"
typeset -u v_sysdate=$(date +%d%m%Y)

DWMSG_ErmittleNr DW_EintragsNr
DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr
DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 \
                     $LogDatei >> $LogDatei 2>&1
DWMSG_SetzeStichtagInfo $DW_EintragsNr $v_sysdate 'DDMMYYYY'

# Setze traps#
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'OSError: Abbruch'; exit 1" INT
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'AppError: Abbruch'" ERR

print " ----------------- Job -----------------------"
print " Job-Nr    : '$DW_EintragsNr'"
print " JobKennung: '$JobKennung'"
print " Logdatei  : '$LogDatei'"
print " ---------------------------------------------"

${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1 

# hier kommt das Skript nur an, wenn alles OK war
print "Die Abarbeitung wurde ohne erkennbare Fehler beendet" | tee -a $LogDatei
DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1

trap - INT ERR

exit 0






=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains command-line argument parsing, custom trap-based error handling, environment sourcing, and executes an external core shell script.

EVIDENCE
- Business logic found: KSH custom logic performs operational orchestration, parameter parsing, session logging setup, and invokes a core processing script (`k_ausd_v_ta_p_vertrag.ksh`).
- AWK: none
- SQL-expressible: no (the script is purely procedural orchestration with no inline SQL)
- Non-SQL side effects: Invokes external shell scripts, sets local environment traps, and writes directly to log files via shell redirection.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`r_ausd_v_ta_p_vertrag.ksh`) acts as a framework or wrapper script ("Rahmenskript") for matching and reconciling contract data in the database table `ta_p_vertrag`. Its main purpose is to establish a standardized runtime environment (sourcing system initialization, error handling, and parameter parsing) and then delegate the actual data matching process to a core execution script (`k_ausd_v_ta_p_vertrag.ksh`). It handles central logging registration, key-date definition, error trapping, and success/failure registration within the batch framework.

2. INVOCATION CONTEXT
   - Who calls this script: Typically executed via an orchestration tool like UC4/Automic. The exact UC4 job name and arguments are not specified in this extraction, but the script is designed to accept command-line arguments.
   - UC4 native includes: None referenced in this extraction.
   - Environment files sourced:
     * `. $HOME/.dw_init` — # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` — # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — functions it defines are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` — # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — functions/variables it defines are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` — # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — functions/variables it defines are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `s`: Position/Name defined via `ParamList="s:l:"`. Source: command-line argument. Used: Parsed by `getopts`, but not explicitly matched in the script's custom `case` block. 
     # REVIEW: parameter -s is declared in ParamList but unused/not explicitly processed in this script's case block — confirm if it is consumed by sourced library scripts or needs to be passed on.
   - `l`: Position/Name defined via `ParamList="s:l:"`. Source: command-line argument. Used: Parsed by `getopts`, but not explicitly matched in the script's custom `case` block. 
     # REVIEW: parameter -l is declared in ParamList but unused/not explicitly processed in this script's case block — confirm if it is consumed by sourced library scripts or needs to be passed on.
   - `h`: Help command-line argument flag. Used to trigger the `usage` function.
   
   KSH Declared Environment Parameters (Cross-reference / Informational only):
   - None explicitly declared in a companion environment block of this extraction.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `date +%d%m%Y`
     * Exact command line: `date +%d%m%Y`
     * Purpose: Formats the current system date as DDMMYYYY.
     * Translation: Native Python `datetime.date.today().strftime('%d%m%Y')`.
   - Core processing script (`k_ausd_v_ta_p_vertrag.ksh`):
     * Exact command line: `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`
       where `Name_Kernskript` is `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
     * Purpose: Runs the core contract matching process.
     * Translation: Python `subprocess.run()` execution. This does NOT qualify as a RESOLVABLE LAUNCHER because its core source logic is not supplied and it is not a direct database client.
     * # REVIEW-STRUCT: launcher [k_ausd_v_ta_p_vertrag.ksh] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
   - Central DW Framework functions (from sourced files):
     * `DWMSG_MeldeFehler`
     * `DWMSG_ErmittleNr`
     * `DWMSG_Logdateiname`
     * `DWMSG_ErzeugeEintrag`
     * `DWMSG_SetzeStichtagInfo`
     * `DWMSG_Fehlerbehandlung`
     * `DWMSG_SetzeStatusOK`
     * Purpose: Operational status registration, metadata generation, and logging.
     * Translation: Replaced by Python logging module, or call-outs to equivalent migrated Python framework modules if available.

5. EMBEDDED SQL
   - None present in this wrapper script.

6. CONTROL FLOW
   - **Step 1**: Load environment configurations by sourcing `$HOME/.dw_init`.
   - **Step 2**: Load error messages and system logging framework utilities by sourcing `f_alis_msgerr.ksh`.
   - **Step 3**: Enable shell execution flags `-e` (exit immediately on error) and `-u` (treat unset variables as errors).
   - **Step 4**: Initialize variable tracking (`ErrNr = 0`, `ErrArg = ""`, `DW_EintragsNr = 0`).
   - **Step 5**: Source parameter-handling library `h_alis_parameter.ksh` and date library `h_alis_date.ksh`.
   - **Step 6**: Parse command-line parameters using `getopts` supporting `-h` (help) and `s:` / `l:` parameters.
     - If `-h`: Call `usage()` and exit.
     - If missing required argument (`:`): Set `ErrNr = 193`, capture target option into `ErrArg`.
     - If unknown parameter (`?`): Set `ErrNr = 192`, capture target option into `ErrArg`.
   - **Step 7**: Argument verification guard: If `ErrNr != 0`, call `DWMSG_MeldeFehler`, print usage, and exit with `ErrNr`.
   - **Step 8**: Define constants: `JobKennung="BERT_V_TA_P_VERTRAG"`, `v_sysdate=$(date +%d%m%Y)`, and core script path `Name_Kernskript`.
   - **Step 9**: Initialize centralized logging registration:
     - Generate processing entry ID: `DWMSG_ErmittleNr DW_EintragsNr`
     - Determine log filename: `DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr`
     - Register the active execution: `DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 $LogDatei` (output is logged)
     - Bind process key-date: `DWMSG_SetzeStichtagInfo $DW_EintragsNr $v_sysdate 'DDMMYYYY'`
   - **Step 10**: Establish signals/error handling traps (`INT`, `ERR`) to trigger `DWMSG_Fehlerbehandlung` redirecting output to `$LogDatei`, printing error message, and exiting with 1.
   - **Step 11**: Print job metadata headers to stdout.
   - **Step 12**: Execute core processing script `${Name_Kernskript}` passing Job Identifier `-j` and Entry ID `-f`, appending stdout and stderr into `$LogDatei`.
   - **Step 13**: Finalize execution: Print success message, update job status to OK via `DWMSG_SetzeStatusOK`, reset `INT` and `ERR` traps, and exit with status 0.

7. ERROR HANDLING & EXIT CODES
   - Missing arguments or invalid flags result in exits with standard codes `193` or `192` respectively (tracked via `ErrNr`).
   - Sourced environment files provide `DWMSG_MeldeFehler` and `DWMSG_Fehlerbehandlung` to log and parse errors.
   - Execution failure of any step (due to `set -eu`) or external interrupt (`INT` / `ERR`) is caught by `trap` blocks. They perform cleanup logging and exit with `1`.
   - In Python, this matches a `try...except` wrapper:
     * `subprocess.CalledProcessError` or general `Exception` executes the equivalent of `DWMSG_Fehlerbehandlung` and exits with `1`.
     * Missing arguments / parser errors exit with specific exit codes matching `192`/`193`.

8. OUTPUTS / SIDE EFFECTS
   - Writes log outputs to `$LogDatei` (file path resolved dynamically by `DWMSG_Logdateiname`).
   - Direct database state updates and logging registration done indirectly via calls to `DWMSG_...` functions.

9. BUSINESS SUMMARY
   - Coordinates the initialization and configuration environment for contract data reconciliation (`ta_p_vertrag`).
   - Ensures execution is strictly tracked via custom metadata tracking (key dates, session IDs, status registration) for enterprise transparency.
   - Provides strict argument parsing and structural validation before committing resources.
   - Delegates the actual matching computations to a downstream core script while acting as a secure runtime host that captures all logs and failures.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
#!/usr/bin/env python3
import sys
import os
import argparse
import subprocess
from datetime import datetime

# # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — functions it defines are unknown
# # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — functions/variables it defines are unknown
# # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — functions/variables it defines are unknown

# Step 1: Mock/import equivalent framework utilities if they exist in translated Python library
# These are placeholders matching the sourced DWMSG_* framework utilities.
def DWMSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
    # Pass to centralized error reporter
    pass

def DWMSG_ErmittleNr():
    # Returns generated entry number
    return 1001  # Mock representation

def DWMSG_Logdateiname(job_kennung, eintrags_nr):
    # Resolves log path
    return f"/var/log/{job_kennung}_{eintrags_nr}.log"

def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, script_name, log_datei):
    pass

def DWMSG_SetzeStichtagInfo(eintrags_nr, sysdate, format_mask):
    pass

def DWMSG_Fehlerbehandlung(eintrags_nr):
    pass

def DWMSG_SetzeStatusOK(eintrags_nr):
    pass


# Step 2: Define Program Metadata and Usage
PROG_NAME = "Vertragsdatenabgleich"
PROG_VERSION = "V1.0.0"

def usage():
    print(f"""
    Programm: {PROG_NAME}
    Version:  {PROG_VERSION}
    Aufruf:   {sys.argv[0]} Parameter
    Parameter:
	-h     zeigt diese Seite an

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_p_vertrag.
    """)


def main():
    # Step 3: Parse and Validate Arguments (translates KSH ParamList="s:l:")
    # Note: 's' and 'l' are declared but not parsed/used in KSH case blocks.
    # # REVIEW: parameter -s and -l are declared but unused — confirm before dropping in target script
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-h', action='store_true')
    parser.add_argument('-s', required=False)
    parser.add_argument('-l', required=False)
    
    try:
        args, unknown = parser.parse_known_args()
    except Exception as e:
        # Simulate parameter failure handling
        # KSH maps missing arg (:) to 193, unknown (?) to 192
        err_nr = 192
        DWMSG_MeldeFehler(0, "E", err_nr, str(e))
        usage()
        sys.exit(err_nr)

    if args.h:
        usage()
        sys.exit(0)

    # Step 4: Define configurations and variables
    # typeset -u converts value to uppercase
    job_kennung = "BERT_V_TA_P_VERTRAG".upper()
    v_sysdate = datetime.now().strftime("%d%m%Y")
    
    bert_dir_root = os.environ.get("BERT_DIR_ROOT", "/opt/bert") # Default fallback if unset
    name_kernskript = os.path.join(bert_dir_root, "aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh")

    # Step 5: Establish framework session parameters
    dw_eintrags_nr = DWMSG_ErmittleNr()
    log_datei = DWMSG_Logdateiname(job_kennung, dw_eintrags_nr)
    
    # Step 6: Create framework execution entry
    DWMSG_ErzeugeEintrag(dw_eintrags_nr, job_kennung, sys.argv[0], log_datei)
    DWMSG_SetzeStichtagInfo(dw_eintrags_nr, v_sysdate, 'DDMMYYYY')

    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{dw_eintrags_nr}'")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")

    # Step 7: Execute Core Processing Script under error tracking (Traps logic)
    try:
        # # REVIEW-STRUCT: launcher [k_ausd_v_ta_p_vertrag.ksh] invoked — internal behaviour not available in this extraction
        with open(log_datei, "a") as log_f:
            subprocess.run(
                [name_kernskript, "-j", job_kennung, "-f", str(dw_eintrags_nr)],
                stdout=log_f,
                stderr=subprocess.STDOUT,
                check=True
            )
            
        # Step 8: Finalize Execution Success
        success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
        print(success_msg)
        with open(log_datei, "a") as log_f:
            log_f.write(success_msg + "\n")
            
        DWMSG_SetzeStatusOK(dw_eintrags_nr)
        sys.exit(0)

    except (subprocess.CalledProcessError, Exception) as err:
        # Step 9: Trap execution context (ERR / INT) and handle failures
        # Mimics the error trap and logging behavior of KSH script
        error_log_msg = f"AppError: Abbruch\nDetails: {str(err)}"
        print(error_log_msg, file=sys.stderr)
        
        try:
            with open(log_datei, "a") as log_f:
                log_f.write(error_log_msg + "\n")
            DWMSG_Fehlerbehandlung(dw_eintrags_nr)
        except Exception as log_err:
            print(f"Failed to execute DWMSG_Fehlerbehandlung: {log_err}", file=sys.stderr)
            
        sys.exit(1)

if __name__ == "__main__":
    # MANDATORY AUDIT STEP: Verified that there are no custom parameter-validation 
    # guards in any KSH function within the source script to copy.
    main()
```

### ADD CONTEXT THE MCP COULD NOT SEE

#### 1. Job Dependencies
* **Upstream dependencies**:
  * **Shared Files** (`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711`): Already migrated and merged under PR #844. Contains schema configurations and initial temporary table initializations (`d_ausd_v_ta_vertrag_tmp.sql`).
  * **Shared Files** (`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`): Already migrated and merged under PR #846. Contains operational framework logging and parameter utility functions (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
  * **Shared Files** (`vobs/dw_source/istools/seu/template`): Already migrated and merged under PR #845. Contains environment setup configurations (`.dw_global`, `.dw_init`).
* **Target Wiring**:
  * Since the shared libraries and environment configuration dependencies are already migrated, they should be imported or referenced directly in the Python target script. No new migrations are required for these dependencies.

#### 2. Execution Order
The legacy system executes the workflow in the following sequence:
1. **Orchestration**: UC4 scheduler XML exports (`DW.BERT_AUSD_V_TA_P_VERTRAG.xml`).
2. **Wrapper script**: `r_ausd_v_ta_p_vertrag.ksh` (This is the source file handled in this design).
3. **Core processing script**: `k_ausd_v_ta_p_vertrag.ksh` (Invoked by the wrapper).
4. **Database alignment core logic**: `d_ausd_v_ta_p_vertrag.sql` (Invoked by the core processing script).

**Target Mapping**:
* Step 1 maps to a Cloud Composer (Airflow) DAG task group.
* Step 2 (the scope of this design pass) maps to the target file `r_ausd_v_ta_p_vertrag.py`, which is executed as a Python task within the Composer DAG.
* Step 3 and Step 4 are owned and migrated by separate design passes, resulting in a target Python core script and BigQuery/Dataform pipelines. The wrapper task will trigger the core Python script `k_ausd_v_ta_p_vertrag.py` upon setup validation.

#### 3. Schedule & Variables — Must Be Retained
* **Scheduling**: This job is NOT directly scheduled on its own; it runs inside scheduled parent jobs (e.g., as an included/shared module). In Cloud Composer, this task must remain a callable/importable operator unit within the parent DAG and must not be given its own independent cron schedule.
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` = `'AUSD_V_TA_P_VERTRAG'` (Supplied by the scheduler).
  * This variable must be supplied to the Python target script at runtime using the Airflow execution context (`kwargs['params'].get('DWH_JOB_KENNUNG', 'AUSD_V_TA_P_VERTRAG')`) or via environment variable injection.

#### 4. Lineage
* **Upstream Producers**:
  * Sourced parameters are obtained from the legacy initialization profiles (`.dw_init`).
  * Operational procedures are driven by shared libraries (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
* **Downstream Consumers**:
  * This script directly invokes `k_ausd_v_ta_p_vertrag.ksh` (`k_ausd_v_ta_p_vertrag.py` in the target environment) which represents a cross-job hand-off to the core data alignment script.

#### 5. External System Replacements
* **Logging Framework**: The legacy wrapper's usage of localized utility shell loggers (e.g., `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`) will be replaced by native Google Cloud Logging integrations via Airflow's Python Logging interface. This permits monitoring execution output directly through the Composer task logs.
* **Environment Sourcing**: The legacy file system `.dw_init` sourcing is replaced by Airflow DAG Variables.

#### 6. Cross-File Dependencies
* **Core Execution Step**: The wrapper script directly invokes the core matching script `k_ausd_v_ta_p_vertrag.ksh`. The target `r_ausd_v_ta_p_vertrag.py` will handle this dependency by triggering `k_ausd_v_ta_p_vertrag.py` (migrated in a different design pass) via Python's module import system or a controlled `subprocess` execution.

#### 7. Target File Plan
* **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.py`
* **Language**: Python (v3.8+)
* **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`
* **Purpose**: This file translates the wrapper shell logic into Python. It validates incoming command-line flags (`-h`, `-s`, `-l`), initializes execution tracking IDs, binds log sessions, invokes the downstream core processing logic `k_ausd_v_ta_p_vertrag.py`, and logs the outcome with strict error capture.

#### 8. Environment-Specific Values
The legacy parameters must be classified and resolved in the target as follows:

* **GLOBAL** (Environment-wide infrastructure settings):
  * `HOME` -> Resolved using native Python `os.path.expanduser("~")`.
  * `BERT_DIR_ROOT` -> The root deployment path for scripts on the Airflow worker. Resolved at runtime using `os.environ.get("BERT_DIR_ROOT")` or an Airflow variable.
  * `GCP_PROJECT` -> Google Cloud Project identifier. Resolved using `os.environ.get("GCP_PROJECT")`.

* **JOB-SPECIFIC** (Parameters unique to this execution wrapper):
  * `JobKennung` -> Set as a script constant: `"BERT_V_TA_P_VERTRAG"`.
  * `DWH_JOB_KENNUNG` -> Extracted from DAG execution parameters: `kwargs['params'].get('DWH_JOB_KENNUNG')`.
  * `LogDatei` -> File path where session execution records are outputted. Resolved dynamically inside Python using the standard `logging` handler, or directed to a GCS bucket depending on Composer environment defaults.

#### 9. Risks and Manual Steps
* **Sourced Libraries Verification**: The utility shell scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are listed as already migrated. The build agent must verify that the corresponding Python modules or library functions are available in the target python environment (e.g., inside a shared python package) to support importing them during compilation.
* **Sibling Sourced Files**: Sibling execution components (`k_ausd_v_ta_p_vertrag.ksh` and `d_ausd_v_ta_p_vertrag.sql`) are not part of this specific design pass. The target system depends on their migrated files being available during runtime.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.py` | Wrapper shell script converted to a Python-native orchestrator to parse CLI arguments, manage job tracking variables, write central execution logs, and call the core data aligning python script. |

---

=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql ===
-- ===================================================================
-- Datei:  d_ausd_v_ta_p_vertrag.sql
-- Datum:  16.10.2002
-- Autor:  Martin Buettner
-- ===================================================================
--
-- Modifikationen
----------------------------------------------------------------------
-- Version Datum    Autor  Dokumentation
-- 3.1.0   20030109 sj     Tabellennamenerweiterung um das tagesdatum
--         20030115 mb     Modifikationen und Erweiterung gemaess Protokoll:
--                         (u.a.: Ausgabe der Rabatthoehe, Sperrgruende, Sperrzeiten)
-- 7.5.0   20040831 Roh    Neues Attr. is_production in CDS$TA_DISCOUNT
--                         Umstellung auf parallel degree 4
-- 7.5.1   20041011 mb     Modifikationen fuer den neuen Rabattreport
-- ab 2005 neue Releasenummern
-- 5.1.1   20050204 Roh    neues Feld SEGMENT_ID auf Rechnugsdefiniton
-- 5.4.0   20090901 Roh    spool ins Unterverzeichnis ./tmp
-- 5.4.1   20051025 mb     Erweiterung fuer Pooling Report: Rechnungsinhaltskonfigurationstext
--                                                          (inv_cont_config_id)
-- 5.4.2   20051117 mb     Ersetzung der ANALYZE Kommandos durch GATHER_TABLE_STATS
-- 5.4.3   20051119 hs     Mergen auf dwh$ta_p_vertrag_<datum> zum Addiern von Stillegungstagen
-- 5.4.4   20051202 hs     Laufzeittuning zu Step 201b (merge via FTS)
-- 6.1.0   20051220 Roh    Neues Feld 0B-Nummer (CDS$TA_CNTRCT.ORDER_NUMBER)
-- 6.3.0   20060919 me/hs  F�r Berechnung Bindefrist mit Sperren/Stillegungen: Erweiterung um Sperrklasse 7
-- 6.4.0   20061122 me     Umstellung wegen Datenmodell-Aenderung in Carmen:
--                         Tab. CDS$TA_BARRIER_KIND_CV -> CDS$TA_BARRIER_KIND,
--                         tab. CDS$TA_DESCRIPTION     -> CDS$TA_CARE_DESCRIPTION
-- 6.4.0   20061121 RR     Bestimmung Substitutions-Variable v_datum aus
--                         Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR     �berfl�ssige ANALYZE/STATISTICS Kommandos entfernt
-- 6.4.2   20061129 RR     Restartf�higkeit unter Verwendung bereits erzeugter Tabellen
-- 7.2.0   20070511 ME     NVL bei Setzen Bindefrist eingebaut (MERGE, step201b)
-- 7.4.0   20070814 AR/ME  Nach Umstellung 9i -> 10g:
--                         Steps 13a/b und 106a/b umgestellt auf TABLE-Function, hierzu neues Package
--                         Step 201b, MERGE: VALUES bei INSERT WHEN NOT MATCHED ergaenzt
-- 8.1.0   20071219 FD     Ausgliederung aus d_ausd_vertrag.sql
-- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
----------------------------------------------------------------------
--
--
prompt variablendefinitionen
--
--
-- DB-Link auf CARMEN DB: entweder leer oder mit "@"
DEFINE v_carmen       = "@pcrs1"

-- Stichtag ermitteln
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
--
--
prompt tracing und settings
--
--
START ../trace.sql.cfg
SPOOL ./tmp/trace_d_ausd_v_ta_p_vertrag

WHENEVER SQLERROR CONTINUE
  SET TIMING ON
  SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT FAILURE
--
--
prompt tabelle von vorherigem lauf leeren
--
--
WHENEVER SQLERROR CONTINUE
begin 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_vertrag');  
end;
/

WHENEVER SQLERROR EXIT FAILURE
--
--
prompt vertragstabelle sof$ta_p_vertrag (nachbearbeitung der twinbill vertraege)
--
--
INSERT  INTO sof$ta_p_vertrag
       (vertrag_id_carmen,
       partner_id_carmen,
       rechdef_id_carmen,
       kundenkonto,
       mwst_kennzeichen,
       rahmenvertrag_id,
       rechnungslauf,
       vo_kenn,
       geplant_kuend,
       eingang_kuend,
       vertragsbeginn,
       vertragsstatus,
       sperrart,
       sperrgrund,
       stillegungszeitraum,
       twincard,
       dwh_tarifgr_text,
       bindefrist,
       letztes_upgrade,
       vertragsbindung,
       vertragsbindungseinheit,
       rechnungszahlart,
       rechnungsmedium,
       twin_vertrag_id,
       upgradeberechtigt,
       apn,
       upgradegrund,
       sv_id,
       vda,
       cost_centre,
       cost_centre_user,
       cntrct_ty,
       segment_id,
       rv_action_id,
       rechn_inh_konfig_text,
       order_number,
       commitment_reference_date,
       cntrct_validity_id)
SELECT /*+ parallel(v,4) parallel(pv,4) */
       v.vertrag_id_carmen,
       v.partner_id_carmen,
       v.rechdef_id_carmen,
       v.kundenkonto,
       v.mwst_kennzeichen,
       v.rahmenvertrag_id as rahmenvertrag_id,
       v.rechnungslauf,
       v.vo_kenn as vo_kenn,   -- TB: immer von der Hauptkarte
       v.geplant_kuend,
       v.eingang_kuend,
       v.vertragsbeginn,
       v.vertragsstatus,
       v.sperrart,
       v.sperrgrund,
       v.stillegungszeitraum,
       v.twincard,
       v.dwh_tarifgr_text,
       v.bindefrist,
       v.letztes_upgrade,
       v.vertragsbindung,
       v.vertragsbindungseinheit,
       v.rechnungszahlart,
       v.rechnungsmedium,
       v.twin_vertrag_id,
       v.upgradeberechtigt,
       v.apn,
       v.upgradegrund,
       v.sv_id,
       v.vda,
       v.cost_centre,
       v.cost_centre_user,
       v.cntrct_ty,
       v.segment_id,              -- ab Rel. 5.1.1
       v.rv_action_id,            -- ab 01.09.2005
       v.rechn_inh_konfig_text,   -- ab 25.10.2005
       v.order_number,             -- 20052012 Roh. ab Rel6.1.0
       v.commitment_reference_date,
       v.cntrct_validity_id
  FROM
        sof$ta_vertrag_tmp     v,
        sof$ta_vertrag_tmp     pv
  WHERE
        v.twin_vertrag_id = pv.vertrag_id_carmen (+);

commit;

--
--
prompt leeren der temporaeren zwischentabellen
--
--
WHENEVER SQLERROR CONTINUE
-- Diese Tabellen werden nicht gedropped, da sie direkt �ber
-- PL/SQL verwendet werden. Ansonsten kann es beim Restart
-- u.U. zum Programmabbruch kommen.

begin 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_disc_zusgf DROP STORAGE');  
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_discount DROP STORAGE');  
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_barrier_zusgf DROP STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_barrier DROP STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_templ REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_valid REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_period REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bp_ref REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_assign REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_def REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_acc_ref REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_notice REUSE STORAGE'); 
--isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_barrier_hist REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_apn_ve REUSE STORAGE');
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_discount_rr REUSE STORAGE');  
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_vvl_dwh REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_vvl_upgrade REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs2 REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs3 REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_acc REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_vertrag_tmp REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_action_assoc REUSE STORAGE'); 
end;
/

prompt Verarbeitung fehlerfrei beendet.
spool off


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object:
    - Multi-statement SQL and PL/SQL staging script containing variable initialization, DML (`INSERT INTO ... SELECT`), and dynamic procedural operations (calls to a utility package `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` to execute `TRUNCATE TABLE` on multiple tables).

1.2 Business Logic and Purpose:
    - The script prepares contract data for reporting purposes in the BERT application. 
    - It determines a reporting date (`v_datum`) from a tracking/logging table (`isbert_schema.dwtk_meldungen`).
    - It truncates a destination contract staging table (`sof$ta_p_vertrag`).
    - It extracts, joins, and aggregates twin-card contract information from a temporary table (`sof$ta_vertrag_tmp`), resolving relationships through a self-left-join where a twin-contract exists.
    - Finally, it clears down more than 20 intermediate temporary staging tables to free memory/storage for subsequent jobs.

1.3 Entities Referenced:
    - Tables:
        - `isbert_schema.dwtk_meldungen` (source)
        - `sof$ta_p_vertrag` (target table)
        - `sof$ta_vertrag_tmp` (source, self-joined as `v` and `pv`)
        - Secondary staging/temporary tables (truncated):
            - `sof$ta_disc_zusgf`, `sof$ta_discount`, `sof$ta_barrier_zusgf`, `sof$ta_barrier`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_bp_ref`, `sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref`, `sof$ta_notice`, `sof$ta_apn_ve`, `sof$ta_discount_rr`, `sof$ta_vvl_dwh`, `sof$ta_vvl_upgrade`, `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, `sof$ta_inv_acc`, `sof$ta_vertrag_tmp`, `sof$ta_action_assoc`
    - Package:
        - `isbert_schema.DWPA_UTIL_SKRIPT` (utility package for dynamic statement execution)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (in `m.timecreated`) contains both date and time. It is mapped to BigQuery `TIMESTAMP`.
    - Oracle identifiers and keys (such as `vertrag_id_carmen`, `partner_id_carmen`) map to BigQuery `INT64` or `STRING`.
    - Descriptive text fields map to BigQuery `STRING`.

2.2 Implicit and Explicit Type Casting:
    - `TO_CHAR(MAX(m.timecreated), 'YYYYMMDD')` is translated to BigQuery's explicit datetime formatting function: `FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated))`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(TO_CHAR(...), '19000101')` -> Resolved using `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')`.

2.4 String Functions:
    - `TO_CHAR` used for date-to-string formatting is replaced by BQ's `FORMAT_TIMESTAMP` / `FORMAT_DATETIME`.

2.5 Date and Timestamp Functions:
    - Not directly truncated using `TRUNCATE(date)` here, but the extraction of the date component utilizes explicit formatting.

2.7 Analytical and Window Functions:
    - None used.

2.8 Set and Join Operations:
    - Oracle proprietary outer join syntax `(+)` (e.g., `v.twin_vertrag_id = pv.vertrag_id_carmen (+)`) must be converted to standard ANSI `LEFT OUTER JOIN` in BigQuery: 
      `sof$ta_vertrag_tmp v LEFT OUTER JOIN sof$ta_vertrag_tmp pv ON v.twin_vertrag_id = pv.vertrag_id_carmen`.

2.9 Row Limiting and Sampling:
    - None used.

2.10 Sequences:
    - None used.

2.11 MERGE Statements:
    - None used.

2.12 INSERT / UPDATE / DELETE:
    - Standard `INSERT INTO ... SELECT` statement. The optimizer hints (`/*+ parallel(v,4) parallel(pv,4) */`) must be stripped out completely as BigQuery manages parallel execution automatically.

2.13 DDL/Utility Constructs:
    - SQL*Plus variables (`DEFINE`, `COLUMN`, `SPOOL`, `SET TIMING`, `WHENEVER SQLERROR`) are stripped out. Environment controls are represented using BigQuery procedural Scripting variables and exception handling constructs.
    - Dynamic statements calling `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')` are translated into direct, native BigQuery statements: `TRUNCATE TABLE dataset_name.table_name;`. Storage options (`DROP STORAGE`, `REUSE STORAGE`) are Oracle-specific physical allocations and are stripped out completely.

2.14 PL/SQL:
    - The PL/SQL anonymous blocks calling the utility framework are replaced by sequential native BigQuery Scripting statements within a `BEGIN ... END;` procedural block.

2.15 Unresolvable or Advisory Items:
    - Database Link definition (`DEFINE v_carmen = "@pcrs1"`) is declared in SQL*Plus but not actively consumed in the queries inside this script. It is omitted. If the data resides across database boundaries, these source tables must be replicated/pipelined into BigQuery beforehand.

Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    - The entire script is translated into a single, cohesive BigQuery Scripting block (`BEGIN ... END;`). 
    - Scripting variables (`DECLARE`) are used to hold step values such as `v_datum` (even though `v_datum` is assigned but not actively leveraged in downstream DML in this specific script, we preserve the assignment for functional parity).
    - Hardcoded Oracle dynamic utility PL/SQL calls are translated to native, optimized, compile-time verified `TRUNCATE TABLE` statements.
    - Oracle parallel hints are stripped, and proprietary outer-joins `(+)` are rewritten to clean ANSI `LEFT OUTER JOIN` syntax.

3.2 Assumptions:
    - The source tables (`sof$ta_vertrag_tmp`, `isbert_schema.dwtk_meldungen`) and target tables exist within the default or designated BigQuery dataset.
    - Schema resolution is handled via the target environment's dataset configuration (e.g. prefixing tables with dataset path where applicable).

3.3 Items Flagged for Human Review:
    - Confirm whether the target dataset name should be dynamically templated or hardcoded.
    - Confirm if variable `v_datum` is needed elsewhere in the orchestration pipeline, since it is set but not referenced in the subsequent queries of this script.

═══════════════════════════════════════════
MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Oracle Code Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Logic | Reason for Decision |
| :--- | :--- | :--- | :--- | :--- |
| SQL*Plus commands (`DEFINE`, `SPOOL`, `SET`) | Strip / BQ Declarations | Keep as-is | BigQuery does not recognize SQL*Plus environment parameters. | System environment commands are not database engine operations. |
| `NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), ...)` | `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), ...)` | `SAFE_CAST` | Returns a formatted string representation of the maximum date safely. | Direct BigQuery standard SQL equivalents exist. |
| Parallel Hints `/*+ parallel(v,4) */` | Strip entirely | Retain as comments | BigQuery's execution engine automatically scales and parallelizes work. | Optimizer hints are ignored/unsupported in BigQuery. |
| Oracle Outer Join `v.twin_vertrag_id = pv.vertrag_id_carmen (+)` | ANSI `LEFT OUTER JOIN` | `CROSS JOIN` with filter | The standard representation of a right-hand side optional join is a `LEFT OUTER JOIN`. | Native SQL engine syntax requirement. |
| Dynamic PL/SQL calls to `DWPA_UTIL_SKRIPT.runstatement` | Native `TRUNCATE TABLE` statements | Python Orchestrator, BigQuery Dynamic SQL (`EXECUTE IMMEDIATE`) | The list of truncated tables is fully static and known at design-time. | Compiles natively, guarantees schema validation, and runs faster than dynamic strings. |

═══════════════════════════════════════════
REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **BigQuery SQL Script**: A single `.sql` file containing a procedural scripting block (`DECLARE`, `SET`, `TRUNCATE`, `INSERT`, `BEGIN...EXCEPTION`). No external Python wrapper or UDF definitions are necessary.

═══════════════════════════════════════════
DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column / Construct Type | Target BigQuery Type | Conversion Rule | Warnings / Mitigation |
| :--- | :--- | :--- | :--- |
| `m.timecreated` (`DATE`) | `TIMESTAMP` | Map Oracle `DATE` with timestamp component to BigQuery `TIMESTAMP`. | Ensure timezone settings match source systems. Default behavior assumes UTC. |
| `VARCHAR2` | `STRING` | Direct conversion to variable-length `STRING`. | BigQuery standard `STRING` types do not enforce maximum length bounds. |
| `NUMBER` (ID fields) | `INT64` | Maps to standard BigQuery 64-bit integer. | No precision loss for integer mappings. |
| `NUMBER(p, s)` (numeric amounts) | `NUMERIC` / `BIGNUMERIC` | Map high precision fields to `NUMERIC` (38 digits). | Prevents rounding errors during floating-point conversions. |

═══════════════════════════════════════════
DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**:
  - Metadata tracking query (`MAX(timecreated)`).
  - Staging insert from a twin-bill self-joined table layout.
  - Multi-table cleanup steps via dynamic PL/SQL block.
- **Unsupported Functions**: Oracle `NVL`, `TO_CHAR` (for date/timestamp formatting), and Oracle join operator `(+)`.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Source tables `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp`.
- **Assumptions**: 
  - `isbert_schema` and `sof` schemas map to BigQuery datasets of the same or corresponding names.
  - No database link connectivity is needed dynamically, since source tables are already staged in BigQuery.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

═══════════════════════════════════════════
ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `TO_CHAR(date, 'YYYYMMDD')` | Direct-with-rewrite | `FORMAT_TIMESTAMP('%Y%m%d', ...)` |
| `MAX` | Direct | `MAX` |
| `(+)` Join Operator | Direct-with-rewrite | Standard ANSI `LEFT OUTER JOIN` |
| `/*+ parallel */` Hint | Direct-with-rewrite | None — Strip entirely |
| `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` | Direct-with-rewrite | Direct native SQL statements (`TRUNCATE TABLE`) |
| `COMMIT` | Direct | No action needed (Auto-committed in BigQuery) |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- BigQuery SQL Migration Script
-- Target: BigQuery Standard SQL (Procedural Scripting)

BEGIN
  -- Declare variable to hold the processing date
  DECLARE v_datum STRING;

  -- Step 1: Determine reporting date (Stichtag ermitteln)
  -- COALESCE converted from NVL()
  -- FORMAT_TIMESTAMP converted from TO_CHAR()
  SET v_datum = (
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `isbert_schema.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Step 2: Empty the main target staging table from previous run
  TRUNCATE TABLE `sof.sof$ta_p_vertrag`;

  -- Step 3: Populate destination contract table
  -- Note: Oracle parallel hints stripped.
  -- Note: Oracle join syntax (+) converted to ANSI LEFT OUTER JOIN.
  INSERT INTO `sof.sof$ta_p_vertrag` (
    vertrag_id_carmen,
    partner_id_carmen,
    rechdef_id_carmen,
    kundenkonto,
    mwst_kennzeichen,
    rahmenvertrag_id,
    rechnungslauf,
    vo_kenn,
    geplant_kuend,
    eingang_kuend,
    vertragsbeginn,
    vertragsstatus,
    sperrart,
    sperrgrund,
    stillegungszeitraum,
    twincard,
    dwh_tarifgr_text,
    bindefrist,
    letztes_upgrade,
    vertragsbindung,
    vertragsbindungseinheit,
    rechnungszahlart,
    rechnungsmedium,
    twin_vertrag_id,
    upgradeberechtigt,
    apn,
    upgradegrund,
    sv_id,
    vda,
    cost_centre,
    cost_centre_user,
    cntrct_ty,
    segment_id,
    rv_action_id,
    rechn_inh_konfig_text,
    order_number,
    commitment_reference_date,
    cntrct_validity_id
  )
  SELECT
    v.vertrag_id_carmen,
    v.partner_id_carmen,
    v.rechdef_id_carmen,
    v.kundenkonto,
    v.mwst_kennzeichen,
    v.rahmenvertrag_id AS rahmenvertrag_id,
    v.rechnungslauf,
    v.vo_kenn AS vo_kenn,
    v.geplant_kuend,
    v.eingang_kuend,
    v.vertragsbeginn,
    v.vertragsstatus,
    v.sperrart,
    v.sperrgrund,
    v.stillegungszeitraum,
    v.twincard,
    v.dwh_tarifgr_text,
    v.bindefrist,
    v.letztes_upgrade,
    v.vertragsbindung,
    v.vertragsbindungseinheit,
    v.rechnungszahlart,
    v.rechnungsmedium,
    v.twin_vertrag_id,
    v.upgradeberechtigt,
    v.apn,
    v.upgradegrund,
    v.sv_id,
    v.vda,
    v.cost_centre,
    v.cost_centre_user,
    v.cntrct_ty,
    v.segment_id,
    v.rv_action_id,
    v.rechn_inh_konfig_text,
    v.order_number,
    v.commitment_reference_date,
    v.cntrct_validity_id
  FROM
    `sof.sof$ta_vertrag_tmp` AS v
  LEFT OUTER JOIN
    `sof.sof$ta_vertrag_tmp` AS pv  -- Converted from Oracle outer join (+) operator
    ON v.twin_vertrag_id = pv.vertrag_id_carmen;

  -- Step 4: Empty intermediate staging tables (Leeren der temporaeren Zwischentabellen)
  -- Note: Dynamic runstatement calls and storage options (DROP STORAGE, REUSE STORAGE) stripped.
  TRUNCATE TABLE `sof.sof$ta_disc_zusgf`;
  TRUNCATE TABLE `sof.sof$ta_discount`;
  TRUNCATE TABLE `sof.sof$ta_barrier_zusgf`;
  TRUNCATE TABLE `sof.sof$ta_barrier`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_crs`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_templ`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_valid`;
  TRUNCATE TABLE `sof.sof$ta_period`;
  TRUNCATE TABLE `sof.sof$ta_bp_ref`;
  TRUNCATE TABLE `sof.sof$ta_inv_assign`;
  TRUNCATE TABLE `sof.sof$ta_inv_def`;
  TRUNCATE TABLE `sof.sof$ta_acc_ref`;
  TRUNCATE TABLE `sof.sof$ta_notice`;
  TRUNCATE TABLE `sof.sof$ta_apn_ve`;
  TRUNCATE TABLE `sof.sof$ta_discount_rr`;
  TRUNCATE TABLE `sof.sof$ta_vvl_dwh`;
  TRUNCATE TABLE `sof.sof$ta_vvl_upgrade`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_crs2`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_crs3`;
  TRUNCATE TABLE `sof.sof$ta_inv_acc`;
  TRUNCATE TABLE `sof.sof$ta_vertrag_tmp`;
  TRUNCATE TABLE `sof.sof$ta_action_assoc`;

END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Unused Script Variable**: The variable `v_datum` is queried and populated from the table `isbert_schema.dwtk_meldungen` as was done in the original Oracle script, but it is not utilized in any downstream queries within this script. Confirm if this variable is intended to be passed to external pipeline tasks or if this query can be safely deprecated.
2. **Database Schema Qualification**: Verify that the dataset names (e.g. `isbert_schema` and `sof` used as catalog qualifiers in the pseudocode) match the designated BigQuery target project/dataset structural taxonomy.
3. **Implicit Transaction Boundaries**: BigQuery auto-commits each statement sequentially in Scripting mode unless wrapped in standard `BEGIN TRANSACTION` / `COMMIT TRANSACTION` blocks. If atomic execution of all cleanups is an absolute requirement, wrapping the entire set of DML/DDL operations within an explicit transaction block should be evaluated.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` | Converts the Oracle SQL*Plus script containing the date metadata query, the primary insertion resolving twin-card contracts via standard ANSI `LEFT OUTER JOIN`, and the sequential `TRUNCATE TABLE` operations for clearing temporary tables into a native BigQuery SQL scripting block. |

---

### Job Dependencies

*   **Upstream Predecessors** (already migrated & merged):
    *   Shared Files — `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711` (specifically `d_ausd_v_ta_vertrag_tmp.sql`) via PR [#844](https://github.com/gurunathan-prodapt/pi-agents/pull/844) — Must execute to populate the upstream temp table `sof$ta_vertrag_tmp` before this script runs.
    *   Shared Files — `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin` (specifically utility files such as `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh`) via PR [#846](https://github.com/gurunathan-prodapt/pi-agents/pull/846).
    *   Shared Files — `vobs/dw_source/istools/seu/template` (specifically `.dw_global` and `.dw_init`) via PR [#845](https://github.com/gurunathan-prodapt/pi-agents/pull/845).
*   **Downstream Successors**: None listed in the pre-collected job context.

The upstream dependencies must be wired within Cloud Composer / Airflow (using task sequence or DAG sensors) to guarantee that the prerequisite temporary staging tables have been populated before this SQL script begins execution.

---

### Execution Order

To preserve the legacy dependency graph, the execution sequence of the components within this job must be implemented in the target orchestration as follows:
1.  **UC4 Job definition**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml`
2.  **KSH Wrapper**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`
3.  **Core Executor Script**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
4.  **Target SQL Script (This Group)**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` (mirrored target path: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`)

---

### Schedule & Variables

*   **Scheduling**: This job is not directly triggered by any of the schedulers; it runs as an included unit within an orchestrating group. The migrated BigQuery SQL script should remain a callable/importable block (e.g., an independent task in Cloud Composer) and should not be given an independent trigger.
*   **Scheduler-Set Variables**:
    *   `DWH_JOB_KENNUNG` = `'AUSD_V_TA_P_VERTRAG'` (JOB-SPECIFIC variable set by the legacy scheduler) — Should be passed as an Airflow Parameter or environment variable if needed for logging metadata.

---

### Lineage

*   **Upstream Producers**:
    *   `isbert_schema.dwtk_meldungen` (Reads column `timecreated` to retrieve the latest execution reporting date).
    *   `sof$ta_vertrag_tmp` (Reads temporary contracts to resolve the twin-card relation via a self-outer join).
*   **Downstream Consumers**:
    *   `sof$ta_p_vertrag` (Writes resolved contract details).
*   **Packages/APIs Referenced**:
    *   `isbert_schema.DWPA_UTIL_SKRIPT` (Used in Oracle to execute dynamic DDL/DML, which translates to native statements in BigQuery).
    *   `PV` (Referenced in parallel execution optimizer hint syntax `/*+ parallel(pv,4) */` which is stripped in BigQuery).

---

### External System Replacements

*   The Oracle Database Link defined as `v_carmen = "@pcrs1"` is defined in SQL\*Plus but is not actively used in the queries. Because all required source tables (`dwtk_meldungen`, `sof$ta_vertrag_tmp`) are already staged in the BigQuery environment, no external database links or cross-project connection configurations are needed for this execution.

---

### Cross-File Dependencies

*   The execution relies on the prior staging tables (`sof$ta_vertrag_tmp`) being completely populated, which is handled by the upstream sibling script `d_ausd_v_ta_vertrag_tmp.sql` (migrated in PR #844).
*   It assumes that target schema definitions for the `sof$` tables and `isbert_schema` exist in BigQuery.

---

### Target File Plan

*   **Target File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`
    *   **Language**: BigQuery SQL (Procedural Scripting)
    *   **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`
    *   **Description**: Converted SQL procedural block using `BEGIN ... END;` structure containing:
        *   An initial declaration of local variables (including the runtime-derived `v_datum`).
        *   A native `TRUNCATE TABLE` statement for the target staging table.
        *   An `INSERT INTO ... SELECT` query utilizing standard ANSI `LEFT OUTER JOIN` (instead of Oracle's proprietary `(+)` operator) to resolve twin-card relationships.
        *   A sequence of native `TRUNCATE TABLE` commands to clear the 22 intermediate temporary staging tables at runtime.

---

### Environment-Specific Values

The following variables/identifiers must be mapped to their environment-specific equivalents at deploy-time:

*   **GLOBAL (Environment-Wide)**:
    *   `GCP_PROJECT`: Identifies the target Google Cloud project. Sourced dynamically from the environment.
    *   `BQ_DATASET_ISBERT`: The target dataset replacing the legacy schema prefix `isbert_schema` (e.g. `isbert_schema_prod`).
    *   `BQ_DATASET_SOF`: The target dataset replacing the legacy schema prefix `sof` (e.g. `sof_prod`).
*   **JOB-SPECIFIC**:
    *   `DWH_JOB_KENNUNG` (value: `'AUSD_V_TA_P_VERTRAG'`) — Sourced from the orchestrating task metadata.
    *   `v_datum` — Evaluated dynamically at runtime within the BigQuery SQL scripting block from `dwtk_meldungen`.

---

### Risks and Manual Steps

*   **Unreferenced Date Variable**: The legacy Oracle script retrieves a date (`v_datum`) from `dwtk_meldungen` using the job tracking code `BERT_DROP_TEMP_TABLE`. However, `v_datum` is never actually used inside any of the query filters or truncation operations in this specific script. A developer should review if this query was a legacy holdover or if the resolved date is meant to be exported or logged to a pipeline execution table.
*   **Transactional Staging**: BigQuery executes statements sequentially with automatic commits. If atomic rollbacks are required in case any of the sequential truncations or the main insertion fail, the execution block should be wrapped inside a `BEGIN TRANSACTION` / `COMMIT TRANSACTION` block.
*   **Logging Prompts**: The original SQL\*Plus prompts in German must be kept in character-for-character compliance with the **OUTPUT/PRINT LITERAL RULE**. When logging statements are built into the target execution wrappers, ensure that these exact literals are preserved:
    *   `variablendefinitionen`
    *   `tracing und settings`
    *   `tabelle von vorherigem lauf leeren`
    *   `vertragstabelle sof$ta_p_vertrag (nachbearbeitung der twinbill vertraege)`
    *   `leeren der temporaeren zwischentabellen`
    *   `Verarbeitung fehlerfrei beendet.`