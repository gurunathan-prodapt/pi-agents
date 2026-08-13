=== OBJECT: DW.BERT_AUSD_V_TA_PERIOD (JOBS_UNIX) ===
active=1
title=Mirror Carmen period definitions
login=DW.UNIX.ISBERT
host=|DWHDWH1P|HOST
ert_seconds=6
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
. $HOME/.dw_init
&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
:inc DW.BERT_LESE_LOG
operational_notes=

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4/Automic to Apache Airflow Migration Design Document

## 1. Overview
UNCERTAIN: This extraction contains only a single UC4 `JOBS_UNIX` object (`DW.BERT_AUSD_V_TA_PERIOD`) without its parent workflow (`JOBP`) or schedule (`JSCH`). Based on its title "Mirror Carmen period definitions" and script body, it executes a Korn-Shell (KSH) script (`r_ausd_v_ta_period.ksh`) to synchronize or process Carmen period definitions. Because no surrounding parent workflow, scheduling configuration, or active trigger script was provided in this extraction bundle, this job is modeled here as a standalone, externally triggered Airflow DAG.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.BERT_AUSD_V_TA_PERIOD` | JOBS_UNIX | 1 (Active) | Mirror Carmen period definitions |

## 3. Scheduling
- **Schedule Policy**: No `EVNT_TIME` object or calendar configurations are present in this bundle.
- **Triggers**: This workflow has no calendar-based schedule of its own. It is assumed to be externally triggered, or is a component of a larger workflow not included in this extraction.
- **Airflow Configuration**: `schedule=None` (no manual cron logic has been assumed or invented).

## 4. Airflow DAG Properties
This properties mapping represents a standalone wrapper DAG created specifically to house the isolated UNIX job.

| Property | Value |
|---|---|
| **dag_id** | `dw_bert_ausd_v_ta_period` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{"owner": "DW.UNIX.ISBERT", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dw_bert_ausd_v_ta_period_task` | `DW.BERT_AUSD_V_TA_PERIOD` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | False | None | #REVIEW-STRUCT: launcher command `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` not recognised — confirm target operator/script manually. |

## 6. Task Dependency Map
Because this is a single-task DAG representing an isolated job, there is no task chain:
```python
dw_bert_ausd_v_ta_period_task
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or mutual-exclusion logic was provided. Concurrency is limited to:
- `max_active_runs=1` (as a safe default for standalone data jobs).

## 8. Error Handling and Retry Strategy
- Default failure handling is mapped via standard task retries (`retries=1`).
- No native UC4 postconditions or execution blocks were defined.
- Execution is synchronous (`wait_for_completion=True` equivalent).

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'AUSD_V_TA_PERIOD'` | Airflow Task Env Variable / Conf parameter |
| `&HOME` | Environment variable | Environment variable accessed via execution context |

## 10. Developer Notes
*   **#REVIEW-STRUCT: Unrecognized Launcher**: The original execution script relies on a local Korn-shell utility (`&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`). It has been mapped to an `EmptyOperator` stub. Developers should replace this with a `BashOperator` (running on an Airflow Worker/Runner with appropriate filesystem access) or migrate the underlying shell logic to an appropriate Cloud Operator (e.g., executing the SQL statements inside the script via a Cloud SQL/BigQuery operator).
*   **#REVIEW: Standalone Object Isolation**: This job was supplied without a parent `JOBP`. Verify where this task fits within the wider data warehouse orchestration pipeline. It may need to be integrated as a task in a parent DAG rather than living as a standalone DAG.
*   **Environment Sourcing**: The original script sources `$HOME/.dw_init` and includes header logic (`DW.HOLE_PFAD`). Ensure any target environment replication supports these dependencies or provides equivalent runtime environment variables.

---

# Numbered Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# REVIEW-STRUCT: If converting to BashOperator later, import:
# from airflow.operators.bash import BashOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP configurations identified in the extraction.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "DW.UNIX.ISBERT",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error callbacks specified in the source.

# ── DAG Definition ───────────────────────────────────────
# REVIEW: Created wrapper DAG for isolated JOBS_UNIX object
with DAG(
    dag_id="dw_bert_ausd_v_ta_period",
    default_args=DEFAULT_ARGS,
    description="Mirror Carmen period definitions",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # ── Guard Task ───────────────────────────────────────
    # None required (No self-lock Else=Skip sync detected)

    # ── Sensor Task ──────────────────────────────────────
    # None required (No earliest_start_time constraint)

    # ── Calendar Check Task ──────────────────────────────
    # None required (No calendar constraints detected)

    # ── Task: dw_bert_ausd_v_ta_period_task ──────────────
    # REVIEW-STRUCT: Launcher command is a direct KSH call:
    # &HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
    # Target execution environment must replicate $HOME/.dw_init sourcing.
    # Mapped to EmptyOperator stub per migration rules.
    dw_bert_ausd_v_ta_period_task = EmptyOperator(
        task_id="dw_bert_ausd_v_ta_period_task",
        # To resolve manually:
        # op_kwargs={
        #     "env": {
        #         "DWH_JOB_KENNUNG": "AUSD_V_TA_PERIOD"
        #     }
        # }
    )

    # ── Dependencies ─────────────────────────────────────
    # Single-task pipeline. No dependency chain required.
    dw_bert_ausd_v_ta_period_task
```

# Migration Design Document: DW.BERT_AUSD_V_TA_PERIOD

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` | `sql_bqsql_linked_job/DWH_BERT_JOB/dw_bert_ausd_v_ta_period.py` | Converts the UC4 Job XML into an Airflow DAG to orchestrate the downstream data pipeline steps. |

---

## Job Dependencies
* **Upstream**: 
  * `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh` — Already migrated separately as a shared utility (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/847). In the target environment, this is referenced as a shared Python module or utility in the DAG execution environment.

---

## Execution Order
The execution sequence of the legacy components must be preserved in the target Cloud Composer DAG task flow:
1. `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` (The orchestrating UC4 job — converted to the Airflow DAG wrapper)
2. `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` (The control and wrapper script — migrated in its own design pass to Python)
3. `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` (The contract data reconciliation wrapper script — migrated in its own design pass to Python)
4. `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` (The core SQL transformation script — migrated in its own design pass to BigQuery SQL/Dataform)

The target Airflow DAG will orchestrate this execution chain sequentially to maintain full visibility and execution integrity.

---

## Schedule & Variables — Must Be Retained
* **Scheduling**: The UC4 job indicates `schedule_interval=None` (no manual cron logic has been assumed or invented), and is triggered manually or by an external system scheduler.
* **Variables**:
  * `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'` — A job-specific environment parameter. It will be passed to the task execution environment using Airflow task `env` configurations or task parameter structures.

---

## Lineage
* **Upstream Components (Retired per Human-Confirmed Resolutions)**:
  * `UNRESOLVED:DW.HOLE_PFAD` — No source needed. Legacy path resolution is retired.
  * `UNRESOLVED:DW.BERT_LESE_LOG` — No source needed. Legacy log reading inclusion is retired.
  * `UNRESOLVED:.DW_INIT` — No source needed. Legacy environment initialization is retired.
* **Downstream Components (Triggered by this Job)**:
  * `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` — Triggered directly by the UC4 job execution. Maps to `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py` in its respective migration pass.
* **Infrastructure**:
  * Legacy execution host `dwhdwh1p` maps to Cloud Composer GKE worker nodes.
  * Login credential context `DW.UNIX.ISBERT` maps to the Composer service account role.

---

## External System Replacements
* **Legacy Unix Host (`dwhdwh1p`)**: Replaced by Google Cloud Composer (Airflow Workers).
* **Legacy Login User (`DW.UNIX.ISBERT`)**: Replaced by the dedicated GCP Service Account assigned to Cloud Composer for executing GKE/Dataform/BigQuery tasks.

---

## Cross-File Dependencies
* **Shared Utility Module**: Relies on the already migrated shared library `h_alis_sqlplus.ksh` (under `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh`), which is used downstream for executing BigQuery queries.
* **Core Pipeline Components**: Relies on the downstream KSH wrappers (`r_ausd_v_ta_period.ksh` and `k_ausd_v_ta_period.ksh`) and BigQuery SQL scripts, which are designed and converted in separate migration passes but are sequentially triggered by this DAG.

---

## Target File Plan
* **Target File Path**: `sql_bqsql_linked_job/DWH_BERT_JOB/dw_bert_ausd_v_ta_period.py`
* **Language**: Python (Apache Airflow DAG)
* **Source File**: `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml`
* *(No implementation code or pseudocode is provided here; the MCP tool's output is the authoritative reference for the DAG structure).*

---

## Environment-Specific Values

### 1. GLOBAL (Environment-Wide)
These values identify target infrastructure and remain consistent across all jobs in a given environment (dev/test/prod):
* `GCP_PROJECT` — The target Google Cloud Project ID where Composer and BigQuery resources reside.
* `GCP_REGION` — The target GCP region for Composer/BigQuery execution.
* `Composer/Airflow Service Account` — The GCP Service Account replacing the legacy Unix login user `DW.UNIX.ISBERT`.

### 2. JOB-SPECIFIC
These values are particular to this orchestration job:
* `DWH_JOB_KENNUNG` — Retained with its value `'AUSD_V_TA_PERIOD'`, passed directly to the task execution environment.

---

## Risks and Manual Steps
* **Downstream Deployment Order**: The Airflow DAG orchestrates `r_ausd_v_ta_period.py` and its children. Because these downstream files are converted in separate migration passes, they must be deployed and validated in the environment before this orchestrating Airflow DAG is enabled.
* **Sourcing Environment Removal**: The legacy job sourced `.dw_init` and included `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` (all human-confirmed as retired/not needed). Ensure that any legacy environment variables or connection handles previously set by these inclusions (such as Oracle database connection details) are successfully mapped to native Airflow Connections and BigQuery credentials.

---

=== FILE: sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh ===
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
v_TabName='ta_period'

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
Name_SQLskript="${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql"

# Temporares File fuer die Zahl der Records
tmpFile="$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp"

# *******************************************************

# DB-Script ausfuehren
# hierbei werden aktive Jobs ignoriert 
starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung 

print " ---------- ENDE Datenverarbeitung ----------"

# Hole Zahl der Bereitgestellten Records
eval "v_records=`cat $tmpFile`"





=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script parses command-line arguments using getopts, validates inputs with custom error handlers, and reads a temporary file containing process metrics, making Python the appropriate target.

EVIDENCE
- Business logic found: KSH custom logic for parameter validation, setting up temporary tracking files, launching an external SQL script, and reading the resulting record count from a temporary file.
- AWK: none
- SQL-expressible: No. The script relies on shell-level parameter parsing, temporary file reading, and external process execution whose SQL logic is not inline.
- Non-SQL side effects: Reads a temporary file whose path contains a dynamic PID ($$).
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`k_ausd_v_ta_period.ksh`) is a control wrapper for database processing related to the contract evaluation process (`r_ausd_vertrag.ksh`). It parses and validates command-line parameters (Job ID and entry number), initiates an external database SQL script (`d_ausd_v_ta_period.sql`) via a utility launcher, and reads a temporary file to capture the count of records processed. This script ensures that database updates are executed only when the required parameter context is valid.

2. INVOCATION CONTEXT
   - Who calls this script: Typically invoked by a parent workflow/framework script (indicated by the usage message "Bitte ueber Rahmenscript aufrufen"). No specific UC4 job extraction was supplied.
   - UC4 includes: None referenced in the extraction.
   - Environment files sourced:
     * `. $HOME/.dw_init` — # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` — # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` — # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` — # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` — # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `j` (maps to `p_JobKennung`): Command-line option. Job identifier. Required for database updates. Surfaced in Python via `argparse` as `--job-kennung` / `-j`.
   - `f` (maps to `p_EintragsNr`): Command-line option. Entry tracking number. Required for database execution. Surfaced in Python via `argparse` as `--eintrags-nr` / `-f`.
   - `h`: Command-line option. Displays usage message ("Bitte ueber Rahmenscript aufrufen") and exits. Surfaced via native `argparse` help.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
     * Exact command: `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
     * Purpose: Runs the SQL script `d_ausd_v_ta_period.sql` with specific parameters (entry number and job identifier).
     * Action: Remain as an external process call since `starteSQLSkript` is an opaque shell-defined utility function whose connection configurations and internal logic are unsupplied.
     * Resolvable Launcher: No. Although the targets are SQL files, the utility `starteSQLSkript`'s complete environment, logging behavior, and error mechanics are hidden.
     * Flag: # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - Source file: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql`
   - SQL Text: Not supplied in this extraction.
   - Statement type: Unknown (External SQL script).
   - Tables touched: `ta_period` (inferred from `v_TabName='ta_period'`).
   - Dialect: Unconfirmed, but sourcing `h_alis_sqlplus.ksh` strongly implies Oracle SQL*Plus.
   - Flag: # REVIEW-STRUCT: SQL script [d_ausd_v_ta_period.sql] not supplied — behavior and query details unknown

6. CONTROL FLOW
   1. **Environment Setup**: Sourced initialization files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) to establish environment variables and validation routines.
   2. **Parameter Parsing**: Uses `getopts` to parse options `-j` and `-f`. If `-h` is passed, prints help and exits. Unknown options set `ErrNr` to `192` or `193`.
   3. **Variable Assignment**: Sets `v_TabName` to `'ta_period'`.
   4. **Parameter Validation**: Temporarily disables automatic exit (`set +e`). Asserts that both `p_JobKennung` and `p_EintragsNr` are set using custom function `pruefeParameterGesetzt`. If validation fails, triggers `DWMSG_MeldeFehler` and exits with the validation error code.
   5. **Environment Configuration**: Re-enables strict error execution (`set -eu`). Sources `h_alis_sqlplus.ksh` to load the database execution framework.
   6. **Temporary File Setup**: Defines `tmpFile` as `$DW_DIR_UTL/bert_k_ausd_v_ta_period_<PID>.tmp`.
   7. **Database SQL Script Execution**: Invokes `starteSQLSkript` with parameter parameters.
   8. **Metrics Capture**: Reads the output of `tmpFile` into the `v_records` variable using a command substitution block.

7. ERROR HANDLING & EXIT CODES
   - Missing required arguments (`-j`, `-f`) triggers `ErrNr=193` or validation error paths.
   - Unknown arguments trigger `ErrNr=192`.
   - Parameter failures invoke `DWMSG_MeldeFehler` to output standardized errors before exiting with the corresponding `ErrNr`.
   - Post-validation, the script sets `set -eu` to ensure any command failure (like database execution) causes immediate script termination.
   - Python equivalent: Map parameters validation to `argparse` required validations. For custom validation errors, throw structured exceptions or invoke `sys.exit(error_code)` following the original error codes.

8. OUTPUTS / SIDE EFFECTS
   - Writes to/Modifies: Database tables modified by the external SQL script `d_ausd_v_ta_period.sql`.
   - Creates/Modifies: A temporary file containing process metrics located in `$DW_DIR_UTL`.

9. BUSINESS SUMMARY
   - Coordinates the setup and validation of execution parameters for contract period analysis tasks.
   - Ensures rigorous parameter validation to protect database transactions from running with incomplete parameters.
   - Triggers targeted database operations via external SQL files using a centralized DB-script launcher.
   - Captures process performance and execution metrics (number of processed rows) from physical files for downstream tracking.

=== PSEUDOCODE ===

```python
import os
import sys
import argparse
import subprocess

# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables it sets are unknown; do not guess their names or values

# Step 1: Environment Setup & Variable Initialization
BERT_DIR_ROOT = os.environ.get("BERT_DIR_ROOT", "")
DW_DIR_UTL = os.environ.get("DW_DIR_UTL", "/tmp")
v_TabName = "ta_period"
PID = os.getpid()
tmpFile = os.path.join(DW_DIR_UTL, f"bert_k_ausd_v_ta_period_{PID}.tmp")
Name_SQLskript = os.path.join(BERT_DIR_ROOT, "aufbereitung/sql/d_ausd_v_ta_period.sql")

def melde_fehler(err_nr, err_arg):
    # Mimics the shell utility 'DWMSG_MeldeFehler'
    print(f"FEHLER: 0 E {err_nr} {err_arg}", file=sys.stderr)
    print("Bitte ueber Rahmenscript aufrufen")
    sys.exit(err_nr)

# Step 2: Parameter Parsing
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("-j", "--job-kennung", dest="p_JobKennung", default=None)
parser.add_argument("-f", "--eintrags-nr", dest="p_EintragsNr", default=None)
parser.add_argument("-h", "--help", action="store_true")

try:
    args, unknown = parser.parse_known_args()
except Exception as e:
    # Error code 192 matches original 'Parameter unbekannt' error handling
    melde_fehler(192, str(e))

if args.help:
    print("Bitte ueber Rahmenscript aufrufen")
    sys.exit(0)

# Step 3: Parameter Validation
p_JobKennung = args.p_JobKennung
p_EintragsNr = args.p_EintragsNr

if not p_JobKennung:
    # Error code 193 matches 'Notwendiges Argument fehlt'
    melde_fehler(193, "Jobkennung")

if not p_EintragsNr:
    melde_fehler(193, "EintragsNr")

# Step 4: DB-Script Execution
# REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
# REVIEW-STRUCT: SQL script d_ausd_v_ta_period.sql not supplied — behaviour and query details unknown
try:
    # Invoking the launcher process
    cmd = [
        "starteSQLSkript",
        p_EintragsNr,
        Name_SQLskript,
        p_EintragsNr,
        p_JobKennung
    ]
    subprocess.run(cmd, check=True)
except subprocess.CalledProcessError as e:
    print(f"Database execution failed: {e}", file=sys.stderr)
    sys.exit(e.returncode)

print(" ---------- ENDE Datenverarbeitung ----------")

# Step 5: Capture Process Metrics (Row count)
v_records = 0
if os.path.exists(tmpFile):
    try:
         with open(tmpFile, "r") as f:
             v_records = f.read().strip()
    except Exception as e:
         # REVIEW: Ambiguity regarding metrics file error severity. Continuing with 0 if file is unreadable.
         print(f"WARNING: Could not read temporary metrics file {tmpFile}: {e}", file=sys.stderr)
else:
    # REVIEW: Metrics file was not generated by starteSQLSkript; logging warning
    print(f"WARNING: Temporary metrics file {tmpFile} does not exist", file=sys.stderr)

print(f"Processed records count: {v_records}")
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` | `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py` | Converted to a Python wrapper script to handle command-line parameter parsing, validation, and control execution flow. |

***

### Job dependencies
* **Upstream**: 
  * Shared Files — `sql_bqsql_linked_job/isbert/allgemein/is/util/bin` has been previously migrated and merged (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/847`). This utility suite contains `h_alis_sqlplus.ksh` which contains database execution logic.

### Execution order
To preserve the legacy dependency graph, the orchestrator (Cloud Composer / Airflow DAG) must execute tasks in the following sequential order:
1. **Orchestration Task**: Initiated by `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml`
2. **Parent Wrapper Task**: Executes the parent script `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` (or its migrated Python equivalent).
3. **Control Wrapper Task (This Job)**: Executes our migrated python script `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py`.
4. **SQL Execution Task**: Executes the target BigQuery transformation derived from `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`.

### Schedule & variables
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'` (derived from `DW.BERT_AUSD_V_TA_PERIOD`).
* **Target Delivery**: This variable must be provided at runtime. In Airflow, this can be passed using the DAG configuration (`params={"DWH_JOB_KENNUNG": "AUSD_V_TA_PERIOD"}`) or mapped directly via command-line arguments (`-j AUSD_V_TA_PERIOD`) into the Python wrapper script.

### Lineage
* **Upstream Producers (Inputs / Utilities)**:
  * `.DW_INIT` (Environment initialization; human-reviewed: **NO SOURCE NEEDED**).
  * `H_ALIS_PARAMETER.KSH` (Command parameter utility; human-reviewed: **NO SOURCE NEEDED**).
  * `H_ALIS_DATE.KSH` (Date validation helper; human-reviewed: **NO SOURCE NEEDED**).
  * `F_ALIS_MSGERR.KSH` (Standard error logging framework; human-reviewed: **NO SOURCE NEEDED**).
  * `h_alis_sqlplus.ksh` (Database launcher module; migrated separately under shared files).
* **Downstream Consumers (Outputs / Side Effects)**:
  * `d_ausd_v_ta_period.sql` (The database execution SQL target; cross-job/cross-file hand-off).

### External system replacements
* **Oracle SQL\*Plus Execution**: The call to `starteSQLSkript` (defined in `h_alis_sqlplus.ksh`) which ran SQL via SQL\*Plus on Oracle, is replaced with BigQuery API calls using the `google-cloud-bigquery` library or Airflow's BigQuery/Dataform operators executing the translated SQL target.

### Cross-file dependencies
* The script depends on the SQL logic within `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` (which is migrated separately).
* It relies on parameters and functions historically provided by the shared module `h_alis_sqlplus.ksh`.

### Target file plan
* **Target File Path**: `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py`
  * **Language**: Python
  * **Source File**: `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh`
  * **Description**: Python executable script mirroring the control flow, validation rules, and database script trigger interface.

### Environment-specific values
* **GCP_PROJECT** (GLOBAL): Google Cloud Project ID hosting the target BigQuery datasets. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow variables.
* **GCP_REGION** (GLOBAL): The execution region for BigQuery resources. Sourced via `os.environ.get("GCP_REGION")`.
* **DW_DIR_UTL** (GLOBAL): Target environment temporary or utility tracking folder. Sourced via `os.environ.get("DW_DIR_UTL", "/tmp")`.
* **BERT_DIR_ROOT** (GLOBAL): Root path of the migrated script folder structure. Sourced via `os.environ.get("BERT_DIR_ROOT")`.
* **DWH_JOB_KENNUNG** (JOB-SPECIFIC): Specific execution code for this job. Handled via command arguments (`-j`) with a fallback default value of `'AUSD_V_TA_PERIOD'`.

### Risks and manual steps
* **SQL Target Association**: The SQL script `d_ausd_v_ta_period.sql` is not in this design pass's scope. A manual check must ensure that the BigQuery query run by this python wrapper points to the correct migrated SQL target on GCP (e.g. standard BigQuery SQL or Dataform).
* **Replaced Shell Utilities**: Environment files and utilities (`.DW_INIT`, `F_ALIS_MSGERR.KSH`, `H_ALIS_DATE.KSH`, `H_ALIS_PARAMETER.KSH`) are marked as "NO SOURCE NEEDED" by human-confirmed resolutions. Their legacy capabilities (standard logging, argument parsing, error thresholds) must be handled natively in Python using libraries such as `argparse`, `logging`, and `datetime`.
* **Output/Print Literal Rule**: The output messages `"Bitte ueber Rahmenscript aufrufen"`, `"FEHLER: 0 E ..."`, and `" ---------- ENDE Datenverarbeitung ----------"` must remain exactly in German inside the Python file to avoid disrupting downstream execution flows and automated log parsing utilities.

---

=== FILE: sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh ===
#!/bin/ksh

# Zweck:
#    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period
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
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.
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

Name_Kernskript="${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh"


####################
# Fehlermeldekonzept
####################
typeset -u JobKennung="BERT_V_TA_PERIOD"
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
REASON: The script contains complex orchestration logic, command-line parameter parsing via getopts, signal traps for error handling, and invokes custom database logging utilities and a child shell script.

EVIDENCE
- Business logic found: KSH custom logic orchestrates the environment setup, initializes custom error tracking and logging registration (`DWMSG_*` utility functions), configures signal traps, and launches a child kernel script (`k_ausd_v_ta_period.ksh`).
- AWK: none
- SQL-expressible: No, this is an orchestration wrapper and logging controller with no direct SQL.
- Non-SQL side effects: Writes execution logs to a file (`$LogDatei`), registers job runs and states via external DB-logging utility scripts, and executes external child scripts.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script serves as the master orchestration wrapper ("Rahmenskript") for the contract data reconciliation process ("Vertragsdatenabgleich") targeting the `ta_period` table. Its main purpose is to initialize the execution environment, parse input parameters, register the job run with a centralized logging database framework, monitor execution via signal traps, and trigger the core kernel execution script (`k_ausd_v_ta_period.ksh`).

2. INVOCATION CONTEXT
   - Called by: Typically scheduled and triggered via Automic/UC4 or run manually from the command line.
   - Command line: `r_ausd_v_ta_period.ksh [-h] [-s <val>] [-l <val>]`
   - UC4 Includes Referenced: None.
   - Environment files sourced:
     * `. $HOME/.dw_init`
       # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
       # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — functions it defines (such as DWMSG_*) are unknown; behavior must be stubbed or mapped to a logging module
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
       # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables/functions it defines are unknown
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
       # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables/functions it defines are unknown

3. PARAMETERS / INPUTS
   - `s`: Configured in `ParamList="s:l:"`. Source is a command-line argument. It is declared in `getopts` but not explicitly routed in the legacy `case` statement.
     # REVIEW: Parameter 's' is defined in the getopts string but has no matching branch in the case block. Confirm if it should be captured and forwarded to the child script.
   - `l`: Configured in `ParamList="s:l:"`. Source is a command-line argument. It is declared in `getopts` but not explicitly routed in the legacy `case` statement.
     # REVIEW: Parameter 'l' is defined in the getopts string but has no matching branch in the case block. Confirm if it should be captured and forwarded to the child script.
   - `h`: Triggers the help/usage page. Map to Python `argparse` built-in help handling.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `date +%d%m%Y`: Used to fetch current system date. Convert to native Python `datetime`.
   - `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh`: The kernel script executed to perform the actual reconciliation.
     * Command: `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`
     * Purpose: Runs core reconciliation logic for `ta_period`.
     * Native Python or subprocess: Must remain a subprocess call since it is an external shell script whose internal logic is not supplied.
     * Resolvable Launcher: No, this is an external `.ksh` orchestration step.
     # REVIEW-STRUCT: launcher [k_ausd_v_ta_period.ksh] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   (No inline SQL or SQL files are present inside this wrapper script).

6. CONTROL FLOW
   1. **Initialization**: Initialize variables (`ErrNr = 0`, `ErrArg = ""`, `ErrVal = 0`, `DW_EintragsNr = 0`).
   2. **Environment Setup**: Source `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`. Set shell options to exit on error or unset variables (`set -eu`).
   3. **Argument Parsing**: Evaluate arguments via `getopts`. If `-h` is supplied, execute `usage()` and exit. Catch unknown options or missing option arguments, setting `ErrNr` to `192` or `193`.
   4. **Pre-execution Parameter Validation**: If `ErrNr != 0`, invoke logging utility `DWMSG_MeldeFehler` with parameters, output usage, and exit.
   5. **Variable Definitions**: Define `Name_Kernskript` path, set `JobKennung = "BERT_V_TA_PERIOD"`, and calculate `v_sysdate` (`date +%d%m%Y`).
   6. **Logging Registration**:
      - Call `DWMSG_ErmittleNr` to obtain a run/entry ID (`DW_EintragsNr`).
      - Call `DWMSG_Logdateiname` to fetch/generate the absolute path for the log file (`LogDatei`).
      - Call `DWMSG_ErzeugeEintrag` to initialize the database log entry, appending output to `$LogDatei`.
      - Call `DWMSG_SetzeStichtagInfo` to associate the runtime business date.
   7. **Signal Trapping**: Register signal handlers for interrupts (`INT`) and execution errors (`ERR`) to invoke `DWMSG_Fehlerbehandlung` and exit with an error.
   8. **Print Banner**: Write execution header block to standard output.
   9. **Kernel Execution**: Execute `${Name_Kernskript}` passing the job identity parameters, redirecting stdout/stderr to `$LogDatei`.
   10. **Success Cleanup**: Write a completion message, call `DWMSG_SetzeStatusOK` to mark the run successful, deactivate traps, and exit with status code `0`.

7. ERROR HANDLING & EXIT CODES
   - Command errors are caught by `set -eu` and mapped to the `ERR` trap.
   - Signal Interrupt (`INT`) trap: Runs `DWMSG_Fehlerbehandlung`, prints `OSError: Abbruch`, and exits with code `1`.
   - Script Failure (`ERR`) trap: Runs `DWMSG_Fehlerbehandlung`, prints `AppError: Abbruch`, and propagates failure.
   - Initial Parameter Validation failure: Exits with `192` or `193` depending on the getopts parsing outcome.
   - Success execution: Exits with code `0`.
   - Python mapping: Wrap main execution block in a `try...except` statement. Map `INT` to `KeyboardInterrupt`, and subprocess failures to `subprocess.CalledProcessError`. Run the python equivalents of `DWMSG_*` error reporting inside exception handlers.

8. OUTPUTS / SIDE EFFECTS
   - Writes log outputs to the path resolved in `$LogDatei`.
   - Modifies job run status inside the database/logging framework via external `DWMSG_*` utilities.

9. BUSINESS SUMMARY
   - Orchestrates the daily contract data reconciliation processing steps for the table `ta_period`.
   - Provides standardized system logging, exit code control, and database run-status updates.
   - Parses incoming runtime arguments and validates environmental settings before running the sub-process.
   - Isolates operational error trapping from business-level logic.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import argparse
import subprocess
import datetime

# Sourced environment constants (Mock representations of values provided by sourced scripts)
# # REVIEW-STRUCT: environment variables and paths are inferred from legacy shell initializations
BERT_DIR_ROOT = os.environ.get("BERT_DIR_ROOT", "/opt/bert")
HOME = os.environ.get("HOME", "/home/bert")

# Define external script paths
NAME_KERNSKRIPT = os.path.join(BERT_DIR_ROOT, "aufbereitung/bin/k_ausd_v_ta_period.ksh")

# Helper stubs for unsupplied external DWMSG utilities
# # REVIEW-STRUCT: DWMSG_* functions originate from unsupplied f_alis_msgerr.ksh. Implementations must be resolved against the existing enterprise logging modules.
def dwmsg_melde_fehler(eintrags_nr, severity, err_nr, err_arg):
    print(f"ERROR: {severity} {err_nr} {err_arg}", file=sys.stderr)

def dwmsg_ermittle_nr():
    # Simulated generation of a run/tracking ID
    return 12345

def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # Simulated logfile path generation
    return os.path.join(BERT_DIR_ROOT, "log", f"{job_kennung}_{eintrags_nr}.log")

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, script_name, log_datei):
    # Register job entry in logging DB
    pass

def dwmsg_setze_stichtag_info(eintrags_nr, sysdate, date_format):
    # Set the execution business date context
    pass

def dwmsg_fehlerbehandlung(eintrags_nr, log_file_path):
    # Performs database error state logging on failure
    with open(log_file_path, "a") as f:
        f.write(f"DWMSG: Registering execution failure for job {eintrags_nr}\n")

def dwmsg_setze_status_ok(eintrags_nr, log_file_path):
    # Mark job as successfully completed in database
    with open(log_file_path, "a") as f:
        f.write(f"DWMSG: Registering execution success for job {eintrags_nr}\n")


def usage():
    help_text = """
    Programm: Vertragsdatenabgleich
    Version:  V1.0.0
    Aufruf:   r_ausd_v_ta_period.py [-s <val>] [-l <val>]
    Parameter:
        -h, --help     zeigt diese Seite an

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.
    """
    print(help_text)


def main():
    # Step 1 & 2: Environment setup and custom parameter mapping
    # Note: KSH defined ParamList="s:l:". We replicate this using argparse.
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--help", action="store_true")
    parser.add_argument("-s", required=False)
    parser.add_argument("-l", required=False)
    
    try:
        args, unknown = parser.parse_known_args()
    except Exception as e:
        # Step 3 & 4: Parameter error handling
        # Emulate getopts failure behavior
        err_nr = 192  # Unknown parameter
        dwmsg_melde_fehler(0, "E", err_nr, str(e))
        usage()
        sys.exit(err_nr)

    if args.help:
        usage()
        sys.exit(0)

    # Step 5: Declare constants and runtime parameters
    job_kennung = "BERT_V_TA_PERIOD"
    v_sysdate = datetime.date.today().strftime("%d%m%Y")
    
    # Step 6: Initialize logging metadata
    dw_eintrags_nr = dwmsg_ermittle_nr()
    log_datei = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)
    
    # Ensure log directory exists
    log_dir = os.path.dirname(log_datei)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
        
    dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, sys.argv[0], log_datei)
    dwmsg_setze_stichtag_info(dw_eintrags_nr, v_sysdate, "DDMMYYYY")

    # Step 7: Print runtime metadata block
    banner = f""" ----------------- Job -----------------------
 Job-Nr    : '{dw_eintrags_nr}'
 JobKennung: '{job_kennung}'
 Logdatei  : '{log_datei}'
 ---------------------------------------------"""
    print(banner)
    with open(log_datei, "a") as f:
        f.write(banner + "\n")

    # Step 8 & 9: Invoke core execution block with Exception / Trap management
    try:
        # Build command invocation
        cmd = [NAME_KERNSKRIPT, "-j", job_kennung, "-f", str(dw_eintrags_nr)]
        
        # # REVIEW: Forwarding optional s and l parameters if supplied in command line
        if args.s:
            cmd.extend(["-s", args.s])
        if args.l:
            cmd.extend(["-l", args.l])

        # Execute child launcher, redirecting stdout/stderr to the logfile
        with open(log_datei, "a") as log_out:
            subprocess.run(cmd, stdout=log_out, stderr=subprocess.STDOUT, check=True)

        # Step 10: Successful completion logging and status registration
        success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
        print(success_msg)
        with open(log_datei, "a") as f:
            f.write(success_msg + "\n")
            
        dwmsg_setze_status_ok(dw_eintrags_nr, log_datei)
        sys.exit(0)

    except KeyboardInterrupt:
        # Step 7 (INT trap equivalent)
        dwmsg_fehlerbehandlung(dw_eintrags_nr, log_datei)
        print("OSError: Abbruch", file=sys.stderr)
        sys.exit(1)
        
    except (subprocess.CalledProcessError, Exception) as err:
        # Step 7 (ERR trap equivalent)
        dwmsg_fehlerbehandlung(dw_eintrags_nr, log_datei)
        print(f"AppError: Abbruch - {str(err)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` | `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py` | Converted to a Python orchestration wrapper to initialize logging, parse arguments, and run the kernel script on Cloud Composer. |

### Job Dependencies
* **Upstream**:
  * **Shared Files** (`sql_bqsql_linked_job/isbert/allgemein/is/util/bin`): Already migrated and merged under PR `https://github.com/gurunathan-prodapt/pi-agents/pull/847`. Converted utilities (such as date and parameter handling helpers) should be referenced or imported in the target script as necessary.
* **Downstream**:
  * **Core Kernel Script** (`k_ausd_v_ta_period.ksh`): This script runs immediately downstream of the wrapper. Since it belongs to a separate execution group, its migration is handled in its own dedicated design pass. The python script wrapper will trigger `k_ausd_v_ta_period.py` once that file is migrated.

### Execution Order
The target orchestration (Airflow DAG) must preserve the 4-step execution order from the legacy dependency graph:
1. `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` (UC4 Orchestration Trigger)
2. `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` (Wrapper script - this script)
3. `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` (Core KSH launcher called by wrapper)
4. `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` (Core SQL script called by launcher)

### Scheduling
* **Trigger Event**: Legacy execution is scheduled and triggered as UC4 job `DW.BERT_AUSD_V_TA_PERIOD`. In the target environment, this orchestration sequence will be mapped to a Cloud Composer (Airflow) DAG scheduled to run with equivalent cron settings or upstream dataset trigger sensors.

### Schedule & Variables
* **Scheduler-set Variables**:
  * `DWH_JOB_KENNUNG = 'AUSD_V_TA_PERIOD'`: This scheduler variable must be retained. It will be mapped to an Airflow Variable or configured via DAG parameters (`params`) and fed to the Python script at runtime.

### Lineage
* **Upstream Configs / Utility Scripts**:
  * Config `.DW_INIT` (lineage edge: `USES_CONFIG`) and utility scripts `F_ALIS_MSGERR.KSH`, `H_ALIS_PARAMETER.KSH`, and `H_ALIS_DATE.KSH` (lineage edge: `INVOKES`) are human-confirmed as **NO SOURCE NEEDED** (representing legacy utility infrastructure). Their environment-setting and parsing functionalities are replaced with Python native modules like `argparse`, `datetime`, and environment variables.
* **Downstream Consumers**:
  * `k_ausd_v_ta_period.ksh` (lineage edge: `INVOKES`): Called directly by this wrapper to execute the core data load.

### External System Replacements
* **Legacy Logging Framework**: The custom logging DB framework utilities (`DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`, etc.) are replaced by native Cloud Composer task logs, Python's `logging` API, and/or standard Cloud Logging mechanisms.
* **File Redirection**: Local log files (`$LogDatei`) are redirected to standard output or a dedicated Google Cloud Storage (GCS) logging bucket.

### Cross-File Dependencies
* **Core Launcher dependency**: The wrapper script has a direct dependency on `k_ausd_v_ta_period.ksh` (migrating to `k_ausd_v_ta_period.py`). The Python script will call `k_ausd_v_ta_period.py` as a subprocess or via a native Python module import.

### Target File Plan
| Target File Path | Language | Source File Path | Purpose |
| :--- | :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py` | Python | `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` | Converted Python orchestrator wrapper responsible for logging initialization, CLI parameter parsing, exception tracking, and executing the core kernel. |

### Environment-Specific Values
* **GLOBAL**:
  * `BERT_DIR_ROOT`: Sourced via `os.environ.get("BERT_DIR_ROOT")` to identify the base deployment directory.
  * `HOME`: Sourced via `os.environ.get("HOME")` for environment compatibility.
* **JOB-SPECIFIC**:
  * `JobKennung`: Defined inline as `"BERT_V_TA_PERIOD"`.
  * `DWH_JOB_KENNUNG`: Derived from the scheduler variable `'AUSD_V_TA_PERIOD'` and passed to the script.
  * `LogDatei`: Resolved dynamically at runtime based on the calculated run entry ID (`DW_EintragsNr`).

### Risks and Manual Steps
* **Downstream Script Dependency**: The Python script wrapper is configured to execute the downstream script `k_ausd_v_ta_period.py`. It is critical to verify that the core script has been migrated in its respective pass and that its execution path matches the invocation in `r_ausd_v_ta_period.py`.
* **Sourced Environment Files**: The shell scripts sourced in the legacy code (`.DW_INIT`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are marked as "NO SOURCE NEEDED" by human-review. A manual review must confirm that any global configurations or environment values previously supplied by `.DW_INIT` are properly mapped as environment variables or Airflow Variables in Cloud Composer.

---

=== FILE: sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql ===
-- ===================================================================
-- Datei:  d_ausd_v_ta_period.sql
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
DEFINE v_carmen       = "@pcrs1.de.tinternal.com"
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
SPOOL ./tmp/trace_d_ausd_v_ta_period

WHENEVER SQLERROR CONTINUE
  SET TIMING ON
  SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT FAILURE
--
--
prompt tabelle von vorherigem lauf loeschen
--
--
WHENEVER SQLERROR CONTINUE
begin 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_period'); 
end;
/

WHENEVER SQLERROR EXIT FAILURE
--
--
prompt zieltabelle anlegen: carmen-period-tabelle
--
--
INSERT  INTO sof$ta_period(
        period_id,
        number_time_measurement,
        time_meas_cv,
        einheit,
        bfc_age
)
SELECT
        p.period_id,
        p.number_time_measurement,
        p.time_meas_cv,
        d.description,
        p.insert_at
FROM
        cds$ta_period           &v_carmen       p,
        CDS$TA_TIME_MEAS_CV     &v_carmen       tm,
        cds$ta_description      &v_carmen       d
WHERE
        tm.time_meas_cv   = p.time_meas_cv
AND     tm.DESCRIPTION_ID = d.DESCRIPTION_ID
AND
        p.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
AND     (   p.modified_at IS NULL
         OR p.modified_at > TO_DATE('&v_datum','YYYYMMDD'));

commit;

prompt Verarbeitung fehlerfrei beendet.
spool off


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object:
    - Multi-statement SQL*Plus and PL/SQL integration script (variable extraction, dynamic table truncation, and an INSERT-SELECT DML operation utilizing database links).

1.2 Business Logic and Purpose:
    - This script processes historical contract periods from a source system (Carmen database via DB Link) and loads them into a local target table (`sof$ta_period`).
    - First, it dynamically retrieves a cutoff date (`v_datum`) from a logging/notifications table (`isbert_schema.dwtk_meldungen`) based on a specific job code.
    - Second, it truncates the target table `sof$ta_period` using an administrative utility procedure.
    - Lastly, it performs an incremental/conditional load of period and time-measurement metadata from the Carmen system. The loaded periods must be active on or before the cutoff date (`insert_at <= v_datum`) and either not yet modified, or modified after the cutoff date (`modified_at > v_datum`).

1.3 Entities Referenced:
    - `isbert_schema.dwtk_meldungen` (Source metadata table)
    - `sof$ta_period` (Target table)
    - `cds$ta_period@pcrs1.de.tinternal.com` (Remote source table - period master)
    - `CDS$TA_TIME_MEAS_CV@pcrs1.de.tinternal.com` (Remote source table - time-measure lookup)
    - `cds$ta_description@pcrs1.de.tinternal.com` (Remote source table - description text)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (`timecreated`, `insert_at`, `modified_at`) contains both date and time components. In BigQuery, these will map to `TIMESTAMP` to preserve temporal precision.
    - Oracle `NUMBER` (`period_id`, `number_time_measurement`) will map to `INT64` for ID fields and `NUMERIC` for metric/measurement quantities unless they are explicitly integers.
    - Oracle `VARCHAR2` (`time_meas_cv`, `description`) will map to `STRING`.

2.2 Implicit and Explicit Type Casting:
    - In Oracle, the comparison `p.insert_at <= TO_DATE('&v_datum','YYYYMMDD')` dynamically converts the SQL*Plus substitution string `&v_datum` into a date. In BigQuery, this will be handled via an explicit `PARSE_TIMESTAMP('%Y%m%d', v_datum)` comparison against the scripting variable.

2.3 NULL Handling and Conditional Functions:
    - `NVL(TO_CHAR(MAX(...)), '19000101')` is replaced by `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(...)), '19000101')`.

2.4 String Functions:
    - `TO_CHAR(max_date, 'YYYYMMDD')` is replaced by `FORMAT_TIMESTAMP('%Y%m%d', max_date)`.

2.5 Date and Timestamp Functions:
    - `TO_DATE('&v_datum', 'YYYYMMDD')` is replaced by `PARSE_TIMESTAMP('%Y%m%d', v_datum)` to guarantee type compatibility with the source columns mapped to `TIMESTAMP`.

2.10 Sequences / Unique IDs:
    - Not applicable in this script.

2.11 MERGE / Dynamic DML:
    - The truncation of `sof$ta_period` via dynamic PL/SQL block execution (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement(...)`) is replaced with a direct and native BigQuery standard SQL `TRUNCATE TABLE` command.

2.15 Unresolvable or Advisory Items:
    - Database Link `@pcrs1.de.tinternal.com` cannot be natively resolved in BigQuery. The schema/tables (`cds$ta_period`, etc.) must be replicated into a BigQuery dataset (e.g., `carmen_replicated`) prior to execution. This is treated as a dependency requiring manual or external orchestration.

Step 3: Conversion Strategy Summary
3.1 Overall Approach:
    - Refactor the SQL*Plus and PL/SQL wrapper into a native BigQuery SQL scripting block (`DECLARE ... SET ... TRUNCATE ... INSERT`).
3.2 Assumptions:
    - All remote tables accessible via the `@pcrs1.de.tinternal.com` DB Link have been replicated or exposed as tables within a BigQuery dataset named `carmen_replicated_dataset`.
    - Target table `sof$ta_period` exists in the `target_dataset`.
3.3 Flagged Items:
    - The DB link `@pcrs1.de.tinternal.com` requires an external ETL pipeline (such as a Python/Cloud Composer job) to ingest the tables before this script runs.

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| SQL*Plus Variable definition (`&v_datum`) | BigQuery Scripting Variable (`DECLARE v_datum STRING;`) | Hardcoded values, Python execution parameters | Scripting allows seamless dynamic variable assignment inside a single transactional block or session. |
| DB Link `@pcrs1.de.tinternal.com` | Replicated Dataset Reference | BQ External Connections (Federated Queries) | Federated query connections can suffer from performance/network latency issues on large transactional datasets. Replicating the tables to local BQ storage is optimal. |
| Dynamic PL/SQL Truncate Wrapper | Native BigQuery SQL (`TRUNCATE TABLE`) | Python wrapper, BigQuery EXECUTE IMMEDIATE | Native `TRUNCATE TABLE` is fully supported in BigQuery DDL, rendering dynamic SQL wrappers obsolete. |
| `NVL` / `TO_CHAR` | `COALESCE` / `FORMAT_TIMESTAMP` | UDF, `CASE` statement | Built-in standard functions are more performant and maintainable than custom UDFs. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════

- **BigQuery SQL Script**: A single SQL file containing the variable declaration, initialization query, table truncation, and final insert statement.
- **Python / Cloud Data Fusion Pipeline (Pre-requisite)**: An orchestration script or ingestion workflow to replicate the following remote tables from Oracle to BigQuery:
  - `cds$ta_period`
  - `CDS$TA_TIME_MEAS_CV`
  - `cds$ta_description`

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Source Oracle Table | Source Column | Source Type | BigQuery Target Type | Conversion Rule / Warnings |
| :--- | :--- | :--- | :--- | :--- |
| `dwtk_meldungen` | `timecreated` | `DATE` | `TIMESTAMP` | Explicit cast using `FORMAT_TIMESTAMP` to string variable. |
| `cds$ta_period` | `period_id` | `NUMBER` | `INT64` | Inferred integer identifier. No precision loss. |
| `cds$ta_period` | `number_time_measurement` | `NUMBER` | `NUMERIC` | Preserves scale and precision of potential decimal time values. |
| `cds$ta_period` | `time_meas_cv` | `VARCHAR2` | `STRING` | Standard string representation. |
| `cds$ta_period` | `insert_at` | `DATE` | `TIMESTAMP` | Converted to TIMESTAMP to handle time portions safely. |
| `cds$ta_period` | `modified_at` | `DATE` | `TIMESTAMP` | Converted to TIMESTAMP. Handle NULLs in conditional checks. |
| `cds$ta_description` | `description` | `VARCHAR2` | `STRING` | Standard string representation. |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: SQL*Plus variables (`DEFINE`, `COLUMN`), Oracle DB Links, dynamic PL/SQL wrapper execution, explicit formatting functions.
- **Unsupported Functions**: DB Link syntax (`@dblink`), SQL*Plus commands (`SPOOL`, `SET`, `WHENEVER`).
- **UDF Required**: No.
- **Python Required**: Yes, for extracting and loading the source tables from the source Oracle system to the BigQuery environment.
- **Direct Dependencies**: `isbert_schema.dwtk_meldungen` must have current data.
- **Assumptions**: The target table schema `sof$ta_period` is pre-created in BigQuery.
- **Warnings**: Timezone offsets must be monitored when converting Oracle `DATE` types to BigQuery `TIMESTAMP` to avoid off-by-one errors during day boundaries.

OVERALL MIGRATION STRATEGY: Python Wrapper Required

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `DEFINE v_carmen = ...` | Direct-with-rewrite | Map to replicated dataset name (e.g. `carmen_replicated_dataset.`) |
| `COLUMN s_datum...` | Direct-with-rewrite | `DECLARE v_datum STRING; SET v_datum = (...)` |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `TO_CHAR(date, 'YYYYMMDD')` | Direct-with-rewrite | `FORMAT_TIMESTAMP('%Y%m%d', date)` |
| `TO_DATE(str, 'YYYYMMDD')` | Direct-with-rewrite | `PARSE_TIMESTAMP('%Y%m%d', str)` |
| `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` | Direct-with-rewrite | Native `TRUNCATE TABLE` statement |
| `@pcrs1.de.tinternal.com` | Unsupported | None — manual intervention (Replicate tables to BigQuery beforehand) |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- ===================================================================
-- BigQuery Script: d_ausd_v_ta_period
-- Equivalent logic for Oracle d_ausd_v_ta_period.sql
-- ===================================================================

-- Step 1: Declare and initialize variables
DECLARE v_datum STRING;

-- Retrieve the cutoff date from the tracking table
-- Converted from NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101')
SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Empty target table
-- Converted from dynamic PL/SQL: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_period')
TRUNCATE TABLE `target_dataset.sof$ta_period`;

-- Step 3: Populate target table with period and time-measurement configuration
-- Database links resolved to native table paths in replicated dataset: `carmen_replicated_dataset`
INSERT INTO `target_dataset.sof$ta_period` (
  period_id,
  number_time_measurement,
  time_meas_cv,
  einheit,
  bfc_age
)
SELECT
  p.period_id,
  p.number_time_measurement,
  p.time_meas_cv,
  d.description,
  p.insert_at
FROM
  `carmen_replicated_dataset.cds$ta_period` AS p
INNER JOIN
  `carmen_replicated_dataset.cds$ta_time_meas_cv` AS tm
  ON tm.time_meas_cv = p.time_meas_cv
INNER JOIN
  `carmen_replicated_dataset.cds$ta_description` AS d
  ON tm.description_id = d.description_id
WHERE
  -- Converted from TO_DATE('&v_datum', 'YYYYMMDD')
  p.insert_at <= PARSE_TIMESTAMP('%Y%m%d', v_datum)
  AND (
    p.modified_at IS NULL
    OR p.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum)
  );
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **DB Link Migration**: The target database link `@pcrs1.de.tinternal.com` was resolved to a dummy BigQuery dataset `carmen_replicated_dataset`. These tables (`cds$ta_period`, `cds$ta_time_meas_cv`, `cds$ta_description`) must be replicated from the source Carmen DB to BigQuery using an external ETL/ELT tool before executing this script.
2. **Timezone Preservation**: In Oracle, `DATE` contains both date and time but has no timezone. When converted to BigQuery `TIMESTAMP`, values will default to UTC. Ensure that the ingestion pipeline preserves the timezone of the original source system to prevent boundary mismatch on the evaluations `p.insert_at <= PARSE_TIMESTAMP('%Y%m%d', v_datum)`.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` | `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` | Native BigQuery SQL script to truncate the target table `sof$ta_period` and repopulate it using replicated Carmen period data filtered by a dynamic date variable. |

---

### Job Dependencies
- **Upstream (must run/exist before this job)**:
  - `Shared Files — sql_bqsql_linked_job/isbert/allgemein/is/util/bin` has been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/847). On BigQuery / Cloud Composer, this shared module provides utility components and must be accessible.
  - Since this upstream dependency is already migrated, there are no blocking migration dependencies for setting up orchestration.

### Execution Order
The target orchestration (e.g., Cloud Composer DAG task ordering) must preserve the original execution sequence:
1. Orchestration metadata & configuration initialization: `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` (Handles UC4 job definition and schedule variables; out of scope for this design pass).
2. KSH wrapper phase 1: `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` (Out of scope for this design pass).
3. KSH wrapper phase 2: `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` (Out of scope for this design pass).
4. Core SQL execution: `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` (This file; must run as the final step after successful execution of step 3).

### Schedule & Variables
- **Schedule**: Inherited from the orchestration wrapper DAG (`DW.BERT_AUSD_V_TA_PERIOD`).
- **Scheduler-Set Variables**:
  - `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'`: Sourced via the target orchestration engine (e.g., Airflow DAG run configuration, `params`, or Variable) and made available to execution wrappers.

### Lineage
- **Upstream Producers (Read Tables)**:
  - `isbert_schema.dwtk_meldungen`: Queried to extract the cutoff date variable (`v_datum`).
  - `cds$ta_period` (job: Carmen external DB): Replicated period master table containing temporal data.
  - `cds$ta_time_meas_cv` (job: Carmen external DB): Replicated code-list lookup table.
  - `cds$ta_description` (job: Carmen external DB): Replicated description table.
- **Downstream Consumers (Write Tables)**:
  - `sof$ta_period`: Target table containing active period and time-measurement configuration.
- **Package Dependency**:
  - `isbert_schema.DWPA_UTIL_SKRIPT` (Package): Oracle PL/SQL package calling `runstatement` to execute truncates. This dependency is removed on the target platform by replacing it with BigQuery standard SQL DDL `TRUNCATE TABLE`.

### External System Replacements
- **Carmen DB Link Replacement**: The Oracle DB Link `@pcrs1.de.tinternal.com` is replaced by an independent ingestion pipeline (e.g., Cloud Data Fusion, Datastream, or an Airflow OracleToGCS/GCSToBigQuery workflow) that replicates the `cds$ta_period`, `cds$ta_time_meas_cv`, and `cds$ta_description` tables from the remote Carmen database into a local BigQuery dataset prior to executing this script.

### Cross-File Dependencies
- **Shared Schemas/Tables**: Target table `sof$ta_period` and control table `dwtk_meldungen` are shared across other ISBERT pipeline components.
- **Utility Script**: The legacy SQL script references `START ../trace.sql.cfg` for environment configuration. On BigQuery, standard logging and monitoring are handled natively by Cloud Logging, rendering this SQL*Plus trace config obsolete.

### Target File Plan
- **Target File Path**: `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`
  - **Language**: BigQuery SQL
  - **Source File**: `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`
  - **Conversion Approach**: Replaces Oracle SQL*Plus commands (`DEFINE`, `COLUMN`, `SPOOL`) with native BigQuery SQL scripting variables (`DECLARE`, `SET`). Replaces the PL/SQL dynamic truncate call with native BQ DDL `TRUNCATE TABLE`. Directs queries to local replicated datasets instead of Oracle database links.

### Environment-Specific Values
The environment parameters must be classified and resolved as follows:

1. **GLOBAL (Environment-wide infrastructure)**:
   - `GCP_PROJECT`: Target Google Cloud Project ID.
   - `BQ_DATASET_ISBERT`: Dataset mapping to the legacy `isbert_schema` schema (contains `dwtk_meldungen`).
   - `BQ_DATASET_SOF`: Dataset mapping to the target `sof` schema (contains target table `sof$ta_period`).
   - `BQ_DATASET_CDS`: Dataset representing the landing/replication zone for Carmen source tables (`cds$ta_period`, `cds$ta_time_meas_cv`, `cds$ta_description`), replacing the DB Link.
   - *Resolution*: SQL query parameters (e.g., `@GCP_PROJECT`, `@BQ_DATASET_ISBERT`) must be injected at runtime by the execution wrapper (Composer/Dataform), avoiding any hardcoded schemas.

2. **JOB-SPECIFIC (Workflow-level parameters)**:
   - `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'`: Captured at the DAG run-level configuration and propagated down to execution logs.
   - `v_datum`: Computed dynamically within the SQL script from `dwtk_meldungen` and stored as a script-scoped variable (`DECLARE v_datum STRING;`).

### Risks and Manual Steps
1. **Replication Sync Dependency**: The tables replicated from the Carmen Oracle DB must be kept in sync. If the replication process fails or lags, this script will run on stale period definitions. This dependency must be managed and monitored in the orchestration tool (e.g., a sensor checking the landing dataset status before triggering this task).
2. **Timezone Handling on Dates**: The Oracle date comparisons (`p.insert_at <= TO_DATE('&v_datum','YYYYMMDD')`) must be carefully evaluated during migration. In BigQuery, timezone-naive inputs might default to UTC timestamps, causing potential timezone shifts (e.g., Europe/Berlin local time boundary mismatches). The replication pipeline must preserve original timezone configurations.