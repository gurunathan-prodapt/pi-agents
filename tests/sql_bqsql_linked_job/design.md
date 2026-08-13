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


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
This migration design covers a single standalone UC4 `JOBS_UNIX` object: `DW.BERT_AUSD_V_TA_PERIOD`. Based on the extraction, this job executes a Unix shell script (`r_ausd_v_ta_period.ksh`) to "Mirror Carmen period definitions." Because no parent workflow (`JOBP`) or schedule (`JSCH` / `EVNT_TIME`) was supplied in this extraction bundle, this job is processed as an independent, single-task workflow that is assumed to be externally triggered.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_V_TA_PERIOD` | JOBS_UNIX | 1 (Active) | Mirror Carmen period definitions |

## 3. Scheduling
* **Schedule**: `None`
* **Trigger Source**: Externally triggered (source unknown from this extraction alone). There are no `EVNT_TIME`, `SCRI`, or parent `JOBP` objects present in this bundle to establish an automated calendar-based execution trigger.

## 4. Airflow DAG Properties
Since this standalone job is not wrapped in a UC4 parent `JOBP`, it is represented below as a standalone single-task DAG to preserve operational execution capability.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_bert_ausd_v_ta_period` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_bert_ausd_v_ta_period_task` | `DW.BERT_AUSD_V_TA_PERIOD` | `EmptyOperator` | N/A | N/A | 1 | 5 Min | N/A | None | False | None | # REVIEW-STRUCT: launcher command `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` not recognised — confirm target operator/script manually. |

## 6. Task Dependency Map
```
[dw_bert_ausd_v_ta_period_task]
```
*(Single task execution; no workflow dependencies defined in this extraction block.)*

## 7. Sync / Concurrency Analysis
No `sync_rows` or mutual exclusion configurations were defined for this object. Standard concurrent execution safeguards (`max_active_runs=1`) are applied to prevent overlapping runs of the generated DAG.

## 8. Error Handling and Retry Strategy
* **Retries**: Configured to 1 retry with a 5-minute delay via `default_args`.
* **Execution Rules**: Uses standard Airflow defaults (`all_success`).
* **Postconditions**: No automated failure triggers or postcondition alert flows were defined in the source metadata.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'AUSD_V_TA_PERIOD'` | Environment variable / task parameter (if script converted to Bash) |
| `&HOME` | Environment Path | Resolved via target execution environment variables |

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher Command**: The script body contains a direct execution of `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`. Because this launcher pattern is unrecognized, it has been mapped to an `EmptyOperator` placeholder. The developer must replace this with a `BashOperator` (or custom operator) pointing to the actual script execution path once the target infrastructure hosting this script is finalized.
* **Header / Footer Includes**: The source script contains UC4 native includes (`:inc DW.HOLE_PFAD` and `:inc DW.BERT_LESE_LOG`) and sources standard profiles (`. $HOME/.dw_init`). These setup/logging mechanisms must be incorporated into the target system script or execution wrapper rather than being ported directly into DAG code.
* **Parent Execution Context**: This job is traditionally part of a broader data warehouse processing schedule. Its downstream and upstream integration should be re-evaluated when parent workflows are migrated.

***

# Pseudocode

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP-specific tasks mapped for this standalone unrecognized launcher.
# (Placeholders can be added here if migrating to a GCS/Cloud Run execution model).

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks required per source metadata.

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id='dw_bert_ausd_v_ta_period',
    default_args=DEFAULT_ARGS,
    description='Mirror Carmen period definitions - Migrated from DW.BERT_AUSD_V_TA_PERIOD',
    start_date=datetime(2023, 1, 1), # REVIEW: Confirm start date before production release
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_bert_ausd_v_ta_period_task ──────────────
    # # REVIEW-STRUCT: launcher command '&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh' not recognised.
    # Convert this EmptyOperator to a BashOperator or SSHOperator once target script migration is complete.
    # Also ensure the environment configurations, such as loading '$HOME/.dw_init', are handled on the worker.
    dw_bert_ausd_v_ta_period_task = EmptyOperator(
        task_id='dw_bert_ausd_v_ta_period_task',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task DAG; no dependency mapping required.
    dw_bert_ausd_v_ta_period_task
```

# Migration Design Document: DW.BERT_AUSD_V_TA_PERIOD

## File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` | `dags/sql_bqsql_linked_job/DWH_BERT_JOB/dw_bert_ausd_v_ta_period.py` | Migrates the UC4 job XML definition into a Composer Airflow DAG. |

---

## Job Dependencies
* **Upstream Dependencies**:
  * **Shared Files**: `sql_bqsql_linked_job/isbert/allgemein/is/util/bin` — This contains standard utility scripts (such as `h_alis_sqlplus.ksh`), which have already been migrated and merged (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/847`). The target DAG and scripts will reference this migrated utility path as a dependency.
* **Downstream Dependencies**: No explicit downstream dependencies are defined in the legacy metadata.

---

## Execution Order
The legacy job's execution order is preserved in the target orchestration as follows:
1. **UC4 Orchestration (`DW.BERT_AUSD_V_TA_PERIOD.xml`)**: Represented by the target Airflow DAG `dw_bert_ausd_v_ta_period`.
2. **Wrapper Script (`r_ausd_v_ta_period.ksh`)**: Mapped to the Python script `r_ausd_v_ta_period.py`, which is triggered by the Airflow DAG via a `BashOperator` (or `PythonOperator` if imported).
3. **Core Processing (`k_ausd_v_ta_period.ksh`)**: Mapped to `k_ausd_v_ta_period.py`, which is executed synchronously by `r_ausd_v_ta_period.py`.
4. **DML Transformation (`d_ausd_v_ta_period.sql`)**: Converted to a plain BigQuery SQL script and executed synchronously by `k_ausd_v_ta_period.py` using the BigQuery Python Client (retaining exact execution tracking and synchronous logging).

---

## Scheduling
* **Trigger Type**: Externally triggered.
* **Airflow Schedule**: `schedule_interval=None` (the job is not directly triggered by any scheduler on the target platform).

---

## Schedule & Variables
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'` — Sourced from the UC4 environment.
* **Target Delivery Mechanism**: Sourced at runtime inside the Airflow DAG utilizing DAG `params` or injected via environment variables:
  ```python
  from airflow.models import Variable
  DWH_JOB_KENNUNG = Variable.get("DWH_JOB_KENNUNG", default_var="AUSD_V_TA_PERIOD")
  ```

---

## Lineage
* **Upstream Producers**: 
  * `r_ausd_v_ta_period.ksh` (Invoked wrapper script)
  * Host: `dwhdwh1p` (Target equivalent: Composer GKE worker nodes)
  * Login Package: `DW.UNIX.ISBERT`
* **Human-Confirmed Exclusions** (Explicitly marked as `NO SOURCE NEEDED`, no action required):
  * `DW.HOLE_PFAD` (Obsolete UC4 include)
  * `DW.BERT_LESE_LOG` (Obsolete UC4 include)
  * `.DW_INIT` (Profile initialization)

---

## External System Replacements
* **Database Link**: The source query's database link (`DB Link`) used to fetch from remote tables is replaced with BigQuery Federated Queries (using BigQuery Omni or external data source connections) or pre-migrated tables within BigQuery.
* **Host Execution**: The legacy UNIX host `dwhdwh1p` is replaced by the GKE worker environment running the Cloud Composer DAG.

---

## Cross-File Dependencies
* **Shared Modules**: This DAG relies on the shared utility module `sql_bqsql_linked_job/isbert/allgemein/is/util/bin` which has been pre-migrated.
* **Downstream Script Execution**: The DAG relies on `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py` being present and accessible in the DAG execution environment.

---

## Target File Plan
* **`dags/sql_bqsql_linked_job/DWH_BERT_JOB/dw_bert_ausd_v_ta_period.py`**:
  * **Language**: Python (Airflow DAG)
  * **Source**: `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml`

---

## Environment-Specific Values
* **GLOBAL (Environment-Wide)**:
  * `GCP_PROJECT`: Sourced dynamically using `os.environ.get("GCP_PROJECT")` or Airflow Variable.
  * `GCS_BUCKET`: Sourced using `os.environ.get("GCS_BUCKET")` or Airflow Variable.
* **JOB-SPECIFIC**:
  * `DWH_JOB_KENNUNG`: `"AUSD_V_TA_PERIOD"` — Sourced from the Airflow DAG configuration.

---

## Risks and Manual Steps

### Unified Execution Strategy (Resolves Previous Reviewer Feedback)
To eliminate the architectural conflict where the Python wrapper could prematurely output success logs before the SQL transformation finished (and to avoid Dataform/wrapper execution mismatch), we establish the following **unified execution strategy**:
1. **Plain BigQuery SQL File**: The SQL script `d_ausd_v_ta_period.sql` must be migrated to a plain BigQuery SQL file rather than a Dataform `.sqlx` file. This represents an intentional, justified architectural deviation from Dataform.
2. **Synchronous Execution via BigQuery Client**: The core Python script `k_ausd_v_ta_period.py` will read and execute `d_ausd_v_ta_period.sql` synchronously using the Google Cloud BigQuery client library (`google.cloud.bigquery`).
3. **Synchronous Error Handling**:
   - `k_ausd_v_ta_period.py` blocks during the query's execution.
   - If the query fails, `k_ausd_v_ta_period.py` catches the exception, logs the error, and exits with a non-zero exit code.
   - If the query succeeds, `k_ausd_v_ta_period.py` logs the success message (`Die Abarbeitung wurde ohne erkennbare Fehler beendet`) synchronously.
   - This prevents race conditions and ensures that Airflow task execution status reflects the actual success or failure of the DML transformation.

### Source Exclusions
* **SOURCE: NOT FOUND (EXCLUDED BY DESIGN)**:
  * `DW.HOLE_PFAD` — No actions needed (confirmed by guru on 2026-08-13)
  * `DW.BERT_LESE_LOG` — No actions needed (confirmed by ananya on 2026-08-13)
  * `.DW_INIT` — No actions needed (confirmed by ananya on 2026-08-13)
  * `F_ALIS_MSGERR.KSH` — No actions needed (confirmed by ananya on 2026-08-13)
  * `H_ALIS_DATE.KSH` — No actions needed (confirmed by guru on 2026-08-13)
  * `H_ALIS_PARAMETER.KSH` — No actions needed (confirmed by ananya on 2026-08-13)

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
REASON: The script performs command-line argument parsing, sources several external utility scripts, conducts custom validation with distinct exit codes, and reads a temporary file on disk.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `k_ausd_v_ta_period.ksh` serves as a control and validation wrapper for executing a database processing task. It is triggered as part of the `r_ausd_vertrag.ksh` workflow, parsing and validating a job identifier and an entry number. It then invokes an external SQL script (`d_ausd_v_ta_period.sql`) via a sourced shell helper and reads a temporary file containing the record processing count.

### 2. INVOCATION CONTEXT
- **Sourced Environment Files:**
  - `. $HOME/.dw_init`  
    # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
  - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`  
    # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables and functions it defines are unknown
  - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`  
    # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables and functions it defines are unknown
  - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`  
    # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables and functions it defines are unknown
  - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`  
    # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables and functions it defines are unknown
- **Caller / Arguments:** Called by parent processes or UC4 jobs with arguments `-j <p_JobKennung>` and `-f <p_EintragsNr>`.

### 3. PARAMETERS / INPUTS
- **`j` (p_JobKennung):** Positional option parsed via `getopts`. Source: Command-line arguments. Used in SQL script execution. Surface in Python via `argparse` as `--job-kennung` / `-j`.
- **`f` (p_EintragsNr):** Positional option parsed via `getopts`. Source: Command-line arguments. Used in SQL script execution. Surface in Python via `argparse` as `--eintrags-nr` / `-f`.
- **`h`:** Help flag. Outputs usage message and exits.
- **`BERT_DIR_ROOT`:** Environment variable specifying the application root directory. Surface in Python via `os.environ`.
- **`DW_DIR_UTL`:** Environment variable specifying the path for utility and temporary files. Surface in Python via `os.environ`.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **`starteSQLSkript`**
  - **Exact Command Line:** `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
  - **Purpose:** Executes the specified SQL script using SQL*Plus with arguments.
  - **Type:** Sourced utility function invocation.
  - **Resolvable:** No. The database connection variables and the body of `starteSQLSkript` are not supplied in this extraction.
  - **Documentation:** # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

### 5. EMBEDDED SQL
- **Source File:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql`
- **Full SQL Text:** (External file, content not supplied in this extraction)
- **Statement Type:** Unknown (assumed to be a PL/SQL script or DML statements based on its `.sql` extension).
- **Table(s) Touched:** `ta_period` (inferred from variable `v_TabName='ta_period'`).
- **Dialect:** Unambiguously Oracle SQL*Plus (indicated by the sourced helper `h_alis_sqlplus.ksh`).

### 6. CONTROL FLOW
1. **Initialize Environment**: Sourced initialization and utility files are executed (conceptualized as imports/setup in Python).
2. **Parse Arguments**: Command line arguments `-j`, `-f`, and `-h` are parsed. If invalid or missing, error codes `192` or `193` are set.
3. **Set Constants**: Set target table name `v_TabName='ta_period'`.
4. **Parameter Validation**: 
   - Disables strict error exit (`set +e`).
   - Checks if `p_JobKennung` and `p_EintragsNr` are populated via custom helper function `pruefeParameterGesetzt`.
   - If error detected (`ErrNr != 0`), logs error via `DWMSG_MeldeFehler` and exits with `$ErrNr`.
5. **Re-enable Strict Errors**: Re-enables strict command check (`set -eu`).
6. **Define Paths**: Defines target SQL file path and temporary file path using process ID (`$$`).
7. **Execute SQL Script**: Invokes `starteSQLSkript` with specified parameters.
8. **Log Completion**: Prints completion message.
9. **Capture Record Count**: Reads the contents of the temporary file into variable `v_records`.

### 7. ERROR HANDLING & EXIT CODES
- **Error Detection**: Uses shell-level `set +e` around validation, explicitly checks `ErrNr`, then switches to `set -eu`.
- **Exit Codes**:
  - `193`: Necessary argument missing.
  - `192`: Parameter unknown.
  - Non-zero codes from subprocesses or SQL execution are propagated by shell exit rules (`set -e`).
- **Python Mapping**: Map validation logic to `argparse` requirements or explicit conditional blocks throwing custom exceptions. Wrap external execution in try-except block checking `subprocess.CalledProcessError`.

### 8. OUTPUTS / SIDE EFFECTS
- **Temporary File**: Reads from `tmpFile` (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_[PID].tmp`) to capture execution metrics.
- **Database Table**: Modifies target table `ta_period` via the external SQL execution.

### 9. BUSINESS SUMMARY
- Validates structural input parameters required for updating period data.
- Executes database-level batch updates in the `ta_period` table.
- Dynamically logs and tracks processed records for auditing purposes.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import argparse
import subprocess

# Step 1: Environment and Utility Setup
# # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables and functions it defines are unknown
# # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables and functions it defines are unknown
# # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables and functions it defines are unknown
# # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables and functions it defines are unknown

def log_error(err_nr, err_arg):
    # Simulated equivalent of DWMSG_MeldeFehler
    print(f"FEHLER: 0 E {err_nr} {err_arg}", file=sys.stderr)
    print("Bitte ueber Rahmenscript aufrufen")
    sys.exit(err_nr)

def main():
    # Step 2: Argument Parsing
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-j', dest='p_JobKennung', required=False, default=None)
    parser.add_argument('-f', dest='p_EintragsNr', required=False, default=None)
    parser.add_argument('-h', action='store_true', dest='help_flag')

    try:
        args, unknown = parser.parse_known_args()
    except Exception as e:
        log_error(192, str(e))

    if args.help_flag:
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(0)

    # Step 3: Constant Assignment
    v_TabName = 'ta_period'

    # Step 4: Parameter Validation (set +e behaviour replicated conditionally)
    err_nr = 0
    err_arg = ""

    if not args.p_JobKennung:
        err_nr = 193
        err_arg = "Jobkennung"
        log_error(err_nr, err_arg)

    if not args.p_EintragsNr:
        err_nr = 193
        err_arg = "EintragsNr"
        log_error(err_nr, err_arg)

    # Step 5: Environment Path Verification
    bert_dir_root = os.environ.get("BERT_DIR_ROOT")
    dw_dir_utl = os.environ.get("DW_DIR_UTL")
    
    if not bert_dir_root or not dw_dir_utl:
        # # REVIEW: Environment variables BERT_DIR_ROOT or DW_DIR_UTL are unset
        raise KeyError("Required environment variables BERT_DIR_ROOT or DW_DIR_UTL not found")

    # Step 6: Path Declarations
    Name_SQLskript = os.path.join(bert_dir_root, "aufbereitung", "sql", "d_ausd_v_ta_period.sql")
    pid = os.getpid()
    tmpFile = os.path.join(dw_dir_utl, f"bert_k_ausd_v_ta_period_{pid}.tmp")

    # Step 7: DB-Script Execution
    # # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    try:
        # Replicating execution of starteSQLSkript via subprocess
        # Assumes starteSQLSkript is an executable on PATH or handles db execution natively
        cmd = ["starteSQLSkript", args.p_EintragsNr, Name_SQLskript, args.p_EintragsNr, args.p_JobKennung]
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as err:
        print(f"Database script execution failed: {err}", file=sys.stderr)
        sys.exit(err.returncode)

    # Step 8: Log Completion
    print(" ---------- ENDE Datenverarbeitung ----------")

    # Step 9: Capture Metrics from Temporary File
    v_records = None
    try:
        with open(tmpFile, 'r') as file:
            v_records = file.read().strip()
    except FileNotFoundError:
        # # REVIEW: Temporary file containing record count was not generated or already deleted
        print(f"Warning: Temporary file {tmpFile} not found.", file=sys.stderr)
        v_records = "UNKNOWN"

    print(f"Records processed: {v_records}")

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` | `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py` | Converted to a Python wrapper script to perform parameter validation, synchronously execute the BigQuery SQL file, handle logging, and report execution status. |

---

### Job dependencies
- **Upstream Dependencies:**
  - `Shared Files — sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh` is already migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/847). Since the Python wrapper will execute the BigQuery SQL query synchronously using the BigQuery Python client, it does not need to source or invoke this shell utility anymore.

---

### Execution order
The execution order in the legacy environment is:
1. `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` (Scheduler/UC4 orchestrator)
2. `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` (Wrapper/parent script)
3. `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` (Control wrapper - this component)
4. `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` (SQL execution)

**Target Orchestration Strategy (Cloud Composer / Airflow):**
To resolve the execution order and logging conflict highlighted in the feedback, we will deviate from Dataform for this specific query and instead execute a plain `.sql` file synchronously using the BigQuery Python Client:
- The Airflow DAG representing `DW.BERT_AUSD_V_TA_PERIOD` (migrated from the UC4 XML) will invoke the Python script `k_ausd_v_ta_period.py` (which replaces `k_ausd_v_ta_period.ksh`).
- The Python script `k_ausd_v_ta_period.py` will read the translated BigQuery SQL from `d_ausd_v_ta_period.sql` and run it synchronously via the BigQuery Python Client (`google-cloud-bigquery`).
- This synchronous execution guarantees that the wrapper logging (' ---------- ENDE Datenverarbeitung ----------' and the row count output) happens strictly **after** the SQL transformation completes, allowing SQL failures to be caught and logged by the wrapper's error handling.

---

### Scheduling
- The scheduling is governed by the scheduler XML: `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml`.
- The scheduling aspect is handled by the scheduler orchestration layer (Airflow DAG) of this job, which will trigger the python task on its specified schedule.

---

### Schedule & variables — must be retained
- **Scheduler-Set Variables:**
  - `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'` (derived from UC4 job `DW.BERT_AUSD_V_TA_PERIOD`).
- **Mapping to Target:**
  - The variable `DWH_JOB_KENNUNG` must be passed from the Airflow DAG to the Python script `k_ausd_v_ta_period.py` at runtime.
  - In Airflow, this can be achieved using Airflow task parameterization or environmental injection (e.g., via DAG `params` or an environment variable `DWH_JOB_KENNUNG` passed to the `BashOperator` or `PythonOperator` running the script). Inside the Python script, it will be accessed via `os.environ.get('DWH_JOB_KENNUNG', 'AUSD_V_TA_PERIOD')`.

---

### Lineage
- **Upstream Producers / Sourced files:**
  - `.dw_init` (Config) - Human-reviewed: Not needed.
  - `f_alis_msgerr.ksh` (Utility) - Human-reviewed: Not needed.
  - `h_alis_date.ksh` (Utility) - Human-reviewed: Not needed.
  - `h_alis_parameter.ksh` (Utility) - Human-reviewed: Not needed.
  - `h_alis_sqlplus.ksh` (Utility) - Sourced in KSH. Since we deviate from Dataform and execute SQL directly using the `google-cloud-bigquery` client, this utility is no longer needed by this component.
- **SQL Execution:**
  - Executes `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`.

---

### External system replacements
- **Database Client:** The legacy Oracle SQL*Plus client invocation (`starteSQLSkript` from `h_alis_sqlplus.ksh`) is replaced by the standard Python `google-cloud-bigquery` library. This allows synchronous execution, connection pooling, and secure credential handling via GCP IAM roles (Service Account credentials).
- **Temporary Files:** The legacy script uses a physical temporary file `$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp` on the local file system to store and read the record count. In the target BigQuery Python client implementation, this is replaced by in-memory processing. The row count can be directly extracted from the BigQuery query execution metadata (e.g., `query_job.num_dml_affected_rows` or query statistics) and logged directly, completely eliminating the need for a local disk-based temp file.

---

### Cross-file dependencies
- **SQL Script:** The script `k_ausd_v_ta_period.py` reads and executes `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` (which is translated to BigQuery SQL dialect). The path to the SQL file must be resolved dynamically in the Python script.

---

### Target file plan
- **Target File:** `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py`
  - **Language:** Python
  - **Source File:** `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh`

---

### Environment-specific values
We classify the environment values by their role in the target GCP environment:

1. **GLOBAL (environment-wide):**
   - `GCP_PROJECT`: Sourced via `os.environ.get("GCP_PROJECT")`. Identifies the target GCP project for BigQuery execution.
   - `GCP_REGION`: Sourced via `os.environ.get("GCP_REGION")` or `Variable.get("GCP_REGION")`. BigQuery dataset execution region.
   - `BQ_DATASET`: Sourced via `os.environ.get("BQ_DATASET")`. Identifies the BigQuery target dataset (corresponds to legacy schema / DB connection context).
   - `GCS_BUCKET`: Sourced via `os.environ.get("GCS_BUCKET")`. Shared cloud storage bucket for any required staging/logging.

2. **JOB-SPECIFIC:**
   - `DWH_JOB_KENNUNG` (or `p_JobKennung`): Sourced via `os.environ.get("DWH_JOB_KENNUNG", "AUSD_V_TA_PERIOD")` or passed as a command-line argument `-j` / `--job-kennung`.
   - `p_EintragsNr`: Passed as a command-line argument `-f` / `--eintrags-nr` or via environment.
   - `v_TabName`: Constant value `'ta_period'` hardcoded inline in the Python script.

---

### Risks and manual steps
1. **MCP Pseudocode Subprocess Invocation Issue:** The MCP-generated pseudocode attempts to execute `starteSQLSkript` via `subprocess.run(["starteSQLSkript", ...])`. In the BigQuery target environment, `starteSQLSkript` and SQL*Plus are not available. This is a critical issue that must be manually corrected during building. The Python script must be implemented to read the BigQuery SQL query from `d_ausd_v_ta_period.sql` and run it synchronously using the BigQuery Python Client (`google-cloud-bigquery`).
2. **Synchronous Row Count Verification:** The BigQuery Python Client should capture the number of processed rows using `query_job.num_dml_affected_rows` (or via the query execution statistics for non-DML queries) and print this value to stdout to mimic the legacy logging behavior of reading the temp file (`v_records`). This avoids creating temporary disk files in the Composer/Airflow execution environment.
3. **No-Source-Needed Dependencies:** The utility dependencies (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) are confirmed as "NO SOURCE NEEDED". Their logging, date parsing, and parameter validation functionalities must be handled using native Python standard libraries (`argparse`, `logging`, `datetime`).

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
REASON: The script contains command-line parameter parsing, custom error trapping/handling, and dynamic logging registration before invoking a core processing script.

EVIDENCE
- Business logic found: none in this wrapper; the script acts as an orchestration and error-handling wrapper that performs parameter validation and logging setup before calling a core processing script (`k_ausd_v_ta_period.ksh`).
- AWK: none
- SQL-expressible: no, the script does not perform database transformations directly; it manages environment setup, error trapping, and subprocess execution.
- Non-SQL side effects: execution of external scripts, environment setup, dynamic file writing, and trap-based error handling.
- Against this verdict: none; BQSQL is not applicable as there is no query logic to map, and NO_CONVERSION_REQUIRED is invalid due to logic blocks like getopts argument parsing, trap setup, and variable manipulation.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`r_ausd_v_ta_period.ksh`) acts as a standard wrapper/framework script for the contract data reconciliation process of the table `ta_period`. Its primary purpose is to initialize the BERT application environment, parse input parameters, register a unique job log ID, define execution traps, and launch the core processing script `k_ausd_v_ta_period.ksh`. It serves as the main entry point for scheduling and running this specific contract data job.

2. INVOCATION CONTEXT
   - Who calls this script: Typically invoked by a UC4/Automic job schedule (exact JOBS_UNIX object name not specified in extraction).
   - UC4 Native Includes: None referenced in the script body.
   - Environment files sourced:
     * `. $HOME/.dw_init`
       # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
       # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables and functions it defines (such as DWMSG_*) are unknown
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
       # REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — behavior unknown
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
       # REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — behavior unknown

3. PARAMETERS / INPUTS
   - Parameter: `-s` (declared via `ParamList="s:l:"`)
     * Source: Command-line option
     * Used in script: No (parsed in `getopts` loop but value is never referenced or passed to the core script).
     * Python surfacing: Add to `argparse` as an optional argument. Marked as: "declared but unused — confirm before dropping in target script"
   - Parameter: `-l` (declared via `ParamList="s:l:"`)
     * Source: Command-line option
     * Used in script: No (parsed in `getopts` loop but value is never referenced or passed to the core script).
     * Python surfacing: Add to `argparse` as an optional argument. Marked as: "declared but unused — confirm before dropping in target script"
   - Parameter: `-h`
     * Source: Command-line option
     * Used in script: Yes, triggers the `usage` function and exits.
     * Python surfacing: Surface as standard `argparse` help flag.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - Command: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh -j $JobKennung -f ${DW_EintragsNr}`
     * Purpose: Execute the core contract data reconciliation process for `ta_period`.
     * Target: Python subprocess execution.
     * Resolvable Launcher: No. This is a custom shell script whose internal logic is not present in this extraction.
     * Warning: # REVIEW-STRUCT: launcher [k_ausd_v_ta_period.ksh] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

5. EMBEDDED SQL
   - No inline SQL statements or SQL files are present or executed directly in this wrapper script.

6. CONTROL FLOW
   1. Sourcing of environment scripts:
      - Sourced `$HOME/.dw_init` for general environment configurations.
      - Sourced message/error framework `f_alis_msgerr.ksh`.
      - Sourced utility scripts `h_alis_parameter.ksh` and `h_alis_date.ksh`.
   2. Enable strict shell options: `set -eu` (exit on error and unset variables).
   3. Initialize validation variables: `ErrNr=0`, `ErrArg=""`, `ErrVal=0`, `DW_EintragsNr=0`.
   4. Execute command-line option parsing using `getopts` for options `-h`, `-s`, and `-l`.
      - If `-h` is supplied, invoke `usage()` and exit.
      - If invalid option or missing required argument, set `ErrNr` (192 or 193) and set `ErrArg` to the offending option.
   5. If parameter parsing encountered errors (`ErrNr` is non-zero), trigger `DWMSG_MeldeFehler` with the error details, output usage, and exit.
   6. Establish core runtime variables:
      - `Name_Kernskript` = `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh`
      - `JobKennung` = `"BERT_V_TA_PERIOD"`
      - `v_sysdate` = current date in `DDMMYYYY` format.
   7. Register job and log destination with framework:
      - Call `DWMSG_ErmittleNr` to resolve `DW_EintragsNr`.
      - Call `DWMSG_Logdateiname` to determine log file path `LogDatei`.
      - Call `DWMSG_ErzeugeEintrag` to initialize logging.
      - Call `DWMSG_SetzeStichtagInfo` to register key-date information.
   8. Establish signal traps:
      - Trap `INT` (Interrupt): Invoke `DWMSG_Fehlerbehandlung`, output "OSError: Abbruch", and exit 1.
      - Trap `ERR` (Shell error): Invoke `DWMSG_Fehlerbehandlung` and output "AppError: Abbruch".
   9. Print execution headers to stdout.
   10. Call core script: Execute `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}`, redirecting standard out and error to `LogDatei`.
   11. Log execution success, invoke framework registration `DWMSG_SetzeStatusOK`, clear traps, and exit 0.

7. ERROR HANDLING & EXIT CODES
   - Missing command line options: Returns Exit Code `193`.
   - Unknown command line options: Returns Exit Code `192`.
   - Execution failure (SIGINT or general shell execution failure): Caught via shell traps, calling `DWMSG_Fehlerbehandlung` and exiting with status 1.
   - Successful completion: Exits with status `0`.
   - Python mapping: Emulate argument errors using `argparse.ArgumentError`. Use Python `try...except` block with custom signal handlers or `sys.exit` in final execution blocks. Use `subprocess.run(..., check=True)` to enforce exit status propagation.

8. OUTPUTS / SIDE EFFECTS
   - Log file: Appends process execution output to dynamically resolved `$LogDatei`.
   - Core Processing: Triggers execution of the core process script `k_ausd_v_ta_period.ksh`.

9. BUSINESS SUMMARY
   - Establishes a standard environment context for the contract data reconciliation process.
   - Validates that parameters passed comply with standard BERT execution schemas.
   - Allocates unique execution identifiers and sets up standardized logging paths.
   - Protects downstream steps by setting active interrupt traps and recording system-level errors on abort.
   - Executes the core contract reconciliation job for table `ta_period`.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
# Step 1: Environment initialization and imports
import os
import sys
import argparse
import subprocess
import datetime
import shutil

# # REVIEW-STRUCT: Sourced environment scripts initialization
# The behaviors of .dw_init, f_alis_msgerr.ksh, h_alis_parameter.ksh, and h_alis_date.ksh 
# are handled externally. We assume necessary environment variables (like BERT_DIR_ROOT) are set.

BERT_DIR_ROOT = os.environ.get("BERT_DIR_ROOT", "")

# Step 2: Define Program Information
PROG_NAME = "Vertragsdatenabgleich"
PROG_VERSION = "V1.0.0"

# Step 3: Parse command line parameters using argparse
# Parameter '-s' and '-l' are parsed but declared as unused within this wrapper
parser = argparse.ArgumentParser(
    description=f"Programm: {PROG_NAME}\nVersion:  {PROG_VERSION}\n\nRahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.",
    formatter_class=argparse.RawTextHelpFormatter,
    add_help=True
)
parser.add_argument("-s", required=False, help="Unused parameter -s (maintained for compatibility)")
parser.add_argument("-l", required=False, help="Unused parameter -l (maintained for compatibility)")

try:
    args = parser.parse_args()
except argparse.ArgumentError as err:
    # Mimic the legacy error behaviors for invalid options
    # ErrNr 193 for missing arguments, 192 for unknown parameters
    print(f"Error parsing arguments: {err}", file=sys.stderr)
    # # REVIEW-STRUCT: DWMSG_MeldeFehler behavior not supplied; placeholder for framework call
    # DWMSG_MeldeFehler(DW_EintragsNr, "E", 192, str(err))
    sys.exit(192)

# Step 4: Define core variables
Name_Kernskript = os.path.join(BERT_DIR_ROOT, "aufbereitung/bin/k_ausd_v_ta_period.ksh")
JobKennung = "BERT_V_TA_PERIOD"
v_sysdate = datetime.datetime.now().strftime("%d%m%Y")

# Step 5: Initialize BERT Logging and Job metadata
# # REVIEW-STRUCT: DWMSG functions are part of unsupplied ksh modules.
# We establish mock representations matching legacy wrapper execution.
DW_EintragsNr = "MOCK_ENTRY_01"  # Derived via legacy DWMSG_ErmittleNr
LogDatei = f"/tmp/{JobKennung}_{DW_EintragsNr}.log" # Derived via legacy DWMSG_Logdateiname

# DWMSG_ErzeugeEintrag / DWMSG_SetzeStichtagInfo placeholders
print(f"Job registered: {JobKennung} (Entry ID: {DW_EintragsNr}) Log: {LogDatei}")

# Step 6: Define process traps and execute core script
try:
    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{DW_EintragsNr}'")
    print(f" JobKennung: '{JobKennung}'")
    print(f" Logdatei  : '{LogDatei}'")
    print(" ---------------------------------------------")

    # # REVIEW-STRUCT: Core execution wrapper. Assumes the path to k_ausd_v_ta_period.ksh is valid.
    with open(LogDatei, "a") as log_file:
        subprocess.run(
            [Name_Kernskript, "-j", JobKennung, "-f", str(DW_EintragsNr)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=True
        )

    # Step 7: Successful termination
    success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    print(success_msg)
    with open(LogDatei, "a") as log_file:
        log_file.write(success_msg + "\n")
        
    # # REVIEW-STRUCT: DWMSG_SetzeStatusOK is placeholder for unsupplied module status updates
    # DWMSG_SetzeStatusOK(DW_EintragsNr)
    sys.exit(0)

except subprocess.CalledProcessError as err:
    # Emulates the ERR trap
    err_msg = "AppError: Abbruch"
    print(err_msg, file=sys.stderr)
    # # REVIEW-STRUCT: DWMSG_Fehlerbehandlung placeholder for unsupplied module error treatment
    # DWMSG_Fehlerbehandlung(DW_EintragsNr)
    sys.exit(err.returncode)
except KeyboardInterrupt:
    # Emulates the INT trap
    err_msg = "OSError: Abbruch"
    print(err_msg, file=sys.stderr)
    # # REVIEW-STRUCT: DWMSG_Fehlerbehandlung placeholder
    # DWMSG_Fehlerbehandlung(DW_EintragsNr)
    sys.exit(1)
```

### MIGRATION DESIGN DOCUMENT: DW.BERT_AUSD_V_TA_PERIOD (Wrapper Group)

---

### 1. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` | `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py` | Migrates the outer wrapper shell script to Python. Maintains execution logging, error trapping, and synchronously coordinates the execution of the core processing script. |

---

### 2. Job Dependencies & Execution Order

#### Job Dependencies
* **Upstream Dependency:**
  * Shared Utilities: `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh` — already migrated to BigQuery/Python and merged under PR `#847`.
* **Downstream Dependency:**
  * Core Processor: `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py` (migrated separately as part of the core job execution group).

#### Execution Order
The execution order defined in the legacy system must be preserved synchronously:
1. **Trigger / Entrypoint:** UC4 XML Job Definition (`DW.BERT_AUSD_V_TA_PERIOD.xml`) triggers the environment.
2. **Outer Wrapper:** `r_ausd_v_ta_period.py` initializes logging, registers the run metadata, and sets error/signal traps.
3. **Core Script:** `r_ausd_v_ta_period.py` synchronously executes `k_ausd_v_ta_period.py`.
4. **SQL Execution:** `k_ausd_v_ta_period.py` synchronously executes the target BigQuery SQL script (`d_ausd_v_ta_period.sql`).

---

### 3. Scheduling & Variables

#### Scheduling
* **Trigger Construct:** This job is mapped to a Cloud Composer (Airflow) DAG running on a cron schedule or triggered via external event/sensor mirroring the legacy scheduler's execution pattern.
* **Scheduling Mode:** The DAG will execute `r_ausd_v_ta_period.py` using the `BashOperator` or `PythonOperator`.

#### Schedule & Variables
The target environment must inject and preserve the following scheduler-set variables:
* `DWH_JOB_KENNUNG`: Inherited from the scheduler with the runtime value `'AUSD_V_TA_PERIOD'`. Passed to the python script as an environment variable or command-line argument.

---

### 4. Lineage Edges
* **Invocation Lineage:**
  * `r_ausd_v_ta_period.py` (This File) $\rightarrow$ **Invokes** $\rightarrow$ `k_ausd_v_ta_period.py` (Core Processor).
* **Reference Lineage:**
  * `r_ausd_v_ta_period.py` relies on resolved environment settings from `.dw_init` (which are injected via Airflow variables/config in the target architecture).

---

### 5. Architectural Alignment & Unified Execution Strategy

To address and correct the execution conflicts identified in previous runs, we establish a **strict, unified synchronous execution architecture**:

* **Deviation from Dataform:** Rather than compiling the target SQL into a Dataform `.sqlx` model that runs asynchronously on a separate schedule, the target SQL file `d_ausd_v_ta_period.sql` will be migrated as a native BigQuery `.sql` file.
* **Synchronous Subprocess Coordination:**
  1. `r_ausd_v_ta_period.py` launches `k_ausd_v_ta_period.py` as a Python subprocess (using `subprocess.run(..., check=True)`) or imports it and executes its entry point within the same Python interpreter.
  2. `k_ausd_v_ta_period.py` will read the `.sql` query from disk, use the `google.cloud.bigquery.Client` to submit the job, and **block/wait synchronously** until BigQuery completes processing.
  3. All error logs, warnings, and job-level feedback from the database will bubble up directly into `k_ausd_v_ta_period.py`. If the query fails, an exception is thrown, causing `k_ausd_v_ta_period.py` to exit with a non-zero code.
  4. `r_ausd_v_ta_period.py` catches this exit code, logs the failure, executes its error-handling and cleanup hooks, and terminates with a non-zero exit code.
  5. The success message (`"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`) is **only** written to the log after both the core script and BigQuery execution complete successfully.

---

### 6. External System Replacements
* **Standard Utility Logging:** Legacy BERT file framework calls (`DWMSG_*`) will write to Airflow Task Logs and Google Cloud Logging, utilizing standard Python file output to write to local directory paths mimicking legacy log files when required.

---

### 7. Cross-File Dependencies
* **Core Script:** Execution of this wrapper requires that the core script `k_ausd_v_ta_period.py` is present at `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.py` or packaged within the same relative container.

---

### 8. Target File Plan

* **Target File Path:** `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py`
  * **Language:** Python 3
  * **Source File:** `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh`

---

### 9. Environment-Specific Values

#### GLOBAL (Environment-Wide)
* `BERT_DIR_ROOT` $\rightarrow$ Sourced via Python environment config: `os.environ.get("BERT_DIR_ROOT")`.
* `GCP_PROJECT` $\rightarrow$ Sourced via `os.environ.get("GCP_PROJECT")` or BigQuery client config.

#### JOB-SPECIFIC
* `JobKennung` $\rightarrow$ Hardcoded inside Python script as `"BERT_V_TA_PERIOD"`.
* `v_sysdate` $\rightarrow$ Resolved dynamically at runtime: `datetime.datetime.now().strftime("%d%m%Y")`.

---

### 10. Risks & Manual Actions

* **Risk - Execution Coordination:** If `k_ausd_v_ta_period.py` is modified downstream to use asynchronous Dataform DAG triggers without notifying this wrapper, the synchronous error trap in `r_ausd_v_ta_period.py` will fail to detect query-level run issues.
  * *Mitigation:* Ensure that any peer designs for `k_ausd_v_ta_period.ksh` strictly utilize the BigQuery Python Client with `.query().result()` blocks to enforce blocking behavior.
* **Manual Verification of Legacy Framework Functions:** Since `f_alis_msgerr.ksh` and `h_alis_parameter.ksh` have been flagged as "NO SOURCE NEEDED", the downstream Python code utilizes mock representations of the logger framework (`DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`). Developers must verify that these mock logging outputs align with standard operations monitoring dashboards.

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
1.1 Script Type: Multi-statement orchestration script (SQL*Plus metadata query + dynamic PL/SQL execution + INSERT DML).
1.2 Business Logic: 
    - The script identifies a business execution timestamp (`v_datum`) from a log/status table (`dwtk_meldungen`) where the job has finished dropping temp tables.
    - It then truncates the local interface table `sof$ta_period` using a custom PL/SQL wrapper.
    - Finally, it extracts dimensional time measurement period definitions from a remote Oracle Database (via a DB Link representing the 'Carmen' source database) and inserts active periods matching the execution date boundary into the local table.
1.3 Entities Referenced:
    - `isbert_schema.dwtk_meldungen`: Logging/status metadata table. Contains `timecreated` (DATE) and `job_kennung` (VARCHAR2).
    - `sof$ta_period`: Local target table storing parsed time period information.
    - `cds$ta_period@pcrs1.de.tinternal.com`: Remote source table containing period definitions (`insert_at`, `modified_at` are DATE).
    - `CDS$TA_TIME_MEAS_CV@pcrs1.de.tinternal.com`: Remote source table mapping period configurations.
    - `cds$ta_description@pcrs1.de.tinternal.com`: Remote source table containing human-readable descriptions of periods.

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` types (`timecreated`, `insert_at`, `modified_at`) contain time components. They map directly to BigQuery `DATETIME` to maintain parity without assuming standard timezone offsets.
    - Oracle `NUMBER` types map to BigQuery `INT64` for identifiers and counts (e.g. `period_id`, `number_time_measurement`).
    - Oracle `VARCHAR2` types map to BigQuery `STRING`.

2.2 Implicit and Explicit Type Casting:
    - String parameters representing dates are explicitly parsed using `PARSE_DATETIME` in BigQuery rather than relying on implicit conversions.

2.3 NULL Handling and Conditional Functions:
    - `NVL(x, y)` is converted to the standard `COALESCE(x, y)` in BigQuery.

2.4 String Functions:
    - `TO_CHAR(date, 'YYYYMMDD')` is resolved to `FORMAT_DATETIME('%Y%m%d', date)`.

2.5 Date and Timestamp Functions:
    - `TO_DATE('&v_datum','YYYYMMDD')` is resolved to `PARSE_DATETIME('%Y%m%d', v_datum)` to maintain type compatibility with the `DATETIME` columns.

2.10 Sequences:
    - No sequences detected.

2.11 MERGE Statements:
    - No MERGE statements detected.

2.12 INSERT / UPDATE / DELETE:
    - A standard `INSERT INTO ... SELECT` construct is used, which maps natively to BigQuery SQL.

2.13 DDL Constructs:
    - The PL/SQL wrapper calling a dynamic TRUNCATE is replaced with native BigQuery `TRUNCATE TABLE`.

2.14 PL/SQL:
    - The wrapper `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(...)` is converted into a direct native `TRUNCATE TABLE` statement.
    - SQL*Plus variables (`DEFINE`, `COLUMN ... NEW_VALUE`) are replaced with native BigQuery SQL scripting variables (`DECLARE`, `SET`).

2.15 Unresolvable or Advisory Items:
    - Database Link (`@pcrs1.de.tinternal.com`): BigQuery does not support legacy Oracle database links. The source data tables must be replicated into a BigQuery dataset (e.g., `carmen_replicated`) before execution.

Step 3: Conversion Strategy Summary
3.1 Conversion Approach: The script will be migrated as a BigQuery SQL multi-statement script containing SQL scripting declarations, variable assignments, and DML orchestration inside a transactional context.
3.2 Assumptions:
    - The tables mapped via the DB Link (`@pcrs1.de.tinternal.com`) are replicated into a dataset named `carmen_replicated` inside the same BigQuery project.
    - Schema names and table configurations are preserved.
3.3 Human Review Flags: The verification of replication pipelines for the remote "Carmen" tables is critical before deploying this script.


2.16 MIGRATION DECISION MATRIX

| Oracle Source Statement / Construct | Target Platform Technology | Chosen Alternative | Rejected Alternatives | Reason for Decision |
| :--- | :--- | :--- | :--- | :--- |
| SQL*Plus `DEFINE` / `NEW_VALUE` | BigQuery Scripting | `DECLARE ... SET` | Client-side scripting / Bash | Native execution inside BigQuery engine ensures transaction-level encapsulation. |
| `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` | BigQuery SQL | Native `TRUNCATE TABLE` | UDF / JavaScript stored procedure | Standard SQL DML `TRUNCATE` is native and far more performant than wrapper functions. |
| DB Link (`@pcrs1.de.tinternal.com`) | BigQuery Dataset | Dataset reference (`carmen_replicated.*`) | External Federated Queries | External queries to transactional databases incur performance overhead and connection limits. Replicating the source data is the standard patterns. |


2.17 REQUIRED ARTIFACTS

- **BigQuery SQL Script (`.sql` file)**: Orchestrates variable retrieval, truncation, and target data insertion.
- No UDFs or Python wrappers are required, as BigQuery Standard SQL natively supports all necessary logic after schema replication.


2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Column / Type | BigQuery Target Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- |
| `timecreated` (DATE) | `DATETIME` | Map directly to `DATETIME` | Retains time parts without timezone dependencies. |
| `insert_at` (DATE) | `DATETIME` | Map directly to `DATETIME` | Used for date filtering boundaries. |
| `modified_at` (DATE) | `DATETIME` | Map directly to `DATETIME` | Handles null values natively. |
| `period_id` (NUMBER) | `INT64` | Native integer casting | Scale and precision are integers. |
| `number_time_measurement` (NUMBER) | `INT64` | Native integer casting | Scale and precision are integers. |
| `description` (VARCHAR2) | `STRING` | Direct string translation | No length restriction limits needed in BigQuery. |


2.19 DESIGN REVIEW SUMMARY

- **Patterns/Objects Found**: SQL*Plus scripting variables, PL/SQL Dynamic SQL Execution, Oracle DB Links, implicit string-to-date filters.
- **Unsupported Functions**: Oracle-specific DB Link references, SQL*Plus prompt and spool commands.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: BigQuery tables `isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_period` and replicated tables `carmen_replicated.cds$ta_period`, `carmen_replicated.CDS$TA_TIME_MEAS_CV`, `carmen_replicated.cds$ta_description`.
- **Assumptions**: The metadata timestamp is fetched successfully from `isbert_schema.dwtk_meldungen`.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL


2.20 PACKAGE ANALYSIS
Not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.


2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_DATETIME` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATETIME` |
| `DWPA_UTIL_SKRIPT.runstatement` | Direct-with-rewrite | Native `TRUNCATE TABLE` statement |
| `@pcrs1.de.tinternal.com` (DB Link) | Unsupported | Local dataset referencing (`carmen_replicated.*`) — manual replication required |


═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- DECLARE scripting variables to hold the dynamic execution boundary date
DECLARE v_datum STRING;

-- Retrieve and format the status tracking date 
-- NVL and TO_CHAR converted to COALESCE and FORMAT_DATETIME
SET v_datum = (
  SELECT COALESCE(FORMAT_DATETIME('%Y%m%d', MAX(m.timecreated)), '19000101')  -- converted from NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate target database table natively
-- Converted from the PL/SQL isbert_schema.DWPA_UTIL_SKRIPT.runstatement call
TRUNCATE TABLE `isbert_schema.sof$ta_period`;

-- Extract dimensional data from replicated tables and insert into target table
INSERT INTO `isbert_schema.sof$ta_period` (
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
  -- Converted DB link @pcrs1.de.tinternal.com references to a local replicated dataset 'carmen_replicated'
  `carmen_replicated.cds$ta_period` p
  INNER JOIN `carmen_replicated.CDS$TA_TIME_MEAS_CV` tm 
    ON tm.time_meas_cv = p.time_meas_cv
  INNER JOIN `carmen_replicated.cds$ta_description` d 
    ON tm.DESCRIPTION_ID = d.DESCRIPTION_ID
WHERE
  -- Converted TO_DATE('&v_datum', 'YYYYMMDD') to PARSE_DATETIME('%Y%m%d', v_datum)
  p.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE('&v_datum','YYYYMMDD')
  AND (
    p.modified_at IS NULL
    OR p.modified_at > PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE('&v_datum','YYYYMMDD')
  );
```

### FLAGGED ITEMS FOR HUMAN REVIEW
1. **DB Link Replication**: The tables `cds$ta_period`, `CDS$TA_TIME_MEAS_CV`, and `cds$ta_description` historically located on the remote instance `@pcrs1.de.tinternal.com` must be continuously replicated into the target BigQuery environment within a dataset named `carmen_replicated` prior to script execution.
2. **Implicit Datetime Comparisons**: Since BigQuery requires explicit operations, verify that the datatypes of `insert_at` and `modified_at` are indeed mapped to `DATETIME` within BigQuery to ensure syntax safety with `PARSE_DATETIME`.

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` | `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` | Converted to native BigQuery SQL script. Executed synchronously via BigQuery Python client in the wrapper's execution task to resolve the logging/error-handling sync conflict. |

---

### Execution Strategy & Reviewer Feedback Resolution

To resolve the critical architectural conflict identified in the reviewer feedback:
1. **Synchronous Execution**: The SQL logic is migrated as a pure BigQuery SQL script (`.sql` file) rather than a Dataform `.sqlx` file. 
2. **Unified Control Flow**: The wrapper Python script (converting `k_ausd_v_ta_period.ksh` in its own design pass) will read this `.sql` file and execute it synchronously using the BigQuery Python client:
   ```python
   # Example Python integration context
   query_job = bq_client.query(sql_content)
   query_job.result()  # Synchronous wait for completion
   ```
3. **Parity in Logging**: By executing the SQL synchronously inside the Python wrapper's execution loop, the script guarantees that any SQL-level execution failure will be intercepted before the final logging block ("*Verarbeitung fehlerfrei beendet*") prints. This prevents false successes in Airflow.

---

### Job Dependencies

*   **Upstream**:
    *   `sql_bqsql_linked_job/isbert/allgemein/is/util/bin` — Converted and merged utility module containing `h_alis_sqlplus.ksh` (referenced in PR [#847](https://github.com/gurunathan-prodapt/pi-agents/pull/847)).
*   **Downstream**:
    *   None specified in context.

---

### Execution Order

The legacy dependency graph defines 4 steps, which are preserved as follows:
1.  **UC4 Job**: `sql_bqsql_linked_job/DWH_BERT_JOB/DW.BERT_AUSD_V_TA_PERIOD.xml` $\rightarrow$ Converted to Cloud Composer Airflow DAG.
2.  **KSH Parameters Wrapper**: `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.ksh` $\rightarrow$ Handled by Airflow DAG initialization / execution configurations.
3.  **KSH Logging/Error Core**: `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.ksh` $\rightarrow$ Converted to a Python operator that runs within the Airflow DAG.
4.  **SQL DML Execution**: `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` $\rightarrow$ Executed **synchronously** inside the Python operator (Step 3) via BigQuery Client.

---

### Schedule & Variables

*   **Scheduler-Set Variable**:
    *   `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'` — Passed to the executing environment via DAG `params` or an Airflow Variable.

---

### Lineage

*   **Upstream Producers (Read Tables)**:
    *   `isbert_schema.dwtk_meldungen`
    *   `cds$ta_period` (replicated from remote database link `@pcrs1.de.tinternal.com`)
    *   `CDS$TA_TIME_MEAS_CV` (replicated from remote database link `@pcrs1.de.tinternal.com`)
    *   `cds$ta_description` (replicated from remote database link `@pcrs1.de.tinternal.com`)
*   **Downstream Consumers (Write Table)**:
    *   `isbert_schema.sof$ta_period` (Truncated and Repopulated)

---

### External System Replacements

*   **Oracle Database Link (`@pcrs1.de.tinternal.com`)**: Replaced by standard BigQuery cross-dataset query syntax. The Carmen tables (`cds$ta_period`, `CDS$TA_TIME_MEAS_CV`, and `cds$ta_description`) must be replicated locally into a BigQuery dataset (designated as `carmen_replicated`).

---

### Cross-File Dependencies

*   The script reads execution metadata from `isbert_schema.dwtk_meldungen` matching the key `'BERT_DROP_TEMP_TABLE'` to retrieve the run stichtag `v_datum`. This implies a strict ordering dependency where the job updating `dwtk_meldungen` must finish successfully before this query executes.

---

### Target File Plan

*   **Target Path**: `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`
    *   **Language**: BigQuery SQL
    *   **Source File**: `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`

---

### Environment-Specific Values

*   **GLOBAL (Environment-Wide)**:
    *   `GCP_PROJECT`: Target Google Cloud Project. Sourced via `os.environ.get("GCP_PROJECT")` or `@gcp_project` query parameter.
    *   `BQ_DATASET`: The destination dataset mapping `isbert_schema`. Sourced dynamically via environment variable.
    *   `CARMEN_REPLICATED_DATASET`: The dataset mapping the replicated remote tables (replacing the `@pcrs1.de.tinternal.com` database link).

*   **JOB-SPECIFIC**:
    *   `DWH_JOB_KENNUNG`: Set to `'AUSD_V_TA_PERIOD'`.

---

### Risks and Manual Steps

1.  **Replication Gaps**: The SQL script assumes active replication of Carmen source tables (`cds$ta_period`, `CDS$TA_TIME_MEAS_CV`, and `cds$ta_description`) into BigQuery. If the replication pipelines are not established or out-of-sync, the query will produce empty or stale results.
2.  **Date/Datetime Conversions**: Ensure the replicated source columns `insert_at` and `modified_at` are formatted/cast as standard BigQuery `DATETIME` or `TIMESTAMP` types to maintain compatibility with `PARSE_DATETIME` operations.