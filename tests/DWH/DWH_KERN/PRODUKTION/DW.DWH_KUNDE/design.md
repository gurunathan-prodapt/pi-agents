=== OBJECT: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS (JOBS_UNIX) ===
active=1
title=Job startet Shellscript r_abgl_kunde_woech.ksh zum Adressabgleich der Kundenstammdaten
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=None
launcher_type=unrecognized
launcher_details={'raw_command': ":SET &DWH_JOB_KENNUNG = 'KUNDE_ABGL_WOECHENTLICH'"}
script_body:
:SET &DWH_JOB_KENNUNG = 'KUNDE_ABGL_WOECHENTLICH'
:SET &LAUF_WOCHE      = SYS_DATE("YYYYMMDD")

$HOME/aktuell/dw_source/isdwh/exporter/kunde/bin/r_abgl_kunde_woech.ksh -s &LAUF_WOCHE

:PRINT "Kundenadressabgleich fuer Lauf &LAUF_WOCHE angestossen"
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


## 1. Overview

This UC4 extraction contains a single Unix job that launches a shell script for weekly customer master-data address reconciliation. The job sets a job identifier variable, derives the current run date in `YYYYMMDD` format, and then invokes `r_abgl_kunde_woech.ksh` with that date as a parameter. No workflow/jobplan, schedule object, or trigger script is present in the bundle, so this object appears to be a standalone job rather than a multi-step UC4 workflow. Based on the extraction alone, it is externally triggered or started by some source not included here.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---:|---|
| DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS | JOBS_UNIX | 1 | Job startet Shellscript r_abgl_kunde_woech.ksh zum Adressabgleich der Kundenstammdaten |

---

## 3. Scheduling

No EVNT_TIME object is present in this extraction, and no SCRI trigger object or JOBP reference is supplied that would indicate an internal trigger source. Therefore, this job has no calendar-based schedule of its own in the bundle and should be treated as externally triggered, with the source unknown from this extraction alone.

---

## 4. Airflow DAG Properties

No JOBP objects are present in the bundle, so there is no Airflow DAG to define from a workflow/jobplan. If this JOBS_UNIX object is later wrapped into a DAG, the following placeholder properties would apply to that future DAG only.

| Property | Value |
|---|---|
| dag_id | dw_dwh_kunde_abgl_woechentlich_js |
| schedule | None |
| start_date | 2024-01-01 |
| catchup | False |
| max_active_runs | 1 |
| is_paused_upon_creation | False |
| default_args | retries=0, retry_delay=5 minutes, owner=DW |

---

## 5. Task Inventory

There are no JOBP tasks in this extraction. For the supplied JOBS_UNIX object, the corresponding Airflow task would be a single stubbed task representing the launcher command, since the launcher type is unrecognized.

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---:|---|---|---|---|---|---|
| dwh_kunde_abgl_woechentlich_js | DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS | EmptyOperator | N/A | N/A | 0 | 5 minutes | None | None | N/A | None | # REVIEW-STRUCT: launcher command `:SET &DWH_JOB_KENNUNG = 'KUNDE_ABGL_WOECHENTLICH'` not recognised — confirm target operator/script manually |

---

## 6. Task Dependency Map

No JOBP workflow tasks are present, so there is no task chain to derive.

---

## 7. Sync / Concurrency Analysis

No JOBP sync rows are present in this extraction. No self-lock or cross-lock concurrency constraints can be inferred.

---

## 8. Error Handling and Retry Strategy

The supplied JOBS_UNIX object does not expose UC4 postcondition actions in a JOBP task context, so there is no workflow-level retry/error mapping to apply.

For the unrecognized launcher:
- No automatic retry semantics are inferred.
- No `on_failure_callback` is defined from the extraction.
- The launcher command is not recognized as a supported script-launch pattern, so manual implementation is required.

---

## 9. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | Set to `'KUNDE_ABGL_WOECHENTLICH'` in script body | Python variable / task-local constant |
| `&LAUF_WOCHE` | `SYS_DATE("YYYYMMDD")` | `datetime.now().strftime("%Y%m%d")` or equivalent runtime date formatting |
| `-s &LAUF_WOCHE` | Passed to shell script `r_abgl_kunde_woech.ksh` | BashOperator argument or Python subprocess parameter |
| `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` | UC4 object name | `dw_dwh_kunde_abgl_woechentlich_js` DAG/task identifier if wrapped into Airflow |

---

## 10. Developer Notes

- # REVIEW-STRUCT: launcher command `:SET &DWH_JOB_KENNUNG = 'KUNDE_ABGL_WOECHENTLICH'` not recognised as a supported launcher pattern; manual confirmation is required for the correct Airflow operator and target script.
- No unresolved references were present in the extraction.
- No schedule object was supplied; the job appears externally triggered or invoked from outside this bundle.
- The script path `$HOME/aktuell/dw_source/isdwh/exporter/kunde/bin/r_abgl_kunde_woech.ksh` is environment-specific and should be parameterized in the Airflow implementation.
- The job uses UC4 runtime date substitution (`SYS_DATE("YYYYMMDD")`), which must be replicated in Airflow at execution time rather than hardcoded.

---

# Pseudocode Outline

1. **Imports**
   - Import `DAG` from `airflow`.
   - Import `EmptyOperator` from `airflow.operators.empty`.
   - Import standard Airflow utilities needed for future expansion:
     - `datetime` from `datetime`
     - `timedelta` from `datetime`
   - No trigger, sensor, or Dataproc operators are required from the extraction, because the launcher is unrecognized.

2. **GCP Configuration**
   - Define placeholder constants only if future conversion is needed:
     - `GCP_PROJECT_ID = "YOUR_PROJECT_ID"`
     - `GCP_REGION = "YOUR_REGION"`
     - `GCP_CLUSTER_NAME = "YOUR_CLUSTER_NAME"`
   - These are not used by the current extraction but may be retained for consistency in the build framework.

3. **Default Args**
   - Set:
     - `owner = "DW"`
     - `retries = 0`
     - `retry_delay = timedelta(minutes=5)`
   - Use `start_date = datetime(2024, 1, 1)` as a placeholder.
   - Set `depends_on_past = False`.

4. **on_failure_callback stubs**
   - No workflow-specific callback is derivable from the extraction.
   - Define a generic stub only if the build framework requires one:
     - `def on_failure_stub(context):`
       - log that the launcher command is unrecognized
       - include the UC4 object name `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`
   - Do not attach a retry-based callback unless manually confirmed.

5. **DAG Definition (one per JOBP in the bundle)**
   - No JOBP exists in this bundle.
   - If the build framework requires a DAG wrapper for standalone jobs, define:
     - `dag_id="dw_dwh_kunde_abgl_woechentlich_js"`
     - `schedule=None`
     - `catchup=False`
     - `max_active_runs=1`
     - `is_paused_upon_creation=False`
   - Otherwise, treat this as a task-only extraction awaiting a parent DAG.

6. **Guard Task (if any self-lock Else=Skip sync detected)**
   - None present.

7. **Sensor Task (if any earliest_start_time constraint)**
   - None present.

8. **Calendar Check Task (if any CaleOn=1 detected)**
   - None present.

9. **Task: dwh_kunde_abgl_woechentlich_js**
   - Create an `EmptyOperator` named `dwh_kunde_abgl_woechentlich_js`.
   - Add a code comment:
     - `# REVIEW-STRUCT: launcher command ":SET &DWH_JOB_KENNUNG = 'KUNDE_ABGL_WOECHENTLICH'" not recognised — confirm target operator/script manually`
   - If later clarified, replace this stub with the correct operator and script invocation.

10. **Dependencies**
   - No upstream/downstream dependencies exist in the extraction.
   - If wrapped in a DAG, the single task stands alone with no internal chaining.

### Execution order
The target orchestration (Airflow DAG) must preserve the execution order from the legacy dependency graph:
1. **Triggering/Initialization**: The execution of the UC4 job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` (which is mapped to the Airflow DAG itself).
2. **KornShell Script Execution**: Execution of `r_abgl_kunde_woech.ksh` (converted to a Python/Bash task in a separate migration pass).
3. **Database Processing**: The executing script runs the Oracle SQL script `d_abgl_kunde_woech.sql` (to be converted to BigQuery SQL/Dataform).

In the Airflow DAG, this sequence will be preserved by executing a downstream task that invokes the converted Python/Bash module representing the script.

### Lineage
The legacy lineage edges define the following upstream and downstream connections:
- **Upstream Workflow**: This job is triggered by the parent JobPlan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml` (noted in lineage as a cross-job hand-off/parent invocation). In the target environment, this DAG will either be triggered via a `TriggerDagRunOperator` from the master parent DAG or integrated directly as a task group within the parent DAG.
- **Downstream Script**: This job directly invokes `FILE:DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.ksh`.
- **Target Host/Credentials**: This job connects and runs on the target host `EXT:DWHDWH1P` using credentials from `PACKAGE:DW.UNIX.ISTNS`.

### External system replacements
- **Legacy UNIX Host Execution**: The job was executed on host `DWHDWH1P` under the login `DW.UNIX.ISTNS`. In Google Cloud, this environment is replaced by the Cloud Composer (Airflow) worker environment and GKE/Dataproc.
- **Connection Configuration**: Credentials and host details are managed using native Airflow Connections rather than hardcoded configuration files.

### Cross-file dependencies
- **KornShell Script Call**: The job references and invokes `$HOME/aktuell/dw_source/isdwh/exporter/kunde/bin/r_abgl_kunde_woech.ksh` with the parameter `-s &LAUF_WOCHE`. The downstream Python script resulting from the migration of `r_abgl_kunde_woech.ksh` must expose an execution parameter to receive this date variable.

### Target file plan
- **Target File Path**: `dags/dw_dwh_kunde_abgl_woechentlich_js.py`
  - **Language**: Python (Airflow DAG)
  - **Source File**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml`
  - **Purpose**: Defines the Airflow DAG that replaces the UC4 job structure, computes the logical run date parameter, and triggers the downstream task. 
  - **Text Output Preserving Rule**: The print statement from the source must be printed verbatim in the task logs using the exact original text: `"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen"`.

### Environment-specific values

1. **GLOBAL (Environment-wide)**
   - **GCP_PROJECT**: The target Google Cloud Project ID. Sourced at runtime via `Variable.get("GCP_PROJECT")`.
   - **GCS_BUCKET**: The GCS bucket containing the migrated scripts and DAG assets. Sourced at runtime via `Variable.get("GCS_BUCKET")`.

2. **JOB-SPECIFIC**
   - **DWH_JOB_KENNUNG**: Defined as a local constant in the DAG configuration or params: `JOB_CONFIG = {"DWH_JOB_KENNUNG": "KUNDE_ABGL_WOECHENTLICH"}`.
   - **LAUF_WOCHE**: Represented by the UC4 dynamic system date `SYS_DATE("YYYYMMDD")`. In Airflow, this must map to the DAG's logical execution date parameter `{{ ds_nodash }}` rather than system clock time to maintain idempotency.
   - **SCRIPT_PATH**: Mapped to the location of the migrated Python/Bash script in the target GCS bucket or local DAG folder, resolved using `Variable.get("dw_dwh_kunde_abgl_woechentlich_js_script_path")`.

### Risks and manual steps
- **Idempotency and Logical Date Alignment**: The legacy job uses the current execution date (`SYS_DATE("YYYYMMDD")`). Migrating this to Airflow's logical execution date (`{{ ds_nodash }}`) is highly recommended for historical execution consistency and reruns. A manual review is required to confirm that the downstream reconciliation logic behaves correctly under backfills.
- **Cross-Pass Dependency**: The DAG directly schedules the execution of the converted `r_abgl_kunde_woech.ksh` script. The path and invocation wrapper for that script (migrated under a separate pass) must be manually verified and linked.
- **Verbatim Text Retention**: Downstream logging and messaging components must preserve the original German log output: `"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen"`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml` | `dags/dw_dwh_kunde_abgl_woechentlich_js.py` | Replaces the UC4 Unix job with an Airflow DAG that orchestrates the execution date logic and schedules the downstream customer address reconciliation task. |

---

=== FILE: DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.ksh ===
#!/bin/ksh_dwh
#-----------------------------------------------------------------------------
#- Ersterstellung am:                           2020-03-09T09:00:00+02:00
#- Ersterstellung durch:                        mschaefer
#- Aenderung:                                   2022-11-20  khoffmann - Zusaetzliches Logging
#-----------------------------------------------------------------------------
ProgName="Ausfuehrung Script $0"
ProgVersion="1.1.0"

usage(){
cat <<EOF
   Programm: $ProgName
   Zweck: Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE)
          gegen das Referenzsystem STAMMDATEN
   Parameter:
       -s     Stichtag (Format: 'YYYYMMDD')
EOF
}

DW_DIR_ROOT="$HOME/aktuell/dw_source/isdwh"
DW_DIR_LOG="$HOME/aktuell/log"
DW_ORAUSER="dwh_kern/${DWH_ORA_PWD:-changeit}@DWHP1"

f_alis_msgerr() {
    typeset l_Level="$1"
    typeset l_Text="$2"
    echo "[$l_Level] $(date '+%Y-%m-%d %H:%M:%S') $l_Text" >&2
}

set -e

typeset l_Stichtag
Protokoll_Datei="${DW_DIR_LOG}/kunde/abgl_kunde_woech_$$.log"

cd ${DW_DIR_ROOT}/exporter/kunde/sql

while getopts ":s:h" param
do
    case $param in
        s) l_Stichtag="$OPTARG";;
        h) usage; exit;;
    esac
done

if [ -z "$l_Stichtag" ]; then
    l_Stichtag=$(date -d '7 days ago' '+%Y%m%d' 2>/dev/null || date '+%Y%m%d')
fi

echo "Starte Adressabgleich Kundenstammdaten fuer Stichtag $l_Stichtag" | tee "$Protokoll_Datei"

sqlplus -s ${DW_ORAUSER} @d_abgl_kunde_woech.sql $l_Stichtag >> "$Protokoll_Datei"

l_Abweichungen=$(grep -c "^ABWEICHUNG" "$Protokoll_Datei" || true)
echo "Anzahl gefundener Abweichungen: $l_Abweichungen" >> "$Protokoll_Datei"

if [ "$l_Abweichungen" -gt 0 ]; then
    f_alis_msgerr "W" "$l_Abweichungen Abweichungen im Kundenadressabgleich gefunden, siehe $Protokoll_Datei"
fi

echo "Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet"


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains real business logic beyond wrapper orchestration, including argument parsing, default-date computation, SQL execution, log parsing, and conditional warning output.

EVIDENCE
- Business logic found: KSH custom logic only; it parses `-s/-h`, computes a default Stichtag from the current date if omitted, runs an Oracle SQL*Plus script, counts `ABWEICHUNG` lines in the log, and emits a warning when discrepancies are found.
- AWK: none
- SQL-expressible: partly; the date defaulting and log-line counting are not SQL work, and the SQL itself is invoked from an external `.sql` file rather than embedded here.
- Non-SQL side effects: writes a timestamped log file, invokes `sqlplus`, reads the generated log file, and prints a warning to stderr when discrepancies exist.
- Against this verdict: the core data comparison is delegated to `d_abgl_kunde_woech.sql`, which might itself be SQL-only, but the shell wrapper still contains nontrivial control flow and file/log handling that must be converted.

1. SCRIPT OVERVIEW
This script runs a weekly customer master-data address reconciliation against the reference system STAMMDATEN. It accepts an optional cutoff date, defaults it to seven days ago when omitted, launches an Oracle SQL*Plus script, and then inspects the generated log for discrepancy markers. If discrepancies are found, it writes a warning to stderr and records the count in the log.

2. INVOCATION CONTEXT
- Who calls this script: unknown from the extraction; no UC4 JOBS_UNIX object or job name was supplied.
- Command line / arguments: `-s <Stichtag>` is supported; `-h` prints usage and exits.
- UC4 native includes: none observed.
- Environment files sourced: none observed.
- The script is a standalone `.ksh` wrapper with no supplied UC4 invocation body.

3. PARAMETERS / INPUTS
- `-s` / `l_Stichtag`
  - Source: command-line option via `getopts`
  - Used: yes
  - Python surface: `argparse.ArgumentParser()` with an optional `--stichtag`/`-s` argument
- `-h`
  - Source: command-line option via `getopts`
  - Used: yes, for usage output and immediate exit
  - Python surface: `argparse` help handling or explicit `-h` flag
- `DWH_ORA_PWD`
  - Source: environment variable
  - Used: yes, indirectly in `DW_ORAUSER="dwh_kern/${DWH_ORA_PWD:-changeit}@DWHP1"`
  - Python surface: `os.environ.get("DWH_ORA_PWD", "changeit")`
- `DW_DIR_ROOT`
  - Source: hardcoded assignment in script
  - Used: yes
  - Python surface: `os.environ.get(...)` is not needed; keep as a constant unless externalized
- `DW_DIR_LOG`
  - Source: hardcoded assignment in script
  - Used: yes
  - Python surface: keep as a constant unless externalized
- `DW_ORAUSER`
  - Source: derived from `DWH_ORA_PWD`
  - Used: yes, passed to `sqlplus`
  - Python surface: build connection string or, preferably, use a DB driver with separate credentials
- `l_Stichtag`
  - Source: derived from `-s` or default date computation
  - Used: yes
  - Python surface: local variable from parsed args / computed default

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- `date -d '7 days ago' '+%Y%m%d' 2>/dev/null || date '+%Y%m%d'`
  - Purpose: compute default Stichtag as seven days ago, with fallback to current date if GNU `date -d` is unavailable
  - Should become native Python logic, not subprocess
  - RESOLVABLE LAUNCHER: no; this is shell date computation, not a launcher
- `tee "$Protokoll_Datei"`
  - Exact command line context: `echo "Starte Adressabgleich Kundenstammdaten fuer Stichtag $l_Stichtag" | tee "$Protokoll_Datei"`
  - Purpose: write the start message both to stdout and to the log file
  - Should become native Python file I/O plus `print`
  - RESOLVABLE LAUNCHER: no
- `sqlplus -s ${DW_ORAUSER} @d_abgl_kunde_woech.sql $l_Stichtag >> "$Protokoll_Datei"`
  - Purpose: run the Oracle SQL*Plus script with the cutoff date and append output to the log
  - Should become a native Python Oracle DB-client call if the SQL file is converted, otherwise remain an external subprocess call
  - RESOLVABLE LAUNCHER: not as a generic launcher; the wrapped `.sql` file is supplied only by name, but its contents are not extracted here, so the launcher cannot be resolved from this extraction alone
- `grep -c "^ABWEICHUNG" "$Protokoll_Datei" || true`
  - Purpose: count discrepancy markers in the log while suppressing grep’s nonzero exit when no matches are found
  - Should become native Python file reading and counting
  - RESOLVABLE LAUNCHER: no
- `echo "..." >&2`
  - Purpose: emit warning message to stderr
  - Should become native Python `print(..., file=sys.stderr)`
  - RESOLVABLE LAUNCHER: no

5. EMBEDDED SQL
- None in this script body.
- The script invokes `@d_abgl_kunde_woech.sql`, but the SQL file contents were not supplied, so no embedded SQL can be documented here.

6. CONTROL FLOW
1. Set script metadata variables `ProgName` and `ProgVersion`.  
   - Python: plain assignments.
2. Define `usage()` function that prints help text.  
   - Python: `def usage(): ...`
3. Set directory and Oracle credential variables:
   - `DW_DIR_ROOT="$HOME/aktuell/dw_source/isdwh"`
   - `DW_DIR_LOG="$HOME/aktuell/log"`
   - `DW_ORAUSER="dwh_kern/${DWH_ORA_PWD:-changeit}@DWHP1"`
   - Python: constants / environment lookup.
4. Define `f_alis_msgerr()` helper that prints a timestamped message to stderr.  
   - Python: `def f_alis_msgerr(level, text): ...`
5. Enable `set -e` so failures abort the script.  
   - Python: use `check=True` for subprocesses or explicit exception handling.
6. Declare `l_Stichtag` and set `Protokoll_Datei` to a PID-specific log path.  
   - Python: local variables.
7. Change directory to `${DW_DIR_ROOT}/exporter/kunde/sql`.  
   - Python: `os.chdir(...)`
8. Parse command-line options with `getopts ":s:h"`.  
   - Python: `argparse`.
9. If `-s` is supplied, assign `l_Stichtag="$OPTARG"`.  
   - Python: parsed argument assignment.
10. If `-h` is supplied, call `usage` and exit.  
    - Python: help handling / `sys.exit(0)`.
11. If `l_Stichtag` is empty, compute a default:
    - try `date -d '7 days ago' '+%Y%m%d'`
    - if that fails, use `date '+%Y%m%d'`
    - Python: `datetime.date.today() - timedelta(days=7)` fallback logic.
12. Print the start message and tee it to the log file.  
    - Python: write to log file and stdout.
13. Run `sqlplus -s ${DW_ORAUSER} @d_abgl_kunde_woech.sql $l_Stichtag` and append output to the log.  
    - Python: DB-client call or subprocess, depending on how the SQL file is handled in the next stage.
14. Count lines in the log beginning with `ABWEICHUNG`.  
    - Python: read file and count matching lines.
15. Append the count to the log.  
    - Python: file write.
16. If the count is greater than zero, call `f_alis_msgerr "W" ...`.  
    - Python: conditional stderr print.
17. Print the final success message.  
    - Python: stdout print.
18. Exit with the status of the last command implicitly.  
    - Python: `sys.exit(0)` on success, exceptions on failure.

7. ERROR HANDLING & EXIT CODES
- Failure detection:
  - `set -e` causes the script to abort on any failing command except where explicitly masked.
  - `grep -c ... || true` suppresses failure when no `ABWEICHUNG` lines are found.
- Failure reaction:
  - Immediate termination on command failure due to `set -e`.
  - Warning-only behavior when discrepancies are found; this does not fail the script.
- Success exit code convention:
  - Implicit zero exit on normal completion.
- Python mapping:
  - Use `subprocess.run(..., check=True)` for external commands.
  - Use explicit file handling and `sys.exit(0)` for normal completion.
  - Use exceptions for fatal errors; do not treat discrepancy count as an exception.

8. OUTPUTS / SIDE EFFECTS
- Log file: `${DW_DIR_LOG}/kunde/abgl_kunde_woech_$$.log`
- Standard output:
  - start message
  - final success message
- Standard error:
  - warning message if discrepancies are found
- External Oracle session output appended to the log file via `sqlplus`

9. BUSINESS SUMMARY
- Determines the effective weekly reconciliation date, defaulting to seven days ago when not supplied.
- Executes the customer address comparison SQL process against Oracle.
- Captures all SQL*Plus output in a run-specific log file.
- Counts discrepancy markers in the log after the SQL run completes.
- Emits a warning when discrepancies are detected, while still completing successfully.

# Step 1: define metadata and helper functions
ProgName = f"Ausfuehrung Script {sys.argv[0]}"
ProgVersion = "1.1.0"

def usage():
    print(f"""   Programm: {ProgName}
   Zweck: Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE)
          gegen das Referenzsystem STAMMDATEN
   Parameter:
       -s     Stichtag (Format: 'YYYYMMDD')""")

def f_alis_msgerr(level, text):
    print(f"[{level}] {datetime.now():%Y-%m-%d %H:%M:%S} {text}", file=sys.stderr)

# Step 2: initialize paths and credentials
DW_DIR_ROOT = os.path.join(os.environ["HOME"], "aktuell", "dw_source", "isdwh")
DW_DIR_LOG = os.path.join(os.environ["HOME"], "aktuell", "log")
DW_ORAUSER = f"dwh_kern/{os.environ.get('DWH_ORA_PWD', 'changeit')}@DWHP1"

# Step 3: enable fail-fast behavior
# Python equivalent is to let exceptions propagate from subprocess/file operations

# Step 4: declare runtime variables and log path
l_Stichtag = None
Protokoll_Datei = os.path.join(DW_DIR_LOG, "kunde", f"abgl_kunde_woech_{os.getpid()}.log")

# Step 5: change working directory
os.chdir(os.path.join(DW_DIR_ROOT, "exporter", "kunde", "sql"))

# Step 6: parse command-line options
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("-s", dest="stichtag")
parser.add_argument("-h", action="store_true")
args = parser.parse_args()

# Step 7: handle help
if args.h:
    usage()
    sys.exit(0)

# Step 8: assign supplied Stichtag
l_Stichtag = args.stichtag

# Step 9: compute default Stichtag when omitted
if not l_Stichtag:
    l_Stichtag = (date.today() - timedelta(days=7)).strftime("%Y%m%d")

# Step 10: write start message to stdout and log
start_msg = f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_Stichtag}"
print(start_msg)
with open(Protokoll_Datei, "w", encoding="utf-8") as log:
    log.write(start_msg + "\n")

# Step 11: run the Oracle SQL process
subprocess.run(
    ["sqlplus", "-s", DW_ORAUSER, "@d_abgl_kunde_woech.sql", l_Stichtag],
    check=True,
    stdout=open(Protokoll_Datei, "a", encoding="utf-8"),
    stderr=subprocess.STDOUT,
)

# Step 12: count discrepancy markers in the log
with open(Protokoll_Datei, encoding="utf-8") as log:
    l_Abweichungen = sum(1 for line in log if line.startswith("ABWEICHUNG"))

# Step 13: append discrepancy count to the log
with open(Protokoll_Datei, "a", encoding="utf-8") as log:
    log.write(f"Anzahl gefundener Abweichungen: {l_Abweichungen}\n")

# Step 14: warn if discrepancies were found
if l_Abweichungen > 0:
    f_alis_msgerr("W", f"{l_Abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {Protokoll_Datei}")

# Step 15: print final success message
print("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.ksh` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.py` | Converts the KornShell wrapper script into a Python script, preserving its parameter parsing, default-date derivation, file-logging, discrepancy checking, and exit behavior. |

---

### Execution Order
The legacy execution sequence consists of the following three steps:
1. **Orchestration**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml` (Handles the weekly UC4 job scheduling and trigger)
2. **Shell script wrapper**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.ksh` (Runs the wrapper executing the SQL and checking outputs)
3. **SQL Script**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql` (Performs the actual master data address reconciliation)

**Target Mapping**:
* **Step 1** is an orchestration component. Its migration is owned by a different design pass (e.g., converting UC4 to a Cloud Composer DAG).
* **Step 2** is handled by this design pass. The target file is `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.py` and will be executed by a PythonOperator, BashOperator, or KubernetesPodOperator within the Cloud Composer DAG.
* **Step 3** is a database component. Its conversion to BigQuery SQL / Dataform is owned by a separate design pass. In Python, this step will translate from calling `sqlplus` to executing the migrated BigQuery SQL script (either via the BigQuery client library or a Dataform job trigger, depending on the SQL design pass).

---

### Lineage
* **Upstream Producer**: The orchestrator `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml` (via the parent workflow/job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`, which belongs to a different assembled job group) triggers this wrapper.
* **Downstream Consumer**: The wrapper script executes the Oracle SQL script `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql`.

---

### External System Replacements
* **Oracle Database & SQL\*Plus**: In the legacy environment, the script connects to Oracle (`DWHP1`) via `sqlplus` using credentials `dwh_kern/...`. In the target Google Cloud environment, this execution is replaced with BigQuery SQL execution using the native BigQuery Python client API (`google.cloud.bigquery`) or Dataform.
* **Local Filesystem Logs**: Logs are written to `${DW_DIR_LOG}/kunde/...`. In Google Cloud, logging must write to Standard Output/Error so Cloud Logging captures it, or be redirected to a GCS bucket (`GCS_BUCKET/logs/kunde/...`) for persistent log archiving.

---

### Target File Plan
* **File Path**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.py`
  * **Language**: Python (3.x)
  * **Source File**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.ksh`
  * **Description**: A Python translation of the wrapper script. It parses arguments (retaining `-s` and `-h`), computes the default Stichtag (7 days ago) in native Python, executes the translated BigQuery SQL (via BigQuery API), counts discrepancies based on target output patterns, and implements logging. No SQL code or logic is embedded in this file. (Implementation logic is fully detailed in the automatically attached MCP output).

---

### Environment-Specific Values

| Legacy Variable | Role Classification | Target Equivalent / Sourcing Mechanism |
| :--- | :--- | :--- |
| `DW_DIR_ROOT` | **GLOBAL** | Mirrored target directory root on Composer/local workspace, or a relative root path. |
| `DW_DIR_LOG` | **GLOBAL** | Mapped to `GCS_BUCKET` for logging, or sourced from system environment using `os.environ.get("DW_DIR_LOG")`. |
| `DW_ORAUSER` / connection to `DWHP1` | **GLOBAL** | Replaced with native GCP environment settings: `GCP_PROJECT`, `BQ_DATASET`, and `BQ_LOCATION`. Sourced via `os.environ` or Airflow Variables. |
| `DWH_ORA_PWD` | **GLOBAL** | No longer needed for SQL\*Plus. BigQuery authentication is handled via GCP Service Accounts / IAM. |
| `l_Stichtag` | **JOB-SPECIFIC** | Sourced via argparse (`-s` / `--stichtag`) or Airflow DAG parameters/macros (e.g., `{{ ds_nodash }}` or custom context parameter) when executed inside Composer. |

---

### Risks and Manual Steps
1. **Dependency on External SQL File**: The shell script triggers `sqlplus ... @d_abgl_kunde_woech.sql`. This SQL script is not part of this design group. The integration of the Python code with the output of the migrated BigQuery SQL script must be manually verified. Specifically, the Python code parses the log file for lines starting with `ABWEICHUNG`. If the BigQuery query translation returns discrepancies through a table or custom log output instead of text printing, the logic counting `l_Abweichungen` must be adjusted accordingly (e.g., checking if a discrepancy table contains rows).
2. **Hardcoded Home Directory Paths**: The script references paths starting with `$HOME/aktuell/...`. These directories do not exist on the target Cloud Composer (GCP) workers. The environment-wide configuration (`DW_DIR_ROOT` and `DW_DIR_LOG`) must be set globally in the environment (Airflow variables or environment variables) to point to GCS buckets or valid container paths.
3. **No Translation of Print Statements**: All print and log messages are retained verbatim in German to ensure downstream log-monitoring and alerting tools parsing these logs do not fail. For example, `"Starte Adressabgleich Kundenstammdaten fuer Stichtag"`, `"Anzahl gefundener Abweichungen: "`, and `"Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet"` must not be modified or translated.

---

=== FILE: DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql ===
whenever sqlerror exit failure

DEFINE p_Stichtag='&1'

set pagesize 0
set linesize 300
set feedback off
set heading off

select
  'ABWEICHUNG' as MARKER,
  k.KUNDE,
  k.NACHNAME,
  k.VORNAME,
  k.PLZ,
  k.ORT,
  k.STRASSE,
  r.PLZ       as REF_PLZ,
  r.ORT       as REF_ORT,
  r.STRASSE   as REF_STRASSE
from DWH_KERN.T_KUNDE k
join STAMMDATEN.T_KUNDE_REFERENZ r
  on r.KUNDE = k.KUNDE
where k.AKTUALISIERT_AM <= to_date('&p_Stichtag','YYYYMMDD')
  and (
        nvl(k.PLZ,'x')     != nvl(r.PLZ,'x')
     or nvl(k.ORT,'x')     != nvl(r.ORT,'x')
     or nvl(k.STRASSE,'x') != nvl(r.STRASSE,'x')
      )
order by k.KUNDE;

exit;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Statement / Construct | Target | Rejected Alternatives | Evidence | Exact Reason |
|---|---|---|---|---|
| `SELECT ... FROM ... JOIN ... WHERE ... ORDER BY ...` | Direct BigQuery Standard SQL | UDF, Python wrapper, Manual intervention | Single set-based query only | Fully expressible in BigQuery SQL |
| `DEFINE p_Stichtag='&1'` | Manual intervention / parameter binding | Direct SQL, UDF, Python wrapper | SQL*Plus substitution variable | BigQuery does not support SQL*Plus `DEFINE`; caller must pass a query parameter or script variable |
| `whenever sqlerror exit failure`, `set pagesize`, `set linesize`, `set feedback`, `set heading`, `exit` | Direct BigQuery Standard SQL with stripping of client directives | UDF, Python wrapper | Client/session directives only | Not part of BigQuery SQL semantics |
| `to_date('&p_Stichtag','YYYYMMDD')` | Direct BigQuery Standard SQL rewrite | UDF, Python wrapper | Deterministic date parsing | Can be rewritten to `PARSE_DATE('%Y%m%d', @p_Stichtag)` |
| `nvl(x,'x')` | Direct BigQuery Standard SQL rewrite | UDF, Python wrapper | Simple null substitution | Can be rewritten to `COALESCE(x, 'x')` |
| `!=` comparisons on strings | Direct BigQuery Standard SQL | UDF, Python wrapper | Standard comparison operator | Supported directly |
| `ORDER BY k.KUNDE` | Direct BigQuery Standard SQL | UDF, Python wrapper | Standard ordering | Supported directly |

2.17 REQUIRED ARTIFACTS

| Artifact | Required | Details |
|---|---|---|
| BigQuery SQL | Yes | Single SELECT statement with parameterized date parsing and COALESCE-based null handling |
| CREATE TEMP FUNCTION UDF | No | Not required |
| Python wrapper | No | Not required |
| Manual task | Yes | Replace SQL*Plus substitution variable `&1` / `&p_Stichtag` with a BigQuery query parameter or script variable supplied by the caller |

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Type / Inferred Type | BigQuery Type | Conversion Rule | Warning |
|---|---|---|---|
| `K.KUNDE` | STRING or INT64 depending source definition | Preserve as-is in projection and join key | Source type not declared in script; confirm actual schema |
| `K.NACHNAME` | STRING | `VARCHAR2`/`NVARCHAR2` → STRING | None |
| `K.VORNAME` | STRING | `VARCHAR2`/`NVARCHAR2` → STRING | None |
| `K.PLZ` | STRING | `VARCHAR2`/`CHAR` → STRING | Null-handling comparison uses sentinel `'x'` |
| `K.ORT` | STRING | `VARCHAR2`/`CHAR` → STRING | None |
| `K.STRASSE` | STRING | `VARCHAR2`/`CHAR` → STRING | None |
| `K.AKTUALISIERT_AM` | DATETIME or TIMESTAMP | Oracle `DATE` includes time component; parse/compare using a compatible temporal type | Source column type not declared; if stored as Oracle DATE, preserve time semantics with DATETIME/TIMESTAMP rather than DATE |
| `r.KUNDE` | STRING or INT64 depending source definition | Preserve as-is in join predicate | Source type not declared in script |
| `r.PLZ` | STRING | `VARCHAR2`/`CHAR` → STRING | None |
| `r.ORT` | STRING | `VARCHAR2`/`CHAR` → STRING | None |
| `r.STRASSE` | STRING | `VARCHAR2`/`CHAR` → STRING | None |
| `&p_Stichtag` input | STRING parameter | Parse with explicit format | Must be supplied externally in `YYYYMMDD` format |

2.19 DESIGN REVIEW SUMMARY

| Item | Summary |
|---|---|
| Patterns/objects found | Standalone SELECT query with inner join, date filter, null-safe string difference checks, ordering |
| Unsupported functions | SQL*Plus `DEFINE`, `whenever sqlerror`, `set pagesize`, `set linesize`, `set feedback`, `set heading`, `exit` |
| UDF required | No |
| Python required | No |
| Direct dependencies | `DWH_KERN.T_KUNDE`, `STAMMDATEN.T_KUNDE_REFERENZ` |
| Assumptions | `&p_Stichtag` is provided externally as `YYYYMMDD`; `AKTUALISIERT_AM` is compared using a compatible temporal type; `KUNDE` key types match across both tables |
| Warnings | Oracle SQL*Plus client directives must be removed; source column data types are not declared in the script; sentinel `'x'` may collide with real data values if present |
| Manual-intervention items | Bind/query-parameter replacement for `&1` / `&p_Stichtag`; schema confirmation for join and filter columns |
| Ready for human approval | Yes, pending parameter binding and schema confirmation |

2.20 PACKAGE ANALYSIS — not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery — Direct | BigQuery Equivalent / Alternative |
|---|---|---|
| `DEFINE p_Stichtag='&1'` | Unsupported | none — manual intervention |
| `whenever sqlerror exit failure` | Unsupported | none — manual intervention |
| `set pagesize 0` | Unsupported | none — manual intervention |
| `set linesize 300` | Unsupported | none — manual intervention |
| `set feedback off` | Unsupported | none — manual intervention |
| `set heading off` | Unsupported | none — manual intervention |
| `to_date('&p_Stichtag','YYYYMMDD')` | Direct-with-rewrite | `PARSE_DATE('%Y%m%d', @p_Stichtag)` |
| `nvl(k.PLZ,'x')` | Direct-with-rewrite | `COALESCE(k.PLZ, 'x')` |
| `nvl(r.PLZ,'x')` | Direct-with-rewrite | `COALESCE(r.PLZ, 'x')` |
| `nvl(k.ORT,'x')` | Direct-with-rewrite | `COALESCE(k.ORT, 'x')` |
| `nvl(r.ORT,'x')` | Direct-with-rewrite | `COALESCE(r.ORT, 'x')` |
| `nvl(k.STRASSE,'x')` | Direct-with-rewrite | `COALESCE(k.STRASSE, 'x')` |
| `nvl(r.STRASSE,'x')` | Direct-with-rewrite | `COALESCE(r.STRASSE, 'x')` |
| `exit` | Unsupported | none — manual intervention |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- BigQuery Standard SQL pseudocode
-- Manual input requirement: provide p_Stichtag as a query parameter in YYYYMMDD format

SELECT
  'ABWEICHUNG' AS MARKER,
  k.KUNDE,
  k.NACHNAME,
  k.VORNAME,
  k.PLZ,
  k.ORT,
  k.STRASSE,
  r.PLZ AS REF_PLZ,
  r.ORT AS REF_ORT,
  r.STRASSE AS REF_STRASSE
FROM DWH_KERN.T_KUNDE AS k
JOIN STAMMDATEN.T_KUNDE_REFERENZ AS r
  ON r.KUNDE = k.KUNDE
WHERE k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', @p_Stichtag)  -- converted from TO_DATE('&p_Stichtag','YYYYMMDD'); confirm temporal type compatibility if AKTUALISIERT_AM is DATETIME/TIMESTAMP
  AND (
       COALESCE(k.PLZ, 'x') != COALESCE(r.PLZ, 'x')           -- converted from NVL(k.PLZ,'x') != NVL(r.PLZ,'x')
    OR COALESCE(k.ORT, 'x') != COALESCE(r.ORT, 'x')           -- converted from NVL(k.ORT,'x') != NVL(r.ORT,'x')
    OR COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x')   -- converted from NVL(k.STRASSE,'x') != NVL(r.STRASSE,'x')
      )
ORDER BY k.KUNDE;
```

FLAGGED ITEMS FOR HUMAN REVIEW

- Replace SQL*Plus substitution variable `&1` / `&p_Stichtag` with a BigQuery query parameter or script variable.
- Confirm the actual BigQuery-compatible type for `DWH_KERN.T_KUNDE.AKTUALISIERT_AM`; if it is DATETIME or TIMESTAMP, the filter must be adjusted to a matching temporal parse/cast instead of DATE comparison.
- Confirm the actual data types of `KUNDE` in both tables to ensure join-key compatibility.

# MIGRATION DESIGN DOCUMENT
**Source Job:** `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`  
**Target Platform:** BigQuery  

---

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql` | Migrates the Oracle SQL*Plus script to a BigQuery-compatible standard SQL file. Removes legacy SQL*Plus formatting commands and configures runtime query parameters. |

---

### Job dependencies
Based on the **LINEAGE EDGES** in the source context:
- **Upstream Dependency**: `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` (associated with job file `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`). This upstream process is responsible for producing the tables `T_KUNDE` and `T_KUNDE_REFERENZ`.
- **Target Orchestration Wiring**: To prevent race conditions, the Airflow DAG running this query must include an upstream sensor (e.g. an `ExternalTaskSensor` or a BigQuery table sensor) that verifies the completion of `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` before executing the reconciliation.

---

### Execution order
The target orchestration (Airflow/Cloud Composer) must preserve the 3-step legacy execution sequence:
1. **Orchestration Definition**: Legacy UC4 Job definition `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml` is converted to an Airflow DAG.
2. **Execution Wrapper**: Legacy wrapper `r_abgl_kunde_woech.ksh` is migrated to an Airflow Python task (e.g., executing a `BigQueryInsertJobOperator`).
3. **Core SQL Script**: The BigQuery SQL script corresponding to `d_abgl_kunde_woech.sql` is invoked by the Airflow task as the final step.

---

### Lineage
- **Upstream Input Tables**: 
  - `DWH_KERN.T_KUNDE` (Produced by job: `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`)
  - `STAMMDATEN.T_KUNDE_REFERENZ` (Produced by job: `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`)
- **Downstream Consumer**:
  - The direct output of this query is a projection of mismatched addresses. In the target environment, the stdout/result of this BigQuery script should be logged, directed to a report table, or sent as an automated notification.

---

### External system replacements
- **Database Backend**: The Oracle Database is fully replaced by **BigQuery**. Legacy Oracle schemas map directly to BigQuery datasets:
  - `DWH_KERN` $\rightarrow$ `DWH_KERN` BigQuery dataset.
  - `STAMMDATEN` $\rightarrow$ `STAMMDATEN` BigQuery dataset.
- **Orchestration / Client Shell**: Oracle SQL*Plus client-specific session directives (such as `set pagesize`, `set linesize`, `set feedback`, `set heading`, and `whenever sqlerror exit failure`) are retired and replaced by native Airflow task failure configurations and logging.

---

### Cross-file dependencies
- **Wrapper Execution**: This SQL script requires parameters and execution context supplied by the wrapper script `r_abgl_kunde_woech.ksh` (which is managed in a sibling design pass).
- **Shared Datasets**: The tables `T_KUNDE` and `T_KUNDE_REFERENZ` are shared models in the data warehouse, meaning the schemas must remain compatible across both the producer job (`DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`) and this consumer script.

---

### Target file plan
- **Target Path**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql`
- **Source Path**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql`
- **Language**: SQL (BigQuery Standard SQL)
- **Mapping Details**: This file implements the BigQuery Standard SQL translation. It replaces SQL*Plus substitution variables with standard `@p_Stichtag` query parameters, utilizes `PARSE_DATE` for parameter parsing, and implements standard `COALESCE` logic for null handling.

---

### Environment-specific values
Values from the legacy script are classified and mapped according to target environment roles:

1. **GLOBAL (Environment-Wide Infrastructure)**
   - **GCP_PROJECT**: The target Google Cloud Project ID. Sourced at runtime from the environment configuration or Airflow Variable.
   - **BQ_DATASET (DWH_KERN, STAMMDATEN)**: The target datasets. Sourced as standard dataset aliases within the environment (e.g. dynamically prefixed based on development/production environments).

2. **JOB-SPECIFIC (Query Parameters)**
   - **p_Stichtag**: The key reporting/reconciliation date. Passed at runtime by the calling Airflow task as a BigQuery query parameter (`@p_Stichtag`) in `YYYYMMDD` format.

---

### Risks and manual steps
- **Date Comparison Type Safety**: In the legacy source, `k.AKTUALISIERT_AM <= to_date('&p_Stichtag','YYYYMMDD')` is compared. If the target column `k.AKTUALISIERT_AM` in BigQuery is a `DATETIME` or `TIMESTAMP` type, an implicit conversion failure may occur. The database schema must be verified; if it is a timestamp, the query parameter must be cast accordingly (e.g., `CAST(PARSE_DATE('%Y%m%d', @p_Stichtag) AS TIMESTAMP)`).
- **String Null Comparison Sentinel**: The script uses a sentinel value of `'x'` in `NVL(..., 'x')` / `COALESCE(..., 'x')`. If `'x'` is a valid value in any of the checked address fields (`PLZ`, `ORT`, `STRASSE`), this sentinel approach can hide legitimate discrepancies. It is recommended to verify if a more robust comparison strategy is required.
- **Literal Conservation Rule**: Per the Output/Print Literal Rule, the literal German text string `'ABWEICHUNG'` returned in the SELECT statement as `MARKER` must be conserved exactly as-is in the target SQL query to maintain reporting compatibility with downstream systems.