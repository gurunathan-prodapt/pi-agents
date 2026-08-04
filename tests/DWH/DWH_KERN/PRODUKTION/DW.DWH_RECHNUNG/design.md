=== OBJECT: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP (JOBP) ===
active=1
title=Taeglicher Export der Rechnungsdaten (RECHNUNG) aus der DWH-Kernschicht in das Reporting-Verzeichnis
max_parallel_runs=0
ert_seconds=15
sync_rows:
  (none)
tasks:
  Lnr=1 OType=<START> Object=START predecessors=[] run_when='' else_halt=0 earliest_start_time=00:00 fire_and_forget_flag=1 calendar_on=0 calendar_name=None
  Lnr=2 OType=JOBS_UNIX Object=DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS predecessors=[] run_when='' else_halt=0 earliest_start_time=00:00 fire_and_forget_flag=1 calendar_on=0 calendar_name=None
  Lnr=3 OType=<END> Object=END predecessors=[] run_when='' else_halt=0 earliest_start_time=00:00 fire_and_forget_flag=1 calendar_on=0 calendar_name=None

=== OBJECT: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS (JOBS_UNIX) ===
active=1
title=Job startet Shellscript r_exp_rechnung_taeglich.ksh zum Export der Rechnungsdaten
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=None
launcher_type=unrecognized
launcher_details={'raw_command': ":SET &DWH_JOB_KENNUNG = 'RECHNUNG_EXPORT_TAEGLICH'"}
script_body:
:SET &DWH_JOB_KENNUNG = 'RECHNUNG_EXPORT_TAEGLICH'
:SET &EXPORT_STICHTAG = SYS_DATE("YYYYMMDD")

$HOME/aktuell/dw_source/isdwh/exporter/rechnung/bin/r_exp_rechnung_taeglich.ksh -s &EXPORT_STICHTAG

:PRINT "Rechnungsexport fuer Stichtag &EXPORT_STICHTAG angestossen"
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


## 1. Overview

This UC4 bundle defines a simple two-step workflow for exporting daily invoice/rechnung data from the DWH core layer into a reporting directory. The workflow is started by the job plan `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`, which immediately launches a Unix job that prepares a business date and invokes a shell script for the export. No calendar object is present in the extraction, so there is no native schedule visible here; the workflow appears to be triggered externally or by a mechanism not included in this bundle. There are no unresolved references and no cross-workflow dependencies shown.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---:|---|
| DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP | JOBP | 1 | Taeglicher Export der Rechnungsdaten (RECHNUNG) aus der DWH-Kernschicht in das Reporting-Verzeichnis |
| DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS | JOBS_UNIX | 1 | Job startet Shellscript r_exp_rechnung_taeglich.ksh zum Export der Rechnungsdaten |

---

## 3. Scheduling

No EVNT_TIME object is present in this extraction, so this workflow has no calendar-based schedule of its own in the supplied bundle.

Trigger source assessment:
- No SCRI object in this bundle targets `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`
- No other JOBP in this bundle references this workflow
- Therefore, the workflow is externally triggered or the trigger source is unknown from this extraction alone

---

## 4. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_dwh_rechnung_export_taeglich_jp` |
| schedule | `None` |
| start_date | `placeholder_start_date` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args | `retries=0, retry_delay=5 minutes, owner='uc4_migration'` |

Notes:
- `max_parallel_runs=0` is treated as no explicit concurrency allowance in UC4; for this simple workflow, `max_active_runs=1` is the safe default.
- No self-lock or cross-lock sync rows are present.

---

## 5. Task Inventory

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---:|---|---|---|---|---|---|
| `dw_dwh_rechnung_export_taeglich_js` | `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` | `EmptyOperator` | N/A | N/A | 0 | 5 minutes | 00:00 | None | N/A | None | `launcher command ":SET &DWH_JOB_KENNUNG = 'RECHNUNG_EXPORT_TAEGLICH'" not recognised — confirm target operator/script manually` |

### Exclusions
- `<START>` and `<END>` are pseudo-tasks and are not included as real work items.

---

## 6. Task Dependency Map

For `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`:

`dw_dwh_rechnung_export_taeglich_js`

Reasoning:
- The only real task is the JOBS_UNIX task at Lnr=2.
- No predecessor relationships are defined in the extraction, so there is no explicit task-to-task chain beyond the single task itself.

---

## 7. Sync / Concurrency Analysis

No sync rows are present for this JOBP.

| UC4 Sync Else value | lock_kind | Airflow mapping |
|---|---|---|
| N/A | N/A | No sync-based concurrency constraint detected |

---

## 8. Error Handling and Retry Strategy

### Task: `dw_dwh_rechnung_export_taeglich_js`
- The launcher is classified as `unrecognized`, so no reliable retry/error translation can be derived from the launcher itself.
- No postcondition actions were supplied for this task.
- No earliest-start delay is required because `earliest_start_time=00:00`.
- No calendar gating is required because `calendar_on=0`.
- Since this is not a trigger task, `fire_and_forget_flag` does not apply.

No `on_failure_callback` is mandated by the extraction.

---

## 9. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` | UC4 JOBP object name | `dag_id = dw_dwh_rechnung_export_taeglich_jp` |
| `active=1` | JOBP active flag | `is_paused_upon_creation=False` |
| `max_parallel_runs=0` | JOBP concurrency setting | `max_active_runs=1` |
| `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` | UC4 JOBS_UNIX object name | Airflow task `dw_dwh_rechnung_export_taeglich_js` |
| `launcher_type=unrecognized` | Parsed launcher classification | `EmptyOperator` stub pending manual implementation |
| `&DWH_JOB_KENNUNG` | UC4 script variable assignment | No direct Airflow equivalent; preserve in eventual script logic |
| `&EXPORT_STICHTAG` | UC4 script variable assignment | No direct Airflow equivalent; preserve in eventual script logic |
| `SYS_DATE("YYYYMMDD")` | UC4 runtime date function | Use Airflow execution date / logical date formatting in eventual implementation |

---

## 10. Developer Notes

- `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` has `launcher_type=unrecognized`; the raw command is `:SET &DWH_JOB_KENNUNG = 'RECHNUNG_EXPORT_TAEGLICH'`. This is a structural extraction gap and must be manually mapped to the correct Airflow operator/script.
- No schedule object is present; the workflow is externally triggered or trigger source is unknown from this extraction alone.
- No unresolved references were listed in the extraction.
- The shell script path visible in the script body is `$HOME/aktuell/dw_source/isdwh/exporter/rechnung/bin/r_exp_rechnung_taeglich.ksh -s &EXPORT_STICHTAG`; this should be preserved as implementation context when the launcher is manually resolved.
- The UC4 script uses `SYS_DATE("YYYYMMDD")`; the build stage should translate this to the Airflow logical date or execution date in a deterministic way.

---

# Pseudocode Outline

1. **Imports**
   - Import Airflow DAG, `EmptyOperator`
   - Import any needed standard operators/utilities for future manual replacement
   - Import `datetime` and `timedelta`
   - Prepare placeholders for GCP operators if launcher resolution changes later

2. **GCP Configuration**
   - Define placeholder constants:
     - `PROJECT_ID = "YOUR_PROJECT_ID"`
     - `REGION = "YOUR_REGION"`
     - `CLUSTER_NAME = "YOUR_CLUSTER_NAME"`
     - `GCS_BUCKET = "YOUR_BUCKET_NAME"`

3. **Default Args**
   - `default_args = {`
     - `owner: "uc4_migration"`
     - `retries: 0`
     - `retry_delay: timedelta(minutes=5)`
   - `}`

4. **on_failure_callback stubs**
   - Define a generic stub callback function:
     - Accept `context`
     - Log that `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` requires manual launcher resolution
     - Do not raise unless implementation requires it
   - No specific UC4 postcondition callback is derivable from the extraction

5. **DAG Definition**
   - Create DAG:
     - `dag_id="dw_dwh_rechnung_export_taeglich_jp"`
     - `schedule=None`
     - `start_date=placeholder_start_date`
     - `catchup=False`
     - `max_active_runs=1`
     - `is_paused_upon_creation=False`
     - `default_args=default_args`

6. **Guard Task**
   - None required
   - No self-lock `Else=Skip` sync detected

7. **Sensor Task**
   - None required
   - No `earliest_start_time` other than `00:00`

8. **Calendar Check Task**
   - None required
   - No `calendar_on=1` detected

9. **Task: `dw_dwh_rechnung_export_taeglich_js`**
   - Create `EmptyOperator(task_id="dw_dwh_rechnung_export_taeglich_js")`
   - Add comment:
     - launcher command `:SET &DWH_JOB_KENNUNG = 'RECHNUNG_EXPORT_TAEGLICH'` not recognised
     - manual replacement required
   - No retries beyond default
   - No special callback unless developer later maps the launcher

10. **Dependencies**
    - Since only one real task exists, no task chaining is required
    - If future launcher resolution converts this into a trigger or executable task, keep it as the sole task under the DAG unless additional UC4 tasks are introduced

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml | DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py | Main Airflow DAG orchestrating the export workflow. |
| DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml | DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py | Converted into an Airflow task (`dw_dwh_rechnung_export_taeglich_js`) within the main DAG. Since both source files reside in the same folder, they are merged into a single target DAG file to maintain folder integrity. |

# Add Context the MCP Could Not See

### Execution order
The target Airflow DAG preserves the sequential step execution mapping:
* **Step 1:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` maps to the creation and configuration of the Airflow DAG container `dw_dwh_rechnung_export_taeglich_jp` in target file `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py`.
* **Step 2:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` maps to the Airflow task `dw_dwh_rechnung_export_taeglich_js` inside the DAG.
* **Step 3:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` (out of scope for this design pass) will be invoked by the `dw_dwh_rechnung_export_taeglich_js` task using its migrated Python equivalent.
* **Step 4:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` (out of scope for this design pass) is executed as part of the step 3 script execution via BigQuery SQL or Dataform.

### Lineage
* **Upstream:** None.
* **Downstream Consumer:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` (invoked script, belongs to a different group and is out of scope for this pass).
* **External Host Connection:** Host `DWHDWH1P` (represented as standard GCP execution environment).
* **System Login / Profile:** `DW.UNIX.ISTNS` (Login identity, mapped to GCP Service Account/IAM role or Airflow SSH Connection).

### External system replacements
* **Oracle DWH Host (`DWHDWH1P`)** -> Google Cloud Composer (GKE-based orchestration environment).
* **DWH Core Oracle Database** -> Google BigQuery.
* **UNIX Login (`DW.UNIX.ISTNS`)** -> Cloud Composer IAM service account or Airflow execution credentials.
* **Local Export Flat File Directory** -> Google Cloud Storage (GCS) export bucket (e.g., `gs://{GCS_BUCKET}/exports/rechnung/`).

### Cross-file dependencies
* The Airflow task `dw_dwh_rechnung_export_taeglich_js` dynamically executes the Python conversion of `r_exp_rechnung_taeglich.ksh`. This is a tight dependency on the migrated version of that script, which must be deployed to the Cloud Composer environment's environment folder or execution path.

### Target file plan
* `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/dw_dwh_rechnung_export_taeglich_jp.py` | Python (Airflow DAG) | Orchestrates the daily invoice data export workflow. It parses the reporting date dynamically and schedules/invokes the downstream processing task. Derived from `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` and `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml`.

### Environment-specific values
* `GCP_PROJECT`: **GLOBAL**. Defines the target Google Cloud Project ID. Sourced at runtime using `os.environ.get("GCP_PROJECT")` or Airflow variable `Variable.get("GCP_PROJECT")`.
* `GCS_BUCKET`: **GLOBAL**. Represents the target Cloud Storage bucket where the exported invoice flat files are loaded. Sourced at runtime via `Variable.get("GCS_BUCKET")`.
* `AIRFLOW_CONN_DW_UNIX_ISTNS`: **GLOBAL**. Airflow connection ID representing the target SSH / execution profile replacing the UNIX Login `DW.UNIX.ISTNS`.
* `DWH_JOB_KENNUNG`: **JOB-SPECIFIC**. Job identifier constant set to `'RECHNUNG_EXPORT_TAEGLICH'`.
* `EXPORT_STICHTAG`: **JOB-SPECIFIC**. Business reporting/extraction date. Resolved at runtime using Airflow logical/execution date formatting macro `{{ ds_nodash }}`.
* `SCRIPT_PATH`: **JOB-SPECIFIC**. Local path on Cloud Composer environment pointing to the migrated Python script replacing `r_exp_rechnung_taeglich.ksh`.

### Risks and manual steps
* **Unresolved Script Launcher (UC4 Parser Limitations):** The parser classified the launcher in `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` as `unrecognized` because it performs variable setting and a script invocation inside the UC4 script body. An `EmptyOperator` task placeholder (`dw_dwh_rechnung_export_taeglich_js`) is created. A manual step is required to replace the `EmptyOperator` with the appropriate execution operator (such as a `BashOperator` invoking the Python equivalent of the shell script, or a `PythonOperator` importing the converted script).
* **Downstream Script Dependency:** The target Airflow task calls the translated version of `r_exp_rechnung_taeglich.ksh`. The actual conversion of this script (and its associated SQL script `d_exp_rechnung_taeglich.sql`) is out of scope for this pass and must be coordinated so that the script is deployed and runnable in the environment.
* **OUTPUT/PRINT LITERAL RULE:** The UC4 script logs the German message `"Rechnungsexport fuer Stichtag &EXPORT_STICHTAG angestossen"`. In compliance with the rules, this literal text must be preserved in German in the log/print statement of the final target Python code: `"Rechnungsexport fuer Stichtag {stichtag} angestossen"`.

---

=== FILE: DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh ===
#!/bin/ksh_dwh
#-----------------------------------------------------------------------------
#- Ersterstellung am:                           2019-05-14T09:00:00+02:00
#- Ersterstellung durch:                        khoffmann
#- Aenderung:                                   2023-02-02  jschulz  - Umstellung auf neuen Exportpfad
#-----------------------------------------------------------------------------
ProgName="Ausfuehrung Script $0"
ProgVersion="1.2.0"

usage(){
cat <<EOF
   Programm: $ProgName
   Zweck: Taeglicher Export der Rechnungsdaten (RECHNUNG) aus DWH_KERN
          in das Reporting-Austauschverzeichnis
   Parameter:
       -s     Stichtag (Format: 'YYYYMMDD')
EOF
}

DW_DIR_ROOT="$HOME/aktuell/dw_source/isdwh"
DW_DIR_EXPORT="$HOME/aktuell/export"
DW_ORAUSER="dwh_kern/${DWH_ORA_PWD:-changeit}@DWHP1"

f_alis_msgerr() {
    typeset l_Level="$1"
    typeset l_Text="$2"
    echo "[$l_Level] $(date '+%Y-%m-%d %H:%M:%S') $l_Text" >&2
}

set -e

typeset l_Stichtag
Ziel_Verzeichnis="${DW_DIR_EXPORT}/rechnung/ausgang"

cd ${DW_DIR_ROOT}/exporter/rechnung/sql

while getopts ":s:h" param
do
    case $param in
        s) l_Stichtag="$OPTARG";;
        h) usage; exit;;
    esac
done

if [ -z "$l_Stichtag" ]; then
    l_Stichtag=$(date -d 'yesterday' '+%Y%m%d' 2>/dev/null || date '+%Y%m%d')
fi

echo "Starte Export Rechnungsdaten fuer Stichtag $l_Stichtag"

Export_Datei="${Ziel_Verzeichnis}/rechnung_export_${l_Stichtag}.dat"

sqlplus -s ${DW_ORAUSER} @d_exp_rechnung_taeglich.sql $l_Stichtag > "$Export_Datei"

l_Anzahl=$(wc -l < "$Export_Datei" | tr -d ' ')
echo "Anzahl exportierter Rechnungssaetze: $l_Anzahl"

if [ "$l_Anzahl" -eq 0 ]; then
    f_alis_msgerr "W" "Keine Rechnungsdaten fuer Stichtag $l_Stichtag exportiert"
fi

echo "Export Rechnungsdaten ohne erkennbare Fehler beendet"


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains argument parsing, date fallback logic, logging, file output, and a SQL*Plus invocation whose result is post-processed, so it is not a pure wrapper and must be converted.

EVIDENCE
- Business logic found: KSH custom logic only; it parses `-s/-h`, derives a default Stichtag from yesterday or today, builds an output filename, runs SQL*Plus, counts exported lines, and emits a warning when no rows were exported.
- AWK: none
- SQL-expressible: partly; the data extraction itself is SQL, but the shell-side date fallback, file naming, line counting, and warning logic are not pure SQL orchestration.
- Non-SQL side effects: writes `rechnung_export_${l_Stichtag}.dat` to the export directory; invokes `sqlplus`; writes warning/error messages to stderr/stdout.
- Against this verdict: the core export query is executed via SQL*Plus and could be viewed as mostly database work, but the surrounding control flow and file handling require Python.

1. SCRIPT OVERVIEW
This script performs a daily invoice-data export for the RECHNUNG domain from `DWH_KERN` into a reporting exchange directory. It is triggered manually or by UC4 with an optional cutoff date (`-s YYYYMMDD`), defaults the date to yesterday if omitted, runs an Oracle SQL*Plus export, and reports how many records were written. If no rows are exported, it logs a warning.

2. INVOCATION CONTEXT
- Called by: unknown UC4 job name / JOBS_UNIX object not supplied in the extraction
- Command line / arguments: `-s <Stichtag>` optionally, `-h` for usage
- UC4 native includes: none observed
- Environment files sourced: none observed

3. PARAMETERS / INPUTS
- `-s` / `l_Stichtag`
  - Source: command-line option parsed via `getopts`
  - Used in script body: yes
  - Python surface: `argparse.ArgumentParser()` with optional `--stichtag` or `-s`
- `-h`
  - Source: command-line option parsed via `getopts`
  - Used in script body: yes, prints usage and exits
  - Python surface: `argparse` help flag
- `DW_ORA_PWD`
  - Source: environment variable
  - Used in script body: yes, via `${DWH_ORA_PWD:-changeit}`
  - Python surface: `os.environ.get("DWH_ORA_PWD", "changeit")`
- `HOME`
  - Source: environment variable
  - Used in script body: yes, for directory construction
  - Python surface: `os.environ["HOME"]`
- `l_Stichtag` default derivation
  - Source: shell date command fallback
  - Used in script body: yes
  - Python surface: `datetime` logic in Python, not a parameter
- `DW_DIR_ROOT`, `DW_DIR_EXPORT`, `DW_ORAUSER`, `Ziel_Verzeichnis`, `Export_Datei`
  - Source: internal assignments
  - Used in script body: yes
  - Python surface: plain variables

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- `date '+%Y-%m-%d %H:%M:%S'`
  - Purpose: timestamp for warning/error logging
  - Python handling: native `datetime.now()` formatting, not subprocess
  - RESOLVABLE LAUNCHER: no; this is a standard utility, not a DB launcher
- `date -d 'yesterday' '+%Y%m%d' 2>/dev/null || date '+%Y%m%d'`
  - Purpose: derive default Stichtag
  - Python handling: native `datetime` logic, not subprocess
  - RESOLVABLE LAUNCHER: no
- `cd ${DW_DIR_ROOT}/exporter/rechnung/sql`
  - Purpose: change working directory before SQL*Plus invocation
  - Python handling: `os.chdir(...)`
  - RESOLVABLE LAUNCHER: no
- `sqlplus -s ${DW_ORAUSER} @d_exp_rechnung_taeglich.sql $l_Stichtag > "$Export_Datei"`
  - Purpose: run Oracle SQL*Plus export script and redirect output to the export file
  - Python handling: should become a native Oracle DB-client implementation if the SQL file is available; otherwise preserve as subprocess call to `sqlplus`
  - RESOLVABLE LAUNCHER: yes, partially — the launcher is `sqlplus`, and the invocation clearly indicates Oracle SQL*Plus dialect; however the SQL file body is not supplied here, so the exact SQL cannot be inlined from this extraction alone

5. EMBEDDED SQL
- Source file / label: `@d_exp_rechnung_taeglich.sql`
  - Full SQL text exactly as extracted: not supplied in the extraction
  - Statement type: unknown
  - Table(s) touched: unknown
  - Dialect identifiable: Oracle SQL*Plus is strongly implied by `sqlplus -s`, `@file.sql`, and the Oracle-style connection string `${DWH_ORA_PWD:-changeit}@DWHP1`

6. CONTROL FLOW
1. Set script metadata variables `ProgName` and `ProgVersion` — boilerplate.
2. Define `usage()` function to print help text.
3. Set directory and Oracle connection variables.
4. Define `f_alis_msgerr()` helper to print timestamped messages to stderr.
5. Enable `set -e` so failures abort the script.
6. Declare `l_Stichtag` and compute `Ziel_Verzeichnis`.
7. Change directory to `${DW_DIR_ROOT}/exporter/rechnung/sql`.
8. Parse command-line options with `getopts ":s:h"`.
9. If `-s` is provided, store the argument in `l_Stichtag`.
10. If `-h` is provided, print usage and exit.
11. If `l_Stichtag` is empty, compute yesterday’s date in `YYYYMMDD`, falling back to today if GNU `date -d` is unavailable.
12. Print start message.
13. Build `Export_Datei` path using the Stichtag.
14. Run `sqlplus -s ${DW_ORAUSER} @d_exp_rechnung_taeglich.sql $l_Stichtag > "$Export_Datei"`.
15. Count lines in the export file with `wc -l`.
16. Print the number of exported records.
17. If the count is zero, call `f_alis_msgerr "W" ...`.
18. Print completion message.

7. ERROR HANDLING & EXIT CODES
- Failure detection: `set -e` causes the script to exit on any failing command, including `cd`, `sqlplus`, `wc`, or `tr`
- Failure reaction: immediate termination; no explicit cleanup is present
- Success exit code convention: implicit shell success (`0`) if all commands succeed
- Python mapping: use `subprocess.run(..., check=True)` for external commands; use `try/except` around DB calls and filesystem operations; propagate failures with `sys.exit(code)` or re-raise exceptions

8. OUTPUTS / SIDE EFFECTS
- Writes export file: `${HOME}/aktuell/export/rechnung/ausgang/rechnung_export_${l_Stichtag}.dat`
- Writes informational messages to stdout
- Writes warning messages to stderr via `f_alis_msgerr`
- Reads from Oracle via SQL*Plus

9. BUSINESS SUMMARY
- Determines the reporting cutoff date for the daily invoice export.
- Executes an Oracle SQL*Plus export script for that date.
- Stores the exported invoice data in a dated `.dat` file under the reporting exchange directory.
- Counts exported rows and warns if none were produced.
- Emits start and completion status messages for operational monitoring.

1. # Step 1: Define script metadata and helper functions
   import argparse
   import os
   import sys
   from datetime import datetime, timedelta

   def usage():
       print("   Programm: Ausfuehrung Script ...")
       print("   Zweck: Taeglicher Export der Rechnungsdaten (RECHNUNG) aus DWH_KERN")
       print("          in das Reporting-Austauschverzeichnis")
       print("   Parameter:")
       print("       -s     Stichtag (Format: 'YYYYMMDD')")

   def f_alis_msgerr(level, text):
       print(f"[{level}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {text}", file=sys.stderr)

2. # Step 2: Parse command-line arguments
   parser = argparse.ArgumentParser(add_help=False)
   parser.add_argument("-s", dest="stichtag")
   parser.add_argument("-h", action="store_true")
   args, _ = parser.parse_known_args()

3. # Step 3: Handle help request
   if args.h:
       usage()
       sys.exit(0)

4. # Step 4: Resolve environment-based directories and credentials
   dw_dir_root = os.path.join(os.environ["HOME"], "aktuell", "dw_source", "isdwh")
   dw_dir_export = os.path.join(os.environ["HOME"], "aktuell", "export")
   dw_orapwd = os.environ.get("DWH_ORA_PWD", "changeit")
   dw_orauser = f"dwh_kern/{dw_orapwd}@DWHP1"
   ziel_verzeichnis = os.path.join(dw_dir_export, "rechnung", "ausgang")

5. # Step 5: Determine the Stichtag
   if args.stichtag:
       l_stichtag = args.stichtag
   else:
       try:
           l_stichtag = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
       except Exception:
           l_stichtag = datetime.now().strftime("%Y%m%d")

6. # Step 6: Emit start message and build output filename
   print(f"Starte Export Rechnungsdaten fuer Stichtag {l_stichtag}")
   export_datei = os.path.join(ziel_verzeichnis, f"rechnung_export_{l_stichtag}.dat")

7. # Step 7: Change to SQL working directory
   os.chdir(os.path.join(dw_dir_root, "exporter", "rechnung", "sql"))

8. # Step 8: Execute the Oracle SQL*Plus export
   # REVIEW-STRUCT: launcher sqlplus invoked — internal behaviour not available in this extraction;
   # confirm logging, error propagation, and credential handling before finalizing the conversion
   subprocess.run(
       ["sqlplus", "-s", dw_orauser, "@d_exp_rechnung_taeglich.sql", l_stichtag],
       check=True,
       stdout=open(export_datei, "w"),
   )

9. # Step 9: Count exported rows
   with open(export_datei, "r", encoding="utf-8") as f:
       l_anzahl = sum(1 for _ in f)

10. # Step 10: Emit count and warn on empty export
    print(f"Anzahl exportierter Rechnungssaetze: {l_anzahl}")
    if l_anzahl == 0:
        f_alis_msgerr("W", f"Keine Rechnungsdaten fuer Stichtag {l_stichtag} exportiert")

11. # Step 11: Emit completion message
    print("Export Rechnungsdaten ohne erkennbare Fehler beendet")

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.py` | KornShell wrapper script converted to Python to parse arguments, handle date defaulting, count rows, write logs, and trigger the export of invoice data to Google Cloud Storage (GCS). |

### Execution Order
The target orchestration in Cloud Composer (Airflow) must preserve the execution sequence established in the legacy dependency graph:
1. **DAG Orchestration Level**: Triggers the workflow sequence (originally initiated by `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` and `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml`).
2. **Execution Task**: Runs the Python script `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.py` (which replaces `bin/r_exp_rechnung_taeglich.ksh`).
3. **Data Extraction Task**: The Python script executes the extraction query logic defined in `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` against BigQuery, writing directly to Google Cloud Storage.

### Lineage
- **Upstream Producer**: Oracle table `T_RECHNUNG` (migrates to BigQuery table `BQ_DATASET.T_RECHNUNG`).
- **Downstream Consumer**: Flat file output `rechnung_export_${l_Stichtag}.dat` (migrates to `gs://GCS_BUCKET/rechnung/ausgang/rechnung_export_${l_Stichtag}.dat` in GCS).

### External System Replacements
- **Oracle Database / SQL\*Plus** $\rightarrow$ **Google Cloud BigQuery** (accessed natively via Python BigQuery API client or Airflow BigQuery-to-GCS Operators).
- **Local Unix Directory** (`$HOME/aktuell/export/rechnung/ausgang`) $\rightarrow$ **Google Cloud Storage (GCS) Bucket** (`gs://GCS_BUCKET/rechnung/ausgang/`).

### Cross-File Dependencies
- The Python script `r_exp_rechnung_taeglich.py` executes the SQL query logic originally contained in the companion file `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` (which belongs to a different migration design group).
- The BigQuery schema for table `T_RECHNUNG` must exist and match the query's schema requirements.

### Target File Plan
- **Target File**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.py`
  - **Language**: Python
  - **Source File**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh`
  - **Purpose**: Replaces the shell script wrapper. It parses arguments (optional reporting date `-s`), computes yesterday's date as a fallback if omitted, runs the BigQuery-to-GCS export task using the query from `d_exp_rechnung_taeglich.sql`, counts the exported lines in GCS, and prints the exact operational messages.
  - **Output/Print Literal Rule Adherence**: The following original logging statements must be output character-for-character in German:
    * `Starte Export Rechnungsdaten fuer Stichtag {l_stichtag}`
    * `Anzahl exportierter Rechnungssaetze: {l_anzahl}`
    * `[W] {timestamp} Keine Rechnungsdaten fuer Stichtag {l_stichtag} exportiert` (emitted to stderr)
    * `Export Rechnungsdaten ohne erkennbare Fehler beendet`

### Environment-Specific Values

| Legacy Variable/Value | Target Concept | Classification | Sourcing Mechanism |
| :--- | :--- | :--- | :--- |
| Oracle Connection `DWHP1` / `dwh_kern` | `GCP_PROJECT`, `BQ_DATASET` | GLOBAL | Sourced at runtime via `os.environ.get("GCP_PROJECT")` and `os.environ.get("BQ_DATASET")` or via Airflow Connection settings. |
| `${DWH_ORA_PWD:-changeit}` | Database Credentials | GLOBAL | Handled natively by GCP service account IAM roles; no passwords needed in the Python code. |
| `$HOME/aktuell/export` | `GCS_BUCKET` | GLOBAL | Sourced at runtime via `os.environ.get("GCS_BUCKET")` or Airflow Variables. |
| `rechnung/ausgang` | Target Folder Path | JOB-SPECIFIC | Maintained as a constant path suffix within the target script configuration. |
| `d_exp_rechnung_taeglich.sql` | SQL Query Script | JOB-SPECIFIC | Mapped to a local file path or inline template reference in BigQuery. |

### Risks and Manual Steps
- **Unmigrated Orchestration Elements**: The upstream UC4 Jobplan `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` and Job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` are not part of this design pass and are not yet migrated. Complete operational integration of this Python script depends on their migration to Airflow DAGs.
- **External SQL Script Dependency**: The Python script relies on `d_exp_rechnung_taeglich.sql` to execute the actual database query. This SQL file is not in scope for this design pass and must be converted to BigQuery SQL format separately.
- **Export Formatting Differences**: SQL\*Plus output format options (e.g. column separators, null formatting, header suppression) must be mapped carefully in BigQuery GCS export configuration to ensure that downstream consumers receive pipe-separated data structured exactly as before.

---

=== FILE: DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql ===
whenever sqlerror exit failure

DEFINE p_Stichtag='&1'

set pagesize 0
set linesize 400
set feedback off
set heading off
set colsep '|'

select
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  r.RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
from DWH_KERN.T_RECHNUNG r
where r.RECHNUNGSDATUM = to_date('&p_Stichtag','YYYYMMDD')
order by r.RECHNUNGSNUMMER;

exit;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

### 1.1 Type of Oracle SQL Object
- Multi-statement SQL*Plus script containing:
  - SQL*Plus session directives
  - A standalone SELECT query
  - Exit command

### 1.2 Business Logic and Purpose
- Extract invoice records from `DWH_KERN.T_RECHNUNG` for a single business date supplied as an input parameter (`p_Stichtag`).
- Return invoice attributes ordered by invoice number.
- Intended for daily invoice reporting/export.

### 1.3 Entities Referenced
| Entity | Type | Alias | Columns / Attributes | Inferred Oracle Type |
|---|---|---:|---|---|
| `DWH_KERN.T_RECHNUNG` | Table | `r` | `RECHNUNGSNUMMER` | Likely `VARCHAR2` or `NUMBER` |
|  |  |  | `VERTRAG` | Likely `VARCHAR2` |
|  |  |  | `KUNDE` | Likely `VARCHAR2` or `NUMBER` |
|  |  |  | `TARIF` | Likely `VARCHAR2` |
|  |  |  | `ABRECHNUNGSZEITRAUM` | Likely `DATE` or `VARCHAR2` |
|  |  |  | `RECHNUNGSBETRAG` | Likely `NUMBER(p,s)` |
|  |  |  | `WAEHRUNG` | Likely `CHAR(3)` / `VARCHAR2(3)` |
|  |  |  | `RECHNUNGSDATUM` | Oracle `DATE` |

| Script Variable | Purpose | Type |
|---|---|---|
| `p_Stichtag` | Input date parameter in `YYYYMMDD` format | String |

No sequences, synonyms, database links, directory objects, or external files detected.

---

### 2.1 Data Type Conversions
| Oracle Type / Construct | BigQuery Type / Equivalent | Notes |
|---|---|---|
| Oracle `DATE` (`RECHNUNGSDATUM`) | `DATE` or `DATETIME` depending on source semantics | In this script the value is compared to a date-only literal parsed from `YYYYMMDD`, so `DATE` is the safest target for the comparison expression. |
| `TO_DATE('&p_Stichtag','YYYYMMDD')` | `PARSE_DATE('%Y%m%d', @p_Stichtag)` | Oracle string-to-date conversion must be rewritten with explicit BigQuery parsing. |
| SQL*Plus substitution variable `&1` / `DEFINE` | Query parameter / script parameter | Not a SQL construct in BigQuery; must be supplied by the caller or wrapper. |

### 2.2 Implicit and Explicit Type Casting
- `r.RECHNUNGSDATUM = to_date('&p_Stichtag','YYYYMMDD')`
  - Oracle implicitly compares `DATE` to `DATE`.
  - BigQuery equivalent must explicitly parse the input string to `DATE`.
- No other implicit casts detected.

### 2.3 NULL Handling and Conditional Functions
- None detected.

### 2.4 String Functions
- None detected.

### 2.5 Date and Timestamp Functions
| Oracle Function | BigQuery Equivalent | Validation |
|---|---|---|
| `TO_DATE(str, fmt)` | `PARSE_DATE('%Y%m%d', @p_Stichtag)` | Syntactically valid and semantically equivalent for date-only input. |
| `ORDER BY r.RECHNUNGSNUMMER` | `ORDER BY r.RECHNUNGSNUMMER` | Compatible; no conversion needed. |

### 2.6 Numeric and Aggregate Functions
- None detected.

### 2.7 Analytical and Window Functions
- None detected.

### 2.8 Set and Join Operations
- None detected.

### 2.9 Row Limiting and Sampling
- None detected.

### 2.10 Sequences
- None detected.

### 2.11 MERGE Statements
- None detected.

### 2.12 INSERT / UPDATE / DELETE
- None detected.

### 2.13 DDL Constructs
- None detected.

### 2.14 PL/SQL
- None detected.

### 2.15 Unresolvable or Advisory Items
| Item | Status | Reason |
|---|---|---|
| SQL*Plus directives (`whenever sqlerror`, `DEFINE`, `set`, `exit`) | Advisory / stripped | Not supported as-is in BigQuery SQL; must be handled by orchestration or wrapper. |
| `&1` positional substitution | Manual input handling required | BigQuery SQL does not support SQL*Plus positional substitution variables directly. |

---

### 2.16 MIGRATION DECISION MATRIX
| Statement / Construct | Target | Rejected Alternatives | Evidence | Exact Reason |
|---|---|---|---|---|
| SQL*Plus session directives | Manual intervention / wrapper | Direct BigQuery SQL, UDF | Non-SQL execution controls | BigQuery does not implement SQL*Plus session commands. |
| `DEFINE p_Stichtag='&1'` | Python wrapper or caller-supplied parameter | Direct SQL, UDF | Positional substitution variable | BigQuery requires external parameterization. |
| `TO_DATE('&p_Stichtag','YYYYMMDD')` | Direct BigQuery Standard SQL | UDF | Deterministic scalar conversion | `PARSE_DATE('%Y%m%d', @p_Stichtag)` is a direct equivalent. |
| `SELECT ... FROM DWH_KERN.T_RECHNUNG ... WHERE ... ORDER BY ...` | Direct BigQuery Standard SQL | UDF, Python wrapper | Pure relational query | No procedural or external behavior required. |
| `whenever sqlerror exit failure` / `exit` | Manual intervention / orchestration | Direct SQL, UDF | Execution flow control | Must be handled by job runner, not SQL. |

---

### 2.17 REQUIRED ARTIFACTS
- BigQuery Standard SQL query
- External caller parameterization for `p_Stichtag`
- No UDF required
- No Python wrapper required for the query itself

If SQL*Plus failure handling must be preserved, the job orchestrator must implement:
- parameter injection
- error capture
- non-zero exit propagation

---

### 2.18 DATA TYPE COMPATIBILITY TABLE
| Oracle Source Type | BigQuery Target Type | Conversion Rule | Warning |
|---|---|---|---|
| `DATE` | `DATE` | Direct comparison after parsing input as `DATE` | If source column contains time semantics, truncation may be needed; not indicated here. |
| `VARCHAR2` | `STRING` | Direct mapping | None |
| `CHAR` | `STRING` | Direct mapping | Fixed-length padding semantics may differ |
| `NUMBER(p,s)` | `INT64` / `NUMERIC` / `BIGNUMERIC` / `FLOAT64` | Choose based on precision/scale | Not enough metadata to determine exact target for non-date columns |
| `RECHNUNGSBETRAG` likely `NUMBER(p,s)` | `NUMERIC` likely | Preserve decimal precision | Exact precision/scale not provided |
| `RECHNUNGSNUMMER` likely `VARCHAR2` or `NUMBER` | `STRING` or `INT64` | Preserve sort semantics | Type not confirmed from script alone |

---

### 2.19 DESIGN REVIEW SUMMARY
| Category | Summary |
|---|---|
| Patterns/objects found | SQL*Plus script, single SELECT, date filter, ORDER BY |
| Unsupported functions | SQL*Plus directives, positional substitution variable handling |
| UDF required | No |
| Python required | No, unless wrapper is needed for SQL*Plus-style parameterization/error handling |
| Direct dependencies | `DWH_KERN.T_RECHNUNG` |
| Assumptions | `RECHNUNGSDATUM` is date-only for filtering; `p_Stichtag` is supplied as `YYYYMMDD` |
| Warnings | SQL*Plus control flow must be moved outside SQL; source column types other than `RECHNUNGSDATUM` are inferred |
| Manual-intervention items | Parameter passing, error handling, execution wrapper/orchestration |
| Ready for human approval | Yes, for the SQL translation; orchestration handling remains external |

---

### 2.20 PACKAGE ANALYSIS
- Not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

---

### 2.21 ORACLE FUNCTION ANALYSIS TABLE
| Oracle Function/Construct | Supported in BigQuery — Direct | BigQuery Equivalent / Alternative |
|---|---|---|
| `whenever sqlerror exit failure` | Unsupported | none — manual intervention |
| `DEFINE p_Stichtag='&1'` | Unsupported | none — manual intervention |
| `set pagesize 0` | Unsupported | none — manual intervention |
| `set linesize 400` | Unsupported | none — manual intervention |
| `set feedback off` | Unsupported | none — manual intervention |
| `set heading off` | Unsupported | none — manual intervention |
| `set colsep '|'` | Unsupported | none — manual intervention |
| `TO_DATE('&p_Stichtag','YYYYMMDD')` | Direct-with-rewrite | `PARSE_DATE('%Y%m%d', @p_Stichtag)` |
| `ORDER BY r.RECHNUNGSNUMMER` | Direct | `ORDER BY r.RECHNUNGSNUMMER` |
| `exit` | Unsupported | none — manual intervention |

---

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

### BLOCK 1 — QUERY TRANSLATION

```sql
-- Input parameter expected from caller:
-- p_Stichtag as STRING in YYYYMMDD format

SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  r.RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM DWH_KERN.T_RECHNUNG AS r
WHERE r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @p_Stichtag)  -- converted from TO_DATE('&p_Stichtag','YYYYMMDD')
ORDER BY r.RECHNUNGSNUMMER;
```

### FLAGGED ITEMS FOR HUMAN REVIEW
- SQL*Plus execution directives must be handled outside BigQuery:
  - `whenever sqlerror exit failure`
  - `DEFINE p_Stichtag='&1'`
  - `set pagesize 0`
  - `set linesize 400`
  - `set feedback off`
  - `set heading off`
  - `set colsep '|'`
  - `exit`
- Caller must supply `@p_Stichtag` as a BigQuery query parameter in `YYYYMMDD` format.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sqlx` | Migrates the daily Oracle SQL extraction logic into a Dataform SQLX model inside the mirrored folder structure. |

### Execution Order
The execution sequence defined in the pre-collected context must be preserved in the target Airflow (Cloud Composer) DAG orchestration:
1. **Upstream Orchestrator**: `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` (UC4 Jobplan) maps to the master Airflow DAG orchestration.
2. **Upstream Job Task**: `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` (UC4 Job) maps to the primary task execution sequence.
3. **Execution Script**: `r_exp_rechnung_taeglich.ksh` (Unix shell script wrapper) maps to an Airflow operator executing a Python task or running a Dataform pipeline.
4. **Target Script**: `d_exp_rechnung_taeglich.sql` (Oracle SQL*Plus script) maps to the final step of executing the BigQuery query defined in `d_exp_rechnung_taeglich.sqlx`.

### Lineage
* **Upstream Producers (Reads Table)**:
  * Reads from Oracle table `DWH_KERN.T_RECHNUNG`. This is mapped to the target BigQuery table `T_RECHNUNG` in the core dataset (`BQ_DATASET_DWH_KERN`).
* **Downstream Consumers**:
  * The pipe-separated spool file output produced by this script is logically consumed by the downstream steps in `r_exp_rechnung_taeglich.ksh` (which is part of a separate orchestration design pass).

### External System Replacements
* **SQL*Plus Directives to Cloud Native Export**:
  * Legacy SQL*Plus parameters (`set pagesize 0`, `set linesize 400`, `set feedback off`, `set heading off`, `set colsep '|'`) are used to spool a pipe-separated text file.
  * In the target architecture, formatting and database queries are decoupled. This is replaced by a BigQuery export job (e.g., via Airflow `BigQueryToGCSOperator` or standard export functions) configured to output pipe-delimited CSVs to a Google Cloud Storage (GCS) bucket.

### Cross-File Dependencies
* **Core Table**:
  * `T_RECHNUNG` is a shared DWH core table that must be fully populated and current before this export step is executed.
* **Orchestration Chain**:
  * The execution of `d_exp_rechnung_taeglich.sqlx` depends on the environment variable injection (`p_Stichtag`) propagated by the parent shell script `r_exp_rechnung_taeglich.ksh` (or its migrated target).

### Target File Plan
* **Target File Path**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sqlx`
* **Language**: SQLX (Dataform)
* **Source File**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql`
* **Description**: A Dataform SQLX model executing the translated standard BigQuery SQL select query, receiving runtime parameters, and processing the target query block.

### Environment-Specific Values
* **GCP_PROJECT (GLOBAL)**:
  * *Legacy Mapping*: Implicitly defined by database connection strings.
  * *Target Resolution*: Sourced at runtime via Google Cloud Composer environment configuration.
* **BQ_DATASET_DWH_KERN (GLOBAL)**:
  * *Legacy Mapping*: `DWH_KERN` schema qualifier.
  * *Target Resolution*: Resolved dynamically at runtime via Dataform's database/schema reference functions (e.g., `${ref("T_RECHNUNG")}`) or through query variable substitution in standard SQL execution.
* **p_Stichtag (JOB-SPECIFIC)**:
  * *Legacy Mapping*: Positional script variable `&1` (aliased as `p_Stichtag`).
  * *Target Resolution*: Passed as a query parameter (`@p_Stichtag`) by the orchestration DAG at execution time, maintaining the `YYYYMMDD` format and parsed using `PARSE_DATE('%Y%m%d', @p_Stichtag)`.

### Risks and Manual Steps
* **Export Formatting & Spooling**: BigQuery SQL cannot directly manage local shell spool parameters. Standardizing file properties (removing headers, specifying columns, setting pipe delimiters) requires manual design configuration on the Google Cloud Storage export step rather than within the SQL logic itself.
* **Exit and Error State Propagations**: The Oracle command `whenever sqlerror exit failure` must be manually addressed by configuring the calling Airflow task to automatically fail and notify administrators if the underlying BigQuery query fails.