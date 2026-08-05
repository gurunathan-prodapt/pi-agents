=== OBJECT: DW.DWH_VVTN_IAR_BGF_GUTSCHR (JOBS_UNIX) ===
active=1
title=Transform Gutschrift files to one file CSV
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=unrecognized
launcher_details={'raw_command': ': set &Month_ID = &LASTMONTH_YYYYMM'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='VVTN_IAR_BGF_GUTSCHR'
: set &Month_ID = &LASTMONTH_YYYYMM
:print Lastmonth is &Month_ID
. $HOME/.dw_init

$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift

:inc DW.LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
This migration design document covers the transition of a standalone UC4 Unix job (`DW.DWH_VVTN_IAR_BGF_GUTSCHR`) to Apache Airflow. This job is responsible for transforming "Gutschrift" (credit note) files into a unified CSV format by executing a local Unix script (`r_vvtn_iar_bgf_gutschrift`). Since no enclosing workflow (JOBP) or schedule (EVNT_TIME) was supplied in this extraction, this process is treated as an independently executable, externally triggered Airflow DAG.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_VVTN_IAR_BGF_GUTSCHR` | JOBS_UNIX | Active (1) | Transform Gutschrift files to one file CSV |

## 3. Scheduling
- **Schedule**: `None`
- **Trigger Source**: No calendar-based schedule, JOBP parent, or trigger script (SCRI) was supplied in this extraction. This workflow is classified as **externally triggered** (source unknown from this extraction alone).

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_vvtn_iar_bgf_gutschr` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_vvtn_iar_bgf_gutschr_task` | `DW.DWH_VVTN_IAR_BGF_GUTSCHR` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | N/A | None | #REVIEW-STRUCT: launcher command not recognised — confirm target operator/script manually. Script executes: `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`. Host: `|DWHDWH1P|HOST`. |

## 6. Task Dependency Map
```python
dw_dwh_vvtn_iar_bgf_gutschr_task
```
*(Single-task DAG; no internal dependencies are defined in this extraction)*

## 7. Sync / Concurrency Analysis
No `sync_rows` or mutual exclusion configurations were defined for this object. Standard execution concurrency rules apply (`max_active_runs=1`).

## 8. Error Handling and Retry Strategy
- No custom postcondition actions or failure notifications were provided.
- Default retry configuration of 1 retry with a 5-minute delay is implemented.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&Month_ID` | `&LASTMONTH_YYYYMM` | Can be resolved dynamically using Airflow execution macros, e.g., `{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y%m') }}` |
| `DWHDWH1P` (Host) | Target Environment Variable | Airflow connection parameter or SSH target |

## 10. Developer Notes
*   #REVIEW-STRUCT: The launcher type for `DW.DWH_VVTN_IAR_BGF_GUTSCHR` is unrecognized. The raw script execution logic references a local command: `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` under host `|DWHDWH1P|HOST`. The developer must confirm whether to map this task to an `SSHOperator` (to run on the designated host) or convert the target bash execution into a containerized operator (e.g., `GKEStartPodOperator`).
*   The variable `&LASTMONTH_YYYYMM` should be mapped to the logical execution date's previous month to maintain idempotent behavior across runs.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP/Environment Configuration ────────────────────────
# Placeholder for connection IDs and remote environment variables
SSH_CONN_ID = "ssh_dwh_host"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks required as per the extraction bundle

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_vvtn_iar_bgf_gutschr',
    default_args=DEFAULT_ARGS,
    description='Transform Gutschrift files to one file CSV',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'jobs_unix'],
) as dag:

    # ── Task: dw_dwh_vvtn_iar_bgf_gutschr_task ───────────
    # #REVIEW-STRUCT: launcher command not recognised. 
    # Action Required: Confirm target operator/script manually.
    # Original host: |DWHDWH1P|HOST
    # Original script command: 
    # . $HOME/.dw_init
    # $HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
    # Environment variable extraction requirement: 
    # Month_ID = Last month in YYYYMM format (Airflow Equivalent: {{ (data_interval_start - macros.timedelta(days=28)).strftime('%Y%m') }})
    dw_dwh_vvtn_iar_bgf_gutschr_task = EmptyOperator(
        task_id='dw_dwh_vvtn_iar_bgf_gutschr_task',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone task; no workflow dependencies defined.
    dw_dwh_vvtn_iar_bgf_gutschr_task
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml` | `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py` | Migrates UC4 UNIX job XML definition to a Python Airflow DAG for Cloud Composer orchestration. |

### Execution Order
The execution sequence defined in the legacy dependency graph must be preserved in the target environment:
1. **Orchestration Entry Point**: `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml` (maps to the initialization of the target DAG `dw_dwh_vvtn_iar_bgf_gutschr`).
2. **Main Transformation Script**: `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` (runs next; maps to a downstream task/operator inside the target DAG).
3. **Footer Processing**: `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gut_foot.awk` (invoked and executed during step 2).
4. **Core AWK Translation**: `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk` (invoked and executed during step 2).

### Schedule & Variables — Must Be Retained
- **Trigger/Schedule**: The legacy job is externally triggered or invoked via parent scheduler sequence. The target DAG should be defined with `schedule_interval=None` to preserve this behavior, allowing it to be triggered via Airflow DAG runs or upstream DAG sensors.
- **Scheduler-Set Variables**:
  - `DWH_JOB_KENNUNG` = `'VVTN_IAR_BGF_GUTSCHR'`
    - *Target Delivery*: Passed as a DAG parameter or task-level environment variable.
  - `Month_ID` = `&LASTMONTH_YYYYMM`
    - *Target Delivery*: Resolved dynamically using the Airflow native context macro: `{{ (data_interval_start.add(months=-1)).strftime('%Y%m') }}`.

### Lineage & Cross-File Dependencies
- **Upstream / Shared Includes (Legacy)**:
  - `DW.HOLE_PFAD` (Human confirmed: NO SOURCE NEEDED) — Replaced by native Airflow variables or Google Cloud Storage environment configurations.
  - `DW.LESE_LOG` (Human confirmed: NO SOURCE NEEDED) — Replaced by standard GCP / Cloud Composer task logging.
  - `.DW_INIT` (Human confirmed: NO SOURCE NEEDED) — Replaced by the native runtime environment initialization.
- **Downstream Invocation**:
  - This job invokes `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`. This executable belongs to a separate design pass but is represented as a task dependency in the DAG structure.

### External System Replacements
- The execution environment changes from the legacy host `|DWHDWH1P|HOST` to standard Google Cloud Composer (Airflow) worker instances.
- Workspace file reads/writes under `$HOME/aktuell/vorverarbeitung/tn/bin/` are redirected to a Google Cloud Storage bucket (`GCS_BUCKET`).

### Target File Plan
- **Target File**: `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py`
  - *Language*: Python (Apache Airflow DAG)
  - *Source File*: `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml`

### Environment-Specific Values

| Legacy Value | Category | Target Variable | Realization / Resolution |
| :--- | :--- | :--- | :--- |
| `$HOME` | GLOBAL | `GCS_BUCKET` | Sourced from GCS config bucket at runtime: `os.environ.get("GCS_BUCKET")`. |
| `|DWHDWH1P|HOST` | GLOBAL | `GCP_PROJECT` | Executed directly within the Cloud Composer target environment. |
| `LASTMONTH_YYYYMM` | GLOBAL | Native Airflow Macro | Resolved at runtime via `{{ (data_interval_start.add(months=-1)).strftime('%Y%m') }}`. |
| `DWH_JOB_KENNUNG` | JOB-SPECIFIC | `params['DWH_JOB_KENNUNG']` | Inline parameter hardcoded to `'VVTN_IAR_BGF_GUTSCHR'`. |
| `Month_ID` | JOB-SPECIFIC | `params['Month_ID']` | Resolved dynamically in the calling task using the macro value. |

### Risks & Manual Steps
- **Interface Dependency**: The wrapper script `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` is invoked by this job but is not in our current design scope. The Airflow task execution relies on the availability of its translated Python/BigQuery counterpart from a separate design group.
- **Literal Logging Preservation**: The legacy log statement `:print Lastmonth is &Month_ID` must be preserved. Under the **OUTPUT/PRINT LITERAL RULE**, it must log/print exactly: `"Lastmonth is {month_id}"` in English/German as written in the source script, maintaining strict character-for-character compliance:
  ```python
  print(f"Lastmonth is {month_id}")
  ```

---

=== FILE: isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk ===
#  Zweck:
#   Preprocessing of input data: add the footer
#
#  Prerequisites:
#   Pass the file name via the FLNM Parameter 
#
# Historie  :
#   20.4.0   19.10.2020 ; Miroslav Mihal ; 
BEGIN {
   FS = ";"
   OFS = ";"
}
{
print "X;Datei " FLNM ";" $1 ";" $2 ";File for BGF IAR Gutschrift;" $1
}    
END {
    } 


TARGET: PYTHON

DECISION RATIONALE:
The AWK script performs a per-record transformation on an input file, prepending and appending specific text to each line. Crucially, it relies on an external parameter `FLNM` (representing the input file name) passed into the script to construct the output records (e.g., `"Datei " FLNM`). According to the migration rules, deriving values from out-of-band metadata not present in the record data itself (such as deriving values from the filename or external environment variables) is a BQSQL-disqualifying condition. Because the business logic requires injecting this external filename variable into every row, a Python script is the appropriate target as it can easily ingest command-line arguments or environment variables and process the file stream. The conversion confidence is High.

FEATURE INVENTORY:
- BEGIN block: Expressible in BigQuery (as metadata settings, though BQ SQL does not have an explicit BEGIN block for declarative queries).
- END block: Expressible in BigQuery (as an empty/no-op block).
- Pattern-action rules: Expressible in BigQuery (the match-all block translates to a standard SELECT projection).
- Field references ($1, $2): Expressible in BigQuery (via SPLIT or column selection).
- FS/OFS (semicolon separator): Expressible in BigQuery (by defining the field delimiter in the table definition).
- NR/FNR/NF/FILENAME: Not present in the script.
- Variables (FLNM): Not expressible in pure declarative BigQuery SQL as it is an out-of-band parameter passed during execution.
- associative arrays: Not present in the script.
- user-defined functions: Not present in the script.
- getline: Not present in the script.
- print/printf: Expressible in BigQuery (via SELECT with CONCAT).
- redirection: Not present in the script.
- pipes: Not present in the script.
- loops: Not present in the script.
- conditions: Not present in the script.
- next/nextfile/exit: Not present in the script.
- command-line -v variables: Not expressible in pure BigQuery SQL (requires external parameter binding which is not standard SQL-compliant).

Here is the explainable Design Document and Python-oriented pseudocode for migrating the AWK script to Python 3.

---

### 1. MIGRATION DECISION SUMMARY

* **Target Language:** Python 3
* **Rationale for Ruling Out BigQuery SQL:** 
  The AWK script performs a record-by-record transformation that injects an external parameter, `FLNM` (representing the input filename), directly into the output records. In standard BigQuery SQL, accessing external execution-time environment variables or metadata (like the source file name currently being processed in a stream) is not supported natively in a stateless projection. This makes a procedural, streaming Python script the ideal target, as it can easily parse command-line arguments and run within containerized or local OS orchestration.
* **Conversion Confidence:** High.
* **Human Review Required:** No (the logic is simple, but a standard automated test is recommended).

---

### 2. PROGRAM OVERVIEW

* **Purpose:** This script preprocesses an input data file by taking each line and prepending/appending structured data, including the name of the file passed via the `FLNM` parameter.
* **Input Stream/Files:** Standard input or text files passed as arguments.
* **Output Stream:** Standard output (`stdout`).
* **Command-line Variables:** `FLNM` (passed to AWK as `-v FLNM=...`).
* **Expected Record Format:** Semicolon-separated lines. The script references `$1` (first field) and `$2` (second field).
* **Observable Side Effects:** Outputs formatted text records to standard output.

---

### 3. AWK FEATURE INVENTORY

* **`BEGIN` block:** Sets `FS = ";"` and `OFS = ";"`. 
  * *Python equivalent:* Define a variable for the separator `";"` or pass it directly to `split()`. Note that since the print statement relies on explicit string concatenation rather than comma-separated fields, `OFS` is set but not implicitly used.
* **`END` block:** Declared empty.
  * *Python equivalent:* None.
* **Pattern-action rules (`{ ... }`):** Executes on every line.
  * *Python equivalent:* Stream records via a `for` loop over `sys.stdin` or file arguments.
* **Field references (`$1`, `$2`):** Extracts fields 1 and 2.
  * *Python equivalent:* Split the input line by `";"` and access indices `0` and `1`. To prevent crash-on-empty/malformed lines, safety guards must be used to provide empty-string fallbacks.
* **String Concatenation:** `print "X;Datei " FLNM ";" $1 ";" $2 ";File for BGF IAR Gutschrift;" $1`
  * *Python equivalent:* Construct a formatted f-string: `f"X;Datei {flnm};{f1};{f2};File for BGF IAR Gutschrift;{f1}\n"`.

---

### 4. PYTHON IMPLEMENTATION STRATEGY

* **Libraries:** `sys`, `argparse` (to handle standard streaming inputs and cleanly accept the `FLNM` variable).
* **Streaming Loop:** Iterate over lines from `sys.stdin` or files passed as arguments.
* **1-Based to 0-Based Conversion:** Map `$1` to index `0` and `$2` to index `1`.
* **Field Splitting Safety:** If a record contains fewer than two fields, Python's split list will not contain indices `0` or `1`. Implement a safe indexing helper that returns `""` if the index is out of bounds, mimicking AWK's behavior.

---

### 5. INPUTS, OUTPUTS, AND DEPENDENCIES

* **Inputs:** 
  * Command-line argument: `--flnm` (required string).
  * Data source: Positional file arguments, or standard input (`stdin`) if no files are provided.
* **Outputs:** 
  * Writes directly to `sys.stdout`.
* **Dependencies:** Only standard Python libraries (`sys`, `argparse`).

---

### 6. UNSUPPORTED FEATURES, WARNINGS, AND ASSUMPTIONS

* **Missing `FLNM` Value:** AWK treats uninitialized variables as empty strings `""`. In Python, we will set a default value of `""` for `--flnm` but allow it to be required if strict adherence to downstream business logic is desired.
* **Safe Field Extraction:** If the input line contains zero or one semicolon, splitting it will yield an array of length < 2. AWK tolerates this and evaluates missing fields as empty strings. The Python code must replicate this tolerance.

---

### 7. MANUAL REVIEW ITEMS

* **Orchestrator Integration:** Ensure that the shell command calling this script replaces `-v FLNM="filename"` with `--flnm "filename"`.

---

### 8. NUMBERED PSEUDOCODE

```python
# 1. Import required standard libraries
import sys
import argparse

# 2. Define main execution block
def main():
    # 3. Configure argument parser to replicate AWK's -v FLNM parameter and file handling
    parser = argparse.ArgumentParser(description="Add footer/metadata info to input data lines.")
    parser.add_argument('--flnm', type=str, default="", help="File name to inject into output records")
    parser.add_argument('files', nargs='*', help="Input file paths (reads from stdin if none provided)")
    
    args = parser.parse_args()
    flnm = args.flnm
    
    # 4. Helper function to safely extract fields to prevent index errors
    def get_field(fields_list, index_1_based):
        index_0_based = index_1_based - 1
        if 0 <= index_0_based < len(fields_list):
            return fields_list[index_0_based]
        return ""

    # 5. Determine input sources (either files or standard input)
    input_sources = args.files if args.files else ['-']

    # 6. Stream through input lines
    for source in input_sources:
        try:
            # Handle stdin vs actual file path
            if source == '-':
                file_stream = sys.stdin
            else:
                file_stream = open(source, 'r', encoding='utf-8')
                
            for line in file_stream:
                # 7. Strip trailing newline character
                cleaned_line = line.rstrip('\r\n')
                
                # 8. Split record by semicolon (FS = ";")
                fields = cleaned_line.split(';')
                
                # 9. Extract fields mimicking AWK's $1 and $2 safely
                f1 = get_field(fields, 1)
                f2 = get_field(fields, 2)
                
                # 10. Reconstruct the output line using Python f-string
                # Matches AWK print: "X;Datei " FLNM ";" $1 ";" $2 ";File for BGF IAR Gutschrift;" $1
                output_line = f"X;Datei {flnm};{f1};{f2};File for BGF IAR Gutschrift;{f1}\n"
                
                # 11. Write output to stdout
                sys.stdout.write(output_line)
                
            if source != '-':
                file_stream.close()
                
        except FileNotFoundError:
            sys.stderr.write(f"Error: File not found: {source}\n")
            sys.exit(1)

# 12. Standard entry point guard
if __name__ == '__main__':
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk` | `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py` | Migrated to standard Python 3. Python is chosen because the business logic requires injecting the dynamic external file name (`FLNM`) into every row, which disqualifies standard stateless BigQuery SQL projections. |

---

### Execution Order

The execution sequence defined in the legacy dependency graph must be preserved in the target orchestration (e.g., Cloud Composer/Airflow DAG). The mapping of each step is as follows:

1. **Step 1:** `DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml`  
   * **Target:** Airflow DAG Orchestration (migrated separately as part of the UC4 XML migration pass).
2. **Step 2:** `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`  
   * **Target:** Bash/Python Task running the wrapper logic (migrated separately as part of the KSH migration pass).
3. **Step 3:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk`  
   * **Target:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py` (this target file, executed inside the DAG).
4. **Step 4:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk`  
   * **Target:** Target script/query for processing the primary Gutschrift logic (migrated separately as part of its own design pass).

---

### Schedule & Variables

The target Airflow DAG must maintain equivalent execution triggers and pass required metadata to the jobs:

* **Scheduler-Set Variables:**
  * `DWH_JOB_KENNUNG` (Value: `'VVTN_IAR_BGF_GUTSCHR'`): **JOB-SPECIFIC**. Managed as a constant parameter inside the Airflow DAG definition or passed via runtime variable parameters.
  * `Month_ID` (Value: `'&LASTMONTH_YYYYMM'`): **JOB-SPECIFIC**. This is a dynamic execution variable that must resolve to the previous month in `YYYYMM` format. In Airflow, this can be dynamically injected at runtime via Jinja template rendering:  
    `{{ (execution_date - macros.timedelta(days=15)).strftime('%Y%m') }}` (or equivalent macros evaluating the previous month relative to the run date).

---

### Lineage

* **Lineage Edges:** No direct table-level lineage edges were identified for this standalone AWK script. It operates purely as a streaming preprocessor (receiving input data streams and piping output to standard out) orchestrated by the parent wrapper script.

---

### Cross-File Dependencies

* **Upstream Wrapper Invocation:** This AWK script is executed by the shell script `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` (Step 2 of the execution order). The wrapper must supply the name of the file being processed to the script. In the target environment, the wrapper task or DAG operator must invoke the Python script with the `--flnm` parameter instead of AWK's `-v FLNM=` parameter.

---

### Target File Plan

* **Target File:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py`
  * **Language:** Python 3
  * **Source File:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk`

---

### Environment-Specific Values

All environment/runtime values are classified by role to align with GCP best practices:

* **JOB-SPECIFIC:**
  * `FLNM`: Represents the name of the current file being processed. Passed dynamically as a command-line parameter (e.g. `--flnm <filename>`) to the Python script at run time.
  * `DWH_JOB_KENNUNG`: Constant value `'VVTN_IAR_BGF_GUTSCHR'`. Passed as an environment variable or job metadata in the Airflow Task.
  * `Month_ID`: Resolved dynamically in the Airflow orchestration using Jinja templates and passed down to downstream operators.

---

### Risks & Manual Steps

* **Wrapper Integration Interface Change:** This design pass strictly covers the `k_vvtn_iar_bgf_gutsch_foot.awk` script. The calling script `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` is migrated independently. The developer must manually verify that the migrated wrapper script or DAG operator calls the Python version using `--flnm "<filename>"` instead of the legacy `-v FLNM="<filename>"` syntax.
* **Character Encoding Check:** The original AWK script processed streams without strict character set constraints. The Python target script opens files using `utf-8` encoding. If the source Gutschrift files contain legacy non-UTF character encodings (e.g., ISO-8859-1), the encoding parameter inside the file open statement of the Python target should be manually updated to match the input format.
* **German & English Print Literals:** The literal output strings constructed in the script, specifically `"X;Datei "` and `";File for BGF IAR Gutschrift;"`, must be preserved exactly as-is without translation or alteration, as downstream parser logic expects this exact structure.

---

=== FILE: isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk ===
#  Zweck:
#   Preprocessing of input data 
#
# Historie  :
#   20.2.0;  27.05.2020 ; Miroslav Mihal  ; Initial Version
# /view/mmihal_tmd2468_ux/vobs/dw_source/isdwh/vorverarbeitung/tn/awk
BEGIN {
   FS = ";"
   OFS = ";"
}
{
   if (NF == 25)
     {
     print "D;" $0;
     }
   else
       {
       print "Error: Incorrect nos of Fields "
       exit 2
       }
  }    
END {
    } 


TARGET: PYTHON

DECISION RATIONALE
The AWK script performs schema-validation and formatting on an incoming delimited file. It verifies that each record has exactly 25 fields (`NF == 25`), prepends "D;" to valid lines, and prints them. If any line fails this validation, it prints an error message and terminates immediately with a process-level exit code of `2` (`exit 2`). This explicit use of a non-zero exit status to fail the pipeline and signal an error to the orchestrating environment is a BQSQL-disqualifying feature. While BigQuery can validate field counts using SQL functions, it cannot natively raise a custom process-level exit code (like exit code 2) to command-line orchestrators. A Python script can perfectly replicate this behavior by streaming the file, validating field counts, and calling `sys.exit(2)` on failure.
Conversion Confidence: High.

FEATURE INVENTORY
- `BEGIN` block: Expressible in BQ SQL by defining the field delimiter (`;`) in the external table definition, though not as dynamic code.
- `END` block: Expressible in BQ SQL as a no-op (empty block).
- Pattern-action rules (unconditional `{ ... }`): Expressible in BQ SQL as a query projecting and transforming rows.
- Field references (`$0`): Expressible in BQ SQL as the raw text line or reconstructed columns.
- `FS` / `OFS` variables: Expressible in BQ SQL via external table definitions or manual string splitting and joining.
- `NF` variable: Expressible in BQ SQL using `ARRAY_LENGTH(SPLIT(row, ';'))`.
- `print` statement: Expressible in BQ SQL as the result set of a projection.
- `exit` statement (with non-zero code `exit 2`): NOT expressible in BQ SQL because SQL queries cannot return custom process-level exit codes to shell orchestrators.
- Conditions (`if / else`): Expressible in BQ SQL using `CASE` or `IF` statements.

Here is the transparent, explainable Design Document and numbered pseudocode for migrating the AWK script to Python 3.

---

### 1. MIGRATION DECISION SUMMARY

*   **Target Language:** Python 3
*   **Why BigQuery SQL was Ruled Out:**
    The upstream decision analysis correctly identified a critical constraint: this script serves as an active execution gatekeeper. It validates incoming file schemas by asserting that every line has exactly 25 fields (`NF == 25`). If any line fails this assertion, the script prints an error message and immediately halts processing with a process-level exit code of `2` (`exit 2`). 
    While BigQuery SQL can easily split strings and count elements, SQL is inherently designed for set-based execution. It cannot dynamically raise a custom process-level exit status code to the host OS or orchestration tool (e.g., Apache Airflow, Control-M) mid-query. 
    Python 3, however, can stream-read the data source, perform real-time record validation, and natively return `sys.exit(2)` immediately upon encountering an invalid record, preventing down-stream processing of corrupted data.
*   **Conversion Confidence:** High.
*   **Human Review:** Required to verify downstream handling of exit code `2` and confirm whether validation errors should be routed to `stderr` instead of `stdout`.

---

### 2. PROGRAM OVERVIEW

*   **Purpose:** Preprocess and validate incoming semicolon-delimited files. Valid lines are tagged with a `"D;"` prefix, representing "Data". Invalid files trigger an immediate pipeline failure.
*   **Input Streams:** Reads from standard input (`stdin`) or from files provided as command-line arguments.
*   **Output Streams:** 
    *   Valid processed records are written to standard output (`stdout`).
    *   Errors are printed to standard output (`stdout`) immediately before process termination (matching AWK's default print behavior).
*   **Command-line Variables:** None.
*   **Expected Record Format:** Semicolon-delimited (`FS = ";"`) text with exactly 25 fields per record.
*   **Observable Side Effects:** Terminates execution with exit status `2` on the first malformed record.

---

### 3. AWK FEATURE INVENTORY

*   **`BEGIN` block:** Sets field separator `FS` and output field separator `OFS` to `";"`.
    *   *Python Equivalent:* Define a string constant `DELIMITER = ';'` for splitting.
*   **`NF` (Number of Fields):** Checked for equality to 25.
    *   *Python Equivalent:* `len(line.rstrip('\r\n').split(';'))`.
*   **`$0` (Whole Record):** Printed with a `"D;"` prefix.
    *   *Python Equivalent:* Print `"D;"` concatenated with the raw line (after stripping trailing newlines to avoid double-spacing).
*   **`print` statement:**
    *   *Python Equivalent:* Standard `print()` or `sys.stdout.write()`.
*   **`exit 2`:** Immediately stops the program and returns exit code 2. Note that in AWK, `exit` jumps to the `END` block first. Since the `END` block is empty here, an immediate exit is semantically identical.
    *   *Python Equivalent:* `sys.exit(2)`.

---

### 4. PYTHON IMPLEMENTATION STRATEGY

*   **Standard Libraries:** `sys`, `fileinput`.
*   **Streaming Loop:** Use `fileinput.input()` to transparently read from either files passed as CLI arguments or `sys.stdin` if no files are provided. This preserves standard AWK stream/file consumption semantics.
*   **Record Parsing:**
    1.  Read raw line.
    2.  Strip trailing newline characters (`\r`, `\n`) to safely isolate the text content.
    3.  Split the stripped string by the delimiter (`;`).
    4.  Verify length of the resulting array is 25.
*   **Coercion & Semantics:** Semicolon split in Python matches AWK's behavior. For instance, `"a;b;"` splits into 3 items in both languages.
*   **Error Handling:** If validation fails, write `"Error: Incorrect nos of Fields "` to stdout (to preserve AWK's stdout target) and exit via `sys.exit(2)`.

---

### 5. INPUTS, OUTPUTS, AND DEPENDENCIES

*   **Inputs:** Delimited text via standard input stream or file arguments.
*   **Outputs:** Formatted dataset to `stdout`.
*   **Direct External Dependencies:** None.

---

### 6. UNSUPPORTED FEATURES, WARNINGS, AND ASSUMPTIONS

*   **# REVIEW:** In AWK, the error message `"Error: Incorrect nos of Fields "` is output to `stdout` because `print` defaults to standard output. In modern pipeline design, error messages should ideally be written to standard error (`sys.stderr`). The pseudocode maintains AWK's `stdout` behavior to preserve exact behavioral compatibility, but this should be reviewed.
*   **# REVIEW:** Suffix/Prefix Newlines: AWK's `$0` excludes the record separator. Python's `line.rstrip('\r\n')` safely removes trailing carriage returns and line feeds before we prepend `"D;"` and print.

---

### 7. MANUAL REVIEW ITEMS

1.  Confirm if the validation error message should be redirected to `sys.stderr` instead of `sys.stdout`.
2.  Ensure that the orchestrator monitoring this process is configured to handle exit status code `2` as a hard failure.

---

### 8. NUMBERED PSEUDOCODE

```python
# 1. Import necessary system modules
IMPORT sys
IMPORT fileinput

# 2. Define constants representing AWK configuration
CONSTANT FIELD_SEPARATOR = ";"
CONSTANT EXPECTED_FIELD_COUNT = 25

# 3. Main processing loop executing for each input record
TRY:
    FOR raw_line IN fileinput.input():
        # 3.1. Strip carriage return and line feed from the end of the line
        SET clean_line = raw_line.rstrip("\r\n")

        # 3.2. Split line by delimiter to calculate number of fields (NF)
        SET fields = clean_line.split(FIELD_SEPARATOR)
        SET field_count = length(fields)

        # 3.3. Validate field count constraint (equivalent to if (NF == 25))
        IF field_count EQUALS EXPECTED_FIELD_COUNT:
            # Replicate 'print "D;" $0'
            PRINT "D;" CONCATENATED WITH clean_line
        ELSE:
            # Replicate 'print "Error: Incorrect nos of Fields "'
            PRINT "Error: Incorrect nos of Fields " TO stdout
            
            # Replicate 'exit 2' - immediately terminate process
            sys.exit(2)

# 4. Handle standard pipe errors gracefully
CATCH KeyboardInterrupt or BrokenPipeError:
    sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk` | `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.py` | Transformed from AWK to Python 3 to maintain row-by-row field validation and process-level termination with exit status 2 on malformed input data. |

---

### Execution Order
The legacy orchestration order must be preserved in the target Cloud Composer DAG. The execution sequence maps as follows:

1. **Step 1: Orchestration Config** (`DWH_IAR_BGF_GUTSCHRIFT_JOB/DW.DWH_VVTN_IAR_BGF_GUTSCHR.xml`)  
   *Target Mapping:* Defined as the scheduling metadata of the Cloud Composer DAG (handled in a separate orchestration migration pass; out of scope for this file's target code).
2. **Step 2: Wrapper Shell Script** (`isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`)  
   *Target Mapping:* Migrated to a Python-based wrapper or standard Airflow operators calling the downstream scripts (handled in a separate KSH/wrapper migration pass; out of scope for this file's target code).
3. **Step 3: Preceding AWK Step** (`isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk`)  
   *Target Mapping:* Migrated to its own Python 3 utility at `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py` (handled in a separate AWK migration pass; out of scope for this file's target code).
4. **Step 4: Active AWK Processing** (`isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk`)  
   *Target Mapping:* Migrated to `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.py` (the active Python transformation logic produced by this design pass).

---

### Schedule & Variables
The target environment must dynamically feed the legacy variables into the processing context using Airflow's native mechanisms:

*   **`DWH_JOB_KENNUNG`** (`'VVTN_IAR_BGF_GUTSCHR'`)  
    *Classification:* JOB-SPECIFIC.  
    *Target Action:* Sourced as an environment variable or DAG parameter (`params`) in the Airflow task, and passed to the Python execution environment.
*   **`Month_ID`** (`'&LASTMONTH_YYYYMM'`)  
    *Classification:* JOB-SPECIFIC.  
    *Target Action:* Sourced dynamically at runtime using Airflow macro templating (e.g., `{{ macros.ds_format(macros.ds_add(ds, -30), "%Y-%m", "%Y%m") }}` or equivalent depending on the exact run-date calculation logic) and passed as an argument or environment variable to the execution task.

---

### Target File Plan

*   **Target File Path:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.py`
*   **Target Language:** Python 3
*   **Source File:** `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk`

---

### Risks and Manual Steps

1.  **Orchestrator Handling of Exit Status 2:**  
    The migrated Python script exits with a non-zero exit status (`2`) upon detecting any row that does not contain exactly 25 fields. The Airflow Task / Cloud Composer operator invoking this script must be configured to correctly capture this exit code as a hard task failure so that downstream DAG execution immediately halts, preventing the ingestion of malformed data.
2.  **Preservation of Print Output Target (`stdout`):**  
    In the legacy AWK script, the error message `"Error: Incorrect nos of Fields "` is output to standard output (`stdout`) because the AWK `print` statement defaults to standard output. While best practices dictate writing error streams to standard error (`stderr`), standard compatibility requires that the target Python script reproduces this output to standard output, unless a manual adjustment is confirmed by downstream operations team.
3.  **Literal Text Matching (Output/Print Rule):**  
    The exact original literal strings `"D;"` and `"Error: Incorrect nos of Fields "` must be emitted without any modification, translation, or structural rephrasing, preserving character-for-character behavioral parity.

---

### group 4/4 — DESIGN FAILED

ERROR: NO_MCP_TOOL — design cannot proceed for 'DW.DWH_VVTN_IAR_BGF_GUTSCHR' — no MCP tool is confirmed for this job's source pattern ('UNKNOWN'). Contact the platform team to add or confirm support for this source type before retrying.
