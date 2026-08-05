=== OBJECT: DW.EXTTEST_LEGACY_DWH (JOBS_UNIX) ===
active=1
title=legacy_ksh_dwh
login=DW.UNIX.ISXTST
host=|DWHDWH2P|HOST
ert_seconds=0
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/scripts/r_legacy_ksh_dwh'}
script_body:
:inc DW.EXTTEST_HOLE_PFAD
:set &DWH_JOB_KENNUNG='EXTTEST_LEGACY_DWH'
&HOME/scripts/r_legacy_ksh_dwh
:inc DW.EXTTEST_LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
This migration design document covers the transition of the UC4 job `DW.EXTTEST_LEGACY_DWH` into an Apache Airflow DAG. The source object is a standalone Unix job (`JOBS_UNIX`) designed to execute a legacy shell script (`r_legacy_ksh_dwh`). 

Because no parent Workflow (`JOBP`) or Schedule (`JSCH` / `EVNT_TIME`) was supplied in this extraction bundle, this job is assumed to be either externally triggered or historically executed as an ad-hoc/standalone utility. It is wrapped here in a single-task Airflow DAG to preserve its operational capability in the target environment.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.EXTTEST_LEGACY_DWH` | JOBS_UNIX | 1 (Active) | legacy_ksh_dwh |

---

## 3. Scheduling
* **Schedule**: `None`
* **Trigger Source**: No parent `JOBP`, schedule object, or triggering script was found in this extraction bundle. This workflow is flagged as **externally triggered, source unknown** from this extraction alone.
* **Airflow Configuration**: The DAG will be configured with `schedule=None` to prevent accidental scheduled executions.

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_exttest_legacy_dwh` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Derived from Active=1)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_exttest_legacy_dwh_task` | `DW.EXTTEST_LEGACY_DWH` | `EmptyOperator` | N/A | N/A | 1 | 5 Min | None | None | False | None | # REVIEW-STRUCT: Launcher command `&HOME/scripts/r_legacy_ksh_dwh` is unrecognized. Target operator must be confirmed and implemented manually (e.g., mapped to a `BashOperator` or migrated to a Python/GCP equivalent). |

---

## 6. Task Dependency Map
Since this DAG represents a single-task job wrapper, there are no dependencies to map:
```
dw_exttest_legacy_dwh_task
```

---

## 7. Sync / Concurrency Analysis
No sync rules, mutual exclusion locks, or concurrency controls were defined in the source export.
* **Airflow Mapping**: Standard native concurrency control (`max_active_runs=1`) is sufficient.

---

## 8. Error Handling and Retry Strategy
* **Retries**: Configured to inherit default DAG arguments (1 retry, 5-minute delay).
* **Callbacks**: No explicit postcondition actions or failure notifications were provided in the UC4 extract.
* **Trigger Rules**: Standard default (`ALL_SUCCESS`) is utilized.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'EXTTEST_LEGACY_DWH'` | To be passed as an environment variable or Airflow Variable if the underlying script is migrated to a run-ready operator. |

---

## 10. Developer Notes
* **# REVIEW: Standalone Job Plan Assumption**: No parent workflow (`JOBP`) was provided. We have created a wrapper DAG `dw_exttest_legacy_dwh` containing a single task representing the `JOBS_UNIX` object. Confirm if this job should instead be integrated as a task into a larger existing Airflow DAG.
* **# REVIEW-STRUCT: Unrecognized Launcher**: The launcher command `&HOME/scripts/r_legacy_ksh_dwh` is unrecognized and has been stubbed out as an `EmptyOperator`. 
  * If the target environment permits raw shell script execution, this should be refactored into a `BashOperator` or `SSHOperator`.
  * If migrating to a cloud-native architecture, convert the shell script logic into a Python script (e.g., executing on GCS/Dataproc or Cloud Run).
* **# REVIEW: Script Context Variables**: The script references `:inc DW.EXTTEST_HOLE_PFAD` and `:inc DW.EXTTEST_LESE_LOG`. These include scripts are missing from the bundle and must be resolved manually during script conversion.

---

# Pseudocode Outline

```python
# ==============================================================================
# ── Imports ───────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── GCP Configuration ─────────────────────────────────────────────────────────
# ==============================================================================
# No GCP-specific resources are declared due to unrecognized launcher type.
# Placeholders for future operator conversion:
# BUCKET_NAME = "gs://YOUR_BUCKET_NAME"

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── on_failure_callback stubs ─────────────────────────────────────────────────
# ==============================================================================
# No custom error callbacks specified in the UC4 source.

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
# # REVIEW: Created standalone DAG because no parent JOBP was provided.
with DAG(
    dag_id="dw_exttest_legacy_dwh",
    default_args=DEFAULT_ARGS,
    description="Legacy KSH DWH execution - migrated from UC4",
    schedule_interval=None,  # Externally triggered / Standalone utility
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active=1 in UC4 source
    tags=["migrated_uc4", "legacy_dwh"],
) as dag:

    # ==========================================================================
    # ── Guard Task ────────────────────────────────────────────────────────────
    # ==========================================================================
    # None required (No 'Else=Skip' self-locks or cross-DAG locks detected)

    # ==========================================================================
    # ── Sensor Task ───────────────────────────────────────────────────────────
    # ==========================================================================
    # None required (No earliest_start_time constraints detected)

    # ==========================================================================
    # ── Calendar Check Task ───────────────────────────────────────────────────
    # ==========================================================================
    # None required (No calendar_on = 1 constraints detected)

    # ==========================================================================
    # ── Task: dw_exttest_legacy_dwh_task ──────────────────────────────────────
    # ==========================================================================
    # # REVIEW-STRUCT: Launcher command '&HOME/scripts/r_legacy_ksh_dwh' 
    # was unrecognized. Remap this EmptyOperator to a BashOperator, SSHOperator, 
    # or appropriate Cloud Operator once the script logic is finalized.
    #
    # Original Environment Variables set in UC4:
    # DWH_JOB_KENNUNG = 'EXTTEST_LEGACY_DWH'
    # Includes used: DW.EXTTEST_HOLE_PFAD, DW.EXTTEST_LESE_LOG
    dw_exttest_legacy_dwh_task = EmptyOperator(
        task_id="dw_exttest_legacy_dwh_task",
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-task DAG. No execution dependencies.
    dw_exttest_legacy_dwh_task
```

# File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.xml` | `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.py` | Migrated to an Airflow DAG to encapsulate and orchestrate the legacy Unix execution logic. |

# Migration Design Document

### Job dependencies
*   **Downstream Dependency**:
    *   `DW.EXTTEST_ABLAUFSTEUERUNG` is a downstream job that consumes this job's output. It is marked as **not yet migrated**.
    *   *Wiring on BigQuery/Airflow*: End-to-end integration wiring cannot be finalized until `DW.EXTTEST_ABLAUFSTEUERUNG` exists on the target environment. Once migrated, a `TriggerDagRunOperator` or `ExternalTaskSensor` can be configured to maintain the downstream execution sequence.

### Execution order
The target Airflow DAG orchestrates tasks according to the legacy execution sequence:
1.  **Step 1**: The execution of the UC4 job metadata represented by the DAG `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.py` itself.
2.  **Step 2**: The execution of the task within the DAG which runs the shell script `/isxtst/scripts/r_legacy_ksh_dwh` (represented by the task `dw_exttest_legacy_dwh_task`).

### Scheduling
*   This job is **not directly triggered** by any scheduler. It runs inside scheduled jobs (such as an include or a shared execution module).
*   *Airflow Mapping*: The target DAG is defined with `schedule=None`. It remains an unscheduled, callable/importable unit that will be triggered dynamically or as part of a parent controller DAG.

### Schedule & variables
*   **Retained Variables**:
    *   `DWH_JOB_KENNUNG` = `'EXTTEST_LEGACY_DWH'`
    *   *Airflow Mapping*: This scheduler-set variable must be preserved and passed into the task execution context. In the target Airflow DAG, this is passed into the operator's environment dictionary (`env`) or as a task parameter.

### Lineage
*   **Upstream / Includes**:
    *   `UNRESOLVED:DW.EXTTEST_HOLE_PFAD` (included in the UC4 script block; human-confirmed as "not needed")
    *   `UNRESOLVED:DW.EXTTEST_LESE_LOG` (included in the UC4 script block; human-confirmed as "not needed")
*   **Downstream Invocation**:
    *   `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` is invoked by the job. This script resides on the host `/home/gurunathan_t/de_extensionless_bug_repo/vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh`.
*   **Execution Target / Packages**:
    *   `EXT:dwhdwh2p` (original host where execution occurs)
    *   `PACKAGE:DW.UNIX.ISXTST` (the package or user login context `DW.UNIX.ISXTST` used to run the job)

### External system replacements
*   **Legacy Host `dwhdwh2p`**: Maps to the GCP Cloud Composer environment. Execution will run on Airflow workers (or via Cloud Run if containerized) under the target GCP project infrastructure.

### Cross-file dependencies
*   The Airflow DAG references the legacy KornShell script `r_legacy_ksh_dwh` which executes the Oracle SQL*Plus export script `d_legacy_ksh_dwh.sql`. 
*   The Oracle SQL*Plus script and KornShell script are handled in separate migration passes (or are pre-resolved), but they are critical execution dependencies for the main task in this DAG.

### Target file plan
*   **Target File Path**: `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.py`
    *   **Language**: `Python (Airflow DAG)`
    *   **Source File**: `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.xml`

### Environment-specific values
*   **GLOBAL**:
    *   `GCP_PROJECT`: Sourced via `os.environ.get("GCP_PROJECT")` or Airflow variable.
    *   `GCS_BUCKET`: The GCS bucket where script assets are stored. Sourced via Airflow variable `Variable.get("GCS_BUCKET")`.
    *   `HOME` (mapped to Airflow home or script deployment folder): Sourced via `os.environ.get("AIRFLOW_HOME")` or an environment variable.
*   **JOB-SPECIFIC**:
    *   `DWH_JOB_KENNUNG`: Set inline as `'EXTTEST_LEGACY_DWH'` within the DAG task params or environment block.

### Risks and manual steps
*   **Unmigrated Downstream**: Downstream consumer `DW.EXTTEST_ABLAUFSTEUERUNG` is not yet migrated, meaning end-to-end integration testing and trigger wiring cannot be completed until its migration.
*   **Shell Script Execution Scope**: The shell script `r_legacy_ksh_dwh` is invoked via `&HOME/scripts/r_legacy_ksh_dwh`. Since that script is not in our `SOURCE FILES` scope, a human developer or a separate migration pass must ensure it is correctly converted and placed in the appropriate runtime path (e.g., in the Composer `/home/airflow/gcs/data` directory or containerized environment).
*   **Human-Confirmed Negations**: The includes `:inc DW.EXTTEST_HOLE_PFAD` and `:inc DW.EXTTEST_LESE_LOG` were flagged as "not needed" by human-reviewed resolutions. Ensure their functionality (which usually handles path retrieval and log reading) is indeed obsolete or handled natively by Airflow's environment logging and bucket configurations.

---

=== FILE: vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh ===
#!/bin/ksh_dwh
#
# Zweck: EXTTEST export script — legacy_ksh_dwh (site-specific ksh_dwh shebang variant)
#
# Erzeugt am: 2026-08-05
# Versions-Anmerkungen: DE extensionless-file bug test fixture
#
ProgName="EXTTEST legacy_ksh_dwh export"
ProgVersion="V1.0.0"

usage(){
cat <<EOF
Usage: $(basename $0) [-h]
  Runs the legacy_ksh_dwh export.
EOF
}

log_msg(){
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

run_export(){
    log_msg "Starting legacy_ksh_dwh export"
    sqlplus -s /nolog @/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql
    if [ $? -ne 0 ]; then
        log_msg "ERROR: legacy_ksh_dwh export failed"
        exit 1
    fi
    log_msg "legacy_ksh_dwh export completed"
}

case "$1" in
    -h|--help) usage ;;
    *) run_export ;;
esac

exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains custom functions, command-line argument parsing, and executes an external SQL*Plus file whose source is not provided.

EVIDENCE
- Business logic found: KSH custom logic contains argument checking, custom logging functions, and error propagation wrappers around a SQL*Plus execution.
- AWK: none
- SQL-expressible: no, because it executes `sqlplus` with an external SQL file whose contents are not supplied, making it impossible to map to pure SQL.
- Non-SQL side effects: invokes the external `sqlplus` binary.
- Against this verdict: none, since the external SQL file is missing, making a pure BQSQL migration impossible.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `r_legacy_ksh_dwh` runs the EXTTEST export process for `legacy_ksh_dwh`. It is designed to be executed as part of an automated scheduler (such as UC4/Automic). It runs a SQL*Plus script (`d_legacy_ksh_dwh.sql`) to perform an export and contains logging and error handling to ensure failures are propagated correctly.

2. INVOCATION CONTEXT
   - Who calls this script: UC4 job (implied by context, exact JOBS_UNIX object is unknown from the extraction).
   - UC4 native includes: None referenced.
   - Environment files sourced: None referenced (uses the custom shebang `#!/bin/ksh_dwh` representing a site-specific KornShell variant).

3. PARAMETERS / INPUTS
   - Name: `$1` (positional argument)
   - Source: CLI argument
   - Used: Yes, in the `case "$1" in` block to check for `-h` or `--help`.
   - Python equivalent: `sys.argv[1]` or `argparse` module.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - Exact command line: `sqlplus -s /nolog @/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql`
   - Purpose: Launches SQL*Plus in silent mode to execute the SQL export logic defined in `d_legacy_ksh_dwh.sql`.
   - Python DB-client call or subprocess: It must remain an external process invocation via `subprocess` because the SQL source file is not provided in this extraction, meaning we cannot safely parse or convert the database connection mechanism or SQL statements.
   - Resolvable Launcher: No, because the SQL source is missing and there are no declared environment connection parameters in this extraction to infer database context.
   - Structural Gap Flag: # REVIEW-STRUCT: launcher [sqlplus] invoked — internal SQL file /vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql not available in this extraction; confirm database credentials, target platform, and error handling before finalizing the conversion.

5. EMBEDDED SQL
   - Source file: `/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql`
   - Full SQL text: Not supplied in this extraction.
   - Statement type: Unknown.
   - Tables touched: Unknown.
   - Dialect: SQL*Plus (inferred from launcher).

6. CONTROL FLOW
   1. Initialize environment metadata (`ProgName="EXTTEST legacy_ksh_dwh export"` and `ProgVersion="V1.0.0"`).
   2. Evaluate the first positional argument (`$1`):
      - If `-h` or `--help`, call the `usage()` function and display command usage details, then exit.
      - For any other input (or no arguments), execute the `run_export()` function.
   3. In `run_export()`:
      - Log start message ("Starting legacy_ksh_dwh export") via the `log_msg()` custom function.
      - Execute `sqlplus -s /nolog @/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql`.
      - Check the exit status (`$?`). If non-zero, log an error ("ERROR: legacy_ksh_dwh export failed") and exit with code 1.
      - If successful, log the completion message ("legacy_ksh_dwh export completed").
   4. Exit script with code 0.

7. ERROR HANDLING & EXIT CODES
   - How does the script detect failure: Uses a direct check on the exit code of `sqlplus` (`if [ $? -ne 0 ]`).
   - What does it do on failure: Logs a custom failure message and exits with status code 1.
   - Success exit code convention: Exits with code 0.
   - Python mapping: Wrap the `subprocess.run()` invocation inside a try-except block handling `subprocess.CalledProcessError` or check the `returncode` of the completed process and raise `sys.exit(1)` upon failure.

8. OUTPUTS / SIDE EFFECTS
   - Logs standard output messages with timestamps via stdout.
   - Database side effects or generated export files from the unsupplied `d_legacy_ksh_dwh.sql` script.

9. BUSINESS SUMMARY
   - Coordinates the legacy database export task for the `legacy_ksh_dwh` platform.
   - Handles basic help and command-line usage routing.
   - Launches a SQL*Plus session to perform database operations.
   - Confirms successful execution of database commands, raising alert logs and standard exit status codes upon failure.

=== PSEUDOCODE ===

```python
import sys
import os
import subprocess
import datetime

# Step 1: Initialize program metadata
PROG_NAME = "EXTTEST legacy_ksh_dwh export"
PROG_VERSION = "V1.0.0"

# Step 2: Define helper functions
def log_msg(message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} {message}")

def usage() -> None:
    script_name = os.path.basename(sys.argv[0])
    print(f"Usage: {script_name} [-h]")
    print("  Runs the legacy_ksh_dwh export.")

def run_export() -> None:
    log_msg("Starting legacy_ksh_dwh export")
    
    # REVIEW-STRUCT: launcher [sqlplus] invoked — internal SQL file /vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql not available in this extraction; confirm database credentials, target platform, and error handling before finalizing the conversion
    cmd = [
        "sqlplus",
        "-s",
        "/nolog",
        "@/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql"
    ]
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        log_msg("ERROR: legacy_ksh_dwh export failed")
        sys.exit(1)
        
    log_msg("legacy_ksh_dwh export completed")

# Step 3: Main execution flow / argument parsing
if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
    usage()
    sys.exit(0)
else:
    run_export()
    sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` | `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py` | KornShell wrapper script converted to Python to handle command-line execution and logging. Since the underlying SQL file `d_legacy_ksh_dwh.sql` is confirmed as "not needed", the internal execution of SQL*Plus is bypassed/stubbed. |

### Job dependencies
- **Downstream Job**: `DW.EXTTEST_ABLAUFSTEUERUNG` is currently marked as "not yet migrated". Once migrated, the dependency will be wired on the target Cloud Composer environment via a direct trigger or an Airflow `ExternalTaskSensor`.

### Execution order
- **Step 1**: The legacy scheduler XML definition `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.xml` is translated into the Airflow DAG orchestration wrapper.
- **Step 2**: The script `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` maps to the `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py` task, which is executed as a standard task within the Airflow DAG.

### Scheduling
- This job is not directly triggered by any of the run's schedulers. It runs as an include/shared module (e.g. called as part of another workflow). Therefore, it must remain a callable/importable task unit in the target environment (Cloud Composer) without receiving its own standalone cron schedule.

### Schedule & variables
- **DWH_JOB_KENNUNG** = `'EXTTEST_LEGACY_DWH'`: This scheduler-set variable must reach the migrated Python job at runtime. In Cloud Composer, this will be passed to the task as an environment variable (`os.environ["DWH_JOB_KENNUNG"]`) or via DAG `params`.

### Lineage
- **Lineage Edge**: The legacy script `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` executes `UNRESOLVED:D_LEGACY_KSH_DWH.SQL`.

### External system replacements
- **SQL*Plus / Oracle Database**: The legacy shell script launches SQL*Plus to execute `d_legacy_ksh_dwh.sql`. In the target Google Cloud Platform environment, database operations are executed against BigQuery. Because `d_legacy_ksh_dwh.sql` is retired (human-confirmed as "not needed"), the database execution is bypassed in the target Python script, which will log a success status and exit.

### Cross-file dependencies
- The script `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh` references `/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql` (retired/not needed).

### Target file plan
- **Target File**: `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py`
  - **Language**: Python
  - **Source File**: `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh`

### Environment-specific values
- **DWH_JOB_KENNUNG** (JOB-SPECIFIC): Sourced via task parameters or Airflow task environment variables, resolving to the literal value `'EXTTEST_LEGACY_DWH'`.

### Risks and manual steps
- `SOURCE: NOT FOUND — D_LEGACY_KSH_DWH.SQL — no candidate (human-reviewed as NO SOURCE NEEDED)`
- Downstream job `DW.EXTTEST_ABLAUFSTEUERUNG` is "not yet migrated", meaning the final dependency/orchestration hook cannot be finalized until it exists.