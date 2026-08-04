=== OBJECT: CUSTOMER.HISTORIZATION_LOAD (JOBS_UNIX) ===
active=1
title=SCD2 historization of the weekly customer segment/score into the segment dimension
login=UNIX.ETL_SVC
host=|ETLHOST2|HOST
ert_seconds=20
launcher_type=unrecognized
launcher_details={'raw_command': '#!/bin/ksh'}
script_body:
#!/bin/ksh
# CUSTOMER.HISTORIZATION_LOAD
:SET &RUN_DATE='&$TODAY'
. &HOME/customer/r_historization_load.ksh
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

This document details the migration design of a standalone UC4 UNIX job to an Apache Airflow DAG. Since the extraction bundle contains only a single `JOBS_UNIX` object with no parent `JOBP` (workflow) container or calendar schedule, it is represented as a single-task DAG.

---

## 1. Overview
This migration contains a single standalone UC4 UNIX job, `CUSTOMER.HISTORIZATION_LOAD`, which executes an SCD2 (Slowly Changing Dimension Type 2) historization process. It loads weekly customer segments and scores into a segment dimension table. Because no parent workflow (`JOBP`) was provided in the extraction, this job is modeled as a standalone Airflow DAG containing a single task. This process is triggered externally as no scheduling configurations were defined in the extraction.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `CUSTOMER.HISTORIZATION_LOAD` | JOBS_UNIX | `1` (Active) | SCD2 historization of the weekly customer segment/score into the segment dimension |

---

## 3. Scheduling
* **Schedule Trigger:** This workflow has no calendar-based schedule of its own within the extraction bundle (no `EVNT_TIME` or schedule triggers present). 
* **Trigger Source:** Neither a parent `JOBP` nor a triggering `SCRI` was supplied. This workflow is noted as externally triggered, source unknown from this extraction alone.
* **DAG Schedule:** `schedule=None`

---

## 4. Airflow DAG Properties
The following DAG properties are mapped from the `CUSTOMER.HISTORIZATION_LOAD` job:

| Property | Value |
| :--- | :--- |
| `dag_id` | `customer_historization_load` |
| `schedule` | `None` |
| `start_date` | `datetime(2023, 1, 1)` *(placeholder)* |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `is_paused_upon_creation` | `False` *(Active=1 maps to is_paused_upon_creation=False)* |
| `default_args` | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `customer_historization_load` | `CUSTOMER.HISTORIZATION_LOAD` | `EmptyOperator` | N/A | N/A | 1 | 5m | None | None | False | None | #REVIEW-STRUCT: launcher command [`#!/bin/ksh`] not recognised — confirm target operator/script manually. Script runs `. &HOME/customer/r_historization_load.ksh`. |

---

## 6. Task Dependency Map
Since there is only one task in this DAG, there are no dependencies to map:

```python
customer_historization_load
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` (locks or mutual exclusions) are defined for this object. No additional concurrency guards are required beyond setting `max_active_runs=1` as a standard production practice.

---

## 8. Error Handling and Retry Strategy
No custom postcondition actions, run-time branches, or execution guards are defined in the UC4 extraction. Standard default Airflow retry mechanisms apply.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Dynamic Mapping |
| :--- | :--- | :--- |
| `&RUN_DATE` | `&$TODAY` | `{{ ds }}` (Airflow execution date string in `YYYY-MM-DD` format) |
| `&HOME` | Environment Variable | Set via Airflow environment variable configurations or system-level pathing. |

---

## 10. Developer Notes
* **#REVIEW-STRUCT:** The original launcher command `#!/bin/ksh` was classified as unrecognized. The underlying shell command is `. &HOME/customer/r_historization_load.ksh`. For production Airflow, migrate this to an `SSHOperator` (to run on a remote edge host) or a `BashOperator` (if running locally on the Airflow worker), replacing the `EmptyOperator` placeholder.
* **#REVIEW-STRUCT:** No parent workflow (`JOBP`) was supplied in this extraction. The standalone task has been wrapped in its own dedicated DAG `customer_historization_load`. If this task is part of a larger, unprovided workflow, verify whether it should be integrated into a larger DAG instead.
* **Parameter Initialization:** The script references `&RUN_DATE='&$TODAY'`. Ensure that your converted script or command uses the Airflow execution date macro `{{ ds }}` or `{{ logical_date }}` to maintain historical backfill consistency.

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
# No GCP connections are mapped for this generic UNIX execution.
# (If migrated to GKE/Compute, define connections or SSH hooks here)

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ── on_failure_callback stubs ─────────────────────────────────────────────────
# ==============================================================================
# No custom failure callback objects are specified in the extraction.

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id='customer_historization_load',
    default_args=DEFAULT_ARGS,
    description='SCD2 historization of the weekly customer segment/score into the segment dimension',
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['customer', 'historization', 'uc4_migration'],
) as dag:

    # ==========================================================================
    # ── Task: customer_historization_load ─────────────────────────────────────
    # ==========================================================================
    # #REVIEW-STRUCT: Unrecognized launcher type in UC4 (raw_command: #!/bin/ksh).
    # Script body content:
    #   :SET &RUN_DATE='&$TODAY'
    #   . &HOME/customer/r_historization_load.ksh
    #
    # Recommended Airflow Target: SSHOperator (to run on host |ETLHOST2|), or BashOperator.
    # Currently stubbed as EmptyOperator per migration rules.
    customer_historization_load = EmptyOperator(
        task_id='customer_historization_load',
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-task DAG: No dependencies to register.
    pass
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/CUSTOMER.HISTORIZATION_LOAD.xml` | `dags/customer/customer_historization_load.py` | Migrates the UC4 job definition into an Airflow DAG to orchestrate the historization script and manage execution parameters. |

---

### Job dependencies
* **Downstream Job:** `CUSTOMER.WEEKLY_SCHEDULE` is currently marked as **not yet migrated**. Once migrated, it must be wired to execute upon the successful completion of this DAG. This can be achieved using Airflow's `TriggerDagRunOperator` or an `ExternalTaskSensor` in the downstream DAG. A warning is added under Risks & Manual Actions because this downstream connection cannot be completed or tested until the downstream job is migrated.

### Execution order
* **Task Sequence Mapping:** The orchestration must preserve the legacy execution flow sequence:
  1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` -> Represented by the parent Airflow DAG (`dags/customer/customer_historization_load.py`).
  2. `customer/r_historization_load.ksh` -> Executed as a task in this DAG calling the migrated Python wrapper equivalent (migrated under a separate design pass).
  3. `customer/k_historization_load.ksh` -> Invoked downstream via the wrapper task (migrated under a separate design pass).
  4. `customer/d_historization_load.sql` -> Executed downstream via BigQuery/Dataform within the pipeline (migrated under a separate design pass).
  5. `customer/d_segment_quality_check.sql` -> Final quality check query executed at the end of the pipeline (migrated under a separate design pass).

### Scheduling
* **Trigger Mechanism:** This job is not directly triggered by any independent schedulers; it executes as an included/shared module inside other scheduled processes. Accordingly, the migrated Airflow DAG must be configured with `schedule=None` so that it remains a callable, externally-triggered unit. It will be initiated programmatically via `TriggerDagRunOperator` from parent workflows or called directly via the Airflow API.

### Schedule & variables
* **SCHEDULER-SET VARIABLES:**
  * `RUN_DATE`: Set dynamically in the source as `&$TODAY`. In Airflow, this maps to the logical date macro `{{ ds }}`.
  * `MAX_EXPECTED_CHANGE_PCT`: Defined in the source XML as `25` (`<row Name="MAX_EXPECTED_CHANGE_PCT" Value="25"/>`). This must be exposed as a DAG parameter or task parameter with a default value of `25` so it can be cleanly propagated downstream to the quality check script.

### Lineage
* **Upstream Producers:** None.
* **Downstream Consumers:** `CUSTOMER.WEEKLY_SCHEDULE` (job: not yet migrated) — representing a cross-job hand-off.
* **Invoked Scripts:** `customer/r_historization_load.ksh`.
* **Execution Host:** `EXT:ETLHOST2` (legacy UNIX host).
* **Target Table Lineage (Parser Artifacts):** The source metadata includes lineage relationships pointing to `TABLE:THE` and `TABLE:OF`. These are parser noise artifacts extracted from the job description ("SCD2 historization of the... segment dimension of...") and do not represent actual database tables. They must be ignored during final implementation.

### External system replacements
* **Legacy UNIX Host:** The legacy execution host `ETLHOST2` is retired. Execution shifts directly to the Google Cloud Composer GKE environment or GCSF/BigQuery operations, removing the need for dedicated remote host SSH connections.

### Cross-file dependencies
* **Script Invocation:** This DAG is responsible for executing the migrated version of `customer/r_historization_load.ksh` (which is converted in a separate design pass). Runtime parameters (e.g., `RUN_DATE` and `MAX_EXPECTED_CHANGE_PCT`) must be passed from this DAG to the target script task.

### Target file plan
* **`dags/customer/customer_historization_load.py`**
  * **Language:** Python (Airflow DAG)
  * **Source File:** `customer/CUSTOMER.HISTORIZATION_LOAD.xml`
  * **Description:** Contains the Airflow DAG definition that configures variables, sets default execution arguments, and triggers the Python task representing the migrated `r_historization_load.ksh` script.

### Environment-specific values
* **`GCP_PROJECT`** (GLOBAL): Identifies the target Google Cloud Project. Resolved via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
* **`GCP_REGION`** (GLOBAL): The deployment region for composer/tasks. Resolved via `Variable.get("GCP_REGION")`.
* **`MAX_EXPECTED_CHANGE_PCT`** (JOB-SPECIFIC): Parameter specifying the change threshold for the historization check. Defaults to `25` and is passed via DAG `params`.
* **`RUN_DATE`** (JOB-SPECIFIC): The logical execution date of the run. Captured dynamically using the `{{ ds }}` macro.

### Risks and manual steps
* **WIRING FOR UNMIGRATED DOWNSTREAM:** The downstream workflow `CUSTOMER.WEEKLY_SCHEDULE` is marked as **not yet migrated**. The dependency linkage cannot be finalized in the target environment until this downstream asset has been migrated.
* **SIBLING PASS ALIGNMENT:** The underlying execution scripts (`r_historization_load.ksh`, `k_historization_load.ksh`, etc.) are owned by different design passes. A manual verification step is required during integration to ensure that this wrapper DAG correctly imports and references the Python task representing the migrated `r_historization_load.ksh`.
* **LEGACY HOST DEPRECATION:** The execution environment is transitioning from a dedicated UNIX host `ETLHOST2` to Cloud Composer. Any absolute paths or local environment references (such as `&HOME`) must be updated to reference correct GCS bucket paths (`gcs/dags/...`) or Cloud Composer environment variables.

---

### group 2/5 — DESIGN FAILED

ERROR: Design cannot proceed — REQUIRED TOOL returned errors, empty, or hollow (no real content extracted) responses on every attempt. Investigate the MCP service and retry the job.


---

### group 3/5 — DESIGN FAILED

ERROR: Design cannot proceed — REQUIRED TOOL returned errors, empty, or hollow (no real content extracted) responses on every attempt. Investigate the MCP service and retry the job.


---

=== FILE: customer/k_historization_load.ksh ===
#!/bin/ksh
###############################################################################
# k_historization_load.ksh
#
# SCD Type 2 merge of this week's customer score/segment into the segment
# dimension, followed by a sanity check that the number of newly-versioned
# rows is not implausibly large (a common symptom of a bad join key causing
# every row to look "changed").
###############################################################################

CRM_HOME=${CRM_HOME:-/opt/etl/customer}
CRM_ORA_USER=${CRM_ORA_USER:-crm_etl}
CRM_ORA_PASS=${CRM_ORA_PASS:-changeit}
CRM_ORA_SID=${CRM_ORA_SID:-CRMPRD}
MAX_EXPECTED_CHANGE_PCT=25

log() {
    print "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "Running SCD2 merge for customer segment dimension"
sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_historization_load.sql "${RUN_DATE}"
merge_rc=$?

if [ ${merge_rc} -ne 0 ]; then
    log "ERROR: d_historization_load.sql failed with rc=${merge_rc}"
    exit 1
fi

changed_pct=$(sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_segment_quality_check.sql "${RUN_DATE}" \
    | tr -d '[:space:]')

if [ -z "${changed_pct}" ]; then
    log "WARN: could not compute changed-row percentage - skipping sanity check"
    exit 0
fi

if [ "${changed_pct}" -gt "${MAX_EXPECTED_CHANGE_PCT}" ] 2>/dev/null; then
    log "WARN: ${changed_pct}% of customers changed segment this week (expected <= ${MAX_EXPECTED_CHANGE_PCT}%) - flagging for review, not failing the job"
fi

log "Historization merge complete, ${changed_pct}% of customers re-versioned"
exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains custom shell-based logging, captures and sanitizes SQL execution outputs, and performs mathematical validation threshold checks on the results.

EVIDENCE
- Business logic found: Yes, in the KSH custom logic where it executes a dimension merge and runs a quality check comparing the percentage of changed records against a safety threshold.
- AWK: none
- SQL-expressible: No, the control flow relies on capturing external program stdout, string sanitization, and conditional alerting logic based on threshold comparison.
- Non-SQL side effects: Command execution with output capture, console logging, and distinct exit/warning codes.
- Against this verdict: One could argue for BQSQL if the external SQL files were supplied and fully translatable, but the operational checks and parameter-driven execution are best preserved in Python.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script executes a weekly slowly changing dimension (SCD) Type 2 merge to load customer scores and segments into a segment dimension table. Following the merge, it performs a data quality sanity check by invoking a query to calculate the percentage of newly-versioned customer records. If the percentage of changed records exceeds a configurable safety threshold (default 25%), it raises a warning to flag potential join corruption without failing the pipeline.

2. INVOCATION CONTEXT
   - Who calls this script: Unknown (typically triggered by a UC4 job, but JOBS_UNIX object metadata is not supplied in this extraction).
   - UC4 native includes: None referenced in this extraction.
   - Environment files sourced: None sourced.

3. PARAMETERS / INPUTS
   - `CRM_HOME` (Environment Variable): Base directory path for CRM ETL assets. Default: `/opt/etl/customer`. Used to locate SQL scripts.
   - `CRM_ORA_USER` (Environment Variable): Oracle database username. Default: `crm_etl`. Used for database connections.
   - `CRM_ORA_PASS` (Environment Variable): Oracle database password. Default: `changeit`. Used for database connections.
   - `CRM_ORA_SID` (Environment Variable): Oracle System Identifier (SID) or connection string. Default: `CRMPRD`. Used for database connections.
   - `MAX_EXPECTED_CHANGE_PCT` (Environment Variable): Threshold percentage of changed records allowed before generating a warning. Default: `25`. Used in validations.
   - `RUN_DATE` (Environment Variable): Parameter passed into the SQL execution. Appears as `${RUN_DATE}` but is not declared locally in the shell. Surfaces in Python via `os.environ.get("RUN_DATE")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - Command 1: `sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_historization_load.sql "${RUN_DATE}"`
     - Purpose: Executes the SCD Type 2 dimension merge SQL script.
     - Target: Should remain an external process invocation via `subprocess.run` to execute SQL*Plus if SQL*Plus environment dependencies must be preserved, or can be converted to run the SQL file natively using a Python Oracle driver (such as `oracledb`).
     - Resolvable Launcher: No, because the contents of `d_historization_load.sql` are not supplied in this extraction.
   - Command 2: `sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_segment_quality_check.sql "${RUN_DATE}"`
     - Purpose: Computes the percentage of customers whose segments changed this week.
     - Target: Processed via `subprocess.run` capturing standard output for verification.

5. EMBEDDED SQL
   - No SQL is written inline. Two external Oracle SQL files are referenced:
     - SQL File 1: `${CRM_HOME}/customer/d_historization_load.sql`
       - # REVIEW-STRUCT: SQL file [d_historization_load.sql] body not supplied — behaviour unknown
     - SQL File 2: `${CRM_HOME}/customer/d_segment_quality_check.sql`
       - # REVIEW-STRUCT: SQL file [d_segment_quality_check.sql] body not supplied — behaviour unknown
   - Dialect Identification: The use of `sqlplus` with the `@` operator and positional arguments (`"${RUN_DATE}"`) confirms the target platform is Oracle SQL*Plus.
     - # REVIEW: target database platform is Oracle; python-oracledb or subprocess-based sqlplus execution choice is provisional based on target system capabilities.

6. CONTROL FLOW
   1. **Initialization**: Read and assign default values to connection and execution environment variables (`CRM_HOME`, `CRM_ORA_USER`, `CRM_ORA_PASS`, `CRM_ORA_SID`, `MAX_EXPECTED_CHANGE_PCT`, `RUN_DATE`).
   2. **SCD2 Execution**: Log the start of the process and invoke `d_historization_load.sql` via SQL*Plus, passing `RUN_DATE`.
   3. **Execution Status Check**: Verify the return code of the SCD2 execution. If it is non-zero, log an error message and exit the program with status `1`.
   4. **Quality Check Execution**: Log and execute `d_segment_quality_check.sql` via SQL*Plus, passing `RUN_DATE`. Capture standard output.
   5. **Data Sanitization**: Strip all whitespaces, line breaks, and carriage returns from the captured quality check output (mimicking `tr -d '[:space:]'`).
   6. **Sanity Validation**:
      - If the cleaned output is empty, log a warning stating that the check is being skipped, and exit successfully with status `0`.
      - Try converting the output to an integer. If successful and the value is greater than `MAX_EXPECTED_CHANGE_PCT` (25%), print a warning log alerting that the changes are abnormally high, but do not fail the program. Suppress conversion errors (mimicking `2>/dev/null`).
   7. **Success Logging**: Log successful completion of the historization process, showing the computed changed percentage, and exit with status `0`.

7. ERROR HANDLING & EXIT CODES
   - Merging Errors: Checked immediately via return code verification (`$?`). Non-zero causes an immediate exit with `1`.
   - Quality Check Output Empty: Handled gracefully. Prints a warning and exits with `0`.
   - Comparison Parsing Errors: Non-integer strings are safely handled by suppressing standard error redirecting `2>/dev/null` in KSH, translated in Python using a robust `try-except` block when casting to `int`.
   - Success Exit Code: `0`.

8. OUTPUTS / SIDE EFFECTS
   - Mutates data in the target Oracle Dimension Table via `d_historization_load.sql`.
   - Writes logs and warnings directly to standard output and standard error.

9. BUSINESS SUMMARY
   - Performs automated weekly updates of customer segment dimensions using SCD Type 2 logic.
   - Calculates the statistical deviation (percentage of segment shifts) to ensure data integrity.
   - Flags potentially corrupted ETL pipeline runs (e.g., massive segment shifts caused by key mismatches) before downstream applications consume the mutated data.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Environment Setup
import os
import sys
import subprocess
from datetime import datetime

# # REVIEW: target database platform is Oracle; python-oracledb or subprocess-based sqlplus execution choice is provisional
CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
CRM_ORA_USER = os.environ.get("CRM_ORA_USER", "crm_etl")
CRM_ORA_PASS = os.environ.get("CRM_ORA_PASS", "changeit")
CRM_ORA_SID = os.environ.get("CRM_ORA_SID", "CRMPRD")

try:
    MAX_EXPECTED_CHANGE_PCT = int(os.environ.get("MAX_EXPECTED_CHANGE_PCT", "25"))
except ValueError:
    MAX_EXPECTED_CHANGE_PCT = 25

RUN_DATE = os.environ.get("RUN_DATE", "")

# Step 2: Custom Logging Definition
def log(message: str, file=sys.stdout):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", file=file)

# Step 3: Run SCD2 Merge
log("Running SCD2 merge for customer segment dimension")

# # REVIEW-STRUCT: SQL file [d_historization_load.sql] body not supplied — behaviour unknown
sql_load_path = os.path.join(CRM_HOME, "customer/d_historization_load.sql")
connection_string = f"{CRM_ORA_USER}/{CRM_ORA_PASS}@{CRM_ORA_SID}"

try:
    # Executing external SQL script using SQL*Plus as a subprocess to preserve legacy execution behavior
    merge_process = subprocess.run(
        ["sqlplus", "-s", connection_string, f"@{sql_load_path}", RUN_DATE],
        capture_output=False,
        text=True,
        check=True
    )
except subprocess.CalledProcessError as e:
    # Step 4: Handle Failure of SCD2 Merge
    log(f"ERROR: d_historization_load.sql failed with rc={e.returncode}", file=sys.stderr)
    sys.exit(1)

# Step 5: Run Segment Quality Check
# # REVIEW-STRUCT: SQL file [d_segment_quality_check.sql] body not supplied — behaviour unknown
sql_qc_path = os.path.join(CRM_HOME, "customer/d_segment_quality_check.sql")

try:
    qc_process = subprocess.run(
        ["sqlplus", "-s", connection_string, f"@{sql_qc_path}", RUN_DATE],
        capture_output=True,
        text=True,
        check=True
    )
    # Step 6: Sanitize Output (tr -d '[:space:]')
    changed_pct_str = "".join(qc_process.stdout.split())
except subprocess.CalledProcessError as e:
    # If the quality check script execution fails completely, handle gracefully or raise error
    log(f"ERROR: Quality check database execution failed with rc={e.returncode}", file=sys.stderr)
    sys.exit(1)

# Step 7: Handle Empty Output Check
if not changed_pct_str:
    log("WARN: could not compute changed-row percentage - skipping sanity check")
    sys.exit(0)

# Step 8: Perform Safety Threshold Comparisons
try:
    # Replicates the KSH numeric evaluation and redirects stderr to null (2>/dev/null)
    changed_pct = int(changed_pct_str)
    if changed_pct > MAX_EXPECTED_CHANGE_PCT:
        log(f"WARN: {changed_pct}% of customers changed segment this week (expected <= {MAX_EXPECTED_CHANGE_PCT}%) - flagging for review, not failing the job")
except ValueError:
    # Replicates 2>/dev/null behavior where non-integer values ignore comparison check
    pass

# Step 9: Process Completion
log(f"Historization merge complete, {changed_pct_str}% of customers re-versioned")
sys.exit(0)
```

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/k_historization_load.ksh` | `customer/k_historization_load.py` | Converted to a Python script that orchestrates the BigQuery-based SCD2 merge and quality validation using the BigQuery Python Client instead of SQL*Plus, maintaining logging and threshold checks. |

***

### Job Dependencies
- **Downstream Job:** `CUSTOMER.WEEKLY_SCHEDULE` — not yet migrated.
- **Wiring on BigQuery / Cloud Composer:** Since the downstream job `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated, the orchestration link cannot be fully finalized. Once `CUSTOMER.WEEKLY_SCHEDULE` is migrated to Airflow, it must be wired as a downstream task or DAG dependency (e.g., using a `TriggerDagRunOperator` or shared task group) that consumes the output of the customer segment dimension table generated by this historization job.

### Execution Order
The original execution sequence must be preserved in the target orchestration (Cloud Composer):
1. **UC4 Job Definition:** `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (Defines job execution metadata and scheduler variables)
2. **KSH Wrapper:** `customer/r_historization_load.ksh` (Logs execution and captures exit codes; triggers `k_historization_load.ksh`)
3. **Execution Logic:** `customer/k_historization_load.ksh` (Target: `customer/k_historization_load.py` - coordinates step 4 and step 5)
4. **SCD2 Load:** `customer/d_historization_load.sql` (Target: BigQuery/Dataform SQL query executing the SCD2 merge)
5. **Quality Check:** `customer/d_segment_quality_check.sql` (Target: BigQuery SQL query calculating changed row percentage)

In the target Python execution, `customer/k_historization_load.py` (Step 3) will call the target equivalents of `customer/d_historization_load.sql` (Step 4) followed by `customer/d_segment_quality_check.sql` (Step 5) against BigQuery.

### Scheduling
- **Triggering details:** This job is not directly triggered by any of the run's schedulers; it runs as an included or shared module inside scheduled parent jobs.
- **Target Orchestration:** The migrated Python script (`customer/k_historization_load.py`) must remain an importable, callable unit (such as a task or TaskGroup within an Airflow DAG) and must not be given its own standalone Airflow schedule.

### Schedule & Variables
- **Scheduler-Set Variable:** `RUN_DATE` (legacy value `&$TODAY`).
- **Target Mapping:** This variable will be injected at runtime by the Airflow scheduler using execution date macros (e.g., `{{ ds }}` or `dag_run.logical_date`) and passed into the Python script's execution environment.

### Lineage
- **Upstream SQL Executes:**
  - `customer/k_historization_load.ksh` executes SQL file `customer/d_historization_load.sql`
  - `customer/k_historization_load.ksh` executes SQL file `customer/d_segment_quality_check.sql`
- **Downstream Consumers:** The job updates the customer segment dimension table (`DIM_CUSTOMER_SEGMENT`), which is subsequently used by downstream weekly schedules and analytical reports.

### External System Replacements
- **Legacy Database Execution:** SQL*Plus connection and execution (`sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID}`) is retired.
- **Target Replacement:** The python script will use `google.cloud.bigquery.Client` to submit asynchronous query jobs to BigQuery, utilizing service account authentication configured on the Cloud Composer environment.

### Cross-File Dependencies
- **SQL Files:** The script depends on the external files `customer/d_historization_load.sql` and `customer/d_segment_quality_check.sql` which belong to a different group in this migration. 
- **Integration:** The Python script expects these SQL queries to be deployed to a accessible GCS bucket location or embedded as package assets in the target repository. It will read their contents and submit them as parametrized queries.

### Target File Plan
- **Target File Path:** `customer/k_historization_load.py`
  - **Language:** Python
  - **Source File:** `customer/k_historization_load.ksh`
  - **Purpose:** Executable Python script deployed to Cloud Composer. It leverages the BigQuery Python Client to execute the SQL scripts, parses the numeric output of the quality check, and issues warnings when thresholds are violated.

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
- `GCP_PROJECT`: Sourced via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")`. Represents the target GCP project ID.
- `BQ_DATASET`: Sourced via `Variable.get("BQ_DATASET")`. Represents the target BigQuery dataset containing the customer segment dimension tables.
- `CRM_HOME` (Re-mapped to GCS/local mount): Sourced via `Variable.get("CRM_HOME")`. Represents the path where SQL scripts are stored in Cloud Composer.

#### 2. JOB-SPECIFIC
- `MAX_EXPECTED_CHANGE_PCT`: Defined inside a job-level configuration object (default value: `25`).
- `RUN_DATE`: Passed dynamically at runtime to the Python execution task from the parent scheduler (e.g., using `{{ ds }}`).

***

### Risks and Manual Steps
- **Downstream Dependency:** The downstream job `CUSTOMER.WEEKLY_SCHEDULE` is marked "not yet migrated". Airflow task connections or sensor linkages cannot be completed until that job is migrated. This is flagged as a blocking dependency.
- **Query Parametrization:** The original SQL scripts receive positional arguments via SQL*Plus (`"${RUN_DATE}"`). The corresponding converted BigQuery SQL queries must be designed to accept named query parameters (such as `@run_date`), and `k_historization_load.py` must pass this parameter explicitly via `google.cloud.bigquery.QueryJobConfig`.
- **Quality Check Output Parsing:** The original KSH script strips whitespace (`tr -d '[:space:]'`) from the SQL*Plus output. In the Python/BigQuery version, the BigQuery Client will return a structured row iterator. The Python script must extract the scalar value of the first column of the first row to perform the threshold validation, rather than parsing raw stdout strings.
- **SQL File Locations:** Since `customer/d_historization_load.sql` and `customer/d_segment_quality_check.sql` are outside the scope of this migration group, their exact target deployment paths must be coordinated and verified prior to executing the Python orchestration.

---

=== FILE: customer/r_historization_load.ksh ===
#!/bin/ksh
###############################################################################
# r_historization_load.ksh
#
# Invoked by CUSTOMER.HISTORIZATION_LOAD. Wraps the SCD2 historization merge
# so a partial/failed merge is always logged with its row-impact counts
# before the job exits, rather than only surfacing sqlplus's raw exit code.
###############################################################################
set -e

CRM_HOME=${CRM_HOME:-/opt/etl/customer}

log() {
    print "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "Starting SCD2 historization for run date ${RUN_DATE}"
. ${CRM_HOME}/customer/k_historization_load.ksh
rc=$?

if [ ${rc} -ne 0 ]; then
    log "ERROR: k_historization_load.ksh failed with rc=${rc}"
    exit ${rc}
fi

log "Historization load completed for ${RUN_DATE}"
exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains functional logic including a custom logging function and error-handling conditions, and it sources another shell script whose implementation is not provided, requiring Python for robust orchestration.

EVIDENCE
- Business logic found: KSH custom logic defines a custom timestamped `log` function and contains conditional error trapping/logging when sourcing the downstream script.
- AWK: none
- SQL-expressible: no (the script is primarily an orchestration wrapper and error catcher sourcing a downstream shell script)
- Non-SQL side effects: Sourcing and executing an external shell script (`k_historization_load.ksh`), writing to standard output with custom timestamped logs.
- Against this verdict: NO_CONVERSION_REQUIRED could be argued if this was treated as a pure wrapper, but the presence of the `log` function and post-execution evaluation logic strictly disqualifies it.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`r_historization_load.ksh`) acts as a logging and error-trapping wrapper for the SCD2 historization load process. It is triggered during the customer historization phase, sets up runtime environment variables, invokes the core shell script (`k_historization_load.ksh`), and intercepts any non-zero exit codes to log specific failure details before propagating the exit status.

2. INVOCATION CONTEXT
   - Who calls this script: Invoked within the UC4 scheduler by the job `CUSTOMER.HISTORIZATION_LOAD` (JOBS_UNIX object).
   - UC4 native includes: None referenced in this extraction.
   - Environment files sourced: Sources `${CRM_HOME}/customer/k_historization_load.ksh` dynamically. 
     # REVIEW-STRUCT: environment file / script k_historization_load.ksh body not supplied — behaviour and variables it sets are unknown.

3. PARAMETERS / INPUTS
   - `CRM_HOME`: Environment variable specifying the base path for CRM ETL assets. Source: Sourced environment / UC4 configuration. Defaulted to `/opt/etl/customer` if not set. Used in script body. Surface in Python via `os.environ.get("CRM_HOME", "/opt/etl/customer")`.
   - `RUN_DATE`: Environment variable denoting the business date of execution. Source: UC4 execution context. Used inside the log statements. Surface in Python via `os.environ.get("RUN_DATE")`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `date '+%Y-%m-%d %H:%M:%S'`: Command-line utility invoked inside the `log` function to obtain formatting timestamps. In Python, replace with native `datetime.now().strftime("%Y-%m-%d %H:%M:%S")`.
   - `. ${CRM_HOME}/customer/k_historization_load.ksh`: Sourced shell script execution. Must remain an external process invocation via `subprocess.run` since the script content is not supplied. 
     # REVIEW-STRUCT: launcher k_historization_load.ksh invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

5. EMBEDDED SQL
   - None directly present in this wrapper script.

6. CONTROL FLOW
   1. Set shell error execution option (`set -e`).
   2. Establish parameter defaults (`CRM_HOME`).
   3. Define utility function `log()` to write timestamped messages to standard output.
   4. Log the initiation of SCD2 historization for `${RUN_DATE}`.
   5. Source and execute `${CRM_HOME}/customer/k_historization_load.ksh`.
   6. Capture the exit status code (`rc`).
   7. Conditional check: if execution failed (`rc != 0`), log a formatted error message and exit with the captured `rc`.
   8. If successful, log the completion of the historization load and exit 0.

7. ERROR HANDLING & EXIT CODES
   - Installs `set -e` to abort on immediate command failures.
   - Captures status code of the sourced script via `rc=$?`.
   - Checks if `[ ${rc} -ne 0 ]`. If true, emits an explicit log entry `ERROR: k_historization_load.ksh failed with rc=${rc}` and exits with `exit ${rc}`.
   - Successful execution returns `exit 0`.
   - Python mapping: Map execution of external script to `subprocess.run(..., check=True)`. Catch `subprocess.CalledProcessError` to safely capture the exit code, write the error log, and exit using `sys.exit(e.returncode)`.

8. OUTPUTS / SIDE EFFECTS
   - Writes timestamped log records to standard output (stdout).
   - Execution of `k_historization_load.ksh` (which performs SCD2 historization merge operations, modifying database tables or flat files).

9. BUSINESS SUMMARY
   - Coordinates and executes the core customer SCD2 historization process.
   - Ensures runtime logging captures startup and completion timestamps for operational tracking.
   - Explicitly logs failure states when the underlying historization load crashes.
   - Returns precise exit codes back to the UC4/Automic scheduler to manage job dependencies.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
from datetime import datetime

# Step 1: Initialize environment variables and defaults
# Map environment parameters to Python equivalents
CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
RUN_DATE = os.environ.get("RUN_DATE", "")

# Step 2: Define standard logging utility
def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Step 3: Log initiation of process
log(f"Starting SCD2 historization for run date {RUN_DATE}")

# Step 4: Resolve path to underlying script
# # REVIEW-STRUCT: environment file k_historization_load.ksh not supplied — variables it sets are unknown; do not guess their names or values
script_to_run = os.path.join(CRM_HOME, "customer", "k_historization_load.ksh")

# Step 5: Execute the sourced script and monitor status
try:
    # Sourcing k_historization_load.ksh is simulated by subprocess execution.
    # Current environment (os.environ) is forwarded to preserve context variables.
    result = subprocess.run([script_to_run], shell=True, env=os.environ, check=True)
    rc = result.returncode
except subprocess.CalledProcessError as e:
    rc = e.returncode
    # Step 6: Conditional error handling for non-zero execution statuses
    log(f"ERROR: k_historization_load.ksh failed with rc={rc}")
    sys.exit(rc)
except Exception as ex:
    # Handle unexpected failures launching the process
    log(f"ERROR: Failed to launch historization script: {str(ex)}")
    sys.exit(1)

# Step 7: Log completion and exit successfully
log(f"Historization load completed for {RUN_DATE}")
sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/r_historization_load.ksh` | `customer/r_historization_load.py` | Migrated from a KornShell wrapper to a Python script that preserves logging, error propagation, and execution logic of the historization load. |

---

### Job Dependencies
* **Downstream Jobs:**
  * `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated): This job consumes the output of the historization load. Because it is not yet migrated, the final cross-DAG wiring (e.g., Airflow `ExternalTaskSensor` or event-based triggers) on Google Cloud Composer / BigQuery cannot be fully established. 

---

### Execution Order
The legacy job execution order consists of the following steps:
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 Orchestration)
2. `customer/r_historization_load.ksh` (Wrapper / Execution script)
3. `customer/k_historization_load.ksh` (SCD2 Historization core load script)
4. `customer/d_historization_load.sql` (Historization DB Merge SQL)
5. `customer/d_segment_quality_check.sql` (Segment Quality Check SQL)

In the target environment (Cloud Composer/Airflow), this task ordering must be preserved:
* The orchestration DAG (replacing the UC4 XML) will execute the Python task for `customer/r_historization_load.py`, which in turn triggers/coordinates the execution of the migrated `k_historization_load` module.

---

### Schedule & Variables
* **Scheduling:**
  * This job is not directly triggered by any of the run's primary schedulers; it is designed to run inside scheduled parent jobs (as an included/shared module). 
  * On Cloud Composer, the migrated Python module must remain a callable/importable DAG task or a reusable task block rather than an independently scheduled DAG.
* **Scheduler-Set Variables:**
  * `RUN_DATE` (derived from UC4 parameter `&$TODAY`): Must be injected dynamically into the script at runtime using Cloud Composer's parameterization or Airflow's execution date context macro `{{ ds }}`.

---

### Lineage
* **Internal Lineage:**
  * `customer/r_historization_load.ksh` invokes `customer/k_historization_load.ksh`. On the target platform, the migrated `customer/r_historization_load.py` will execute the Python equivalent of `k_historization_load.ksh` or run it as a subprocess.

---

### Cross-File Dependencies
* **Sourced Scripts:**
  * The wrapper depends directly on the existence and interface of `customer/k_historization_load.ksh`. Any variables, return codes, and system-level operations exported or performed by `k_historization_load` must align with how `r_historization_load.py` captures and reacts to its execution status.

---

### Target File Plan

* **`customer/r_historization_load.py`**
  * **Language:** Python
  * **Source File:** `customer/r_historization_load.ksh`

---

### Environment-Specific Values

1. **GLOBAL (Environment-wide)**
   * **`CRM_HOME`**
     * *Target Mapping:* Environment-wide execution base path. Sourced from the Airflow environment configuration or system env.
     * *Resolution:* Read at runtime via `os.environ.get("CRM_HOME", "/opt/etl/customer")`.

2. **JOB-SPECIFIC**
   * **`RUN_DATE`**
     * *Target Mapping:* Execution/business execution date.
     * *Resolution:* Sourced from Airflow task execution parameter context (e.g., `{{ ds }}` or `params` object), passing it dynamically into the execution script environment.

---

### Risks and Manual Steps
* **Downstream Integration Risk:** The downstream consumer `CUSTOMER.WEEKLY_SCHEDULE` has not been migrated yet. Orchestration wiring (such as Airflow DAG sensors or event triggers) cannot be finalized or verified end-to-end until that job is migrated.
* **Sourced Script Interface Boundary:** `customer/k_historization_load.ksh` belongs to a different file group and is not covered under this design pass. Manual validation is required to ensure that the variables passed dynamically through the shell environment in legacy (via `. k_historization_load.ksh`) are properly resolved and decoupled as explicit parameters in the migrated Python version.