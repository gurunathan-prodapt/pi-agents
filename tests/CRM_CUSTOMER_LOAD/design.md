=== FILE: customer/process_customer_data.ksh ===
#!/bin/ksh
# =============================================================================
# Script  : process_customer_data.ksh
# Purpose : Orchestrates the weekly CRM customer data load:
#           1. Extracts customer profiles from CRM source
#           2. Runs SQL*Plus segment extract
#           3. Calls PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD
#           4. Invokes Python customer scoring
#           Waits on upstream events: FINANCE_GL_CLOSE_COMPLETE,
#           RETAIL_DAILY_COMPLETE (from finance/ and sales/ pipelines).
#
# Usage   : process_customer_data.ksh <RUN_DATE> <SEGMENT> [FORCE_RELOAD]
# Example : process_customer_data.ksh 2024-01-15 ALL N
#
# Called by: UC4 job CRM_CUSTOMER_EXTRACT_VIP/RETAIL/WHOLESALE
# =============================================================================

set -u

RUN_DATE=${1:?"RUN_DATE (YYYY-MM-DD) required"}
CUSTOMER_SEGMENT=${2:-"ALL"}
FORCE_RELOAD=${3:-"N"}

# ----------------------------------------------------------------------------
# Load shared library (retry_handler, wait_for_event, log_job_audit)
# lib/retry_handler.ksh provides retry_command(), wait_for_event(), etc.
# ----------------------------------------------------------------------------
. "${ETL_LIB_DIR}/retry_handler.ksh"

ENV_CONFIG="${ENV_CONFIG_DIR:-/opt/etl/config}/env_crm.properties"
. "$ENV_CONFIG"

RUN_DATE_FMT=$(echo $RUN_DATE | tr '-' '')
LOG_DIR="${LOG_DIR:-/opt/etl/logs/crm}"
LOG_FILE="${LOG_DIR}/crm_load_${CUSTOMER_SEGMENT}_${RUN_DATE_FMT}_$(date '+%H%M%S').log"
ORA_CONNECT="${DB_USER}/${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_SID}"

export RUN_DATE CUSTOMER_SEGMENT FORCE_RELOAD RUN_DATE_FMT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${CUSTOMER_SEGMENT}] $1" | tee -a "$LOG_FILE"; }

log "=== process_customer_data.ksh START ==="
log "RUN_DATE=$RUN_DATE  SEGMENT=$CUSTOMER_SEGMENT  FORCE=$FORCE_RELOAD"

# ----------------------------------------------------------------------------
# 1. Wait for upstream event: Finance GL Close must complete before we can
#    access FACT_PERIOD_RECONCILIATION which feeds our scoring model.
#    wait_for_event() is in lib/retry_handler.ksh
# ----------------------------------------------------------------------------
log "Waiting for FINANCE_GL_CLOSE_COMPLETE event..."
wait_for_event "FINANCE_GL_CLOSE_COMPLETE" "$RUN_DATE"
if [ $? -ne 0 ]; then
    log "ERROR: FINANCE_GL_CLOSE_COMPLETE did not complete. Aborting."
    exit 1
fi
log "FINANCE_GL_CLOSE_COMPLETE received."

# ----------------------------------------------------------------------------
# 2. Wait for RETAIL_DAILY_COMPLETE - needed for DW_OWNER.STG_CUSTOMER_SALES
# ----------------------------------------------------------------------------
log "Waiting for RETAIL_DAILY_COMPLETE event..."
wait_for_event "RETAIL_DAILY_COMPLETE" "$RUN_DATE"
if [ $? -ne 0 ]; then
    log "WARN: RETAIL_DAILY_COMPLETE timed out. Proceeding with available data."
fi
log "RETAIL_DAILY_COMPLETE received (or timed out)."

# ----------------------------------------------------------------------------
# 3. SQL*Plus customer segment extract
# ----------------------------------------------------------------------------
log "Step 1: Running customer segment extract via SQL*Plus..."

sqlplus -s "$ORA_CONNECT" <<SQLPLUS_EOF > "${LOG_DIR}/sqlplus_crm_extract_${RUN_DATE_FMT}.log" 2>&1
    DEFINE RUN_DATE          = '$RUN_DATE'
    DEFINE CUSTOMER_SEGMENT  = '$CUSTOMER_SEGMENT'
    DEFINE BATCH_SIZE        = '${BATCH_SIZE:-5000}'
    DEFINE REGION_CODE       = '${REGION_CODE:-ALL}'
    DEFINE RUN_DATE_FMT      = '$RUN_DATE_FMT'

    @${SQLPLUS_DIR}/customer_segment_extract.sql
    EXIT SQL.SQLCODE;
SQLPLUS_EOF

SQLPLUS_RC=$?
if [ $SQLPLUS_RC -ne 0 ]; then
    log "ERROR: SQL*Plus extract failed (rc=$SQLPLUS_RC)"
    exit 2
fi
log "Step 1: SQL*Plus extract complete."

# ----------------------------------------------------------------------------
# 4. Validate staging counts
# ----------------------------------------------------------------------------
STG_COUNT=$(sqlplus -s "$ORA_CONNECT" <<COUNT_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM STG_CUSTOMER_PROFILE
    WHERE LOAD_DATE  = TO_DATE('$RUN_DATE','YYYY-MM-DD')
    AND   ETL_STATUS = 'PENDING';
    EXIT;
COUNT_EOF
)
STG_COUNT=$(echo $STG_COUNT | tr -d ' ')
log "STG_CUSTOMER_PROFILE PENDING rows: $STG_COUNT"

if [ -z "$STG_COUNT" ] || [ "$STG_COUNT" -eq 0 ]; then
    log "WARN: No customer staging rows for $RUN_DATE"
    if [ "$FORCE_RELOAD" != "Y" ]; then
        log "Exiting - no data to process (use FORCE_RELOAD=Y to override)."
        exit 0
    fi
fi

# ----------------------------------------------------------------------------
# 5. PL/SQL Master Load
# ----------------------------------------------------------------------------
log "Step 2: Running PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD..."

sqlplus -s "$ORA_CONNECT" <<PLSQL_EOF >> "$LOG_FILE" 2>&1
    SET SERVEROUTPUT ON SIZE UNLIMITED FEEDBACK OFF
    BEGIN
        PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD(
            p_run_date  => TO_DATE('$RUN_DATE','YYYY-MM-DD'),
            p_segment   => '$CUSTOMER_SEGMENT'
        );
    END;
    /
    EXIT SQL.SQLCODE;
PLSQL_EOF

PLSQL_RC=$?
if [ $PLSQL_RC -ne 0 ]; then
    log "ERROR: PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD failed (rc=$PLSQL_RC)"
    exit 3
fi
log "Step 2: MASTER_CRM_LOAD complete."

# ----------------------------------------------------------------------------
# 6. Python customer scoring
# ----------------------------------------------------------------------------
log "Step 3: Running Python customer scoring..."

python3 ${PYTHON_DIR}/customer_scoring.py \
    --run-date "$RUN_DATE" \
    --segment  "$CUSTOMER_SEGMENT" \
    --env      "${ETL_ENV:-PROD}" \
    >> "$LOG_FILE" 2>&1

PYTHON_RC=$?
if [ $PYTHON_RC -ne 0 ]; then
    log "WARN: Python scoring returned non-zero (rc=$PYTHON_RC) - non-fatal"
fi
log "Step 3: Python scoring complete (rc=$PYTHON_RC)."

# ----------------------------------------------------------------------------
# 7. Audit log
# ----------------------------------------------------------------------------
log_job_audit "CRM_CUSTOMER_LOAD" "$RUN_DATE" "SUCCESS" "$STG_COUNT"

log "=== process_customer_data.ksh COMPLETED: $CUSTOMER_SEGMENT / $RUN_DATE ==="
exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains substantial business logic and orchestration beyond a pure wrapper, including event waits, SQL*Plus/PLSQL execution, validation, and conditional branching, so it must be converted.

EVIDENCE
- Business logic found: both — the KSH custom logic waits on upstream events, validates staging counts, conditionally exits, and orchestrates multiple processing steps; the UC4 invocation body was not supplied, so no additional logic could be confirmed there.
- AWK: none
- SQL-expressible: partly — the SQL*Plus count query and PL/SQL call are database-executable, but the event waits, shell control flow, and Python launcher are not fully expressible as BigQuery SQL.
- Non-SQL side effects: file logging, sourcing environment files, waiting on external events, invoking SQL*Plus, invoking Python, and audit logging.
- Against this verdict: the SQL portions themselves are database-centric, but the surrounding orchestration and external process calls make Python the safer and correct target.

1. SCRIPT OVERVIEW
This script orchestrates a weekly CRM customer data load. It waits for upstream finance and retail completion events, runs an Oracle SQL*Plus extract, checks staging row counts, executes a PL/SQL master load procedure, and then launches a Python scoring step. It also writes operational logs and records an audit entry for the job run.

2. INVOCATION CONTEXT
- Called by UC4 job CRM_CUSTOMER_EXTRACT_VIP/RETAIL/WHOLESALE
- Command line / arguments:
  - process_customer_data.ksh <RUN_DATE> <SEGMENT> [FORCE_RELOAD]
  - Example: process_customer_data.ksh 2024-01-15 ALL N
- UC4 native includes: none supplied in the extraction
- Environment files sourced:
  - . "${ETL_LIB_DIR}/retry_handler.ksh"
  - . "$ENV_CONFIG" where ENV_CONFIG="${ENV_CONFIG_DIR:-/opt/etl/config}/env_crm.properties"

3. PARAMETERS / INPUTS
- RUN_DATE
  - Source: positional $1 / UC4 job argument
  - Used: yes
  - Python surface: sys.argv or argparse
- CUSTOMER_SEGMENT
  - Source: positional $2 / UC4 job argument
  - Used: yes
  - Python surface: sys.argv or argparse
- FORCE_RELOAD
  - Source: positional $3 / UC4 job argument
  - Used: yes
  - Python surface: sys.argv or argparse
- ETL_LIB_DIR
  - Source: environment variable
  - Used: yes, for sourcing retry_handler.ksh
  - Python surface: os.environ.get(...)
- ENV_CONFIG_DIR
  - Source: environment variable
  - Used: yes, to build ENV_CONFIG path
  - Python surface: os.environ.get(...)
- ENV_CONFIG
  - Source: derived environment variable/path
  - Used: yes, sourced as config file
  - Python surface: os.environ.get(...) or computed path
- LOG_DIR
  - Source: environment variable with default /opt/etl/logs/crm
  - Used: yes
  - Python surface: os.environ.get(...)
- DB_USER
  - Source: sourced config file
  - Used: yes, for Oracle connection string
  - Python surface: os.environ.get(...)
- DB_PASS
  - Source: sourced config file
  - Used: yes, for Oracle connection string
  - Python surface: os.environ.get(...)
- DB_HOST
  - Source: sourced config file
  - Used: yes, for Oracle connection string
  - Python surface: os.environ.get(...)
- DB_PORT
  - Source: sourced config file
  - Used: yes, for Oracle connection string
  - Python surface: os.environ.get(...)
- DB_SID
  - Source: sourced config file
  - Used: yes, for Oracle connection string
  - Python surface: os.environ.get(...)
- BATCH_SIZE
  - Source: environment variable with default 5000
  - Used: yes, passed into SQL*Plus DEFINE
  - Python surface: os.environ.get(...)
- REGION_CODE
  - Source: environment variable with default ALL
  - Used: yes, passed into SQL*Plus DEFINE
  - Python surface: os.environ.get(...)
- RUN_DATE_FMT
  - Source: computed from RUN_DATE
  - Used: yes
  - Python surface: computed value
- ETL_ENV
  - Source: environment variable with default PROD
  - Used: yes, passed to Python scoring
  - Python surface: os.environ.get(...)
- PYTHON_DIR
  - Source: environment variable
  - Used: yes, for Python scoring script path
  - Python surface: os.environ.get(...)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- Exact command line:
  - . "${ETL_LIB_DIR}/retry_handler.ksh"
  - Purpose: source shared retry/event/audit helper functions
  - Should become native Python DB-client call: no, this is a shell library source
  - RESOLVABLE LAUNCHER: no, source body not supplied
  - # REVIEW-STRUCT: environment file [${ETL_LIB_DIR}/retry_handler.ksh] not supplied — variables/functions it sets are unknown; do not guess their names or values
- Exact command line:
  - . "$ENV_CONFIG"
  - Purpose: source CRM environment properties
  - Should become native Python DB-client call: no, this is a config source
  - RESOLVABLE LAUNCHER: no
  - # REVIEW-STRUCT: environment file [$ENV_CONFIG] not supplied — variables it sets are unknown; do not guess their names or values
- Exact command line:
  - wait_for_event "FINANCE_GL_CLOSE_COMPLETE" "$RUN_DATE"
  - Purpose: wait for upstream finance completion event
  - Should become native Python DB-client call: no, helper function from sourced library
  - RESOLVABLE LAUNCHER: no, function body not supplied
  - # REVIEW-STRUCT: launcher wait_for_event invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
- Exact command line:
  - wait_for_event "RETAIL_DAILY_COMPLETE" "$RUN_DATE"
  - Purpose: wait for upstream retail completion event
  - Should become native Python DB-client call: no, helper function from sourced library
  - RESOLVABLE LAUNCHER: no
  - # REVIEW-STRUCT: launcher wait_for_event invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
- Exact command line:
  - sqlplus -s "$ORA_CONNECT" <<SQLPLUS_EOF > "${LOG_DIR}/sqlplus_crm_extract_${RUN_DATE_FMT}.log" 2>&1
    DEFINE RUN_DATE          = '$RUN_DATE'
    DEFINE CUSTOMER_SEGMENT  = '$CUSTOMER_SEGMENT'
    DEFINE BATCH_SIZE        = '${BATCH_SIZE:-5000}'
    DEFINE REGION_CODE       = '${REGION_CODE:-ALL}'
    DEFINE RUN_DATE_FMT      = '$RUN_DATE_FMT'

    @${SQLPLUS_DIR}/customer_segment_extract.sql
    EXIT SQL.SQLCODE;
SQLPLUS_EOF
  - Purpose: run Oracle SQL*Plus customer segment extract
  - Should become native Python DB-client call: yes, if the referenced SQL file is a pure SQL script
  - RESOLVABLE LAUNCHER: not fully resolvable from this extraction because the referenced SQL file body is not supplied
  - # REVIEW-STRUCT: launcher sqlplus invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
- Exact command line:
  - sqlplus -s "$ORA_CONNECT" <<COUNT_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM STG_CUSTOMER_PROFILE
    WHERE LOAD_DATE  = TO_DATE('$RUN_DATE','YYYY-MM-DD')
    AND   ETL_STATUS = 'PENDING';
    EXIT;
COUNT_EOF
  - Purpose: count pending staging rows
  - Should become native Python DB-client call: yes
  - RESOLVABLE LAUNCHER: yes, direct SQL is visible and Oracle dialect is clear
- Exact command line:
  - sqlplus -s "$ORA_CONNECT" <<PLSQL_EOF >> "$LOG_FILE" 2>&1
    SET SERVEROUTPUT ON SIZE UNLIMITED FEEDBACK OFF
    BEGIN
        PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD(
            p_run_date  => TO_DATE('$RUN_DATE','YYYY-MM-DD'),
            p_segment   => '$CUSTOMER_SEGMENT'
        );
    END;
    /
    EXIT SQL.SQLCODE;
PLSQL_EOF
  - Purpose: execute Oracle PL/SQL master load procedure
  - Should become native Python DB-client call: yes, via Oracle driver executing anonymous PL/SQL
  - RESOLVABLE LAUNCHER: yes, direct PL/SQL is visible and Oracle dialect is clear
- Exact command line:
  - python3 ${PYTHON_DIR}/customer_scoring.py \
    --run-date "$RUN_DATE" \
    --segment  "$CUSTOMER_SEGMENT" \
    --env      "${ETL_ENV:-PROD}" \
    >> "$LOG_FILE" 2>&1
  - Purpose: run customer scoring
  - Should become native Python DB-client call: no, external Python program invocation
  - RESOLVABLE LAUNCHER: no, custom script body not supplied
  - # REVIEW-STRUCT: launcher customer_scoring.py invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
- Exact command line:
  - log_job_audit "CRM_CUSTOMER_LOAD" "$RUN_DATE" "SUCCESS" "$STG_COUNT"
  - Purpose: write audit record
  - Should become native Python DB-client call: no, helper function from sourced library
  - RESOLVABLE LAUNCHER: no
  - # REVIEW-STRUCT: launcher log_job_audit invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
- Source file / label: SQL*Plus COUNT_EOF block
  - Full SQL text exactly as extracted:
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM STG_CUSTOMER_PROFILE
    WHERE LOAD_DATE  = TO_DATE('$RUN_DATE','YYYY-MM-DD')
    AND   ETL_STATUS = 'PENDING';
    EXIT;
  - Statement type: SELECT
  - Tables touched: STG_CUSTOMER_PROFILE
  - Dialect identifiable: yes, Oracle SQL*Plus directives are present
- Source file / label: PLSQL_EOF block
  - Full SQL text exactly as extracted:
    SET SERVEROUTPUT ON SIZE UNLIMITED FEEDBACK OFF
    BEGIN
        PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD(
            p_run_date  => TO_DATE('$RUN_DATE','YYYY-MM-DD'),
            p_segment   => '$CUSTOMER_SEGMENT'
        );
    END;
    /
    EXIT SQL.SQLCODE;
  - Statement type: PL/SQL block / stored-proc call
  - Tables touched: not explicit in the block; procedure body unknown
  - Dialect identifiable: yes, Oracle PL/SQL with SQL*Plus block terminator `/`
- Source file / label: @${SQLPLUS_DIR}/customer_segment_extract.sql
  - Full SQL text exactly as extracted: not supplied in the extraction
  - Statement type: unknown
  - Tables touched: unknown
  - Dialect identifiable: unknown
  - # REVIEW-STRUCT: referenced SQL file body not supplied — behaviour unknown

6. CONTROL FLOW
1. Set shell options with `set -u` to fail on unset variables.
2. Read RUN_DATE from $1 and require it; default CUSTOMER_SEGMENT to ALL and FORCE_RELOAD to N.
3. Source shared retry/event/audit helper library from `${ETL_LIB_DIR}/retry_handler.ksh`.
4. Build ENV_CONFIG path from `${ENV_CONFIG_DIR:-/opt/etl/config}/env_crm.properties`.
5. Source the CRM environment config file.
6. Compute RUN_DATE_FMT by stripping hyphens from RUN_DATE.
7. Set LOG_DIR default and build LOG_FILE with segment, date, and timestamp.
8. Build ORA_CONNECT from DB_USER/DB_PASS/DB_HOST/DB_PORT/DB_SID.
9. Export RUN_DATE, CUSTOMER_SEGMENT, FORCE_RELOAD, RUN_DATE_FMT.
10. Define log() helper function to append timestamped messages to LOG_FILE.
11. Log script start and input parameters.
12. Call wait_for_event for FINANCE_GL_CLOSE_COMPLETE and abort with exit 1 if it fails.
13. Call wait_for_event for RETAIL_DAILY_COMPLETE and continue even if it times out.
14. Run SQL*Plus customer segment extract, capturing output to a dedicated log file.
15. Check SQL*Plus return code and exit 2 on failure.
16. Run SQL*Plus count query to determine pending staging rows.
17. Normalize STG_COUNT by stripping spaces from SQL*Plus output.
18. Log the staging count.
19. If STG_COUNT is empty or zero, warn; if FORCE_RELOAD is not Y, exit 0.
20. Run PL/SQL master load via SQL*Plus.
21. Check PL/SQL return code and exit 3 on failure.
22. Run Python customer scoring script and append output to LOG_FILE.
23. Check Python return code; if non-zero, warn but do not fail the job.
24. Write audit record via log_job_audit.
25. Log completion message and exit 0.

7. ERROR HANDLING & EXIT CODES
- Failure detection:
  - Explicit `$?` checks after wait_for_event, SQL*Plus extract, PL/SQL load, and Python scoring
  - `set -u` causes failure on unset variables
- Failure actions:
  - FINANCE_GL_CLOSE_COMPLETE failure: log error and exit 1
  - SQL*Plus extract failure: log error and exit 2
  - MASTER_CRM_LOAD failure: log error and exit 3
  - Python scoring failure: log warning only, continue
  - No staging rows and FORCE_RELOAD != Y: exit 0
- Success exit code convention:
  - 0 on normal completion
- Python mapping:
  - Use try/except around subprocess.run(..., check=True) for external commands
  - Use DB-driver exceptions for SQL/PLSQL execution
  - Use sys.exit(1/2/3) for the distinct failure codes
  - Preserve non-fatal scoring failure as logged warning only

8. OUTPUTS / SIDE EFFECTS
- `${LOG_DIR}/crm_load_${CUSTOMER_SEGMENT}_${RUN_DATE_FMT}_$(date '+%H%M%S').log`
- `${LOG_DIR}/sqlplus_crm_extract_${RUN_DATE_FMT}.log`
- Oracle database access via SQL*Plus and PL/SQL procedure execution
- Audit log entry via `log_job_audit`
- Potential downstream effects from `PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD`
- Potential downstream effects from `customer_scoring.py`

9. BUSINESS SUMMARY
- Waits for upstream finance and retail pipeline completion before processing CRM data.
- Extracts customer segment data from Oracle using SQL*Plus.
- Verifies that staging data exists before proceeding, unless forced.
- Runs a PL/SQL master load procedure to populate historical/customer load structures.
- Launches a Python scoring step to enrich or score customers.
- Records an audit trail for the CRM customer load run.

# Step 1: Parse required and optional arguments
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("RUN_DATE")
parser.add_argument("CUSTOMER_SEGMENT", nargs="?", default="ALL")
parser.add_argument("FORCE_RELOAD", nargs="?", default="N")
args = parser.parse_args()

# Step 2: Load environment variables and config file paths
import os
etl_lib_dir = os.environ["ETL_LIB_DIR"]
env_config_dir = os.environ.get("ENV_CONFIG_DIR", "/opt/etl/config")
env_config = os.path.join(env_config_dir, "env_crm.properties")

# Step 3: Source shared library and environment config
# REVIEW-STRUCT: environment file [${ETL_LIB_DIR}/retry_handler.ksh] not supplied — variables/functions it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [$ENV_CONFIG] not supplied — variables it sets are unknown; do not guess their names or values

# Step 4: Compute derived values and logging paths
run_date_fmt = args.RUN_DATE.replace("-", "")
log_dir = os.environ.get("LOG_DIR", "/opt/etl/logs/crm")
log_file = os.path.join(
    log_dir,
    f"crm_load_{args.CUSTOMER_SEGMENT}_{run_date_fmt}_{datetime.now().strftime('%H%M%S')}.log",
)

# Step 5: Build Oracle connection string from environment
db_user = os.environ["DB_USER"]
db_pass = os.environ["DB_PASS"]
db_host = os.environ["DB_HOST"]
db_port = os.environ["DB_PORT"]
db_sid = os.environ["DB_SID"]
ora_connect = f"{db_user}/{db_pass}@{db_host}:{db_port}/{db_sid}"

# Step 6: Wait for upstream finance event
# REVIEW-STRUCT: launcher wait_for_event invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
wait_for_event("FINANCE_GL_CLOSE_COMPLETE", args.RUN_DATE)

# Step 7: Wait for upstream retail event, but continue on timeout/failure
# REVIEW-STRUCT: launcher wait_for_event invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
try:
    wait_for_event("RETAIL_DAILY_COMPLETE", args.RUN_DATE)
except Exception:
    pass

# Step 8: Run SQL*Plus customer segment extract
# REVIEW-STRUCT: launcher sqlplus invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
subprocess.run(
    ["sqlplus", "-s", ora_connect],
    input=sqlplus_extract_script_text,
    text=True,
    check=True,
)

# Step 9: Query staging count
with oracle_connection.cursor() as cursor:
    cursor.execute(
        "SELECT COUNT(*) FROM STG_CUSTOMER_PROFILE WHERE LOAD_DATE = TO_DATE(:run_date,'YYYY-MM-DD') AND ETL_STATUS = 'PENDING'",
        run_date=args.RUN_DATE,
    )
    stg_count = cursor.fetchone()[0]

# Step 10: Exit early if no rows and not forced
if not stg_count and args.FORCE_RELOAD != "Y":
    sys.exit(0)

# Step 11: Run PL/SQL master load
with oracle_connection.cursor() as cursor:
    cursor.execute(
        """
        BEGIN
            PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD(
                p_run_date => TO_DATE(:run_date,'YYYY-MM-DD'),
                p_segment  => :segment
            );
        END;
        """,
        run_date=args.RUN_DATE,
        segment=args.CUSTOMER_SEGMENT,
    )

# Step 12: Run Python customer scoring script
subprocess.run(
    [
        "python3",
        os.path.join(os.environ["PYTHON_DIR"], "customer_scoring.py"),
        "--run-date",
        args.RUN_DATE,
        "--segment",
        args.CUSTOMER_SEGMENT,
        "--env",
        os.environ.get("ETL_ENV", "PROD"),
    ],
    check=False,
)

# Step 13: Write audit record
# REVIEW-STRUCT: launcher log_job_audit invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
log_job_audit("CRM_CUSTOMER_LOAD", args.RUN_DATE, "SUCCESS", stg_count)

# Step 14: Exit successfully
sys.exit(0)

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/process_customer_data.ksh` | `customer/process_customer_data.py` | Translates the KornShell orchestration, event polling, database execution, and subprocess coordination into clean, maintainable Python 3, leveraging standard Google Cloud APIs. |

---

### Execution Order
The legacy orchestration defines a sequence of two logical steps. The target orchestration in Python or Cloud Composer must preserve this execution sequence:
1. **customer/process_customer_data.py**: Runs the core weekly CRM customer data load orchestration (event waiting, segment extract, staging validation, PL/SQL load, and Python customer scoring).
2. **lib/retry_handler** (Shared Utility / Module): Sourced utility functions (`wait_for_event`, `retry_command`, etc.) are modularized into a shared Python module (e.g., `etl_lib.retry_handler`) and imported directly by `customer/process_customer_data.py` instead of executing as a sequential standalone job step.

---

### Lineage
* **Upstream Producers (Reads)**:
  * Reads/Validates data in the legacy staging table: `TABLE:STG_CUSTOMER_PROFILE` (Oracle-based staging table, mapped to a Google BigQuery staging dataset).
* **Downstream Consumers / Utility Invocations**:
  * Invokes `FILE:lib/retry_handler.ksh` (shared utility library, handled by its own independent design and migration pass; will be accessed via a Python import/module in the target environment).

---

### External System Replacements
* **Oracle SQL*Plus Execution (`sqlplus`)** -> **Google BigQuery Client API / Stored Procedures**:
  * Legacy database-side extract and validation queries run on the Oracle DB connection (`ORA_CONNECT`) are replaced with native BigQuery SQL execution via the Python BigQuery Client (`google-cloud-bigquery`).
* **Oracle PL/SQL (`PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD`)** -> **BigQuery Stored Procedure**:
  * The PL/SQL master load procedure is called natively from Python as a BigQuery stored procedure invocation.
* **On-Premise File Logging** -> **Google Cloud Logging**:
  * Local file appending in `/opt/etl/logs/crm` is replaced with standard Python logging integrated with GCP Cloud Logging to capture operational steps and execution status.

---

### Cross-File Dependencies
* **`lib/retry_handler.ksh` (Shared Utility)**:
  * The script relies heavily on functions defined in `retry_handler.ksh` (e.g., `wait_for_event`, `log_job_audit`). In the target architecture, these utilities must be imported from a centralized, migrated Python library (`from etl_lib.retry_handler import wait_for_event, log_job_audit`).
* **`${SQLPLUS_DIR}/customer_segment_extract.sql` (External SQL Script)**:
  * The script calls this local SQL file. During migration, this file must be transpiled to BigQuery SQL dialect and located in a structured BigQuery SQL directory, or loaded and run via Python's BigQuery Client.
* **`${PYTHON_DIR}/customer_scoring.py` (Downstream Python Execution)**:
  * The script runs this scoring script in place as a subprocess. In Google Cloud, this scoring program should be packaged alongside the orchestration code or invoked on a scalable platform (such as a Dataproc Serverless job or GKE) depending on compute size.
* **`env_crm.properties` (Environment Configuration)**:
  * Contains legacy variables and database connection parameters. In the target environment, these configurations are managed via Google Cloud Secrets Manager or Airflow Variables/Environment Configs.

---

### Target File Plan
* **Target File**: `customer/process_customer_data.py`
  * **Language**: Python 3
  * **Source File**: `customer/process_customer_data.ksh`
  * **Purpose**: Orchestrates the weekly CRM customer load. It leverages `argparse` for parameter handling (`RUN_DATE`, `CUSTOMER_SEGMENT`, `FORCE_RELOAD`), interfaces with BigQuery for extraction and staging counts, invokes BigQuery Stored Procedures for data historization, and triggers the customer scoring Python program.

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-wide configuration)
* **`GCP_PROJECT`**: The target GCP Project ID where the BigQuery datasets and scoring infrastructure reside.
* **`GCP_REGION`**: The GCP execution region (e.g., `us-east1` or `europe-west3`).
* **`BQ_DATASET`**: The target BigQuery dataset containing the staging and historical customer tables (replacing legacy schemas).
* **`ETL_ENV`**: The stage environment parameter (defaulting to `PROD`), passed directly to downstream processes.

#### 2. JOB-SPECIFIC (Variables unique to this job orchestration)
* **`LOG_DIR`**: The job-specific directory or bucket prefix for logs (mapped to a GCS bucket path or GCP Cloud Logging group, replacing local `/opt/etl/logs/crm`).
* **`BATCH_SIZE`**: The extraction batch size parameter (defaulting to `5000`).
* **`REGION_CODE`**: Job segment filter parameter (defaulting to `ALL`).
* **`SQLPLUS_DIR`**: Local path or GCS path where SQL query files (like `customer_segment_extract.sql`) are stored.
* **`PYTHON_DIR`**: Location of the `customer_scoring.py` script.

---

### Risks and Manual Steps
* **Sourced Library Dependency (`lib/retry_handler.ksh`)**:
  * **Risk**: The core functions `wait_for_event`, `retry_command`, and `log_job_audit` are defined in `lib/retry_handler.ksh`, which is outside this design scope.
  * **Action**: Ensure that the Python equivalent (`etl_lib.retry_handler` or Cloud Composer sensor/logging equivalents) has been fully migrated and is available for import before deploying this orchestration script.
* **Transpilation of `customer_segment_extract.sql`**:
  * **Risk**: The query script `customer_segment_extract.sql` is referenced in the legacy code but was not supplied in this scope.
  * **Action**: A developer must manually transpile the Oracle-SQL dialect inside `customer_segment_extract.sql` into Google BigQuery SQL dialect.
* **Stored Procedure Migration (`PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD`)**:
  * **Risk**: The stored procedure contains PL/SQL-specific logic that must be transpiled to BigQuery Stored Procedures.
  * **Action**: This procedure must be migrated and tested in BigQuery before executing `process_customer_data.py`, as the job will fail at Step 2 if the target routine does not exist.
* **External Environment Configuration (`env_crm.properties`)**:
  * **Risk**: Sourced configurations are not supplied in this pass.
  * **Action**: Configure equivalent environment variables, Cloud Composer Variables, or secret entries in Google Cloud Secrets Manager to supply standard pipeline variables securely.

---

=== FILE: lib/retry_handler.ksh ===
#!/bin/ksh
# =============================================================================
# Library : retry_handler.ksh
# Purpose : Shared ETL utility library providing:
#           - retry_command()     : execute with exponential backoff
#           - wait_for_event()    : poll for UC4 event marker file
#           - check_prereq_job()  : verify upstream job completed
#           - log_job_audit()     : write to ETL_JOB_AUDIT table
#
# Usage   : . "${ETL_LIB_DIR}/retry_handler.ksh"
# Sourced by: customer/process_customer_data.ksh
#             (and any other orchestrator scripts needing retry/event logic)
# =============================================================================

# ----------------------------------------------------------------------------
# retry_command <command> [max_retries] [base_wait_sec]
# Executes command with exponential backoff on failure.
# Returns 0 on success, 1 after max_retries failures.
# ----------------------------------------------------------------------------
retry_command() {
    local CMD="$1"
    local MAX_RETRIES=${2:-3}
    local BASE_WAIT=${3:-30}
    local ATTEMPT=0

    while [ $ATTEMPT -lt $MAX_RETRIES ]; do
        ATTEMPT=$((ATTEMPT + 1))
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [retry_command] Attempt $ATTEMPT/$MAX_RETRIES: $CMD"

        eval "$CMD"
        RC=$?

        if [ $RC -eq 0 ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [retry_command] Success on attempt $ATTEMPT."
            return 0
        fi

        if [ $ATTEMPT -lt $MAX_RETRIES ]; then
            WAIT=$((BASE_WAIT * ATTEMPT))
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [retry_command] Failed (rc=$RC). Waiting ${WAIT}s..."
            sleep $WAIT
        fi
    done

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [retry_command] All $MAX_RETRIES attempts failed."
    return 1
}


# ----------------------------------------------------------------------------
# wait_for_event <event_name> <event_value> [max_polls] [poll_interval_sec]
# Polls for UC4 event completion via marker file or UC4 API.
# Event marker files are created at: /opt/etl/events/<EVENT_NAME>_<VALUE>.done
# Returns 0 if event detected within timeout, 1 on timeout.
# ----------------------------------------------------------------------------
wait_for_event() {
    local EVENT_NAME="$1"
    local EVENT_VALUE="$2"
    local MAX_POLLS=${3:-60}
    local POLL_INTERVAL=${4:-60}

    local MARKER_FILE="${ETL_EVENTS_DIR:-/opt/etl/events}/${EVENT_NAME}_${EVENT_VALUE}.done"
    local ATTEMPT=0

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [wait_for_event] Waiting for $EVENT_NAME=$EVENT_VALUE"
    echo "  Marker: $MARKER_FILE"

    while [ $ATTEMPT -lt $MAX_POLLS ]; do
        ATTEMPT=$((ATTEMPT + 1))

        # Check marker file first (fastest, no API call)
        if [ -f "$MARKER_FILE" ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [wait_for_event] Event detected via marker: $MARKER_FILE"
            return 0
        fi

        # Fallback: query UC4 API (if uc4api is available)
        if command -v uc4api >/dev/null 2>&1; then
            UC4_STATUS=$(uc4api check_event "$EVENT_NAME" value="$EVENT_VALUE" 2>/dev/null)
            if [ "$UC4_STATUS" = "COMPLETED" ] || [ "$UC4_STATUS" = "RAISED" ]; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] [wait_for_event] Event confirmed via UC4 API."
                # Create marker file for future lookups
                touch "$MARKER_FILE" 2>/dev/null
                return 0
            fi
        fi

        if [ $ATTEMPT -lt $MAX_POLLS ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [wait_for_event] Poll $ATTEMPT/$MAX_POLLS - not yet. Sleeping ${POLL_INTERVAL}s..."
            sleep $POLL_INTERVAL
        fi
    done

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [wait_for_event] TIMEOUT: $EVENT_NAME=$EVENT_VALUE not detected after $MAX_POLLS polls."
    return 1
}


# ----------------------------------------------------------------------------
# check_prereq_job <job_name> <run_date>
# Checks whether a prerequisite UC4 job completed successfully for the date.
# Queries ETL_JOB_AUDIT table in Oracle.
# Returns 0 if prereq completed, 1 otherwise.
# ----------------------------------------------------------------------------
check_prereq_job() {
    local JOB_NAME="$1"
    local RUN_DATE="$2"

    # Requires ORA_CONNECT to be set in calling script
    if [ -z "${ORA_CONNECT:-}" ]; then
        echo "[check_prereq_job] WARN: ORA_CONNECT not set. Skipping DB check."
        return 0
    fi

    STATUS=$(sqlplus -s "$ORA_CONNECT" <<CHECK_EOF
        SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
        SELECT NVL(MAX(JOB_STATUS), 'NOT_FOUND')
        FROM   ETL_JOB_AUDIT
        WHERE  JOB_NAME  = '$JOB_NAME'
        AND    RUN_DATE   = TO_DATE('$RUN_DATE','YYYY-MM-DD')
        AND    ROWNUM     = 1
        ORDER BY AUDIT_TIMESTAMP DESC;
        EXIT;
CHECK_EOF
    )
    STATUS=$(echo $STATUS | tr -d ' ')

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [check_prereq_job] $JOB_NAME @ $RUN_DATE => $STATUS"

    case "$STATUS" in
        SUCCESS|COMPLETED) return 0 ;;
        *) return 1 ;;
    esac
}


# ----------------------------------------------------------------------------
# log_job_audit <job_name> <run_date> <status> [rows_processed]
# Writes a job completion record to ETL_JOB_AUDIT via MERGE (upsert).
# ----------------------------------------------------------------------------
log_job_audit() {
    local JOB_NAME="$1"
    local RUN_DATE="$2"
    local JOB_STATUS="$3"
    local ROWS_PROCESSED=${4:-0}

    if [ -z "${ORA_CONNECT:-}" ]; then
        echo "[log_job_audit] WARN: ORA_CONNECT not set. Skipping audit write."
        return 0
    fi

    sqlplus -s "$ORA_CONNECT" <<AUDIT_EOF >/dev/null 2>&1
        BEGIN
            MERGE INTO ETL_JOB_AUDIT tgt
            USING (SELECT '$JOB_NAME'      AS JOB_NAME,
                          TO_DATE('$RUN_DATE','YYYY-MM-DD') AS RUN_DATE
                   FROM DUAL) src
            ON (tgt.JOB_NAME = src.JOB_NAME AND tgt.RUN_DATE = src.RUN_DATE)
            WHEN MATCHED THEN UPDATE SET
                tgt.JOB_STATUS      = '$JOB_STATUS',
                tgt.ROWS_PROCESSED  = $ROWS_PROCESSED,
                tgt.AUDIT_TIMESTAMP = SYSDATE,
                tgt.HOST_NAME       = SYS_CONTEXT('USERENV','HOST')
            WHEN NOT MATCHED THEN INSERT (
                AUDIT_ID, JOB_NAME, RUN_DATE, JOB_STATUS,
                ROWS_PROCESSED, AUDIT_TIMESTAMP, HOST_NAME
            ) VALUES (
                ETL_AUDIT_SEQ.NEXTVAL,
                '$JOB_NAME',
                TO_DATE('$RUN_DATE','YYYY-MM-DD'),
                '$JOB_STATUS',
                $ROWS_PROCESSED,
                SYSDATE,
                SYS_CONTEXT('USERENV','HOST')
            );
            COMMIT;
        END;
        /
        EXIT;
AUDIT_EOF

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [log_job_audit] $JOB_NAME / $RUN_DATE => $JOB_STATUS ($ROWS_PROCESSED rows)"
}


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: This library contains multiple shell functions with retry logic, polling, SQL execution, and data-dependent branching, so it is business logic that must be converted.

EVIDENCE
- Business logic found: KSH custom logic only; the file defines reusable functions for exponential-backoff retries, UC4 event polling, prerequisite-job checking, and audit upserts.
- AWK: none
- SQL-expressible: partly; the Oracle SQL in check_prereq_job() and log_job_audit() is SQL-expressible, but the retry/polling control flow and external command orchestration are not pure SQL.
- Non-SQL side effects: sleep, command execution via eval, UC4 API probing via uc4api, file marker creation via touch, and Oracle sqlplus invocation.
- Against this verdict: the SQL portions themselves could be migrated directly to BigQuery/SQL, but the surrounding shell functions require Python control flow and subprocess handling.

1. SCRIPT OVERVIEW
This is a shared ETL utility library, not a standalone job. It provides reusable functions for retrying commands with exponential backoff, waiting for UC4 event completion, checking whether an upstream job completed successfully, and writing audit records to an Oracle ETL_JOB_AUDIT table. It is sourced by orchestrator scripts such as customer/process_customer_data.ksh.

2. INVOCATION CONTEXT
- Who calls this script: sourced by customer/process_customer_data.ksh; no UC4 JOBS_UNIX object or job name is provided in the extraction for this library itself.
- UC4 native includes: none observed.
- Environment files sourced: none observed.

3. PARAMETERS / INPUTS
- ETL_LIB_DIR: referenced in the usage comment as the directory containing this library; source is calling-script environment; not directly used in executable code here; surface in Python as os.environ.get("ETL_LIB_DIR") if needed by the caller.
- ETL_EVENTS_DIR: source environment variable; used in wait_for_event() to build the marker-file path; surface in Python as os.environ.get("ETL_EVENTS_DIR", "/opt/etl/events").
- ORA_CONNECT: source environment variable; used in check_prereq_job() and log_job_audit(); surface in Python as os.environ.get("ORA_CONNECT").
- retry_command() parameters:
  - CMD: positional argument $1; used; surface in Python as a function argument.
  - MAX_RETRIES: positional argument $2 with default 3; used; surface in Python as a function argument with default.
  - BASE_WAIT: positional argument $3 with default 30; used; surface in Python as a function argument with default.
- wait_for_event() parameters:
  - EVENT_NAME: positional argument $1; used; surface in Python as a function argument.
  - EVENT_VALUE: positional argument $2; used; surface in Python as a function argument.
  - MAX_POLLS: positional argument $3 with default 60; used; surface in Python as a function argument with default.
  - POLL_INTERVAL: positional argument $4 with default 60; used; surface in Python as a function argument with default.
- check_prereq_job() parameters:
  - JOB_NAME: positional argument $1; used; surface in Python as a function argument.
  - RUN_DATE: positional argument $2; used; surface in Python as a function argument.
- log_job_audit() parameters:
  - JOB_NAME: positional argument $1; used; surface in Python as a function argument.
  - RUN_DATE: positional argument $2; used; surface in Python as a function argument.
  - JOB_STATUS: positional argument $3; used; surface in Python as a function argument.
  - ROWS_PROCESSED: positional argument $4 with default 0; used; surface in Python as a function argument with default.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- `eval "$CMD"`
  - Purpose: execute the retry target command string.
  - Python handling: must remain an external process invocation via subprocess, with careful shell-equivalent handling if the command is a string.
  - RESOLVABLE LAUNCHER: no; this is arbitrary command execution, not a known SQL launcher.
- `date '+%Y-%m-%d %H:%M:%S'`
  - Purpose: timestamp log messages.
  - Python handling: replace with datetime formatting in-process, not subprocess.
  - RESOLVABLE LAUNCHER: no.
- `sleep $WAIT`
  - Purpose: backoff delay between retries and polling intervals.
  - Python handling: replace with time.sleep().
  - RESOLVABLE LAUNCHER: no.
- `command -v uc4api >/dev/null 2>&1`
  - Purpose: detect whether the UC4 CLI/API tool is available.
  - Python handling: replace with shutil.which("uc4api").
  - RESOLVABLE LAUNCHER: no.
- `uc4api check_event "$EVENT_NAME" value="$EVENT_VALUE" 2>/dev/null`
  - Purpose: query UC4 for event completion status.
  - Python handling: must remain an external process invocation via subprocess unless a native UC4 API library is available; no source for uc4api is supplied.
  - RESOLVABLE LAUNCHER: no; internal behavior is not available in this extraction.
- `touch "$MARKER_FILE" 2>/dev/null`
  - Purpose: create a local marker file after UC4 confirms the event.
  - Python handling: replace with pathlib.Path(...).touch().
  - RESOLVABLE LAUNCHER: no.
- `sqlplus -s "$ORA_CONNECT" <<CHECK_EOF ...`
  - Purpose: query ETL_JOB_AUDIT for prerequisite job status.
  - Python handling: should become a native Oracle DB-client call if Oracle is the confirmed target platform.
  - RESOLVABLE LAUNCHER: yes for the SQL content itself, because the SQL is unambiguously Oracle SQL*Plus and the extraction includes DB-connection-style parameter ORA_CONNECT.
- `sqlplus -s "$ORA_CONNECT" <<AUDIT_EOF ...`
  - Purpose: execute MERGE-based audit upsert into ETL_JOB_AUDIT.
  - Python handling: should become a native Oracle DB-client call if Oracle is the confirmed target platform.
  - RESOLVABLE LAUNCHER: yes for the SQL content itself, because the SQL is unambiguously Oracle SQL*Plus and the extraction includes DB-connection-style parameter ORA_CONNECT.

5. EMBEDDED SQL
- Source file / label: `check_prereq_job()` heredoc `CHECK_EOF`
  - Full SQL text exactly as extracted:
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT NVL(MAX(JOB_STATUS), 'NOT_FOUND')
    FROM   ETL_JOB_AUDIT
    WHERE  JOB_NAME  = '$JOB_NAME'
    AND    RUN_DATE   = TO_DATE('$RUN_DATE','YYYY-MM-DD')
    AND    ROWNUM     = 1
    ORDER BY AUDIT_TIMESTAMP DESC;
    EXIT;
  - Statement type: SELECT
  - Tables touched: ETL_JOB_AUDIT
  - Dialect identification: unambiguously Oracle SQL*Plus due to `SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON`, `TO_DATE`, `ROWNUM`, and `EXIT;`
- Source file / label: `log_job_audit()` heredoc `AUDIT_EOF`
  - Full SQL text exactly as extracted:
    BEGIN
        MERGE INTO ETL_JOB_AUDIT tgt
        USING (SELECT '$JOB_NAME'      AS JOB_NAME,
                      TO_DATE('$RUN_DATE','YYYY-MM-DD') AS RUN_DATE
               FROM DUAL) src
        ON (tgt.JOB_NAME = src.JOB_NAME AND tgt.RUN_DATE = src.RUN_DATE)
        WHEN MATCHED THEN UPDATE SET
            tgt.JOB_STATUS      = '$JOB_STATUS',
            tgt.ROWS_PROCESSED  = $ROWS_PROCESSED,
            tgt.AUDIT_TIMESTAMP = SYSDATE,
            tgt.HOST_NAME       = SYS_CONTEXT('USERENV','HOST')
        WHEN NOT MATCHED THEN INSERT (
            AUDIT_ID, JOB_NAME, RUN_DATE, JOB_STATUS,
            ROWS_PROCESSED, AUDIT_TIMESTAMP, HOST_NAME
        ) VALUES (
            ETL_AUDIT_SEQ.NEXTVAL,
            '$JOB_NAME',
            TO_DATE('$RUN_DATE','YYYY-MM-DD'),
            '$JOB_STATUS',
            $ROWS_PROCESSED,
            SYSDATE,
            SYS_CONTEXT('USERENV','HOST')
        );
        COMMIT;
    END;
    /
    EXIT;
  - Statement type: PL/SQL block containing MERGE
  - Tables touched: ETL_JOB_AUDIT
  - Dialect identification: unambiguously Oracle due to `BEGIN ... END;`, `/` block terminator, `DUAL`, `SYSDATE`, `SYS_CONTEXT`, and `ETL_AUDIT_SEQ.NEXTVAL`

6. CONTROL FLOW
1. Library is sourced by another script; no top-level executable flow beyond function definitions.
2. `retry_command()` initializes local variables `CMD`, `MAX_RETRIES`, `BASE_WAIT`, and `ATTEMPT`.
3. `retry_command()` enters a `while [ $ATTEMPT -lt $MAX_RETRIES ]` loop.
4. Each retry attempt increments `ATTEMPT`.
5. It logs the attempt with a timestamp.
6. It executes the supplied command string via `eval "$CMD"`.
7. It captures the return code in `RC`.
8. If `RC -eq 0`, it logs success and returns 0.
9. If not successful and more retries remain, it computes `WAIT=$((BASE_WAIT * ATTEMPT))`, logs the delay, and sleeps.
10. After exhausting retries, it logs failure and returns 1.
11. `wait_for_event()` initializes `EVENT_NAME`, `EVENT_VALUE`, `MAX_POLLS`, `POLL_INTERVAL`, `MARKER_FILE`, and `ATTEMPT`.
12. It logs that it is waiting for the event and prints the marker path.
13. It enters a polling loop while `ATTEMPT -lt $MAX_POLLS`.
14. Each poll increments `ATTEMPT`.
15. It checks for the marker file with `-f`; if present, it logs detection and returns 0.
16. If no marker exists, it checks whether `uc4api` is available with `command -v`.
17. If available, it runs `uc4api check_event ...` and captures `UC4_STATUS`.
18. If `UC4_STATUS` is `COMPLETED` or `RAISED`, it logs confirmation, touches the marker file, and returns 0.
19. If the event is still not detected and polls remain, it logs the poll status and sleeps.
20. After all polls are exhausted, it logs timeout and returns 1.
21. `check_prereq_job()` initializes `JOB_NAME` and `RUN_DATE`.
22. It checks whether `ORA_CONNECT` is set.
23. If `ORA_CONNECT` is missing, it logs a warning and returns 0, skipping the DB check.
24. Otherwise it runs a SQL*Plus query against ETL_JOB_AUDIT and captures `STATUS`.
25. It strips spaces from `STATUS`.
26. It logs the resolved status.
27. It uses a `case` statement: `SUCCESS` or `COMPLETED` returns 0; anything else returns 1.
28. `log_job_audit()` initializes `JOB_NAME`, `RUN_DATE`, `JOB_STATUS`, and `ROWS_PROCESSED`.
29. It checks whether `ORA_CONNECT` is set.
30. If `ORA_CONNECT` is missing, it logs a warning and returns 0, skipping the audit write.
31. Otherwise it runs a SQL*Plus PL/SQL block that MERGEs into ETL_JOB_AUDIT and commits.
32. It logs the audit write completion message.

7. ERROR HANDLING & EXIT CODES
- Failure detection:
  - `retry_command()` checks `$?` after `eval`.
  - `wait_for_event()` checks file existence, UC4 API status, and loop exhaustion.
  - `check_prereq_job()` checks `ORA_CONNECT` presence and the SQL*Plus-derived status string.
  - `log_job_audit()` checks `ORA_CONNECT` presence; SQL*Plus errors are suppressed to `/dev/null`.
- Failure reaction:
  - `retry_command()` retries until max attempts, then returns 1.
  - `wait_for_event()` returns 1 on timeout.
  - `check_prereq_job()` returns 1 unless status is `SUCCESS` or `COMPLETED`.
  - `log_job_audit()` does not propagate SQL errors explicitly; it logs success message regardless after the call, so the caller cannot reliably detect failure from this function alone.
- Success convention:
  - 0 indicates success or acceptable completion.
  - 1 indicates failure/timeout/non-success status.
- Python mapping:
  - Use subprocess.run(..., check=True) or explicit returncode checks for external commands.
  - Use exceptions for Oracle DB-driver failures.
  - Use time.sleep() for delays.
  - Preserve the current “skip if ORA_CONNECT missing” behavior unless the caller wants stricter validation.

8. OUTPUTS / SIDE EFFECTS
- Writes log messages to stdout.
- Reads Oracle table ETL_JOB_AUDIT.
- Writes Oracle table ETL_JOB_AUDIT via MERGE/INSERT/UPDATE.
- Creates local marker files under `${ETL_EVENTS_DIR:-/opt/etl/events}`.
- May query UC4 via `uc4api`.
- Sleeps between retries/polls.

9. BUSINESS SUMMARY
- Provides reusable retry logic for fragile ETL commands.
- Waits for asynchronous UC4 event completion using marker files or UC4 API checks.
- Verifies prerequisite job completion from an audit table before proceeding.
- Records job completion/audit metadata back into ETL_JOB_AUDIT.
- Standardizes logging, retry timing, and audit behavior across orchestrator scripts.

# Step 1: Define a timestamp helper for log messages
from datetime import datetime
def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Step 2: Implement retry_command(command, max_retries=3, base_wait_sec=30)
def retry_command(cmd, max_retries=3, base_wait_sec=30):
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        print(f"[{ts()}] [retry_command] Attempt {attempt}/{max_retries}: {cmd}")
        cp = subprocess.run(cmd, shell=True)
        if cp.returncode == 0:
            print(f"[{ts()}] [retry_command] Success on attempt {attempt}.")
            return 0
        if attempt < max_retries:
            wait = base_wait_sec * attempt
            print(f"[{ts()}] [retry_command] Failed (rc={cp.returncode}). Waiting {wait}s...")
            time.sleep(wait)
    print(f"[{ts()}] [retry_command] All {max_retries} attempts failed.")
    return 1

# Step 3: Implement wait_for_event(event_name, event_value, max_polls=60, poll_interval_sec=60)
def wait_for_event(event_name, event_value, max_polls=60, poll_interval_sec=60):
    marker_file = pathlib.Path(os.environ.get("ETL_EVENTS_DIR", "/opt/etl/events")) / f"{event_name}_{event_value}.done"
    attempt = 0
    print(f"[{ts()}] [wait_for_event] Waiting for {event_name}={event_value}")
    print(f"  Marker: {marker_file}")
    while attempt < max_polls:
        attempt += 1
        if marker_file.is_file():
            print(f"[{ts()}] [wait_for_event] Event detected via marker: {marker_file}")
            return 0
        if shutil.which("uc4api"):
            cp = subprocess.run(["uc4api", "check_event", event_name, f"value={event_value}"], capture_output=True, text=True)
            uc4_status = cp.stdout.strip()
            if uc4_status in ("COMPLETED", "RAISED"):
                print(f"[{ts()}] [wait_for_event] Event confirmed via UC4 API.")
                marker_file.parent.mkdir(parents=True, exist_ok=True)
                marker_file.touch()
                return 0
        if attempt < max_polls:
            print(f"[{ts()}] [wait_for_event] Poll {attempt}/{max_polls} - not yet. Sleeping {poll_interval_sec}s...")
            time.sleep(poll_interval_sec)
    print(f"[{ts()}] [wait_for_event] TIMEOUT: {event_name}={event_value} not detected after {max_polls} polls.")
    return 1

# Step 4: Implement check_prereq_job(job_name, run_date)
def check_prereq_job(job_name, run_date):
    ora_connect = os.environ.get("ORA_CONNECT")
    if not ora_connect:
        print("[check_prereq_job] WARN: ORA_CONNECT not set. Skipping DB check.")
        return 0
    sql = """
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT NVL(MAX(JOB_STATUS), 'NOT_FOUND')
    FROM   ETL_JOB_AUDIT
    WHERE  JOB_NAME  = :job_name
    AND    RUN_DATE   = TO_DATE(:run_date,'YYYY-MM-DD')
    AND    ROWNUM     = 1
    ORDER BY AUDIT_TIMESTAMP DESC;
    EXIT;
    """
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
    # Use python-oracledb cursor.execute(...) against ETL_JOB_AUDIT
    ...

# Step 5: Implement log_job_audit(job_name, run_date, job_status, rows_processed=0)
def log_job_audit(job_name, run_date, job_status, rows_processed=0):
    ora_connect = os.environ.get("ORA_CONNECT")
    if not ora_connect:
        print("[log_job_audit] WARN: ORA_CONNECT not set. Skipping audit write.")
        return 0
    sql = """
    BEGIN
        MERGE INTO ETL_JOB_AUDIT tgt
        USING (SELECT :job_name      AS JOB_NAME,
                      TO_DATE(:run_date,'YYYY-MM-DD') AS RUN_DATE
               FROM DUAL) src
        ON (tgt.JOB_NAME = src.JOB_NAME AND tgt.RUN_DATE = src.RUN_DATE)
        WHEN MATCHED THEN UPDATE SET
            tgt.JOB_STATUS      = :job_status,
            tgt.ROWS_PROCESSED  = :rows_processed,
            tgt.AUDIT_TIMESTAMP = SYSDATE,
            tgt.HOST_NAME       = SYS_CONTEXT('USERENV','HOST')
        WHEN NOT MATCHED THEN INSERT (
            AUDIT_ID, JOB_NAME, RUN_DATE, JOB_STATUS,
            ROWS_PROCESSED, AUDIT_TIMESTAMP, HOST_NAME
        ) VALUES (
            ETL_AUDIT_SEQ.NEXTVAL,
            :job_name,
            TO_DATE(:run_date,'YYYY-MM-DD'),
            :job_status,
            :rows_processed,
            SYSDATE,
            SYS_CONTEXT('USERENV','HOST')
        );
        COMMIT;
    END;
    /
    EXIT;
    """
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
    # Use python-oracledb cursor.execute(...) and commit against ETL_JOB_AUDIT
    print(f"[{ts()}] [log_job_audit] {job_name} / {run_date} => {job_status} ({rows_processed} rows)")

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `lib/retry_handler.ksh` | `lib/retry_handler.py` | Shared KornShell ETL utility library migrated to a Python module to provide reusable retry, polling, and logging logic for BigQuery/GCP. |

#### Execution order
The legacy execution order specifies:
1. `customer/process_customer_data.ksh` (migrated in a separate design pass as `customer/process_customer_data.py` or equivalent Airflow Operator)
2. `lib/retry_handler.ksh` (migrated here as `lib/retry_handler.py`)

Since `lib/retry_handler.py` is a shared Python library module rather than an independent orchestrator, its integration in Cloud Composer (Airflow) will be achieved by importing it into the Python operators/scripts executed during the orchestration workflow. This ensures that the functional execution order and utility availability are preserved in the target environment.

#### Lineage
- `lib/retry_handler.ksh --[READS_TABLE]--> TABLE:ETL_JOB_AUDIT`: The migrated Python library will query the target equivalent of the audit table (`BQ_DATASET.etl_job_audit`) in BigQuery or a metadata database using the BigQuery client to verify prerequisite job statuses.
- `lib/retry_handler.ksh --[WRITES_TABLE]--> TABLE:ETL_JOB_AUDIT`: The migrated Python library will perform MERGE/INSERT operations on the target equivalent of `BQ_DATASET.etl_job_audit` to write job audit records.
- `lib/retry_handler.ksh --[WRITES_TABLE]--> TABLE:SET` (False Positive): This lineage edge is an artifact of the legacy parser misinterpreting the SQL*Plus configuration command `SET HEADING OFF` as a table modification. There is no physical `SET` table, and this does not map to any target database entity.

#### External system replacements
- **Oracle SQL\*Plus & PL/SQL block**: Legacy `sqlplus` invocations to query and MERGE into `ETL_JOB_AUDIT` must be replaced with native BigQuery API client calls (e.g., `google.cloud.bigquery`) using standardized parameterized queries.
- **Oracle Sequences**: The use of `ETL_AUDIT_SEQ.NEXTVAL` must be replaced. In BigQuery, which lacks native transactional sequences, unique IDs can be generated within Python via `uuid.uuid4()` or using timestamp-based unique identifiers, or managed via an external metadata backend (such as Cloud SQL) if strict sequential primary keys are required.
- **UC4 CLI / Event Polling**: The legacy polling mechanism queries `uc4api check_event`. In the target GCP environment, this UC4 polling is replaced by Cloud Composer (Airflow) sensors (such as `GCSObjectExistenceSensor` to look for marker files on Google Cloud Storage, or `PubSubPullSensor` for event triggers).

#### Cross-file dependencies
- **Sourcing Script Dependency**: `lib/retry_handler.ksh` is historically sourced (`. "${ETL_LIB_DIR}/retry_handler.ksh"`) by `customer/process_customer_data.ksh` and other ETL wrapper scripts. In the target environment, the Python scripts will import `lib.retry_handler` as a package module.
- **Marker File Path**: Polling for event marker files is dependent on the `ETL_EVENTS_DIR` folder structure, which must be mirrored or configured as a shared GCS bucket prefix in Cloud Composer.

#### Target file plan
- **Target File**: `lib/retry_handler.py`
  - **Source File**: `lib/retry_handler.ksh`
  - **Language**: Python
  - **Purpose**: A clean translation of the shell helper functions (`retry_command`, `wait_for_event`, `check_prereq_job`, `log_job_audit`) into an importable Python library module. It will leverage native Python modules like `subprocess`, `time`, `pathlib`, `shutil`, and the `google.cloud.bigquery` library.

#### Environment-specific values
- **`GCP_PROJECT`** (GLOBAL)
  - *Role*: Identifies the GCP project hosting the BigQuery datasets.
  - *Resolution*: Sourced at runtime via `os.environ.get("GCP_PROJECT")` or `Variable.get("GCP_PROJECT")` in Cloud Composer.
- **`BQ_DATASET`** (GLOBAL)
  - *Role*: The BigQuery dataset where the `etl_job_audit` table resides.
  - *Resolution*: Sourced at runtime via `os.environ.get("BQ_DATASET")` or `Variable.get("BQ_DATASET")` in Cloud Composer.
- **`GCS_BUCKET`** (GLOBAL)
  - *Role*: The cloud storage bucket used to store event marker files (replacing the legacy `/opt/etl/events/` directory).
  - *Resolution*: Sourced at runtime via `os.environ.get("GCS_BUCKET")` or `Variable.get("GCS_BUCKET")` in Cloud Composer.
- **`ETL_EVENTS_DIR`** (JOB-SPECIFIC)
  - *Role*: Local or remote path used to check for `.done` marker files.
  - *Resolution*: Sourced at runtime via `os.environ.get("ETL_EVENTS_DIR", "/opt/etl/events")` as a fallback or configured dynamically per environment.

#### Risks and manual steps
- **Sequence Generation (`ETL_AUDIT_SEQ.NEXTVAL`)**: BigQuery does not have a native sequence generator. Replacing it with UUIDs or timestamps requires downstream tables to accept non-integer or larger unique identifiers. A schema update for `etl_job_audit` might be required.
- **`uc4api` Command Dependency**: The legacy code attempts to run `uc4api` as a fallback. Since Cloud Composer will orchestrate the target environment, the UC4-specific polling code must be deactivated, and event orchestration should be natively handled by Airflow DAG sensors or event triggers. A human review is required to verify if any remaining legacy systems still rely on UC4 to publish events.
- **Output/Print Literal Preservation**: Ensure that all logging text strings, such as `[retry_command] Attempt` and `Failed (rc=...)`, are kept exactly as written in the source code to avoid breaking legacy log parsers or automated monitoring tools.