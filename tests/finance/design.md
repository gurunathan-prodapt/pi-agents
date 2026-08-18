=== OBJECT: FINANCE.GL_AGGREGATE_AND_CLOSE (JOBS_UNIX) ===
active=1
title=Spark aggregation for analytical outputs, then write the close audit record and send notification
login=UNIX.ETL_SVC
host=|ETLHOST1|HOST
ert_seconds=60
launcher_type=unrecognized
launcher_details={'raw_command': '#!/bin/ksh'}
script_body:
#!/bin/ksh
# FINANCE.GL_AGGREGATE_AND_CLOSE
:SET &PERIOD_NAME='&$PREV_MONTH_MON_YYYY'
:SET &FISCAL_YEAR='&$CURRENT_FISCAL_YEAR'
. &HOME/finance/r_gl_aggregate_and_close.ksh
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
This migration design covers a single UC4 Unix job, `FINANCE.GL_AGGREGATE_AND_CLOSE`. This job is responsible for executing a Spark-based financial ledger aggregation for analytical outputs, writing a closing audit record, and dispatching a status notification. The underlying script depends on environment variables for the financial period name and the current fiscal year. Because this bundle contains only a standalone `JOBS_UNIX` task and no orchestrating Workflow (`JOBP`) or Script (`SCRI`) trigger, this process is assumed to be externally triggered.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `FINANCE.GL_AGGREGATE_AND_CLOSE` | JOBS_UNIX | Active (1) | Spark aggregation for analytical outputs, then write the close audit record and send notification |

## 3. Scheduling
* **Calendar-Based Schedule:** No `EVNT_TIME` or native calendar-based schedule was supplied within this extraction bundle.
* **Trigger Source:** No parent `JOBP` or triggering `SCRI` was provided. This workflow is classified as **externally triggered** (source unknown from this extraction alone).
* **Airflow Schedule Property:** `schedule=None`

## 4. Airflow DAG Properties
Since this is a standalone UC4 Unix job, it is wrapped in a dedicated single-task DAG to represent its execution boundaries.

| Property | Value |
| :--- | :--- |
| **dag_id** | `finance_gl_aggregate_and_close` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{"owner": "UNIX_ETL_SVC", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `gl_aggregate_and_close` | `FINANCE.GL_AGGREGATE_AND_CLOSE` | `EmptyOperator` *(Stub)* | N/A | N/A | 1 | 5 min | None | None | False | None | #REVIEW-STRUCT: Launcher command `#!/bin/ksh` not recognized. Target script `. &HOME/finance/r_gl_aggregate_and_close.ksh` must be manually migrated to an appropriate Airflow operator (e.g., `BashOperator`, `SSHOperator`, or `DataprocSubmitPySparkJobOperator` if rewritten). |

## 6. Task Dependency Map
```python
# Standalone task DAG - no dependencies
gl_aggregate_and_close
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or mutual exclusion locks were defined for this object. Standard task concurrency constraints apply.

## 8. Error Handling and Retry Strategy
* **Retries:** Inherits the default argument of `1` retry with a `5-minute` delay.
* **Postconditions:** No explicit UC4 postconditions or status-branching conditions were specified.
* **Trigger Rules:** Default rule `all_success` will be utilized.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&PERIOD_NAME` | `&$PREV_MONTH_MON_YYYY` | `#REVIEW-STRUCT:` Custom Python execution context macro evaluating to previous month's abbreviation and year (e.g., `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b-%Y').upper() }}`). |
| `&FISCAL_YEAR` | `&$CURRENT_FISCAL_YEAR` | `#REVIEW-STRUCT:` Custom organization-specific macro or Airflow Variable containing the active fiscal year. |

## 10. Developer Notes
* **#REVIEW-STRUCT: Unrecognized Launcher Script:** The source job executes a Korn Shell (`.ksh`) script: `. &HOME/finance/r_gl_aggregate_and_close.ksh`. The developer must determine if this shell script will be executed directly via an `SSHOperator` on an edge node, containerized and run on Kubernetes, or rewritten to run on a managed Spark environment like GCP Dataproc.
* **#REVIEW-STRUCT: Parameter Resolution:** The variables `&$PREV_MONTH_MON_YYYY` and `&$CURRENT_FISCAL_YEAR` are UC4 system/user-defined variables. Equivalent Airflow jinja expressions or Airflow variables must be configured to pass these parameters to the target script environment.

---

# Numbered Pseudocode Outline

```python
# ==============================================================================
# 1. Imports
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: If executing via Bash or SSH after migration, import:
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.operators.bash import BashOperator

# ==============================================================================
# 2. GCP Configuration
# ==============================================================================
# No direct GCP resources assigned yet due to unrecognized ksh launcher.
# (Placeholders for target environment connections should be defined here)

# ==============================================================================
# 3. Default Args
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "UNIX_ETL_SVC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# 4. on_failure_callback stubs
# ==============================================================================
# No global or task-level failure alerts defined in the source UC4 metadata.

# ==============================================================================
# 5. DAG Definition
# ==============================================================================
with DAG(
    dag_id="finance_gl_aggregate_and_close",
    default_args=DEFAULT_ARGS,
    description="Spark aggregation for analytical outputs, write close audit record, and notify.",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["finance", "migration_uc4"],
) as dag:

    # ==========================================================================
    # 6. Guard Task (None)
    # ==========================================================================

    # ==========================================================================
    # 7. Sensor Task (None)
    # ==========================================================================

    # ==========================================================================
    # 8. Calendar Check Task (None)
    # ==========================================================================

    # ==========================================================================
    # 9. Task: gl_aggregate_and_close
    # ==========================================================================
    # #REVIEW-STRUCT: This is currently stubbed as an EmptyOperator because the
    # launcher type was 'unrecognized' (#!/bin/ksh script).
    #
    # Suggested Target Implementation:
    # If maintaining as a shell execution on a remote host:
    # gl_aggregate_and_close = SSHOperator(
    #     task_id="gl_aggregate_and_close",
    #     ssh_conn_id="ssh_etl_host",
    #     command="""
    #         export PERIOD_NAME="{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b-%Y').upper() }}"
    #         export FISCAL_YEAR="{{ var.value.get('CURRENT_FISCAL_YEAR') }}"
    #         . /path/to/HOME/finance/r_gl_aggregate_and_close.ksh
    #     """
    # )
    
    gl_aggregate_and_close = EmptyOperator(
        task_id="gl_aggregate_and_close",
    )

    # ==============================================================================
    # 10. Dependencies
    # ==============================================================================
    # Single-task workflow: no downstream or upstream dependencies to orchestrate.
    gl_aggregate_and_close
```

### Job Dependencies
* **Downstream Job:** `FINANCE.MONTH_END_SCHEDULE` — not yet migrated.
  * *Target Wiring:* Since this downstream job is not yet migrated, the orchestration wiring cannot be fully finalized. Once migrated, it should be triggered via Airflow's `TriggerDagRunOperator` at the end of the `finance_gl_aggregate_and_close` DAG, or by utilizing an `ExternalTaskSensor` within the downstream DAG to monitor this task’s completion.

### Execution Order
The multi-step legacy execution sequence must be preserved in the target orchestration as follows:
1. **`finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml`** maps to the parent orchestrator DAG itself: `dags/finance/finance_gl_aggregate_and_close.py`.
2. **`finance/r_gl_aggregate_and_close.ksh`** maps to the task that triggers the Spark aggregation job (e.g., executing the migrated Spark logic on Google Cloud Dataproc).
3. **`finance/d_gl_close_audit.sql`** maps to a BigQuery or Dataform execution task that runs the close-audit insert and status updates.

### Schedule & Variables
* **Schedule/Trigger:** This job is not directly triggered by any scheduler; instead, it is executed as an included/shared module inside scheduled workflows. Consequently, the migrated Airflow DAG must not have a standalone cron schedule (`schedule=None`) and should remain a callable unit triggered externally.
* **Scheduler-Set Variables:**
  * `PERIOD_NAME`: Dynamically resolved at runtime and passed to the task environment. The Airflow Jinja template equivalent representing the previous month's abbreviation and year is:
    `{{ (dag_run.logical_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b-%Y').upper() }}`
  * `FISCAL_YEAR`: Read at runtime using Airflow Variables or DAG parameters:
    `{{ var.value.get('CURRENT_FISCAL_YEAR') }}`

### Lineage
* **Upstream Producers:** None (this job acts as the entry orchestrator for the execution flow).
* **Downstream Consumers:** Invokes the shell script `finance/r_gl_aggregate_and_close.ksh` (which historically executed on legacy host `etlhost1`).

### Cross-File Dependencies
* **Shared Tables / Common Schemas:**
  * `GL_CLOSE_AUDIT`: Shared BigQuery target table containing period name, fiscal year, closer ID, and timestamp.
  * `GL_PERIOD_STATUS`: Shared target table containing the status of GL periods.
  * `ANALYTICS_SCHEMA`: Shared target dataset schema on BigQuery.
* **Call Chains:**
  * `FINANCE.GL_AGGREGATE_AND_CLOSE.xml` (Airflow DAG) -> Invokes `r_gl_aggregate_and_close.ksh` (Spark execution task) -> Executes `d_gl_close_audit.sql` (Database update task on BQ/Dataform).

### Target File Plan
* **`dags/finance/finance_gl_aggregate_and_close.py`**
  * **Language:** Python (Apache Airflow DAG)
  * **Source File:** `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml`

### Environment-Specific Values
We classify each environment-sourced variable based on its target role:
1. **GLOBAL (Environment-Wide Infrastructure)**
   * `GCP_PROJECT`: Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow Variable.
   * `GCP_REGION`: Sourced at runtime via `os.environ.get("GCP_REGION")` or Airflow Variable.
   * `BQ_DATASET`: Target database dataset representing `ANALYTICS_SCHEMA`. Sourced at runtime via Airflow Variable.
   * `&HOME`: The directory path of execution, sourced via `os.environ.get("AIRFLOW_HOME")`.
   * `|ETLHOST1|HOST` / `UNIX.ETL_SVC`: Legacy host login and execution parameters, mapping to Airflow SSH/GCP Connection configurations.
2. **JOB-SPECIFIC (Particular to this DAG / workflow)**
   * `NOTIFY_EMAIL`: `finance-etl@example.com` (to be set inside the DAG default parameters or variables specific to the financial close workflow).

### File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml` | `dags/finance/finance_gl_aggregate_and_close.py` | Migrates the UC4 job definition and orchestration into a Cloud Composer Airflow DAG. |

### Risks & Manual Actions
* **DOWNSTREAM: NOT YET MIGRATED — `FINANCE.MONTH_END_SCHEDULE`** — Wiring cannot be finalized until this downstream DAG exists.
* **CROSS-FILE DISPOSITION:** Sibling files `finance/r_gl_aggregate_and_close.ksh` and `finance/d_gl_close_audit.sql` belong to separate migration design passes and are not included in the source files of this pass. The tasks representing these steps inside `dags/finance/finance_gl_aggregate_and_close.py` must remain as placeholders or stubs until those dependent files are converted.

---

=== FILE: finance/d_gl_close_audit.sql ===
-- d_gl_close_audit.sql
-- Writes the immutable close-audit record once aggregation has succeeded.
-- Params: &1 = period name, &2 = fiscal year
-- Schema: ANALYTICS_SCHEMA

INSERT INTO ANALYTICS_SCHEMA.GL_CLOSE_AUDIT
    (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
VALUES
    ('&1', '&2', USER, SYSTIMESTAMP);

UPDATE ANALYTICS_SCHEMA.GL_PERIOD_STATUS
SET    CLOSE_STATUS = 'CLOSED',
       CLOSED_AT    = SYSTIMESTAMP
WHERE  PERIOD_NAME  = '&1';

COMMIT;
EXIT;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Script Object Type: Multi-statement transactional DML script with SQL*Plus substitution variables and session control commands.
1.2 Business Logic Summary: Once financial data aggregation is successful, this script performs a sequential two-step transactional update to close a period:
    1. Inserts an immutable audit record tracking who closed the period, for which fiscal year, and at what exact time.
    2. Updates the status of that period to 'CLOSED' and records the completion timestamp.
    Both operations must succeed as a atomic transaction, which is then committed.
1.3 Entities Referenced:
    * `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (Table)
      - `PERIOD_NAME` (Inferred: VARCHAR2 / BigQuery: STRING)
      - `FISCAL_YEAR` (Inferred: VARCHAR2 or NUMBER / BigQuery: STRING)
      - `CLOSED_BY` (Inferred: VARCHAR2 / BigQuery: STRING)
      - `CLOSED_AT` (Inferred: TIMESTAMP / BigQuery: TIMESTAMP)
    * `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` (Table)
      - `CLOSE_STATUS` (Inferred: VARCHAR2 / BigQuery: STRING)
      - `CLOSED_AT` (Inferred: TIMESTAMP / BigQuery: TIMESTAMP)
      - `PERIOD_NAME` (Inferred: VARCHAR2 / BigQuery: STRING)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    * Oracle `TIMESTAMP` (via `SYSTIMESTAMP`) maps directly to BigQuery `TIMESTAMP`. No truncation of fractional seconds.
    * Inferred alphanumeric columns mapped to BigQuery `STRING`.

2.2 Implicit and Explicit Type Casting:
    * No complex type casting or date arithmetic resides in the source code.

2.3 NULL Handling and Conditional Functions:
    * None present in the source script.

2.4 String Functions:
    * None present in the source script.

2.5 Date and Timestamp Functions:
    * `SYSTIMESTAMP` → `CURRENT_TIMESTAMP()`. Standard BigQuery parameter-free timestamp generator returning UTC-based timestamp. If local timezone alignment is needed, target datasets must use customized timezone parameters. For this general conversion, we use `CURRENT_TIMESTAMP()`.

2.6 Numeric and Aggregate Functions:
    * None present in the source script.

2.7 Analytical and Window Functions:
    * None present in the source script.

2.8 Set and Join Operations:
    * None present in the source script.

2.9 Row Limiting and Sampling:
    * None present in the source script.

2.10 Sequences:
    * None present in the source script.

2.11 MERGE Statements:
    * None present in the source script.

2.12 INSERT / UPDATE / DELETE:
    * An `INSERT` followed by an `UPDATE` are executed sequentially. In BigQuery, these are executed inside a Scripting Block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) to preserve transactional atomicity.

2.13 DDL Constructs:
    * None present in the source script.

2.14 PL/SQL / Scripting Constructs:
    * SQL*Plus Lexical Substitution Variables (`&1`, `&2`): BigQuery does not natively support interactive ampersand prompts. Resolved using BigQuery scripting declaration (`DECLARE ... DEFAULT ...`) or query parameterization (`@param1`, `@param2`). This architecture implements the `DECLARE` block approach for standalone script execution.
    * `USER` Pseudo-column: Represents the database user context executing the transaction. Resolved via BigQuery's `SESSION_USER()` system function.
    * `COMMIT`: Replaced with an explicit BQ transaction wrapper `BEGIN TRANSACTION` / `COMMIT TRANSACTION` to ensure atomic execution.
    * `EXIT`: Client-side exit directive. Stripped out as it is handled by the calling orchestration framework (e.g., dbt, Airflow, or bq CLI).

2.15 Unresolvable or Advisory Items:
    * Script-level SQL*Plus directives (`EXIT`, parameter substitution syntax) cannot be natively translated. The calling orchestration system must pass the parameters as variables into the BigQuery session.

2.16 MIGRATION DECISION MATRIX

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| **Substitution parameters (`&1`, `&2`)** | Direct BigQuery SQL (via Scripting `DECLARE` / `SET`) | Standard SQL Query Parameters (`@param`) | Scripting allows standalone executable scripts to run on BigQuery without relying on external parameter binding clients. |
| **`USER` Pseudo-column** | Direct BigQuery Standard SQL (`SESSION_USER()`) | Hardcoded literal or UDF | BigQuery's `SESSION_USER()` natively captures the identity of the authenticated account executing the query. |
| **`SYSTIMESTAMP`** | Direct BigQuery Standard SQL (`CURRENT_TIMESTAMP()`) | `CURRENT_DATETIME()` | `CURRENT_TIMESTAMP()` matches the timezone-aware nature and high-precision fractional seconds of Oracle's `SYSTIMESTAMP`. |
| **`COMMIT`** | Direct BigQuery Standard SQL (`COMMIT TRANSACTION;`) | Omission (Auto-commit) | The script relies on multi-table state consistency. If the `UPDATE` fails, the `INSERT` must rollback. Hence, an explicit scripting transaction block is selected. |
| **`EXIT`** | Manual intervention (stripped) | Python Wrapper wrapper execution | Client exit handling is cleanly managed by Cloud Composer (Airflow) or the BQ runner wrapper, removing script-level exit pollution. |

2.17 REQUIRED ARTIFACTS
The migration will generate a single BigQuery SQL scripting artifact containing a transactional block. External orchestration (such as Airflow or GCP Workflows) will invoke this query with runtime parameters injected either into `DECLARE` headers or passed directly as query execution arguments.

2.18 DATA TYPE COMPATIBILITY TABLE

| Source Oracle Column / Object | Oracle Inferred Type | Target BigQuery Type | Conversion Rule / Logic | Warning / Mitigation |
| :--- | :--- | :--- | :--- | :--- |
| `PERIOD_NAME` | `VARCHAR2` | `STRING` | Direct conversion. | None. |
| `FISCAL_YEAR` | `VARCHAR2` / `NUMBER` | `STRING` | Kept as string to preserve non-numeric fiscal representations if any. | None. |
| `CLOSED_BY` | `VARCHAR2` (via `USER`) | `STRING` | `SESSION_USER()` outputs a string (email address format). | Ensure database schemas hold sufficient length to store emails (user@domain.com) instead of short DB usernames. |
| `CLOSED_AT` | `TIMESTAMP` | `TIMESTAMP` | Captured using `CURRENT_TIMESTAMP()`. | System timestamps in BQ default to UTC. If local business timezone reporting is required, format conversions might be needed downstream. |
| `CLOSE_STATUS` | `VARCHAR2` | `STRING` | Direct conversion. | None. |

2.19 DESIGN REVIEW SUMMARY
* **Patterns found**: Direct positional parameter bindings, transactional updates on dimensional statuses, and logging of execution contexts.
* **Unsupported functions**: SQL*Plus native directives (stripped).
* **UDF Required**: No.
* **Python Required**: No.
* **Direct dependencies**: `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT`, `ANALYTICS_SCHEMA.GL_PERIOD_STATUS`.
* **Assumptions**: 
  1. The running credential executing the script corresponds to the business user or service account to be logged in `GL_CLOSE_AUDIT`.
  2. Tables are already migrated with compatible schemas to the designated BigQuery project.
  3. Parameters are bound via Script-level variables at execution.
* **Warnings**: Timezone drift. Oracle `SYSTIMESTAMP` is relative to the database operating system timezone, whereas BigQuery `CURRENT_TIMESTAMP()` always evaluates to UTC.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.20 PACKAGE ANALYSIS
Not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `&1` (Lexical param) | Direct-with-rewrite | `DECLARE var_period_name STRING;` / Scripting Variable |
| `&2` (Lexical param) | Direct-with-rewrite | `DECLARE var_fiscal_year STRING;` / Scripting Variable |
| `USER` | Direct-with-rewrite | `SESSION_USER()` |
| `SYSTIMESTAMP` | Direct-with-rewrite | `CURRENT_TIMESTAMP()` |
| `COMMIT` | Direct-with-rewrite | `BEGIN TRANSACTION;` ... `COMMIT TRANSACTION;` block |
| `EXIT` | Unsupported | None — handled by client shell wrapper orchestration |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Explicit scripting variables declaration replacing Oracle SQL*Plus substitution variables
DECLARE var_period_name STRING;
DECLARE var_fiscal_year STRING;

-- Placeholder for parameters mapping - to be bound during runtime execution 
-- or dynamically passed as script parameters
SET var_period_name = 'placeholder_period_name';  -- converted from '&1'
SET var_fiscal_year = 'placeholder_fiscal_year';  -- converted from '&2'

-- Begin atomic database transaction block to replicate implicit Oracle DML safety and explicit COMMIT behavior
BEGIN
  BEGIN TRANSACTION;

  -- 1. Insert execution audit record
  INSERT INTO ANALYTICS_SCHEMA.GL_CLOSE_AUDIT
      (
        PERIOD_NAME, 
        FISCAL_YEAR, 
        CLOSED_BY, 
        CLOSED_AT
      )
  VALUES
      (
        var_period_name, 
        var_fiscal_year, 
        SESSION_USER(),         -- converted from USER pseudo-column
        CURRENT_TIMESTAMP()     -- converted from SYSTIMESTAMP
      );

  -- 2. Update status of the respective period status table record
  UPDATE ANALYTICS_SCHEMA.GL_PERIOD_STATUS
  SET    CLOSE_STATUS = 'CLOSED',
         CLOSED_AT    = CURRENT_TIMESTAMP() -- converted from SYSTIMESTAMP
  WHERE  PERIOD_NAME  = var_period_name;

  -- Commit changes atomically if no exception occurs
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Fallback logic to roll back updates if any error during transaction blocks
  ROLLBACK TRANSACTION;
  -- Escalate exception to execution engine
  RAISE USING MESSAGE = CONCAT('Failure in GL Close Transaction Execution: ', @@error.message);
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Timezone Evaluation**: `SYSTIMESTAMP` in Oracle defaults to database system time (with local timezone offsetting). BigQuery's `CURRENT_TIMESTAMP()` resolves to absolute UTC. If the downstream audit process expects timestamps in a local timezone, convert using `DATETIME(CURRENT_TIMESTAMP(), 'Target/Timezone_Name')`.
2. **Session Identification**: `SESSION_USER()` in BigQuery returns the Google Identity (usually an email, e.g., `user@domain.com` or `sa-name@gcp-project.iam.gserviceaccount.com`), whereas Oracle `USER` returns database schema usernames (e.g., `FIN_ADMIN`). Audit columns must be sized accordingly to support email address formats.
3. **Execution Parameter Binding**: This script wraps variables inside standard declarations. If orchestrating via Apache Airflow (Cloud Composer) or Python SDK, configure the calling task to bind parameters natively using the client parameters dictionary instead of string substitution.
4. **Interactive Exit Command**: The Oracle `EXIT` statement is omitted. Command-line orchestrators must catch the exit code of the execution engine shell task instead of relying on in-script exits.

### Job dependencies
- **Downstream**: `FINANCE.MONTH_END_SCHEDULE` (not yet migrated). Since this downstream job is not yet migrated, the orchestration wiring (such as Airflow DAG sensors or execution triggers) cannot be finalized. It must be completed once `FINANCE.MONTH_END_SCHEDULE` is migrated to BigQuery.

### Execution order
The target orchestration must preserve the legacy execution order:
1. `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml` (UC4 orchestration)
2. `finance/r_gl_aggregate_and_close.ksh` (KornShell wrapper)
3. `finance/d_gl_close_audit.sql` (Oracle SQL*Plus script)

Your scope covers only step 3 (`finance/d_gl_close_audit.sql`). The target task/file plan for this converted SQL script must be orchestrated to execute directly after the preceding Spark aggregation wrapper (`r_gl_aggregate_and_close.ksh` or its converted equivalent) completes successfully.

### Schedule & variables
- **Schedule**: This job is not directly triggered by any of the environment's standalone schedulers; it is designed to run as an included/shared module inside broader scheduled jobs. The migrated BigQuery SQL script must remain an importable/callable task within its calling orchestrator (e.g., Cloud Composer/Airflow DAG) and must not be given an independent standalone schedule.
- **Variables**:
  - `PERIOD_NAME` (Value: `&$PREV_MONTH_MON_YYYY` inherited from `FINANCE.GL_AGGREGATE_AND_CLOSE` context). This variable must be supplied dynamically to the target script at runtime using Airflow template variables or parameterized execution.
  - `FISCAL_YEAR` (Value: `&$CURRENT_FISCAL_YEAR` inherited from `FINANCE.GL_AGGREGATE_AND_CLOSE` context). This must also be supplied dynamically at runtime using the native parameterization mechanism of the calling job.

### Lineage
- **Upstream Producers**: None directly identified in the immediate file metadata (it reads logic states rather than source tables).
- **Downstream Consumers (Writes)**:
  - `TABLE:GL_CLOSE_AUDIT` (Columns: `PERIOD_NAME`, `FISCAL_YEAR`, `CLOSED_BY`, `CLOSED_AT`)
  - `TABLE:GL_PERIOD_STATUS` (Columns: `CLOSE_STATUS`, `CLOSED_AT`, `PERIOD_NAME`)
- **Package Usage**:
  - `PACKAGE:ANALYTICS_SCHEMA` (Reflected as the BigQuery target dataset namespace `ANALYTICS_SCHEMA`).

### Target file plan
The following target file must be generated to mirror the source folder structure:

| Target File Path | Language | Source File Path | Purpose |
| :--- | :--- | :--- | :--- |
| `finance/d_gl_close_audit.sql` | SQL | `finance/d_gl_close_audit.sql` | Contains the BigQuery SQL scripting block executing the insert and update operations inside an atomic transaction block. |

### Environment-specific values
We classify the environment-specific values in this script based on their target environment roles:

1. **GLOBAL (Environment-wide)**
   - `ANALYTICS_SCHEMA`: Target BigQuery dataset name. In the target SQL scripting code, this is classified as `BQ_DATASET`. It must be parameterized and resolved at deploy/run time (e.g., using Dataform dataset references or query parameters substituted by the calling task) to prevent environment-specific dataset names from being hardcoded.

2. **JOB-SPECIFIC**
   - `PERIOD_NAME` / `&1`: Supplied by the scheduler context representing the target accounting period. This must be injected as a runtime query parameter by the calling orchestrator.
   - `FISCAL_YEAR` / `&2`: Supplied by the scheduler context representing the target fiscal year. This must be injected as a runtime query parameter by the calling orchestrator.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `finance/d_gl_close_audit.sql` | `finance/d_gl_close_audit.sql` | Converted to a transactional BigQuery Standard SQL script with dynamic scripting variables replacing Oracle SQL*Plus parameter tokens. |

### Risks & Manual Actions
1. **Timezone Evaluation**: The legacy `SYSTIMESTAMP` functions operate in the source database's local system timezone. BigQuery's `CURRENT_TIMESTAMP()` resolves strictly to UTC. If downstream reporting processes require local timezone-aligned timestamps, manual conversion must be introduced using `DATETIME(CURRENT_TIMESTAMP(), "Target_Timezone")`.
2. **Session User Column Length**: The legacy `USER` pseudo-column returns database-level schema users (e.g., `FIN_ADMIN`). BigQuery's `SESSION_USER()` returns full email formats (e.g., `sa-finance@prod-project.iam.gserviceaccount.com`). The schema of the `GL_CLOSE_AUDIT.CLOSED_BY` column must be verified on the target side to ensure its length accommodates these longer string formats (typically `STRING` or `VARCHAR(256)`).
3. **Execution Parameter Binding**: The SQL*Plus lexical variables (`&1`, `&2`) must be populated via parameter-binding libraries in the calling Composer/Airflow task or client runner, ensuring they do not rely on raw text replacement which is prone to injection risks.

---

=== FILE: finance/r_gl_aggregate_and_close.ksh ===
#!/bin/ksh
###############################################################################
# r_gl_aggregate_and_close.ksh
#
# Invoked by FINANCE.GL_AGGREGATE_AND_CLOSE. Runs the Spark aggregation job
# for analytical outputs, writes the close-audit record once aggregation is
# confirmed, and sends the completion notification. All three steps run in
# one job because the close audit record must never be written unless
# aggregation actually succeeded.
###############################################################################
set -e

FIN_HOME=${FIN_HOME:-/opt/etl/finance}
FIN_ORA_USER=${FIN_ORA_USER:-fin_etl}
FIN_ORA_PASS=${FIN_ORA_PASS:-changeit}
FIN_ORA_SID=${FIN_ORA_SID:-FINPRD}
NOTIFY_EMAIL=${NOTIFY_EMAIL:-finance-etl@example.com}

log() {
    print "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "Submitting GL aggregation Spark job for period ${PERIOD_NAME}, fiscal year ${FISCAL_YEAR}"
spark-submit \
    --master yarn --deploy-mode cluster \
    --num-executors 6 --executor-memory 6g \
    --conf spark.sql.shuffle.partitions=200 \
    /opt/spark/jobs/finance-gl-aggregation-assembly.jar \
    --period-name "${PERIOD_NAME}" \
    --fiscal-year "${FISCAL_YEAR}"
spark_rc=$?

if [ ${spark_rc} -ne 0 ]; then
    log "ERROR: GL aggregation Spark job failed with rc=${spark_rc} - close audit will NOT be written"
    exit 1
fi
log "GL aggregation completed successfully"

log "Writing close-audit record for period ${PERIOD_NAME}"
sqlplus -s ${FIN_ORA_USER}/${FIN_ORA_PASS}@${FIN_ORA_SID} @${FIN_HOME}/finance/d_gl_close_audit.sql "${PERIOD_NAME}" "${FISCAL_YEAR}"
if [ $? -ne 0 ]; then
    log "ERROR: failed to write close-audit record"
    exit 2
fi

log "Notifying stakeholders of month-end close completion"
{
    print "Month-end close complete for period: ${PERIOD_NAME}"
    print "Fiscal year: ${FISCAL_YEAR}"
    print "Completed at: $(date)"
} | mailx -s "[FINANCE-OK] Month-End Close ${PERIOD_NAME}" "${NOTIFY_EMAIL}"

log "FINANCE.GL_AGGREGATE_AND_CLOSE finished successfully"
exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains custom logging, multi-step orchestration of Spark and SQL*Plus, exit-code validation, and mailx notifications.

EVIDENCE
- Business logic found: KSH custom logic orchestrates a Spark job via spark-submit, checks the exit code, runs an Oracle SQL script via SQL*Plus to audit-log the period close, checks that exit code, and sends a notification email via mailx.
- AWK: none
- SQL-expressible: No, because it orchestrates external process executions like `spark-submit` and sends an email via `mailx` which cannot be modeled in BigQuery SQL.
- Non-SQL side effects: Execution of Spark jobs on a YARN cluster, sending emails to stakeholders via standard mail transport.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script automates the month-end closing and analytical data aggregation process for the Finance department. It submits a Spark job in cluster mode to run General Ledger (GL) aggregation for a specific period and fiscal year. Once Spark execution succeeds, it logs a close-audit entry into an Oracle database and subsequently alerts stakeholders of successful completion via email.

2. INVOCATION CONTEXT
   - Who calls this script: UC4 Job `FINANCE.GL_AGGREGATE_AND_CLOSE` (implied by the header documentation block). It is invoked with environment variables set by the UC4 environment.
   - UC4 native includes: None referenced in this script.
   - Environment files sourced: None sourced explicitly in this script.

3. PARAMETERS / INPUTS
   - `FIN_HOME`: Environment variable, defaults to `/opt/etl/finance`. It specifies the location of the SQL audit script. Used in the script to find the SQL script path.
   - `FIN_ORA_USER`: Environment variable, defaults to `fin_etl`. Database username for Oracle close-audit execution.
   - `FIN_ORA_PASS`: Environment variable, defaults to `changeit`. Database password for Oracle.
   - `FIN_ORA_SID`: Environment variable, defaults to `FINPRD`. Oracle System Identifier/service name.
   - `NOTIFY_EMAIL`: Environment variable, defaults to `finance-etl@example.com`. Target email address for month-end close notifications.
   - `PERIOD_NAME`: Environment variable (implied external parameter, no default). Month/period name to run (e.g., 'JAN-2024'). Actually used in Spark call, SQL execution, and notification.
   - `FISCAL_YEAR`: Environment variable (implied external parameter, no default). Fiscal year to run (e.g., '2024'). Actually used in Spark call, SQL execution, and notification.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `spark-submit`:
     - Verbatim: `spark-submit --master yarn --deploy-mode cluster --num-executors 6 --executor-memory 6g --conf spark.sql.shuffle.partitions=200 /opt/spark/jobs/finance-gl-aggregation-assembly.jar --period-name "${PERIOD_NAME}" --fiscal-year "${FISCAL_YEAR}"`
     - Purpose: Submits the Scala/Java Spark assembly jar to Yarn to execute GL data aggregation.
     - Execution type: Must remain an external process invocation via `subprocess` (or mapped to a cloud-equivalent Spark launcher, e.g., Dataproc API, depending on migration environment).
     - Resolvable launcher: No. It is a system CLI tool for distributed computation.
   - `sqlplus`:
     - Verbatim: `sqlplus -s ${FIN_ORA_USER}/${FIN_ORA_PASS}@${FIN_ORA_SID} @${FIN_HOME}/finance/d_gl_close_audit.sql "${PERIOD_NAME}" "${FISCAL_YEAR}"`
     - Purpose: Runs the Oracle close audit logging script.
     - Execution type: Can be run via native Python DB client (`oracledb`) executing the commands extracted from `d_gl_close_audit.sql`, or preserved as an external `subprocess` invocation.
     - Resolvable launcher: No. The wrapped SQL file content is not supplied, making a direct native client SQL translation provisional on retrieving that file.
     - Structural Gap: # REVIEW-STRUCT: SQL file finance/d_gl_close_audit.sql content not supplied — executing via sqlplus subprocess or migrating to native Python DB client requires manual extraction of SQL statements
   - `mailx`:
     - Verbatim: `mailx -s "[FINANCE-OK] Month-End Close ${PERIOD_NAME}" "${NOTIFY_EMAIL}"` (receives stdin payload)
     - Purpose: Sends standard month-end confirmation email.
     - Execution type: Convert to native Python SMTP (`smtplib` and `email.message`) code.

5. EMBEDDED SQL
   - Source file: `${FIN_HOME}/finance/d_gl_close_audit.sql`
   - Full SQL text: Not supplied in this extraction.
   - Statement type: Unknown (Presumed insert/update audit logger).
   - Tables touched: Unknown.
   - Dialect: Oracle SQL*Plus.
   - Structural Gap: # REVIEW-STRUCT: SQL script d_gl_close_audit.sql is not provided in extraction; direct DB-client translation is provisional on obtaining this file.

6. CONTROL FLOW
   - Step 1: Environment Setup. Set up defaults for `FIN_HOME`, `FIN_ORA_USER`, `FIN_ORA_PASS`, `FIN_ORA_SID`, and `NOTIFY_EMAIL`. Verify `PERIOD_NAME` and `FISCAL_YEAR` are present in the environment.
   - Step 2: Define helper `log()` function to output timestamped statements.
   - Step 3: Spark Execution. Call `spark-submit` to aggregate GL data for the specified period and fiscal year.
   - Step 4: Spark Status Check. Verify return code. If Spark fails, log failure, skip close audit/email, and exit script with return code 1.
   - Step 5: Close-Audit Record. Execute `sqlplus` with DB credentials and positional arguments to run `d_gl_close_audit.sql`.
   - Step 6: Oracle Status Check. Verify return code. If SQL*Plus fails, log failure, skip completion email, and exit script with return code 2.
   - Step 7: Notification. Construct completion email body containing execution metadata and pipe it to `mailx` command.
   - Step 8: Complete execution with code 0.

7. ERROR HANDLING & EXIT CODES
   - How failure is detected: The shell script uses `set -e` to catch unhandled command failures, checks `$spark_rc` directly after Spark execution, and checks `$?` directly after `sqlplus`.
   - Reaction:
     - If Spark fails, logs "ERROR: GL aggregation Spark job failed..." and exits with code 1.
     - If Oracle SQL*Plus fails, logs "ERROR: failed to write close-audit record" and exits with code 2.
   - Success Exit Code: 0.
   - Python Mapping: Implement explicitly using `subprocess.run(..., check=True)` capturing `subprocess.CalledProcessError`. On Spark subprocess failure, raise custom error or log and `sys.exit(1)`. On SQL*Plus subprocess failure, log and `sys.exit(2)`.

8. OUTPUTS / SIDE EFFECTS
   - Oracle database record update (via execution of `d_gl_close_audit.sql`).
   - Standard output/error logs generated by Spark, SQL*Plus, and script shell print statements.
   - Delivery of confirmation email via SMTP/mailx.

9. BUSINESS SUMMARY
   - Automates the month-end closing process for General Ledger financial data.
   - Ensures analytical aggregation of GL data is successfully processed prior to committing the period close.
   - Integrates transaction sequencing: the close audit record is never generated unless Spark aggregation is validated as successful.
   - Proactively alerts financial operations of successful month-end reconciliation completion.

=== PSEUDOCODE ===

```python
# Step 1: Import required modules and validate environment inputs
import os
import sys
import subprocess
from datetime import datetime

# Retrieve configuration parameters with fallback defaults
FIN_HOME = os.environ.get("FIN_HOME", "/opt/etl/finance")
FIN_ORA_USER = os.environ.get("FIN_ORA_USER", "fin_etl")
FIN_ORA_PASS = os.environ.get("FIN_ORA_PASS", "changeit")
FIN_ORA_SID = os.environ.get("FIN_ORA_SID", "FINPRD")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "finance-etl@example.com")

# # REVIEW: PERIOD_NAME and FISCAL_YEAR are required parameters but do not have script defaults.
PERIOD_NAME = os.environ.get("PERIOD_NAME")
if not PERIOD_NAME:
    print("ERROR: Environment variable 'PERIOD_NAME' must be set", file=sys.stderr)
    sys.exit(1)

FISCAL_YEAR = os.environ.get("FISCAL_YEAR")
if not FISCAL_YEAR:
    print("ERROR: Environment variable 'FISCAL_YEAR' must be set", file=sys.stderr)
    sys.exit(1)

# Step 2: Define utility function for timestamped logging
def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Step 3: Log initiation and construct/execute Spark-submit job
log(f"Submitting GL aggregation Spark job for period {PERIOD_NAME}, fiscal year {FISCAL_YEAR}")

spark_cmd = [
    "spark-submit",
    "--master", "yarn",
    "--deploy-mode", "cluster",
    "--num-executors", "6",
    "--executor-memory", "6g",
    "--conf", "spark.sql.shuffle.partitions=200",
    "/opt/spark/jobs/finance-gl-aggregation-assembly.jar",
    "--period-name", PERIOD_NAME,
    "--fiscal-year", FISCAL_YEAR
]

# Step 4: Run Spark aggregation job and catch non-zero return codes
try:
    subprocess.run(spark_cmd, check=True)
    log("GL aggregation completed successfully")
except subprocess.CalledProcessError as e:
    log(f"ERROR: GL aggregation Spark job failed with rc={e.returncode} - close audit will NOT be written")
    sys.exit(1)

# Step 5: Execute SQL*Plus audit entry script
log(f"Writing close-audit record for period {PERIOD_NAME}")

# # REVIEW-STRUCT: SQL file finance/d_gl_close_audit.sql content not supplied; executing via sqlplus subprocess or migrating to native Python DB client requires manual extraction of SQL statements
sqlplus_cmd = [
    "sqlplus",
    "-s",
    f"{FIN_ORA_USER}/{FIN_ORA_PASS}@{FIN_ORA_SID}",
    f"@{FIN_HOME}/finance/d_gl_close_audit.sql",
    PERIOD_NAME,
    FISCAL_YEAR
]

# Step 6: Run SQL*Plus subprocess and catch database-write failures
try:
    subprocess.run(sqlplus_cmd, check=True)
except subprocess.CalledProcessError as e:
    log("ERROR: failed to write close-audit record")
    sys.exit(2)

# Step 7: Send month-end close notification email to stakeholders
log("Notifying stakeholders of month-end close completion")

email_body = (
    f"Month-end close complete for period: {PERIOD_NAME}\n"
    f"Fiscal year: {FISCAL_YEAR}\n"
    f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
)

try:
    # Invokes system mailx command using email_body as stdin
    subprocess.run(
        ["mailx", "-s", f"[FINANCE-OK] Month-End Close {PERIOD_NAME}", NOTIFY_EMAIL],
        input=email_body,
        text=True,
        check=True
    )
except subprocess.CalledProcessError as e:
    log(f"WARNING: Email notification failed to send, exit code: {e.returncode}")

# Step 8: Log successful completion of Month-End pipeline and exit
log("FINANCE.GL_AGGREGATE_AND_CLOSE finished successfully")
sys.exit(0)
```

### Job Dependencies
- **Downstream Job:** `FINANCE.MONTH_END_SCHEDULE` (not yet migrated).
  - *Target Wiring:* The connection between the migrated `FINANCE.GL_AGGREGATE_AND_CLOSE` task in Cloud Composer and the downstream `FINANCE.MONTH_END_SCHEDULE` must be managed via an Airflow task trigger, dataset sensor, or cross-DAG dependency. Since the downstream job is not yet migrated, this downstream trigger cannot be finalized and must be stubbed or paused.

### Execution Order
The execution order of the legacy steps is preserved and mapped to the target orchestration tasks:
1. **Legacy Step 1 (`finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml`):** Orchestration metadata. Mapped to the target Cloud Composer DAG definition.
2. **Legacy Step 2 (`finance/r_gl_aggregate_and_close.ksh`):** Spark execution and orchestration shell script. Mapped to Python task(s) (`finance/r_gl_aggregate_and_close.py`) executed within the DAG.
3. **Legacy Step 3 (`finance/d_gl_close_audit.sql`):** Close-audit registration SQL. Mapped to a subsequent BigQuery/Dataform task in the target orchestration (to be migrated in its own separate design pass).

### Scheduling
- **Trigger Event:** This job is not directly triggered by any of the environment's standalone schedulers. It runs as an include/shared module inside other scheduled processes.
- **Target Mapping:** The migrated Python script must remain a callable/importable unit (such as a task or TaskGroup within a shared Airflow DAG structure) and must not be given its own standalone schedule.

### Schedule & Variables
The target environment must dynamically feed the equivalent variables to the Python task:
- **`PERIOD_NAME`** (Inherited legacy variable `&$PREV_MONTH_MON_YYYY`): Must be supplied at runtime using Airflow macro-based execution parameters, e.g., `{{ logical_date.subtract(months=1).strftime('%b-%Y').upper() }}`.
- **`FISCAL_YEAR`** (Inherited legacy variable `&$CURRENT_FISCAL_YEAR`): Must be resolved dynamically at runtime using Airflow templates, e.g., `{{ logical_date.strftime('%Y') }}`.

### Lineage
- **Upstream Producers:** None.
- **Downstream Consumers / External Targets:**
  - `EXT:mailx` (Lineage Edge): Replaced by native SMTP / Mail notification within Cloud Composer.
  - `FILE:finance/d_gl_close_audit.sql` (Lineage Edge): This is a cross-job sibling file hand-off representing the SQL audit logger that is executed upon successful Spark run completion. It will be converted in a separate Dataform/BigQuery design pass.

### Cross-File Dependencies
- **Shared Files / Scripts:**
  - `finance/d_gl_close_audit.sql`: The audit log SQL script that is called by this wrapper. The Python code expects the BigQuery/Dataform table `GL_CLOSE_AUDIT` to be available.

### Target File Plan
- **File Path:** `finance/r_gl_aggregate_and_close.py`
  - **Language:** Python
  - **Source File:** `finance/r_gl_aggregate_and_close.ksh`

### Environment-Specific Values
The environment variables used in the source logic are mapped to target configurations as follows:

| Legacy Source Construct | Target Parameter / Key | Classification | Target Resolution / Sourcing Mechanism |
| :--- | :--- | :--- | :--- |
| *N/A* (New Cloud Spark Setup) | `GCP_PROJECT` | GLOBAL | Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow `Variable.get("GCP_PROJECT")` |
| *N/A* (New Cloud Spark Setup) | `DATAPROC_REGION` | GLOBAL | Sourced at runtime via `os.environ.get("DATAPROC_REGION")` |
| *N/A* (New Cloud Spark Setup) | `DATAPROC_CLUSTER` | GLOBAL | Sourced at runtime via `os.environ.get("DATAPROC_CLUSTER")` |
| `/opt/spark/jobs/...` | `GCS_BUCKET` | GLOBAL | Sourced at runtime via `os.environ.get("GCS_BUCKET")` to locate the Spark jar path |
| `FIN_HOME` | `FIN_HOME` | JOB-SPECIFIC | Sourced from job-level runtime configuration or Airflow task parameters |
| `NOTIFY_EMAIL` | `NOTIFY_EMAIL` | JOB-SPECIFIC | Configured as a parameter in the DAG `params` or Airflow `Variable.get("finance_notify_email")` |
| `PERIOD_NAME` | `PERIOD_NAME` | JOB-SPECIFIC | Derived dynamically at run time via Airflow templating |
| `FISCAL_YEAR` | `FISCAL_YEAR` | JOB-SPECIFIC | Derived dynamically at run time via Airflow templating |

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `finance/r_gl_aggregate_and_close.ksh` | `finance/r_gl_aggregate_and_close.py` | Converted to a Python orchestrator to manage Spark job execution on Dataproc, BigQuery audit-log triggers, and SMTP alerting. |

### Risks & Manual Actions
- **Downstream Dependency:** `FINANCE.MONTH_END_SCHEDULE` is not yet migrated; the final downstream orchestration trigger cannot be wired until this job is migrated.
- **SQL File Separation:** The script executing `sqlplus` references `finance/d_gl_close_audit.sql` which belongs to a different migration group. The native Python transition must be carefully synchronized with the Dataform migration of `d_gl_close_audit.sql`.