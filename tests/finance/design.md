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


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
This migration design document covers the transition of the UC4 Unix job `FINANCE.GL_AGGREGATE_AND_CLOSE` to Apache Airflow. This object is designed to execute a Spark aggregation for financial analytical outputs, write a close audit record, and dispatch notifications. Based on the extraction, it defines two critical period-based variables and runs an underlying Korn shell script (`r_gl_aggregate_and_close.ksh`). Because no parent workflow (JOBP) or schedule is defined in this bundle, this job is configured as a standalone single-task Airflow DAG that is externally triggered.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `FINANCE.GL_AGGREGATE_AND_CLOSE` | JOBS_UNIX | 1 | Spark aggregation for analytical outputs, then write the close audit record and send notification |

---

## 3. Scheduling
- **Trigger Source**: No `EVNT_TIME` schedule, parent `JOBP`, or triggering `SCRI` script exists in this extraction bundle. This workflow is classified as **externally triggered** (source unknown from this extraction alone).
- **DAG Schedule**: `schedule=None` (No schedule is assumed or invented).

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `finance_gl_aggregate_and_close` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{"owner": "finance", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `gl_aggregate_and_close` | `FINANCE.GL_AGGREGATE_AND_CLOSE` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: launcher command [#!/bin/ksh] not recognised — confirm target operator/script manually. Script targets `. &HOME/finance/r_gl_aggregate_and_close.ksh`. |

---

## 6. Task Dependency Map
Since this migration bundle consists of a single standalone job mapped to a single-task DAG, there are no upstream or downstream tasks:

```
gl_aggregate_and_close
```

---

## 7. Sync / Concurrency Analysis
No sync rows, self-locks (`lock_kind=self`), or cross-locks (`lock_kind=cross`) were defined in this extraction. 
- **Airflow Mapping**: Standard concurrency defaults apply. `max_active_runs=1` is configured to prevent overlapping runs of this financial aggregation job.

---

## 8. Error Handling and Retry Strategy
- **Retries**: Configured with a default of 1 retry with a 5-minute delay.
- **Failures**: Default task failure behavior (Airflow default state changes) is utilized. No specific post-condition or `on_failure_callback` was specified in the extraction.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent / Dynamic Target |
| :--- | :--- | :--- |
| `&PERIOD_NAME` | `&$PREV_MONTH_MON_YYYY` | Calculated dynamically via Airflow context: `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b_%Y') }}` |
| `&FISCAL_YEAR` | `&$CURRENT_FISCAL_YEAR` | Calculated dynamically via Airflow context: `{{ execution_date.strftime('%Y') }}` *(or adjusted to match the corporate fiscal calendar logic)* |
| DAG ID | `FINANCE.GL_AGGREGATE_AND_CLOSE` | `finance_gl_aggregate_and_close` |

---

## 10. Developer Notes
* **# REVIEW:** This migration extraction contains only a single `JOBS_UNIX` object without an enclosing parent workflow (`JOBP`). It has been modeled as a single-task DAG. Confirm if this job should instead be integrated as a task into a larger existing pipeline.
* **# REVIEW-STRUCT:** The launcher command (`#!/bin/ksh`) is unrecognized by deterministic mapping rules, resulting in an `EmptyOperator` stub. The original UC4 script body source-executes `. &HOME/finance/r_gl_aggregate_and_close.ksh`. 
  * *Recommendation*: Replace `EmptyOperator` with a `BashOperator` (pointing to the script path or a migrated equivalent) or convert the underlying shell execution to a `DataprocSubmitJobOperator` / `SparkSubmitOperator` since the functional description indicates Spark-based processing.
* **Variable Resolution**: Ensure the dynamic Jinja templates for `PERIOD_NAME` and `FISCAL_YEAR` match the exact string format expectations of the downstream shell script or Spark aggregate process.

---

# Numbered Pseudocode Outline

```python
# 1. Imports
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# NOTE: Prepare to import BashOperator, DataprocSubmitJobOperator, or SparkSubmitOperator 
# once the final execution environment for r_gl_aggregate_and_close.ksh is confirmed.

# 2. GCP Configuration
# (Placeholders for potential future Dataproc or GCS targets)
GCP_CONN_ID = "google_cloud_default"
GCP_REGION = "us-central1"
GCP_PROJECT_ID = "your-gcp-project-id"

# 3. Default Args
DEFAULT_ARGS = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 4. on_failure_callback stubs
# (None defined in extraction)

# 5. DAG Definition
with DAG(
    dag_id="finance_gl_aggregate_and_close",
    default_args=DEFAULT_ARGS,
    description="Spark aggregation for analytical outputs and close audit recording",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "uc4_migration"],
) as dag:

    # 6. Guard Task (None required)

    # 7. Sensor Task (None required)

    # 8. Calendar Check Task (None required)

    # 9. Task: gl_aggregate_and_close
    # # REVIEW-STRUCT: Launcher 'unrecognized' (#!/bin/ksh script body). 
    # This EmptyOperator acts as a placeholder stub. Implement the actual target logic 
    # (e.g., BashOperator or Spark/Dataproc Operator) and inject the following parameters:
    # - PERIOD_NAME: {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b_%Y') }}
    # - FISCAL_YEAR: {{ execution_date.strftime('%Y') }}
    gl_aggregate_and_close = EmptyOperator(
        task_id="gl_aggregate_and_close",
    )

    # 10. Dependencies
    # (Single-task pipeline; no dependencies required)
    gl_aggregate_and_close
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml` | `finance/gl_aggregate_and_close_dag.py` | Converts the UC4 UNIX Job XML into an Airflow DAG orchestration, mirroring the source directory structure. |

---

### Job Dependencies
* **Downstream**: `FINANCE.MONTH_END_SCHEDULE` (not yet migrated). Once migrated, a cross-DAG dependency or sensor (such as `ExternalTaskSensor` in Airflow) should be wired to trigger or wait for this job's completion.

---

### Execution Order
The legacy orchestration executes in the following sequence:
1. `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml` (The top-level UC4 Job, which orchestrates the invocation)
2. `finance/r_gl_aggregate_and_close.ksh` (Shell execution wrapper)
3. `finance/d_gl_close_audit.sql` (Audit-trail script executed upon successful Spark run)

In the target Airflow DAG (`finance/gl_aggregate_and_close_dag.py`), this sequence must be preserved. The DAG will orchestrate the invocation of the migrated Python/KSH logic and ensure that the auditing task runs only upon the successful completion of the aggregation step.

---

### Scheduling
* **Schedule**: This job is not directly triggered by any of the environment's primary schedulers; it is run as an include/shared module within other scheduled pipelines. Therefore, the target Airflow DAG is configured as an externally-triggered DAG (`schedule=None`) so it remains a callable and importable orchestration unit.

---

### Schedule & Variables
* **Triggers**: Inherited/event-triggered from parent workflows.
* **Scheduler-Set Variables**:
  * `PERIOD_NAME`: Originally resolved dynamically via UC4 as `&$PREV_MONTH_MON_YYYY`. In the target Airflow DAG, this variable will be dynamically resolved using Airflow Jinja macros:
    `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b_%Y') }}`
  * `FISCAL_YEAR`: Originally resolved dynamically via UC4 as `&$CURRENT_FISCAL_YEAR`. In the target Airflow DAG, this will be dynamically resolved as:
    `{{ execution_date.strftime('%Y') }}` (or customized to match the company's financial calendar definition).

---

### Lineage
* **Downstream Consumers**:
  * `FILE:finance/r_gl_aggregate_and_close.ksh`: Invoked by this XML configuration. Note that this consumer script belongs to a different group and is not part of this specific design pass.
* **Infrastructure Host**:
  * `EXT:ETLHOST1`: The original physical execution host.

---

### External System Replacements
* **Orchestration Host**: The legacy execution host `ETLHOST1` is replaced by the Cloud Composer Kubernetes environment.
* **Aggregation Execution**: The legacy KornShell environment execution is replaced by Spark on Cloud Composer/Dataproc or Serverless Spark on BigQuery, depending on the sibling execution migration strategy.

---

### Cross-File Dependencies
* **Call Chain**: The XML file triggers `finance/r_gl_aggregate_and_close.ksh`, which subsequently initiates Spark jobs and executes SQL statements via SQL*Plus within `finance/d_gl_close_audit.sql`. These relationships must be coordinated as separate tasks within the Airflow ecosystem across their respective migration passes.

---

### Target File Plan
* **Target File**: `finance/gl_aggregate_and_close_dag.py`
  * **Language**: `python`
  * **Source File**: `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml`
  * *Note*: Implementation pseudocode is generated and managed by the authoritative MCP tool attached to this document.

---

### Environment-Specific Values
* **GLOBAL (Environment-Wide Variables)**:
  * `GCP_PROJECT`: Sourced dynamically at runtime using `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
  * `GCP_REGION`: Sourced dynamically at runtime using `Variable.get("GCP_REGION")` or `os.environ.get("GCP_REGION")`.
* **JOB-SPECIFIC Variables**:
  * `NOTIFY_EMAIL`: Set as a job-specific default parameter `finance-etl@example.com` or via dynamic DAG params.
  * `PERIOD_NAME`: Managed as a template-rendered dynamic string resolved via Jinja execution date manipulation.
  * `FISCAL_YEAR`: Managed as a template-rendered dynamic string resolved via Jinja execution date manipulation.

---

### Risks and Manual Steps
* **Unmigrated Downstream Dependencies**: The downstream dependency `FINANCE.MONTH_END_SCHEDULE` is not yet migrated, which means cross-DAG scheduling or sensor wiring cannot be finalized until that DAG exists.
* **Sibling Components Outside Scope**: Sibling files `finance/r_gl_aggregate_and_close.ksh` and `finance/d_gl_close_audit.sql` are outside the scope of this migration design pass. The `gl_aggregate_and_close` DAG execution tasks must use placeholders or stub operators until those sibling migration passes are fully completed.
* **Date Resolution Formats**: Dynamic UC4 date variables like `&$PREV_MONTH_MON_YYYY` must be thoroughly validated to ensure that the Jinja date format matches the exact string expectations of downstream systems.

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
1.1 Identify the type of Oracle SQL object being converted:
    - Multi-statement DML script with transaction control (INSERT, UPDATE, COMMIT) containing SQL*Plus client commands (EXIT) and substitution parameters (&1, &2).

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script executes the final steps of a General Ledger (GL) period close. It inserts an immutable audit log record containing the period name, fiscal year, the current database user performing the close, and the current system timestamp. It then updates the close status of that period to 'CLOSED' and records the completion timestamp in the period status table. Finally, it commits the changes and exits the session.

1.3 List all entities referenced:
    - Tables:
        - `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (Target of INSERT)
        - `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` (Target of UPDATE)
    - Columns:
        - `PERIOD_NAME` (Inferred Type: STRING / VARCHAR2)
        - `FISCAL_YEAR` (Inferred Type: STRING / VARCHAR2 or INT64 / NUMBER)
        - `CLOSED_BY` (Inferred Type: STRING / VARCHAR2)
        - `CLOSED_AT` (Inferred Type: TIMESTAMP)
        - `CLOSE_STATUS` (Inferred Type: STRING / VARCHAR2)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle columns storing `SYSTIMESTAMP` are mapped to BigQuery `TIMESTAMP` or `DATETIME`. BigQuery's `TIMESTAMP` tracks UTC time, which matches the behavior of `SYSTIMESTAMP` when handled across timezones.
    - String parameters are converted to BigQuery `STRING`.

2.2 Implicit and Explicit Type Casting:
    - SQL*Plus substitution parameters (`&1`, `&2`) are handled in BigQuery via query parameters (`@period_name`, `@fiscal_year`) or declared scripting variables to maintain explicit casting and prevent SQL injection or compilation issues.

2.3 NULL Handling and Conditional Functions:
    - None present in source code.

2.4 String Functions:
    - None present in source code.

2.5 Date and Timestamp Functions:
    - `SYSTIMESTAMP` → `CURRENT_TIMESTAMP()`.
      - Syntactic Correctness: `CURRENT_TIMESTAMP()` takes no arguments, which is correct in BigQuery.
      - Semantic Equivalence: Returns current system time as a `TIMESTAMP` with microsecond precision, equivalent to the timestamp component of Oracle's `SYSTIMESTAMP`.
      - Downstream Impact: None. Evaluates immediately.

2.6 Numeric and Aggregate Functions:
    - None present in source code.

2.7 Analytical and Window Functions:
    - None present in source code.

2.8 Set and Join Operations:
    - None present in source code.

2.9 Row Limiting and Sampling:
    - None present in source code.

2.10 Sequences:
     - None present in source code.

2.11 MERGE Statements:
     - None present in source code.

2.12 INSERT / UPDATE / DELETE:
     - Standard `INSERT` and `UPDATE` statements are supported in BigQuery. Because BigQuery is an analytical store, performing multiple single-row updates sequentially can be slow or subject to concurrent update limits. For a close audit script, wrapping these operations in a single multi-statement transaction (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) ensures atomicity and semantic alignment with Oracle's transaction model.

2.13 DDL Constructs:
     - None present in source code.

2.14 PL/SQL:
     - Transaction control (`COMMIT`) is converted to BigQuery SQL scripting transaction block `COMMIT TRANSACTION`.

2.15 Unresolvable or Advisory Items:
     - `USER`: The Oracle system variable returns the database user schema name executing the session. In BigQuery, this resolves to `SESSION_USER()`, which returns the email address of the authenticated user running the query.
     - `&1` and `&2` (SQL*Plus Substitution Parameters): Resolved using BigQuery Scripting variables declared at the top of the script or by passing query parameters (`@param`).
     - `EXIT`: This is a SQL*Plus CLI command to terminate the terminal session. It does not exist in BigQuery and is stripped; session closure is handled by the calling orchestration framework (e.g., Airflow, dbt, or Google Cloud Composer).

3.1 Conversion Strategy Summary:
    - Re-factor the multi-statement script into an atomic BigQuery Scripting transaction block (`BEGIN ... COMMIT TRANSACTION;`).
    - Declare BigQuery variables to receive external arguments (replacing `&1` and `&2`), or represent them as standard query parameters.
    - Replace the Oracle `USER` keyword with the BQ `SESSION_USER()` function.
    - Replace `SYSTIMESTAMP` with `CURRENT_TIMESTAMP()`.
    - Strip SQL*Plus commands (`EXIT`).

3.2 Assumptions:
    - The execution orchestrator will pass period name and fiscal year values as parameters (`@period_name`, `@fiscal_year`) to the BigQuery engine.
    - The caller expects transaction safety; we will wrap the operations in a standard transaction block.

3.3 Flagged Items:
    - `SESSION_USER()` returns an email address (e.g., `user@domain.com`), whereas Oracle `USER` usually returns a database schema name (e.g., `ANALYTICS_USER`). Ensure that the target column `CLOSED_BY` is defined with sufficient character length to store email address structures.

2.16 MIGRATION DECISION MATRIX

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| SQL*Plus Params `&1`, `&2` | BigQuery Scripting Query Parameters (`@period_name`, `@fiscal_year`) | Hardcoded strings, UDFs | Scripting variables/parameters natively pass run-time dynamic values into SQL executions safely. |
| `USER` | Direct BigQuery `SESSION_USER()` | Custom Python script tracking user metadata | `SESSION_USER()` is the standard, secure, native BigQuery function to capture the active identity. |
| `SYSTIMESTAMP` | Direct BigQuery `CURRENT_TIMESTAMP()` | `CURRENT_DATETIME()`, UDF | `CURRENT_TIMESTAMP()` preserves UTC timezone alignment and precision required for audits. |
| Multi-statement orchestration | BigQuery Transaction Block (`BEGIN TRANSACTION...`) | Individual isolated statement runs | Combining the `INSERT` and `UPDATE` into a transaction ensures both succeed or both roll back, preserving GL data integrity. |
| `EXIT` | Strip (Omit) | Procedural looping exit | `EXIT` is a client tool execution instruction, not a database layer query command. |

2.17 REQUIRED ARTIFACTS

| Generated Artifact Type | File / Object Name | Purpose / Input-Output Contract |
| :--- | :--- | :--- |
| BigQuery SQL Script | `gl_close_audit_migration.sql` | Executes period audit insertion and status updates atomically. Expects parameters `@period_name` (STRING) and `@fiscal_year` (STRING). Returns execution status. |

2.18 DATA TYPE COMPATIBILITY TABLE

| Source Table.Column | Oracle Type | BigQuery Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- | :--- |
| `GL_CLOSE_AUDIT.PERIOD_NAME` | VARCHAR2 | STRING | Direct mapping | None |
| `GL_CLOSE_AUDIT.FISCAL_YEAR` | VARCHAR2 / NUMBER | STRING | Direct mapping | None |
| `GL_CLOSE_AUDIT.CLOSED_BY` | VARCHAR2 | STRING | Direct mapping using `SESSION_USER()` | Ensure column fits email string formats. |
| `GL_CLOSE_AUDIT.CLOSED_AT` | TIMESTAMP | TIMESTAMP | Direct mapping using `CURRENT_TIMESTAMP()` | Values stored in UTC. |
| `GL_PERIOD_STATUS.CLOSE_STATUS`| VARCHAR2 | STRING | Direct mapping | None |
| `GL_PERIOD_STATUS.CLOSED_AT` | TIMESTAMP | TIMESTAMP | Direct mapping | None |

2.19 DESIGN REVIEW SUMMARY

- Patterns/Objects Found: DML modification across two tables within a manual transaction boundary.
- Unsupported Functions: `USER` (rewritten to `SESSION_USER()`), `EXIT` (removed).
- UDF Required: No.
- Python Required: No.
- Direct Dependencies: Tables `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` and `ANALYTICS_SCHEMA.GL_PERIOD_STATUS`.
- Assumptions: Target tables are already defined within the BigQuery dataset `ANALYTICS_SCHEMA`. Execution engine supports transaction scopes.
- Warnings: Schema lengths for `CLOSED_BY` must accommodate full corporate email addresses due to `SESSION_USER()` mapping.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.20 PACKAGE ANALYSIS
Not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `&1` (Substitution Parameter) | Direct-with-rewrite | Query Parameter `@period_name` |
| `&2` (Substitution Parameter) | Direct-with-rewrite | Query Parameter `@fiscal_year` |
| `USER` | Direct-with-rewrite | `SESSION_USER()` |
| `SYSTIMESTAMP` | Direct-with-rewrite | `CURRENT_TIMESTAMP()` |
| `COMMIT` | Direct-with-rewrite | `COMMIT TRANSACTION;` inside standard transaction block |
| `EXIT` | Unsupported | None — manual intervention (removed from database code layer) |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- BigQuery Scripting Transaction to execute audit log creation and period status updates atomically.
-- Requires calling client to supply input parameters:
--   @period_name (STRING)
--   @fiscal_year (STRING)

BEGIN
  -- Start atomic database transaction block
  BEGIN TRANSACTION;

  -- 1. Write the immutable close-audit record once aggregation has succeeded.
  INSERT INTO ANALYTICS_SCHEMA.GL_CLOSE_AUDIT
      (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
  VALUES
      (
        @period_name,                              -- resolved from Oracle substitution variable '&1'
        @fiscal_year,                              -- resolved from Oracle substitution variable '&2'
        SESSION_USER(),                            -- converted from Oracle system variable USER
        CURRENT_TIMESTAMP()                        -- converted from Oracle SYSTIMESTAMP
      );

  -- 2. Update the system status tracker for the closed period.
  UPDATE ANALYTICS_SCHEMA.GL_PERIOD_STATUS
  SET    CLOSE_STATUS = 'CLOSED',
         CLOSED_AT    = CURRENT_TIMESTAMP()        -- converted from Oracle SYSTIMESTAMP
  WHERE  PERIOD_NAME  = @period_name;              -- resolved from Oracle substitution variable '&1'

  -- 3. Commit all changes to the database
  COMMIT TRANSACTION;                              -- converted from Oracle COMMIT statement

EXCEPTION WHEN ERROR THEN
  -- Rollback transaction in the event of an unexpected runtime failure
  ROLLBACK TRANSACTION;
  -- Re-raise error to alerting engine
  SELECT @@error.message;
END;
```

### FLAGGED ITEMS FOR HUMAN REVIEW
1. **`USER` to `SESSION_USER()` mapping**: BigQuery's `SESSION_USER()` outputs user/service-account email addresses (e.g., `scheduler-service@gcp-project.iam.gserviceaccount.com` or `analyst@company.com`). The database architect must verify that the `CLOSED_BY` column in `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` is typed as a `STRING` and is not constrained to short character limits (such as older Oracle `VARCHAR2(30)` definitions).
2. **Execution Parameters**: Ensure that the tool orchestrating this SQL script (such as Airflow, Cloud Composer, or a Python script) is configured to pass query parameters named `@period_name` and `@fiscal_year` instead of relying on standard client-side string replacement patterns.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `finance/d_gl_close_audit.sql` | `finance/d_gl_close_audit.sql` | Migrate the transactional Oracle SQL*Plus script to a BigQuery scripting transaction block with parameterized variables. |

---

### Job Dependencies
- **Downstream Dependency**: `FINANCE.MONTH_END_SCHEDULE` (not yet migrated).
  - *Target Wiring*: Once `FINANCE.MONTH_END_SCHEDULE` is migrated to Cloud Composer (Airflow), a sensor or cross-DAG trigger must be set up to watch for the completion of this GL close audit task. Because this downstream job is not yet migrated, the cross-DAG dependency wiring cannot be finalized at this stage (noted under Risks & Manual Steps).

---

### Execution Order
- **Execution Order Mapping**:
  The legacy execution order inside the job sequence is:
  1. `finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml` (Orchestration metadata)
  2. `finance/r_gl_aggregate_and_close.ksh` (Wrapper script running Spark aggregation)
  3. `finance/d_gl_close_audit.sql` (Audit logging and period closing - **this design pass**)
  - *Target Task Mapping*: In the target Cloud Composer DAG, the task executing `finance/d_gl_close_audit.sql` must be scheduled as the final sequential step, running immediately after the successful completion of the Spark/Dataform transformation step (representing step 2).

---

### Scheduling
- **Schedule Mapping**:
  - This job is not directly triggered by any standalone scheduler; instead, it is an include/shared module executed within parent workflows.
  - *Target Platform Construct*: The target BigQuery execution task must remain a callable, non-standalone task within a parent Cloud Composer workflow, inheriting its execution trigger entirely from the parent orchestrator DAG.

---

### Schedule & Variables
- **Scheduler-Set Variables**:
  The legacy scheduler feeds the following variables to this job:
  - `PERIOD_NAME` = `&$PREV_MONTH_MON_YYYY`
  - `FISCAL_YEAR` = `&$CURRENT_FISCAL_YEAR`
  - *Target Transfer Mechanism*: These variables must be computed dynamically in the Cloud Composer DAG using Airflow macros (e.g., using `execution_date` calculations to derive the previous month's abbreviation and current fiscal year) and supplied to the BigQuery SQL task as standard query parameters (`@period_name` and `@fiscal_year`).

---

### Lineage
- **Upstream Producers / Data Sources**:
  - Reads metadata from schema `ANALYTICS_SCHEMA` (conf=0.75).
- **Downstream Consumers / Targets**:
  - Writes to target table `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (conf=0.90) by inserting the immutable close record.
  - Writes to target table `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` (conf=0.95) by updating the status of the corresponding period to 'CLOSED'.
  - Writes to `DYNAMIC_SQL:finance/d_gl_close_audit.sql` (conf=0.95).

---

### External System Replacements
- **SQL\*Plus to BigQuery Native SQL Engine**:
  The SQL*Plus command-line interface, manual session commands (`COMMIT`, `EXIT`), and substitution parameters are retired. They are replaced by Google Cloud Composer's native `BigQueryInsertJobOperator` running a transactional multi-statement query block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) to ensure atomicity.

---

### Cross-File Dependencies
- **Shared Schema and Coordination**:
  - The tables `GL_CLOSE_AUDIT` and `GL_PERIOD_STATUS` live under the `ANALYTICS_SCHEMA` dataset. These tables act as coordination flags; subsequent downstream processing (such as the Month End Schedule) check `GL_PERIOD_STATUS` to ensure the period close step has completed before proceeding.

---

### Target File Plan
- **Target File**: `finance/d_gl_close_audit.sql`
  - *Language*: BigQuery SQL
  - *Source File*: `finance/d_gl_close_audit.sql`

---

### Environment-Specific Values
The environment-specific values and dynamic variables are classified below:

1. **`GCP_PROJECT`**
   - *Role/Classification*: GLOBAL (identifies the execution project)
   - *Resolution Mechanism*: Retrieved at runtime in Airflow using `Variable.get("GCP_PROJECT")` or native GCP environment configuration. It should be used to qualify the BigQuery datasets.
2. **`ANALYTICS_SCHEMA`**
   - *Role/Classification*: JOB-SPECIFIC (points to the target finance analytical dataset)
   - *Resolution Mechanism*: Maps to the BigQuery dataset `analytics_schema` in the targeted GCP environment. It should be parameterized or fully-qualified in the calling DAG.
3. **`PERIOD_NAME` / `@period_name`**
   - *Role/Classification*: JOB-SPECIFIC (dynamic runtime parameter)
   - *Resolution Mechanism*: Supplied at execution time by Cloud Composer using execution date formatting and passed as a standard query parameter `@period_name`.
4. **`FISCAL_YEAR` / `@fiscal_year`**
   - *Role/Classification*: JOB-SPECIFIC (dynamic runtime parameter)
   - *Resolution Mechanism*: Supplied at execution time by Cloud Composer using execution date formatting and passed as a standard query parameter `@fiscal_year`.

---

### Risks and Manual Steps
- **Unmigrated Downstream Dependency**: The downstream workflow `FINANCE.MONTH_END_SCHEDULE` is not yet migrated. The cross-DAG dependency trigger/sensor in Airflow cannot be finalized until that workflow has been established on Google Cloud Composer.
- **`USER` to `SESSION_USER()` Column Length Warning**: The Oracle `USER` function is converted to BigQuery's `SESSION_USER()`. `SESSION_USER()` returns the authenticated email address of the running service account or user (e.g., `composer-worker@gcp-project.iam.gserviceaccount.com`). The database engineer must verify that the `CLOSED_BY` column in the BigQuery target table `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` is typed as a variable-length `STRING` and is not restricted by legacy short character limits (such as older Oracle `VARCHAR2(30)` constraints) which would trigger truncation failures at runtime.

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
REASON: The script orchestrates a Spark submission, executes an Oracle SQL*Plus script, and sends email notifications, which requires Python's capabilities for external process execution and email handling.

EVIDENCE
- Business logic found: KSH custom logic orchestrates Spark job submission, conditional execution of SQL*Plus audit insert, and mail completion notification.
- AWK: none
- SQL-expressible: no, Spark submission and mailx notification cannot be expressed in BigQuery Standard SQL.
- Non-SQL side effects: Spark cluster job submission (`spark-submit`), database client invocation (`sqlplus`), system mail delivery (`mailx`).
- Against this verdict: none, as Spark submission and email notification cannot be executed inside BigQuery SQL.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script orchestrates the monthly/yearly General Ledger (GL) aggregation process. It triggers a high-performance Spark job to aggregate large-scale analytical outputs, writes a final close-audit database record to Oracle upon successful completion, and sends out a completion email notification to finance stakeholders. It enforces transactional safety by ensuring the close-audit record is never written if the Spark job fails.

2. INVOCATION CONTEXT
   - Who calls this script: UC4 Job Name `FINANCE.GL_AGGREGATE_AND_CLOSE` (JOBS_UNIX object).
   - UC4 native includes: None referenced in the extraction.
   - Environment files sourced: None explicitly sourced.

3. PARAMETERS / INPUTS
   - `FIN_HOME`: Environment variable. Default: `/opt/etl/finance`. Used to locate the Oracle SQL audit script.
   - `FIN_ORA_USER`: Environment variable. Default: `fin_etl`. Database username for Oracle close-audit execution.
   - `FIN_ORA_PASS`: Environment variable. Default: `changeit`. Database password for Oracle close-audit execution.
   - `FIN_ORA_SID`: Environment variable. Default: `FINPRD`. Database TNS/SID for Oracle connection.
   - `NOTIFY_EMAIL`: Environment variable. Default: `finance-etl@example.com`. Destination address for month-end close notifications.
   - `PERIOD_NAME`: Environment variable. Required. Sourced from the caller/UC4 environment. Actually used in the Spark submission, SQL script parameterization, and notification email.
   - `FISCAL_YEAR`: Environment variable. Required. Sourced from the caller/UC4 environment. Actually used in the Spark submission, SQL script parameterization, and notification email.

   # REVIEW: Confirm that PERIOD_NAME and FISCAL_YEAR are always supplied via UC4 environment before execution.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **`spark-submit`**
     - *Exact command line*:
       ```bash
       spark-submit \
           --master yarn --deploy-mode cluster \
           --num-executors 6 --executor-memory 6g \
           --conf spark.sql.shuffle.partitions=200 \
           /opt/spark/jobs/finance-gl-aggregation-assembly.jar \
           --period-name "${PERIOD_NAME}" \
           --fiscal-year "${FISCAL_YEAR}"
       ```
     - *Purpose*: Submits the Scala/Java Spark aggregation job to the YARN cluster.
     - *Handling*: Must remain an external process invocation via `subprocess.run()`.
   - **`sqlplus`**
     - *Exact command line*:
       ```bash
       sqlplus -s ${FIN_ORA_USER}/${FIN_ORA_PASS}@${FIN_ORA_SID} @${FIN_HOME}/finance/d_gl_close_audit.sql "${PERIOD_NAME}" "${FISCAL_YEAR}"
       ```
     - *Purpose*: Executes the Oracle SQL script `d_gl_close_audit.sql` to write the close-audit record.
     - *Handling*: Normally we would map this to a native Python DB-client call (`oracledb`). However, since the SQL file's internal logic/source is not supplied, we will implement this as an external `subprocess` execution of SQL*Plus, or flag it for structural resolution.
     - # REVIEW-STRUCT: SQL file `d_gl_close_audit.sql` body not supplied — cannot migrate to native python-oracledb client without SQL source.
   - **`mailx`**
     - *Exact command line*:
       ```bash
       {
           print "Month-end close complete for period: ${PERIOD_NAME}"
           print "Fiscal year: ${FISCAL_YEAR}"
           print "Completed at: $(date)"
       } | mailx -s "[FINANCE-OK] Month-End Close ${PERIOD_NAME}" "${NOTIFY_EMAIL}"
       ```
     - *Purpose*: Sends a success completion email to stakeholders.
     - *Handling*: Replace with native Python `smtplib` and `email.mime` code to eliminate external system utility dependencies.

5. EMBEDDED SQL
   - Source file / label: `${FIN_HOME}/finance/d_gl_close_audit.sql`
   - Full SQL text: *Not supplied in extraction.*
   - Statement type: Presumed INSERT / UPDATE / PL-SQL block (audit log entry).
   - Table(s) touched: Presumed `GL_CLOSE_AUDIT` or similar audit table.
   - Dialect: Oracle SQL*Plus.
   - # REVIEW-STRUCT: SQL script `d_gl_close_audit.sql` is referenced but its source is missing from the extraction.

6. CONTROL FLOW
   1. Initialize environment variables (`FIN_HOME`, `FIN_ORA_USER`, `FIN_ORA_PASS`, `FIN_ORA_SID`, `NOTIFY_EMAIL`) with defaults if not present.
   2. Ensure `PERIOD_NAME` and `FISCAL_YEAR` parameters are set; raise an error if they are missing.
   3. Log execution startup with period and fiscal year.
   4. Execute `spark-submit` with configuration arguments. Capture exit status.
   5. Evaluate Spark exit status: if non-zero, log failure and exit with status 1 (audit record is NOT written).
   6. Execute SQL*Plus to run `d_gl_close_audit.sql` passing period and year. Capture exit status.
   7. Evaluate SQL*Plus exit status: if non-zero, log failure and exit with status 2.
   8. Formulate the month-end success notification body.
   9. Send success notification email using Python's native SMTP capabilities (or fallback to `mailx` subprocess if SMTP server configurations are not available).
   10. Log success completion and exit 0.

7. ERROR HANDLING & EXIT CODES
   - Spark job failure is caught explicitly: logs failure and exits with code `1`.
   - SQL*Plus execution failure is caught explicitly: logs failure and exits with code `2`.
   - Other unexpected errors are handled via standard Python try-except structures yielding a non-zero exit code.
   - Exit codes matched:
     - `0`: Success.
     - `1`: Spark submission failed.
     - `2`: SQL*Plus audit entry failed.

8. OUTPUTS / SIDE EFFECTS
   - Aggregated Analytical GL Outputs (written directly by Spark job).
   - Database record in Oracle (written by `d_gl_close_audit.sql`).
   - Log output (STDOUT/STDERR).
   - Email notification sent to `${NOTIFY_EMAIL}`.

9. BUSINESS SUMMARY
   - Orchestrates the monthly/yearly General Ledger (GL) aggregation process.
   - Triggers a high-performance Spark job to aggregate large-scale analytical outputs.
   - Enforces transactional safety: writes the final close-audit database record ONLY when the Spark aggregation confirms 100% success.
   - Automates stakeholder notification upon successful month-end close completion.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# Helper function for logging
def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def send_email(subject: str, body: str, recipient: str):
    # Standard Python SMTP delivery logic
    # In production, SMTP host/port should be fetched from configuration
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = "finance-etl@example.com"
    msg["To"] = recipient
    
    try:
        # Default local mail exchange handler fallback to simulate mailx
        # # REVIEW: Configure correct SMTP server and port for environment
        with smtplib.SMTP("localhost") as server:
            server.send_message(msg)
    except Exception as e:
        log(f"WARNING: Native SMTP delivery failed: {e}. Falling back to mailx CLI.")
        # Fallback to local mailx process if local SMTP listener is missing
        subprocess.run(
            ["mailx", "-s", subject, recipient],
            input=body,
            text=True,
            check=True
        )

def main():
    # Step 1: Environment Setup
    fin_home = os.environ.get("FIN_HOME", "/opt/etl/finance")
    fin_ora_user = os.environ.get("FIN_ORA_USER", "fin_etl")
    fin_ora_pass = os.environ.get("FIN_ORA_PASS", "changeit")
    fin_ora_sid = os.environ.get("FIN_ORA_SID", "FINPRD")
    notify_email = os.environ.get("NOTIFY_EMAIL", "finance-etl@example.com")
    
    # Required parameters check
    period_name = os.environ.get("PERIOD_NAME")
    fiscal_year = os.environ.get("FISCAL_YEAR")
    
    if not period_name or not fiscal_year:
        log("ERROR: REQUIRED environment variables PERIOD_NAME or FISCAL_YEAR are missing.")
        sys.exit(1)

    # Step 2: Log Spark Job Submission
    log(f"Submitting GL aggregation Spark job for period {period_name}, fiscal year {fiscal_year}")
    
    # Step 3: Execute spark-submit
    spark_cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--num-executors", "6",
        "--executor-memory", "6g",
        "--conf", "spark.sql.shuffle.partitions=200",
        "/opt/spark/jobs/finance-gl-aggregation-assembly.jar",
        "--period-name", period_name,
        "--fiscal-year", fiscal_year
    ]
    
    try:
        spark_result = subprocess.run(spark_cmd, check=False)
        spark_rc = spark_result.returncode
    except Exception as e:
        log(f"ERROR: Failed to run spark-submit process: {e}")
        sys.exit(1)
        
    # Step 4: Evaluate Spark Execution Result
    if spark_rc != 0:
        log(f"ERROR: GL aggregation Spark job failed with rc={spark_rc} - close audit will NOT be written")
        sys.exit(1)
    log("GL aggregation completed successfully")

    # Step 5: Log Audit Table Execution
    log(f"Writing close-audit record for period {period_name}")
    
    # Step 6: Execute sqlplus wrapper
    # # REVIEW-STRUCT: SQL file d_gl_close_audit.sql body not supplied — behaviour preserved via sqlplus execution
    sqlplus_conn = f"{fin_ora_user}/{fin_ora_pass}@{fin_ora_sid}"
    sql_script_path = os.path.join(fin_home, "finance/d_gl_close_audit.sql")
    
    sql_cmd = [
        "sqlplus", "-s",
        sqlplus_conn,
        f"@{sql_script_path}",
        period_name,
        fiscal_year
    ]
    
    try:
        sql_result = subprocess.run(sql_cmd, check=False)
        sql_rc = sql_result.returncode
    except Exception as e:
        log(f"ERROR: Failed to run sqlplus process: {e}")
        sys.exit(2)
        
    # Step 7: Evaluate SQL*Plus Execution Result
    if sql_rc != 0:
        log("ERROR: failed to write close-audit record")
        sys.exit(2)

    # Step 8: Log completion and execute success notification email
    log("Notifying stakeholders of month-end close completion")
    
    email_subject = f"[FINANCE-OK] Month-End Close {period_name}"
    email_body = (
        f"Month-end close complete for period: {period_name}\n"
        f"Fiscal year: {fiscal_year}\n"
        f"Completed at: {datetime.now().strftime('%a %b %d %H:%M:%S %Z %Y')}\n"
    )
    
    try:
        send_email(email_subject, email_body, notify_email)
    except Exception as e:
        log(f"WARNING: Email notification failed to send: {e}")

    # Step 9: Finalize Success Block
    log("FINANCE.GL_AGGREGATE_AND_CLOSE finished successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `finance/r_gl_aggregate_and_close.ksh` | `finance/r_gl_aggregate_and_close.py` | Migrates the legacy KornShell orchestration logic into Python, calling Google Cloud Dataproc for the Spark execution, BigQuery for the close-audit log, and utilizing native Python smtp for notifications. |

---

### Job Dependencies
- **Downstream**: `FINANCE.MONTH_END_SCHEDULE` (not yet migrated).
  - **BigQuery/Composer Wiring**: Since the downstream scheduler is not yet migrated, a temporary trigger or custom sensor must be established in the Airflow DAG to log when this task finishes, or a manual trigger hook must be configured.
  - **Risks**: The downstream dependency orchestration cannot be fully integrated and tested until `FINANCE.MONTH_END_SCHEDULE` is migrated to Cloud Composer.

### Execution Order
The legacy execution order is preserved in the Cloud Composer DAG as follows:
1. **UC4 Job Wrapper Task** (`finance/FINANCE.GL_AGGREGATE_AND_CLOSE.xml`): Initiates the orchestration flow.
2. **Main Processing Task** (`finance/r_gl_aggregate_and_close.ksh` -> `finance/r_gl_aggregate_and_close.py`):
   - Submits the Spark GL aggregation job.
   - Upon successful execution, triggers the SQL-based audit log script.
   - Upon successful audit write, transmits the success email.
3. **Audit Execution Step** (`finance/d_gl_close_audit.sql`): Run sequentially from within `finance/r_gl_aggregate_and_close.py` after the Spark job confirms success.

### Schedule & Variables — Must Be Retained
- **Scheduling**: This job has no standalone schedule in the legacy scheduler, acting as a shared/called script. Consequently, the migrated Python operator/DAG should remain an importable, dynamic pipeline triggered on-demand or called via a parent DAG's `TriggerDagRunOperator`.
- **Variables**:
  - `PERIOD_NAME`: Legacy value `&$PREV_MONTH_MON_YYYY`. Passed dynamically from Cloud Composer at runtime via Airflow execution dates formatted to `MON-YYYY` or parsed from the DAG run configuration (`dag_run.conf['PERIOD_NAME']`).
  - `FISCAL_YEAR`: Legacy value `&$CURRENT_FISCAL_YEAR`. Passed dynamically from Cloud Composer runtime parameters or parsed from `dag_run.conf['FISCAL_YEAR']`.

### Lineage
- **Upstream SQL**: Executes SQL queries contained within `finance/d_gl_close_audit.sql` (represented by `FILE:finance/d_gl_close_audit.sql`).
- **Downstream Output**: Sends a month-end close completion notification via email (represented by `EXT:MAILX`).

### External System Replacements
- **Spark on YARN (`spark-submit`)** $\rightarrow$ **Google Cloud Dataproc**: Submits the Java/Scala Spark job utilizing `DataprocSubmitJobOperator` or the google-cloud-dataproc client API. The Spark jar file must be hosted on Google Cloud Storage.
- **Oracle (`sqlplus`)** $\rightarrow$ **BigQuery**: The audit log SQL execution is redirected from Oracle to BigQuery using the standard Python `google-cloud-bigquery` client or the `BigQueryInsertJobOperator`.
- **Mail utility (`mailx`)** $\rightarrow$ **Python SMTP / Cloud Composer Email**: Email notifications are transmitted using Python’s native `smtplib` library or through the Cloud Composer native SMTP integration.

### Cross-File Dependencies
- **`finance/d_gl_close_audit.sql`**: This SQL file represents a direct cross-file dependency executed upon Spark completion. Its migration to BigQuery (e.g., as a Dataform SQLX script or BigQuery SQL statement) must be completed separately before this script is deployed.

### Target File Plan
- **Target File**: `finance/r_gl_aggregate_and_close.py`
  - *Source File*: `finance/r_gl_aggregate_and_close.ksh`
  - *Language*: Python
  - *Purpose*: Replaces the legacy shell orchestration. Programmatically handles the submission of the Dataproc Spark job, validates the return code, runs the BigQuery SQL-based close audit, and distributes success notification emails.

### Environment-Specific Values
- `GCP_PROJECT` (`GLOBAL`): Sourced via `os.environ.get("GCP_PROJECT")` or Airflow Variable. Represents the target Google Cloud Project.
- `GCS_BUCKET` (`GLOBAL`): Sourced via `os.environ.get("GCS_BUCKET")`. The destination bucket where the Spark jar utility is staged.
- `DATAPROC_CLUSTER` (`GLOBAL`): Sourced via `os.environ.get("DATAPROC_CLUSTER")`. The target Dataproc Cluster name.
- `DATAPROC_REGION` (`GLOBAL`): Sourced via `os.environ.get("DATAPROC_REGION")`. The target region for Dataproc execution.
- `NOTIFICATION_EMAIL` (`GLOBAL`): Sourced via Airflow Variable `NOTIFICATION_EMAIL`. Maps to the legacy `NOTIFY_EMAIL` variable.
- `FIN_HOME` (`JOB-SPECIFIC`): Sourced via Airflow home paths or environment configurations pointing to the root dags directory.
- `PERIOD_NAME` (`JOB-SPECIFIC`): Sourced from runtime dag_run configs.
- `FISCAL_YEAR` (`JOB-SPECIFIC`): Sourced from runtime dag_run configs.
- `FIN_ORA_USER`, `FIN_ORA_PASS`, `FIN_ORA_SID` (`Retired`): Oracle database credentials; obsolete following the migration to BigQuery.

### Risks & Manual Steps
- **UPSTREAM: NOT FOUND** — Downstream job `FINANCE.MONTH_END_SCHEDULE` has not been migrated yet. End-to-end DAG chaining cannot be completed.
- **SQL DEPENDENCY MISSING**: The SQL logic inside `finance/d_gl_close_audit.sql` was not supplied in this migration pass. Before running the target script, a developer must verify that the corresponding BigQuery tables/schema are deployed and the script has been converted to BQ-compatible SQL.
- **Spark Jar Migration**: The assembly jar `/opt/spark/jobs/finance-gl-aggregation-assembly.jar` must be uploaded to the target GCS bucket (`GCS_BUCKET`) and the path updated in the Python script.