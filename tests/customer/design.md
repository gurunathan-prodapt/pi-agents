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

## 1. Overview
This migration design captures a single, isolated UC4 UNIX job: `CUSTOMER.HISTORIZATION_LOAD`. The job is responsible for executing the Slowly Changing Dimension Type 2 (SCD2) historization process, loading weekly customer segment and score data into the customer segment dimension. Since no wrapping workflow (`JOBP`) or schedule was provided in this extraction bundle, this job is represented as an independent Airflow DAG configured for external manual triggering or upstream integration.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `CUSTOMER.HISTORIZATION_LOAD` | JOBS_UNIX | 1 (Active) | SCD2 historization of the weekly customer segment/score into the segment dimension |

---

## 3. Scheduling
- **Trigger Source**: No `EVNT_TIME` or scheduling definitions are present in this extraction bundle. There are no native triggering scripts (`SCRI`) or master workflows (`JOBP`) referencing this object within this export.
- **Airflow Schedule**: `schedule=None` (Externally triggered or run on-demand).
- **Target DAG Property**: `schedule=None`.

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `customer_historization_load` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Mapped from Active=1) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `customer_historization_load` | `CUSTOMER.HISTORIZATION_LOAD` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: Launcher command `#!/bin/ksh` not recognised — confirm target operator/script manually. Under UC4, this executed `. &HOME/customer/r_historization_load.ksh`. |

---

## 6. Task Dependency Map
Since this DAG contains only a single task representing the migrated `JOBS_UNIX` object, there are no dependencies to map:

```python
customer_historization_load
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` (self or cross locks) were defined in the extraction for this object.
- **Airflow Mapping**: standard `max_active_runs=1` is applied to prevent concurrent runs of the historization process.

---

## 8. Error Handling and Retry Strategy
- Default task retries are set to `1` with a `5` minute delay.
- No postcondition actions, alerts, or complex failure execution paths are specified in the extraction.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&RUN_DATE` | `&$TODAY` (UC4 System Date) | `{{ ds }}` (Airflow Logical Date string `YYYY-MM-DD`) |

---

## 10. Developer Notes
- **# REVIEW-STRUCT: Unrecognized Launcher**: The original execution script leverages a KornShell (`#!/bin/ksh`) wrapper script executing `. &HOME/customer/r_historization_load.ksh` on a target host (`|ETLHOST2|HOST`). The Airflow representation has been modeled using an `EmptyOperator` stub. The developer must determine if this should be implemented using an `SSHOperator` to target `ETLHOST2`, or if the shell script logic should be containerized and run via `GKEStartPodOperator` / local `BashOperator` if migrated to the Airflow runner.
- **Date Variable**: The UC4 script explicitly sets `:SET &RUN_DATE='&$TODAY'`. In the target Airflow task implementation, use the Airflow template variable `{{ ds }}` or `{{ logical_date }}` to maintain runtime context compatibility.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP configurations identified in this extraction

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No failure callbacks specified in UC4 extraction

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id='customer_historization_load',
    default_args=DEFAULT_ARGS,
    description='SCD2 historization of the weekly customer segment/score into the segment dimension',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: customer_historization_load ─────────────────
    # # REVIEW-STRUCT: launcher command [#!/bin/ksh] not recognised — confirm target operator/script manually.
    # Original command context:
    #   :SET &RUN_DATE='&$TODAY'
    #   . &HOME/customer/r_historization_load.ksh
    # Target Host: |ETLHOST2|HOST
    # Target Login: UNIX.ETL_SVC
    customer_historization_load = EmptyOperator(
        task_id='customer_historization_load',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task DAG; no dependency chain required.
    customer_historization_load
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/CUSTOMER.HISTORIZATION_LOAD.xml` | `dags/customer/customer_historization_load.py` | Migrates the UC4 UNIX job definition into a Python-based Apache Airflow DAG to orchestrate the historization load sequence. |

# Add Context the MCP Could Not See

### Job dependencies
* **Downstream Job**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated).
  * **BigQuery/Airflow Wiring**: This downstream job should be triggered via an `ExternalTaskSensor` configured in the downstream DAG to sense the completion of the `customer_historization_load` task, or directly triggered via a `TriggerDagRunOperator` as the terminal step of this DAG once the downstream DAG is migrated.

### Execution order
The target orchestration (Airflow DAG) preserves the execution sequence defined by the legacy execution order:
1. **Orchestration Entry**: `CUSTOMER.HISTORIZATION_LOAD.xml` is mapped to the Airflow DAG definition in `dags/customer/customer_historization_load.py`.
2. **Wrapper Script**: `r_historization_load.ksh` is mapped to an Airflow task (`r_historization_load` executing the migrated Python wrapper equivalent).
3. **Execution Script**: `k_historization_load.ksh` is mapped to an Airflow task (`k_historization_load` executing the migrated Python controller equivalent).
4. **SCD2 Historization**: `d_historization_load.sql` is mapped to a Dataform invocation task or BigQuery operator executing the migrated SQL logic.
5. **Quality Check**: `d_segment_quality_check.sql` is mapped to a Dataform assertion or BigQuery-based validation task verifying the shift percentage.

*Note: Steps 2 through 5 represent separate components whose actual script conversions are handled by their own independent migration design passes.*

### Scheduling
* **Triggering Pattern**: This job is not directly triggered by any standalone scheduler; it executes inside other workflows/scheduled jobs (e.g., as an include/shared module).
* **Airflow Configuration**: The migrated Airflow DAG is configured with `schedule=None`. It must remain an on-demand, callable unit triggered either manually, by an upstream DAG run via a `TriggerDagRunOperator`, or by an external Airflow dataset/event trigger.

### Schedule & variables
* **Inherited / Event Triggering**: The legacy job executes as an included component or is called dynamically. The equivalent scheduling approach in Airflow is setting `schedule=None`.
* **Scheduler-Set Variables**:
  * `RUN_DATE` (Value: `'&$TODAY'`): Maps to the standard Airflow execution date macro `{{ ds }}`. To ensure support for manual runs and historic backfills, it should be defined using DAG run parameters with a logical date fallback, such as: `{{ dag_run.conf.get('RUN_DATE', ds) }}`.

### Lineage
* **Downstream Consumers**:
  * `CUSTOMER.WEEKLY_SCHEDULE` (job: `CUSTOMER.WEEKLY_SCHEDULE`): A downstream consumer of this job's outcome, which is a cross-job hand-off to reference on BigQuery.
* **Lineage Edges Analysis**:
  * Invokes `FILE:customer/r_historization_load.ksh` (conf=0.90) - handled by another pass, called as a child task.
  * Runs on host `EXT:ETLHOST2` (conf=0.85) - mapped to Google Cloud infrastructure execution environment.
  * Writes to Table `THE` & Table `OF` (conf=0.80) - identified as likely parsing remnants/hallucinations from the legacy metadata extractor. No tables by these names should be created.

### External system replacements
* **Legacy Host (`ETLHOST2`) & Login (`UNIX.ETL_SVC`)**: Migrated execution environment runs on Cloud Composer (GKE-managed workers) or a dedicated GKE Pod, utilizing Google Cloud IAM service accounts with appropriate roles (e.g., BigQuery Admin, Composer Worker) instead of legacy SSH credentials.

### Cross-file dependencies
* **Direct Invocation**: This UC4 XML job directly targets the execution of `customer/r_historization_load.ksh`.
* **Target Integration**: The DAG task `r_historization_load` must call the migrated Python equivalent of the wrapper script. This represents a modular dependency across files that are integrated at runtime in the dags folder structure.

### Target file plan
* **Target File**: `dags/customer/customer_historization_load.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `customer/CUSTOMER.HISTORIZATION_LOAD.xml`
  * **Purpose**: Defines the Airflow DAG container, properties, default parameters, and the orchestration sequence stubbing the execution of the historization wrapper script.

### Environment-specific values
1. **GLOBAL (environment-wide)**:
   * `GCP_PROJECT`: Identifies the target Google Cloud project. To be sourced via `GCP_PROJECT = os.environ.get("GCP_PROJECT")` or Airflow variables.
   * `GCP_REGION`: The GCP region for execution (e.g., `us-central1`). Sourced via `os.environ.get("GCP_REGION")`.
2. **JOB-SPECIFIC**:
   * `RUN_DATE`: Logical date of execution. Populated dynamically in the DAG using the Jinja template `{{ ds }}` (or `{{ dag_run.conf.get('RUN_DATE', ds) }}`).
   * `MAX_EXPECTED_CHANGE_PCT` (Value: `25`): Segment quality check percentage limit. Defined in a job-specific dictionary within the DAG file: `JOB_CONFIG = {"MAX_EXPECTED_CHANGE_PCT": 25}`.

### Risks and manual steps
* **SOURCE: NOT FOUND** — `customer/r_historization_load.ksh` — Candidate: `customer/r_historization_load.ksh` (This wrapper script is a sibling component handled in a separate migration pass. The DAG task invocation cannot be finalized/compiled as a working call until the Python conversion of this wrapper is delivered).
* **DOWNSTREAM: NOT YET MIGRATED** — `CUSTOMER.WEEKLY_SCHEDULE`. The target DAG cannot configure its final downstream hand-off or triggering mechanism until the downstream scheduling job is migrated to Cloud Composer.
* **PARSING ANOMALY**: Lineage edges refer to Table `THE` and Table `OF` being written by this job. These represent parsing artifacts from the description or logs of the legacy job and should be ignored during physical schema setup.
* **EXECUTION ENVIRONMENT RE-ARCHITECTURE**: The legacy script uses KornShell and runs on a remote server (`ETLHOST2`). A design decision is required to either:
  1. Migrate the shell logic entirely to Python (highly recommended, handled in the sibling migration pass) and run it natively within the Airflow environment.
  2. Maintain a shell-execution model using the `SSHOperator` targeting a containerized worker if native Python rewrite cannot be completed.

---

=== FILE: customer/d_historization_load.sql ===
-- d_historization_load.sql
-- SCD Type 2 merge of the weekly customer score/segment into the segment
-- dimension. A customer's prior current row is expired only when the
-- segment or score band actually changed.
-- Schema: ANALYTICS_SCHEMA

MERGE INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT tgt
USING (
    SELECT CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE
    FROM   ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT
    WHERE  RUN_DATE = TO_DATE('&1', 'YYYY-MM-DD')
) src
ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 1)
WHEN MATCHED AND (
         tgt.SEGMENT_CODE <> src.SEGMENT_CODE
      OR tgt.SCORE_BAND   <> src.SCORE_BAND
     ) THEN
    UPDATE SET tgt.IS_CURRENT = 0,
               tgt.VALID_TO   = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
    VALUES (src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, src.SCORE_VALUE, 1, SYSDATE);

INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
    (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
SELECT src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, src.SCORE_VALUE, 1, SYSDATE
FROM   ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT src
JOIN   ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT tgt
  ON   tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 0 AND tgt.VALID_TO = SYSDATE
WHERE  src.RUN_DATE = TO_DATE('&1', 'YYYY-MM-DD');

COMMIT;
EXIT;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════
 
Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - Multi-statement DML script with transaction control (MERGE, INSERT, COMMIT).
1.2 Business Logic Summary:
    - This script performs a Slowly Changing Dimension (SCD) Type 2 load of weekly customer segments and scores into the `DIM_CUSTOMER_SEGMENT` table.
    - It uses a classic two-step approach:
      1. A MERGE statement that identifies existing active target rows (`IS_CURRENT = 1`) where the staging data (`SEGMENT_CODE` or `SCORE_BAND`) has changed. It marks these target records as expired (`IS_CURRENT = 0`, `VALID_TO = SYSDATE`). It also inserts brand-new customers who do not yet exist in the target dimension.
      2. An INSERT statement that inserts the new active version of the records for the customers who were just expired in the MERGE step. This is identified by joining the staging table back to the target table on the matching `CUSTOMER_ID`, `IS_CURRENT = 0`, and `VALID_TO = SYSDATE` (which represents the rows just expired).
1.3 Entities Referenced:
    - `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` (Target Table `tgt`):
      - `CUSTOMER_ID` (Inferred Type: STRING/INT64)
      - `SEGMENT_CODE` (Inferred Type: STRING)
      - `SCORE_BAND` (Inferred Type: STRING)
      - `SCORE_VALUE` (Inferred Type: NUMERIC)
      - `IS_CURRENT` (Inferred Type: INT64)
      - `VALID_FROM` (Inferred Type: TIMESTAMP)
      - `VALID_TO` (Inferred Type: TIMESTAMP)
    - `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` (Source Staging Table `src`):
      - `CUSTOMER_ID` (Inferred Type: STRING/INT64)
      - `SEGMENT_CODE` (Inferred Type: STRING)
      - `SCORE_BAND` (Inferred Type: STRING)
      - `SCORE_VALUE` (Inferred Type: NUMERIC)
      - `RUN_DATE` (Inferred Type: DATE)
 
Step 2: Oracle-Specific Construct Detection and Resolution
 
2.1 Data Type Conversions:
    - Oracle `DATE` (used for `VALID_FROM` and `VALID_TO` which tracks execution time) → Map to BigQuery `TIMESTAMP`.
    - Oracle `DATE` (used for `RUN_DATE` partitions) → Map to BigQuery `DATE`.
    - Oracle `NUMBER` (for flags like `IS_CURRENT`) → Map to BigQuery `INT64`.
    - Oracle `NUMBER` (for scores like `SCORE_VALUE`) → Map to BigQuery `NUMERIC`.
    - Oracle `VARCHAR2` (for segments/bands) → Map to BigQuery `STRING`.
 
2.2 Implicit and Explicit Type Casting:
    - Oracle `&1` (SQL*Plus parameter) converted via `TO_DATE('&1', 'YYYY-MM-DD')` → Declare a BigQuery scripting variable `v_run_date_str STRING` and convert explicitly using `PARSE_DATE('%Y-%m-%d', v_run_date_str)`.
 
2.3 NULL Handling and Conditional Functions:
    - None used in this script.
 
2.4 String Functions:
    - None used in this script.
 
2.5 Date and Timestamp Functions:
    - `SYSDATE` → Map to BigQuery `CURRENT_TIMESTAMP()`. To ensure transaction-level alignment and absolute matching consistency between the MERGE (expiration) and the INSERT (new active version) steps, `SYSDATE` is evaluated once and captured into a script-level local variable `v_current_timestamp` at the start of execution.
    - `TO_DATE` → Map to BigQuery `PARSE_DATE` with syntax `PARSE_DATE('%Y-%m-%d', ...)`.
 
2.6 Numeric and Aggregate Functions:
    - None used in this script.
 
2.7 Analytical and Window Functions:
    - None used in this script.
 
2.8 Set and Join Operations:
    - Implicit Inner Join used in the final INSERT. Will be represented as an explicit `INNER JOIN` in BigQuery.
 
2.9 Row Limiting and Sampling:
    - None used in this script.
 
2.10 Sequences:
    - None used in this script.
 
2.11 MERGE Statements:
    - BigQuery supports the standard `MERGE` statement. The Oracle syntax mapping is directly compatible, except we must ensure the source query is isolated and types match.
 
2.12 INSERT / UPDATE / DELETE:
    - The secondary `INSERT SELECT` statement is directly translatable to BigQuery syntax.
 
2.13 DDL Constructs:
    - No DDL present. Transaction logic (COMMIT, EXIT) is handled via BigQuery Scripting Transactions (`BEGIN TRANSACTION` / `COMMIT TRANSACTION`).
 
2.14 PL/SQL:
    - Multi-statement SQL*Plus script converted to a clean BigQuery procedural scripting block (`BEGIN ... END`) containing local variable declarations, transaction boundaries, and procedural statements.
 
2.15 Unresolvable or Advisory Items:
    - Substitution variables (`&1`) are SQL*Plus concepts. In BigQuery, these are represented as parameterized values or scripting variables. They are mapped to a declared variable `v_run_date_str` which must be passed as a query parameter or set dynamically.
 
Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    - A procedural BigQuery scripting block wrapped inside a `BEGIN TRANSACTION` / `COMMIT TRANSACTION` to ensure consistency and exact functional replication of the sequential SCD Type 2 logic.
3.2 Assumptions:
    - The value of `&1` is provided at execution time as a parameter or parameter-assigned variable.
    - The staging table matches the target schema data types.
3.3 Items Flagged for Human Review:
    - SQL*Plus execution parameter assignment (`&1`). Ensure the orchestrator (e.g., Airflow, dbt, or gcloud) maps the run date to the parameter.
 
═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════
 
| Source Construct / Statement | Selected Target | Rejected Alternatives | Reason for Selection / Evidence |
| :--- | :--- | :--- | :--- |
| **SQL\*Plus Script with multi-DML** | BigQuery Scripting Block (`BEGIN...END`) | Python Wrapper, Scheduled Queries | Scripting blocks naturally support multi-statement transactions and variable declaration without the overhead of external runtimes. |
| **SCD Type 2 Load Execution** | BigQuery Transaction Block (`BEGIN TRANSACTION...COMMIT TRANSACTION`) | Separate non-transactional statements | Absolute consistency is required. The second INSERT relies on joining rows updated in the preceding MERGE step based on the exact same execution timestamp. |
| **`SYSDATE`** | Local Script Variable `v_current_timestamp` | Inline `CURRENT_TIMESTAMP()` | To maintain referential integrity between the MERGE (expiration) and subsequent INSERT (creation), the execution timestamp must be identical across statements. |
| **`TO_DATE('&1', 'YYYY-MM-DD')`** | `PARSE_DATE('%Y-%m-%d', v_run_date_str)` | `CAST(x AS DATE)` | Safe, explicit date formatting match. Protects against varying environment-level default formats. |
 
═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **Artifact Type**: BigQuery Standard SQL Script (Scripting Block).
- **Execution Interface**: To be executed via the BigQuery client API, dbt, or airflow passing a query parameter named `run_date_param` for the substitution variable.
 
═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════
 
| Oracle Column / Type | BigQuery Target Type | Conversion Rule / Logic | Warnings / Comments |
| :--- | :--- | :--- | :--- |
| `CUSTOMER_ID` (NUMBER/VARCHAR2) | `STRING` | Direct mapping (assuming alphanumeric identity) | If numerical index, map to `INT64`. Treat here as `STRING` or `INT64` generic. |
| `SEGMENT_CODE` (VARCHAR2) | `STRING` | Direct conversion. | None. |
| `SCORE_BAND` (VARCHAR2) | `STRING` | Direct conversion. | None. |
| `SCORE_VALUE` (NUMBER) | `NUMERIC` | Maps to BigQuery NUMERIC to preserve fixed precision. | Safe from floating-point errors. |
| `IS_CURRENT` (NUMBER) | `INT64` | Maps to INT64 since value is restricted to {0, 1}. | None. |
| `VALID_FROM` (DATE) | `TIMESTAMP` | Maps to TIMESTAMP to preserve granular time details of SYSDATE. | None. |
| `VALID_TO` (DATE) | `TIMESTAMP` | Maps to TIMESTAMP to preserve granular time details of SYSDATE. | None. |
| `RUN_DATE` (DATE) | `DATE` | Maps to DATE as it only represents a year-month-day business partition. | None. |
 
═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: SCD Type 2 MERGE pattern, SQL*Plus variables, sequential dependent DML statements, session commit/exit.
- **Unsupported Functions**: Oracle `TO_DATE` (rewritten to `PARSE_DATE`), `SYSDATE` (rewritten to local script variable storing `CURRENT_TIMESTAMP()`), `&1` SQL*Plus variable (replaced with scripting parameter).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Target `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`, Source Staging `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`.
- **Assumptions**: Staging and target tables are pre-existing in BigQuery with clean mapping schemas. The orchestrator supplies a valid date string as a runtime parameter.
- **Ready for Human Approval**: Yes.
 
═══════════════════════════════════════════
2.20 PACKAGE ANALYSIS
═══════════════════════════════════════════
- Not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.
 
═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════
 
| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_DATE('&1', 'YYYY-MM-DD')` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', @run_date_param)` |
| `SYSDATE` | Direct-with-rewrite | `CURRENT_TIMESTAMP()` mapped to a scalar variable to prevent runtime drift. |
| `MERGE` | Direct | Standard BigQuery `MERGE` statement. |
| `COMMIT` | Direct-with-rewrite | Script-level `COMMIT TRANSACTION`. |
| `EXIT` | Direct-with-rewrite | End of standard Scripting block boundary. |
 
<br>
 
═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════
 
Step 4: Write Vendor-Neutral Pseudocode
 
```sql
-- BigQuery Scripting block to emulate the Oracle PL/SQL transaction environment
BEGIN
  -- Declare SQL*Plus replacement variable
  DECLARE v_run_date_str STRING;
  DECLARE v_run_date DATE;
  DECLARE v_current_timestamp TIMESTAMP;

  -- Assign input run date string from orchestration parameter (represents '&1')
  SET v_run_date_str = @run_date_param;
  
  -- Convert input parameter to standard DATE format
  SET v_run_date = PARSE_DATE('%Y-%m-%d', v_run_date_str); -- converted from TO_DATE('&1', 'YYYY-MM-DD')
  
  -- Lock execution timestamp for consistent SCD transaction boundaries
  SET v_current_timestamp = CURRENT_TIMESTAMP(); -- converted from SYSDATE

  -- Start of multi-statement transactional block for safe SCD Type 2 execution
  BEGIN TRANSACTION;

  -- STEP 1: Expire old active customer records where values have changed, 
  -- and insert brand-new customers who do not yet exist.
  MERGE INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT AS tgt
  USING (
      SELECT CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE
      FROM   ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT
      WHERE  RUN_DATE = v_run_date
  ) AS src
  ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 1)
  
  WHEN MATCHED AND (
         tgt.SEGMENT_CODE <> src.SEGMENT_CODE
      OR tgt.SCORE_BAND   <> src.SCORE_BAND
     ) THEN
    UPDATE SET 
        tgt.IS_CURRENT = 0,
        tgt.VALID_TO   = v_current_timestamp -- converted from SYSDATE to script execution timestamp
        
  WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
    VALUES (src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, src.SCORE_VALUE, 1, v_current_timestamp); -- converted from SYSDATE

  -- STEP 2: Insert new active versions of records for customers whose prior active row 
  -- was just expired in the MERGE step. Joined based on matching timestamp from Step 1.
  INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
      (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
  SELECT 
      src.CUSTOMER_ID, 
      src.SEGMENT_CODE, 
      src.SCORE_BAND, 
      src.SCORE_VALUE, 
      1, 
      v_current_timestamp -- converted from SYSDATE to preserve timestamp equality
  FROM   ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT AS src
  INNER JOIN ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT AS tgt -- converted from implicit comma-join
    ON   tgt.CUSTOMER_ID = src.CUSTOMER_ID 
    AND  tgt.IS_CURRENT = 0 
    AND  tgt.VALID_TO = v_current_timestamp -- Joins only matching records expired in preceding MERGE
  WHERE  src.RUN_DATE = v_run_date;

  -- Commit transaction to solidify all historical versions atomically
  COMMIT TRANSACTION; -- converted from COMMIT;

END;
```
 
═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **SQL*Plus Parameterization (`&1`)**:
   - The variable replacement mapping relies on `@run_date_param` being supplied as a standard query parameter string (e.g., `'2023-10-31'`) during client execution. If target orchestrators cannot pass parameters natively, a script wrapper or environment-level substitution step must be implemented before submission.
2. **Schema Type Matching**:
   - Ensure target target table `DIM_CUSTOMER_SEGMENT` columns `VALID_FROM` and `VALID_TO` are mapped to standard BigQuery `TIMESTAMP` or `DATETIME` types. If they are mapped to `DATE`, the assignment of `v_current_timestamp` must be explicitly wrapped in `EXTRACT(DATE FROM v_current_timestamp)` or `CURRENT_DATE()` to avoid runtime type assignment errors. This pseudocode conservatively treats them as `TIMESTAMP` types based on Oracle `DATE` default precision containing hours/minutes/seconds.

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/d_historization_load.sql` | `customer/d_historization_load.sql` | Convert Oracle SQL*Plus script to a native BigQuery Standard SQL scripting block containing sequential SCD Type 2 MERGE and INSERT DMLs wrapped in a transaction block. |

# Add Context the MCP Could Not See

### Job Dependencies
* **Downstream**: `CUSTOMER.WEEKLY_SCHEDULE` — This downstream job is marked **not yet migrated**. Since it is not yet migrated, the final target wiring (such as an Airflow sensor or direct DAG trigger) cannot be fully finalized until that job is migrated.

### Execution Order
The target orchestration (managed via Cloud Composer / Airflow DAG) must preserve the original 5-step sequence. Although only step 4 is in-scope for this design pass, the overall ordering is:
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 orchestration, converted to Airflow DAG structure)
2. `customer/r_historization_load.ksh` (Wrapper script, converted to Python/Airflow Task)
3. `customer/k_historization_load.ksh` (KSH execution/log wrapper, converted to Python/Airflow Task)
4. `customer/d_historization_load.sql` (Historization SQL script, executed via BigQueryInsertJobOperator or native Dataform task)
5. `customer/d_segment_quality_check.sql` (Post-load segment quality verification, executed via BigQuery task)

### Scheduling
* This job is not directly triggered by any of the environment's standalone schedulers. It is designed to run inside parent scheduled jobs as an include/shared module. 
* Do not configure a standalone cron schedule for the migrated artifact; it must remain a callable/importable unit within the master orchestration workflow.

### Schedule & Variables
* **Scheduler-Set Variable**: `RUN_DATE` (dynamically set to `'&$TODAY'` in the source environment).
* **Target Delivery Mechanism**: This value must reach the BigQuery script as a dynamic query parameter (`@run_date_param`). In Airflow, this should be mapped to the templated execution date (e.g., `{{ ds }}`) and injected into the task parameters of the BigQuery operator.

### Lineage
* **Upstream Table Producer (Reads)**: `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`
* **Downstream Table Consumer (Writes)**: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
* **Schema/Package Used**: `ANALYTICS_SCHEMA`

### External System Replacements
* **Database Platform**: Oracle SQL*Plus execution is replaced by Google Cloud BigQuery.
* **Transaction Control**: Oracle native SQL*Plus session commits (`COMMIT; EXIT;`) are replaced by standard BigQuery Scripting Transactions (`BEGIN TRANSACTION` and `COMMIT TRANSACTION`).

### Cross-File Dependencies
* This SQL script operates as the primary DML task of the `CUSTOMER.HISTORIZATION_LOAD` job. It depends on staging data loaded by the preceding KSH wrappers (`r_historization_load.ksh` / `k_historization_load.ksh`) and supplies populated dimensional data required by the downstream quality check task (`d_segment_quality_check.sql`).

### Target File Plan
* **Target Path**: `customer/d_historization_load.sql`
* **Language**: BigQuery Standard SQL (Procedural Scripting)
* **Source File**: `customer/d_historization_load.sql`

### Environment-Specific Values
1. **GLOBAL (Environment-Wide)**
   * **`ANALYTICS_SCHEMA`**: Identifies the dataset namespace. In BigQuery, this must be mapped to a canonical environment variable `BQ_DATASET`.
     * *Retrieval*: Airflow DAGs should fetch this using `Variable.get("BQ_DATASET")` or refer to the environment-configured project/dataset.
2. **JOB-SPECIFIC**
   * **`RUN_DATE`**: Represents the specific business run partition date.
     * *Retrieval*: Handled as a query parameter (`@run_date_param`) substituted at execution time using DAG params (e.g. `{{ ds }}`).

### Risks & Manual Steps
* **Downstream Orchestration Gap**: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated. The cross-job dependency trigger or sensor can only be stubbed out and must be verified once `CUSTOMER.WEEKLY_SCHEDULE` is moved to BigQuery.
* **Date / Timestamp Mapping**: Oracle `DATE` columns `VALID_FROM` and `VALID_TO` in `DIM_CUSTOMER_SEGMENT` store granular time. They must be mapped as BigQuery `TIMESTAMP` types. If they have been mapped as `DATE` in the target schema, the script's `v_current_timestamp` local variable must be cast to `DATE` using `EXTRACT(DATE FROM v_current_timestamp)` or `CURRENT_DATE()` to avoid a type mismatch during script execution.
* **Variable Binding Verification**: Ensure the Airflow operator or orchestration wrapper is correctly configured to pass `@run_date_param` as a parameter to the BigQuery API call, replacing the legacy `&1` SQL*Plus positional parameter.

---

=== FILE: customer/d_segment_quality_check.sql ===
-- d_segment_quality_check.sql
-- Computes the percentage of customers whose current segment version was
-- freshly created (i.e. re-versioned) on the given run date, so callers can
-- flag an implausibly large weekly shift.
-- Schema: ANALYTICS_SCHEMA

SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF

SELECT ROUND(
           (SELECT COUNT(*) FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
            WHERE IS_CURRENT = 1 AND VALID_FROM = TRUNC(TO_DATE('&1', 'YYYY-MM-DD')))
           /
           NULLIF((SELECT COUNT(*) FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
                    WHERE IS_CURRENT = 1), 0)
           * 100
       ) AS CHANGED_PCT
FROM DUAL;

EXIT;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - Multi-statement script containing SQL*Plus formatting commands (`SET`), a standalone parameterized `SELECT` query referencing the dummy table `DUAL`, and an `EXIT` instruction.

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script calculates the percentage of currently active customer segments (`IS_CURRENT = 1`) whose record was started (`VALID_FROM`) on a specific input run-date. This metric allows downstream systems or orchestrators to perform quality checks and detect anomalies, such as an unexpectedly large block of customers shifting segments in a single run.

1.3 List all entities referenced:
    - Table: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
        - `IS_CURRENT`: Numeric flag/indicator.
        - `VALID_FROM`: Oracle `DATE` (contains date/time).
    - Pseudotable: `DUAL`
    - Parameter: `&1` (SQL*Plus positional substitution variable representing the target run-date).

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (for `VALID_FROM`) → Maps to BigQuery `DATE` as it is compared with a calendar date string without time components.
    - Parameter `&1` → Represented as a BigQuery query parameter `@run_date` of type `STRING`.

2.2 Implicit and Explicit Type Casting:
    - Oracle `TO_DATE('&1', 'YYYY-MM-DD')` is used to explicitly cast the string parameter. This will be mapped to `PARSE_DATE('%Y-%m-%d', @run_date)` in BigQuery to ensure strong explicit typing.

2.3 NULL Handling and Conditional Functions:
    - `NULLIF(..., 0)` → BigQuery natively supports `NULLIF()`. Compatible as-is.

2.4 String Functions:
    - None used.

2.5 Date and Timestamp Functions:
    - `TRUNC(TO_DATE('&1', 'YYYY-MM-DD'))`:
        - `TO_DATE` converted to `PARSE_DATE('%Y-%m-%d', @run_date)`.
        - `TRUNC(<date>)` without a format mask defaults to Day granularity. This is converted to `DATE_TRUNC(<date>, DAY)`. BigQuery requires the second argument explicitly.

2.6 Numeric and Aggregate Functions:
    - `ROUND(val)` → Fully compatible with BigQuery `ROUND()`.

2.7 Analytical and Window Functions:
    - None used.

2.8 Set and Join Operations:
    - None used.

2.9 Row Limiting and Sampling:
    - None used.

2.10 Sequences:
    - None used.

2.11 MERGE Statements:
    - None used.

2.12 INSERT / UPDATE / DELETE:
    - None used.

2.13 DDL Constructs:
    - None used.

2.14 PL/SQL:
    - None used.

2.15 Unresolvable or Advisory Items:
    - SQL*Plus directives (`SET HEADING OFF...`, `EXIT`) cannot be executed in BigQuery SQL engines. These are stripped and noted as tasks for the pipeline orchestrator (e.g., Airflow or a shell wrapper).
    - `FROM DUAL` is obsolete in BigQuery; expressions can be queried directly via a standalone `SELECT` without a `FROM` clause.

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - Convert the SQL*Plus query into a clean, parameterized BigQuery Standard SQL query.
    - Remove the SQL*Plus header configurations and the `FROM DUAL` expression.
    - Replace the substitution variable `&1` with a query parameter `@run_date`.

3.2 List any assumptions made during conversion:
    - It is assumed that the pipeline orchestrator passing the argument `@run_date` ensures it is in `'YYYY-MM-DD'` format.
    - It is assumed that `VALID_FROM` is stored with date resolution or can be safely compared to a truncated date.

3.3 List any items flagged for human review before the build stage proceeds:
    - Ensure downstream consumers of this script capture the scalar output of the BigQuery job correctly since SQL*Plus output formatting is no longer handling stdout.

2.16 MIGRATION DECISION MATRIX

| Oracle Construct | Target Option | Rejected Alternatives | Reason for Selection |
| :--- | :--- | :--- | :--- |
| `SET ...` / `EXIT;` | Orchestration Wrapper | SQL Scripting | SQL*Plus instructions are client-side commands; BigQuery engines do not execute them. |
| `&1` | BigQuery Query Parameter (`@run_date`) | Hardcoded literal | Dynamic injection via standard parameterization is the most secure and clean pattern in BigQuery. |
| `FROM DUAL` | Direct SELECT | `SELECT ... FROM (SELECT 1)` | BigQuery allows execution of scalar SELECT statements without any `FROM` clause. |
| `TRUNC(TO_DATE(...))` | `DATE_TRUNC(PARSE_DATE(...), DAY)` | UDF | BigQuery has direct built-in functions for explicit date parsing and truncation. |

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: `d_segment_quality_check.sql` containing a clean, parameterized standard query. No procedural blocks or custom UDFs are required.

2.18 DATA TYPE COMPATIBILITY TABLE

| Source Table/Column | Oracle Type | BigQuery Type | Conversion Rule / Warning |
| :--- | :--- | :--- | :--- |
| `DIM_CUSTOMER_SEGMENT.IS_CURRENT` | `NUMBER` | `INT64` | Native numeric mapping. |
| `DIM_CUSTOMER_SEGMENT.VALID_FROM` | `DATE` | `DATE` | Mapped to `DATE` since time values are truncated to calendar days. |
| `&1` | String Substitution | `STRING` | Passed as parameter `@run_date` and explicitly parsed to `DATE`. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: SQL*Plus directives, DUAL pseudo-table, scalar subqueries.
- **Unsupported Functions**: None (all native alternatives exist).
- **UDF Required**: No.
- **Python Required**: No (except for scheduling/orchestration wrapping).
- **Direct Dependencies**: Table `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`.
- **Status**: Ready for Human Approval.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `SET ...` | Unsupported | None (strip; handle in orchestration) |
| `ROUND` | Direct | `ROUND` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', ...)` |
| `TRUNC` | Direct-with-rewrite | `DATE_TRUNC(..., DAY)` |
| `NULLIF` | Direct | `NULLIF` |
| `DUAL` | Direct-with-rewrite | Remove `FROM DUAL` entirely |
| `EXIT` | Unsupported | None (strip) |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- BigQuery SQL equivalent for customer/d_segment_quality_check.sql
-- Parameter declaration placeholder (handled by BigQuery Client / API):
-- DECLARE run_date STRING; 

SELECT 
  ROUND(
    (
      SELECT COUNT(1) 
      FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
      WHERE IS_CURRENT = 1 
        -- CONVERTED: TO_DATE and TRUNC replaced with PARSE_DATE and explicit DATE_TRUNC
        AND VALID_FROM = DATE_TRUNC(PARSE_DATE('%Y-%m-%d', @run_date), DAY)
    )
    /
    -- CONVERTED: Native NULLIF retained for safety against zero division
    NULLIF(
      (
        SELECT COUNT(1) 
        FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
        WHERE IS_CURRENT = 1
      ), 
      0
    )
    * 100
  ) AS CHANGED_PCT
-- CONVERTED: Oracle FROM DUAL removed; BigQuery supports standalone SELECT
;
```

FLAGGED ITEMS FOR HUMAN REVIEW:
- **Orchestration Integration**: SQL*Plus-specific settings (`SET HEADING OFF`, `EXIT`) have been removed. The orchestrator invoking this script must execute it as a parameterized query passing `@run_date` and capturing the scalar `CHANGED_PCT` output from the BigQuery query job results.

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/d_segment_quality_check.sql` | `customer/d_segment_quality_check.sql` | Oracle SQL*Plus script converted to standard BigQuery parameterized SQL. SQL*Plus-specific options (`SET`, `EXIT`) and the obsolete `FROM DUAL` are retired. |

---

### 1. Job Dependencies
- **Downstream Consumer**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated).
  - *Wiring on BigQuery/Cloud Composer*: Because `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated, the final hand-off cannot be completed in this pass. Once migrated, a cross-DAG trigger or an Airflow sensor (e.g., `ExternalTaskSensor`) should be implemented in the target DAG to notify or trigger `CUSTOMER.WEEKLY_SCHEDULE`.

---

### 2. Execution Order
The target Cloud Composer orchestration must preserve the sequential execution order established by the legacy dependency graph. Note that files 1 through 4 are handled by independent design passes; this design pass is responsible only for step 5.
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 Job Definition)
2. `customer/r_historization_load.ksh` (KSH Wrapper Script)
3. `customer/k_historization_load.ksh` (Historization Core Script)
4. `customer/d_historization_load.sql` (Historization Merge Script)
5. `customer/d_segment_quality_check.sql` (**This File** — Executes as the final post-load quality gate)

---

### 3. Scheduling
- **Triggering Pattern**: This job is not directly triggered by a standalone cron schedule. It executes as an included, shared module inside the parent job `CUSTOMER.HISTORIZATION_LOAD`.
- **Target Platform Solution**: The converted BigQuery script must remain an importable, callable SQL task within the parent Cloud Composer DAG, scheduled to run immediately after the historization load task finishes. It should not be given an independent schedule.

---

### 4. Schedule & Variables
- **Scheduler-Set Variables**:
  - `RUN_DATE` (historically populated by UC4's `&$TODAY` in the parent job `CUSTOMER.HISTORIZATION_LOAD`).
- **Target Platform Resolution**: In Cloud Composer, the `RUN_DATE` must be dynamically resolved using Airflow execution date templates (e.g., `{{ ds }}` in `YYYY-MM-DD` format) and passed directly as the query parameter `@run_date` to the BigQuery operator.

---

### 5. Lineage
- **Upstream Producer**: None directly identified in metadata, but table `DIM_CUSTOMER_SEGMENT` is loaded during step 4 (`d_historization_load.sql`).
- **Read Table**: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
- **Downstream Consumers**: The output metric (`CHANGED_PCT`) is evaluated by the parent orchestrator script to trigger threshold alerts or stop execution in case of anomalous shifts.

---

### 6. External System Replacements
- **Database Engine**: Oracle Database replaced by Google Cloud BigQuery.
- **Client Utility**: Oracle SQL*Plus client utility replaced by the BigQuery execution engine in Cloud Composer (e.g., via `BigQueryExecuteQueryOperator`).

---

### 7. Cross-File Dependencies
- **Shared Table**: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` is shared. It is updated/historized in Step 4 (`customer/d_historization_load.sql`) and read by this script in Step 5 (`customer/d_segment_quality_check.sql`) to verify the load quality.

---

### 8. Target File Plan
- **Path**: `customer/d_segment_quality_check.sql`
- **Language**: SQL (BigQuery Standard SQL dialect)
- **Source File**: `customer/d_segment_quality_check.sql`
- **Purpose**: Verifies that the percentage of freshly re-versioned active customers on the specified run date remains within an acceptable threshold.

---

### 9. Environment-Specific Values
- **`ANALYTICS_SCHEMA`**: Classified as **GLOBAL** (environment-wide).
  - *GCP Target Concept*: `BQ_DATASET`
  - *Resolution Mechanism*: In the BigQuery execution operator, the schema prefix must be parameterized dynamically using the environment configuration or Airflow variables (e.g., `{{ var.value.BQ_DATASET_ANALYTICS }}.DIM_CUSTOMER_SEGMENT`) rather than hardcoding.

---

### 10. Risks and Manual Steps
- **Orchestration Result Capture (Critical)**: In the legacy environment, the SQL*Plus script relied on stdout redirection to return `CHANGED_PCT` to the calling KSH script. In BigQuery, the Cloud Composer DAG must be configured to explicitly query and capture the scalar value of the execution result (e.g., by using BigQuery's XCom push features or a lightweight Python operator reading from the query execution job) to perform threshold comparisons.
- **Unmigrated Dependencies**: The downstream `CUSTOMER.WEEKLY_SCHEDULE` job has not yet been migrated. End-to-end integration testing of the historization pipeline remains blocked until the downstream consumer is available.
- **Upstream Sequence Gaps**: Steps 1 through 4 are outside the scope of this file's design pass. Integration testing of this quality check relies on the presence of the freshly loaded target tables produced by step 4. Ensure dummy test data is populated in `DIM_CUSTOMER_SEGMENT` with matching `VALID_FROM` values to simulate the verification gate in isolation.

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
REASON: The script performs database orchestration by executing multiple external Oracle SQL scripts, capturing stdout to extract a numeric threshold value, and performing conditional threshold validation logic.

EVIDENCE
- Business logic found: KSH custom logic coordinates the SCD Type 2 merge load, executes a quality check SQL statement, captures stdout, cleans whitespace, and evaluates the change percentage against a threshold.
- AWK: none
- SQL-expressible: partly, the data transformations are SQL, but the orchestration and control flow (error handling, command substitution, and string cleaning) require a host programming language.
- Non-SQL side effects: execution of external database client (sqlplus) and console logging.
- Against this verdict: none

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `k_historization_load.ksh` script manages the weekly Slowly Changing Dimension (SCD) Type 2 merge process for the customer segment dimension. It reads current customer score and segment data, merges it into the historical dimension table, and then executes a data quality sanity check. This sanity check computes the percentage of customers whose segments changed this week and issues a warning if the percentage exceeds a pre-defined threshold, helping catch potential bad join-key issues before they silently corrupt downstream reporting.

2. INVOCATION CONTEXT
   - Invoked by: Unknown (no UC4/Automic extraction metadata provided).
   - Expected command line execution: `k_historization_load.ksh` (requires the environment variable `RUN_DATE` to be set).
   - UC4 Native Includes: None.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   - `RUN_DATE` (Environment variable): Used as a positional argument passed to both `d_historization_load.sql` and `d_segment_quality_check.sql`. Surface in Python via `os.environ.get("RUN_DATE")` or `argparse`.
   - `CRM_HOME` (Environment variable): Base path of the ETL code. Defaults to `/opt/etl/customer`. Surface in Python via `os.environ.get("CRM_HOME", "/opt/etl/customer")`.
   - `CRM_ORA_USER` (Environment variable): Database user. Defaults to `crm_etl`. Usable for Python DB-client initialization.
   - `CRM_ORA_PASS` (Environment variable): Database password. Defaults to `changeit`. Usable for Python DB-client initialization.
   - `CRM_ORA_SID` (Environment variable): Database Oracle SID. Defaults to `CRMPRD`. Usable for connection string initialization.
   - `MAX_EXPECTED_CHANGE_PCT` (Environment variable / local variable): Threshold value for sanity checking. Defaults to `25`.
   - KSH DECLARED ENVIRONMENT PARAMETERS:
     - `CRM_HOME` (Informational/ETL home path)
     - `CRM_ORA_USER` (DB connection parameter)
     - `CRM_ORA_PASS` (DB connection parameter)
     - `CRM_ORA_SID` (DB connection parameter)

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - First `sqlplus` execution:
     - Command: `sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_historization_load.sql "${RUN_DATE}"`
     - Purpose: Run the main SCD2 merge SQL script against the Oracle Database.
     - Recommendation: Convert to a native Python database client execution using `oracledb` (or `google-cloud-bigquery` if migrating to GCP).
   - Second `sqlplus` execution (with `tr` cleanup):
     - Command: `sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_segment_quality_check.sql "${RUN_DATE}" | tr -d '[:space:]'`
     - Purpose: Calculate the percentage of changed records and capture it as a clean numeric string by stripping whitespaces.
     - Recommendation: Convert to native database client querying, reading a scalar integer value from a cursor.

5. EMBEDDED SQL
   - No SQL is inline. The SQL commands reside in external scripts:
     - `d_historization_load.sql`:
       - # REVIEW-STRUCT: SQL file [d_historization_load.sql] body not supplied — contents are external to this script.
       - Statement Type: Assumed to be MERGE / INSERT / UPDATE (SCD Type 2 logic).
       - Tables Touched: Segment dimension table.
       - Dialect: Oracle SQL*Plus.
     - `d_segment_quality_check.sql`:
       - # REVIEW-STRUCT: SQL file [d_segment_quality_check.sql] body not supplied — contents are external to this script.
       - Statement Type: SELECT (returning a scalar percentage value).
       - Tables Touched: Segment dimension table.
       - Dialect: Oracle SQL*Plus.
   - # REVIEW: target database platform not specified; DB-client library choice below is provisional. Standard Oracle `oracledb` library is assumed based on the legacy SQL*Plus commands.

6. CONTROL FLOW
   1. Initialize configuration parameters with defaults (`CRM_HOME`, `CRM_ORA_USER`, `CRM_ORA_PASS`, `CRM_ORA_SID`, `MAX_EXPECTED_CHANGE_PCT`).
   2. Fetch `RUN_DATE` from the environment. Check if it exists; log warning or fail if missing (depending on business rules).
   3. Log step start: "Running SCD2 merge for customer segment dimension".
   4. Execute `d_historization_load.sql` with connection string and `RUN_DATE` parameter.
   5. Capture execution return code. If non-zero, log error "ERROR: d_historization_load.sql failed with rc=..." and exit 1.
   6. Execute `d_segment_quality_check.sql` using same parameters.
   7. Strip whitespaces from the database output to parse the `changed_pct` value.
   8. If `changed_pct` is empty/null, log "WARN: could not compute changed-row percentage - skipping sanity check" and exit 0.
   9. Convert `changed_pct` to an integer. If conversion fails or if `changed_pct` is greater than `MAX_EXPECTED_CHANGE_PCT`, log warning "WARN: {changed_pct}% of customers changed segment this week (expected <= {MAX_EXPECTED_CHANGE_PCT}%) - flagging for review, not failing the job".
   10. Log completion message: "Historization merge complete, {changed_pct}% of customers re-versioned" and exit 0.

7. ERROR HANDLING & EXIT CODES
   - KornShell captures status using `merge_rc=$?` and validates with `if [ ${merge_rc} -ne 0 ]; then exit 1; fi`.
   - Standard shell redirection and parsing failures on `changed_pct` are suppressed or redirected using `2>/dev/null` in `[ "${changed_pct}" -gt "${MAX_EXPECTED_CHANGE_PCT}" ]`.
   - In Python, wrap DB-client executions in `try...except` blocks using driver-specific exceptions (e.g., `oracledb.DatabaseError` or `google.cloud.exceptions.GoogleCloudError`). Explicitly raise `RuntimeError` on load failures and gracefully handle parsing issues with standard `try...except ValueError`.

8. OUTPUTS / SIDE EFFECTS
   - Mutated database records (inserts/updates on customer segment dimension table).
   - Console logs (stdout) detailing execution status and warning messages.

9. BUSINESS SUMMARY
   - Executes the weekly Slowly Changing Dimension Type 2 (SCD2) history loading of customer segments and scores.
   - Protects downstream analytics from massive data corruption (e.g., cartesian join errors) by verifying that the weekly records change threshold does not exceed 25%.
   - Alerts operational teams if record updates exceed normal limits, while allowing the pipeline to complete successfully without a hard crash.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import sys
import os
import datetime
import subprocess

# REVIEW: target database platform not specified; DB-client library choice below is provisional
# (Oracle 'oracledb' is shown based on sqlplus usage)
try:
    import oracledb
except ImportError:
    # Fallback to subprocess if driver is unavailable or if subprocess migration is preferred
    oracledb = None


def log(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def main():
    # Step 1: Initialize environment and default parameters
    crm_home = os.environ.get("CRM_HOME", "/opt/etl/customer")
    crm_ora_user = os.environ.get("CRM_ORA_USER", "crm_etl")
    crm_ora_pass = os.environ.get("CRM_ORA_PASS", "changeit")
    crm_ora_sid = os.environ.get("CRM_ORA_SID", "CRMPRD")
    
    try:
        max_expected_change_pct = int(os.environ.get("MAX_EXPECTED_CHANGE_PCT", 25))
    except ValueError:
        max_expected_change_pct = 25

    run_date = os.environ.get("RUN_DATE")
    if not run_date:
        log("ERROR: RUN_DATE environment variable is not set.")
        sys.exit(1)

    log("Running SCD2 merge for customer segment dimension")

    # Step 2: Execute SCD2 merge using d_historization_load.sql
    # # REVIEW-STRUCT: SQL file [d_historization_load.sql] body not supplied — content is external
    sql_load_path = os.path.join(crm_home, "customer", "d_historization_load.sql")

    if oracledb:
        try:
            # Native Python DB client implementation
            dsn = oracledb.makemdsn(crm_ora_sid, 1521, service_name=crm_ora_sid)
            connection = oracledb.connect(user=crm_ora_user, password=crm_ora_pass, dsn=dsn)
            with connection.cursor() as cursor:
                # Assuming the external .sql file's logic is loaded or run via dynamic PL/SQL
                # For equivalent emulation of SQL*Plus file execution:
                with open(sql_load_path, 'r') as sql_file:
                    sql_content = sql_file.read()
                cursor.execute(sql_content, [run_date])
            connection.commit()
            connection.close()
        except Exception as e:
            log(f"ERROR: d_historization_load.sql failed: {str(e)}")
            sys.exit(1)
    else:
        # Fallback to subprocess preservation if native DB-driver is not used
        cmd_load = [
            "sqlplus", "-s", 
            f"{crm_ora_user}/{crm_ora_pass}@{crm_ora_sid}", 
            f"@{sql_load_path}", 
            run_date
        ]
        result = subprocess.run(cmd_load, capture_output=True, text=True)
        if result.returncode != 0:
            log(f"ERROR: d_historization_load.sql failed with rc={result.returncode}")
            log(result.stderr)
            sys.exit(1)

    # Step 3: Run segment quality check query
    # # REVIEW-STRUCT: SQL file [d_segment_quality_check.sql] body not supplied — content is external
    sql_check_path = os.path.join(crm_home, "customer", "d_segment_quality_check.sql")
    changed_pct_raw = ""

    if oracledb:
        try:
            dsn = oracledb.makemdsn(crm_ora_sid, 1521, service_name=crm_ora_sid)
            connection = oracledb.connect(user=crm_ora_user, password=crm_ora_pass, dsn=dsn)
            with connection.cursor() as cursor:
                with open(sql_check_path, 'r') as sql_file:
                    sql_content = sql_file.read()
                cursor.execute(sql_content, [run_date])
                row = cursor.fetchone()
                if row:
                    changed_pct_raw = str(row[0])
            connection.close()
        except Exception as e:
            log(f"WARN: could not compute changed-row percentage - skipping sanity check. Error: {str(e)}")
            sys.exit(0)
    else:
        cmd_check = [
            "sqlplus", "-s", 
            f"{crm_ora_user}/{crm_ora_pass}@{crm_ora_sid}", 
            f"@{sql_check_path}", 
            run_date
        ]
        result_check = subprocess.run(cmd_check, capture_output=True, text=True)
        changed_pct_raw = result_check.stdout

    # Step 4: String cleanup and parsing (reproducing tr -d '[:space:]')
    changed_pct_clean = "".join(changed_pct_raw.split())

    # Step 5: Validate quality metric
    if not changed_pct_clean:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        sys.exit(0)

    try:
        changed_pct = int(changed_pct_clean)
        if changed_pct > max_expected_change_pct:
            log(f"WARN: {changed_pct}% of customers changed segment this week "
                f"(expected <= {max_expected_change_pct}%) - flagging for review, not failing the job")
        
        log(f"Historization merge complete, {changed_pct}% of customers re-versioned")
    except ValueError:
        log(f"WARN: could not compute changed-row percentage (invalid int format: '{changed_pct_clean}') - skipping sanity check")

    sys.exit(0)


if __name__ == "__main__":
    main()
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/k_historization_load.ksh` | `customer/k_historization_load.py` | Converts KornShell orchestrator into a Python script using the BigQuery Python Client to execute the SCD2 load and perform the quality check. |

---

# Additional Context

### Job dependencies
* **Downstream Job**: `CUSTOMER.WEEKLY_SCHEDULE` — Not yet migrated. Once migrated, this downstream job should be triggered from the main DAG using an Airflow sensor (such as `ExternalTaskSensor`) or via Pub/Sub message.
* **Risks & Manual Actions**: The downstream wiring cannot be finalized until `CUSTOMER.WEEKLY_SCHEDULE` is migrated to the target platform.

### Execution order
The target orchestration sequence must preserve the legacy order:
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 Orchestration job, handled in its own design pass).
2. `customer/r_historization_load.ksh` (Wrapper script, handled in its own design pass).
3. `customer/k_historization_load.ksh` (This step, mapped to `customer/k_historization_load.py`).
4. `customer/d_historization_load.sql` (Main SCD2 SQL load, handled in its own design pass).
5. `customer/d_segment_quality_check.sql` (Quality check query, handled in its own design pass).

Our target file `customer/k_historization_load.py` will handle the runtime step of executing the translated equivalents of `d_historization_load.sql` and `d_segment_quality_check.sql` sequentially and parsing the resulting output.

### Scheduling
* This job is not directly triggered by any standalone scheduler; it acts as an include/shared module within scheduled workflows. The migrated Python artifact must remain a callable/importable unit (e.g., as a Python task or a reusable Airflow operator) and should not be given an independent standalone schedule.

### Schedule & variables
* **RUN_DATE**: Inherited from the scheduler variable `&$TODAY`. In the migrated environment, this variable must be passed at runtime using Cloud Composer's context parameters (e.g., `{{ ds }}` or `{{ dag_run.conf['RUN_DATE'] }}`) and mapped to an environment variable `RUN_DATE` for the Python script.

### Lineage
* **SQL Executions**:
  * Upstream Execution: `customer/d_historization_load.sql`
  * Upstream Execution: `customer/d_segment_quality_check.sql`
  Both SQL components are referenced inside `k_historization_load.ksh` and are executed against the database. Note that these belong to different file groups and will be migrated separately to BigQuery standard SQL or Dataform models.

### External system replacements
* **Oracle Database** (`CRM_ORA_USER`, `CRM_ORA_PASS`, `CRM_ORA_SID`) $\rightarrow$ Replaced by Google Cloud BigQuery.
* **Oracle SQL*Plus Client** $\rightarrow$ Replaced by Google Cloud BigQuery Python client library (`google-cloud-bigquery`) or Dataform API client calls within Google Cloud Composer.

### Cross-file dependencies
* `customer/k_historization_load.py` depends directly on:
  * The migrated BigQuery/Dataform SQL equivalent of `customer/d_historization_load.sql` (for SCD2 loading).
  * The migrated BigQuery/Dataform SQL equivalent of `customer/d_segment_quality_check.sql` (for retrieving the change percentage).

### Target file plan
* **Target File**: `customer/k_historization_load.py`
  * **Language**: Python (3.x)
  * **Source File**: `customer/k_historization_load.ksh`
  * **Purpose**: Orchestrates the run. It uses the `google-cloud-bigquery` library to execute the merged historization query, runs the segment quality check query, captures the scalar `changed_pct` output, and performs threshold-based logic.

### Environment-specific values
* **GCP_PROJECT** (GLOBAL): The target GCP Project ID. Sourced at runtime via `os.environ.get("GCP_PROJECT")`.
* **BQ_DATASET** (GLOBAL): The target BigQuery dataset containing the segment dimension. Sourced at runtime via `os.environ.get("BQ_DATASET")`.
* **RUN_DATE** (JOB-SPECIFIC): The execution date. Sourced at runtime via the environment variable `RUN_DATE`.
* **MAX_EXPECTED_CHANGE_PCT** (JOB-SPECIFIC): Threshold value for quality checks. Sourced at runtime via `os.environ.get("MAX_EXPECTED_CHANGE_PCT", "25")`.
* **RETIRED**: Oracle connection variables (`CRM_HOME`, `CRM_ORA_USER`, `CRM_ORA_PASS`, `CRM_ORA_SID`) are retired and replaced by IAM-based authentication inside BigQuery.

### Risks and manual steps
* **Separately Migrated SQL Dependencies**: The SQL logic in `d_historization_load.sql` and `d_segment_quality_check.sql` must be migrated to BigQuery syntax (or Dataform models) in their own respective design passes. The Python script cannot successfully execute without these translated queries.
* **Output Literal Consistency**: The literal log and print messages from the source shell script must be kept identical in the python translation.
  * `"Running SCD2 merge for customer segment dimension"`
  * `"ERROR: d_historization_load.sql failed with rc=..."`
  * `"WARN: could not compute changed-row percentage - skipping sanity check"`
  * `"WARN: {changed_pct}% of customers changed segment this week (expected <= {max_expected_change_pct}%) - flagging for review, not failing the job"`
  * `"Historization merge complete, {changed_pct}% of customers re-versioned"`
* **Query Parameterization**: The migrated BigQuery queries must support parameter binding for `RUN_DATE` natively to avoid SQL injection risks and ensure schema compatibility.

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
REASON: Sourcing an unsupplied script (k_historization_load.ksh) and utilizing custom logging functions requires a Python-based orchestration to preserve execution context.

EVIDENCE
- Business logic found: Invokes `k_historization_load.ksh` which is described as wrapping an SCD2 historization merge, but the actual logic of that script is not supplied in this extraction.
- AWK: none
- SQL-expressible: no (the body of the sourced script `k_historization_load.ksh` is unknown and cannot be mapped to SQL)
- Non-SQL side effects: none observed in the wrapper itself, but the wrapped script performs database and shell operations
- Against this verdict: none (it is impossible to convert to BQSQL without the sourced script's contents, and it is not a pure wrapper because it defines a custom `log()` function)

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script acts as an orchestration wrapper for the SCD2 (Slowly Changing Dimension Type 2) historization merge process. It is triggered during the customer historization pipeline to execute `k_historization_load.ksh` while capturing, logging, and safely propagating its exit codes. It reads the `RUN_DATE` from the environment and ensures any failures are explicitly captured and logged before terminating.

2. INVOCATION CONTEXT
   - Invoking Job: UC4 job `CUSTOMER.HISTORIZATION_LOAD` (implied by the header comments)
   - Command Line: `r_historization_load.ksh`
   - UC4 includes: none referenced in the extraction
   - Environment files sourced: Sourced inline via `. ${CRM_HOME}/customer/k_historization_load.ksh`
     # REVIEW-STRUCT: environment file k_historization_load.ksh not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `CRM_HOME` (Environment Variable)
     - Source: Environment
     - Used: Yes, to locate `k_historization_load.ksh`
     - Python representation: `os.environ.get("CRM_HOME", "/opt/etl/customer")`
   - `RUN_DATE` (Environment Variable)
     - Source: Environment
     - Used: Yes, in log messages
     - Python representation: `os.environ.get("RUN_DATE")`

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `. ${CRM_HOME}/customer/k_historization_load.ksh`
     - Exact command: `. ${CRM_HOME}/customer/k_historization_load.ksh`
     - Purpose: Executes the core historization logic
     - Python implementation: Must remain an external process invocation via `subprocess`
     - Resolvable: No
     - # REVIEW-STRUCT: launcher [k_historization_load.ksh] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - None (no inline SQL present in this wrapper script).

6. CONTROL FLOW
   1. Environment setup: Set shell option `set -e` (Step 1).
   2. Parameter assignment: Initialize `CRM_HOME` default if not set (Step 2).
   3. Function definition: Define custom timestamped logging function `log()` (Step 3).
   4. Execution start: Log starting SCD2 historization with `${RUN_DATE}` (Step 4).
   5. Core execution: Source and execute `k_historization_load.ksh` (Step 5).
   6. Error capture: Capture the exit status code (`rc`) of the sourced script (Step 6).
   7. Conditional branching: If `rc` is non-zero, log the failure and exit with `rc` (Step 7).
   8. Execution success: Log completion and exit with `0` (Step 8).

7. ERROR HANDLING & EXIT CODES
   - Detection: Captures return status via `rc=$?` immediately after execution.
   - Action: If failure is detected, logs `ERROR: k_historization_load.ksh failed with rc={rc}` and exits with the same status code.
   - Success exit code: `0`.
   - Python Mapping: Map to `subprocess.run()` checking for `subprocess.CalledProcessError` and propagating the exception's return code.

8. OUTPUTS / SIDE EFFECTS
   - Writes log messages containing execution status and timestamps to standard output.
   - Potential database updates executed by the unsupplied `k_historization_load.ksh`.

9. BUSINESS SUMMARY
   - Orchestrates the SCD2 Customer Historization load process.
   - Standardizes execution logging by outputting timestamped statements at the start and completion of the job.
   - Ensures that underlying script failures are explicitly trapped, logged with error context, and returned with correct exit codes to the UC4 orchestrator to prevent silent failures.

=== PSEUDOCODE ===

```python
import os
import sys
import datetime
import subprocess

# Step 1: Environment setup and default initialization
# CRM_HOME defaults to '/opt/etl/customer' if not set
crm_home = os.environ.get("CRM_HOME", "/opt/etl/customer")
run_date = os.environ.get("RUN_DATE", "UNKNOWN_DATE")

# Step 2: Define logging function
def log(message: str) -> None:
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

# Step 3: Log initiation of SCD2 historization
log(f"Starting SCD2 historization for run date {run_date}")

# Step 4: Define path to the script to execute
# # REVIEW-STRUCT: launcher [k_historization_load.ksh] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
script_to_run = os.path.join(crm_home, "customer", "k_historization_load.ksh")

# Step 5: Execute the sourced script using ksh as the execution environment
try:
    # We invoke ksh to run the script to mimic the 'sourcing' behavior as closely as possible in a subshell
    result = subprocess.run(["ksh", script_to_run], check=True, capture_output=False)
    rc = result.returncode
except subprocess.CalledProcessError as e:
    rc = e.returncode
    # Step 6: Handle non-zero exit codes from k_historization_load.ksh
    log(f"ERROR: k_historization_load.ksh failed with rc={rc}")
    sys.exit(rc)
except Exception as e:
    log(f"ERROR: Failed to launch k_historization_load.ksh: {str(e)}")
    sys.exit(1)

# Step 7: Log completion and exit with success code
log(f"Historization load completed for {run_date}")
sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/r_historization_load.ksh` | `customer/r_historization_load.py` | Migrates the shell wrapper to a Python script to manage execution logging, launch the historization run, and safely propagate return status codes. |

---

### Job Dependencies
* **Downstream**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated).
  * *Wiring Approach*: Once the downstream job `CUSTOMER.WEEKLY_SCHEDULE` is migrated, cross-job orchestration will be handled via Airflow's native dependency mechanisms (e.g., an `ExternalTaskSensor` or a dataset-triggered DAG run).

---

### Execution Order
The legacy orchestration sequence is preserved in the target Cloud Composer environment:
1. **Orchestration Entry**: Represented by the migrated Airflow DAG (`CUSTOMER_HISTORIZATION_LOAD_dag.py`).
2. **Execution Wrapper**: The DAG runs the Python script `customer/r_historization_load.py` (migrated from `r_historization_load.ksh`).
3. **Core Historization Execution**: `customer/r_historization_load.py` triggers the core logic corresponding to `customer/k_historization_load.ksh`.
4. **Data Load**: Sequentially executes the target SQLX assets converted from `customer/d_historization_load.sql`.
5. **Quality Check**: Executes the target SQLX assets converted from `customer/d_segment_quality_check.sql`.

---

### Scheduling
* **Trigger Mechanism**: This job is not directly triggered by a standalone cron schedule. It operates as an included/shared module within parent executions.
* **Target Construct**: In Cloud Composer / Airflow, this job must not be given its own independent schedule interval. Instead, it will be modeled as a reusable, importable `TaskGroup` or a callable DAG (triggered via `TriggerDagRunOperator`) to maintain its status as an importable unit.

---

### Schedule & Variables
* **RUN_DATE**: This variable is set by the legacy scheduler as `RUN_DATE = '&$TODAY'`.
  * *Target Delivery*: Sourced dynamically in Airflow using Airflow's native execution context variable `{{ ds }}` (logical date) and passed as an environment variable or execution argument to the Python script.

---

### Lineage
* **Downstream Invocation**: `customer/r_historization_load.ksh` --[INVOKES]--> `FILE:customer/k_historization_load.ksh`.

---

### External System Replacements
* **Shell Execution**: Legacies executing nested shells are modernized to native Cloud Composer / Airflow execution. The subprocess execution of `k_historization_load.ksh` can be replaced with direct Airflow task invocations or BigQuery operator execution once the underlying script is migrated.

---

### Cross-File Dependencies
* **Sourced Scripts**: Direct dependency on `customer/k_historization_load.ksh` which contains the core historization logic.

---

### Target File Plan

| Target File Path | Language | Source File |
| :--- | :--- | :--- |
| `customer/r_historization_load.py` | Python | `customer/r_historization_load.ksh` |

---

### Environment-Specific Values

1. **GLOBAL**
   * `CRM_HOME`: Sourced via `os.environ.get("CRM_HOME")`. This maps to the shared DAGs/scripts folder root path inside the GCS bucket or local path of the Cloud Composer environment.

2. **JOB-SPECIFIC**
   * `RUN_DATE`: Map to Airflow's runtime execution date `{{ ds }}` and pass it into the task execution context.

---

### Risks & Manual Steps
* **Downstream Blockers**: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated; the final cross-job orchestration wiring cannot be completed until that job exists on the target platform.
* **Coordination of Sourced File**: `customer/k_historization_load.ksh` is invoked by this wrapper but is not part of this design pass's source files. You must coordinate with the migration pass responsible for `k_historization_load.ksh` to ensure its target entrypoint is properly called by `customer/r_historization_load.py`.