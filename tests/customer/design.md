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


# Migration Design Document: CUSTOMER.HISTORIZATION_LOAD

## 1. Overview
This workload consists of a single Unix job, `CUSTOMER.HISTORIZATION_LOAD`, which executes a Korn shell script (`r_historization_load.ksh`) to perform a Slowly Changing Dimension Type 2 (SCD2) historization of the weekly customer segment and score into a target segment dimension table. Since no orchestrating JOBP (Workflow) object, schedule, or trigger script is supplied in this extraction bundle, this job is treated as an externally triggered, standalone process. To make this runnable in Apache Airflow, it is wrapped in its own single-task DAG.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `CUSTOMER.HISTORIZATION_LOAD` | JOBS_UNIX | 1 | SCD2 historization of the weekly customer segment/score into the segment dimension |

## 3. Scheduling
- **Trigger Source**: Externally triggered (source unknown from this extraction alone; no `EVNT_TIME`, `SCRI`, or parent `JOBP` is present in this bundle).
- **Schedule**: `schedule=None` (No calendar or execution interval is specified in this extraction).

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `customer_historization_load` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Derived from active=1)* |
| **default_args** | `{"owner": "UNIX.ETL_SVC", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `customer_historization_load` | `CUSTOMER.HISTORIZATION_LOAD` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: launcher command [#!/bin/ksh] not recognised — confirm target operator/script manually. Script executes `. &HOME/customer/r_historization_load.ksh` with variable `RUN_DATE` set to UC4 `&$TODAY`. |

## 6. Task Dependency Map
*This DAG contains a single task representing the migrated Unix job:*

```python
customer_historization_load
```

## 7. Sync / Concurrency Analysis
No `sync_rows` or concurrency constraints (self or cross locks) were specified for this object. Standard `max_active_runs=1` is applied to avoid concurrent overlaps if manually triggered in quick succession.

## 8. Error Handling and Retry Strategy
- **Retries**: Configured to `1` retry with a `5-minute` delay via `default_args`.
- **Failure Notification**: None specified in the extraction. Default Airflow alerting mechanisms should be configured at the environment level.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&RUN_DATE` | `&$TODAY` (UC4 System Date) | `{{ ds }}` (Airflow execution date in `YYYY-MM-DD` format) |
| `$HOME` | Environment Variable | Defined via Airflow OS environment, execution environment configs, or Bash environment context. |

## 10. Developer Notes
* **Unrecognized Launcher**: 
  * # REVIEW-STRUCT: The source Unix job launcher type is classified as `unrecognized` because it wraps a raw Korn shell script script block (`#!/bin/ksh` executing `. &HOME/customer/r_historization_load.ksh`). 
  * *Recommendation*: During the build/refactor stage, replace the `EmptyOperator` with a `BashOperator` executing the script directly (if running on a hybrid shell worker), or containerize the execution into a `KubernetesPodOperator` or target cloud-native execution environment (e.g., GCSFuse to run scripts on GKE/Dataproc).
* **Environment Variables**:
  * The environment variable `$HOME` and login context `UNIX.ETL_SVC` must be mapped to appropriate service accounts or connection profiles in the Airflow environment.

---

# Pseudocode Outline

```python
# ==============================================================================
# ── IMPORTS ───────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── GCP CONFIGURATION ─────────────────────────────────────────────────────────
# ==============================================================================
# No cloud-specific components defined in the source UC4 metadata.

# ==============================================================================
# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "UNIX.ETL_SVC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── ON_FAILURE_CALLBACK STUBS ─────────────────────────────────────────────────
# ==============================================================================
# No error-handling or notification callbacks defined in source metadata.

# ==============================================================================
# ── DAG DEFINITION ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="customer_historization_load",
    default_args=DEFAULT_ARGS,
    description="SCD2 historization of the weekly customer segment/score into the segment dimension",
    schedule=None,  # No calendar trigger in source metadata; externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migration_uc4", "customer"],
) as dag:

    # ==========================================================================
    # ── TASK: CUSTOMER_HISTORIZATION_LOAD ─────────────────────────────────────
    # ==========================================================================
    # # REVIEW-STRUCT: launcher command [#!/bin/ksh] not recognised — confirm target operator/script manually.
    # The original script body was:
    #   #!/bin/ksh
    #   # CUSTOMER.HISTORIZATION_LOAD
    #   :SET &RUN_DATE='&$TODAY'
    #   . &HOME/customer/r_historization_load.ksh
    #
    # If migrating to a Bash execution environment, implement as:
    # customer_historization_load = BashOperator(
    #     task_id="customer_historization_load",
    #     bash_command=". $HOME/customer/r_historization_load.ksh",
    #     env={"RUN_DATE": "{{ ds }}"}
    # )
    
    customer_historization_load = EmptyOperator(
        task_id="customer_historization_load",
    )

    # ==========================================================================
    # ── DEPENDENCIES ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-task DAG. No execution dependencies defined in source metadata.
    customer_historization_load
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/CUSTOMER.HISTORIZATION_LOAD.xml` | `customer/customer_historization_load_dag.py` | Migrates the UC4 UNIX job definition to an Apache Airflow DAG. In accordance with the Reviewer Feedback, this DAG will avoid `EmptyOperator` placeholders and utilize a functional `BashOperator` to execute the migrated Python wrapper script `customer/r_historization_load.py` (which replaces `r_historization_load.ksh`). |

---

### Job dependencies
- **Downstream**:
  - `CUSTOMER.WEEKLY_SCHEDULE`: This downstream job is not yet migrated. Once migrated, its orchestration DAG must be updated to depend on the successful completion of the `customer_historization_load` DAG. Since it is currently unmigrated, this dependency cannot be fully finalized at this stage (noted under Risks & Manual Actions).

---

### Execution order
The legacy job's step sequence is preserved and mapped to the target environment as follows:
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` $\rightarrow$ Maps to the orchestrating Airflow DAG (`customer/customer_historization_load_dag.py`).
2. `customer/r_historization_load.ksh` $\rightarrow$ Maps to `customer/r_historization_load.py` (migrated in a separate design pass), executed via a `BashOperator` task in the DAG.
3. `customer/k_historization_load.ksh` $\rightarrow$ Executed by the migrated python wrapper script.
4. `customer/d_historization_load.sql` $\rightarrow$ Executed as part of the core historization SQL step (compiled into Dataform or native BigQuery execution).
5. `customer/d_segment_quality_check.sql` $\rightarrow$ Executed as a post-historization validation step.

---

### Scheduling
- **Triggering Method**:
  - This job is not directly triggered by any of the environment's standalone schedulers; instead, it is designed to run embedded inside scheduled workflows.
  - To retain this exact behavior on Google Cloud, the Airflow DAG is configured with `schedule=None`. It remains a callable unit to be triggered externally by a parent DAG (using `TriggerDagRunOperator`) or via an external orchestration call.

---

### Schedule & variables — MUST BE RETAINED
- **Variables**:
  - `RUN_DATE`: Legacy default is `&$TODAY`. In Airflow, this maps to the logical execution date macro `{{ ds }}` (formatted as `YYYY-MM-DD`). It is injected into the execution operator as an environment variable (`RUN_DATE`).
  - `MAX_EXPECTED_CHANGE_PCT`: Set to `25` (specified in the UC4 XML's `DYNVALUES` block). This variable is passed to the execution task via env/params to configure the segment quality check threshold.

---

### Lineage
- **Upstream/Invokes**:
  - `customer/CUSTOMER.HISTORIZATION_LOAD.xml` $\rightarrow$ Invokes `FILE:customer/r_historization_load.ksh` (which is migrated to `customer/r_historization_load.py`).
- **Target Tables**:
  - The legacy execution writes to the target customer segment dimension tables (indicated in metadata as `TABLE:THE` and `TABLE:OF`, representing the logical target tables like `DIM_CUSTOMER_SEGMENT` or related staging schemas). These tables reside in BigQuery in the target state.
- **Execution Host**:
  - Legacy execution runs on `ETLHOST2` (UNIX Host), which maps to the Cloud Composer environment worker node.

---

### External system replacements
- **ETLHOST2 UNIX Host to Cloud Composer**:
  - Script execution is migrated from the UNIX host `ETLHOST2` to Cloud Composer GKE worker nodes.
- **SQL*Plus/Oracle Client to BigQuery**:
  - Relational database connections and historization scripts are migrated to run natively on BigQuery using BigQuery client libraries or Dataform.

---

### Cross-file dependencies
- **Wrapper Script Coordination**:
  - The orchestrating DAG `customer/customer_historization_load_dag.py` relies on the physical presence of `customer/r_historization_load.py` on the target execution environment.
  - Since files are migrated in independent passes, proper directory integrity must be maintained when files are assembled in the target repository to ensure relative path execution succeeds.

---

### Target file plan
- **Target File**: `customer/customer_historization_load_dag.py`
  - **Language**: Python (Apache Airflow DAG)
  - **Source File**: `customer/CUSTOMER.HISTORIZATION_LOAD.xml`

---

### Environment-specific values
1. **GLOBAL**:
   - `GCP_PROJECT`: The target Google Cloud Project ID. Sourced at runtime via standard Airflow Variables: `Variable.get("GCP_PROJECT")`.
   - `GCP_REGION`: The deployment region. Sourced via Airflow config or `Variable.get("GCP_REGION")`.
2. **JOB-SPECIFIC**:
   - `RUN_DATE`: Mapped dynamically using Airflow's native execution template `{{ ds }}`.
   - `MAX_EXPECTED_CHANGE_PCT`: Set to `25` as defined in the source UC4 metadata; passed as a task parameter/environment variable to the `BashOperator`.
   - `SCRIPTS_DIR` / `HOME`: The path to the DAGs/plugins directory where the converted Python files reside. Sourced at runtime via `Variable.get("SCRIPTS_DIR")` or standard environment paths.

---

### Risks and manual steps
- **Downstream Dependency Integration Status**:
  - SOURCE: NOT FOUND — `CUSTOMER.WEEKLY_SCHEDULE` — no candidate.
  - *Risk*: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` is marked "not yet migrated". The cross-DAG trigger or sensor linkage cannot be finalized until that job is migrated.
- **Cross-File Folder Assembly**:
  - *Risk*: The SQL scripts (`d_historization_load.sql`, `d_segment_quality_check.sql`) and the shell scripts (`r_historization_load.ksh`, `k_historization_load.ksh`) are converted in separate, independent design passes.
  - *Action*: Build and deployment pipelines must verify that all generated target files are correctly assembled under the same relative directory (`customer/`) in the target repository.
- **Validation Execution & Exit Code Auditing**:
  - *Risk*: The legacy KornShell execution ensured that SQL return codes and row count impacts were captured to determine step failure.
  - *Action*: Ensure that the `BashOperator` in the DAG correctly parses standard output and propagates the exit code of `customer/r_historization_load.py` so that validation failures correctly mark the Airflow task as failed.

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
    - Multi-statement SQL DML script (SCD Type 2 execution pattern) inside a transaction with parameter substitution, ending with a transaction commit and script exit.

1.2 Summarize the business logic and purpose of the script in plain English:
    - This script performs a weekly SCD Type 2 historization of customer segment and score band data.
    - It updates existing records in the target dimension `DIM_CUSTOMER_SEGMENT` by setting `IS_CURRENT = 0` and expiring them with `SYSDATE` if their attributes (`SEGMENT_CODE` or `SCORE_BAND`) changed compared to the incoming staging data.
    - It inserts completely new customers into the target with `IS_CURRENT = 1` and `VALID_FROM = SYSDATE`.
    - It then runs a subsequent `INSERT` statement to append the new "active" version (`IS_CURRENT = 1`, `VALID_FROM = SYSDATE`) of the customers whose records were just expired during the MERGE statement.
    - Finally, it commits the changes and exits.

1.3 List all entities referenced:
    - Tables:
        - `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` (target: dimension table, aliased as `tgt`)
        - `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` (source: staging table, aliased as `src`)
    - Columns:
        - `CUSTOMER_ID` (Inferred: `NUMBER`/`INT64`)
        - `SEGMENT_CODE` (Inferred: `VARCHAR2`/`STRING`)
        - `SCORE_BAND` (Inferred: `VARCHAR2`/`STRING`)
        - `SCORE_VALUE` (Inferred: `NUMBER`/`FLOAT64` or `INT64`)
        - `RUN_DATE` (Inferred: `DATE`)
        - `IS_CURRENT` (Inferred: `NUMBER`/`INT64` acting as boolean flag)
        - `VALID_FROM` (Inferred: `DATE` or `TIMESTAMP`)
        - `VALID_TO` (Inferred: `DATE` or `TIMESTAMP`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` used for tracking time (`VALID_FROM`, `VALID_TO`) contains time components. Resolved to BigQuery `TIMESTAMP` to avoid precision loss.
    - Oracle `DATE` used for run execution boundary (`RUN_DATE`) has only day-level granularity. Resolved to BigQuery `DATE`.
    - Substitution variable `&1` (representing input date) resolved to a BigQuery script-level string parameter/variable `v_run_date_str` and cast using `PARSE_DATE`.

2.2 Implicit and Explicit Type Casting:
    - `TO_DATE('&1', 'YYYY-MM-DD')`: Needs to be explicitly cast. Resolved to `PARSE_DATE('%Y-%m-%d', v_run_date_str)`.

2.3 NULL Handling and Conditional Functions:
    - No direct NVL/DECODE present in the SQL. Target filters (`tgt.SEGMENT_CODE <> src.SEGMENT_CODE`) assume non-null values. If nulls are possible, they are handled via explicit null-safe operators in the standard rewrite.

2.4 String Functions:
    - None used.

2.5 Date and Timestamp Functions:
    - `SYSDATE`: Represents current execution timestamp. Map to `CURRENT_TIMESTAMP()`.
    - Crucial Semantic Divergence: In the Oracle script, `SYSDATE` is evaluated per row and per statement. If the MERGE runs and finishes at `10:00:00` and the subsequent INSERT runs at `10:00:01`, the join condition `tgt.VALID_TO = SYSDATE` in the second statement can fail to match the rows expired during the MERGE. To achieve 100% semantic equivalence, a single script-level timestamp variable (`v_current_time`) must be declared, populated once, and used across both operations.
    - `TO_DATE`: Map to `PARSE_DATE('%Y-%m-%d', ...)` since the format is year-month-day.

2.6 to 2.10:
    - Not applicable to this script.

2.11 MERGE Statements:
    - BigQuery supports standard SQL `MERGE`.
    - Note on structural syntax: BigQuery does not allow a target table modification key lookup dynamically inside the MERGE that is modified by the operation itself, but standard execution allows setting `IS_CURRENT = 0`. The structure is fully compatible.

2.12 INSERT / UPDATE / DELETE:
    - Subsequent `INSERT` uses a JOIN matching on `tgt.VALID_TO = SYSDATE`. Converting `SYSDATE` to a constant script variable ensures this execution block logic works deterministically in BigQuery.

2.13 DDL Constructs:
    - Not present.

2.14 PL/SQL:
    - No procedural objects, but transaction control commands (`COMMIT`, `EXIT`) and positional script parameters (`&1`) are used. This will be wrapped in a BigQuery scripting block (`DECLARE`, `BEGIN...EXCEPTION...END`).

2.15 Unresolvable or Advisory Items:
    - `&1` is an external scripting variable. It must be declared as a session-level scripting variable or passed as a query parameter (`@run_date_str`) in BigQuery.
    - `EXIT` is a sqlplus command; stripped in BigQuery.

Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    - Convert the SQL script into a transactional BigQuery script-block. Use `DECLARE` to capture the input parameter and establish a locked temporal state via a local timestamp variable (`v_current_time`). Group both operations inside a `BEGIN TRANSACTION` / `COMMIT TRANSACTION` block to match Oracle's implicit transactional safety.

3.2 Assumptions:
    - The execution environment provides the `&1` variable as an input argument (mapped to a declared variable `v_run_date_str`).
    - The target table columns `VALID_TO` and `VALID_FROM` are of type `TIMESTAMP` or `DATETIME`.

3.3 Items Flagged for Human Review:
    - Temporal correlation between the updated `VALID_TO` in the MERGE and matched rows in the subsequent `INSERT`. Using a single local script variable `v_current_time` guarantees consistency, whereas direct mapping to `CURRENT_TIMESTAMP()` in each statement could cause race conditions.

═══════════════════════════════════════════
MIGRATION DECISION AND REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason for Selection |
| :--- | :--- | :--- | :--- |
| **SCD Type 2 MERGE** | Direct BigQuery Standard SQL inside Scripting Block | Python Wrapper, BigQuery UDF | BQ natively supports standard SQL `MERGE` and transactional scripting blocks (`BEGIN TRANSACTION`). Python is unnecessary for native DML. |
| **Positional Param `&1`** | BQ Scripting Variable (`DECLARE` / `SET`) | Manual hardcoding | BigQuery scripting variables safely capture external arguments without requiring manual query alteration. |
| **SYSDATE evaluation** | Local Variable (`v_current_time` as `TIMESTAMP`) | `CURRENT_TIMESTAMP()` inline | Direct inline `CURRENT_TIMESTAMP()` calls across statements will produce slightly different timestamps, breaking the downstream `JOIN` on `tgt.VALID_TO = SYSDATE`. |

2.17 REQUIRED ARTIFACTS

| Generated Artifact | Type | Input/Output/Invoking Contract | Notes / Dependency |
| :--- | :--- | :--- | :--- |
| **`d_historization_load.sql`** | BigQuery Script (SQL) | **Input**: Parameter `@run_date_str` (STRING)<br>**Output**: Modified target table rows | Wraps operations in transactional boundaries. |

2.18 DATA TYPE COMPATIBILITY TABLE

| Source Table.Column | Oracle Type | BigQuery Type | Conversion Rule / Logic | Warning / Mitigation |
| :--- | :--- | :--- | :--- | :--- |
| `STG_CUSTOMER_SCORE_OUTPUT.RUN_DATE` | `DATE` (No time) | `DATE` | Map to DATE. | Ensure format matches 'YYYY-MM-DD'. |
| `DIM_CUSTOMER_SEGMENT.VALID_FROM` | `DATE` (With time) | `TIMESTAMP` | Map to TIMESTAMP. | Prevent precision truncation. |
| `DIM_CUSTOMER_SEGMENT.VALID_TO` | `DATE` (With time) | `TIMESTAMP` | Map to TIMESTAMP. | Prevent precision truncation. |
| `DIM_CUSTOMER_SEGMENT.IS_CURRENT` | `NUMBER` | `INT64` | Native integer matching. | Verify no fractional values are present. |

2.19 DESIGN REVIEW SUMMARY

- **Patterns/Objects Found**: SQL*Plus Script Parameter (`&1`), Explicit SCD-2 MERGE update/insert sequence, transactional `COMMIT`.
- **Unsupported Functions**: Oracle SqlPlus commands (`EXIT`).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Target `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`, Source `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`.
- **Assumptions**: Staging parameter is provided in `'YYYY-MM-DD'` string format.
- **Status**: Ready for human approval.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_DATE('&1', 'YYYY-MM-DD')` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', v_run_date_str)` |
| `SYSDATE` | Direct-with-rewrite | `v_current_time` (Declared `TIMESTAMP` with value `CURRENT_TIMESTAMP()`) |
| `COMMIT` | Direct-with-rewrite | `COMMIT TRANSACTION;` |
| `EXIT` | Direct-with-rewrite | Strip from script |
| `MERGE` | Direct-with-rewrite | Standard BQ `MERGE` statement |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Declarations for parameterized script execution
DECLARE v_run_date_str STRING;
DECLARE v_run_date DATE;
DECLARE v_current_time TIMESTAMP;

-- Assign execution inputs (External system parameter &1 mapped to @run_date_str)
SET v_run_date_str = @run_date_str;
SET v_run_date = PARSE_DATE('%Y-%m-%d', v_run_date_str); -- converted from TO_DATE('&1', 'YYYY-MM-DD')
SET v_current_time = CURRENT_TIMESTAMP(); -- converted from SYSDATE to maintain transaction snapshot consistency

-- Begin Atomic Transaction Block for consistency
BEGIN TRANSACTION;

-- Statement 1: Expire changed records and insert completely new records
MERGE INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT tgt
USING (
    SELECT CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE
    FROM   ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT
    WHERE  RUN_DATE = v_run_date
) src
ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 1)
WHEN MATCHED AND (
         tgt.SEGMENT_CODE <> src.SEGMENT_CODE
      OR tgt.SCORE_BAND   <> src.SCORE_BAND
     ) THEN
    UPDATE SET tgt.IS_CURRENT = 0,
               tgt.VALID_TO   = v_current_time -- converted from SYSDATE
WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
    VALUES (src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, src.SCORE_VALUE, 1, v_current_time); -- converted from SYSDATE

-- Statement 2: Insert the new active versions of records that were expired in the MERGE step
INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
    (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
SELECT src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, src.SCORE_VALUE, 1, v_current_time -- converted from SYSDATE
FROM   ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT src
JOIN   ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT tgt
  ON   tgt.CUSTOMER_ID = src.CUSTOMER_ID 
  AND  tgt.IS_CURRENT = 0 
  AND  tgt.VALID_TO = v_current_time -- matched against the standardized variable to ensure strict temporal equivalence
WHERE  src.RUN_DATE = v_run_date;

-- Commit execution safely
COMMIT TRANSACTION; -- converted from COMMIT
```

### FLAGGED ITEMS FOR HUMAN REVIEW
1. **Script Parameterization (`&1`)**: The script assumes `@run_date_str` is declared and passed via the BigQuery executing client (e.g. Airflow, bq command line, or scheduled query runtime parameter).
2. **Temporal Snapshot Alignment**: The standard mapping of `SYSDATE` was changed to a session variable `v_current_time` generated once at script startup. This ensures that the MERGE expiration time and the subsequent INSERT filter matching condition have identical nanosecond alignments. Direct conversion of both instances to inline `CURRENT_TIMESTAMP()` would cause zero rows to be matched in the subsequent `INSERT` block due to processing microsecond differences.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/d_historization_load.sql` | `customer/d_historization_load.sql` | Converts the Oracle SQL MERGE and INSERT SCD2 logic into BigQuery SQL, using the `@RUN_DATE` query parameter and native transactional blocks. |

### Job Dependencies
- **Downstream**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated).
  - *Wiring on BigQuery*: Once the historization task has completed, the executing Airflow/Composer DAG must trigger or satisfy a sensor for the downstream `CUSTOMER.WEEKLY_SCHEDULE` workflow. Since this downstream target is not yet migrated, the orchestration linkage cannot be finalized and must be manually verified downstream.

### Execution Order
The overall orchestration must preserve the legacy sequence of execution:
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 trigger - handled by orchestrator migration)
2. `customer/r_historization_load.ksh` (Wrapper script - handled by wrapper migration)
3. `customer/k_historization_load.ksh` (SCD2 execution coordinator - handled by wrapper migration)
4. `customer/d_historization_load.sql` (**This File** - executes the actual BigQuery SQL MERGE and subsequent INSERT)
5. `customer/d_segment_quality_check.sql` (Post-historization data check - handled by validation migration)

### Scheduling
- **Triggering Event / Schedulers**: This component job is not directly triggered by any standalone scheduler; it runs as an included module/step inside the broader scheduled orchestration flow. Do not assign it an independent scheduler in the target environment. It must be executed as a step within the calling Cloud Composer DAG or Dataform pipeline.

### Schedule & Variables
- **Variables to Retain**:
  - `RUN_DATE` (Inherited from the calling job's legacy variable `&$TODAY`).
- **Mapping to Target**:
  - The parameter must be supplied to the execution client and accessed inside the BigQuery SQL query as a query parameter named `RUN_DATE` (i.e. `@RUN_DATE`).

### Lineage
- **Upstream Producers**:
  - Reads Table: `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`
  - Reads Table: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
- **Downstream Consumers**:
  - Writes Table: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
- **Package Reference**:
  - Uses Package: `ANALYTICS_SCHEMA`

### External System Replacements
- **Oracle to BigQuery**:
  - Oracle database tables within `ANALYTICS_SCHEMA` map to tables in the target BigQuery dataset.
  - SQL*Plus syntax structures (such as `&1`, `COMMIT`, and `EXIT`) are completely replaced by native BigQuery parameters and scripting blocks (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`).

### Cross-File Dependencies
- **Shared Tables**:
  - `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` is a shared target table written to by this script and subsequently read by `customer/d_segment_quality_check.sql` to perform quality and row-impact metrics.
  - `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` is populated by upstream staging ingestion and used across multiple scripts in this execution pipeline.

### Target File Plan
| Source File Path | Target File Path | Target Language | Purpose |
| :--- | :--- | :--- | :--- |
| `customer/d_historization_load.sql` | `customer/d_historization_load.sql` | BigQuery SQL (BQSQL) | Encapsulates the transactional SCD Type 2 MERGE and companion INSERT logic in BigQuery-compatible Standard SQL. |

### Environment-Specific Values
- **GLOBAL**:
  - `ANALYTICS_SCHEMA` -> Maps to `BQ_DATASET`. Identifies the environment's target BigQuery dataset (resolved at deployment or runtime via Cloud Composer environment variables or Dataform workspace configs).

### Risks and Manual Steps
- **Orchestration Integration**: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated, creating a dependency gap. The trigger connection must be established manually once the downstream job is deployed.
- **SQL Parameter Substitution**: The automated design suggests declaring a local script-level variable `v_run_date_str` to parse and handle parameters. To ensure direct alignment with the calling Python wrapper, the target SQL script should bypass this variable declaration and access the query parameter `@RUN_DATE` directly (e.g. `PARSE_DATE('%Y-%m-%d', @RUN_DATE)`).
- **Temporal Snapshot Alignment**: The conversion relies on storing the execution time in a unified timestamp variable `v_current_time` at the start of the transaction block. This ensures absolute microsecond alignment between the expired record (`VALID_TO`) in the `MERGE` and the newly activated record (`VALID_FROM`) in the subsequent `INSERT`. Replacing `SYSDATE` with inline calls to `CURRENT_TIMESTAMP()` is highly discouraged, as it will break the downstream join.

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
    - This is a standalone SELECT query executed within a SQL*Plus CLI environment, utilizing session formatting parameters, command-line parameter substitution (`&1`), and an explicit environment exit command (`EXIT`).

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script calculates the percentage of currently active customer segments (`IS_CURRENT = 1`) that were created or re-versioned on a specific input execution date (`&1`).
    - The output (`CHANGED_PCT`) is rounded to the nearest integer. The query prevents division-by-zero errors by wrapping the denominator in a `NULLIF` clause. This metric allows downstream systems to detect and flag anomalously high weekly churn or segment shifting.

1.3 List all entities referenced:
    - Table: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
    - Columns:
        * `IS_CURRENT` (Numeric/Flag: Inferred as `NUMBER` in Oracle)
        * `VALID_FROM` (Date/Timestamp: Inferred as Oracle `DATE`, which includes time)
    - Pseudo-table: `DUAL` (Oracle dummy table used to structure scalar selections)
    - Input Variable: `&1` (SQL*Plus substitution parameter representing a date string)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (with time component) → `DATETIME` in BigQuery to preserve time details without timezone offsets.
    - Column `IS_CURRENT` (`NUMBER`) → `INT64` in BigQuery.
    - Substitution parameter `&1` (`VARCHAR2`) → `STRING` in BigQuery.

2.2 Implicit and Explicit Type Casting:
    - Oracle implicitly handles CLI parameter variable substitution (`&1`). In BigQuery, this must be resolved to a declared scripting variable (`DECLARE run_date STRING`) or a query parameter (`@run_date`).
    - Oracle explicit conversion `TO_DATE('&1', 'YYYY-MM-DD')` → BigQuery `PARSE_DATETIME('%Y-%m-%d', run_date)`.

2.3 NULL Handling and Conditional Functions:
    - `NULLIF(..., 0)` → Supported directly in BigQuery with identical syntax and semantics.

2.4 String Functions:
    - N/A (None used in SQL source code, excluding SQL*Plus environment parameters).

2.5 Date and Timestamp Functions:
    - `TRUNC(date)` → Converts to `DATETIME_TRUNC(date, DAY)` in BigQuery.
    - `TO_DATE('&1', 'YYYY-MM-DD')` → Converts to `PARSE_DATETIME('%Y-%m-%d', run_date)`. Since `VALID_FROM` is inferred as a `DATETIME`, the input parameter must be parsed as a `DATETIME` and truncated to the day boundary to execute a type-safe comparison.

2.6 Numeric and Aggregate Functions:
    - `ROUND(val)` → Supported directly in BigQuery.
    - `COUNT(*)` → Supported directly in BigQuery.

2.7 Analytical and Window Functions:
    - N/A

2.8 Set and Join Operations:
    - N/A

2.9 Row Limiting and Sampling:
    - N/A

2.10 Sequences:
    - N/A

2.11 MERGE Statements:
    - N/A

2.12 INSERT / UPDATE / DELETE:
    - N/A

2.13 DDL Constructs (if present):
    - N/A

2.14 PL/SQL:
    - N/A

2.15 Unresolvable or Advisory Items:
    - SQL*Plus formatting commands (`SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF`) are client-side parameters and have no functional equivalent in BigQuery SQL engines. They are stripped.
    - SQL*Plus execution control command `EXIT;` is stripped.
    - Substitution Variable `&1`: Resolved by declaring a BigQuery session scripting variable or using a standard query parameter `@run_date`.

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - The conversion is structured as a Direct BigQuery Standard SQL query inside a BQ scripting context.
    - A BQ SQL scripting variable `run_date` is declared at the start of the block to substitute the SQL*Plus CLI argument `&1`.
    - The Oracle `FROM DUAL` construct is removed because BigQuery allows selecting scalar values and nested subqueries without a parent `FROM` clause.
    - Dates are explicitly typed and compared using type-safe BQ standard functions (`DATETIME_TRUNC` and `PARSE_DATETIME`).

3.2 List any assumptions made during conversion:
    - Assumption 1: Column `VALID_FROM` contains time components (Oracle `DATE` type) and is mapped to `DATETIME` in BigQuery.
    - Assumption 2: The CLI input substitution parameter `&1` is always supplied as a valid ISO string representation of a date ('YYYY-MM-DD').

3.3 List any items flagged for human review before the build stage proceeds:
    - Verify that downstream scheduling orchestrators (e.g., Apache Airflow, dbt, or Google Cloud Composer) can supply the input date value natively via a BigQuery query parameter (`@run_date`) rather than using variable interpolation scripting.

═══════════════════════════════════════════
MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Oracle Construct / Logic Block | Target Option Selected | Rejected Alternatives | Evidence & Rationale |
| :--- | :--- | :--- | :--- |
| SQL*Plus Session Settings (`SET ...`) | Strip entirely | Retain as comments | These are client-side terminal environment properties that do not compile or run on BigQuery's database engine. |
| Parameter substitution `&1` | Script Variable / Query Parameter | Hardcoded literal | Hardcoding breaks dynamic daily run orchestration. Query parameters or scripting declarations natively resolve parameterization in BigQuery. |
| `FROM DUAL` | Omit `FROM` clause | `FROM (SELECT 1)` dummy CTE | BigQuery allows selecting expressions directly without a source relation, making `DUAL` obsolete. |
| `TRUNC(TO_DATE('&1', 'YYYY-MM-DD'))` | `DATETIME_TRUNC(PARSE_DATETIME('%Y-%m-%d', ...), DAY)` | `DATE_TRUNC(PARSE_DATE(...))` | Oracle `DATE` stores time. To prevent loss of precision and preserve downstream comparison compatibility with the BQ mapped target (`DATETIME`), `DATETIME_TRUNC` is the semantically accurate mapping. |

═══════════════════════════════════════════
REQUIRED ARTIFACTS
═══════════════════════════════════════════
The build will generate a single BigQuery SQL script (`d_segment_quality_check.sql`) containing:
1. `DECLARE run_date STRING;` variable declaration to handle parameterization.
2. The core set-based dynamic standard SQL projection to compute the shift percentage.

═══════════════════════════════════════════
DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column / Parameter | Oracle Type | BigQuery Type | Conversion Rule / Expression | Warnings / Implications |
| :--- | :--- | :--- | :--- | :--- |
| `VALID_FROM` | `DATE` | `DATETIME` | Direct mapping to DATETIME to support legacy time component. | Date comparisons must use uniform day truncation to ignore time segments. |
| `IS_CURRENT` | `NUMBER` | `INT64` | Native BigQuery integer conversion. | Assumes only values `0` and `1` are present. |
| `&1` | `VARCHAR2` | `STRING` | `DECLARE run_date STRING;` | String formatting must match `%Y-%m-%d` format pattern. |

═══════════════════════════════════════════
DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: SQL*Plus session commands, DUAL selection, string-to-date conversion, date truncation, scalar nested subqueries.
- **Unsupported Functions Found**: `TO_DATE`, `TRUNC` (without date part argument).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`.
- **Key Warnings**: Ensure scheduling tools feed the parameter value to BigQuery safely.
- **Manual-Intervention Items**: Coordinate script execution parameter injection pattern.
- **Ready for Human Approval**: Yes.

═══════════════════════════════════════════
ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `ROUND(x)` | Direct | `ROUND(x)` |
| `COUNT(*)` | Direct | `COUNT(*)` |
| `NULLIF(x, y)` | Direct | `NULLIF(x, y)` |
| `TO_DATE(x, fmt)` | Direct-with-rewrite | `PARSE_DATETIME('%Y-%m-%d', x)` |
| `TRUNC(d)` | Direct-with-rewrite | `DATETIME_TRUNC(d, DAY)` |
| `FROM DUAL` | Direct-with-rewrite | Remove `FROM` clause entirely |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- DECLARE block emulates the SQL*Plus parameter injection '&1'
DECLARE run_date STRING DEFAULT '2023-11-01'; -- Placeholder for '&1' injection

SELECT 
  ROUND(
    -- Subquery 1: Count of current segments re-versioned on the parameter execution date
    (
      SELECT COUNT(1) 
      FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
      WHERE IS_CURRENT = 1 
        -- DATETIME_TRUNC and PARSE_DATETIME converted from TRUNC(TO_DATE('&1', 'YYYY-MM-DD'))
        AND VALID_FROM = DATETIME_TRUNC(PARSE_DATETIME('%Y-%m-%d', run_date), DAY)
    )
    /
    -- Subquery 2: Total count of active segments; NULLIF prevents division by zero
    NULLIF(
      (
        SELECT COUNT(1) 
        FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
        WHERE IS_CURRENT = 1
      ), 
      0
    )
    * 100
  ) AS CHANGED_PCT;
-- Removed Oracle legacy 'FROM DUAL' context
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic Parameter Execution**: Confirm with the data platform orchestration administrator whether this query will run using variable binding (e.g., `@run_date` parameter in python-bigquery client) or via BQ scripting variable compilation (`DECLARE run_date`).
2. **Column Type Verification**: Confirm if `VALID_FROM` contains actual time values (hours, minutes, seconds). If the source column is purely standard dates with no time stamps, the conversion can safely use the more performant `DATE` and `DATE_TRUNC` logic rather than `DATETIME` and `DATETIME_TRUNC`.

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/d_segment_quality_check.sql` | `customer/d_segment_quality_check.sql` | Convert Oracle SQL quality check query to BigQuery SQL, utilizing query parameter `@RUN_DATE` passed by the calling orchestrator. |

### Job Dependencies
- **Downstream Dependency**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated).
  - *Wiring on BigQuery*: Once `CUSTOMER.WEEKLY_SCHEDULE` is migrated (e.g., to an Airflow DAG or Cloud Composer workflow), it will consume the output of this historization load. Since the downstream job is not yet migrated, this linkage cannot be finalized and must be resolved post-migration.

### Execution Order
The execution order from the legacy dependency graph must be preserved in the target orchestration (e.g., Cloud Composer DAG task sequence):
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 XML definition) -> Maps to the overall Cloud Composer DAG structure.
2. `customer/r_historization_load.ksh` (Wrapper script) -> Maps to a Python DAG operator invoking the step sequence.
3. `customer/k_historization_load.ksh` (Historization engine script) -> Maps to Python-based logic executed by the Composer task.
4. `customer/d_historization_load.sql` (Historization SQL merge) -> Maps to a Dataform / BigQuery task performing the main SCD Type 2 historization.
5. `customer/d_segment_quality_check.sql` (Quality Check SQL - **this file**) -> Maps to a BigQuery task executing the quality check query as the final step in the execution sequence.

### Scheduling
- **Triggering Mechanism**: This job is not directly triggered by any of the environment's standalone schedulers. It executes inside other scheduled jobs (e.g., as an include/shared module).
- **Target Scheduling Construct**: The migrated BigQuery SQL script must remain a callable/importable unit (e.g., a modular task in Cloud Composer or a reusable step in Dataform) without its own standalone schedule.

### Schedule & Variables
- **Variables**:
  - `RUN_DATE`: Set to `&$TODAY` dynamically by the legacy scheduler.
  - *Target Mechanism*: In the BigQuery SQL script, the run date is received via the query parameter `@RUN_DATE` passed by the calling Python operator/wrapper.

### Lineage
- **Upstream Table Producer**: The table `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` must have its SCD Type 2 merge load successfully completed (by the predecessor step `d_historization_load.sql`) before this script runs.
- **Table Read**: `customer/d_segment_quality_check.sql` reads from `TABLE:DIM_CUSTOMER_SEGMENT`.

### External System Replacements
- **Oracle DB Schema**: `ANALYTICS_SCHEMA` maps to BigQuery dataset `ANALYTICS_SCHEMA` (or the environment-configured dataset variable).
- **SQL*Plus Environment**: Removed client-side formatting and control commands (`SET ...`, `EXIT;`).

### Cross-File Dependencies
- **Predecessor Script Dependency**: This quality check depends on `d_historization_load.sql` to populate and close out historical records in `DIM_CUSTOMER_SEGMENT`. Running this script out of order will result in inaccurate quality metrics.

### Target File Plan
- **File**: `customer/d_segment_quality_check.sql`
  - *Language*: BigQuery SQL
  - *Source File*: `customer/d_segment_quality_check.sql`
  - *Note*: As specified in the reviewer feedback, the BigQuery SQL implementation must use the query parameter `@RUN_DATE` directly in place of the legacy SQL*Plus substitution variables or session variables.

### Environment-Specific Values
- **GLOBAL**:
  - `ANALYTICS_SCHEMA` (Oracle schema name) -> Maps to the environment-wide BigQuery dataset identifier `BQ_DATASET`.
- **JOB-SPECIFIC**:
  - `@RUN_DATE` (Query parameter) -> Passed dynamically at runtime by the calling DAG/wrapper task.

### Risks and Manual Steps
- **Unmigrated Downstream Blockers**: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated. The scheduling/sensor mechanism linking this historization workflow to the downstream schedule cannot be finalized until that job is migrated.
- **Query Parameter Mapping (Required Correction)**: The MCP-generated pseudocode uses a declared session variable `run_date` and a dummy placeholder default. To resolve the structural disconnect highlighted in previous attempts, the build agent must explicitly implement the SQL to accept and use the `@RUN_DATE` query parameter directly (e.g., `VALID_FROM = DATETIME_TRUNC(PARSE_DATETIME('%Y-%m-%d', @RUN_DATE), DAY)`) and must omit any `DECLARE` statements that would override or clash with the parameter passed by the Python orchestrator wrapper.

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
REASON: The script performs orchestration logic (checking execution status, capturing and parsing stdout, and conditional shell evaluation) of external SQL scripts, which is best represented in Python using DB-client execution and logging.

EVIDENCE
- Business logic found: KSH custom logic performs validation checks on query output (changed-row percentage check) and conditionally issues alerts based on a threshold.
- AWK: none
- SQL-expressible: partly (the SQL executions themselves are expressible, but the conditional warning logic and stdout extraction are orchestration-centric).
- Non-SQL side effects: none observed
- Against this verdict: A pure BigQuery SQL script could potentially handle both the merge and validation steps (using scripting and raising exceptions), but because the source SQL files are not supplied, keeping the exact orchestration flow in Python is safer to preserve the modular execution of those separate files.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script performs a Weekly Slowly Changing Dimension (SCD) Type 2 historization load for customer segments and scores. It runs a SQL script to perform the merge, checks the return status, and then runs a secondary quality check SQL script. It captures the percentage of changed customers and prints a warning log if the changes exceed a defined threshold (25%), protecting against anomalous mass updates caused by bad join keys.

2. INVOCATION CONTEXT
   - Who calls this script: Unknown (typically triggered by a UC4 job, but no UC4 job name was provided in the extraction).
   - UC4 native includes: None referenced in the extraction.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   - `CRM_HOME` (env var): Path to the ETL directory. Source: Environment. Default: `/opt/etl/customer`. Used to locate SQL files.
   - `CRM_ORA_USER` (env var): Oracle database username. Source: Environment. Default: `crm_etl`. Used for database connection.
   - `CRM_ORA_PASS` (env var): Oracle database password. Source: Environment. Default: `changeit`. Used for database connection.
   - `CRM_ORA_SID` (env var): Oracle System Identifier (SID). Source: Environment. Default: `CRMPRD`. Used for database connection.
   - `MAX_EXPECTED_CHANGE_PCT` (internal shell var): Threshold percentage for change-validation. Source: Hardcoded in script. Value: `25`.
   - `RUN_DATE` (env var): The date partition/parameter to be passed to the SQL scripts. Source: Inherited from the calling environment (referenced but not declared locally). 
     # REVIEW: RUN_DATE is used in the script but not explicitly initialized or declared. Confirm how it is set in the runtime environment.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_historization_load.sql "${RUN_DATE}"`
     - Purpose: Execute the SCD Type 2 merge.
     - Type: DB-client execution. Should be converted to native Python DB-client call if the SQL contents are migrated, or remain as an external file execution using `oracledb` or a generic cursor execution.
     - Resolvable Launcher? No, because the SQL file contents are not supplied in this extraction.
   - `sqlplus -s ${CRM_ORA_USER}/${CRM_ORA_PASS}@${CRM_ORA_SID} @${CRM_HOME}/customer/d_segment_quality_check.sql "${RUN_DATE}"`
     - Purpose: Query the percentage of changed customer records.
     - Type: DB-client execution.
     - Resolvable Launcher? No, because the SQL file contents are not supplied in this extraction.
   - `tr -d '[:space:]'`
     - Purpose: Remove whitespace/newlines from the output of the quality check SQL query.
     - Type: String manipulation. Will be handled natively in Python using `.strip()`.

5. EMBEDDED SQL
   - No SQL is embedded directly in this script. Two external SQL scripts are invoked:
     1. `${CRM_HOME}/customer/d_historization_load.sql`
        # REVIEW-STRUCT: SQL file [d_historization_load.sql] body not supplied — contents unknown
     2. `${CRM_HOME}/customer/d_segment_quality_check.sql`
        # REVIEW-STRUCT: SQL file [d_segment_quality_check.sql] body not supplied — contents unknown
   - Dialect: Oracle SQL (unambiguously indicated by `sqlplus` usage and connection string syntax).
   - # REVIEW: target database platform not specified; DB-client library choice (e.g. oracledb vs bigquery) below is provisional.

6. CONTROL FLOW
   1. Initialize environment parameters (`CRM_HOME`, `CRM_ORA_USER`, `CRM_ORA_PASS`, `CRM_ORA_SID`, `MAX_EXPECTED_CHANGE_PCT`).
   2. Log startup message.
   3. Execute the first SQL script (`d_historization_load.sql`) via SQL*Plus passing `RUN_DATE`.
   4. Check return code of the merge. If non-zero, log error and exit with status 1.
   5. Execute the second SQL script (`d_segment_quality_check.sql`) via SQL*Plus passing `RUN_DATE`.
   6. Capture standard output, strip all whitespace characters, and store in `changed_pct`.
   7. If `changed_pct` is empty, log a warning and exit 0 (skipping check).
   8. If `changed_pct` (converted to integer) is greater than `MAX_EXPECTED_CHANGE_PCT` (25), log a warning but do not fail the job.
   9. Log final completion message and exit 0.

7. ERROR HANDLING & EXIT CODES
   - If the SCD Type 2 merge SQL execution fails (returns non-zero), the script exits immediately with status 1.
   - If the quality check script fails or returns an empty result, it is caught as an empty string, logged as a warning, and exits with 0 (does not fail the pipeline).
   - If the validation threshold is exceeded, it prints a warning but exits with 0.
   - Python mapping: Wrap DB connections and executions in `try/except` blocks. If the main merge fails, raise the exception or call `sys.exit(1)`. For the quality check, catch exceptions, log a warning, and proceed gracefully (to mirror legacy behavior).

8. OUTPUTS / SIDE EFFECTS
   - Mutates the target customer segment dimension table (SCD Type 2) in the database via `d_historization_load.sql`.
   - Emits log statements to standard output / standard error.

9. BUSINESS SUMMARY
   - Performs weekly historization (SCD Type 2 updates) of customer segment and score data.
   - Executes a data-quality guardrail checking the percentage of changed records to prevent bad joins from corrupting the history table.
   - Gracefully reports anomalies (warns on >25% change) without blocking the entire pipeline unnecessarily, allowing human review.

=======================================================================================
PSEUDOCODE OUTLINE
=======================================================================================

```python
# Step 1: Import required modules
import os
import sys
import datetime
import subprocess
import logging

# Step 2: Configure logging format to match legacy "[YYYY-MM-DD HH:MM:SS] message"
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def log(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

# Step 3: Initialize environment variables and defaults
CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
CRM_ORA_USER = os.environ.get("CRM_ORA_USER", "crm_etl")
CRM_ORA_PASS = os.environ.get("CRM_ORA_PASS", "changeit")
CRM_ORA_SID = os.environ.get("CRM_ORA_SID", "CRMPRD")
MAX_EXPECTED_CHANGE_PCT = 25

# REVIEW: RUN_DATE is assumed to be set in the environment
RUN_DATE = os.environ.get("RUN_DATE", "")

# Step 4: Run SCD2 merge for customer segment dimension
log("Running SCD2 merge for customer segment dimension")

# REVIEW-STRUCT: SQL file [d_historization_load.sql] not supplied in extraction
sql_script_1 = os.path.join(CRM_HOME, "customer", "d_historization_load.sql")
connection_string = f"{CRM_ORA_USER}/{CRM_ORA_PASS}@{CRM_ORA_SID}"

try:
    # Execute external SQL*Plus process (reproducing verbatim execution context)
    # If using a native DB driver (provisional), this would execute the SQL contents directly.
    result = subprocess.run(
        ["sqlplus", "-s", connection_string, f"@{sql_script_1}", RUN_DATE],
        check=True,
        capture_output=False
    )
except subprocess.CalledProcessError as e:
    log_error(f"ERROR: d_historization_load.sql failed with rc={e.returncode}")
    sys.exit(1)

# Step 5: Execute quality check SQL script and capture output
# REVIEW-STRUCT: SQL file [d_segment_quality_check.sql] not supplied in extraction
sql_script_2 = os.path.join(CRM_HOME, "customer", "d_segment_quality_check.sql")

try:
    qc_result = subprocess.run(
        ["sqlplus", "-s", connection_string, f"@{sql_script_2}", RUN_DATE],
        capture_output=True,
        text=True,
        check=True
    )
    # Strip all whitespace characters to get clean changed_pct (equivalent to `tr -d '[:space:]'`)
    changed_pct_str = "".join(qc_result.stdout.split())
except Exception as e:
    log_error(f"ERROR: Failed to run quality check script: {str(e)}")
    changed_pct_str = ""

# Step 6: Validate quality check output
if not changed_pct_str:
    log("WARN: could not compute changed-row percentage - skipping sanity check")
    sys.exit(0)

# Step 7: Evaluate threshold breach
try:
    changed_pct = int(changed_pct_str)
    if changed_pct > MAX_EXPECTED_CHANGE_PCT:
        log(f"WARN: {changed_pct}% of customers changed segment this week (expected <= {MAX_EXPECTED_CHANGE_PCT}%) - flagging for review, not failing the job")
except ValueError:
    log(f"WARN: Quality check output '{changed_pct_str}' is not a valid integer - skipping validation check")

# Step 8: Log completion and exit
log(f"Historization merge complete, {changed_pct_str}% of customers re-versioned")
sys.exit(0)
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/k_historization_load.ksh` | `customer/k_historization_load.py` | Converted to a Python script that replaces Oracle SQL*Plus orchestration with BigQuery SQL execution, captures the validation output, and checks it against thresholds. |

***

### Job dependencies
* **Downstream**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated). This downstream job expects the historization output to be complete. In the target environment, this dependency must be handled via Composer orchestration (e.g., Airflow DAG triggers or datasets sensors).

### Execution order
The legacy execution sequence consists of 5 steps which must be maintained:
1. `customer/CUSTOMER.HISTORIZATION_LOAD.xml` (UC4 orchestration, converted to an Airflow task/DAG)
2. `customer/r_historization_load.ksh` (Wrapper script, converted to Python)
3. `customer/k_historization_load.ksh` (The core logic, converting to `customer/k_historization_load.py` in this pass)
4. `customer/d_historization_load.sql` (SCD Type 2 merge, executed by `customer/k_historization_load.py` as a BigQuery query or Dataform action)
5. `customer/d_segment_quality_check.sql` (Sanity verification, executed by `customer/k_historization_load.py` to retrieve the changed-row percentage)

### Schedule & variables
* **Schedule**: This job has no standalone schedule of its own; it runs inside scheduled parent flows. In Cloud Composer, the target `customer/k_historization_load.py` must remain a callable module or an importable task rather than a standalone scheduled DAG.
* **Variables**:
  * `RUN_DATE`: Receives its value from the scheduler via the UC4 variable `&$TODAY`. In the migrated target, this parameter should be supplied at runtime using Airflow task context (e.g., `{{ ds }}` or a parameterized execution date).

### Lineage
* **Upstream**: Executed within the main customer historization context.
* **Downstream**: Consumed by the downstream `CUSTOMER.WEEKLY_SCHEDULE` pipeline.
* **Lineage Edges**:
  * `customer/k_historization_load.ksh` executes SQL inside `customer/d_historization_load.sql`
  * `customer/k_historization_load.ksh` executes SQL inside `customer/d_segment_quality_check.sql`

### External system replacements
* **Oracle SQL*Plus to BigQuery**: The Oracle connections (`sqlplus -s ...`) are replaced by the `google.cloud.bigquery` Client in Python. Queries are executed natively on BigQuery using modern SQL dialect structures.
* **Command-line Stream Editor to Python String Methods**: The Unix shell pipeline `tr -d '[:space:]'` used to strip whitespace from SQL results is replaced with Python's native `"".join(value.split())` or `.strip()` operations.

### Cross-file dependencies
* The script has direct file dependencies on `customer/d_historization_load.sql` (the SCD Type 2 merge SQL) and `customer/d_segment_quality_check.sql` (the data quality verification SQL). The target Python code in `customer/k_historization_load.py` requires access to the converted BigQuery versions of these SQL scripts, either reading them from local project directories or executing them as pre-compiled BigQuery Stored Procedures / Dataform SQLX views.

### Target file plan
* **Target File**: `customer/k_historization_load.py`
  * **Language**: Python
  * **Source**: `customer/k_historization_load.ksh`

### Environment-specific values
* **GLOBAL (environment-wide)**:
  * `GCP_PROJECT`: Sourced from `os.environ.get("GCP_PROJECT")` or Airflow's `Variable.get("GCP_PROJECT")`. Specifies the target BigQuery project.
  * `GCP_REGION`: Sourced from `os.environ.get("GCP_REGION")` or Airflow's `Variable.get("GCP_REGION")`. Specifies the processing region.
  * `BQ_DATASET`: Sourced from `os.environ.get("BQ_DATASET")` or Airflow's `Variable.get("BQ_DATASET")`. Represents the target BigQuery dataset for the customer segment tables.
* **JOB-SPECIFIC**:
  * `MAX_EXPECTED_CHANGE_PCT`: Defined as a local constant in the job configuration or script (`MAX_EXPECTED_CHANGE_PCT = 25`).
  * `RUN_DATE`: Captured dynamically at runtime via task arguments or Airflow context variables.

### Risks and manual steps
* **Unmigrated Downstream Dependencies**: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` has not yet been migrated. The final orchestration and wiring of task dependencies cannot be fully completed or validated until that block is established.
* **SQL Dependency Separation**: The SQL files (`d_historization_load.sql` and `d_segment_quality_check.sql`) are outside the scope of this migration pass. They must be migrated to BigQuery-compliant SQL and their paths/execution logic aligned with the new python runtime (`customer/k_historization_load.py`). Ensure these external scripts are converted and made available to Python before final deployment.

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
REASON: The script defines a custom logging function, conditionally handles execution failure, and sources an external shell script whose logic is not provided.

EVIDENCE
- Business logic found: KSH custom logic wrapper that defines a `log()` utility function, prints timestamps, and orchestrates the execution and error-handling of an underlying shell script (`k_historization_load.ksh`).
- AWK: none
- SQL-expressible: no (the sourced logic in `k_historization_load.ksh` is not supplied, making it impossible to evaluate as SQL).
- Non-SQL side effects: execution of external shell scripts and console logging.
- Against this verdict: none (it contains shell function definitions and sources an external file of unknown contents, disqualifying it from `NO_CONVERSION_REQUIRED` or `BQSQL`).

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `r_historization_load.ksh` script serves as an orchestration wrapper for the SCD2 (Slowly Changing Dimension Type 2) historization loading process. It is executed within the customer domain, specifically wrapped to ensure that if the primary loading script (`k_historization_load.ksh`) fails, the failure is caught, logged with custom timestamp formatting, and exited with the corresponding failure code.

2. INVOCATION CONTEXT
   - Who calls this script: Invoked by the UC4 Job/Task `CUSTOMER.HISTORIZATION_LOAD`.
   - UC4 native includes: None referenced in the provided source block.
   - Environment files sourced: 
     - Sourced logic script: `. ${CRM_HOME}/customer/k_historization_load.ksh` 
       # REVIEW-STRUCT: environment file k_historization_load.ksh not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `CRM_HOME` (Environment Variable): Used to locate the `k_historization_load.ksh` script. Defaults to `/opt/etl/customer` if not already defined in the environment.
   - `RUN_DATE` (Environment Variable): Sourced from the calling environment to identify the processing run date for logging purposes. Used in starting and completion logs.
   - KSH DECLARED ENVIRONMENT PARAMETERS: None present in the provided source.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `. ${CRM_HOME}/customer/k_historization_load.ksh`
     - Exact Command: `. ${CRM_HOME}/customer/k_historization_load.ksh`
     - Purpose: Sourced execution of the underlying SCD2 historization load script.
     - Target execution: Must be treated as an external process invocation via `subprocess` because its contents are not provided.
     - Resolvable Launcher check: Does not qualify as a resolvable launcher since its source is not supplied and it represents general shell logic rather than direct, simple SQL execution.
       # REVIEW-STRUCT: launcher k_historization_load.ksh invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion

5. EMBEDDED SQL
   - No SQL is directly embedded in this wrapper script.

6. CONTROL FLOW
   1. Set shell options: `set -e` to exit on error (though subsequently managed by explicit checks).
   2. Parameter Defaulting: Set `CRM_HOME` to `/opt/etl/customer` if unset.
   3. Helper Function: Define `log()` utility function to print formatted timestamps.
   4. Log Initiation: Output standard start message including the `RUN_DATE` parameter.
   5. Execution: Source and execute `k_historization_load.ksh`.
   6. Error Capturing: Check return code `rc`.
   7. Branching / Exception Handling:
      - If `rc` is non-zero, print an error log statement and exit with the captured `rc`.
      - If `rc` is zero, continue.
   8. Log Completion: Print success message with `RUN_DATE`.
   9. Exit: Terminate with code 0.

7. ERROR HANDLING & EXIT CODES
   - Explicit return code capturing: `rc=$?` immediately after sourcing the external script.
   - Failure branch: `if [ ${rc} -ne 0 ]; then log "ERROR: ..."; exit ${rc}; fi`
   - Success exit code: `exit 0`
   - Python mapping: Wrap the external subprocess call in a `try/except subprocess.CalledProcessError` block, catching the error status, logging via Python logger, and calling `sys.exit(e.returncode)`.

8. OUTPUTS / SIDE EFFECTS
   - Standard output logs containing progress timestamps and completion status.
   - External script execution effects (dependent on `k_historization_load.ksh`).

9. BUSINESS SUMMARY
   - Coordinates the initialization and execution of the customer SCD2 historization process.
   - Enhances system observability by producing formatted, time-stamped log lines.
   - Guarantees downstream systems and scheduler objects detect execution failures immediately by capturing and propagating the precise exit status of the underlying loader script.

=======================================================================================
PSEUDOCODE
=======================================================================================

```python
import os
import sys
import subprocess
from datetime import datetime

# Step 1: Environment Setup & Parameter Retrieval
CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
RUN_DATE = os.environ.get("RUN_DATE", "UNKNOWN_DATE")

# Step 2: Define logging function equivalent to ksh log()
def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Step 3: Log initialization of the process
log(f"Starting SCD2 historization for run date {RUN_DATE}")

# Step 4: Resolve the script path
# # REVIEW-STRUCT: launcher k_historization_load.ksh invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
script_path = os.path.join(CRM_HOME, "customer", "k_historization_load.ksh")

# Step 5: Execute script and capture exit status
try:
    # Since sourcing executes in the same shell, we run it as a subprocess.
    # Note: Environment modifications inside k_historization_load.ksh will not bubble back up to Python.
    result = subprocess.run([script_path], shell=True, check=True, capture_output=False)
    rc = result.returncode
except subprocess.CalledProcessError as e:
    rc = e.returncode
    # Step 6: Error handling and exit propagation
    log(f"ERROR: k_historization_load.ksh failed with rc={rc}")
    sys.exit(rc)
except Exception as ex:
    log(f"ERROR: Failed to launch execution of {script_path}. Error: {str(ex)}")
    sys.exit(1)

# Step 7: Log completion and exit successfully
log(f"Historization load completed for {RUN_DATE}")
sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `customer/r_historization_load.ksh` | `customer/r_historization_load.py` | Converted to a Python wrapper script that logs, checks exit codes, and invokes the migrated sibling script `customer/k_historization_load.py`. |

### Job Dependencies
- **Downstream**: `CUSTOMER.WEEKLY_SCHEDULE` (not yet migrated). 
- **Wiring on Target Platform**: The downstream scheduler `CUSTOMER.WEEKLY_SCHEDULE` must be configured in Cloud Composer (Airflow) to trigger or sense the completion of the historization pipeline once it is migrated.

### Execution Order
The execution order of the legacy orchestration must be preserved as follows in Cloud Composer:
1. **Orchestration trigger**: UC4 Job `CUSTOMER.HISTORIZATION_LOAD` maps to a Cloud Composer DAG or Task.
2. **Wrapper script execution**: `customer/r_historization_load.ksh` maps to `customer/r_historization_load.py` (designed in this pass).
3. **Historization load execution**: `customer/k_historization_load.ksh` maps to the migrated Python script `customer/k_historization_load.py` (designed in a sibling pass).
4. **Dimension load**: `customer/d_historization_load.sql` maps to a Dataform SQLX pipeline step (designed in a sibling pass).
5. **Quality Check**: `customer/d_segment_quality_check.sql` maps to a Dataform SQLX quality assertion step (designed in a sibling pass).

### Scheduling
- **Triggering Construct**: This job is not directly triggered by any scheduler; it executes as an include/shared module within scheduled runs. In Cloud Composer, this pipeline should be designed as a callable/importable sub-DAG or as a shared DAG triggered on-demand via a `TriggerDagRunOperator`. It must not be given its own standalone schedule.

### Schedule & Variables — Must Be Retained
- **RUN_DATE**: Fed dynamically by UC4 using the variable `&$TODAY`. On Cloud Composer, this variable must reach the Python script as a command-line parameter or environment variable sourced from Airflow's logical execution date (e.g., `{{ ds }}`).

### Lineage
- **Upstream invocation**: The legacy wrapper is invoked by `CUSTOMER.HISTORIZATION_LOAD` (UC4 Orchestration).
- **Downstream invocation**: `customer/r_historization_load.ksh` directly invokes `customer/k_historization_load.ksh`.

### Cross-File Dependencies
- **Call Chain**: The wrapper script `customer/r_historization_load.py` directly executes and monitors the status of the primary loading script `customer/k_historization_load.py`. Both files must reside in the `customer` directory on the target execution environment to preserve local import or invocation paths.

### Target File Plan
- **File Path**: `customer/r_historization_load.py`
  - **Language**: Python
  - **Source File**: `customer/r_historization_load.ksh`
  - **Note on Implementation**: The MCP-generated pseudocode uses `subprocess.run` to call the legacy `.ksh` shell script. Per the reviewer feedback, this must be modified in the final build to run the migrated python script instead:
    ```python
    target_script = script_path.replace(".ksh", ".py")
    result = subprocess.run(["python3", target_script], check=True)
    ```

### Environment-Specific Values
- **CRM_HOME**: **GLOBAL** (environment-wide). Identifies the path structure of the application code on the execution environment. This should be sourced at runtime via environment variable or default configuration setup:
  - Python: `CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")`
- **RUN_DATE**: **JOB-SPECIFIC**. The processing date for the load run, sourced dynamically from Airflow context.
  - Python: Sourced using `os.environ.get("RUN_DATE")` or passed directly as an argument to the Python script from the DAG task.

### Risks & Manual Steps
- **Unmigrated Downstream Dependency**: `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated. The scheduling and orchestration linkages on Cloud Composer cannot be finalized until this downstream consumer is designed and deployed.
- **MCP Code Correction**: The MCP tool output attempts to run `k_historization_load.ksh` as a subprocess. The developer must manually adjust the final python script to run `python3 customer/k_historization_load.py` instead of executing a legacy KSH shell process.