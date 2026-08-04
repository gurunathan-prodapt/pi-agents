=== OBJECT: SALES.PRODUCT_AND_SALES_EXTRACT (JOBS_UNIX) ===
active=1
title=Source availability check, product master SCD2 load, and daily sales extract into staging
login=UNIX.ETL_SVC
host=|ETLHOST3|HOST
ert_seconds=30
launcher_type=unrecognized
launcher_details={'raw_command': '#!/bin/ksh'}
script_body:
#!/bin/ksh
# SALES.PRODUCT_AND_SALES_EXTRACT
:SET &RUN_DATE='&$TODAY'
. &HOME/sales/r_product_and_sales_extract.ksh
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 Workload Migration Assessment & Technical Design
**Target Platform:** Apache Airflow 2.x (GCP Composer Environment)

---

## 1. Overview
This migration design document covers the transition of the standalone UC4 UNIX job `SALES.PRODUCT_AND_SALES_EXTRACT` to an Apache Airflow DAG. The original UC4 object acts as a source availability utility, executes a Product master SCD2 (Slowly Changing Dimension Type 2) load, and extracts daily sales data into a staging area. In the source environment, this job executes a Korn Shell script on a target host (`|ETLHOST3|HOST`). Since no workflow wrapper (JOBP) or schedule (JSCH/EVNT) was provided in this extraction, this DAG is configured as an externally triggered, single-task workflow pending downstream or upstream integration.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `SALES.PRODUCT_AND_SALES_EXTRACT` | JOBS_UNIX | Active (`active=1`) | Source availability check, product master SCD2 load, and daily sales extract into staging |

---

## 3. Scheduling
* **Schedule Policy:** No scheduling or calendar objects (`EVNT_TIME` or `JSCH`) are present in this extraction.
* **Trigger Mechanism:** Externally triggered (source unknown from this extraction alone). 
* **DAG Schedule Parameter:** `schedule=None` (manual or external trigger).

---

## 4. Airflow DAG Properties
The following DAG properties are mapped for the single job plan representation:

| Property | Value |
| :--- | :--- |
| **dag_id** | `sales_product_and_sales_extract` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1 mapped to False)* |
| **default_args** | `{ "owner": "UNIX.ETL_SVC", "retries": 1, "retry_delay": timedelta(minutes=5) }` |

---

## 5. Task Inventory
The UNIX job is mapped below. Per rules for `unrecognized` launcher types, it is initially mapped to an `EmptyOperator` placeholder task to prevent execution of unparsed native scripts on arbitrary nodes.

| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `sales_product_and_sales_extract_task` | `SALES.PRODUCT_AND_SALES_EXTRACT` | `EmptyOperator` | N/A | N/A | 1 | 5 mins | None | None | False | None | # REVIEW-STRUCT: launcher command [`#!/bin/ksh`] not recognised — confirm target operator/script manually. Original script ran: `. &HOME/sales/r_product_and_sales_extract.ksh` |

---

## 6. Task Dependency Map
Since this migration contains only one task, the dependency chain is trivial:

```python
sales_product_and_sales_extract_task
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` (UC4 lock objects) or resource limitations were defined in the extracted metadata. Concurrency is governed by standard DAG limits (`max_active_runs=1`).

---

## 8. Error Handling and Retry Strategy
* **Default Retries:** The task inherits `retries=1` and `retry_delay=timedelta(minutes=5)` from default DAG arguments.
* **Failures:** No postcondition actions, alerts, or notification triggers were extracted. Standard Airflow failure logging and UI alerts apply.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Dynamic Mapping |
| :--- | :--- | :--- |
| `&RUN_DATE` | `&$TODAY` | `{{ ds }}` (Airflow logical execution date in `YYYY-MM-DD` format) |
| `&HOME` | Environment variable | To be resolved via Airflow `Variable.get("env_home")` or system environment configurations. |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher Type:** The source job executes a Korn Shell script (`. &HOME/sales/r_product_and_sales_extract.ksh`). Since raw shell execution is not natively supported on modern serverless Cloud Composer worker architectures without specific runner infrastructure, the execution task is stubbed with an `EmptyOperator`. 
  * *Recommendation:* Convert this shell script to run inside a Docker container via the `KubernetesPodOperator`, or migrate the execution target to a virtual machine accessible via the `SSHOperator`.
* **Parameter Injection:** The dynamic UC4 variable `&RUN_DATE` (holding the current execution date) must be explicitly passed into the migrated task execution arguments using Airflow's native Jinja template `{{ ds }}`.

---

## Pseudocode Outline

```python
# ==============================================================================
# ── Imports ──────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── GCP Configuration ────────────────────────────────────────────────────────
# ==============================================================================
# No direct GCP resources configured yet.
# Standard Environment variables or Cloud Composer variables may apply.

# ==============================================================================
# ── Default Args ─────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "UNIX.ETL_SVC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── on_failure_callback stubs ────────────────────────────────────────────────
# ==============================================================================
# No global failure callback actions defined in the source UC4 metadata.

# ==============================================================================
# ── DAG Definition ───────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="sales_product_and_sales_extract",
    default_args=DEFAULT_ARGS,
    description="Source availability check, product master SCD2 load, and daily sales extract into staging",
    schedule_interval=None, # Externally triggered, no schedule found in UC4 extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["sales", "migration_uc4"],
) as dag:

    # ==========================================================================
    # ── Guard Task ────────────────────────────────────────────────────────────
    # ==========================================================================
    # None required (No Else=Skip or self-lock metadata parsed)

    # ==========================================================================
    # ── Sensor Task ───────────────────────────────────────────────────────────
    # ==========================================================================
    # None required (No earliest_start_time constraint parsed)

    # ==========================================================================
    # ── Calendar Check Task ───────────────────────────────────────────────────
    # ==========================================================================
    # None required (No calendar constraints parsed)

    # ==========================================================================
    # ── Task: sales_product_and_sales_extract_task ────────────────────────────
    # ==========================================================================
    # # REVIEW-STRUCT: Original launcher command is unrecognized (#!/bin/ksh)
    # The source script at `. &HOME/sales/r_product_and_sales_extract.ksh`
    # must be migrated. Below is an EmptyOperator stub to act as a placeholder.
    # When configuring target execution, pass the run date parameter:
    # run_date = "{{ ds }}"
    
    sales_product_and_sales_extract_task = EmptyOperator(
        task_id="sales_product_and_sales_extract_task",
        doc_md="""
        ### UC4 Source Migration Note
        * **Original Name:** SALES.PRODUCT_AND_SALES_EXTRACT
        * **Login:** UNIX.ETL_SVC
        * **Host:** |ETLHOST3|HOST
        * **Original Script Body:**
          ```bash
          #!/bin/ksh
          # SALES.PRODUCT_AND_SALES_EXTRACT
          :SET &RUN_DATE='&$TODAY'
          . &HOME/sales/r_product_and_sales_extract.ksh
          ```
        """,
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-task workflow: No dependencies to register.
    sales_product_and_sales_extract_task
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sales/SALES.PRODUCT_AND_SALES_EXTRACT.xml` | `sales/SALES.PRODUCT_AND_SALES_EXTRACT.py` | Converts the UC4 XML job wrapper into an Airflow DAG file, retaining the folder structure and execution orchestrations. |

# Job dependencies
* **Downstream**: 
  * `SALES.DAILY_SCHEDULE` (not yet migrated) — In the target Cloud Composer environment, this will be wired as a downstream consumer. Because this downstream job is not yet migrated, the final connection or Airflow TriggerDagRunOperator cannot be finalized until it exists.

# Execution order
The execution order of the overall legacy workflow is preserved as follows:
1. `sales/SALES.PRODUCT_AND_SALES_EXTRACT.py` (representing the entry orchestrator).
2. `sales/d_daily_sales_extract.sql` (execution inside the invoked script).
3. `sales/d_product_master_load.sql` (execution inside the invoked script).
4. `sales/r_product_and_sales_extract.ksh` (invoked shell script).
5. `sales/k_product_and_sales_extract.ksh` (underlying shell environment/utilities).

Since this design pass owns only the XML job wrapper, the target DAG acts as the entry point that initiates the execution chain by invoking the migrated execution script target.

# Schedule & variables
* **Schedule Policy**: This job is NOT directly triggered by any scheduler. It operates as a callable/importable unit within other scheduled runs. In Airflow, this DAG is configured with `schedule=None` (manual or external trigger only).
* **Scheduler-Set Variables**:
  * `RUN_DATE`: Sourced from the legacy variable `&$TODAY`. In Airflow, this maps to the dynamic logical execution date parameter `{{ ds }}`.

# Lineage
* **Downstream Consumers**: 
  * `FILE:sales/r_product_and_sales_extract.ksh` — This wrapper script is invoked directly by the UC4 XML job.

# External system replacements
* **Execution Host `ETLHOST3`**: The legacy shell script executes on physical host `ETLHOST3`. In the GCP target architecture, execution moves to a Google Cloud Composer environment (GKE Pod executor or Cloud Compute instance). The shell script wrapper will be executed via `KubernetesPodOperator` or `SSHOperator`.

# Cross-file dependencies
* **Invoked Scripts**: The UC4 XML job script relies on the script `&HOME/sales/r_product_and_sales_extract.ksh` to run. The target Airflow DAG task must call the migrated Python/Bash runner equivalent of `r_product_and_sales_extract.ksh`.

# Target file plan
* **`sales/SALES.PRODUCT_AND_SALES_EXTRACT.py`**:
  * **Language**: Python (Airflow DAG)
  * **Source File**: `sales/SALES.PRODUCT_AND_SALES_EXTRACT.xml`

# Environment-specific values
* **`&HOME`**:
  * **Classification**: GLOBAL
  * **Sourced as**: Sourced at runtime using `os.environ.get("ENV_HOME")` or Airflow Variable `Variable.get("env_home")` to locate the base repo directory on the Airflow worker.
* **`UNIX.ETL_SVC`**:
  * **Classification**: GLOBAL
  * **Sourced as**: Configured inside Airflow's DAG `default_args` as the task owner, or referenced in SSH connection IDs for script execution.
* **`CLIENT_QUEUE`**:
  * **Classification**: GLOBAL
  * **Sourced as**: Mapped to the Celery executor queue or GKE namespace configuration within Composer.

# Risks and manual steps
* **Unmigrated Sibling Files**: Sibling files such as `sales/r_product_and_sales_extract.ksh` are not part of this design pass. Until they are fully migrated, the DAG task will execute an empty placeholder or stub operator.
* **Host Migration**: The execution of shell scripts on `ETLHOST3` must be moved to Google Cloud infrastructure. Containerizing the shell logic and utilizing the `KubernetesPodOperator` is recommended to prevent worker pollution on Cloud Composer.
* **Downstream Coordination**: The downstream consumer `SALES.DAILY_SCHEDULE` is not yet migrated, meaning cross-DAG dependencies or trigger sensors cannot be fully tested or wired in Composer.

---

=== FILE: sales/d_daily_sales_extract.sql ===
-- d_daily_sales_extract.sql
-- Extracts the day's sales transactions from the source POS staging area
-- into the warehouse staging table, deriving the region code from the
-- store dimension since the source feed only carries a store ID.
-- Schema: ANALYTICS_SCHEMA

DELETE FROM ANALYTICS_SCHEMA.STG_DAILY_SALES
WHERE  SALE_DATE = TO_DATE('&1', 'YYYY-MM-DD');

INSERT INTO ANALYTICS_SCHEMA.STG_DAILY_SALES
    (SALE_ID, SALE_DATE, PRODUCT_ID, CUSTOMER_ID, STORE_ID, REGION_CODE, SALE_AMOUNT)
SELECT
    p.SALE_ID,
    p.SALE_DATE,
    p.PRODUCT_ID,
    p.CUSTOMER_ID,
    p.STORE_ID,
    st.REGION_CODE,
    p.SALE_AMOUNT
FROM   ANALYTICS_SCHEMA.SRC_POS_TRANSACTIONS p
JOIN   ANALYTICS_SCHEMA.DIM_STORE st
  ON   st.STORE_ID = p.STORE_ID
WHERE  p.SALE_DATE = TO_DATE('&1', 'YYYY-MM-DD');

COMMIT;
EXIT;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - This is a multi-statement Oracle DML transaction script containing a DELETE and an INSERT operation, controlled by a SQL*Plus substitution parameter (`&1`) and transactional control statements (`COMMIT`, `EXIT`).

1.2 Summarize the business logic and purpose of the script:
    - The script executes a daily sales data extraction and load (ETL) pipeline.
    - First, it deletes existing records in the staging table (`STG_DAILY_SALES`) for a specified date parameter to ensure idempotency.
    - Second, it extracts matching transactions from the source table (`SRC_POS_TRANSACTIONS`), joins with the store dimension (`DIM_STORE`) to resolve the respective region code for each store, and inserts the consolidated output into the staging table.

1.3 List all entities referenced:
    - Tables & Aliases:
      * `ANALYTICS_SCHEMA.STG_DAILY_SALES` (Target staging table; no alias)
      * `ANALYTICS_SCHEMA.SRC_POS_TRANSACTIONS` (Source transactions table; aliased as `p`)
      * `ANALYTICS_SCHEMA.DIM_STORE` (Dimension store table; aliased as `st`)
    - Column Names and Inferred Data Types:
      * `SALE_ID` (Inferred: Oracle `NUMBER` / BigQuery `INT64`)
      * `SALE_DATE` (Inferred: Oracle `DATE` / BigQuery `DATE`)
      * `PRODUCT_ID` (Inferred: Oracle `NUMBER` / BigQuery `INT64`)
      * `CUSTOMER_ID` (Inferred: Oracle `NUMBER` / BigQuery `INT64`)
      * `STORE_ID` (Inferred: Oracle `NUMBER` / BigQuery `INT64`)
      * `REGION_CODE` (Inferred: Oracle `VARCHAR2` / BigQuery `STRING`)
      * `SALE_AMOUNT` (Inferred: Oracle `NUMBER` / BigQuery `NUMERIC`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (storing `SALE_DATE`) maps to BigQuery `DATE` since it lacks a time component in this daily partition structure.
    - Oracle `NUMBER` for identifiers maps to BigQuery `INT64`.
    - Oracle `NUMBER` for currency (`SALE_AMOUNT`) maps to BigQuery `NUMERIC` to guarantee exact fractional precision.
    - Oracle `VARCHAR2` maps to BigQuery `STRING`.

2.2 Implicit and Explicit Type Casting:
    - Oracle's `TO_DATE('&1', 'YYYY-MM-DD')` will be explicitly converted to BigQuery's `PARSE_DATE('%Y-%m-%d', <param>)` function.

2.3 NULL Handling and Conditional Functions:
    - N/A (None present in source code).

2.4 String Functions:
    - N/A (None present in source code).

2.5 Date and Timestamp Functions:
    - `TO_DATE('&1', 'YYYY-MM-DD')`: In BigQuery, this must use `PARSE_DATE('%Y-%m-%d', ...)` to safely parse a string parameter into a structural `DATE` type. This satisfies the semantic validation rule by enforcing exact day precision matching without time zones.

2.6 - 2.10:
    - N/A (None present in source code).

2.11 MERGE Statements:
    - N/A (Using DELETE + INSERT pattern).

2.12 INSERT / UPDATE / DELETE:
    - The DML workflow consists of sequential DELETE and INSERT operations. To preserve transactional integrity and prevent partial-failure states, these statements will be wrapped within a BigQuery Scripting transaction block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`).

2.13 DDL Constructs:
    - N/A (DML only).

2.14 PL/SQL Scripting:
    - The SQL*Plus variable substitution (`&1`) is converted to a declared scripting variable inside a BigQuery block (`DECLARE target_date DATE;`).

2.15 Unresolvable or Advisory Items:
    - SQL*Plus commands (`EXIT`) are native to client execution environments and cannot be processed directly by BigQuery. They are stripped; session termination is managed by the execution runner.
    - The substitution variable `&1` must be passed as an external query parameter (`@1` or `@input_date`).

Step 3: Conversion Strategy Summary
3.1 Overall conversion approach:
    - Implement a BigQuery SQL Scripting block with `DECLARE`, enclosing the sequential DML operations inside an explicit `BEGIN TRANSACTION ... COMMIT TRANSACTION` block to ensure transactional atomicity identical to Oracle.

3.2 Assumptions:
    - The environment executing this script will supply the parameter `&1` as a query parameter named `@input_date` of type `STRING` (format: 'YYYY-MM-DD').
    - The target and source tables have already been migrated with proper column schemas mapping to their corresponding BigQuery native types.

3.3 Items flagged for human review:
    - Verification that the orchestration runner passes the runtime date parameter as a string named `@input_date`.

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence / Reason for Selection |
| :--- | :--- | :--- | :--- |
| **Substitution Variable `&1`** | Scripting variable `DECLARE target_date DATE` mapped to parameter `@input_date` | Direct inline injection (Hardcoding) | Inline injection exposes queries to syntax failures and injection risks. Declaration ensures safe variable scoping. |
| **TO_DATE('&1', 'YYYY-MM-DD')** | `PARSE_DATE('%Y-%m-%d', target_date_str)` | `CAST(target_date_str AS DATE)` | `PARSE_DATE` ensures strict format validation against the explicit format string, preventing runtime errors. |
| **DELETE / INSERT Flow** | Standard BigQuery DML wrapped in transaction block | BigQuery `MERGE` statement | While `MERGE` can combine these, a direct transition of `DELETE` + `INSERT` in a `TRANSACTION` preserves exact logic and index behaviors during staging. |
| **COMMIT / EXIT** | `COMMIT TRANSACTION;` | Omit transaction wrapper | Without transaction control, a failure during the `INSERT` would leave the target table empty or corrupt. `EXIT` is dropped as it is client-specific. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **Artifact Name**: `sales/d_daily_sales_extract.sql` (BigQuery SQL Script)
- **Execution Mechanism**: BigQuery SQL Scripting engine.
- **Inputs**: One Query Parameter `@input_date` (type `STRING`, format `YYYY-MM-DD`).
- **Outputs**: DML mutation counts (Rows Deleted, Rows Inserted).

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column / Construct Type | BigQuery Target Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- |
| `SALE_DATE` (Oracle DATE) | `DATE` | Native type conversion | Oracle DATE can store time; mapped to standard BigQuery DATE because the logic resolves exclusively to daily granularity. |
| `SALE_ID`, `PRODUCT_ID`, `CUSTOMER_ID`, `STORE_ID` (NUMBER) | `INT64` | Native type conversion | No loss of precision for integer keys. |
| `SALE_AMOUNT` (NUMBER) | `NUMERIC` | Precision alignment | Using `NUMERIC` instead of `FLOAT64` to prevent rounding issues during financial reporting calculations. |
| `REGION_CODE` (VARCHAR2) | `STRING` | Standard string representation | No padding implications. |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Pattern / Objects Found**: Standard transactional staging purge and reload ETL execution.
- **Unsupported Functions**: None (All constructs have clean native BigQuery equivalents).
- **UDFs Required**: No.
- **Python Code Required**: No.
- **Direct Dependencies**: Target `STG_DAILY_SALES`, sources `SRC_POS_TRANSACTIONS` and `DIM_STORE`.
- **Status**: Ready for human approval.

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATE('%Y-%m-%d', ...)` |
| `DELETE` | Direct | `DELETE FROM ...` |
| `INSERT` | Direct | `INSERT INTO ...` |
| `COMMIT` | Direct-with-rewrite | `COMMIT TRANSACTION` (inside transaction block) |
| `EXIT` | Unsupported | None (Handled implicitly by script completion) |


═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Declarations section to handle the input parameters
-- Parameter @input_date replaces Oracle substitution argument '&1'
DECLARE target_date_str STRING DEFAULT @input_date;
DECLARE target_date DATE;

-- Parse input string into BigQuery DATE object
SET target_date = PARSE_DATE('%Y-%m-%d', target_date_str);  -- converted from TO_DATE('&1', 'YYYY-MM-DD')

-- Wrap operations in a transaction block to preserve Oracle DML atomic execution
BEGIN
  BEGIN TRANSACTION;

  -- Delete existing transactions for the execution date to ensure idempotent run
  DELETE FROM ANALYTICS_SCHEMA.STG_DAILY_SALES
  WHERE SALE_DATE = target_date;

  -- Populate staging with source transaction joined to store dimension for region mappings
  INSERT INTO ANALYTICS_SCHEMA.STG_DAILY_SALES
      (SALE_ID, SALE_DATE, PRODUCT_ID, CUSTOMER_ID, STORE_ID, REGION_CODE, SALE_AMOUNT)
  SELECT
      p.SALE_ID,
      p.SALE_DATE,
      p.PRODUCT_ID,
      p.CUSTOMER_ID,
      p.STORE_ID,
      st.REGION_CODE,
      p.SALE_AMOUNT
  FROM ANALYTICS_SCHEMA.SRC_POS_TRANSACTIONS p
  JOIN ANALYTICS_SCHEMA.DIM_STORE st
    ON st.STORE_ID = p.STORE_ID
  WHERE p.SALE_DATE = target_date;  -- converted from TO_DATE('&1', 'YYYY-MM-DD')

  -- Commit changes safely
  COMMIT TRANSACTION;  -- converted from COMMIT;
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Orchestration Parameter Integration**: Ensure the orchestration system (such as Airflow, Cloud Composer, or dbt) is configured to pass the execution date as a query parameter named `@input_date` in `'YYYY-MM-DD'` string format to match the declared variable contract.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sales/d_daily_sales_extract.sql` | `sales/d_daily_sales_extract.sql` | Converted to a BigQuery SQL Scripting block utilizing explicit transaction control (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) to preserve transactional atomicity. |

---

### Job dependencies
* **Downstream Job:**
  * `SALES.DAILY_SCHEDULE` — *not yet migrated*. This downstream job must be wired via a cross-DAG dependency or task sensor in Cloud Composer once migrated. Since this target job does not yet exist, the final dependency trigger wiring cannot be completed at this stage.

---

### Execution order
The target orchestration (via Cloud Composer/Airflow) must preserve the execution sequence defined in the legacy dependency graph. The mapping of the legacy sequence to target execution steps is as follows:
1. `sales/SALES.PRODUCT_AND_SALES_EXTRACT.xml` (Legacy orchestration trigger — mapped to a Composer DAG sensor or manual trigger)
2. `sales/d_daily_sales_extract.sql` (Mapped to the execution of the BigQuery SQL script target file `sales/d_daily_sales_extract.sql`)
3. `sales/d_product_master_load.sql` (Legacy sibling execution step — out of scope for this pass)
4. `sales/r_product_and_sales_extract.ksh` (Legacy sibling wrapper script — out of scope for this pass)
5. `sales/k_product_and_sales_extract.ksh` (Legacy sibling wrapper script — out of scope for this pass)

---

### Schedule & variables
* **Schedule:** This job is not directly triggered by any scheduler; it executes within scheduled jobs as an include/shared module. Consequently, the migrated BigQuery SQL artifact must remain a callable/importable unit (e.g., a modular DAG task or task group) without an independent standalone schedule.
* **Scheduler-Set Variables:**
  * `RUN_DATE` = `'&$TODAY'` (sourced from parent job `SALES.PRODUCT_AND_SALES_EXTRACT`): This must be supplied to the BigQuery SQL task as a runtime query parameter `@input_date` in `'YYYY-MM-DD'` format using Airflow's native execution date context (e.g., `{{ ds }}`).

---

### Lineage
* **Upstream Table Inputs:**
  * `ANALYTICS_SCHEMA.SRC_POS_TRANSACTIONS` (Read to extract daily transactions)
  * `ANALYTICS_SCHEMA.DIM_STORE` (Read to resolve store-to-region mappings)
* **Downstream Table Outputs:**
  * `ANALYTICS_SCHEMA.STG_DAILY_SALES` (Subject to a targeting delete purge before the subsequent append insert of extracted daily sales records)

---

### Target file plan
* **Target File Path:** `sales/d_daily_sales_extract.sql`
  * **Language:** SQL (BigQuery SQL Script)
  * **Source File:** `sales/d_daily_sales_extract.sql`
  * **Purpose:** Performs the target-date staging table purge and subsequent join-extract-load steps in a transactional block to ensure idempotent loads.

---

### Environment-specific values
* `ANALYTICS_SCHEMA`: Classified as **GLOBAL** (environment-wide). This schema/dataset context maps to the canonical target variable `BQ_DATASET`. At deployment time, it should be dynamically substituted or resolved based on the active GCP project context (e.g., dev/test/prod).
* `RUN_DATE` (`@input_date`): Classified as **JOB-SPECIFIC**. This is a dynamic execution variable passed to the query context as an Airflow or calling-agent parameter during runtime.

---

### Risks and manual steps
* **Wiring Dependency:** The downstream consumer `SALES.DAILY_SCHEDULE` is marked *not yet migrated*. The downstream execution trigger or sensor hook cannot be finalized in Cloud Composer until its corresponding target is deployed.
* **Input Date Format Enforcement:** The BigQuery SQL script utilizes explicit parsing (`PARSE_DATE('%Y-%m-%d', ...)`). If the orchestration system passes the parameter in an unexpected format, the script will raise a runtime execution error. Airflow task parameters must enforce the `'YYYY-MM-DD'` string structure.

---

=== FILE: sales/d_product_master_load.sql ===
-- d_product_master_load.sql
-- SCD Type 2 merge of the product master dimension.
-- Schema: ANALYTICS_SCHEMA

MERGE INTO ANALYTICS_SCHEMA.DIM_PRODUCT tgt
USING ANALYTICS_SCHEMA.STG_PRODUCT_MASTER src
ON (tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt.IS_CURRENT = 1)
WHEN MATCHED AND (
         tgt.PRODUCT_NAME <> src.PRODUCT_NAME
      OR tgt.CATEGORY     <> src.CATEGORY
      OR tgt.UNIT_PRICE   <> src.UNIT_PRICE
     ) THEN
    UPDATE SET tgt.IS_CURRENT = 0,
               tgt.VALID_TO   = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM)
    VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, 1, SYSDATE);

INSERT INTO ANALYTICS_SCHEMA.DIM_PRODUCT
    (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM)
SELECT src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, 1, SYSDATE
FROM   ANALYTICS_SCHEMA.STG_PRODUCT_MASTER src
JOIN   ANALYTICS_SCHEMA.DIM_PRODUCT tgt
  ON   tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt.IS_CURRENT = 0 AND tgt.VALID_TO = SYSDATE;

COMMIT;
EXIT;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - Multi-statement SQL DML script containing a MERGE statement, an INSERT statement, and transaction control commands (COMMIT, EXIT).
1.2 Summarize the business logic and purpose of the script in plain English:
    - This script implements a Slowly Changing Dimension (SCD) Type 2 tracking mechanism for the `DIM_PRODUCT` dimension.
    - First, the MERGE statement looks at active product records (`IS_CURRENT = 1`). If the descriptive fields (name, category, or unit price) have changed in the staging table, it marks the existing target record as inactive (`IS_CURRENT = 0`) and sets its expiration date (`VALID_TO`) to the current run datetime. If a product ID is entirely new, it inserts a new active record.
    - Second, the INSERT statement identifies the staging records that just triggered an expiration in the MERGE step (by joining staging with the newly updated `VALID_TO = SYSDATE` target records) and inserts a new, active version of those products with the current run datetime as `VALID_FROM`.
1.3 List all entities referenced:
    - `ANALYTICS_SCHEMA.DIM_PRODUCT` (tgt)
        - `PRODUCT_ID` (Inferred: INT64 / NUMBER)
        - `PRODUCT_NAME` (Inferred: STRING / VARCHAR2)
        - `CATEGORY` (Inferred: STRING / VARCHAR2)
        - `UNIT_PRICE` (Inferred: NUMERIC / NUMBER)
        - `IS_CURRENT` (Inferred: INT64 / NUMBER)
        - `VALID_FROM` (Inferred: DATETIME / DATE)
        - `VALID_TO` (Inferred: DATETIME / DATE)
    - `ANALYTICS_SCHEMA.STG_PRODUCT_MASTER` (src)
        - `PRODUCT_ID`
        - `PRODUCT_NAME`
        - `CATEGORY`
        - `UNIT_PRICE`

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (includes time component) → `DATETIME` in BigQuery to preserve hours, minutes, and seconds.
    - Oracle `NUMBER` (for keys, flags) → `INT64`.
    - Oracle `NUMBER` (for prices) → `NUMERIC` to guarantee precise decimal representation.
    - Oracle `VARCHAR2` → `STRING`.

2.2 Implicit and Explicit Type Casting:
    - Comparison of `tgt.VALID_TO` and `SYSDATE` in the second INSERT is highly sensitive to the exact microsecond. To preserve transactional and logical consistency between the MERGE and INSERT statements, a session-scoped variable `current_datetime_val` of type `DATETIME` will be initialized to `CURRENT_DATETIME()` at the start of the block and utilized across both statements.

2.3 NULL Handling and Conditional Functions:
    - None detected.

2.4 String Functions:
    - None detected.

2.5 Date and Timestamp Functions:
    - `SYSDATE` → `CURRENT_DATETIME()` (captured in a script variable to guarantee consistency across the multi-statement run).

2.6 Numeric and Aggregate Functions:
    - None detected.

2.7 Analytical and Window Functions:
    - None detected.

2.8 Set and Join Operations:
    - Standard joins are utilized and are directly compatible with BigQuery.

2.9 Row Limiting and Sampling:
    - None detected.

2.10 Sequences:
    - None detected.

2.11 MERGE Statements:
    - The MERGE statement uses a standard `WHEN MATCHED AND ... THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT` pattern. This is fully supported by BigQuery, with the restriction that BigQuery requires a transactional block to ensure ACID execution alongside the subsequent `INSERT` statement.

2.12 INSERT / UPDATE / DELETE:
    - BigQuery supports standard multi-statement transactions (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`). This is used to ensure both the MERGE and follow-up INSERT execute as an atomic unit.

2.13 DDL Constructs:
    - None detected.

2.14 PL/SQL:
    - Oracle-style implicit script execution will be translated into a BigQuery Scripting block (`BEGIN ... END`) containing a `DECLARE` statement and transaction wrappers.

2.15 Unresolvable or Advisory Items:
    - `EXIT` command is a SQL*Plus CLI instruction and does not exist in BigQuery SQL; it will be stripped and managed by the orchestration tool (e.g., Airflow, dbt, or Google Cloud Composer).

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - Deconstruct the execution into a BigQuery SQL Scripting block wrapped in a single explicit transaction (`BEGIN TRANSACTION ... COMMIT TRANSACTION`).
    - Declare a script variable `current_datetime_val` to hold the start-of-run `CURRENT_DATETIME()` value, preventing potential time discrepancies between the MERGE update and the subsequent INSERT select logic.
3.2 List any assumptions made during conversion:
    - It is assumed that the BigQuery target dataset is named `ANALYTICS_SCHEMA`.
    - It is assumed that the pipeline orchestrator handles connection teardown, hence the Oracle-specific `EXIT;` is stripped.
3.3 List any items flagged for human review before the build stage proceeds:
    - Confirm whether transaction control limits in the target BigQuery environment permit active locks on `DIM_PRODUCT` during execution.

═══════════════════════════════════════════
MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason for Selection |
| :--- | :--- | :--- | :--- |
| `SYSDATE` temporal consistency | Scripting Variable + `CURRENT_DATETIME()` | 1. Inline `CURRENT_DATETIME()` <br> 2. Direct system timestamp calls | Using inline timestamp function calls across different statements inside a transaction may yield minor microsecond differences, resulting in the subsequent join failing to identify the newly expired rows. A scripting variable guarantees identical matching values. |
| Multi-statement orchestration | BigQuery SQL Transaction block (`BEGIN TRANSACTION`) | 1. Separate jobs <br> 2. Python-based state management | Standard BigQuery supports scripting transactions. Executing them together ensures that the two-step SCD Type 2 logic remains atomic and thread-safe. |
| SQL*Plus commands (`COMMIT`, `EXIT`) | Standard SQL Transaction control (`COMMIT TRANSACTION`) / Strip `EXIT` | 1. Emulate EXIT in UDF <br> 2. Python container exit | `COMMIT` maps natively to `COMMIT TRANSACTION;` inside scripting blocks. `EXIT` is a client tool command that should be handled by the execution runner. |

═══════════════════════════════════════════
REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **Artifact 1**: BigQuery Standard SQL Script file (.sql) containing a declared variable, transaction block (`BEGIN TRANSACTION` ... `COMMIT TRANSACTION`), MERGE statement, and INSERT statement.

═══════════════════════════════════════════
DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column/Type | Target BigQuery Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- |
| `PRODUCT_ID` (NUMBER) | `INT64` | Direct numeric mapping | Assumed integer identifier. If alpha-numeric, map to STRING. |
| `PRODUCT_NAME` (VARCHAR2) | `STRING` | Direct conversion | No length constraints are enforced in BigQuery. |
| `CATEGORY` (VARCHAR2) | `STRING` | Direct conversion | No length constraints are enforced in BigQuery. |
| `UNIT_PRICE` (NUMBER) | `NUMERIC` | Precision mapping | Use `NUMERIC` to avoid precision loss associated with floating-point calculations. |
| `IS_CURRENT` (NUMBER) | `INT64` | Binary/Integer flag mapping | Maps to `INT64` (values 0 and 1). |
| `VALID_FROM` (DATE) | `DATETIME` | Date/time resolution | Oracle `DATE` stores time. `DATETIME` is preferred over `DATE` to keep timestamps. |
| `VALID_TO` (DATE) | `DATETIME` | Date/time resolution | Oracle `DATE` stores time. `DATETIME` is preferred over `DATE` to keep timestamps. |

═══════════════════════════════════════════
DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: SCD Type 2 merge update pattern, follow-up delta insert, transaction commit, CLI session exit.
- **Unsupported Functions**: `EXIT` (stripped).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Table `ANALYTICS_SCHEMA.STG_PRODUCT_MASTER`, Table `ANALYTICS_SCHEMA.DIM_PRODUCT`.
- **Assumptions**: Dataset exists; transaction blocks are enabled in target BigQuery project; product identifiers fit in INT64.
- **Warnings**: Ensure that the target platform supports multi-statement transactions on partition-locked tables if clustering or partitioning is applied to `DIM_PRODUCT`.
- **Manual Intervention Items**: None.
- **Status**: Ready for human approval.

═══════════════════════════════════════════
ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `MERGE` | Direct-with-rewrite | `MERGE INTO ...` syntax is supported; wrap in transaction for SCD integrity. |
| `SYSDATE` | Direct-with-rewrite | `CURRENT_DATETIME()` captured via scripting variable. |
| `COMMIT` | Direct | `COMMIT TRANSACTION;` inside a transaction block. |
| `EXIT` | Unsupported | None — handled by orchestrator (stripped). |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- Declare runtime tracking variable to ensure absolute consistency across DML operations
DECLARE current_datetime_val DATETIME;
SET current_datetime_val = CURRENT_DATETIME(); -- converted from SYSDATE to capture start-of-run execution time

-- Begin atomic transaction block to enforce consistency for the SCD Type 2 load sequence
BEGIN TRANSACTION;

-- Step 1: Expire changed records in target, and insert entirely new records
MERGE INTO ANALYTICS_SCHEMA.DIM_PRODUCT tgt
USING ANALYTICS_SCHEMA.STG_PRODUCT_MASTER src
ON (tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt.IS_CURRENT = 1)
WHEN MATCHED AND (
         tgt.PRODUCT_NAME <> src.PRODUCT_NAME
      OR tgt.CATEGORY     <> src.CATEGORY
      OR tgt.UNIT_PRICE   <> src.UNIT_PRICE
     ) THEN
    UPDATE SET tgt.IS_CURRENT = 0,
               tgt.VALID_TO   = current_datetime_val -- converted from SYSDATE
WHEN NOT MATCHED THEN
    INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM)
    VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, 1, current_datetime_val); -- converted from SYSDATE

-- Step 2: Insert new active versions of records that were expired in the preceding MERGE step
INSERT INTO ANALYTICS_SCHEMA.DIM_PRODUCT
    (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM)
SELECT 
    src.PRODUCT_ID, 
    src.PRODUCT_NAME, 
    src.CATEGORY, 
    src.UNIT_PRICE, 
    1, 
    current_datetime_val -- converted from SYSDATE
FROM ANALYTICS_SCHEMA.STG_PRODUCT_MASTER src
INNER JOIN ANALYTICS_SCHEMA.DIM_PRODUCT tgt
  ON tgt.PRODUCT_ID = src.PRODUCT_ID 
  AND tgt.IS_CURRENT = 0 
  AND tgt.VALID_TO = current_datetime_val; -- joined on consistent variable to guarantee matching expired rows

-- Commit transaction block to apply all dimension changes atomically
COMMIT TRANSACTION; -- converted from COMMIT
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
- **CLI Commands**: The Oracle command `EXIT;` has been completely stripped. The scheduler running this BigQuery script should handle session termination.
- **Concurrent Transactions**: If multiple jobs attempt to update `ANALYTICS_SCHEMA.DIM_PRODUCT` concurrently, BigQuery may throw concurrency/transaction conflicts. Ensure that this master load runs in a serialized partition or is orchestrated as a single-threaded process.

# MIGRATION DESIGN DOCUMENT: SALES.PRODUCT_AND_SALES_EXTRACT

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sales/d_product_master_load.sql` | `sales/d_product_master_load.sql` | Converted to a BigQuery Standard SQL scripting block wrapping a transaction to execute SCD Type 2 logic atomically. |

---

## Job Dependencies
* **Downstream Jobs**:
  * `SALES.DAILY_SCHEDULE` — **Not yet migrated**. Because this downstream orchestration job is not yet available on the target platform, the end-to-end orchestration linkage (such as Airflow DAG triggers or dataset sensors) cannot be finalized.
  * *Impact/Action*: Added to Risks & Manual Actions. Once `SALES.DAILY_SCHEDULE` is migrated, its Airflow DAG or orchestration trigger must be wired to execute after the successful completion of this job's group.

---

## Execution Order
The legacy dependency graph lists 5 sequential steps. The target orchestration must preserve this overall sequence, although this design pass is solely responsible for migrating step 3:
1. `sales/SALES.PRODUCT_AND_SALES_EXTRACT.xml` (UC4 orchestration wrapper)
2. `sales/d_daily_sales_extract.sql` (Daily sales extract SQL)
3. **`sales/d_product_master_load.sql`** (SCD Type 2 load — **This Scope**)
4. `sales/r_product_and_sales_extract.ksh` (Thin wrapper script)
5. `sales/k_product_and_sales_extract.ksh` (Core KSH script)

* **Target Execution Mapping**:
  * In the target environment (e.g., Cloud Composer / Airflow), the task executing `sales/d_product_master_load.sql` must be positioned downstream of the task executing `sales/d_daily_sales_extract.sql` and upstream of the KSH-translated Python scripts.

---

## Scheduling
* **Trigger Type**: Inherited / Event-Triggered.
* **Target Scheduling**: This job does not run on its own standalone schedule; it runs as an included or shared module inside upstream/parent schedules (orchestrated by the UC4 container `SALES.PRODUCT_AND_SALES_EXTRACT`). On Google Cloud Composer, this component must remain a callable task/DAG within the master sales extraction DAG rather than having its own direct schedule.

---

## Schedule & Variables
* **Scheduler-Set Variables**:
  * `RUN_DATE = '&$TODAY'` (Inherited from the UC4 container `SALES.PRODUCT_AND_SALES_EXTRACT`).
* **Target Delivery Mechanism**: 
  * In Google Cloud Composer / Airflow, this variable should be passed dynamically at runtime to the BigQuery operator using Airflow's execution date context (e.g., `{{ ds }}`) and mapped to a query parameter or declared scripting variable `@run_date` inside the SQL execution block.

---

## Lineage
* **Upstream Producers (Inputs)**:
  * Table: `ANALYTICS_SCHEMA.STG_PRODUCT_MASTER` (Source staging table containing the daily incoming product data feed).
  * Table: `ANALYTICS_SCHEMA.DIM_PRODUCT` (Target dimension table used in historical self-joins for SCD2 matching).
* **Downstream Consumers (Outputs)**:
  * Table: `ANALYTICS_SCHEMA.DIM_PRODUCT` (SCD Type 2 destination; receives updates to mark old versions as inactive and inserts new active versions).

---

## Cross-File Dependencies
* **Shared Tables**: 
  * `ANALYTICS_SCHEMA.DIM_PRODUCT` and `ANALYTICS_SCHEMA.STG_PRODUCT_MASTER` are shared objects across the broader sales pipeline. Step 2 (`sales/d_daily_sales_extract.sql`) and step 4/5 (`sales/k_product_and_sales_extract.ksh`) may read from or rely on the updated product master dimension. Therefore, the task sequence must guarantee that this script completes execution before downstream extracts proceed.

---

## Target File Plan
* **Target File**: `sales/d_product_master_load.sql`
  * **Language**: BigQuery Standard SQL
  * **Source Component**: `sales/d_product_master_load.sql`
  * **Description**: A multi-statement scripting block starting with a variable declaration to ensure temporal consistency (`current_datetime_val`), wrapped in a transaction block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`). It executes the SCD Type 2 MERGE to expire changed records, followed by the INSERT of new active versions.

---

## Environment-Specific Values

### 1. GLOBAL (Environment-Wide Variables)
The infrastructure qualifiers must be resolved at runtime using environment variables or orchestrator configs instead of hardcoded strings:
* `ANALYTICS_SCHEMA` -> Maps to **`BQ_DATASET`** (or specific environment-wide dataset configuration).
  * *Access Method*: Handled via the Airflow BigQueryOperator's configuration or replaced at execution time by template variables (e.g., `{{ var.value.BQ_DATASET }}`).

### 2. JOB-SPECIFIC (Job-Local Variables)
* `RUN_DATE` -> Maps to **`run_date`** (the business date of processing).
  * *Access Method*: Delivered via Airflow task execution parameters using context templates (`{{ ds }}`) and supplied as a BigQuery query parameter.

---

## Risks and Manual Steps
1. **DOWNSTREAM NOT YET MIGRATED: `SALES.DAILY_SCHEDULE`**:
   * *Risk*: The downstream orchestration dependency cannot be fully validated or verified. 
   * *Mitigation*: The integration tests must mock the completion notification to the downstream scheduler. A manual validation step is required once `SALES.DAILY_SCHEDULE` is migrated to BigQuery/Composer.
2. **Concurrent Transaction/Lock Conflicts**:
   * *Risk*: If multiple jobs write to `DIM_PRODUCT` simultaneously, BigQuery's ACID transaction control will reject concurrent updates, resulting in transactional conflicts.
   * *Mitigation*: Ensure that the orchestrator serializes all pipelines loading into `DIM_PRODUCT` so that they do not run concurrently.
3. **Database Timing Discrepancies**:
   * *Risk*: High-frequency updates or joining on microsecond-exact timestamps could fail if timestamps diverge across separate statements.
   * *Mitigation*: The target design mitigates this by capturing `CURRENT_DATETIME()` in a single transaction-scoped script variable (`current_datetime_val`) utilized identically across the MERGE and the subsequent INSERT. Ensure this pattern is maintained in the final build.

---

=== FILE: sales/k_product_and_sales_extract.ksh ===
#!/bin/ksh
###############################################################################
# k_product_and_sales_extract.ksh
#
# Core logic for SALES.PRODUCT_AND_SALES_EXTRACT:
#   1. Source availability check - polls for the upstream POS feed's landing
#      marker, with a bounded wait/retry loop rather than failing on the
#      first miss (the feed occasionally lands a few minutes late).
#   2. Product master dimension, SCD Type 2 merge.
#   3. Daily sales transaction extract into staging.
###############################################################################

RETAIL_HOME=${RETAIL_HOME:-/opt/etl/sales}
RETAIL_ORA_USER=${RETAIL_ORA_USER:-retail_etl}
RETAIL_ORA_PASS=${RETAIL_ORA_PASS:-changeit}
RETAIL_ORA_SID=${RETAIL_ORA_SID:-RETAILPRD}
SOURCE_MARKER=${RETAIL_HOME}/inbound/pos_feed_${RUN_DATE}.done
MAX_WAIT_CHECKS=10
WAIT_INTERVAL_SECONDS=60

log() {
    print "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# --- Step 1: wait for the upstream POS feed's landing marker ---------------
check=1
while [ ${check} -le ${MAX_WAIT_CHECKS} ]; do
    if [ -f "${SOURCE_MARKER}" ]; then
        log "Source feed marker found on check ${check}/${MAX_WAIT_CHECKS}"
        break
    fi
    log "Source feed marker not yet present (check ${check}/${MAX_WAIT_CHECKS}) - waiting ${WAIT_INTERVAL_SECONDS}s"
    if [ ${check} -eq ${MAX_WAIT_CHECKS} ]; then
        log "ERROR: source feed marker never appeared after ${MAX_WAIT_CHECKS} checks - aborting"
        exit 1
    fi
    sleep ${WAIT_INTERVAL_SECONDS}
    check=$((check + 1))
done

# --- Step 2: product master dimension, SCD Type 2 ---------------------------
log "Loading product master dimension (SCD2 merge)"
sqlplus -s ${RETAIL_ORA_USER}/${RETAIL_ORA_PASS}@${RETAIL_ORA_SID} @${RETAIL_HOME}/sales/d_product_master_load.sql
if [ $? -ne 0 ]; then
    log "ERROR: product master dimension load failed"
    exit 2
fi

# --- Step 3: daily sales transaction extract --------------------------------
log "Extracting daily sales transactions for ${RUN_DATE}"
sqlplus -s ${RETAIL_ORA_USER}/${RETAIL_ORA_PASS}@${RETAIL_ORA_SID} @${RETAIL_HOME}/sales/d_daily_sales_extract.sql "${RUN_DATE}"
rc=$?

if [ ${rc} -ne 0 ]; then
    log "ERROR: daily sales extract failed with rc=${rc}"
    exit 3
fi

log "Product master refreshed and daily sales extracted for ${RUN_DATE}"
exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains file-system polling and sleep loops checking for a local marker file, which cannot be modeled in BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic contains a wait/retry loop (up to 10 checks, sleeping 60 seconds) polling the file system for an upstream POS feed marker file, followed by two `sqlplus` database calls.
- AWK: none
- SQL-expressible: No, the file existence checks (`[ -f ...]`) and time-based sleep loop cannot be expressed as SQL statements.
- Non-SQL side effects: Polling the local filesystem path `/opt/etl/sales/inbound/pos_feed_${RUN_DATE}.done` and sleeping between checks.
- Against this verdict: If the file-polling/triggering logic were offloaded completely to the UC4 scheduler, the database loads themselves could run as SQL, but the script's core orchestration contains this logic.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The `k_product_and_sales_extract.ksh` script orchestrates the loading of a product master dimension and the extraction of daily sales transactions. Before running any database steps, it checks for the landing of an upstream POS feed marker file (`pos_feed_${RUN_DATE}.done`) using a polling loop with 10 checks and a 60-second sleep interval. Once the file is present, it executes two Oracle SQL scripts via SQL*Plus: one to perform an SCD Type 2 merge for the product master, and another to extract daily transactions for a specific execution date.

2. INVOCATION CONTEXT
   - Who calls this script: Unknown UC4 job/JOBS_UNIX object. It expects the environment variable `RUN_DATE` to be set externally before invocation.
   - Any UC4 native includes: None referenced in this script's extraction.
   - Environment files sourced: None.

3. PARAMETERS / INPUTS
   - `RUN_DATE`: Environment variable. Used to construct the source file marker name and passed as a command-line parameter to the daily sales extraction script. (Used: Yes. Surface in Python: `os.environ.get("RUN_DATE")`).
   - `RETAIL_HOME`: Environment variable (defaults to `/opt/etl/sales`). Used to determine the path for SQL scripts and local inbound landing folders. (Used: Yes. Surface in Python: `os.environ.get("RETAIL_HOME", "/opt/etl/sales")`).
   - `RETAIL_ORA_USER`: Environment variable (defaults to `retail_etl`). Database username. (Used: Yes. Surface in Python: `os.environ.get("RETAIL_ORA_USER", "retail_etl")`).
   - `RETAIL_ORA_PASS`: Environment variable (defaults to `changeit`). Database password. (Used: Yes. Surface in Python: `os.environ.get("RETAIL_ORA_PASS", "changeit")`).
   - `RETAIL_ORA_SID`: Environment variable (defaults to `RETAILPRD`). Database connection identifier. (Used: Yes. Surface in Python: `os.environ.get("RETAIL_ORA_SID", "RETAILPRD")`).
   - `MAX_WAIT_CHECKS`: Local shell variable default / configurable. Max polling limit (defaults to `10`). (Used: Yes. Surface in Python: `int(os.environ.get("MAX_WAIT_CHECKS", 10))`).
   - `WAIT_INTERVAL_SECONDS`: Local shell variable default / configurable. Polling interval (defaults to `60`). (Used: Yes. Surface in Python: `int(os.environ.get("WAIT_INTERVAL_SECONDS", 60))`).

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -s ${RETAIL_ORA_USER}/${RETAIL_ORA_PASS}@${RETAIL_ORA_SID} @${RETAIL_HOME}/sales/d_product_master_load.sql`
     - Purpose: Launches SQL*Plus to run the product master SCD Type 2 dimension merge SQL script.
     - Target: External subprocess invocation.
     - Resolvable Launcher: No.
     # REVIEW-STRUCT: launcher [sqlplus] invoking [d_product_master_load.sql] — the SQL file body was not supplied in this extraction; internal SQL statement analysis and native client implementation are unconfirmed.
   
   - `sqlplus -s ${RETAIL_ORA_USER}/${RETAIL_ORA_PASS}@${RETAIL_ORA_SID} @${RETAIL_HOME}/sales/d_daily_sales_extract.sql "${RUN_DATE}"`
     - Purpose: Launches SQL*Plus to run the daily transaction extract SQL script for the specified run date.
     - Target: External subprocess invocation.
     - Resolvable Launcher: No.
     # REVIEW-STRUCT: launcher [sqlplus] invoking [d_daily_sales_extract.sql] — the SQL file body was not supplied in this extraction; internal SQL statement analysis and native client implementation are unconfirmed.

5. EMBEDDED SQL
   - Source Files Referenced:
     - `${RETAIL_HOME}/sales/d_product_master_load.sql` (File body not supplied)
     - `${RETAIL_HOME}/sales/d_daily_sales_extract.sql` (File body not supplied)
   - Statement types / Tables touched: Unknown due to unsupplied bodies.
   - Dialect Identification: The connection string syntax (`user/pass@SID`) and invocation command `sqlplus -s` confirm an Oracle target environment.
     # REVIEW: target database platform not explicitly confirmed in instructions; Oracle is provisionally assumed based on sqlplus client usage.

6. CONTROL FLOW
   1. **Environment Setup**: Read and assign default values to `RETAIL_HOME`, `RETAIL_ORA_USER`, `RETAIL_ORA_PASS`, `RETAIL_ORA_SID`, `MAX_WAIT_CHECKS`, and `WAIT_INTERVAL_SECONDS`. Validate that `RUN_DATE` is available.
   2. **Landing File Verification (Polling Loop)**:
      - Construct the marker path using `RUN_DATE`.
      - Loop `check` from 1 to `MAX_WAIT_CHECKS`.
      - If `SOURCE_MARKER` file exists, break out of loop.
      - If missing, print wait log message.
      - If on the last check and file is still missing, print abort message and exit with status 1.
      - Sleep for `WAIT_INTERVAL_SECONDS` before next check.
   3. **Product Master Load Execution**: Execute `sqlplus` pointing to `d_product_master_load.sql`.
      - Check execution status. If exit code is non-zero, print error log and exit with status 2.
   4. **Daily Sales Extract Execution**: Execute `sqlplus` pointing to `d_daily_sales_extract.sql` with `RUN_DATE` as argument.
      - Check execution status. If exit code is non-zero, print error log and exit with status 3.
   5. **Clean Exit**: Print successful completion log and exit with status 0.

7. ERROR HANDLING & EXIT CODES
   - Polling timeout (file not found): Exits with code `1`.
   - Failure in Product Master load (`d_product_master_load.sql` execution failed): Exits with code `2`.
   - Failure in Daily Sales extraction (`d_daily_sales_extract.sql` execution failed): Exits with code `3`.
   - Map to Python: Enclose subprocess calls in try/except blocks catching `subprocess.CalledProcessError`. Use `sys.exit` to propagate identical exit codes for backward-compatibility with downstream schedulers.

8. OUTPUTS / SIDE EFFECTS
   - Log outputs printed to standard output / error.
   - Database mutations on the Oracle DB (Product Master dimension tables modified, and temporary/extract tables updated).

9. BUSINESS SUMMARY
   - Verifies the availability of incoming point-of-sale (POS) raw data before starting the ETL pipeline.
   - Refreshes the Product Master dimension using SCD Type 2 logic, ensuring historically accurate product properties are maintained.
   - Extracts daily sales transaction records corresponding to the `RUN_DATE` for reporting and downstream system usage.

=== PSEUDOCODE ===

```python
# Step 1: Initialization and Environment Setup
import os
import sys
import time
import subprocess
from datetime import datetime

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

retail_home = os.environ.get("RETAIL_HOME", "/opt/etl/sales")
retail_ora_user = os.environ.get("RETAIL_ORA_USER", "retail_etl")
retail_ora_pass = os.environ.get("RETAIL_ORA_PASS", "changeit")
retail_ora_sid = os.environ.get("RETAIL_ORA_SID", "RETAILPRD")
run_date = os.environ.get("RUN_DATE")

if not run_date:
    log("ERROR: RUN_DATE environment variable is not defined.")
    sys.exit(1)

source_marker = os.path.join(retail_home, "inbound", f"pos_feed_{run_date}.done")
max_wait_checks = int(os.environ.get("MAX_WAIT_CHECKS", 10))
wait_interval_seconds = int(os.environ.get("WAIT_INTERVAL_SECONDS", 60))

# Step 2: Poll for upstream POS feed landing marker
check = 1
while check <= max_wait_checks:
    if os.path.isfile(source_marker):
        log(f"Source feed marker found on check {check}/{max_wait_checks}")
        break
    
    log(f"Source feed marker not yet present (check {check}/{max_wait_checks}) - waiting {wait_interval_seconds}s")
    
    if check == max_wait_checks:
        log(f"ERROR: source feed marker never appeared after {max_wait_checks} checks - aborting")
        sys.exit(1)
        
    time.sleep(wait_interval_seconds)
    check += 1

# Step 3: Execute product master dimension load (SCD Type 2 Merge)
log("Loading product master dimension (SCD2 merge)")
db_connection_string = f"{retail_ora_user}/{retail_ora_pass}@{retail_ora_sid}"
sql_script_product = os.path.join(retail_home, "sales", "d_product_master_load.sql")

# REVIEW-STRUCT: SQL file [d_product_master_load.sql] body not supplied — behaviour unknown
try:
    cmd_product = ["sqlplus", "-s", db_connection_string, f"@{sql_script_product}"]
    subprocess.run(cmd_product, check=True)
except subprocess.CalledProcessError as e:
    log("ERROR: product master dimension load failed")
    sys.exit(2)

# Step 4: Execute daily sales transaction extract
log(f"Extracting daily sales transactions for {run_date}")
sql_script_sales = os.path.join(retail_home, "sales", "d_daily_sales_extract.sql")

# REVIEW-STRUCT: SQL file [d_daily_sales_extract.sql] body not supplied — behaviour unknown
try:
    cmd_sales = ["sqlplus", "-s", db_connection_string, f"@{sql_script_sales}", run_date]
    subprocess.run(cmd_sales, check=True)
except subprocess.CalledProcessError as e:
    log(f"ERROR: daily sales extract failed with rc={e.returncode}")
    sys.exit(3)

# Step 5: Successful exit
log(f"Product master refreshed and daily sales extracted for {run_date}")
sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sales/k_product_and_sales_extract.ksh` | `sales/k_product_and_sales_extract.py` | Converts KornShell orchestration logic (GCS sentinel file polling and sequential BigQuery SQL executions) into native Python. |

---

### Job Dependencies
- **Downstream Dependency**: 
  - `SALES.DAILY_SCHEDULE` (not yet migrated)
- **Target Platform Wiring**: 
  - Since `SALES.DAILY_SCHEDULE` is not yet migrated, the orchestration hand-off must be managed via Cloud Composer (Airflow). Once `SALES.DAILY_SCHEDULE` is migrated, its DAG can be configured with an `ExternalTaskSensor` pointing to this job's DAG, or this job can trigger it directly using a `TriggerDagRunOperator`.

---

### Execution Order
The original dependency graph dictates a 5-step execution sequence. On Cloud Composer / BigQuery, these are mapped as follows:
1. **`sales/SALES.PRODUCT_AND_SALES_EXTRACT.xml`**: Legacy UC4 definition mapping to the overall Cloud Composer Airflow DAG structure.
2. **`sales/k_product_and_sales_extract.ksh`**: Migrates to `sales/k_product_and_sales_extract.py` (this target python script), which acts as the core Python task orchestrating steps 3 and 4.
3. **`sales/d_product_master_load.sql`** (Step 2 in SQL execution): Executed as the first database task within `k_product_and_sales_extract.py` using BigQuery client API query calls.
4. **`sales/d_daily_sales_extract.sql`** (Step 3 in SQL execution): Executed as the second database task within `k_product_and_sales_extract.py` (receiving `RUN_DATE` parameter) using BigQuery client API query calls.
5. **`sales/r_product_and_sales_extract.ksh`**: Legacy script wrapper. Decommissioned since its operational variables and environment setups are natively handled by the Cloud Composer environment and Airflow DAG configuration.

---

### Scheduling
- **Triggering Mechanism**: This job is NOT directly triggered by any standalone scheduler; it is designed to execute as an included or shared module inside other scheduled processes. 
- **Target Platform Mapping**: The migrated Python module / DAG task must remain a callable/importable unit (e.g., an independent sub-DAG or a task group) and should not be given its own standalone Airflow cron schedule.

---

### Schedule & Variables — Must Be Retained
- **Scheduler-Set Variables**:
  - `RUN_DATE` (Source value: `'&$TODAY'`)
- **Target Environment Resolution**:
  - This variable must map to the Airflow logical execution date. It will be passed dynamically at runtime using Airflow macro templates (e.g., `{{ ds }}`) into the Python execution task or as an environment variable (`RUN_DATE`) inside the Airflow operator context.

---

### Lineage
- **Upstream Inputs**:
  - Polled file dependency on `/opt/etl/sales/inbound/pos_feed_${RUN_DATE}.done`. This is replaced with a Cloud Storage object sensor looking for `gs://[GCS_BUCKET]/inbound/pos_feed_{run_date}.done`.
- **Downstream Consumers**:
  - Executes `sales/d_product_master_load.sql` (Lineage: `SCRIPT:D_PRODUCT_MASTER_LOAD.SQL`).
  - Executes `sales/d_daily_sales_extract.sql` (Lineage: `SCRIPT:D_DAILY_SALES_EXTRACT.SQL`).

---

### External System Replacements
- **File System Polling**: The legacy `-f` file existence check on a local UNIX path `/opt/etl/sales/...` is replaced by GCS client library checks using `google.cloud.storage.Client` to poll for the object `inbound/pos_feed_{run_date}.done`.
- **Database Client Replacement**: The legacy Oracle interactive command line `sqlplus -s` is replaced with the Google Cloud BigQuery Python client `google.cloud.bigquery.Client` to run target-platform SQL.

---

### Cross-File Dependencies
- **Shared Scripts**:
  - The Python orchestrator `sales/k_product_and_sales_extract.py` directly depends on the existence of SQL scripts `sales/d_product_master_load.sql` and `sales/d_daily_sales_extract.sql`. Their BigQuery-compatible translations must be deployed to the location expected by the Python script (or resolved within Dataform/BigQuery datasets depending on the SQL migration pass).

---

### Target File Plan
- **Target File**: `sales/k_product_and_sales_extract.py`
  - **Language**: Python
  - **Source File**: `sales/k_product_and_sales_extract.ksh`
  - **Implementation Strategy**: A Python script containing environment loading, dynamic GCS file polling logic with sleep intervals, and BigQuery execution blocks for the product master and daily sales SQL logic.

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
- **`GCP_PROJECT`**: Identifies the target Google Cloud Project. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow variables.
- **`GCS_BUCKET`**: Replaces the base path of `RETAIL_HOME` for the inbound landing area. Sourced at runtime via `os.environ.get("GCS_BUCKET")` or Airflow variables.
- **`BQ_DATASET`**: Replaces the schema/connection identifier `RETAIL_ORA_USER` and `RETAIL_ORA_SID`. Sourced at runtime via `os.environ.get("BQ_DATASET")`.

#### 2. JOB-SPECIFIC
- **`RUN_DATE`**: Dynamic execution date, passed to the script via command line arguments or environment variable set by the Airflow DAG context.
- **`MAX_WAIT_CHECKS`**: Polling check limit (default: `10`). Maintained as a job-specific parameter.
- **`WAIT_INTERVAL_SECONDS`**: Polling sleep interval in seconds (default: `60`). Maintained as a job-specific parameter.
- **`SOURCE_MARKER_PATH`**: Configured as `inbound/pos_feed_{run_date}.done` relative to the environment's `GCS_BUCKET`.

---

### Risks & Manual Steps
- **Unresolved SQL Sources**: The SQL script contents for `sales/d_product_master_load.sql` and `sales/d_daily_sales_extract.sql` are not included in this design pass (they belong to separate design groups). The Python script will fail to trigger them unless those SQL files are converted to BigQuery SQL/Dataform and made accessible.
- **Upstream Integration**: The local sentinel file trigger must be replaced by a GCS bucket upload event. A manual verification is required to confirm that the upstream POS data-landing process writes directly to the GCS bucket instead of the legacy `/opt/etl/sales/inbound` filesystem.
- **Legacy Database Migration**: This design assumes target data tables are already modeled and migrated on BigQuery. If the tables are not yet set up, the BigQuery API executions inside the converted script will fail.
- **Unmigrated Downstream Process**: Downstream job `SALES.DAILY_SCHEDULE` is marked "not yet migrated". Manual intervention or an interim integration layer will be needed to trigger legacy processes until the downstream system has been fully migrated.

---

=== FILE: sales/r_product_and_sales_extract.ksh ===
#!/bin/ksh
###############################################################################
# r_product_and_sales_extract.ksh
#
# Invoked by SALES.PRODUCT_AND_SALES_EXTRACT. Thin wrapper: environment setup
# and logging live here, all business logic lives in the core script.
###############################################################################
set -e

RETAIL_HOME=${RETAIL_HOME:-/opt/etl/sales}
LOG_DIR=${RETAIL_HOME}/logs
LOG_FILE=${LOG_DIR}/product_and_sales_extract_$(date +%Y%m%d_%H%M%S).log

mkdir -p "${LOG_DIR}"

log() {
    print "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

if [ -z "${RUN_DATE}" ]; then
    log "ERROR: RUN_DATE is not set - aborting"
    exit 1
fi

log "Starting product-and-sales extract for run date ${RUN_DATE}"
. ${RETAIL_HOME}/sales/k_product_and_sales_extract.ksh
rc=$?

if [ ${rc} -ne 0 ]; then
    log "ERROR: k_product_and_sales_extract.ksh failed with exit code ${rc}"
    exit ${rc}
fi

log "Product-and-sales extract completed successfully for ${RUN_DATE}"
exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script defines custom logging, validates the RUN_DATE parameter, creates log directories, and invokes an external shell script, preventing a direct SQL-only translation.

EVIDENCE
- Business logic found: KSH custom logic performs log directory creation, validates that RUN_DATE is set, and sources an external script `k_product_and_sales_extract.ksh`.
- AWK: none
- SQL-expressible: no, the script performs file-system logging and executes a secondary shell script which are non-SQL side effects.
- Non-SQL side effects: directory creation (`mkdir -p`), logging to a local file, sourcing another shell script.
- Against this verdict: none, since sourcing another script and local file logging cannot be converted directly into BigQuery SQL.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `r_product_and_sales_extract.ksh` is a wrapper script that initiates the product and sales extraction process. It verifies that the environment variable `RUN_DATE` is set, sets up a local logging directory and log file, and then sources the core execution script `k_product_and_sales_extract.ksh` under the sales directory. It is used to ensure the logging environment and prerequisites are met before delegating the business logic to the core extraction script.

### 2. INVOCATION CONTEXT
- **Called by**: UC4 job `SALES.PRODUCT_AND_SALES_EXTRACT` (implied by the header comment "Invoked by SALES.PRODUCT_AND_SALES_EXTRACT").
- **UC4 Includes**: None referenced in the extraction.
- **Environment Files / Scripts Sourced**:
  - `. ${RETAIL_HOME}/sales/k_product_and_sales_extract.ksh` — # REVIEW-STRUCT: environment file [k_product_and_sales_extract.ksh] not supplied — variables it sets are unknown; do not guess their names or values.

### 3. PARAMETERS / INPUTS
- **RUN_DATE**
  - **Source**: Environment variable (usually passed from the calling UC4 job or environment).
  - **Used in script body**: Yes, validated to ensure it is not empty, and logged.
  - **Python representation**: `os.environ.get("RUN_DATE")` or mapped via command-line arguments if preferred.
- **RETAIL_HOME**
  - **Source**: Environment variable with a fallback default value of `/opt/etl/sales`.
  - **Used in script body**: Yes, used to define log directories, log file names, and the path to the sourced script.
  - **Python representation**: `os.environ.get("RETAIL_HOME", "/opt/etl/sales")`.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
- **Sourced script**: `. ${RETAIL_HOME}/sales/k_product_and_sales_extract.ksh`
  - **Exact command line**: `. ${RETAIL_HOME}/sales/k_product_and_sales_extract.ksh`
  - **Purpose**: Executes the core product and sales extraction logic.
  - **Python mapping**: Since the content of `k_product_and_sales_extract.ksh` is not supplied, it must remain an external process invocation via `subprocess` (or be converted separately when its source is available).
  - **Resolvable launcher**: No, this is not a database/SQL launcher, it is a shell script. It is an opaque external script.
  - **Marker**: # REVIEW-STRUCT: launcher [k_product_and_sales_extract.ksh] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

### 5. EMBEDDED SQL
- None found in this script.

### 6. CONTROL FLOW
1. **Shell Environment Setup**: Sets exit-on-error (`set -e`).
2. **Variable Initialization**: Initializes `RETAIL_HOME` (defaulting to `/opt/etl/sales` if not set), `LOG_DIR` as `${RETAIL_HOME}/logs`, and `LOG_FILE` with a timestamp pattern `product_and_sales_extract_$(date +%Y%m%d_%H%M%S).log`.
3. **Directory Creation**: Creates the log directory using `mkdir -p "${LOG_DIR}"`.
4. **Define Logging Function**: Defines a local helper function `log` that outputs the current timestamp along with messages, writing to both stdout and appending to the `LOG_FILE`.
5. **Parameter Validation**: Checks if `RUN_DATE` is set and non-empty. If empty, logs an error message and terminates with exit code 1.
6. **Trigger Core Script**: Logs the start of execution and sources `${RETAIL_HOME}/sales/k_product_and_sales_extract.ksh`.
7. **Capture Sourced Return Code**: Captures the return code of the sourced script with `rc=$?`.
8. **Error Check**: Checks if `rc` is non-zero. If so, logs an error message and exits with the returned exit code.
9. **Success Exit**: If successful, logs a success message and exits with status 0.

### 7. ERROR HANDLING & EXIT CODES
- The script uses `set -e` to exit immediately if any simple command fails, although it explicitly captures the return code `rc` after sourcing `k_product_and_sales_extract.ksh`.
- If `RUN_DATE` is empty, it exits with code 1.
- If the sourced script `k_product_and_sales_extract.ksh` returns a non-zero exit code, it logs the failure and exits with that same non-zero code.
- SUCCESS convention is exit code 0.
- Python translation: Set up explicit error handling. The Python equivalent of `set -e` is using `check=True` on `subprocess.run` or catching `subprocess.CalledProcessError`. For the missing environment variable, raise a `ValueError` or call `sys.exit(1)`.

### 8. OUTPUTS / SIDE EFFECTS
- Log directory `${RETAIL_HOME}/logs` is created.
- Log file `${LOG_DIR}/product_and_sales_extract_<YYYYMMDD_HHMMSS>.log` is written to with execution statements.
- Direct output and side-effects of the sourced script `k_product_and_sales_extract.ksh` (unknown due to lack of source code).

### 9. BUSINESS SUMMARY
- Serves as the wrapper execution entrypoint for the product-and-sales extraction business process, triggered via UC4.
- Validates the critical run dependency `RUN_DATE` before launching the extraction to prevent downstream failures.
- Establishes persistent file-based audit logs capturing standard output and error messages with precise timestamps.
- Manages execution flow, ensuring any failure in the core extraction process is accurately trapped, logged, and propagated back to UC4.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Import required modules
import os
import sys
import datetime
import subprocess

# Step 2: Define environment parameters and paths
retail_home = os.environ.get("RETAIL_HOME", "/opt/etl/sales")
log_dir = os.path.join(retail_home, "logs")

# Step 3: Create log directory if it does not exist
try:
    os.makedirs(log_dir, exist_ok=True)
except Exception as e:
    print(f"Failed to create log directory {log_dir}: {e}", file=sys.stderr)
    sys.exit(1)

# Step 4: Define log file with current timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = os.path.join(log_dir, f"product_and_sales_extract_{timestamp}.log")

# Step 5: Define the logging function to write to stdout and log file
def log_message(message: str):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{current_time}] {message}"
    print(formatted_msg)
    try:
        with open(log_file_path, "a") as f:
            f.write(formatted_msg + "\n")
    except Exception as e:
        print(f"Failed to write to log file: {e}", file=sys.stderr)

# Step 6: Validate RUN_DATE environment variable
run_date = os.environ.get("RUN_DATE")
if not run_date:
    log_message("ERROR: RUN_DATE is not set - aborting")
    sys.exit(1)

# Step 7: Log execution start
log_message(f"Starting product-and-sales extract for run date {run_date}")

# Step 8: Invoke core extraction script
# # REVIEW-STRUCT: launcher [k_product_and_sales_extract.ksh] invoked — internal behaviour not available in this extraction;
# confirm logging, error propagation, and credential handling before finalizing the conversion.
core_script_path = os.path.join(retail_home, "sales", "k_product_and_sales_extract.ksh")

try:
    # Sourcing a script inside Python isn't directly possible; we run it as a separate process.
    # We pass the existing environment variables so that the child process inherits them.
    result = subprocess.run(["/bin/ksh", core_script_path], check=True, env=os.environ)
    rc = result.returncode
except subprocess.CalledProcessError as err:
    rc = err.returncode
    log_message(f"ERROR: k_product_and_sales_extract.ksh failed with exit code {rc}")
    sys.exit(rc)
except Exception as err:
    log_message(f"ERROR: Failed to execute {core_script_path}: {err}")
    sys.exit(1)

# Step 9: Validate the result and exit
if rc != 0:
    log_message(f"ERROR: k_product_and_sales_extract.ksh failed with exit code {rc}")
    sys.exit(rc)

log_message(f"Product-and-sales extract completed successfully for {run_date}")
sys.exit(0)
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `sales/r_product_and_sales_extract.ksh` | `sales/r_product_and_sales_extract.py` | Migrated to a Python wrapper script that creates log directories, validates the `RUN_DATE` environment variable, and invokes the core logic executable while managing error trapping and logs. |

---

### Job dependencies
- **Downstream Job**: `SALES.DAILY_SCHEDULE` (not yet migrated).
- **BigQuery / Composer Wiring**: Since `SALES.DAILY_SCHEDULE` is not yet migrated, the cross-job dependency cannot be fully finalized. In the target environment, once both are migrated, this boundary should be wired using an Airflow sensor (such as `ExternalTaskSensor`) or an explicit cross-DAG trigger to ensure execution is serialized.

### Execution order
The legacy orchestration sequence must be preserved within the target workflow (e.g., Airflow DAG tasks):
1. `sales/SALES.PRODUCT_AND_SALES_EXTRACT.xml` (UC4 Job definition; translates to Airflow DAG structure)
2. `sales/d_daily_sales_extract.sql` (Daily sales extract transformation)
3. `sales/d_product_master_load.sql` (Product master dimension load transformation)
4. `sales/r_product_and_sales_extract.ksh` (Wrapper script; maps to `sales/r_product_and_sales_extract.py`)
5. `sales/k_product_and_sales_extract.ksh` (Core shell logic; executed as a downstream step of the wrapper)

### Scheduling
- **Trigger Event**: This job is not directly triggered by any independent schedulers; it executes as an included/shared module inside broader orchestration sequences.
- **Target Platform Mapping**: The migrated Python artifact must remain a callable/importable unit (e.g., a specific task/operator within an Airflow DAG) and should not be given an independent standalone schedule.

### Schedule & variables
- **Schedule Inheritance**: Sourced dynamically as a shared module.
- **Scheduler-Set Variables**:
  - `RUN_DATE` = `'&$TODAY'` (Provided by the legacy UC4 orchestration job).
- **Delivery Mechanism**: Sourced in Airflow via the execution date context (using `{{ ds }}`) and supplied to the Python runtime as the `RUN_DATE` environment variable.

### Lineage
- **Upstream Producers**: None.
- **Downstream Consumers**:
  - `sales/k_product_and_sales_extract.ksh` (invoked via execution chain; belongs to a different design pass group).

### External system replacements
- **Local Logging and File-system Execution**:
  - Standard Unix commands (`mkdir -p`, `tee -a`) are replaced in Python using the `os.makedirs` library and native logging utilities. Standard output can be piped directly into GCP Cloud Logging via Cloud Composer, or saved into a GCS bucket (`gs://<GCS_BUCKET>/logs/`) if persistent log files are required for auditing.

### Cross-file dependencies
- **Core Executable Invocation**: This script maintains a hard dependency on `sales/k_product_and_sales_extract.ksh`. It executes this sibling script inside its runtime scope and reads its exit status.

### Target file plan
- **Target File Path**: `sales/r_product_and_sales_extract.py`
  - **Language**: Python (3.x)
  - **Source File**: `sales/r_product_and_sales_extract.ksh`
  - **Purpose**: A Python wrapper representing the wrapper shell logic. It asserts the presence of the `RUN_DATE` environment variable, manages logging setup, and invokes the core extraction routine. It ensures that the exact literal logging strings from the legacy shell script are preserved and that non-zero return codes are correctly propagated to the orchestrator.

### Environment-specific values
- **RETAIL_HOME** (GLOBAL):
  - **Role**: Points to the application base folder structure in the deployment environment.
  - **Source Mechanism**: Sourced at runtime from standard environment variables: `RETAIL_HOME = os.environ.get("RETAIL_HOME", "/opt/etl/sales")`.
- **RUN_DATE** (JOB-SPECIFIC):
  - **Role**: Target processing execution date.
  - **Source Mechanism**: Sourced from Airflow task execution context or run parameters: `RUN_DATE = os.environ.get("RUN_DATE")`.
- **LOG_DIR** (JOB-SPECIFIC):
  - **Role**: Directory where logs are written.
  - **Source Mechanism**: Dynamically derived at runtime: `LOG_DIR = os.path.join(RETAIL_HOME, "logs")`.
- **LOG_FILE** (JOB-SPECIFIC):
  - **Role**: Timestamped filename for execution output.
  - **Source Mechanism**: Dynamically generated using Python's `datetime` module.

### Risks and manual steps
- **Downstream Wiring Gap**: The downstream consumer `SALES.DAILY_SCHEDULE` is not yet migrated, meaning the final orchestrator pipeline cannot establish end-to-end serialization.
- **Sourced Shell Script Integration**: The core logic file `sales/k_product_and_sales_extract.ksh` is out of scope for this design pass. Integration testing is required once both files are migrated to ensure they pass variables and handle errors consistently.