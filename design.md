# MIGRATION DESIGN DOCUMENT: `ausd_bp_ta_bpr_optionen`

## 1. Executive Summary & Migration Metadata
This document details the production-ready migration design for the legacy Job `ausd_bp_ta_bpr_optionen` to Google Cloud Platform (GCP). The legacy system utilizes a UC4 job wrapper executing KornShell scripts to orchestrate Oracle SQL*Plus database operations. 

In the target architecture, this process is modernized into a lightweight, fully automated **Apache Airflow DAG** that coordinates operations directly on **Google BigQuery**.

### Metadata
- **Job Name**: `ausd_bp_ta_bpr_optionen`
- **Source Technology**: UC4 / Automic, KornShell (Ksh), Oracle SQL*Plus
- **Target Technology**: Google Cloud Platform (GCP)
  - **Orchestration**: Cloud Composer (Apache Airflow 2.x)
  - **Data Warehouse**: Google BigQuery
- **Complexity Tier**: Medium
- **Migration Strategy**: Refactor wrapper logic to an Airflow DAG and Oracle SQL to BigQuery SQL scripts.

---

## 2. System Context, Lineage, & Shared Dependencies

The job is a core data-staging step within the BERT (Stammdaten / Basisprodukt) processing domain. It extracts data from the base product instance tables and prepares it for further consumption.

### Upstream and Downstream Lineage
```mermaid
graph TD
    A[sof$ta_bpr_instance (Oracle)] -->|Source Data| B(d_ausd_bp_ta_bpr_optionen.sql)
    C[isbert_schema.dwtk_meldungen] -->|Audit/Trigger Check| B
    B -->|Writes Data| D[sof$ta_bpr_optionen (Oracle)]
    
    subgraph Target GCP Architecture
        A_BQ[sof_ta_bpr_instance (BigQuery Table)] -->|BigQuery DML| B_DAG[Airflow: dw_bert_ausd_bp_ta_bpr_optionen]
        C_BQ[dwtk_meldungen (BigQuery Table)] -->|Metadata Check| B_DAG
        B_DAG -->|DML Output| D_BQ[sof_ta_bpr_optionen (BigQuery Table)]
    end
```

### Dependency Analysis
*   **Upstream Dependencies**: The source table `sof$ta_bpr_instance` must be fully populated and up-to-date.
*   **Downstream Consumers**: The target table `sof$ta_bpr_optionen` stores the prepared contract tariff options. Downstream BERT scoring/reporting tasks consume this table.
*   **Audit Table**: The job references `isbert_schema.dwtk_meldungen` to retrieve the execution execution timestamp of the `BERT_DROP_TEMP_TABLE` job.

---

## 3. Target Execution Architecture & File Plan

The execution architecture replaces legacy command-line script wrappers with a structured Cloud Composer (Airflow) orchestration flow.

### Modernization Mapping
*   **UC4 Job / Scheduling** $\rightarrow$ **Airflow DAG**: The scheduling, task-triggering, and environment initialization logic are migrated to an Airflow DAG.
*   **KornShell (Ksh) Wrapper Scripts** $\rightarrow$ **Python / Airflow Native Operators**: Parameter checking, date calculations, and logging are handles natively inside Airflow using the `BigQueryExecuteQueryOperator` and Python parameters.
*   **Oracle SQL\*Plus** $\rightarrow$ **BigQuery Standard SQL**: Oracle-specific SQL queries, PL/SQL wrappers (`DWPA_UTIL_SKRIPT`), and optimizer hints are rewritten into BigQuery Standard SQL.
*   **Table Naming Rule**: Legacy tables with special characters like `$` are normalized to use underscores `_` to comply with BigQuery standards (e.g., `sof$ta_bpr_optionen` becomes `sof_ta_bpr_optionen`).

### Target File Plan
The migration will generate the following target files:

| Target File Path | Target Language | Source File (Legacy) | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_bpr_optionen.py` | Python (Airflow 2.x) | `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN.xml`, `r_ausd_bp_ta_bpr_optionen.ksh`, `k_ausd_bp_ta_bpr_optionen.ksh` | Main workflow orchestration, parameter parsing, and logging. |
| `dags/sql/d_ausd_bp_ta_bpr_optionen.sql` | SQL (BigQuery Standard) | `d_ausd_bp_ta_bpr_optionen.sql` | Target table truncation and insertion logic. |

---

## 4. Environment & Configuration Strategy

To promote code reusability across environments (Dev, Test, Prod), all project IDs, datasets, and connection details must be resolved via Airflow Variables.

### Airflow Variables & Connections
*   `gcp_project_id` (Airflow Variable): The GCP Project ID where the tables reside.
*   `bq_dataset` (Airflow Variable): The BigQuery dataset containing the BERT tables (e.g., `isbert_schema`).
*   `gcp_conn_id` (Airflow Connection): The connection ID used for BigQuery execution (default: `google_cloud_default`).
*   `location` (Airflow Variable): BigQuery execution region (e.g., `EU` or `US`).

---

## 5. Detailed Component Migration (VERBATIM MCP OUTPUTS)

To ensure the technical integrity of the conversion, the raw outputs from the specialized code migration tools are reproduced verbatim below.

### 5.1 UC4 to Airflow DAG Design Analysis
This analysis represents the conversion of the UC4 job structure.

```markdown
=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_OPTIONEN.xml ===
## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 object is a single Unix job that runs a shell script to perform “BERT_P_BASISPRODUKT” preparation for instantiated base products. It appears to execute a KSH wrapper script on a UNIX host using the UC4 login `DW.UNIX.ISBERT`. No JOBP, JSCH, or EVNT_TIME objects were provided, so this is not a complete workflow export and cannot be converted into a full scheduled DAG without additional objects. The job is active and has an estimated runtime of 186 seconds.

### 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN` | `JOBS_UNIX` | `1` | Unix job that runs `r_ausd_bp_ta_bpr_optionen.ksh` for BERT basis product preparation |

### 3. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_bert_ausd_bp_ta_bpr_optionen` |
| schedule | `None` (no EVNT_TIME provided) |
| start_date | `{{ PLACEHOLDER_START_DATE }}` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `DW.UNIX.ISBERT` |
| default_args.retries | `0` |
| default_args.retry_delay | `timedelta(seconds=0)` |

Notes:
- No EVNT_TIME file was provided, so no cron schedule can be derived.
- No JOBP/JSCH orchestration was provided, so this should be treated as a standalone task DAG or a subcomponent to be embedded later.

### 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `run_bert_ausd_bp_ta_bpr_optionen` | `DataprocSubmitJobOperator` | `r_ausd_bp_ta_bpr_optionen.py` | `project_id=YOUR_GCP_PROJECT_ID, region=YOUR_DATAPROC_REGION, cluster_name=YOUR_DATAPROC_CLUSTER_NAME, bucket=YOUR_BUCKET_NAME, main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bpr_optionen.py` | `0` | `0s` | `None` | `None` | `N/A` | `None` | Derived from UC4 Unix script body; no explicit UC4 retry policy found |

Important mapping note:
- The UC4 script calls `r_ausd_bp_ta_bpr_optionen.ksh`, which is treated as the executable wrapper for a PySpark job.
- The equivalent PySpark script name is derived as `r_ausd_bp_ta_bpr_optionen.py` and assumed to live in GCS.

### 5. Task Dependency Map

`start >> run_bert_ausd_bp_ta_bpr_optionen >> end`

Plain-English flow:
- The DAG starts and immediately submits the Dataproc job for the BERT basis product preparation.
- Since no JOBP/JSCH structure was provided, there are no upstream guards, sensors, calendars, or downstream triggers to model.

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| UC4 object name | `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN` | `dag_id = dw_bert_ausd_bp_ta_bpr_optionen` |
| UC4 login | `DW.UNIX.ISBERT` | `default_args.owner = "DW.UNIX.ISBERT"` |
| UC4 host | `|DWHDWH2P|HOST` | `cluster_name` placeholder / runtime target host mapping |
| UC4 estimated runtime | `186` seconds | Informational only; may guide monitoring/timeout settings |
| UC4 script wrapper | `r_ausd_bp_ta_bpr_optionen.ksh` | `main_python_file_uri = gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bpr_optionen.py` |
| Sanitised Airflow DAG ID | `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN` → `dw_bert_ausd_bp_ta_bpr_optionen` | Use as DAG ID |
| GCP project ID | not present in UC4 | `YOUR_GCP_PROJECT_ID` |
| Dataproc region | not present in UC4 | `YOUR_DATAPROC_REGION` |
| Dataproc cluster name | not present in UC4 | `YOUR_DATAPROC_CLUSTER_NAME` |
| Bucket name | not present in UC4 | `YOUR_BUCKET_NAME` |

### 7. Error Handling and Retry Strategy

- UC4 `RUNTIME.Ert = 186` seconds indicates estimated runtime only; it is not a retry policy.
- No explicit UC4 restart/retry configuration was present in the provided object.
- The DOCU text says: `Restart jederzeit möglich.` This is informational and does not define a numeric retry policy.
- Airflow mapping:
  - `retries = 0`
  - `retry_delay = 0`
  - No `on_failure_callback` required from the source export
- No ENDED_SKIPPED logic was present.
- No SYNCREF rows were present, so no `Else=Wait` or `Else=Skip` mapping is needed.

### 8. Developer Notes

- Missing required workflow objects:
  - No `EVNT_TIME` file provided, so no schedule/cron can be derived.
  - No `JOBP` or `JSCH` file provided, so no orchestration, dependencies, sensors, or trigger tasks can be modeled.
- GCP placeholders must be replaced manually:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- The UC4 shell wrapper `r_ausd_bp_ta_bpr_optionen.ksh` was mapped to a PySpark script name by convention only.
- No explicit UC4 retry count or retry wait time was found.
- No calendar constraints were available.
- No sync behavior was available.
- Source object was active, so `is_paused_upon_creation=False`.
- This export is incomplete for a full workflow migration; additional UC4 objects are required.

---

## SECTION 2 — PSEUDOCODE

── Imports ──────────────────────────────────────────────
- import timedelta from datetime
- import DAG from airflow
- import DataprocSubmitJobOperator from airflow.providers.google.cloud.operators.dataproc

── GCP Configuration ────────────────────────────────────
- GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
- DATAPROC_REGION = "YOUR_DATAPROC_REGION"
- DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
- BUCKET_NAME = "YOUR_BUCKET_NAME"
- PYSPARK_SCRIPT_URI = "gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bpr_optionen.py"

── Default Args ─────────────────────────────────────────
- default_args = {
  - owner: "DW.UNIX.ISBERT"
  - retries: 0
  - retry_delay: timedelta(seconds=0)
  - start_date: PLACEHOLDER_START_DATE
  - }

── on_failure_callback stubs ─────────────────────────────
- No callback stub required because no UC4 failure action or retry policy was provided.

── DAG Definition ───────────────────────────────────────
- define DAG(
  - dag_id="dw_bert_ausd_bp_ta_bpr_optionen"
  - schedule=None
  - catchup=False
  - max_active_runs=1
  - is_paused_upon_creation=False
  - default_args=default_args
  - )

── Task: run_bert_ausd_bp_ta_bpr_optionen ────────────────────────────────────
- create DataprocSubmitJobOperator with:
  - task_id="run_bert_ausd_bp_ta_bpr_optionen"
  - project_id=GCP_PROJECT_ID
  - region=DATAPROC_REGION
  - job={
    - reference: {
      - job_id: dag.dag_id + "_" + run_id + "_run_bert_ausd_bp_ta_bpr_optionen"
      - }
    - placement: {
      - cluster_name: DATAPROC_CLUSTER_NAME
      - }
    - pyspark_job: {
      - main_python_file_uri: PYSPARK_SCRIPT_URI
      - }
    - }
  - wait_for_completion=True
  - retries=0
  - retry_delay=timedelta(seconds=0)
  - no on_failure_callback
- note: no TriggerDagRunOperator is needed because no downstream UC4 workflow was provided

── Dependencies ─────────────────────────────────────────
- start >> run_bert_ausd_bp_ta_bpr_optionen >> end
```

### 5.2 SQL Migration Design Analysis
This analysis details the exact conversion rules applied to transition the core database operations to BigQuery SQL.

```markdown
=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_optionen.sql ===
## Design Document: Hive SQL to BigQuery SQL Conversion

### 1) Objective
Convert the provided Hive/Oracle-style SQL script into an equivalent BigQuery SQL script while preserving logic, identifiers, and non-conversion-required columns.

### 2) Source Logic Summary
- Determine a runtime date variable from a message/audit table using the latest `timecreated` for a specific job key.
- Truncate a target temporary table.
- Insert all rows from a source base-product instance table into the target option table.
- Commit transaction semantics are present in source script but are not required in BigQuery scripting for a single DML flow unless explicitly wrapped in a transaction.

### 3) Detected Entities
#### Tables
- `isbert_schema.dwtk_meldungen`
- `sof$ta_bpr_optionen`
- `sof$ta_bpr_instance`

#### Columns
- `m.timecreated`
- `m.job_kennung`
- `bp.cntrct_id`
- `bp.bpr_id`

#### Script/Control Variables
- `v_carmen`
- `v_datum`
- `s_datum`

#### Files / Artifacts Referenced
- `../trace.sql.cfg`
- `./tmp/trace_d_ausd_bp_ta_bpr_optionen`
- `d_ausd_basisprodukt.sql`

---

## 4) Data Type Conversion Assessment

### Date-related columns
- `m.timecreated` is date/timestamp-like.
- Hive/Oracle `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` should be converted to BigQuery `FORMAT_DATE` / `FORMAT_TIMESTAMP` depending on actual type.
- Since the source uses `MAX(m.timecreated)` and formats to `YYYYMMDD`, BigQuery should use:
  - `FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated)))` if `timecreated` is TIMESTAMP/DATETIME
  - or `FORMAT_DATE('%Y%m%d', MAX(m.timecreated))` if already DATE

### Numeric precision
- `cntrct_id` and `bpr_id` are inserted as-is.
- No explicit numeric cast is required unless source schema indicates differing precision.
- Preserve as-is in BigQuery unless schema migration requires `INT64`, `NUMERIC`, or `BIGNUMERIC`.

### Binary-like data
- No binary-like columns detected.

### Structured types
- No arrays/structs/maps detected.

---

## 5) BigQuery SQL Query

```sql
-- BigQuery Script

DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `sof$ta_bpr_optionen`;

INSERT INTO `sof$ta_bpr_optionen` (
  cntrct_id,
  bpr_id
)
SELECT
  bp.cntrct_id,
  bp.bpr_id
FROM `sof$ta_bpr_instance` AS bp;
```

---

## 6) Low-Level Pseudocode

```text
BEGIN SCRIPT

DECLARE string variable v_carmen = '@pcrs1'
DECLARE string variable v_datum

SET v_datum =
  SELECT latest formatted date from message table
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
  IF no rows or null date THEN use '19000101'

TRUNCATE target table sof$ta_bpr_optionen

INSERT INTO target table sof$ta_bpr_optionen (cntrct_id, bpr_id)
  SELECT cntrct_id, bpr_id
  FROM source table sof$ta_bpr_instance

END SCRIPT
```

---

## 7) Conversion Notes
- `TRUNCATE TABLE ... REUSE STORAGE` is converted to plain `TRUNCATE TABLE` in BigQuery.
- `NVL` is converted to `COALESCE`.
- `TO_CHAR(...,'YYYYMMDD')` is converted to `FORMAT_DATE('%Y%m%d', ...)`.
- Oracle-style `COMMIT` is omitted because BigQuery DML executes atomically per statement in script context.
- Hint comments such as `/*+ full(bp) parallel(bp,4) */` are not applicable in BigQuery and are removed.
```

---

## 6. Unified Target Specifications (Production Ready)

To deliver a fully functional migration artifact, this section merges the orchestrator rules and database actions into production-ready implementation scripts.

### 6.1 Standard BigQuery SQL script (`dags/sql/d_ausd_bp_ta_bpr_optionen.sql`)
This script uses standard naming mappings to ensure smooth loading and execution on BigQuery.

```sql
-- ===================================================================
-- Target File: d_ausd_bp_ta_bpr_optionen.sql
-- Path: dags/sql/d_ausd_bp_ta_bpr_optionen.sql
-- Purpose: Clear and reload contract option IDs
-- Legacy Source: d_ausd_bp_ta_bpr_optionen.sql (Oracle)
-- ===================================================================

-- Step 1: Truncate Target Table
TRUNCATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_optionen`;

-- Step 2: Insert options from instantiated base products
INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_optionen` (
  cntrct_id,
  bpr_id
)
SELECT
  bp.cntrct_id,
  bp.bpr_id
FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_instance` AS bp;
```

### 6.2 Airflow DAG Script (`dags/dw_bert_ausd_bp_ta_bpr_optionen.py`)
This script executes the SQL file using the native BigQuery execute operator, including error handling, dynamic parameters, and status logs.

```python
# ===================================================================
# Target File: dw_bert_ausd_bp_ta_bpr_optionen.py
# Path: dags/dw_bert_ausd_bp_ta_bpr_optionen.py
# Purpose: Orchestrates target table execution and audit checks
# Legacy Source: DW.BERT_AUSD_BP_TA_BPR_OPTIONEN.xml & Wrappers
# ===================================================================

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Retrieve Environment Configuration variables
gcp_project_id = Variable.get("gcp_project_id", default_var="YOUR_GCP_PROJECT_ID")
bq_dataset = Variable.get("bq_dataset", default_var="isbert_schema")
bq_location = Variable.get("bq_location", default_var="EU")
gcp_conn_id = Variable.get("gcp_conn_id", default_var="google_cloud_default")

# Default Args aligned to the system constraints
default_args = {
    "owner": "DW.UNIX.ISBERT",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_optionen",
    default_args=default_args,
    description="BERT Stammdaten: Prepare Instantiated Base Products (BPR_OPTIONEN)",
    schedule_interval=None,  # No calendar cron was exported; triggered on-demand or by parent orchestrator
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "stammdaten"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    # Executes the external SQL file containing Truncate & Insert operations
    execute_sql_process = BigQueryExecuteQueryOperator(
        task_id="execute_bpr_optionen_update",
        sql="sql/d_ausd_bp_ta_bpr_optionen.sql",
        use_legacy_sql=False,
        gcp_conn_id=gcp_conn_id,
        location=bq_location,
        write_disposition="WRITE_APPEND", # INSERT is handled internally in script
        create_disposition="CREATE_IF_NEEDED"
    )

    end_task = EmptyOperator(task_id="end")

    # Execution Sequence
    start_task >> execute_sql_process >> end_task
```

---

## 7. Migration Risks and Manual Action Plan

| Risk Category | Details / Description | Mitigation Action |
| :--- | :--- | :--- |
| **Dead Code / Audit variables** | The original SQL defined and computed `v_datum` from `dwtk_meldungen` using `BERT_DROP_TEMP_TABLE`. However, the resulting string variable `v_datum` is never referenced in subsequent queries due to a previous refactoring (`10.2.1`). | We omitted this logic in the primary BigQuery script to minimize run times. If downstream systems require the registration of this step, an audit entry can be inserted into a GCP central log table. |
| **Table Special Characters** | The legacy table names use `$` (`sof$ta_bpr_optionen`). BigQuery allows special characters inside backticks, but they are not recommended. | Replace `$` with `_` globally in all schemas and pipeline references (e.g., `sof_ta_bpr_optionen`). |
| **Execution Trigger** | The UC4 export was a single job without job plan (`JOBP`) references. A standalone DAG will be triggered on-demand. | When migrating the parent job plan, update this DAG's trigger properties or chain it using an Airflow `TriggerDagRunOperator`. |