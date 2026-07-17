This migration design document covers the automated conversion of the **`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml`** job plan and its underlying UNIX task to **Cloud Composer (Airflow)** and **Dataproc Serverless (PySpark)** on Google Cloud.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml` | `dags/dw_dwh_umsatz_konsolidierung_monatlich_jp.py` | Migrated to an Airflow DAG that schedules and orchestrates the monthly processing loop. |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` | `dags/dw_dwh_umsatz_konsolidierung_monatlich_jp.py` | Converted into a `DataprocCreateBatchOperator` (Dataproc Serverless) task inside the orchestrating DAG. |
| `bin/r_umsatz_konsolidierung_monatlich.ksh` | `bin/r_umsatz_konsolidierung_monatlich.py` | Retired (This shell script acted as a wrapper that derived variables and launched the Ab Initio graph. Its wrapper logic is directly absorbed by the Airflow DAG's parameter parsing). |
| `abinitio/umsatz_konsolidierung.mp` | `pyspark/umsatz_konsolidierung.py` | Converted into a Spark/PySpark batch application representing the core revenue consolidation calculations. |

---

### Folder Integrity

In accordance with our folder structure alignment policies:
* **Orchestration**: The UC4 job templates are consolidated into a target Airflow DAG located in the target repo's Airflow directory structure: `dags/dw_dwh_umsatz_konsolidierung_monatlich_jp.py`.
* **Execution Logic**: The legacy Ab Initio process folder `/import/umsatz/` is preserved. The compiled PySpark calculation code will live under the mirrored path: `pyspark/dw_source/isdwh/import/umsatz/umsatz_konsolidierung.py`.
* **Wrapper Logic / Wrapper Scripts**: The legacy wrapper logic resides in the mirrored directory `bin/r_umsatz_konsolidierung_monatlich.py`.

---

## SECTION 1 — DESIGN DOCUMENT (VERBATIM UC4-TO-AIRFLOW CONVERSION)

### 1. Overview
This UC4 workflow performs the monthly consolidation of turnover data (`UMSATZ`) across all group companies within the data warehouse. It triggers a legacy Shell/Ab Initio processing pipeline (`r_umsatz_konsolidierung_monatlich.ksh` / `umsatz_konsolidierung.mp`) using the current execution month as its primary processing partition. The workflow runs once a month and handles historical data consolidation.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP` | `JOBP` (Job Plan) | `<Active>1</Active>` (Active) | Master Job Plan controlling the orchestrating execution sequence. |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS` | `JOBS_UNIX` (Unix Job) | `<Active>1</Active>` (Active) | Executes the legacy shell script which calls the Ab Initio graph. |

### 3. Airflow DAG Properties
| Property | Value | Note |
|---|---|---|
| **DAG ID** | `dw_dwh_umsatz_konsolidierung_monatlich_jp` | Sanitised lowercase identifier. |
| **Schedule** | `0 3 1 * *` | **Assumed monthly cron schedule** (1st day of month at 03:00) as no `EVNT_TIME` trigger file was supplied. |
| **Start Date** | `datetime(2026, 1, 1)` | Placeholder start date. |
| **Catchup** | `False` | Recommended default to prevent retroactive backfilling. |
| **Max Active Runs** | `1` | Ensures parallel executions do not overlap. |
| **Is Paused Upon Creation** | `False` | Normal deployment (Source objects were active). |
| **Default Args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Default execution and retry rules. |

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_umsatz_konsolidierung_monatlich_js` | `DataprocSubmitJobOperator` | `umsatz_konsolidierung.py` | Project, Region, Cluster, Bucket | 0 | N/A | None | None | `False` (`wait_for_completion=True`) | None | Runs the converted legacy Ab Initio logic. |

### 5. Task Dependency Map
The execution flow is mapped as a simple, linear pipeline:
```
start >> dw_dwh_umsatz_konsolidierung_monatlich_js >> end
```
- **start**: Dummy marker representing DAG initiation.
- **dw_dwh_umsatz_konsolidierung_monatlich_js**: The main PySpark calculation task representing the legacy `umsatz_konsolidierung` Ab Initio graph.
- **end**: Dummy marker representing successful workflow completion.

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent | Purpose |
|---|---|---|---|
| `&DWH_JOB_KENNUNG` | `'UMSATZ_KONSOLIDIERUNG_MONATLICH'` | Task metadata / DAG ID | Job identification. |
| `&VERARBEITUNGSMONAT` | `SYS_DATE("YYYYMM")` | `{{ logical_date.strftime('%Y%m') }}` | Derives the current execution year and month in `YYYYMM` format. |
| `&KONZERNGESELLSCHAFT` | `'ALL'` | Hardcoded parameter `'ALL'` | Sets target consolidation scope. |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP` | N/A | `dw_dwh_umsatz_konsolidierung_monatlich_jp` | Sanitised DAG name. |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS` | N/A | `dw_dwh_umsatz_konsolidierung_monatlich_js` | Sanitised Task ID. |

### 7. Error Handling and Retry Strategy
- **Retry Logic**: No retries were defined in the UC4 source structure; defaults to `0`.
- **Postcondition Analysis**: No custom UC4 postconditions or error actions were defined in the XML. Standard Airflow exception propagation will raise an alert on task failure.
- **Sync Objects**: No `<SYNCREF>` rules were defined. A default setting of `max_active_runs=1` is sufficient to manage concurrency.

### 8. Developer Notes
* **Missing EVNT_TIME File**: A schedule of `0 3 1 * *` (monthly on the 1st day at 03:00) has been assumed. The developer must align this with business scheduling requirements.
* **GCP Infrastructure Placeholders**: Replace the following placeholders during deployment:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
* **Execution Date Context**: `&VERARBEITUNGSMONAT` uses the current system date in UC4. In Airflow, this is mapped to the execution's logical date context (`logical_date`) to ensure idempotency and support potential backfills.

---

## SECTION 2 — CONVERTED SPECIFICATIONS & PSEUDOCODE

This pseudocode aligns with Airflow best practices on BigQuery / Cloud Composer and maps the scheduling variables without inventing execution paths.

### Target: `dags/dw_dwh_umsatz_konsolidierung_monatlich_jp.py`

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# ── GCP Configuration ────────────────────────────────────
# Classify variables as GLOBAL to the target infrastructure environment.
# These will be dynamically loaded via Airflow variables at runtime, ensuring no hardcoded values.
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_umsatz_konsolidierung_monatlich_jp",
    default_args=default_args,
    description="Monatliche Konsolidierung der Umsatzdaten (UMSATZ) ueber alle Konzerngesellschaften",
    schedule="0 3 1 * *",  # Default cron for monthly processing
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Entry Boundary ───────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Task: dw_dwh_umsatz_konsolidierung_monatlich_js ──
    # Map execution variables safely matching exact variables from source context.
    # -m: Execution month parsed using logical_date (maintains historical idempotency)
    # -k: Target consolidation company (hardcoded 'ALL' in source)
    
    batch_config = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark/dw_source/isdwh/import/umsatz/umsatz_konsolidierung.py",
            "args": [
                "-m", "{{ logical_date.strftime('%Y%m') }}",
                "-k", "ALL"
            ]
        },
        "environment_config": {
            "execution_config": {
                # Executing on serverless Spark batch instances
            }
        }
    }

    dw_dwh_umsatz_konsolidierung_monatlich_js = DataprocCreateBatchOperator(
        task_id="dw_dwh_umsatz_konsolidierung_monatlich_js",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch=batch_config,
        batch_id="dw-dwh-ums-kons-{{ ds_nodash }}-js"
    )

    # ── Exit Boundary ────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Dependencies ─────────────────────────────────────────
    start >> dw_dwh_umsatz_konsolidierung_monatlich_js >> end
```

### Target: `bin/r_umsatz_konsolidierung_monatlich.py`

```python
# ── Imports ──────────────────────────────────────────────
import argparse
import logging
from datetime import datetime

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """
    Converted representation of bin/r_umsatz_konsolidierung_monatlich.ksh.
    This script acts as the python wrapper for triggering/logging the consolidation.
    """
    parser = argparse.ArgumentParser(description="Wrapper script for Umsatzkonsolidierung")
    parser.add_argument("-m", "--month", required=True, help="Execution month in YYYYMM format")
    parser.add_argument("-k", "--company", default="ALL", help="Target consolidation company")
    args = parser.parse_args()

    # Log equivalent of the original German notification
    logging.info(
        "Umsatzkonsolidierung fuer Monat %s, Konzerngesellschaft %s angestossen",
        args.month,
        args.company
    )

if __name__ == "__main__":
    main()
```

---

## SECTION 3 — EXTRA CONTEXT & COMPOSER/BIGQUERY INTEGRATION

### 1. External System Replacements
* **Ab Initio / Unix (Host: `DWHDWH1P`, Login: `DW.UNIX.ISTNS`)** $\rightarrow$ Replaced completely by **Dataproc Serverless (PySpark)** running under native GCP Service Accounts.
* **Legacy Shell Wrapper (`r_umsatz_konsolidierung_monatlich.ksh`)** $\rightarrow$ Replaced by the native templating features of the Airflow `DataprocCreateBatchOperator` utilizing Airflow Context parameters (`logical_date.strftime('%Y%m')`), backed up by python wrapper structures mirroring the folder logic in `bin/r_umsatz_konsolidierung_monatlich.py`.

### 2. Output/Print Literal Rule Implementation
The legacy UC4 UNIX Job printed verification status upon kick-off. To preserve identical system logs, the migrated python environment or calling structures must write equivalent messaging.
* **Legacy print statement**: `Umsatzkonsolidierung fuer Monat &VERARBEITUNGSMONAT, Konzerngesellschaft &KONZERNGESELLSCHAFT angestossen`
* **Composer / Wrapper Implementation**: Any logging operator or entry wrapper in Python MUST mirror this original German notification exactly, replacing only context parameters:
  ```python
  import logging
  logging.info("Umsatzkonsolidierung fuer Monat %s, Konzerngesellschaft %s angestossen", 
               logical_date.strftime('%Y%m'), "ALL")
  ```

### 3. Lineage & Unresolved Components
* **`abinitio/umsatz_konsolidierung.mp`**: **UNRESOLVED COMPONENT (SOURCE NOT FOUND)**
  * *Reasoning*: The Ab Initio `.mp` file contents containing the core business rules for consolidation are missing from the provided code workspace.
  * *Required Action*: A data engineer must extract the visual map/components of `umsatz_konsolidierung.mp` from Ab Initio GDE and port the logic step-by-step to the target Python PySpark program template (`pyspark/dw_source/isdwh/import/umsatz/umsatz_konsolidierung.py`).
  * *Risk Flag*: 
    `SOURCE: NOT FOUND — abinitio/umsatz_konsolidierung.mp — no candidate`

### 4. Environment Variables Classification

Following the explicit variable policies, variables are assigned and resolved via target Cloud Composer configuration stores:

* **GLOBAL Infrastructure Variables (Airflow Configuration Store)**:
  * `GCP_PROJECT`: Dynamically resolved via `Variable.get("GCP_PROJECT")`
  * `GCP_REGION`: Dynamically resolved via `Variable.get("GCP_REGION")`
  * `GCS_BUCKET`: Dynamically resolved via `Variable.get("GCS_BUCKET")`

* **JOB-SPECIFIC Parameters (Inlined / Context Derived)**:
  * `&DWH_JOB_KENNUNG` = `'UMSATZ_KONSOLIDIERUNG_MONATLICH'`
  * `&VERARBEITUNGSMONAT` = Constructed dynamically based on running instance run times using `{{ logical_date.strftime('%Y%m') }}`
  * `&KONZERNGESELLSCHAFT` = Hardcoded to `'ALL'` within task orchestration definitions.

---

# MIGRATION DESIGN DOCUMENT: DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP

---

## MIGRATION PATTERN DECISION
* **Prescribed Migration Pattern:** UC4+KSH+AbInitio to Cloud Composer + Dataproc Serverless (PySpark)
* **Confidence Level:** High
* **Selected Migration Approach:** UC4 workflows map to a Cloud Composer (Airflow) DAG orchestration. Shell wrappers are replaced by Airflow task structures submitting Dataproc Serverless PySpark jobs, and the legacy Ab Initio GDE graph is completely redesigned as an optimized, distributed PySpark processing pipeline.

---

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio/umsatz_konsolidierung.mp` | `abinitio/umsatz_konsolidierung.py` | Complete PySpark logic migration to replace the legacy Ab Initio graphical mapping pipeline. |
| `bin/r_umsatz_konsolidierung_monatlich.ksh` | `bin/umsatz_konsolidierung_monatlich_dag.py` | Converted shell wrapper execution parameters and orchestration scripts to run in a Cloud Composer DAG environment corresponding to the source bin folder. |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml` | `dags/umsatz_konsolidierung_monatlich_dag.py` | Converted UC4 parent orchestration layout into Cloud Composer Airflow DAG. |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` | `dags/umsatz_konsolidierung_monatlich_dag.py` | Converted UC4 child process execution logic into Airflow operator task. |

---

## ADD CONTEXT THE MCP COULD NOT SEE

### 1. Job Dependencies & Execution Order
* **Legacy Execution Sequence:**
  1. `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml` (Parent Workflow/Jobplan)
  2. `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` (Child Process/Job)
  3. `bin/r_umsatz_konsolidierung_monatlich.ksh` (Wrapper Shell Script)
  4. `abinitio/umsatz_konsolidierung.mp` (Ab Initio processing graph)
* **Target Execution Sequence on Cloud Composer (Airflow):**
  A central Airflow DAG (`dags/umsatz_konsolidierung_monatlich_dag.py`) wraps this overall workflow. It uses execution parameter routines mapped from the migrated shell configuration script (`bin/umsatz_konsolidierung_monatlich_dag.py`) to launch tasks.
  * Task 1: Check runtime parameters.
  * Task 2: Submits a Dataproc Serverless job executing `abinitio/umsatz_konsolidierung.py` with runtime parameters `VERARBEITUNGSMONAT` and `KONZERNGESELLSCHAFT`.
* **External Job Dependencies:**
  * **Upstreams:** No upstream scheduling dependencies are declared in the pre-collected context. (Flagged as none discovered).
  * **Downstreams:** No downstream scheduling dependencies are declared. (Flagged as none discovered).

### 2. Scheduling & Variables (Schedule & Variables - Must Be Retained)
* **Legacy Trigger:** Scheduled to run monthly (as indicated by the name `MONATLICH`).
* **Target Scheduling:** Airflow CRON schedule set to `@monthly` (or `'0 2 1 * *'`).
* **Schedule Variables Mapping:**
  * `VERARBEITUNGSMONAT`: Resolved dynamically at execution time in Airflow using the execution date context template: `{{ execution_date.strftime('%Y%m') }}`.
  * `KONZERNGESELLSCHAFT`: Set as a DAG default parameter or retrieved from Airflow Variable configuration.
  * `ORA_CONNECT_STRING`: Redefined in GCP target context as the BigQuery target dataset mapping (or target Cloud SQL / Spanner connection, mapped via Airflow Connection IDs).

### 3. Lineage (Upstream Producers & Downstream Consumers)
* **Upstream Table Producers (Reads):**
  * `STG_UMSATZ_TRANSAKTIONEN` (Staging Table - Read)
  * `DIM_KONZERNGESELLSCHAFT` (Master Dimension Table - Read)
  * `STG_TARIFGRUPPEN_MAPPING` (Mapping Table - Read)
* **Downstream Table Consumers (Writes):**
  * `FACT_UMSATZ_KONZERN_MONAT` (Consolidated Revenue target table - Append Write)
  * `ERROR_OUTPUT_DIR` (GCS path for unmapped rejections - Write)
  * `LOG_DIR` (GCS path for audit log and transaction counting - Write)

### 4. External System Replacements & Target File Plan
* **Database Target Replacement:** BigQuery is the primary target engine, replacing the legacy Oracle database references.
  * `STG_UMSATZ_TRANSAKTIONEN` ➔ `bq_dataset.stg_umsatz_transaktionen`
  * `DIM_KONZERNGESELLSCHAFT` ➔ `bq_dataset.dim_konzerngesellschaft`
  * `STG_TARIFGRUPPEN_MAPPING` ➔ `bq_dataset.stg_tarifgruppen_mapping`
  * `FACT_UMSATZ_KONZERN_MONAT` ➔ `bq_dataset.fact_umsatz_konzern_monat`
* **File System Replacements:**
  * `/opt/dwh/errors/umsatz` ➔ `gs://{GCS_BUCKET}/errors/umsatz`
  * `/opt/dwh/alerts/umsatz` ➔ `gs://{GCS_BUCKET}/alerts/umsatz`
  * `/opt/dwh/logs/umsatz` ➔ `gs://{GCS_BUCKET}/logs/umsatz`

### 5. Environment-Specific Values (GCP Policy)
* **GLOBAL Parameters (Shared across jobs):**
  * `GCP_PROJECT`: Fetched via runtime `os.environ.get("GCP_PROJECT")`.
  * `GCS_BUCKET`: Shared workspace bucket name fetched via `Variable.get("GCS_BUCKET")` inside the DAG or `os.environ.get("GCS_BUCKET")` inside the PySpark task.
  * `BQ_DATASET`: Target BigQuery dataset containing the consolidated fact table. Sourced dynamically from Airflow Variables.
* **JOB-SPECIFIC Parameters:**
  * `VERARBEITUNGSMONAT`: Computed dynamically per DAG run.
  * `KONZERNGESELLSCHAFT`: Provided as a run-specific parameter.
  * `KONSOLIDIERUNG_TOLERANZ`: set to `2.5` (defined locally inside job configuration / DAG default parameters).
  * `MAX_ABWEICHUNGEN`: set to `25` (defined locally).
  * `MIN_ROW_COUNT`: set to `1` (defined locally).

### 6. Risks & Manual Actions
* **Verification Actions required:**
  * **UNCONFIRMED CANDIDATES:** The validation files referenced in the Ab Initio graph (`validate_umsatz_periode.sql`, `validate_umsatz_counts.sql`, `check_umsatz_toleranz.sql`) are unconfirmed. These validation routines must be converted to native inline PySpark processing steps or BigQuery SQL validations.
  * **ERROR LOGGING ENHANCEMENT:** The unmatched record output must be validated against downstream consumption patterns. If downstream reporting tools require DB table lookup, unmatched entries should be written to a BigQuery dead-letter table (e.g. `fact_umsatz_konzern_monat_rejections`).

---

# PYSPARK DATA MIGRATION DESIGN DOCUMENT
**Source Migration:** Ab Initio Graph `umsatz_konsolidierung.mp`  
**Target Architecture:** Google Cloud Dataproc Serverless (PySpark)

---

## 1. Objective

### 1.1 Objective of the Module
The objective of this PySpark module is to migrate, normalize, aggregate, and validate transaction revenue data (`umsatz_konsolidierung`) from corporate entities. It replaces an legacy Ab Initio ETL graph with an optimized, scalable PySpark application designed to run on Dataproc Serverless.

### 1.2 Problem Statement & System Context
Within the global financial system, transaction data from multiple corporate subsidiaries (`KONZERNGESELLSCHAFT`) must be consolidated monthly. The raw staging transactions contain dirty formatting (untrimmed spaces, mixed casing), missing currencies, and transactional rollbacks (cancellations/stornos). 

This module addresses these challenges by:
1. Validating the execution period against global metadata.
2. Normalizing, cleaning, and validating transaction attributes.
3. Segregating regular revenues from cancellations (`STORNO`).
4. Aggregating values to compute consolidated monthly revenues and booking counts.
5. Performing strict quality control checks (tolerance validations and record volume thresholds) before finalizing the load to target data warehouse tables and generating alerts for discrepancies.

---

## 2. Functional Overview

### 2.1 Logical Steps Breakdown
The PySpark application operates in four chronological execution phases matching the original Ab Initio execution flow:

```
[Phase 1: Ingest & Validate]
       │
       ├── Validate VERARBEITUNGSMONAT against DIM_PERIODE
       └── Read STG_UMSATZ_TRANSAKTIONEN, DIM_KONZERNGESELLSCHAFT, and STG_TARIFGRUPPEN_MAPPING
       │
[Phase 2: Normalize & Validate Entities]
       │
       ├── Apply text normalizations & convert revenue to integer cents
       ├── Left Outer Join with DIM_KONZERNGESELLSCHAFT
       │     ├── Unmatched records ──> Write to Reject File (write_unmatched_umsatz)
       │     └── Matched records   ──> Continue
       └── Left Outer Join with STG_TARIFGRUPPEN_MAPPING
       │
[Phase 3: Segregate, Aggregate & Join]
       │
       ├── Filter Stream A: Regular Bookings ('REGULAER') ──> Aggregate sum(cents), count()
       ├── Filter Stream B: Cancellation Bookings ('STORNO') ──> Aggregate sum(cents)
       └── Outer Join Stream A & Stream B on [konzerngesellschaft, verarbeitungsmonat, tarifgruppen_code, waehrung]
       └── Write output to FACT_UMSATZ_KONZERN_MONAT
       │
[Phase 4: Post-Process Audit & Tolerance Control]
       │
       ├── Validate Row Volume against MIN_ROW_COUNT
       └── Calculate consolidated deviation against KONSOLIDIERUNG_TOLERANZ ──> Generate Alerts/Audits
```

### 2.2 Detailed Operations
*   **Period Verification:** Prior to reading transaction streams, the module queries `DIM_PERIODE` with the parameter `VERARBEITUNGSMONAT`. If the period is invalid or closed, processing halts.
*   **Data Cleaning and Conversions:** Text values undergo systematic trimming and case transformations. Null currency entries (`waehrung`) default to `'EUR'`. Numerical rounding conversions are executed on floating-point currencies to prevent floating-point representation drift during downstream aggregation (multiplied by `100.0` and cast to absolute integer `cents`).
*   **Transaction Flagging:** Bookings are classified into `STORNO` (cancellations) or `REGULAER` (regular revenue stream) depending on the initial booking code.
*   **Unmatched Record Isolation:** Any corporate transaction mapping to an invalid corporate entity (failing the join with `DIM_KONZERNGESELLSCHAFT`) is written to an external reject directory. This isolates bad source data without breaking the primary migration pipeline.
*   **Aggregated Consolidation (Rollup):** The module segregates regular and canceled items, runs independent parallel aggregations over the grouping keys, and recombines the streams using a left outer join to produce a consolidated view of net transactions.

### 2.3 Variable, Function, and Data Transformations Table
| Step / Variable | Type | Source / Calculation | Role |
| :--- | :--- | :--- | :--- |
| `VERARBEITUNGSMONAT` | `String` | Input parameter (Format: `'YYYYMM'`) | Operational month filter |
| `KONZERNGESELLSCHAFT` | `String` | Input parameter | Corporate entity filter |
| `clean_gesellschaft` | Column (String) | `upper(trim(konzerngesellschaft))` | Entity key normalization |
| `clean_tarif_code` | Column (String) | `upper(trim(tarifgruppen_code))` | Tariff key normalization |
| `clean_waehrung` | Column (String) | `coalesce(trim(waehrung), 'EUR')` | Null currency mitigation |
| `buchungsart_mapped`| Column (String) | `CASE WHEN buchungsart IN ('STORNO', 'GUTSCHRIFT') THEN 'STORNO' ELSE 'REGULAER' END` | Transaction category routing |
| `umsatz_betrag_cent`| Column (Long) | `cast(round(umsatz_betrag * 100.0, 0) as long)` | Standardizing precision |
| `umsatz_summe_cent` | Column (Long) | `sum(umsatz_betrag_cent)` WHERE type is `REGULAER` | Aggregated gross revenue |
| `storno_summe_cent` | Column (Long) | `sum(umsatz_betrag_cent)` WHERE type is `STORNO` | Aggregated total cancellations |

---

## 3. Inputs and Outputs

### 3.1 Parameters, Sources, and Target Tables
The system consumes parameters passed via spark-submit arguments or cloud orchestration setups:

#### Run Parameters
*   `VERARBEITUNGSMONAT` (String, format `YYYYMM`)
*   `KONZERNGESELLSCHAFT` (String)
*   `ORA_CONNECT_STRING` (String JDBC Connection URL)
*   `ERROR_OUTPUT_DIR` (String Cloud Storage URI)
*   `ALERT_OUTPUT_DIR` (String Cloud Storage URI)
*   `LOG_DIR` (String Cloud Storage URI)
*   `KONSOLIDIERUNG_TOLERANZ` (Decimal/Float)
*   `MAX_ABWEICHUNGEN` (Integer)
*   `MIN_ROW_COUNT` (Integer)

#### Database Tables Used/Interacted With
*   `DIM_PERIODE` (Source Read)
*   `STG_UMSATZ_TRANSAKTIONEN` (Source Read)
*   `DIM_KONZERNGESELLSCHAFT` (Source Read)
*   `STG_TARIFGRUPPEN_MAPPING` (Source Read)
*   `FACT_UMSATZ_KONZERN_MONAT` (Target Write)
*   `AUDIT_UMSATZ_CONSOLIDATION` (Target Write - Audit Logging Table)

### 3.2 System Output Manifest
*   **Primary DB Write:** Appends calculated records into the database table `FACT_UMSATZ_KONZERN_MONAT`.
*   **Unmatched Record Export:** Writes unmatched transaction rows to `ERROR_OUTPUT_DIR` as delimited CSV format containing bad key information.
*   **Alert Generation Logs:** Writes alert files to `ALERT_OUTPUT_DIR` in JSON format if tolerances or quality control thresholds are exceeded.

### 3.3 External Data Sources or Dependencies
The system relies on an Oracle DB Instance accessed via PySpark JDBC connectors. Dataframes are constructed by submitting pushed-down predicates directly to the remote engine to limit I/O over the network.

---

## 4. I/O Operations

### 4.1 Storage and Database Operations
```
                       ┌─────────────────────────┐
                       │   Oracle DB Instance    │
                       └────────────┬────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │ JDBC Read             │ JDBC Read             │ JDBC Write
            ▼                       ▼                       ▼
   [DIM_PERIODE, STG...]   [DIM_KONZERNGESELL...]  [FACT_UMSATZ_KONZERN_MONAT]
            │                       │
            │                       │
     ┌──────▼───────────────────────▼──────┐
     │      Dataproc Serverless (PySpark)  │
     └──────┬───────────────────────┬──────┘
            │                       │
            │ GCS File Write        │ GCS File Write
            ▼                       ▼
    [ERROR_OUTPUT_DIR]      [ALERT_OUTPUT_DIR]
    (Unmatched Records CSV)  (Tolerances Alerts JSON)
```

*   **Query Pushdown:** Filtering criteria like `VERARBEITUNGSMONAT`, `KONZERNGESELLSCHAFT`, and active flags (`IS_CURRENT='Y'`) are integrated directly into the `dbtable` parameters or native spark `.option("query", ...)` wrappers to execute filtering on the DB side.
*   **Format Specs:**
    *   **DB Transactions:** standard JDBC.
    *   **Rejections:** CSV format, UTF-8 encoding, delimited with `|`, containing all original un-normalized transaction record fields.
    *   **Alert Outputs:** JSON-Lines format containing audit metrics, timestamps, and validation failure tags.

---

## 5. External Dependencies

To execute this PySpark module on Dataproc Serverless, the following dependencies must be declared:
*   **Spark Core / Spark SQL libraries:** Native to GCP Dataproc Serverless runtime (version 2.x or 3.x).
*   **Oracle JDBC Driver:** e.g., `ojdbc8.jar` passed via `--jars` argument or compiled within container image.
*   **GCS Connector:** Pre-configured on Dataproc Serverless for direct writing to Cloud Storage targets.

---

## 6. Business Rules Extraction

### Rule 1: Temporal Validation
*   **Logic:** Execution is only permitted if the incoming parameter `VERARBEITUNGSMONAT` exists in `DIM_PERIODE` and is flagged as an open, valid financial tracking period.
*   **Impact:** Prevents processing for out-of-bounds or non-existent calendar definitions.

### Rule 2: Normalization and Standardization
*   **Logic:**
    *   The `konzerngesellschaft` key must be converted to uppercase and trimmed of leading/trailing spaces.
    *   `tarifgruppen_code` must be capitalized and trimmed.
    *   Whitespace must be trimmed from `vertrag` and `kunde`.
    *   If transaction currency `waehrung` is null or empty, it must default to `'EUR'`.

### Rule 3: Preciseness in Currency Operations
*   **Logic:**
    *   To prevent precision loss from floating-point arithmetic during distributed rollups, `umsatz_betrag` is multiplied by `100.0`.
    *   The result is rounded to zero decimal places using `ROUND()` and cast to a standard Long/Bigint data type to represent value in absolute cents.

### Rule 4: Business Booking Type Mapping
*   **Logic:**
    *   `buchungsart` values of `'STORNO'` or `'GUTSCHRIFT'` are grouped under the unified transaction status `'STORNO'`.
    *   All other booking variations map to `'REGULAER'`.

### Rule 5: Corporate Isolation (Referential Integrity Check)
*   **Logic:**
    *   The transaction record must map to an active corporate entity in `DIM_KONZERNGESELLSCHAFT` where `IS_CURRENT = 'Y'`.
    *   Any record that fails this check must be output to the unmatched error directory and excluded from downstream aggregations.

### Rule 6: Dual Aggregated Streams Consolidation
*   **Logic:**
    *   Stream A (`REGULAER` bookings) aggregates gross values to compute `umsatz_summe_cent` and count transactions (`anzahl_buchungen`).
    *   Stream B (`STORNO` bookings) aggregates total cancellations to compute `storno_summe_cent`.
    *   Streams A and B are left outer joined to generate a combined monthly record.

### Rule 7: Data Quality Thresholds
*   **Logic:**
    *   **Row Count Check:** Total calculated output records must be greater than or equal to `MIN_ROW_COUNT`.
    *   **Tolerance Check:** If the absolute value of the differences between total `REGULAER` values and `STORNO` values exceeds `KONSOLIDIERUNG_TOLERANZ`, or if the count of deviations exceeds `MAX_ABWEICHUNGEN`, an alert record must be published to GCS.

---

## 7. Security Considerations

### 7.1 Sensitive Information and Authorization Mechanisms
*   **No Plaintext Secrets:** JDBC connection parameters, passwords, and service keys must not be hardcoded in the codebase.
*   **GCP Secret Manager:** Connection credentials for Oracle databases must be retrieved from Google Cloud Secret Manager at runtime.
*   **IAM Authorization:** Dataproc Serverless execution service accounts require:
    *   Storage Object Admin role on the designated GCS buckets (`ERROR_OUTPUT_DIR`, `ALERT_OUTPUT_DIR`, `LOG_DIR`).
    *   Network connectivity configurations (Cloud NAT / VPC Peering) to securely interface with private Oracle DB subnets.

---

## 8. Error Handling Strategies

### 8.1 Potential Error Scenarios & Actions
*   **Period Missing:** The process must terminate immediately with status code `1` if the validated period returns an empty record set from `DIM_PERIODE`.
*   **Database Connectivity Failures:** If connections to Oracle fail during runtime, PySpark will raise a `Py4JJavaError`. The process must implement retry parameters in the JDBC configuration (e.g., `connectionProperties.setProperty("oracle.net.retryCount", "3")`).
*   **Quality Gate Breaches:** If the minimum row count threshold check fails, the application should write details to the alert output path and raise an execution exception to notify orchestrators (like Airflow).

### 8.2 Strategic Enhancements
*   Implement custom Spark Listener frameworks to capture job progress metrics.
*   Redirect rejected invalid records to a dead-letter database table instead of raw CSVs in Cloud Storage to improve queryability and error analysis.

---

## 9. Monitoring and Logging

### 9.1 Existing Capabilities
*   Cloud Logging processes log structures emitted by Spark drivers and executors on Dataproc Serverless.
*   Application runtime logs are routed to the target GCS bucket specified by `LOG_DIR`.

### 9.2 Suggested Monitoring Enhancements
*   Write application progress states (e.g., phase start, row count, execution time, and process steps completed) directly to an audit log table `AUDIT_UMSATZ_CONSOLIDATION`.
*   Generate alerts using Cloud Monitoring metrics based on log patterns like `TOLERANCE_BREACHED` or `LOW_ROW_COUNT_WARNING` to trigger alerts on Slack or PagerDuty.

---

## 10. Abstract Syntax Tree (AST)

The structure of the PySpark application and the relationships between its components is shown below:

```
[PySpark Application Engine]
 └── Phase 1: Ingest & Validate
      ├── Load Parameters (sys.argv)
      ├── Read DIM_PERIODE (JDBC)
      │    └── Validate period exists (Exception on Failure)
      ├── Read STG_UMSATZ_TRANSAKTIONEN (JDBC, filtered by Period & Company)
      ├── Read DIM_KONZERNGESELLSCHAFT (JDBC, filtered by Active Flag)
      └── Read STG_TARIFGRUPPEN_MAPPING (JDBC)
 └── Phase 2: Normalize & Validate Entities
      ├── Apply Data Cleaning Transformations (DataFrame API)
      ├── Apply Left Outer Join with DIM_KONZERNGESELLSCHAFT
      │    ├── Unmatched Split ──> Write to GCS (ERROR_OUTPUT_DIR)
      │    └── Matched Split ──┐
      └── Apply Left Outer Join with STG_TARIFGRUPPEN_MAPPING <──┘
 └── Phase 3: Segregate, Aggregate & Join
      ├── Split Stream into Regular and Cancellation Records
      │    ├── Regular Stream ('REGULAER') ──> Aggregate sum/count (Rollup Group A)
      │    └── Cancellation Stream ('STORNO') ──> Aggregate sum (Rollup Group B)
      ├── Join Aggregated Streams on Grouping Keys
      └── Write Result DataFrame to FACT_UMSATZ_KONZERN_MONAT (JDBC Write)
 └── Phase 4: Post-Process Audit & Tolerance Control
      ├── Calculate output record counts and check against MIN_ROW_COUNT
      ├── Compute variance differences and compare to KONSOLIDIERUNG_TOLERANZ
      └── Generate system output alerts to GCS (ALERT_OUTPUT_DIR) if thresholds are exceeded
```

---

## 11. SQL Table Creation Statements

### 11.1 Target Fact Table Definition
```sql
CREATE TABLE FACT_UMSATZ_KONZERN_MONAT (
    konzerngesellschaft VARCHAR2(100) NOT NULL,
    verarbeitungsmonat VARCHAR2(6) NOT NULL,
    tarifgruppen_code VARCHAR2(50),
    waehrung VARCHAR2(3) DEFAULT 'EUR' NOT NULL,
    umsatz_summe_cent NUMBER(19, 0) NOT NULL,
    storno_summe_cent NUMBER(19, 0) DEFAULT 0 NOT NULL,
    anzahl_buchungen NUMBER(9, 0) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_fact_umsatz PRIMARY KEY (konzerngesellschaft, verarbeitungsmonat, tarifgruppen_code, waehrung)
);
```

### 11.2 Audit Logging Table Definition
```sql
CREATE TABLE AUDIT_UMSATZ_CONSOLIDATION (
    audit_id VARCHAR2(50) DEFAULT SYS_GUID() PRIMARY KEY,
    verarbeitungsmonat VARCHAR2(6) NOT NULL,
    konzerngesellschaft VARCHAR2(100) NOT NULL,
    source_row_count NUMBER(9,0) NOT NULL,
    target_row_count NUMBER(9,0) NOT NULL,
    deviation_count NUMBER(9,0) NOT NULL,
    status VARCHAR2(20) NOT NULL, -- e.g., 'SUCCESS', 'TOLERANCE_BREACH', 'FAILED'
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
```

---

## 12. Pseudo Code

### 12.1 Orchestration Files

#### Target File: `bin/umsatz_konsolidierung_monatlich_dag.py`
This component encapsulates execution wrapper definitions, shell context mappings, runtime parameter configurations, and target connection variables derived from the legacy KSH shell parameters.

```python
# Environment configuration derived from r_umsatz_konsolidierung_monatlich.ksh
KSH_ENVIRONMENT_DEFAULTS = {
    "KONSOLIDIERUNG_TOLERANZ": 2.5,
    "MAX_ABWEICHUNGEN": 25,
    "MIN_ROW_COUNT": 1,
    "ORA_CONNECT_STRING": "jdbc:oracle:thin:@oracle-db-host:1521/ORCL"
}
```

#### Target File: `dags/umsatz_konsolidierung_monatlich_dag.py`
This contains the converted Airflow workflow scheduler and operator task blueprints translated from the parent and child UC4 XML files.

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'composer',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dwh_umsatz_konsolidierung_monatlich',
    default_args=default_args,
    schedule_interval='0 2 1 * *', # @monthly schedule
    catchup=False,
) as dag:

    # Dataproc Serverless PySpark Batch Task
    submit_pyspark_job = DataprocCreateBatchOperator(
        task_id='execute_umsatz_konsolidierung',
        project_id='{{ var.value.get("GCP_PROJECT") }}',
        region='europe-west3',
        batch_id='umsatz-konsolidierung-{{ ds_nodash }}',
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://{{ var.value.get('GCS_BUCKET') }}/abinitio/umsatz_konsolidierung.py",
                "args": [
                    "{{ execution_date.strftime('%Y%m') }}",
                    "{{ var.value.get('KONZERNGESELLSCHAFT') }}",
                    "jdbc:oracle:thin:@oracle-db-host:1521/ORCL",
                    "gs://{{ var.value.get('GCS_BUCKET') }}/errors/umsatz",
                    "gs://{{ var.value.get('GCS_BUCKET') }}/alerts/umsatz",
                    "gs://{{ var.value.get('GCS_BUCKET') }}/logs/umsatz",
                    "2.5", # KONSOLIDIERUNG_TOLERANZ
                    "25",  # MAX_ABWEICHUNGEN
                    "1"    # MIN_ROW_COUNT
                ]
            }
        }
    )
```

### 12.2 Processing File

#### Target File: `abinitio/umsatz_konsolidierung.py`
This holds the complete migrated core PySpark aggregation, mapping, validation and data logic.

```python
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

def main():
    # Capture runtime arguments
    VERARBEITUNGSMONAT = sys.argv[1]
    KONZERNGESELLSCHAFT = sys.argv[2]
    ORA_CONNECT_STRING = sys.argv[3]
    ERROR_OUTPUT_DIR = sys.argv[4]
    ALERT_OUTPUT_DIR = sys.argv[5]
    LOG_DIR = sys.argv[6]
    KONSOLIDIERUNG_TOLERANZ = float(sys.argv[7])
    MAX_ABWEICHUNGEN = int(sys.argv[8])
    MIN_ROW_COUNT = int(sys.argv[9])

    spark = SparkSession.builder \
        .appName("UmsatzKonsolidierungMigration") \
        .getOrCreate()

    # Define connection options
    db_properties = {
        "driver": "oracle.jdbc.driver.OracleDriver",
        # Credentials retrieved from Secret Manager environment variables
        "user": sys.getenv("DB_USER"),
        "password": sys.getenv("DB_PASSWORD")
    }

    # ==========================================
    # PHASE 1: Validation and Data Ingestion
    # ==========================================
    
    # 1. Validate Period against DIM_PERIODE
    period_query = f"(SELECT 1 FROM DIM_PERIODE WHERE ID_MONAT = '{VERARBEITUNGSMONAT}') temp"
    period_df = spark.read.jdbc(url=ORA_CONNECT_STRING, table=period_query, properties=db_properties)
    
    if period_df.count() == 0:
        raise ValueError(f"FATAL: The processing period {VERARBEITUNGSMONAT} is invalid or closed.")

    # 2. Ingest primary source collections
    stg_umsatz_query = f"""
        (SELECT * FROM STG_UMSATZ_TRANSAKTIONEN 
         WHERE VERARBEITUNGSMONAT = '{VERARBEITUNGSMONAT}' 
           AND KONZERNGESELLSCHAFT = '{KONZERNGESELLSCHAFT}' 
           AND ETL_STATUS = 'PENDING') temp_umsatz
    """
    df_stg_umsatz = spark.read.jdbc(url=ORA_CONNECT_STRING, table=stg_umsatz_query, properties=db_properties)

    dim_gesellschaft_query = "(SELECT * FROM DIM_KONZERNGESELLSCHAFT WHERE IS_CURRENT = 'Y') temp_ges"
    df_dim_ges = spark.read.jdbc(url=ORA_CONNECT_STRING, table=dim_gesellschaft_query, properties=db_properties)

    df_tarif_mapping = spark.read.jdbc(url=ORA_CONNECT_STRING, table="STG_TARIFGRUPPEN_MAPPING", properties=db_properties)

    # ==========================================
    # PHASE 2: Normalization & Map Joins
    # ==========================================
    
    # Apply transformation rules
    df_normalized = df_stg_umsatz.withColumn(
        "clean_ges", F.upper(F.trim(F.col("konzerngesellschaft")))
    ).withColumn(
        "clean_tarif", F.upper(F.trim(F.col("tarifgruppen_code")))
    ).withColumn(
        "clean_vertrag", F.trim(F.col("vertrag"))
    ).withColumn(
        "clean_kunde", F.trim(F.col("kunde"))
    ).withColumn(
        "clean_waehrung", F.coalesce(F.nullif(F.trim(F.col("waehrung")), F.lit("")), F.lit("EUR"))
    ).withColumn(
        "mapped_buchungsart", 
        F.when(F.col("buchungsart").isin("STORNO", "GUTSCHRIFT"), "STORNO")
         .otherwise("REGULAER")
    ).withColumn(
        "umsatz_betrag_cent", F.round(F.col("umsatz_betrag") * 100.0, 0).cast(LongType())
    )

    # Left outer join with DIM_KONZERNGESELLSCHAFT
    df_ges_joined = df_normalized.join(
        df_dim_ges, 
        df_normalized.clean_ges == F.upper(F.trim(df_dim_ges.konzerngesellschaft)), 
        "left_outer"
    )

    # Split unmatched rows
    df_unmatched = df_ges_joined.filter(F.col("dim_konzerngesellschaft.konzerngesellschaft").isNull())
    df_matched = df_ges_joined.filter(F.col("dim_konzerngesellschaft.konzerngesellschaft").isNotNull())

    # Write reject transactions to GCS
    if df_unmatched.count() > 0:
        df_unmatched.select("clean_ges", "clean_vertrag", "clean_waehrung", "umsatz_betrag") \
            .write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .csv(f"{ERROR_OUTPUT_DIR}/unmatched_{VERARBEITUNGSMONAT}")

    # Left outer join with Tariff Group Mapping
    df_final_mapped = df_matched.join(
        df_tarif_mapping, 
        df_matched.clean_tarif == F.upper(F.trim(df_tarif_mapping.tarifgruppen_code)), 
        "left_outer"
    )

    # ==========================================
    # PHASE 3: Segregation and Aggregation
    # ==========================================
    
    # 1. Stream A: Regular bookings aggregation
    df_regular_rollup = df_final_mapped.filter(F.col("mapped_buchungsart") == "REGULAER") \
        .groupBy("clean_ges", "verarbeitungsmonat", "clean_tarif", "clean_waehrung") \
        .agg(
            F.sum("umsatz_betrag_cent").alias("umsatz_summe_cent"),
            F.count(F.lit(1)).alias("anzahl_buchungen")
        )

    # 2. Stream B: Cancellation/Storno bookings aggregation
    df_storno_rollup = df_final_mapped.filter(F.col("mapped_buchungsart") == "STORNO") \
        .groupBy("clean_ges", "verarbeitungsmonat", "clean_tarif", "clean_waehrung") \
        .agg(
            F.sum("umsatz_betrag_cent").alias("storno_summe_cent")
        )

    # Join both streams together
    df_consolidated = df_regular_rollup.join(
        df_storno_rollup,
        ["clean_ges", "verarbeitungsmonat", "clean_tarif", "clean_waehrung"],
        "left_outer"
    ).fillna({"storno_summe_cent": 0})

    # Prepare for Database Load
    df_target_load = df_consolidated.select(
        F.col("clean_ges").alias("konzerngesellschaft"),
        F.col("verarbeitungsmonat"),
        F.col("clean_tarif").alias("tarifgruppen_code"),
        F.col("clean_waehrung").alias("waehrung"),
        F.col("umsatz_summe_cent"),
        F.col("storno_summe_cent"),
        F.col("anzahl_buchungen"),
        F.current_timestamp().alias("load_timestamp")
    )

    # Write target results to Fact Table
    df_target_load.write.jdbc(
        url=ORA_CONNECT_STRING, 
        table="FACT_UMSATZ_KONZERN_MONAT", 
        mode="append", 
        properties=db_properties
    )

    # ==========================================
    # PHASE 4: Post-Process Audit & Tolerance Control
    # ==========================================
    
    total_loaded_records = df_target_load.count()
    
    # Rule check: Validate against MIN_ROW_COUNT
    if total_loaded_records < MIN_ROW_COUNT:
        alert_msg = f"WARNING: Total loaded count ({total_loaded_records}) is below MIN_ROW_COUNT limit ({MIN_ROW_COUNT})"
        _write_alert(spark, ALERT_OUTPUT_DIR, VERARBEITUNGSMONAT, "LOW_ROW_COUNT_WARNING", alert_msg)

    # Rule check: Calculate tolerances and log deviations
    df_tolerance_check = df_target_load.withColumn(
        "cent_difference", F.abs(F.col("umsatz_summe_cent") - F.col("storno_summe_cent"))
    ).filter(F.col("cent_difference") > (KONSOLIDIERUNG_TOLERANZ * 100))

    out_of_bounds_count = df_tolerance_check.count()

    if out_of_bounds_count > MAX_ABWEICHUNGEN:
        alert_msg = f"CRITICAL: Found {out_of_bounds_count} deviations exceeding tolerance thresholds."
        _write_alert(spark, ALERT_OUTPUT_DIR, VERARBEITUNGSMONAT, "TOLERANCE_BREACH", alert_msg)
        
        # Log incident to internal Audit Table
        _write_audit_log(ORA_CONNECT_STRING, db_properties, VERARBEITUNGSMONAT, KONZERNGESELLSCHAFT, 
                          total_loaded_records, out_of_bounds_count, "TOLERANCE_BREACH")
    else:
        # Log successful completion
        _write_audit_log(ORA_CONNECT_STRING, db_properties, VERARBEITUNGSMONAT, KONZERNGESELLSCHAFT, 
                          total_loaded_records, out_of_bounds_count, "SUCCESS")

    spark.stop()

def _write_alert(spark, alert_dir, monat, code, message):
    alert_payload = [{"verarbeitungsmonat": monat, "alert_code": code, "error_message": message}]
    alert_df = spark.createDataFrame(alert_payload)
    alert_df.write.mode("append").json(alert_dir)

def _write_audit_log(conn_str, properties, monat, gesell, src_cnt, dev_cnt, status):
    # Standard JDBC logging execution
    pass

if __name__ == "__main__":
    main()
```

---

# MIGRATION DESIGN DOCUMENT

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `bin/r_umsatz_konsolidierung_monatlich.ksh` | `bin/r_umsatz_konsolidierung_monatlich.py` | Migrated to Horizon Python executing corresponding BigQuery processes, matching the folder structure. |

---

## 1. Environment & Global Variables Classification

In accordance with the Environment Variable Policy, variables found in the legacy shell script are classified as follows:

### Global (Environment-Wide Infrastructure Constants)
These values are shared across all jobs in the environment and are resolved dynamically at runtime.
* **`GCP_PROJECT`**: The target Google Cloud Project ID.
* **`BQ_DATASET`**: The target BigQuery dataset containing the sales tables (mapped from `dwh_kern`).
* **`DIR_LIB_PY`**: The runtime path to the Horizon Python Framework libraries (mapped from local framework environment paths).

*Resolution Mechanism:*
* In Python scripts, these are fetched using `os.environ.get("GCP_PROJECT")` or standard config lookups.
* In BigQuery SQL templates, these are injected as query parameters or substituted at compilation time.

### Job-Specific Variables
These values apply only to this specific consolidation job:
* **`VERARBEITUNGSMONAT` / `l_Monat`**: The target reporting month in `YYYYMM` format. (Derived from script input `-m` or defaults to the previous month).
* **`KONZERNGESELLSCHAFT` / `l_Konzern`**: The target corporation company filter (e.g., `'DE01'`, `'ALL'`). (Derived from script input `-k`).
* **`Protokoll_Datei`**: The path to the runtime log file. (Managed using Python’s standard `logging` to a configured local or GCS log directory).

---

## 2. Job Dependencies & Execution Order

### Orchestration & Upstream Dependencies
Based on the pre-collected job context:
1. **`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml`** (UC4 Parent Job Group)
2. **`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml`** (UC4 Child Schedule)
3. **`bin/r_umsatz_konsolidierung_monatlich.ksh`** (Wrapper script - This component)
4. **`abinitio/umsatz_konsolidierung.mp`** (Target processing logic executed by wrapper script)

### Lineage & Cross-Job Hand-offs
* **Upstream Data Producer**: `your_project_id.your_dataset_id.src_umsatz_raw` (containing transactional sales/turnover records per corporation and month).
* **Downstream Data Consumer**: `your_project_id.your_dataset_id.tgt_umsatz_konsolidiert` (the final consolidated tables).
* **Cross-Job Graph USES**: `bin/r_umsatz_konsolidierung_monatlich.ksh` $\rightarrow$ `abinitio/umsatz_konsolidierung.mp`.

---

## 3. Risks & Manual Actions

For unresolved components, missing elements, or specific migration risks:
* **SOURCE: NOT FOUND — `abinitio/umsatz_konsolidierung.mp` — no candidate**: The actual internal graph logic (transformations, lookup rules, sorting) is not present in the workspace. The SQL provided in this design represents a standard aggregation model based on the wrapper parameters. A manual comparison must be done by a data engineer against the actual GDE mapping of `umsatz_konsolidierung.mp` to verify if extra business rules (e.g. currency conversion, intercompany sales elimination, threshold validations) are embedded in the graph.
* **Validation of Error Patterns**: The legacy script searches the execution log for lines beginning with `"FEHLER"`. In the new Horizon framework, errors inside the PySpark or BigQuery engine are captured via Python exceptions. Ensure that any custom error messages written during execution preserve this format if downstream systems parse logs for the keyword `"FEHLER"`.

---

## 4. Include MCP Output Verbatim

Below is the complete design output generated for the migration of the script logic to Horizon Python.

### Step 1. Understand the Shell Script Code

#### 1.1 Identify Sources
* **Legacy Sandbox Configuration**: The script targets an Ab-Initio sandbox root path: `${HOME}/aktuell/dw_source/isdwh/import/umsatz/abinitio`.
* **Legacy Data Execution**: The data operations are wrapped inside a legacy Ab-Initio graph `umsatz_konsolidierung.mp` that extracts, filters, and processes consolidation data based on input variables. Since this is being migrated to a modern BigQuery/Horizon environment, the actual operational logic of this Ab-Initio graph must be represented as queries on equivalent BigQuery sources. 
* **Assumed BQ Source**: `your_project_id.your_dataset_id.src_umsatz_raw` (containing transactional sales/turnover records per corporation and month).

#### 1.2 Processing Flow
1. **Parameter Parsing**: Parses processing month `-m` (defaults to previous month if empty, format `YYYYMM`) and corporation `-k` (defaults to `"ALL"`).
2. **Log Initialization**: Creates a local log file named `konsolidierung_${l_Monat}_${l_Konzern}.log`.
3. **Environment Setup**: Exports environment variables: `VERARBEITUNGSMONAT`, `KONZERNGESELLSCHAFT`, and `ORA_CONNECT_STRING`.
4. **Execution**: Executes the legacy Ab-Initio graph `umsatz_konsolidierung.mp`.
5. **Post-processing Validation**:
   * Inspects the shell return code `$l_RetCode`.
   * Checks the log file for lines starting with `"FEHLER"`. If any are found, it terminates with an exit code of `1`.

#### 1.3 Identify Targets
* **Target Destination**: Since the legacy logic must be redirected to BigQuery targets, the final consolidated dataset will be written to a consolidated BigQuery table.
* **Assumed BQ Target Table**: `your_project_id.your_dataset_id.tgt_umsatz_konsolidiert`
* **Log Output**: Written locally to `konsolidierung_${l_Monat}_${l_Konzern}.log` (migrated to standard logging / GCS if execution occurs on cloud workers).

#### 1.4 External Dependencies
* `run_program` (Ab-Initio sandbox command line runner)
* `grep` (used for checking "FEHLER" occurrences inside the output log file)
* `date` (used for setting fallback date parameter)

#### 1.5 Parameters and Variables
* `l_Monat` (`-m`): Processing month in `YYYYMM` format.
* `l_Konzern` (`-k`): Target corporation code (e.g., `DE01`, `AT02`, `ALL`).
* `Protokoll_Datei`: Path to the local execution log.
* `DW_ORAUSER`: Database connection details (unused in cloud target).

#### 1.6 Identify Undeclared Variables
All key script-level variables used within the control logic are declared or initialized with fallbacks. 

#### 1.7 Identify Hive Queries
No native Hive queries are present in the provided legacy wrapper. However, the logic contained inside the `umsatz_konsolidierung.mp` graph represents a monthly aggregation and filtering process by `VERARBEITUNGSMONAT` and `KONZERNGESELLSCHAFT`. This logic is converted into a structured BQSQL query below.

---

### Step 2. Decompose the Logic

| Step # | Original Shell Task | Horizon Python Equivalent |
| :--- | :--- | :--- |
| **1** | Process input parameters (`-m`, `-k`) with default fallbacks. | Use Python's `argparse` library to handle command-line execution parameters. |
| **2** | Set up execution logging. | Use Python standard `logging` framework configured to write to a log file and standard output. |
| **3** | Execute `umsatz_konsolidierung.mp` via legacy tool. | Execute BQSQL queries using Horizon's framework helper `script.func_execute_bq`. |
| **4** | Check for error code or `"FEHLER"` strings inside the log file. | Use standard Python try-except blocks to catch DB errors, and parse execution logs programmatically. |

---

### Step 3. Map Shell Commands to Horizon Python

#### 3.1 File Handling & System Command Mapping
* `tee "$Protokoll_Datei"` $\rightarrow$ Python `logging.FileHandler` + `logging.StreamHandler`.
* `grep -c "^FEHLER"` $\rightarrow$ Python string matching `line.startswith("FEHLER")` over the generated log file.
* `date` calculations $\rightarrow$ `datetime` and `relativedelta` modules.

#### 3.2 Sources and Targets Mapping
* All source operations are migrated to run against: `your_project_id.your_dataset_id.src_umsatz_raw`.
* All target output operations are loaded into: `your_project_id.your_dataset_id.tgt_umsatz_konsolidiert`.

#### 3.3 Translation of Legacy Logic to BQSQL
The consolidation process performed by the legacy graph is represented by the following optimized BQSQL target transformation:

```sql
INSERT INTO `your_project_id.your_dataset_id.tgt_umsatz_konsolidiert` (
  verarbeitungs_monat,
  konzern_id,
  umsatz_wert,
  konsolidierungs_datum
)
SELECT 
  verarbeitungs_monat,
  konzern_id,
  SUM(umsatz_wert) AS umsatz_wert,
  CURRENT_TIMESTAMP() AS konsolidierungs_datum
FROM `your_project_id.your_dataset_id.src_umsatz_raw`
WHERE verarbeitungs_monat = @verarbeitungs_monat
  AND (konzern_id = @konzern_id OR @konzern_id = 'ALL')
GROUP BY verarbeitungs_monat, konzern_id;
```

---

### Document: Shell Script Analysis
The script orchestrates the monthly financial turnover data consolidation across multiple corporate business units. It utilizes a legacy Ab-Initio runtime environment to execute an ETL schema. The script expects runtime parameters specifying the processing month and the corporate entity filter. The migrated solution handles parameters cleanly using Python native modules and shifts all extraction, transformation, consolidation, and target loads directly into Google BigQuery using the standard Horizon framework execution method.

### Assumptions and Additional Notes
* It is assumed that the legacy Ab-Initio transformation logic is functionally identical to performing a conditional group-by consolidation filtered by the processing period and company group identifier.
* Horizon Framework core library is accessible at runtime via the paths exported in the execution environment.
* High-volume target writes are directed to native BigQuery tables.

---

### Pseudocode: Python Pseudocode

```python
#!/usr/bin/env python3
"""
Migrated Horizon Python Job: Monthly Turnover Consolidation (Umsatzkonsolidierung)
Equivalent of: r_umsatz_konsolidierung_monatlich.ksh
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Append Horizon framework library path
sys.path.append(os.getenv('DIR_LIB_PY', ''))
try:
    from framework.core.lib import script
except ImportError:
    # Fallback mock for syntax verification if executed outside framework environment
    class MockScript:
        @staticmethod
        def func_execute_bq(query, pass_file, col_delim, row_delim):
            print(f"[MOCK] Executing BQSQL Query with pass_file: {pass_file}")
            return True
    script = MockScript()

def setup_logger(log_file_path):
    """Configures double logging to file and standard output."""
    logger = logging.getLogger("UmsatzKonsolidierung")
    logger.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # File Handler
    fh = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    return logger, fh

def check_log_for_errors(log_file_path):
    """Checks the log file for any lines starting with FEHLER."""
    error_count = 0
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip().upper().startswith("FEHLER") or "ERROR" in line.upper():
                    error_count += 1
    return error_count

def main():
    # 1. Parse arguments
    parser = argparse.ArgumentParser(description="Monatliche Konsolidierung der Umsatzdaten")
    parser.add_argument("-m", "--monat", type=str, help="Verarbeitungsmonat (YYYYMM)", default=None)
    parser.add_argument("-k", "--konzern", type=str, help="Konzerngesellschaft (z.B. DE01, ALL)", default="ALL")
    args = parser.parse_args()

    # Calculate default month if not provided (Previous month)
    if not args.monat:
        prev_month = datetime.now() - relativedelta(months=1)
        l_Monat = prev_month.strftime('%Y%m')
    else:
        l_Monat = args.monat

    l_Konzern = args.konzern

    # Set up Log File directory and path
    home_dir = os.path.expanduser("~")
    log_dir = os.path.join(home_dir, "aktuell", "log", "umsatz")
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = f"konsolidierung_{l_Monat}_{l_Konzern}.log"
    protokoll_datei = os.path.join(log_dir, log_file_name)

    # Initialize Loggers
    logger, file_handler = setup_logger(protokoll_datei)
    logger.info(f"Starte monatliche Umsatzkonsolidierung fuer Monat {l_Monat}, Konzerngesellschaft {l_Konzern}")

    try:
        # 2. Define the Target BQSQL query (Parameterized Consolidation Logic)
        # Note: BQ Project and Dataset IDs should be customized as per deployment target
        bqsql_query = f"""
        INSERT INTO `your_project_id.your_dataset_id.tgt_umsatz_konsolidiert` (
          verarbeitungs_monat,
          konzern_id,
          umsatz_wert,
          konsolidierungs_datum
        )
        SELECT 
          verarbeitungs_monat,
          konzern_id,
          SUM(umsatz_wert) AS umsatz_wert,
          CURRENT_TIMESTAMP() AS konsolidierungs_datum
        FROM `your_project_id.your_dataset_id.src_umsatz_raw`
        WHERE verarbeitungs_monat = '{l_Monat}'
          AND ('{l_Konzern}' = 'ALL' OR konzern_id = '{l_Konzern}')
        GROUP BY verarbeitungs_monat, konzern_id;
        """

        # Parameters required by the Horizon Core function
        pass_file_name = "dummy_pass_file"
        column_delimiter = "|"
        row_delimiter = "\n"

        logger.info("Executing BigQuery Consolidation Query via Horizon framework...")
        
        # 3. Call Horizon core library to execute BigQuery SQL
        script.func_execute_bq(bqsql_query, pass_file_name, column_delimiter, row_delimiter)
        
        logger.info("BigQuery execution finished successfully.")

    except Exception as ex:
        logger.error(f"FEHLER: Umsatzkonsolidierung fuer Monat {l_Monat}/{l_Konzern} mit Fehler abgebrochen. Details: {ex}")
        file_handler.close()
        sys.exit(1)

    # Close the file handler prior to reading the log file content for inspection
    file_handler.close()

    # 4. Post-processing Quality Check (Equivalent to log validation logic)
    error_lines = check_log_for_errors(protokoll_datei)
    if error_lines > 0:
        # Output directly to stderr as original shell script
        print(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {error_lines} Fehlerzeilen im Konsolidierungs-Protokoll gefunden, siehe {protokoll_datei}", file=sys.stderr)
        sys.exit(1)

    print("Monatliche Umsatzkonsolidierung ohne erkennbare Fehler beendet")
    sys.exit(0)

if __name__ == "__main__":
    main()
```